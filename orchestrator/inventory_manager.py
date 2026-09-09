"""
inventory_manager.py — DESCRIBE DETAIL execution and migration_control upserts.

INVENTORY mode responsibilities:
1. Validate source table existence.
2. Execute DESCRIBE DETAIL (direct_adls only) on the source warehouse.
3. Capture location, sizeInBytes, numFiles, format, Delta version.
4. Classify workload.
5. Idempotently upsert one migration_control record per table.
6. Transition: DISCOVERED → ONBOARDED → WAITING_FOR_LOAD → QUEUED.

This module NEVER executes a clone. It only reads source metadata and
writes to the control table.
"""

from __future__ import annotations
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from orchestrator.models import (
    MigrationRecord, MigrationStatus, TableSelection, TableInventory, WorkloadClass
)
from orchestrator.config import OrchestratorConfig
from orchestrator.sql_client import SqlClient
from orchestrator.workload_classifier import WorkloadClassifier

log = logging.getLogger(__name__)

_TS = lambda: datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class InventoryManager:
    """
    Idempotent inventory: upserts migration_control records.

    An existing COMPLETED or VALIDATED record is not re-onboarded unless
    `force=True` is passed to `run_inventory`.
    """

    def __init__(
        self,
        config:       OrchestratorConfig,
        src_sql:      SqlClient,   # for DESCRIBE DETAIL (direct_adls)
        tgt_sql:      SqlClient,   # for control-table reads/writes
        classifier:   WorkloadClassifier,
        run_id:       str,
    ):
        self._cfg       = config
        self._src       = src_sql
        self._tgt       = tgt_sql
        self._cls       = classifier
        self._run_id    = run_id
        self._ctrl      = f"{config.meta_catalog}.{config.meta_schema}.migration_control"

    # ── Main entry ────────────────────────────────────────────────────────────

    def run_inventory(
        self,
        selections: List[TableSelection],
        force:      bool = False,
    ) -> dict:
        """
        Process a list of TableSelections.

        Returns:
            {
              "total":    int,
              "inserted": int,
              "skipped":  int,  # already completed
              "failed":   int,  # source not found or DESCRIBE error
              "records":  List[MigrationRecord]  # newly onboarded
            }
        """
        stats = {"total": 0, "inserted": 0, "skipped": 0, "failed": 0, "records": []}
        for sel in selections:
            stats["total"] += 1
            try:
                rec = self._process_one(sel, force)
                if rec is None:
                    stats["skipped"] += 1
                else:
                    stats["inserted"] += 1
                    stats["records"].append(rec)
            except Exception as e:
                stats["failed"] += 1
                log.error("Inventory failed for %s: %s", sel.source_fqn, e)
                self._mark_permanent_failure(sel, str(e))
        return stats

    # ── Per-table logic ───────────────────────────────────────────────────────

    def _process_one(self, sel: TableSelection, force: bool) -> Optional[MigrationRecord]:
        # Check if already in control table
        existing = self._get_existing(sel.source_fqn)

        if existing and not force:
            if existing.get("status") in (
                MigrationStatus.COMPLETED.value,
                MigrationStatus.VALIDATED.value,
                MigrationStatus.FAILED_PERMANENT.value,
                MigrationStatus.SKIPPED.value,
            ):
                log.info("Skipping %s — already %s", sel.source_fqn, existing["status"])
                return None

        # Validate source existence (UC metadata check via API)
        inv = self._run_describe_detail(sel)
        if inv is None:
            log.warning("Source table %s not found or not Delta — marking FAILED_PERMANENT", sel.source_fqn)
            self._mark_permanent_failure(sel, "Source table not found or not a Delta table")
            return None

        # Classify workload
        wl_class, wl_weight = self._cls.classify(inv.size_in_bytes)

        # Build record
        mid = (existing or {}).get("migration_id") or str(uuid.uuid4())
        now = _TS()
        rec = MigrationRecord(
            migration_id      = mid,
            run_id            = self._run_id,
            clone_type        = self._cfg.clone_type,
            source_workspace  = self._cfg.source_workspace_url,
            target_workspace  = self._cfg.target_workspace_url,
            source_catalog    = sel.source_catalog,
            source_schema     = sel.source_schema,
            source_table      = sel.source_table,
            target_catalog    = sel.target_catalog,
            target_schema     = sel.target_schema,
            target_table      = sel.target_table,
            source_path       = inv.source_path,
            size_in_bytes     = inv.size_in_bytes,
            size_gb           = inv.size_in_bytes / (1024 ** 3),
            workload_class    = wl_class,
            workload_weight   = wl_weight,
            status            = MigrationStatus.QUEUED.value,
            attempt_number    = 0,
            max_attempts      = self._cfg.max_retries,
            source_num_files  = inv.num_files,
            source_version    = inv.source_version,
            discovered_at     = now,
            onboarded_at      = now,
            queued_at         = now,
            created_at        = now,
            updated_at        = now,
            # Set batch_id immediately so parallel inventories don't steal each other's records
            batch_id          = getattr(self._cfg, "batch_id", "") or "",
        )

        # Upsert to control table
        self._upsert(rec)
        log.info(
            "Onboarded %s → %s [%s %.2f GB w=%d]",
            sel.source_fqn, sel.target_fqn, wl_class, rec.size_gb, wl_weight
        )
        return rec

    # ── DESCRIBE DETAIL ───────────────────────────────────────────────────────

    def _run_describe_detail(self, sel: TableSelection) -> Optional[TableInventory]:
        """
        Execute DESCRIBE DETAIL on source. For delta_share mode, the source FQN
        is available directly; for direct_adls we need the ADLS path.

        Missed scenario handled:
        - Non-Delta objects (views, foreign tables) → return None → SKIPPED.
        - Empty tables (0 rows/files) → still valid, return inventory with zeros.
        """
        try:
            # Use backtick-quoted per-component to handle catalog names with dashes
            fqn = f"`{sel.source_catalog}`.`{sel.source_schema}`.`{sel.source_table}`"
            rows = self._src.execute(f"DESCRIBE DETAIL {fqn}")
            if not rows:
                return None
            r = rows[0]
            fmt = (r.get("format") or "").upper()
            if fmt != "DELTA":
                log.warning("%s is %s, not Delta — will skip", sel.source_fqn, fmt or "UNKNOWN")
                return None
            return TableInventory(
                source_path    = r.get("location") or "",
                size_in_bytes  = int(r.get("sizeInBytes") or 0),
                size_gb        = int(r.get("sizeInBytes") or 0) / (1024 ** 3),
                num_files      = int(r.get("numFiles") or 0),
                format         = "DELTA",
                source_version = self._get_delta_version(sel),
                created_at     = str(r.get("createdAt") or ""),
                last_modified  = str(r.get("lastModified") or ""),
                is_delta       = True,
            )
        except RuntimeError as e:
            err = str(e)
            if "TABLE_OR_VIEW_NOT_FOUND" in err or "SCHEMA_NOT_FOUND" in err:
                return None
            raise

    def _get_delta_version(self, sel: TableSelection) -> Optional[int]:
        """Get current Delta version via DESCRIBE HISTORY LIMIT 1."""
        try:
            fqn = f"`{sel.source_catalog}`.`{sel.source_schema}`.`{sel.source_table}`"
            rows = self._src.execute(f"DESCRIBE HISTORY {fqn} LIMIT 1")
            if rows:
                return int(rows[0].get("version") or 0)
        except Exception:
            pass
        return None

    # ── Control table operations ──────────────────────────────────────────────

    def _get_existing(self, source_fqn: str) -> Optional[dict]:
        parts = source_fqn.split(".")
        if len(parts) != 3:
            return None
        cat, sch, tbl = parts
        try:
            rows = self._tgt.execute(f"""
                SELECT migration_id, status, attempt_number
                FROM {self._ctrl}
                WHERE source_catalog = '{cat}'
                  AND source_schema  = '{sch}'
                  AND source_table   = '{tbl}'
                LIMIT 1
            """)
            return rows[0] if rows else None
        except Exception:
            return None

    def _upsert(self, rec: MigrationRecord) -> None:
        """MERGE into migration_control (idempotent on migration_id)."""
        loc = (rec.source_path or "").replace("'", "\\'")
        err_msg = (rec.error_message or "").replace("'", "\\'")
        batch_id_val = (rec.batch_id or "").replace("'", "\\'")
        q = f"""
        MERGE INTO {self._ctrl} AS t
        USING (SELECT '{rec.migration_id}' AS migration_id) AS s
        ON t.migration_id = s.migration_id
        WHEN MATCHED THEN UPDATE SET
          run_id            = '{rec.run_id}',
          status            = '{rec.status}',
          source_path       = '{loc}',
          size_in_bytes     = {rec.size_in_bytes},
          size_gb           = {rec.size_gb:.6f},
          workload_class    = '{rec.workload_class}',
          workload_weight   = {rec.workload_weight},
          attempt_number    = 0,
          max_attempts      = {rec.max_attempts},
          source_num_files  = {rec.source_num_files or 0},
          source_version    = {rec.source_version or 0},
          batch_id          = '{batch_id_val}',
          -- Target mapping can legitimately change between onboards (e.g. a
          -- CSV/YAML edit renames the target table) — without updating these,
          -- a re-onboarded (esp. force_reonboard=true) row silently kept
          -- whichever target_catalog/schema/table it was FIRST inserted
          -- with, ignoring the current CSV/YAML's mapping.
          target_catalog    = '{rec.target_catalog}',
          target_schema     = '{rec.target_schema}',
          target_table      = '{rec.target_table}',
          -- Re-onboarding resets this row to a genuinely fresh QUEUED state —
          -- clear out any started_at/completed_at/failed_at/error_code/
          -- error_message left over from a PREVIOUS run's abandoned attempt
          -- (e.g. a stale IN_PROGRESS/ASSIGNED record reconciled by
          -- reconcile_stale_records()). Without this, a table that is now
          -- freshly QUEUED under a brand-new batch_id kept showing hours-old
          -- "STALE_EXECUTION" error info from an unrelated past run, even
          -- though nothing is currently wrong with it.
          started_at        = NULL,
          completed_at      = NULL,
          failed_at         = NULL,
          error_code        = NULL,
          error_message     = NULL,
          -- validation_status/row counts also MUST be cleared on re-onboard:
          -- mark_validated() sets validation_status='VALIDATED' and stamps
          -- source_row_count/target_row_count once. The VALIDATE loop
          -- (orchestrator_notebook.py) explicitly SKIPS any record whose
          -- validation_status is already 'VALIDATED' (it assumes that means
          -- "already checked, nothing changed"). But a force_reonboard re-run
          -- physically re-clones the table — the old VALIDATED verdict and
          -- row counts are stale and must not be trusted (or displayed) until
          -- VALIDATE genuinely re-checks the fresh clone.
          validation_status = NULL,
          source_row_count  = NULL,
          target_row_count  = NULL,
          onboarded_at      = TIMESTAMP '{rec.onboarded_at}',
          queued_at         = TIMESTAMP '{rec.queued_at}',
          updated_at        = TIMESTAMP '{rec.updated_at}'
        WHEN NOT MATCHED THEN INSERT (
          migration_id, run_id, clone_type,
          source_workspace, target_workspace,
          source_catalog, source_schema, source_table,
          target_catalog, target_schema, target_table,
          source_path, size_in_bytes, size_gb,
          workload_class, workload_weight,
          status, attempt_number, max_attempts,
          source_num_files, source_version,
          batch_id,
          discovered_at, onboarded_at, queued_at,
          created_at, updated_at
        ) VALUES (
          '{rec.migration_id}', '{rec.run_id}', '{rec.clone_type}',
          '{rec.source_workspace}', '{rec.target_workspace}',
          '{rec.source_catalog}', '{rec.source_schema}', '{rec.source_table}',
          '{rec.target_catalog}', '{rec.target_schema}', '{rec.target_table}',
          '{loc}', {rec.size_in_bytes}, {rec.size_gb:.6f},
          '{rec.workload_class}', {rec.workload_weight},
          '{rec.status}', 0, {rec.max_attempts},
          {rec.source_num_files or 0}, {rec.source_version or 0},
          '{batch_id_val}',
          TIMESTAMP '{rec.discovered_at}', TIMESTAMP '{rec.onboarded_at}', TIMESTAMP '{rec.queued_at}',
          TIMESTAMP '{rec.created_at}', TIMESTAMP '{rec.updated_at}'
        )
        """
        self._tgt.execute_ddl(q)

    def _mark_permanent_failure(self, sel: TableSelection, msg: str) -> None:
        now = _TS()
        msg_esc = msg.replace("'", "\\'")[:500]
        mid = str(uuid.uuid4())
        try:
            q = f"""
            MERGE INTO {self._ctrl} AS t
            USING (SELECT '{sel.source_catalog}' AS sc, '{sel.source_schema}' AS ss,
                          '{sel.source_table}' AS st) AS s
            ON t.source_catalog = s.sc AND t.source_schema = s.ss AND t.source_table = s.st
            WHEN MATCHED THEN UPDATE SET
              status = 'FAILED_PERMANENT', error_message = '{msg_esc}',
              updated_at = TIMESTAMP '{now}'
            WHEN NOT MATCHED THEN INSERT (
              migration_id, run_id, clone_type, source_workspace, target_workspace,
              source_catalog, source_schema, source_table,
              target_catalog, target_schema, target_table,
              status, error_message, created_at, updated_at
            ) VALUES (
              '{mid}', '{self._run_id}', '{self._cfg.clone_type}',
              '{self._cfg.source_workspace_url}', '{self._cfg.target_workspace_url}',
              '{sel.source_catalog}', '{sel.source_schema}', '{sel.source_table}',
              '{sel.target_catalog}', '{sel.target_schema}', '{sel.target_table}',
              'FAILED_PERMANENT', '{msg_esc}',
              TIMESTAMP '{now}', TIMESTAMP '{now}'
            )
            """
            self._tgt.execute_ddl(q)
        except Exception as e:
            log.error("Could not record permanent failure for %s: %s", sel.source_fqn, e)
