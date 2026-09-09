"""
input_resolver.py — Normalise JOB / YAML / CSV inputs into TableSelection objects.

Resolves the requested catalog/schema/table scope and expands catalog-level
and schema-level selections using the UC Tables API.

Design rule: This module does NOT execute DESCRIBE DETAIL and does NOT write
to any control table. It only produces a list of TableSelection objects.
"""

from __future__ import annotations
import csv
import io
import logging
from typing import List, Optional

from orchestrator.models import TableSelection, SelectionType, InputType
from orchestrator.api_client import ApiClient
from orchestrator.config import OrchestratorConfig
from orchestrator.parameter_parser import JobParameters

log = logging.getLogger(__name__)


class InputResolver:
    """
    Produces a deduplicated list of TableSelection from any input tier.

    The API Client is used for Unity Catalog enumeration.
    The SQL Client (optional) provides a fallback via SHOW TABLES/SCHEMAS
    for legacy hive_metastore or when the UC API returns 0 results.
    """

    def __init__(self, config: OrchestratorConfig, source_api: ApiClient, source_sql=None, **kwargs):
        self._cfg = config
        self._api = source_api
        self._sql = source_sql   # optional SqlClient for SHOW TABLES fallback

    # ── Main entry ────────────────────────────────────────────────────────────

    def resolve(self, params: JobParameters) -> List[TableSelection]:
        """
        Resolve the full set of TableSelections based on input_type.

        Precedence:
          JOB  → use widget values directly
          YAML → load YAML then expand
          CSV  → load CSV explicit table mapping
        """
        input_type = params.input_type.upper()

        if input_type == InputType.CSV:
            return self._resolve_csv(params.effective_input_path)

        if input_type == InputType.YAML:
            return self._resolve_yaml(params.effective_input_path)

        # Default: JOB parameters
        return self._resolve_job(params)

    # ── JOB resolver ──────────────────────────────────────────────────────────

    def _resolve_job(self, params) -> List[TableSelection]:
        """params may be JobParameters or a duck-typed _Params object."""
        sel_type = (params.selection_type or "schema").lower()
        tgt_cat  = getattr(params, "target_catalog", "") or self._cfg.default_target_catalog

        # Accept either parsed lists or raw strings (from JobParameters)
        def _as_list(val):
            if isinstance(val, list):
                return val
            if isinstance(val, str):
                import json as _json
                try: return _json.loads(val)
                except Exception: return [x.strip() for x in val.split(",") if x.strip()]
            return []

        selections: List[TableSelection] = []

        if sel_type == SelectionType.TABLE:
            for fqn in _as_list(params.source_tables):
                parts = fqn.split(".")
                if len(parts) != 3:
                    log.warning("Skipping malformed table FQN: %s", fqn)
                    continue
                src_cat, src_sch, src_tbl = parts
                selections.append(TableSelection(
                    source_catalog=src_cat, source_schema=src_sch, source_table=src_tbl,
                    target_catalog=tgt_cat or src_cat,
                    target_schema=src_sch,
                    target_table=src_tbl,
                ))

        elif sel_type == SelectionType.SCHEMA:
            for schema_fqn in _as_list(params.source_schemas):
                parts = schema_fqn.split(".")
                if len(parts) != 2:
                    log.warning("Skipping malformed schema: %s", schema_fqn)
                    continue
                src_cat, src_sch = parts
                # Allow caller to override the target schema (e.g. self-clone test: src→tgt)
                _tgt_sch_override = getattr(params, "target_schema", "") or ""
                tgt_sch = _tgt_sch_override if _tgt_sch_override else src_sch
                selections.extend(self._expand_schema(src_cat, src_sch, tgt_cat or src_cat, tgt_sch))

        elif sel_type == SelectionType.CATALOG:
            for src_cat in _as_list(params.source_catalogs):
                selections.extend(self._expand_catalog(src_cat, tgt_cat or src_cat))

        else:
            log.warning("Unknown selection_type '%s', defaulting to schema", sel_type)

        return self._deduplicate(selections)

    # ── YAML resolver ─────────────────────────────────────────────────────────

    def _resolve_yaml(self, path: str) -> List[TableSelection]:
        """
        Load the canonical migration.yaml and expand all mappings.

        Supports three mapping types (mix freely in one file):
          type: catalog  — expands every schema + table in the catalog
          type: schema   — expands every table in the schema
          type: table    — explicit single-table entry (rename supported)

        Exclusion layers (all applied, highest priority first):
          1. global  migration.exclude.{catalogs|schemas|tables}
          2. per-mapping  exclude_schemas / exclude_tables
          3. execution.exclude_schemas  (config-level, also honoured here)

        Backward-compatible: still reads old-style `selection:` blocks from
        any YAML file that uses that legacy format, in addition to the
        current `mappings:` list format.
        """
        import yaml  # lazy — only when YAML input_type used
        import fnmatch
        import os
        log.info("Loading YAML selection from %s", path)
        with open(path) as f:
            raw = yaml.safe_load(f) or {}

        mig = raw.get("migration", {})

        # ── Optional CSV delegation ──────────────────────────────────────────
        # If migration.csv_path is set in this YAML file, table selection is
        # delegated entirely to the CSV resolver — the `mappings:` / legacy
        # `selection:` blocks below are skipped for this call. This lets a
        # migration.yaml act purely as a named, version-controlled "pointer"
        # to a CSV table list, so switching input sources never requires
        # editing resources/*.yml — only which YAML file is passed in.
        csv_path = mig.get("csv_path")
        if csv_path:
            if not os.path.isabs(csv_path):
                resolved_csv_path = os.path.join(os.path.dirname(path), csv_path)
            else:
                resolved_csv_path = csv_path
            log.info(
                "migration.csv_path is set (%s) — delegating table selection to CSV resolver "
                "(resolved path: %s); skipping mappings:/selection: blocks in %s",
                csv_path, resolved_csv_path, path,
            )
            return self._resolve_csv(resolved_csv_path)

        # ── Global exclusions ────────────────────────────────────────────────
        g_excl        = mig.get("exclude", {})
        g_excl_cats   = {c.lower() for c in g_excl.get("catalogs", [])}
        g_excl_schs   = [p.lower() for p in g_excl.get("schemas",  [])]
        g_excl_tbls   = [p.lower() for p in g_excl.get("tables",   [])]

        # Also honour config-level exclude_schemas
        cfg_excl_schs = [s.lower() for s in self._cfg.exclude_schemas]

        def _sch_excluded(sch: str, extra: list) -> bool:
            sch_l = sch.lower()
            all_pats = g_excl_schs + cfg_excl_schs + [p.lower() for p in extra]
            return any(fnmatch.fnmatch(sch_l, p) for p in all_pats)

        def _tbl_excluded(tbl: str, extra: list) -> bool:
            tbl_l = tbl.lower()
            all_pats = g_excl_tbls + [p.lower() for p in extra]
            return any(fnmatch.fnmatch(tbl_l, p) for p in all_pats)

        selections: List[TableSelection] = []

        # ── New-style: mappings list (mixed catalog / schema / table) ─────────
        for entry in mig.get("mappings", []):
            mtype       = (entry.get("type") or "schema").lower()
            ex_schs     = entry.get("exclude_schemas", [])
            ex_tbls     = entry.get("exclude_tables",  [])

            if mtype == "catalog":
                src_cat = entry["source"]
                tgt_cat = entry.get("target", src_cat)
                if src_cat.lower() in g_excl_cats:
                    log.info("Skipping catalog %s (globally excluded)", src_cat)
                    continue
                raw_sels = self._expand_catalog(src_cat, tgt_cat)
                for s in raw_sels:
                    if _sch_excluded(s.source_schema, ex_schs):
                        log.debug("Excluding %s.%s (schema pattern)", src_cat, s.source_schema)
                        continue
                    if _tbl_excluded(s.source_table, ex_tbls):
                        log.debug("Excluding %s (table pattern)", s.source_table)
                        continue
                    selections.append(s)

            elif mtype == "schema":
                src_cat = entry["source_catalog"]
                src_sch = entry["source_schema"]
                tgt_cat = entry.get("target_catalog", src_cat)
                tgt_sch = entry.get("target_schema",  src_sch)
                if src_cat.lower() in g_excl_cats:
                    log.info("Skipping catalog %s (globally excluded)", src_cat)
                    continue
                if _sch_excluded(src_sch, ex_schs):
                    log.info("Skipping schema %s.%s (excluded)", src_cat, src_sch)
                    continue
                raw_sels = self._expand_schema(src_cat, src_sch, tgt_cat, tgt_sch)
                for s in raw_sels:
                    if _tbl_excluded(s.source_table, ex_tbls):
                        log.debug("Excluding %s (table pattern)", s.source_table)
                        continue
                    selections.append(s)

            elif mtype == "table":
                src_cat = entry["source_catalog"]
                src_sch = entry["source_schema"]
                src_tbl = entry["source_table"]
                tgt_cat = entry.get("target_catalog", src_cat)
                tgt_sch = entry.get("target_schema",  src_sch)
                tgt_tbl = entry.get("target_table",   src_tbl)
                if src_cat.lower() in g_excl_cats:
                    continue
                if _sch_excluded(src_sch, ex_schs):
                    continue
                if _tbl_excluded(src_tbl, ex_tbls):
                    log.debug("Excluding %s (table pattern)", src_tbl)
                    continue
                selections.append(TableSelection(
                    source_catalog=src_cat, source_schema=src_sch, source_table=src_tbl,
                    target_catalog=tgt_cat, target_schema=tgt_sch, target_table=tgt_tbl,
                ))
            else:
                log.warning("Unknown mapping type '%s' — skipping", mtype)

        # ── Backward-compat: old-style `selection:` block ─────────────────────
        sel = mig.get("selection", {})
        if sel and not mig.get("mappings"):
            sel_type = sel.get("type", "schema")
            log.info("Using legacy selection format (type=%s)", sel_type)

            if sel_type == "catalog":
                for mapping in sel.get("catalogs", []):
                    src_cat = mapping["source"]
                    tgt_cat = mapping.get("target", src_cat)
                    if src_cat.lower() not in g_excl_cats:
                        raw_sels = self._expand_catalog(src_cat, tgt_cat)
                        selections.extend(
                            s for s in raw_sels
                            if not _sch_excluded(s.source_schema, [])
                            and not _tbl_excluded(s.source_table, [])
                        )

            elif sel_type == "schema":
                for mapping in sel.get("schemas", []):
                    src_cat = mapping["source_catalog"]
                    src_sch = mapping["source_schema"]
                    tgt_cat = mapping.get("target_catalog", src_cat)
                    tgt_sch = mapping.get("target_schema",  src_sch)
                    if not _sch_excluded(src_sch, []):
                        raw_sels = self._expand_schema(src_cat, src_sch, tgt_cat, tgt_sch)
                        selections.extend(
                            s for s in raw_sels if not _tbl_excluded(s.source_table, [])
                        )

            elif sel_type == "table":
                for mapping in sel.get("tables", []):
                    src_tbl = mapping["source_table"]
                    if not _tbl_excluded(src_tbl, []):
                        selections.append(TableSelection(
                            source_catalog=mapping["source_catalog"],
                            source_schema=mapping["source_schema"],
                            source_table=src_tbl,
                            target_catalog=mapping.get("target_catalog", mapping["source_catalog"]),
                            target_schema=mapping.get("target_schema",  mapping["source_schema"]),
                            target_table=mapping.get("target_table",    src_tbl),
                        ))

        log.info("YAML resolver produced %d table selections (before dedup)", len(selections))
        return self._deduplicate(selections)

    # ── CSV resolver ──────────────────────────────────────────────────────────

    def _resolve_csv(self, path: str) -> List[TableSelection]:
        """
        CSV format (Section 6.3):
        source_catalog,source_schema,source_table,target_catalog,target_schema,target_table
        """
        log.info("Loading CSV selection from %s", path)
        selections: List[TableSelection] = []
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    selections.append(TableSelection(
                        source_catalog=row["source_catalog"].strip(),
                        source_schema=row["source_schema"].strip(),
                        source_table=row["source_table"].strip(),
                        target_catalog=row.get("target_catalog", row["source_catalog"]).strip(),
                        target_schema=row.get("target_schema", row["source_schema"]).strip(),
                        target_table=row.get("target_table", row["source_table"]).strip(),
                    ))
                except KeyError as e:
                    log.warning("CSV row missing required field %s — skipping", e)
        return self._deduplicate(selections)

    # ── Expansion helpers ─────────────────────────────────────────────────────

    def _expand_catalog(self, src_cat: str, tgt_cat: str) -> List[TableSelection]:
        """Expand a catalog into all schemas → all tables."""
        log.info("Expanding catalog %s → %s", src_cat, tgt_cat)
        selections: List[TableSelection] = []
        schema_names: List[str] = []

        # Try UC Schemas API
        try:
            schemas = self._api.list_schemas(src_cat)
            if schemas:
                schema_names = [s["name"] for s in schemas]
        except Exception as e:
            log.warning("UC Schemas API failed for %s: %s — trying SHOW SCHEMAS fallback", src_cat, e)

        # Fallback: SHOW SCHEMAS via SQL warehouse
        if not schema_names and self._sql is not None:
            try:
                rows = self._sql.execute(f"SHOW SCHEMAS IN `{src_cat}`")
                schema_names = [r.get("databaseName") or r.get("namespace") or "" for r in rows]
            except Exception as e2:
                log.error("SHOW SCHEMAS also failed for %s: %s", src_cat, e2)

        for sch_name in schema_names:
            if sch_name.lower() in [x.lower() for x in self._cfg.exclude_schemas]:
                log.debug("Excluding schema %s.%s", src_cat, sch_name)
                continue
            selections.extend(self._expand_schema(src_cat, sch_name, tgt_cat, sch_name))
        return selections

    def _expand_schema(self, src_cat: str, src_sch: str, tgt_cat: str, tgt_sch: str) -> List[TableSelection]:
        """
        Expand a schema into all tables.
        Uses UC Tables API first; falls back to SHOW TABLES via SQL warehouse
        for hive_metastore or when UC API returns 0 results.
        """
        log.info("Expanding schema %s.%s → %s.%s", src_cat, src_sch, tgt_cat, tgt_sch)
        selections: List[TableSelection] = []

        table_names: List[str] = []

        # Try UC Tables API
        try:
            uc_tables = self._api.list_tables(src_cat, src_sch)
            if uc_tables:
                for tbl in uc_tables:
                    tbl_name = tbl.get("name") or tbl.get("full_name","").split(".")[-1]
                    tbl_type = tbl.get("table_type", "").upper()
                    if self._cfg.include_only_delta and tbl_type not in ("MANAGED", "EXTERNAL"):
                        log.debug("Skipping %s (type=%s)", tbl_name, tbl_type)
                        continue
                    if not self._matches_exclusion(tbl_name):
                        table_names.append(tbl_name)
            log.info("UC API returned %d tables for %s.%s", len(table_names), src_cat, src_sch)
        except Exception as e:
            log.warning("UC Tables API failed for %s.%s: %s — trying SHOW TABLES fallback", src_cat, src_sch, e)

        # Fallback: SHOW TABLES via SQL warehouse (hive_metastore / permission issues)
        if not table_names and self._sql is not None:
            log.info("Using SHOW TABLES fallback for %s.%s", src_cat, src_sch)
            try:
                rows = self._sql.execute(f"SHOW TABLES IN `{src_cat}`.`{src_sch}`")
                for r in rows:
                    tbl_name = r.get("tableName") or r.get("name") or ""
                    # isTemporary is returned as string 'false'/'true' by SHOW TABLES
                    is_tmp = str(r.get("isTemporary", "false")).lower() == "true"
                    if tbl_name and not is_tmp and not self._matches_exclusion(tbl_name):
                        table_names.append(tbl_name)
                log.info("SHOW TABLES returned %d tables for %s.%s", len(table_names), src_cat, src_sch)
            except Exception as e2:
                log.error("SHOW TABLES also failed for %s.%s: %s", src_cat, src_sch, e2)

        for tbl_name in table_names:
            selections.append(TableSelection(
                source_catalog=src_cat, source_schema=src_sch, source_table=tbl_name,
                target_catalog=tgt_cat, target_schema=tgt_sch, target_table=tbl_name,
            ))
        return selections

    def _matches_exclusion(self, table_name: str) -> bool:
        import re
        for pattern in self._cfg.exclude_patterns:
            if re.fullmatch(pattern, table_name, re.IGNORECASE):
                return True
        return False

    @staticmethod
    def _deduplicate(selections: List[TableSelection]) -> List[TableSelection]:
        seen = set()
        out  = []
        for s in selections:
            key = s.source_fqn
            if key not in seen:
                seen.add(key)
                out.append(s)
        return out
