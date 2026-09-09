"""
clone_worker.py — Worker dispatch for both single-table (legacy) and chunk modes.

Chunk dispatch (new default, Section 8 redesign):
  dispatch_chunk(chunk) → (ok_count, fail_count)
  • Creates one ephemeral cluster job pointing to chunk_worker_notebook.
  • chunk_worker_notebook runs all tables in the chunk with ThreadPoolExecutor.
  • Each cluster has min_executors_per_chunk ≥ 8 worker nodes.
  • Returns aggregate (ok, fail) counts after polling for completion.

Single-table dispatch (legacy, kept for backward compat):
  dispatch(record, cluster_id) → (success, duration_s, error)
"""

from __future__ import annotations
import logging
import time
from typing import Dict, Tuple

import json
from orchestrator.config import OrchestratorConfig
from orchestrator.api_client import ApiClient
from orchestrator.audit_manager import AuditManager
from orchestrator.models import ChunkAssignment

log = logging.getLogger(__name__)

_TS = lambda: __import__("datetime").datetime.now(
    tz=__import__("datetime").timezone.utc
).strftime("%Y-%m-%d %H:%M:%S")


class CloneWorker:
    """
    Dispatches one table clone to an existing cluster via Jobs Run Submit.
    Polls for completion and relays result to AuditManager.
    """

    def __init__(
        self,
        config:    OrchestratorConfig,
        tgt_api:   ApiClient,
        audit:     AuditManager,
    ):
        self._cfg   = config
        self._api   = tgt_api
        self._audit = audit

    # ── Chunk dispatch (called by BatchChunkScheduler) ────────────────────────

    def dispatch_chunk(self, chunk: ChunkAssignment) -> tuple:
        """
        Submit chunk_worker_notebook to an ephemeral cluster (min 8 executors).
        Blocks until the cluster job finishes, then returns (ok_count, fail_count).

        The chunk_worker_notebook receives batch_id + chunk_id and fetches
        its own table list from migration_control — no long widget strings.
        """
        cfg = self._cfg

        # Build the cluster spec for this chunk — enforce min_executors_per_chunk
        cluster_spec = dict(cfg.worker_cluster_config) if cfg.worker_cluster_config else {}
        min_exec = cfg.min_executors_per_chunk
        if cluster_spec.get("num_workers", 0) < min_exec:
            cluster_spec["num_workers"] = min_exec
            log.info("Chunk %d: enforcing min %d executors", chunk.chunk_id, min_exec)

        # Resolve the chunk_worker_notebook path (alongside orchestrator_notebook.py)
        base_nb = cfg.worker_notebook_path  # e.g. /Users/.../DeepcloneCrossRegion/worker_notebook
        chunk_nb_path = base_nb.rsplit("/", 1)[0] + "/chunk_worker_notebook"

        params = {
            "batch_id":        chunk.batch_id,
            "chunk_id":        str(chunk.chunk_id),
            "meta_catalog":    cfg.meta_catalog,
            "meta_schema":     cfg.meta_schema,
            "parallel_threads": str(cfg.parallel_threads_per_chunk),
        }

        run_name = (
            f"chunk-{chunk.batch_id[:12]}-{chunk.chunk_id:03d}"
            f"-({chunk.size}tbl,{chunk.total_gb:.0f}GB)"
        )

        log.info(
            "Dispatching chunk %d: %d tables, %.1f GB → %s",
            chunk.chunk_id, chunk.size, chunk.total_gb, chunk_nb_path,
        )

        try:
            run_id = self._api.submit_worker_run(
                cluster_id          = "",
                notebook_path       = chunk_nb_path,
                migration_id        = "",
                meta_catalog        = cfg.meta_catalog,
                meta_schema         = cfg.meta_schema,
                run_name            = run_name,
                timeout_minutes     = cfg.worker_timeout_minutes,
                new_cluster_config  = cluster_spec,
                extra_params        = params,
            )
        except Exception as e:
            err = str(e)
            log.error("Failed to submit chunk %d: %s", chunk.chunk_id, err)
            self._mark_chunk_submit_failed(chunk, err)
            return 0, chunk.size

        log.info("Chunk %d → run_id=%d submitted", chunk.chunk_id, run_id)

        # Poll until done
        t0 = time.time()
        try:
            final = self._api.poll_run_until_done(
                run_id     = run_id,
                interval_s = cfg.poll_interval_s,
                timeout_s  = cfg.worker_timeout_minutes * 60,
            )
        except TimeoutError:
            self._api.cancel_run(run_id)
            duration_s = int(time.time() - t0)
            log.error("Chunk %d timed out after %ds", chunk.chunk_id, duration_s)
            return 0, chunk.size

        result_state = final.get("result_state", "")
        duration_s   = int(time.time() - t0)

        # Parse ok/fail from notebook output if available
        output_msg = final.get("notebook_output", {}).get("result", "")
        ok, fail = self._parse_chunk_output(output_msg, chunk.size)

        log.info(
            "Chunk %d done: result=%s | %ds | ok=%d fail=%d | %s",
            chunk.chunk_id, result_state, duration_s, ok, fail,
            final.get("run_page_url", ""),
        )
        return ok, fail

    def _mark_chunk_submit_failed(self, chunk: ChunkAssignment, err: str) -> None:
        """
        Persist a chunk-level submit failure for every table in the chunk.

        `submit_worker_run()` failing means the ephemeral cluster job never
        started, so every migration_id in the chunk is almost certainly still
        QUEUED (dispatch_chunk() itself never transitions per-table state —
        that's normally done by chunk_worker_notebook once it's running).

        mark_failed() only transitions IN_PROGRESS → FAILED/RETRY_PENDING (its
        UPDATE has a `WHERE status = 'IN_PROGRESS'` guard), so we mirror the
        legacy dispatch() convention here: call mark_in_progress() first, then
        mark_failed(), then record the FAILED attempt — using the same
        attempt-number convention as the legacy path
        (`attempt_number + 1` from the current control-table row).
        """
        started_at = _TS()
        try:
            records = self._audit.get_queued_for_chunk(chunk.batch_id, chunk.chunk_id)
            attempt_by_mid = {
                r["migration_id"]: int(r.get("attempt_number") or 0) + 1
                for r in records
            }
        except Exception as fetch_err:
            log.error(
                "Chunk %d: could not fetch records for failure bookkeeping: %s",
                chunk.chunk_id, fetch_err,
            )
            attempt_by_mid = {}

        for mid in chunk.migration_ids:
            attempt_num = attempt_by_mid.get(mid, 1)
            try:
                self._audit.mark_in_progress(mid, attempt_num)
                self._audit.mark_failed(mid, "SUBMIT_FAILED", err)
                self._audit.record_attempt(
                    migration_id  = mid, attempt_num = attempt_num,
                    cluster_id    = "", status = "FAILED",
                    started_at    = started_at, completed_at = _TS(),
                    error_code    = "SUBMIT_FAILED", error_message = err,
                )
            except Exception as write_err:
                log.error(
                    "Chunk %d: failed to record SUBMIT_FAILED for %s: %s",
                    chunk.chunk_id, mid[:8], write_err,
                )

    def _parse_chunk_output(self, output: str, total: int) -> tuple:
        """
        Parse 'SUCCESS:batch=X,chunk=Y,ok=N,fail=M' from notebook exit message.
        Falls back to (total, 0) on SUCCESS, (0, total) on FAILED.
        """
        try:
            if "ok=" in output and "fail=" in output:
                kv = dict(p.split("=") for p in output.split(",") if "=" in p)
                return int(kv.get("ok", total)), int(kv.get("fail", 0))
        except Exception:
            pass
        if output.startswith("SUCCESS"):
            return total, 0
        return 0, total

    # ── Main dispatch (called by legacy Scheduler) ─────────────────────────────

    def dispatch(self, record: Dict, cluster_id: str) -> Tuple[bool, int, str]:
        """
        Submit worker notebook to cluster, poll, return (success, duration_s, error).
        This method drives one table's full lifecycle from ASSIGNED → COMPLETED/FAILED.
        """
        mid         = record["migration_id"]
        attempt_num = int(record.get("attempt_number") or 0) + 1
        started_at  = _TS()

        # Mark IN_PROGRESS
        self._audit.mark_in_progress(mid, attempt_num)

        # Submit notebook run
        # Prefer existing cluster if specified; fall back to new_cluster_config (ephemeral job cluster)
        # "auto" is the virtual cluster sentinel — always use new_cluster_config
        is_virtual = cluster_id in ("", "auto") or (cluster_id or "").startswith("virtual-")
        new_cluster_cfg = self._cfg.worker_cluster_config if (self._cfg.worker_cluster_config or is_virtual) else None
        use_cluster_id  = cluster_id if (not is_virtual and not new_cluster_cfg) else ""

        try:
            run_id = self._api.submit_worker_run(
                cluster_id         = use_cluster_id,
                notebook_path      = self._cfg.worker_notebook_path,
                migration_id       = mid,
                meta_catalog       = self._cfg.meta_catalog,
                meta_schema        = self._cfg.meta_schema,
                run_name           = f"clone-{mid[:8]}-attempt-{attempt_num}",
                timeout_minutes    = self._cfg.worker_timeout_minutes,
                new_cluster_config = new_cluster_cfg,
            )
        except Exception as e:
            err = str(e)
            log.error("Failed to submit worker for %s: %s", mid[:8], err)
            self._audit.mark_failed(mid, "SUBMIT_FAILED", err)
            self._audit.record_attempt(
                migration_id  = mid, attempt_num = attempt_num,
                cluster_id    = cluster_id, status = AttemptStatus.FAILED,
                started_at    = started_at, completed_at = _TS(),
                error_code    = "SUBMIT_FAILED", error_message = err,
            )
            return False, 0, err

        log.info("Worker run %d submitted for %s on %s", run_id, mid[:8], cluster_id[:12])

        # Poll until done
        t0 = time.time()
        try:
            final = self._api.poll_run_until_done(
                run_id     = run_id,
                interval_s = self._cfg.poll_interval_s,
                timeout_s  = self._cfg.worker_timeout_minutes * 60,
            )
        except TimeoutError:
            self._api.cancel_run(run_id)
            duration_s = int(time.time() - t0)
            err = f"Worker run {run_id} timed out after {self._cfg.worker_timeout_minutes}min"
            log.error(err)
            self._audit.mark_failed(mid, "TIMEOUT", err, duration_s)
            self._audit.record_attempt(
                migration_id  = mid, attempt_num = attempt_num,
                cluster_id    = cluster_id, status = "FAILED",
                started_at    = started_at, completed_at = _TS(),
                duration_s    = duration_s,
                error_code    = "TIMEOUT", error_message = err,
            )
            return False, duration_s, err

        duration_s  = int(time.time() - t0)
        result_state = final.get("result_state", "")
        lc_state     = final.get("life_cycle_state", "")
        page_url     = final.get("run_page_url", "")
        completed_at = _TS()

        if result_state == "SUCCESS":
            # The worker notebook itself handles COMPLETED state transition
            # and writes to migration_attempts. We just record the attempt here.
            log.info("Worker %d SUCCESS for %s in %ds | %s", run_id, mid[:8], duration_s, page_url)
            self._audit.record_attempt(
                migration_id  = mid, attempt_num = attempt_num,
                cluster_id    = cluster_id, status = "SUCCESS",
                started_at    = started_at, completed_at = completed_at,
                duration_s    = duration_s,
            )
            return True, duration_s, ""

        else:
            err = f"Run {run_id} ended {lc_state}/{result_state}: {final.get('state_message','')}"
            log.error("Worker FAILED for %s: %s | %s", mid[:8], err, page_url)
            # The worker notebook may have already set FAILED — if not, set it here
            self._audit.mark_failed(mid, result_state or "WORKER_FAILED", err, duration_s)
            self._audit.record_attempt(
                migration_id  = mid, attempt_num = attempt_num,
                cluster_id    = cluster_id, status = "FAILED",
                started_at    = started_at, completed_at = completed_at,
                duration_s    = duration_s,
                error_code    = result_state or "WORKER_FAILED",
                error_message = err,
            )
            return False, duration_s, err


# Import guard for AttemptStatus (avoid circular import)
try:
    from orchestrator.models import AttemptStatus  # noqa: F401
except ImportError:
    class AttemptStatus:  # type: ignore
        FAILED = "FAILED"
        SUCCESS = "SUCCESS"
