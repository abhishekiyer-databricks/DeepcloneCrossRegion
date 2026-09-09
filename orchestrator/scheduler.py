"""
scheduler.py — Batch-aware, chunk-level parallel scheduler.

Architecture (redesigned):
──────────────────────────
                    INVENTORY
                       │
              BatchPlanner.plan()
              ┌──────────────────┐
              │  Bin-pack tables  │  ← greedy FFD by size_gb
              │  → chunk_id per  │
              │    migration_id  │
              └──────────────────┘
                       │
            ┌──────────┼──────────┐
         Chunk 1    Chunk 2    Chunk 3    ← up to max_concurrent_chunks live at once
         (≤50 GB)   (≥50 GB)   (≤50 GB)    per batch; other batches unaffected
         Cluster     Cluster    Cluster   ← each gets its own ephemeral cluster
         8 workers   8 workers  8 workers    (min_executors_per_chunk)
         │4 threads  │4 threads  │4 threads  ← parallel_threads_per_chunk
         Table A     Table F    Table G
         Table B                Table H
         Table C                Table I
         Table D
         Table E

Key properties:
  • A batch_id is the isolation key — each user/team has their own batch.
    DEEP_CLONE and VALIDATE jobs filter to their batch_id only.
  • max_concurrent_chunks limits cluster sprawl per batch (cost control).
  • When all chunk slots are full, pending chunks queue locally and start
    as slots free (sliding-window concurrency).
  • Multiple batches run fully independently and in parallel.
  • Each chunk cluster job calls chunk_worker_notebook, which runs tables
    in a ThreadPoolExecutor with parallel_threads_per_chunk workers.
"""

from __future__ import annotations
import logging
import time
import concurrent.futures
from typing import Dict, List, Optional, Callable, Tuple

from orchestrator.config       import OrchestratorConfig
from orchestrator.audit_manager import AuditManager
from orchestrator.models       import ChunkAssignment, ChunkStatus

log = logging.getLogger(__name__)


# ── Type aliases ───────────────────────────────────────────────────────────────
# dispatch_chunk_fn(chunk: ChunkAssignment) → (success_count, fail_count)
DispatchChunkFn = Callable[[ChunkAssignment], Tuple[int, int]]


class BatchChunkScheduler:
    """
    Concurrent chunk scheduler for DEEP_CLONE and RETRY modes.

    Parameters
    ----------
    config              : OrchestratorConfig with batch/chunk settings.
    audit               : AuditManager for state transitions.
    dispatch_chunk_fn   : Callable that submits one chunk to a cluster job.
                          Signature: (ChunkAssignment) → (success_count, fail_count)
                          It is expected to BLOCK until the cluster job finishes.
    """

    def __init__(
        self,
        config:            OrchestratorConfig,
        audit:             AuditManager,
        dispatch_chunk_fn: DispatchChunkFn,
    ):
        self._cfg      = config
        self._audit    = audit
        self._dispatch = dispatch_chunk_fn

    # ── Main entry point ───────────────────────────────────────────────────────

    def run(self, chunks: List[ChunkAssignment]) -> Dict[str, int]:
        """
        Execute all chunks for the batch using a sliding-window concurrency model.

        The window size is cfg.max_concurrent_chunks. When all N slots are active,
        newly submitted chunks queue in the ThreadPoolExecutor's internal queue.
        As each chunk completes, the next pending chunk starts automatically.

        Returns aggregated stats:
            { dispatched, completed_tables, failed_tables,
              completed_chunks, failed_chunks }
        """
        stats = {
            "dispatched":       0,
            "completed_tables": 0,
            "failed_tables":    0,
            "completed_chunks": 0,
            "failed_chunks":    0,
        }

        if not chunks:
            log.info("BatchChunkScheduler: no chunks to process")
            return stats

        max_workers = self._cfg.max_concurrent_chunks
        log.info(
            "BatchChunkScheduler: %d chunk(s) | max_concurrent=%d | batch=%s",
            len(chunks), max_workers, self._cfg.batch_id,
        )

        # Use ThreadPoolExecutor with max_concurrent_chunks workers.
        # Each Future represents one cluster job (one chunk).
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="chunk",
        ) as executor:
            future_to_chunk: Dict[concurrent.futures.Future, ChunkAssignment] = {}

            for chunk in chunks:
                log.info(
                    "Submitting chunk %d (%d tables, %.2f GB) to executor",
                    chunk.chunk_id, chunk.size, chunk.total_gb,
                )
                f = executor.submit(self._run_chunk, chunk)
                future_to_chunk[f] = chunk
                stats["dispatched"] += chunk.size

            # Collect results as they complete
            for f in concurrent.futures.as_completed(future_to_chunk):
                chunk = future_to_chunk[f]
                try:
                    ok, fail = f.result()
                    stats["completed_tables"] += ok
                    stats["failed_tables"]    += fail
                    if fail == 0:
                        stats["completed_chunks"] += 1
                    else:
                        stats["failed_chunks"] += 1
                    log.info(
                        "Chunk %d finished: %d ok, %d failed",
                        chunk.chunk_id, ok, fail,
                    )
                except Exception as exc:
                    log.error("Chunk %d raised exception: %s", chunk.chunk_id, exc)
                    stats["failed_chunks"] += 1
                    stats["failed_tables"] += chunk.size

        log.info("BatchChunkScheduler done: %s", stats)
        return stats

    # ── Per-chunk runner (called inside a thread) ──────────────────────────────

    def _run_chunk(self, chunk: ChunkAssignment) -> Tuple[int, int]:
        """
        Executed in a thread for each chunk.
        Calls dispatch_chunk_fn and returns (success_count, fail_count).
        """
        log.info("[Thread] Starting chunk %d", chunk.chunk_id)
        t0 = time.time()
        try:
            ok, fail = self._dispatch(chunk)
            elapsed = int(time.time() - t0)
            log.info(
                "[Thread] Chunk %d done in %ds: ok=%d fail=%d",
                chunk.chunk_id, elapsed, ok, fail,
            )
            return ok, fail
        except Exception as e:
            elapsed = int(time.time() - t0)
            log.exception(
                "[Thread] Chunk %d CRASHED after %ds: %s",
                chunk.chunk_id, elapsed, e,
            )
            return 0, chunk.size


# ── Legacy single-table scheduler (retained for backward compatibility) ────────

class Scheduler:
    """
    DEPRECATED: Single-table-per-cluster scheduler from v1.
    Kept for backward compatibility only. Use BatchChunkScheduler for new deployments.
    """

    def __init__(self, config, pool, audit):
        self._cfg   = config
        self._pool  = pool
        self._audit = audit
        log.warning(
            "Scheduler (v1) is deprecated. Use BatchChunkScheduler for "
            "proper batch/chunk/concurrency support."
        )

    def schedule_batch(self, dispatch_fn, batch_size: int = 100) -> Dict[str, int]:
        """Legacy scheduling pass — single-table granularity."""
        stats = {"dispatched": 0, "completed": 0, "failed": 0, "no_capacity": 0}

        self._pool.refresh_availability()
        evicted = self._pool.evict_unavailable()
        if evicted:
            log.warning("Evicting %d tables from unavailable clusters", len(evicted))

        batch_id = self._cfg.batch_id or ""
        records  = self._audit.get_queued_records(limit=batch_size, batch_id=batch_id)
        log.info("Legacy Scheduler found %d QUEUED records (batch=%s)", len(records), batch_id or "ALL")

        for rec in records:
            mid    = rec["migration_id"]
            weight = int(rec.get("workload_weight") or 1)

            cluster_id = self._pool.find_best_cluster(weight)
            if cluster_id is None:
                stats["no_capacity"] += 1
                continue

            acquired = self._pool.acquire(cluster_id, mid, weight)
            if not acquired:
                stats["no_capacity"] += 1
                continue

            self._audit.mark_assigned(mid, cluster_id)
            stats["dispatched"] += 1

            try:
                success, duration_s, err = dispatch_fn(rec, cluster_id)
                if success:
                    stats["completed"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                log.error("dispatch_fn raised for %s: %s", mid[:8], e)
                stats["failed"] += 1
            finally:
                self._pool.release(cluster_id, mid, weight)

        return stats
