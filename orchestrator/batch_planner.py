"""
batch_planner.py — Greedy bin-packing to group tables into chunks.

Strategy (Section 8.2 — redesigned):
──────────────────────────────────────
1. Sort tables by size_gb DESC (largest first).
2. Greedily pack each table into the first open chunk that still fits
   within chunk_capacity_gb.
3. A table that exceeds chunk_capacity_gb on its own gets its own
   dedicated chunk (XL cluster).
4. Each resulting chunk maps to exactly ONE ephemeral cluster job.
5. The scheduler runs up to max_concurrent_chunks chunks in parallel
   per batch (other batches are completely independent).

Example with chunk_capacity_gb=50:
  Table A  45 GB → Chunk 1 (45/50)
  Table B  40 GB → Chunk 2 (40/50)          ← doesn't fit Chunk 1
  Table C   8 GB → Chunk 1 (53>50) → Chunk 2 (48/50) ✓
  Table D   3 GB → Chunk 1 (3/50)  ← reopened but full; into Chunk 3
  Tables E–K 0.5 GB each → packed into Chunk 1, 3 until full …

Result: large tables drive cluster sizing; small tables batch together.

IMPORTANT — the "many small tables" scale case:
──────────────────────────────────────────────
GB-only bin-packing silently defeats parallelism when tables are tiny
(e.g. 130 tables totalling <1 GB): every table satisfies "remaining >=
size_gb" for chunk #1 forever, so ALL of them collapse into ONE chunk
and only ONE cluster ever runs, no matter how high max_concurrent_chunks
is set. To guarantee real cluster-level parallelism regardless of data
volume, the planner also enforces `max_tables_per_chunk` — a table-COUNT
cap per chunk — so a chunk is closed (and a new one opened) once either
the GB cap OR the table-count cap is hit, whichever comes first. The
caller (orchestrator_notebook.py) derives this automatically as
ceil(total_tables / max_concurrent_chunks) so the number of chunks is
never fewer than max_concurrent_chunks (when there are enough tables to
go around), independent of how small the tables are.
"""

from __future__ import annotations
import logging
from typing import List, Dict, Optional

from orchestrator.models import ChunkAssignment, ChunkStatus

log = logging.getLogger(__name__)


class BatchPlanner:
    """
    Converts a flat list of QUEUED migration_control records into
    a list of ChunkAssignment objects (one per cluster job).

    Parameters
    ----------
    batch_id            : Isolation key for this migration owner.
    chunk_capacity_gb   : Soft upper-bound on total GB per chunk.
                          A single table larger than this limit gets its
                          own chunk rather than being split.
    max_tables_per_chunk: Optional hard cap on table COUNT per chunk. Set
                          this (e.g. ceil(total_tables / max_concurrent_chunks))
                          to guarantee a minimum number of chunks even when
                          the tables are too small in aggregate to ever hit
                          chunk_capacity_gb. None = no count cap (GB-only,
                          legacy behavior).
    """

    def __init__(
        self,
        batch_id: str,
        chunk_capacity_gb: float = 50.0,
        max_tables_per_chunk: Optional[int] = None,
    ):
        self.batch_id              = batch_id
        self.chunk_capacity_gb     = chunk_capacity_gb
        self.max_tables_per_chunk  = max_tables_per_chunk

    # ── Public API ─────────────────────────────────────────────────────────────

    def plan(self, records: List[Dict]) -> List[ChunkAssignment]:
        """
        Run bin-packing on *records* and return ordered ChunkAssignments.

        Each record must have:
          • migration_id  (str)
          • size_gb       (float, can be 0)
          • workload_class (str, for logging)
        """
        if not records:
            log.info("BatchPlanner: no records to plan")
            return []

        # Sort largest-first so big tables claim their own chunk early
        sorted_recs = sorted(
            records,
            key=lambda r: float(r.get("size_gb") or 0),
            reverse=True,
        )

        chunks: List[ChunkAssignment] = []

        for rec in sorted_recs:
            mid     = rec["migration_id"]
            size_gb = float(rec.get("size_gb") or 0)
            wclass  = rec.get("workload_class", "SMALL")

            placed = self._try_place(mid, size_gb, chunks)
            if not placed:
                # Open a new chunk for this table
                new_chunk = ChunkAssignment(
                    chunk_id      = len(chunks) + 1,
                    batch_id      = self.batch_id,
                    migration_ids = [mid],
                    total_gb      = size_gb,
                    status        = ChunkStatus.PENDING.value,
                )
                chunks.append(new_chunk)
                log.debug(
                    "Chunk %d opened for %s (size=%.2f GB, class=%s)",
                    new_chunk.chunk_id, mid[:8], size_gb, wclass,
                )

        self._log_summary(chunks)
        return chunks

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _try_place(
        self,
        migration_id: str,
        size_gb:      float,
        chunks:       List[ChunkAssignment],
    ) -> bool:
        """
        First-fit decreasing: scan existing chunks and place in the first
        that can still accommodate size_gb AND is still under the
        max_tables_per_chunk count cap (if set).
        Returns True if placed, False if a new chunk must be opened.
        """
        for chunk in chunks:
            if (
                self.max_tables_per_chunk is not None
                and len(chunk.migration_ids) >= self.max_tables_per_chunk
            ):
                continue  # chunk is full by table COUNT — try next chunk
            remaining = self.chunk_capacity_gb - chunk.total_gb
            if remaining >= size_gb or size_gb == 0:
                chunk.migration_ids.append(migration_id)
                chunk.total_gb += size_gb
                return True
        return False

    @staticmethod
    def _log_summary(chunks: List[ChunkAssignment]) -> None:
        total_tables = sum(c.size for c in chunks)
        total_gb     = sum(c.total_gb for c in chunks)
        log.info(
            "BatchPlanner: %d chunk(s) | %d tables | %.2f GB total",
            len(chunks), total_tables, total_gb,
        )
        for c in chunks:
            log.info(
                "  Chunk %02d — %d tables, %.2f GB",
                c.chunk_id, c.size, c.total_gb,
            )


# ── Convenience function ───────────────────────────────────────────────────────

def plan_chunks(
    records:          List[Dict],
    batch_id:         str,
    chunk_capacity_gb: float = 50.0,
    max_tables_per_chunk: Optional[int] = None,
) -> List[ChunkAssignment]:
    """
    Shorthand: build a BatchPlanner and return its plan.

    Usage::

        chunks = plan_chunks(queued_records, batch_id="vivek-q3", chunk_capacity_gb=50)
        for chunk in chunks:
            print(chunk.chunk_id, chunk.size, chunk.total_gb)
    """
    return BatchPlanner(batch_id, chunk_capacity_gb, max_tables_per_chunk).plan(records)
