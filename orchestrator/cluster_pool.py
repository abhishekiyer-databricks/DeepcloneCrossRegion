"""
cluster_pool.py — In-memory capacity accounting for the logical cluster pool.

The cluster pool tracks:
• Total capacity units per cluster.
• Currently used units (in-flight work).
• Availability (whether the cluster is RUNNING).

Design note from the spec:
  "Use Python threads or asynchronous orchestration only for control-plane
   coordination. Do not build the framework around a single driver executing
   dozens of heavy clone operations through a ThreadPoolExecutor."

This module manages capacity accounting only. Actual clone execution happens
on isolated compute via the clone_worker module.

Missed scenario handled:
• Cluster unavailable: capacity is temporarily removed and work re-queued.
• Stale capacity: refreshed from the cluster state API before scheduling.
"""

from __future__ import annotations
import threading
import logging
from typing import Dict, List, Optional

from orchestrator.models import ClusterCapacity
from orchestrator.config import OrchestratorConfig, ClusterConfig
from orchestrator.api_client import ApiClient

log = logging.getLogger(__name__)


class ClusterPool:
    """
    Thread-safe logical pool of all-purpose clusters.

    Capacity is counted in workload units. Acquiring capacity atomically
    reserves units; releasing them frees them for the next scheduled table.
    """

    def __init__(self, config: OrchestratorConfig, target_api: ApiClient):
        self._api  = target_api
        self._lock = threading.Lock()
        self._pool: Dict[str, ClusterCapacity] = {}
        for cc in config.cluster_pool:
            self._pool[cc.cluster_id] = ClusterCapacity(
                cluster_id    = cc.cluster_id,
                total_units   = cc.capacity_units,
                used_units    = 0,
                available     = True,
            )
        log.info("Cluster pool initialised: %d cluster(s), total %d units",
                 len(self._pool), self.total_capacity())

    # ── Pool info ─────────────────────────────────────────────────────────────

    def total_capacity(self) -> int:
        return sum(c.total_units for c in self._pool.values())

    def available_capacity(self) -> int:
        return sum(c.free_units for c in self._pool.values() if c.available)

    def snapshot(self) -> List[Dict]:
        with self._lock:
            return [
                {
                    "cluster_id":   cid,
                    "total":        c.total_units,
                    "used":         c.used_units,
                    "free":         c.free_units,
                    "available":    c.available,
                    "in_flight":    len(c.in_flight_tables),
                }
                for cid, c in self._pool.items()
            ]

    # ── Cluster health refresh ────────────────────────────────────────────────

    def refresh_availability(self) -> None:
        """Query cluster states and update availability flags.
        Virtual 'auto' cluster (for ephemeral new_cluster dispatch) is always marked available.
        """
        with self._lock:
            for cid, cap in self._pool.items():
                if cid in ("auto", "") or cid.startswith("virtual-"):
                    # Ephemeral / virtual cluster — always available
                    cap.available = True
                    continue
                was = cap.available
                cap.available = self._api.is_cluster_available(cid)
                if was and not cap.available:
                    log.warning("Cluster %s became UNAVAILABLE", cid)
                elif not was and cap.available:
                    log.info("Cluster %s is AVAILABLE again", cid)

    # ── Capacity reservation ──────────────────────────────────────────────────

    def find_best_cluster(self, weight: int) -> Optional[str]:
        """
        Work-conserving scheduler: find the least-utilized cluster that
        has enough free capacity for the given workload weight.

        Returns cluster_id or None if no capacity available.
        """
        with self._lock:
            candidates = [
                c for c in self._pool.values()
                if c.can_fit(weight)
            ]
            if not candidates:
                return None
            # Prefer cluster with most free units (least relative utilization)
            best = max(candidates, key=lambda c: c.free_units)
            return best.cluster_id

    def acquire(self, cluster_id: str, migration_id: str, weight: int) -> bool:
        """
        Atomically reserve `weight` units on `cluster_id` for `migration_id`.
        Returns True if successful, False if insufficient capacity.
        """
        with self._lock:
            cap = self._pool.get(cluster_id)
            if cap is None or not cap.can_fit(weight):
                return False
            cap.used_units += weight
            cap.in_flight_tables.append(migration_id)
            log.debug("Acquired %d units on %s for %s (free=%d)",
                      weight, cluster_id, migration_id[:8], cap.free_units)
            return True

    def release(self, cluster_id: str, migration_id: str, weight: int) -> None:
        """Release capacity previously acquired by `migration_id`."""
        with self._lock:
            cap = self._pool.get(cluster_id)
            if cap is None:
                return
            cap.used_units = max(0, cap.used_units - weight)
            if migration_id in cap.in_flight_tables:
                cap.in_flight_tables.remove(migration_id)
            log.debug("Released %d units on %s for %s (free=%d)",
                      weight, cluster_id, migration_id[:8], cap.free_units)

    def evict_unavailable(self) -> List[str]:
        """
        Return migration_ids on unavailable clusters so the scheduler can
        requeue them. Clears their capacity reservations.
        """
        evicted = []
        with self._lock:
            for cid, cap in self._pool.items():
                if not cap.available and cap.in_flight_tables:
                    log.warning("Evicting %d tables from unavailable cluster %s",
                                len(cap.in_flight_tables), cid)
                    evicted.extend(cap.in_flight_tables)
                    cap.used_units = 0
                    cap.in_flight_tables.clear()
        return evicted
