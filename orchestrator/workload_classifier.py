"""
workload_classifier.py — Size → workload class → weight mapping.

Thresholds and weights are configurable. Defaults:
  SMALL   < 10 GB   → weight 1
  MEDIUM  10–100 GB → weight 3
  LARGE   100–1 TB  → weight 8
  XLARGE  > 1 TB    → weight 20
"""

from __future__ import annotations
from typing import Dict, Tuple

from orchestrator.config import OrchestratorConfig
from orchestrator.models import WorkloadClass


class WorkloadClassifier:
    def __init__(self, config: OrchestratorConfig):
        self._thresholds = config.workload_thresholds
        self._weights    = config.workload_weights

    def classify(self, size_bytes: int) -> Tuple[str, int]:
        """Return (workload_class, weight) for a given size in bytes."""
        small  = self._thresholds.get("SMALL",  10   * 1024**3)
        medium = self._thresholds.get("MEDIUM", 100  * 1024**3)
        large  = self._thresholds.get("LARGE",  1000 * 1024**3)

        if size_bytes < small:
            cls = WorkloadClass.SMALL
        elif size_bytes < medium:
            cls = WorkloadClass.MEDIUM
        elif size_bytes < large:
            cls = WorkloadClass.LARGE
        else:
            cls = WorkloadClass.XLARGE

        weight = self._weights.get(cls.value, 1)
        return cls.value, weight

    def classify_gb(self, size_gb: float) -> Tuple[str, int]:
        return self.classify(int(size_gb * 1024**3))
