"""
parameter_parser.py — Parse and validate Databricks job widget parameters.

Handles the highest-precedence input tier (JOB overrides YAML overrides CSV).

Widget / parameter names follow Section 15 of the design document.
"""

from __future__ import annotations
import json
import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


def _widget(name: str, default: str = "") -> str:
    """
    Read a Databricks widget value.
    Tries: dbutils (notebook context) → IPython user_ns → env var → default.
    """
    import os
    # Try Databricks dbutils via IPython namespace (works in notebooks)
    try:
        import IPython
        ip = IPython.get_ipython()
        if ip is not None:
            dbutils = ip.user_ns.get("dbutils")
            if dbutils is not None:
                val = dbutils.widgets.get(name)
                if val:
                    return val
    except Exception:
        pass
    # Fallback: environment variable (useful for local testing)
    return os.environ.get(name, default)


class JobParameters:
    """
    Reads and validates all expected Databricks job widget parameters.

    Widget schema (all optional unless noted):
      mode                 INVENTORY | DEEP_CLONE | VALIDATE | RETRY | DRY_RUN
      clone_type           delta_share | direct_adls
      input_type           JOB | YAML | CSV
      selection_type       catalog | schema | table
      source_workspace_url
      target_workspace_url
      source_warehouse_id
      input_path           path to YAML or CSV file (YAML/CSV input types)
      source_catalogs      JSON array: ["cat1","cat2"] or comma-sep
      source_schemas       JSON array: ["cat.schema1","cat.schema2"]
      source_tables        JSON array: ["cat.sch.tbl1","cat.sch.tbl2"]
      target_catalog       default target catalog override
      cluster_pool_config  JSON: [{"cluster_id":"...","capacity_units":10}]
      max_retries          integer
      validation_enabled   true | false
      row_count_validation true | false
      run_id               explicit run identifier (auto-generated if empty)
      yaml_config_path     alias for input_path when input_type=YAML
      csv_path             alias for input_path when input_type=CSV
    """

    def __init__(self):
        self.mode                 = _widget("mode",                 "INVENTORY")
        self.clone_type           = _widget("clone_type",           "direct_adls")
        self.input_type           = _widget("input_type",           "JOB")
        self.selection_type       = _widget("selection_type",       "schema")
        self.source_workspace_url = _widget("source_workspace_url", "")
        self.target_workspace_url = _widget("target_workspace_url", "")
        self.source_warehouse_id  = _widget("source_warehouse_id",  "")
        self.target_warehouse_id  = _widget("target_warehouse_id",  "")
        self.input_path           = _widget("input_path",           "")
        self.yaml_config_path     = _widget("yaml_config_path",     "")
        self.csv_path             = _widget("csv_path",             "")
        self.source_catalogs_raw  = _widget("source_catalogs",      "[]")
        self.source_schemas_raw   = _widget("source_schemas",       "[]")
        self.source_tables_raw    = _widget("source_tables",        "[]")
        self.target_catalog       = _widget("target_catalog",       "")
        self.cluster_pool_raw     = _widget("cluster_pool_config",  "[]")
        self.max_retries_raw      = _widget("max_retries",          "3")
        self.validation_enabled   = _widget("validation_enabled",   "true").lower() == "true"
        self.row_count_validation = _widget("row_count_validation", "false").lower() == "true"
        self.run_id               = _widget("run_id",               "")
        self.meta_catalog         = _widget("meta_catalog",         "")
        self.meta_schema          = _widget("meta_schema",          "")

    # ── Parsed properties ─────────────────────────────────────────────────────

    @property
    def source_catalogs(self) -> List[str]:
        return self._parse_list(self.source_catalogs_raw)

    @property
    def source_schemas(self) -> List[str]:
        return self._parse_list(self.source_schemas_raw)

    @property
    def source_tables(self) -> List[str]:
        return self._parse_list(self.source_tables_raw)

    @property
    def cluster_pool(self) -> List[Dict[str, Any]]:
        try:
            return json.loads(self.cluster_pool_raw) if self.cluster_pool_raw.strip() else []
        except json.JSONDecodeError:
            log.warning("Invalid cluster_pool_config JSON; using empty pool")
            return []

    @property
    def max_retries(self) -> int:
        try:
            return int(self.max_retries_raw)
        except ValueError:
            return 3

    @property
    def effective_input_path(self) -> str:
        return self.yaml_config_path or self.csv_path or self.input_path

    @staticmethod
    def _parse_list(raw: str) -> List[str]:
        """Accept JSON array or comma-separated string."""
        raw = raw.strip()
        if not raw or raw in ("[]", ""):
            return []
        try:
            result = json.loads(raw)
            if isinstance(result, list):
                return [str(x).strip() for x in result if x]
            return [str(result)]
        except json.JSONDecodeError:
            return [x.strip() for x in raw.split(",") if x.strip()]

    def validate(self) -> List[str]:
        """Return list of error strings (empty = OK)."""
        errors = []
        valid_modes = {"INVENTORY", "DEEP_CLONE", "VALIDATE", "RETRY", "DRY_RUN"}
        if self.mode not in valid_modes:
            errors.append(f"Invalid mode '{self.mode}'. Must be one of {valid_modes}")
        valid_clone_types = {"delta_share", "direct_adls"}
        if self.clone_type not in valid_clone_types:
            errors.append(f"Invalid clone_type '{self.clone_type}'")
        valid_input_types = {"JOB", "YAML", "CSV"}
        if self.input_type not in valid_input_types:
            errors.append(f"Invalid input_type '{self.input_type}'")
        if self.input_type in ("YAML", "CSV") and not self.effective_input_path:
            errors.append(f"input_path is required when input_type={self.input_type}")
        return errors

    def apply_to_config(self, cfg) -> None:
        """Overlay non-empty widget values onto an OrchestratorConfig."""
        if self.source_workspace_url:
            cfg.source_workspace_url = self.source_workspace_url
        if self.target_workspace_url:
            cfg.target_workspace_url = self.target_workspace_url
        if self.source_warehouse_id:
            cfg.source_warehouse_id = self.source_warehouse_id
        if self.target_warehouse_id:
            cfg.target_warehouse_id = self.target_warehouse_id
        if self.target_catalog:
            cfg.default_target_catalog = self.target_catalog
        if self.max_retries:
            cfg.max_retries = self.max_retries
        if self.cluster_pool:
            from orchestrator.config import ClusterConfig
            cfg.cluster_pool = [
                ClusterConfig(cluster_id=c["cluster_id"], capacity_units=c.get("capacity_units", 10))
                for c in self.cluster_pool
            ]
        cfg.validation_enabled   = self.validation_enabled
        cfg.row_count_validation = self.row_count_validation
        cfg.clone_type           = self.clone_type
        if self.meta_catalog:
            cfg.meta_catalog = self.meta_catalog
        if self.meta_schema:
            cfg.meta_schema = self.meta_schema
