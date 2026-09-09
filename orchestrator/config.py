"""
config.py — Configuration loading and defaults.

Precedence (highest → lowest):
  1. Explicit job parameters (Databricks widgets)
  2. YAML configuration file
  3. CSV table mapping (extends/overrides selection only)
  4. Framework defaults (this file)

Secrets must NEVER appear in YAML or the control table.
Load them from environment variables or Databricks Secrets.
"""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


# ── Workload classification thresholds (bytes) ────────────────────────────────

DEFAULT_WORKLOAD_THRESHOLDS: Dict[str, int] = {
    "SMALL":  10 * (1024 ** 3),         # < 10 GB
    "MEDIUM": 100 * (1024 ** 3),        # 10–100 GB
    "LARGE":  1_000 * (1024 ** 3),      # 100 GB–1 TB
    # XLARGE: anything above LARGE
}

DEFAULT_WORKLOAD_WEIGHTS: Dict[str, int] = {
    "SMALL":  1,
    "MEDIUM": 3,
    "LARGE":  8,
    "XLARGE": 20,
}


# ── Cluster pool entry ────────────────────────────────────────────────────────

@dataclass
class ClusterConfig:
    cluster_id:      str
    capacity_units:  int   = 10     # logical workload units


# ── Top-level configuration ───────────────────────────────────────────────────

@dataclass
class OrchestratorConfig:
    # ── Workspace credentials (from env vars / Databricks Secrets) ──
    source_workspace_url:    str = ""
    source_client_id:        str = ""
    source_client_secret:    str = ""     # from env
    source_warehouse_id:     str = ""     # required for DIRECT_ADLS inventory

    target_workspace_url:    str = ""
    target_client_id:        str = ""
    target_client_secret:    str = ""     # from env
    target_warehouse_id:     str = ""     # for control-table reads/writes

    # ── Control table location (on target workspace) ──
    meta_catalog:  str = "azure_uc_demo_region1"
    meta_schema:   str = "migration_meta"

    # ── Clone settings ──
    clone_type:            str = "direct_adls"   # delta_share | direct_adls
    default_target_catalog: str = ""             # override target catalog

    # ── Cluster pool ──
    cluster_pool: List[ClusterConfig] = field(default_factory=list)

    # ── Workload classification ──
    workload_thresholds: Dict[str, int]  = field(default_factory=lambda: dict(DEFAULT_WORKLOAD_THRESHOLDS))
    workload_weights:    Dict[str, int]  = field(default_factory=lambda: dict(DEFAULT_WORKLOAD_WEIGHTS))

    # ── Retry ──
    max_retries:         int = 3
    retry_backoff_base_s: int = 30      # exponential backoff base (seconds)

    # ── Validation ──
    validation_enabled:        bool = True
    row_count_validation:      bool = False    # expensive — opt-in
    size_tolerance_pct:        float = 1.0     # acceptable size diff %
    file_count_tolerance_pct:  float = 5.0

    # ── Batch / Chunk model ──
    # batch_id: user-defined isolation key. All INVENTORY, DEEP_CLONE and VALIDATE
    #   jobs for one migration owner use the same batch_id. Multiple parties can
    #   run concurrent batches without interference.
    batch_id:                  str   = ""       # auto-generated if empty

    # max_concurrent_chunks: upper bound on simultaneously active cluster jobs
    #   PER BATCH. A value of 3 means at most 3 ephemeral clusters are live at
    #   the same time for this batch.  Other batches are unaffected.
    max_concurrent_chunks:     int   = 3

    # parallel_threads_per_chunk: threads inside each chunk cluster job that run
    #   DEEP CLONE statements concurrently. 4 means 4 tables copy in parallel on
    #   the same 8-executor cluster.
    parallel_threads_per_chunk: int  = 4

    # chunk_capacity_gb: greedy bin-packing target size per chunk. Tables are
    #   sorted by size DESC and packed until this limit is reached. A single
    #   table that exceeds this threshold gets its own dedicated chunk/cluster.
    chunk_capacity_gb:         float = 50.0

    # min_executors_per_chunk: minimum number of Spark worker nodes per chunk
    #   cluster.  Overrides num_workers in worker_cluster_config if lower.
    min_executors_per_chunk:   int   = 8

    # ── Execution ──
    stale_threshold_minutes:   int  = 120      # reconcile stuck IN_PROGRESS records
    poll_interval_s:           int  = 30
    worker_timeout_minutes:    int  = 180
    api_throttle_delay_s:      float = 0.2

    # ── Exclusion ──
    exclude_catalogs:  List[str] = field(default_factory=list)                   # catalog names (exact, lower-case)
    exclude_schemas:   List[str] = field(default_factory=lambda: ["information_schema", "__databricks_internal"])
    exclude_patterns:  List[str] = field(default_factory=list)   # glob/regex on table name
    include_only_delta: bool     = True        # skip views / non-Delta sources

    # ── Worker notebook path (on target workspace) ──
    # Last-resort fallback only. JOB-mode runs should always pass the real
    # deployed path via the "worker_notebook_path" widget/job-parameter
    # (wired from the ${var.worker_notebook} bundle variable), since this
    # hardcoded path will not match the actual deployed workspace location
    # for other users/targets.
    worker_notebook_path: str = "/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion/worker_notebook"

    # ── Worker cluster config (used when cluster_pool is empty or cluster unavailable) ──
    # If set, each worker dispatch creates an ephemeral job cluster.
    # Dict matching Databricks NewCluster spec: spark_version, node_type_id, num_workers, etc.
    worker_cluster_config: Dict[str, Any] = field(default_factory=dict)

    # ── Run tracking ──
    run_id: str = ""    # Set at runtime


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_env(config: OrchestratorConfig) -> None:
    """Populate secrets from environment variables (never from files)."""
    config.source_workspace_url   = os.environ.get("AZ2AZ_SRC_URL", config.source_workspace_url)
    config.source_client_id       = os.environ.get("AZ2AZ_SRC_CID", config.source_client_id)
    config.source_client_secret   = os.environ.get("AZ2AZ_SRC_SECRET", config.source_client_secret)
    config.source_warehouse_id    = os.environ.get("AZ2AZ_SRC_WH_ID", config.source_warehouse_id)

    config.target_workspace_url   = os.environ.get("AZ2AZ_TGT_URL", config.target_workspace_url)
    config.target_client_id       = os.environ.get("AZ2AZ_TGT_CID", config.target_client_id)
    config.target_client_secret   = os.environ.get("AZ2AZ_TGT_SECRET", config.target_client_secret)
    config.target_warehouse_id    = os.environ.get("AZ2AZ_TGT_WH_ID", config.target_warehouse_id)


def load_from_yaml(path: str) -> OrchestratorConfig:
    """Load configuration from a YAML file, then overlay env-var secrets."""
    import yaml  # lazy import — only needed when YAML input_type is used
    with open(path) as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    cfg = OrchestratorConfig()

    mig = raw.get("migration", {})
    cfg.clone_type           = mig.get("clone_type", cfg.clone_type)
    cfg.source_warehouse_id  = mig.get("source_warehouse_id", cfg.source_warehouse_id)

    src = raw.get("source", {})
    cfg.source_workspace_url = src.get("workspace_url", cfg.source_workspace_url)
    cfg.source_client_id     = src.get("client_id", cfg.source_client_id)

    tgt = raw.get("target", {})
    cfg.target_workspace_url = tgt.get("workspace_url", cfg.target_workspace_url)
    cfg.target_client_id     = tgt.get("client_id", cfg.target_client_id)
    cfg.target_warehouse_id  = tgt.get("warehouse_id", cfg.target_warehouse_id)
    cfg.default_target_catalog = tgt.get("catalog", cfg.default_target_catalog)

    meta = raw.get("control_tables", {})
    cfg.meta_catalog = meta.get("catalog", cfg.meta_catalog)
    cfg.meta_schema  = meta.get("schema",  cfg.meta_schema)

    exec_ = raw.get("execution", {})
    cfg.max_retries             = exec_.get("max_retries",             cfg.max_retries)
    cfg.validation_enabled      = exec_.get("validation_enabled",      cfg.validation_enabled)
    cfg.row_count_validation    = exec_.get("row_count_validation",    cfg.row_count_validation)
    cfg.stale_threshold_minutes = exec_.get("stale_threshold_minutes", cfg.stale_threshold_minutes)
    cfg.worker_timeout_minutes  = exec_.get("worker_timeout_minutes",  cfg.worker_timeout_minutes)
    cfg.worker_notebook_path    = exec_.get("worker_notebook_path",    cfg.worker_notebook_path)
    cfg.exclude_schemas         = exec_.get("exclude_schemas",         cfg.exclude_schemas)
    cfg.exclude_patterns        = exec_.get("exclude_patterns",        cfg.exclude_patterns)
    cfg.include_only_delta      = exec_.get("include_only_delta",      cfg.include_only_delta)

    # ── Global exclusions from migration.exclude block (new-style YAML) ──────
    # Merge into exclude_schemas and exclude_patterns so _expand_catalog /
    # _expand_schema honour them even when called from JOB or CSV modes.
    g_excl = mig.get("exclude", {})
    if g_excl:
        # catalogs — stored separately on cfg for InputResolver to check
        cfg.exclude_catalogs = list({
            *getattr(cfg, "exclude_catalogs", []),
            *[c.lower() for c in g_excl.get("catalogs", [])],
        })
        # schemas — merge glob patterns into exclude_schemas
        extra_schs = g_excl.get("schemas", [])
        if extra_schs:
            cfg.exclude_schemas = list({*cfg.exclude_schemas, *extra_schs})
        # tables — merge glob patterns into exclude_patterns
        extra_tbls = g_excl.get("tables", [])
        if extra_tbls:
            cfg.exclude_patterns = list({*cfg.exclude_patterns, *extra_tbls})

    batch_ = raw.get("batch", {})
    cfg.batch_id                = batch_.get("batch_id",                cfg.batch_id)
    cfg.max_concurrent_chunks   = batch_.get("max_concurrent_chunks",   cfg.max_concurrent_chunks)
    cfg.parallel_threads_per_chunk = batch_.get("parallel_threads_per_chunk", cfg.parallel_threads_per_chunk)
    cfg.chunk_capacity_gb       = batch_.get("chunk_capacity_gb",       cfg.chunk_capacity_gb)
    cfg.min_executors_per_chunk = batch_.get("min_executors_per_chunk", cfg.min_executors_per_chunk)

    wl = raw.get("workload", {})
    if wl.get("thresholds"):
        cfg.workload_thresholds.update({k: v * (1024**3) for k, v in wl["thresholds"].items()})
    if wl.get("weights"):
        cfg.workload_weights.update(wl["weights"])

    pool = raw.get("cluster_pool", [])
    cfg.cluster_pool = [
        ClusterConfig(cluster_id=c["cluster_id"], capacity_units=c.get("capacity_units", 10))
        for c in pool
    ]

    wc = raw.get("worker_cluster", {})
    if wc:
        cfg.worker_cluster_config = wc

    _load_env(cfg)
    return cfg


def load_defaults() -> OrchestratorConfig:
    """Return a default config with secrets from env vars."""
    cfg = OrchestratorConfig()
    _load_env(cfg)
    return cfg


def validate_config(cfg: OrchestratorConfig, mode: str) -> List[str]:
    """Return list of validation errors (empty = OK).

    NOTE on source_* fields: for clone_type=delta_share, the SOURCE workspace
    is NEVER contacted directly — tables are read via the shared catalog that
    already lives on the TARGET workspace (see orchestrator_notebook.py's
    _disc_sql/_val_src_sql routing and the conditional src_sql.start_warehouse()
    guard). So source_workspace_url/source_client_id/source_client_secret/
    source_warehouse_id are ONLY required when clone_type=direct_adls, where
    the source SQL warehouse must be queried (DESCRIBE DETAIL) to resolve the
    underlying abfss:// path. Requiring them unconditionally used to force
    every delta_share deployment to configure unused source SP credentials.
    """
    errors = []
    if cfg.clone_type == "direct_adls":
        if not cfg.source_workspace_url:
            errors.append("source_workspace_url is required for direct_adls clone type")
        if not cfg.source_client_id or not cfg.source_client_secret:
            errors.append("Source SP credentials (CID + SECRET) are required for direct_adls clone type")
        if not cfg.source_warehouse_id:
            errors.append("source_warehouse_id is required for direct_adls clone type")
    if not cfg.target_workspace_url:
        errors.append("target_workspace_url is required")
    if not cfg.target_client_id or not cfg.target_client_secret:
        errors.append("Target SP credentials (CID + SECRET) are required")
    if not cfg.target_warehouse_id:
        errors.append("target_warehouse_id is required for control-table access")
    if mode == "DEEP_CLONE" and not cfg.cluster_pool and not cfg.worker_cluster_config:
        errors.append(
            "For DEEP_CLONE mode, provide either cluster_pool_config (existing cluster IDs) "
            "or worker_cluster_json (new ephemeral job cluster spec)"
        )
    return errors
