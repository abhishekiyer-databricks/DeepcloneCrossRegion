# Databricks notebook source
# MAGIC %md
# MAGIC # YAML All-Modes Test
# MAGIC
# MAGIC Tests all 5 orchestrator modes using the new `mappings`-based YAML config.
# MAGIC
# MAGIC **Mapping types tested:**
# MAGIC - `type: catalog` — clone all schemas in a catalog with exclusions
# MAGIC - `type: schema`  — clone one schema with target rename
# MAGIC - `type: table`   — explicit single-table entry with rename
# MAGIC
# MAGIC **Modes run in sequence:** DRY_RUN → INVENTORY → DEEP_CLONE → VALIDATE → RETRY
# MAGIC
# MAGIC **Prerequisites:** Run `setup_control_tables` notebook first if not already done.

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

import sys, os, json, time, uuid, threading
sys.path.insert(0, "/dbfs/deepclone_orchestrator")

# ── Test parameters ───────────────────────────────────────────────────────────
BATCH_ID        = f"yaml-test-{time.strftime('%Y%m%d-%H%M')}"
META_CATALOG    = "hive_metastore"
META_SCHEMA     = "migration_meta"
CLONE_TYPE      = "delta_share"      # same-metastore self-clone
SRC_SCHEMA_A    = "dc_src_alpha"     # source schema A
SRC_SCHEMA_B    = "dc_src_beta"      # source schema B
TGT_SCHEMA_A    = "dc_tgt_alpha_yaml"   # target for catalog-mapping
TGT_SCHEMA_B    = "dc_tgt_beta_yaml"    # target for schema-mapping
TGT_SCHEMA_TBL  = "dc_tgt_alpha_tbm"   # target for table-mapping entry

print(f"batch_id = {BATCH_ID}")
print(f"meta    = {META_CATALOG}.{META_SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Create source + target schemas and test tables

# COMMAND ----------

# Source tables (reuse if they already exist)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{SRC_SCHEMA_A}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{SRC_SCHEMA_B}")

spark.sql(f"""
CREATE OR REPLACE TABLE hive_metastore.{SRC_SCHEMA_A}.customers USING DELTA AS
SELECT id,
       CONCAT('Customer_', CAST(id AS STRING)) AS name,
       CASE WHEN id % 3 = 0 THEN 'GOLD' WHEN id % 3 = 1 THEN 'SILVER' ELSE 'BRONZE' END AS tier
FROM (SELECT explode(sequence(1, 100)) AS id)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE hive_metastore.{SRC_SCHEMA_A}.orders_2024 USING DELTA AS
SELECT id,
       CONCAT('PROD_', CAST(id % 20 AS STRING)) AS product,
       ROUND(RAND() * 1000 + 5, 2) AS amount,
       DATE_ADD('2024-01-01', id % 365) AS order_date
FROM (SELECT explode(sequence(1, 200)) AS id)
""")

# Table with tmp_ prefix — should be EXCLUDED by global exclude
spark.sql(f"""
CREATE OR REPLACE TABLE hive_metastore.{SRC_SCHEMA_A}.tmp_scratch USING DELTA AS
SELECT 1 AS id, 'should_be_excluded' AS note
""")

spark.sql(f"""
CREATE OR REPLACE TABLE hive_metastore.{SRC_SCHEMA_B}.transactions USING DELTA AS
SELECT id,
       CONCAT('ACC_', CAST(id % 50 AS STRING)) AS account_id,
       ROUND(RAND() * 5000, 2) AS amount,
       CASE WHEN id % 2 = 0 THEN 'DEBIT' ELSE 'CREDIT' END AS txn_type
FROM (SELECT explode(sequence(1, 150)) AS id)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE hive_metastore.{SRC_SCHEMA_B}.risk_scores USING DELTA AS
SELECT id, ROUND(RAND(), 4) AS score, current_timestamp() AS computed_at
FROM (SELECT explode(sequence(1, 80)) AS id)
""")

# staging_ table — should be EXCLUDED by per-mapping exclude
spark.sql(f"""
CREATE OR REPLACE TABLE hive_metastore.{SRC_SCHEMA_B}.staging_load USING DELTA AS
SELECT 1 AS id, 'excluded_by_mapping' AS note
""")

# Target schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{TGT_SCHEMA_A}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{TGT_SCHEMA_B}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{TGT_SCHEMA_TBL}")

print("✓ Source tables created:")
for sch, tbls in [(SRC_SCHEMA_A, ["customers","orders_2024","tmp_scratch"]),
                  (SRC_SCHEMA_B, ["transactions","risk_scores","staging_load"])]:
    for t in tbls:
        cnt = spark.sql(f"SELECT COUNT(*) AS n FROM hive_metastore.{sch}.{t}").collect()[0]["n"]
        print(f"  hive_metastore.{sch}.{t}: {cnt} rows")

# COMMAND ----------

# MAGIC %md ## Step 2 — Write test_migration.yaml to DBFS

# COMMAND ----------

YAML_CONTENT = f"""
migration:
  clone_type: {CLONE_TYPE}

  mappings:
    # CATALOG mapping — clone all of dc_src_alpha (excluding tmp_*)
    - type: catalog
      source: hive_metastore
      target: hive_metastore
      exclude_schemas:
        - information_schema
        - __databricks_internal
        - migration_meta
        - {SRC_SCHEMA_B}          # beta covered by schema entry
        - {TGT_SCHEMA_A}          # skip already-cloned targets
        - {TGT_SCHEMA_B}
        - {TGT_SCHEMA_TBL}
      exclude_tables:
        - tmp_*
        - staging_*

    # SCHEMA mapping — explicit rename dc_src_beta → dc_tgt_beta_yaml
    - type: schema
      source_catalog: hive_metastore
      source_schema:  {SRC_SCHEMA_B}
      target_catalog: hive_metastore
      target_schema:  {TGT_SCHEMA_B}
      exclude_tables:
        - staging_*               # per-mapping exclude

    # TABLE mapping — customers with explicit rename
    - type: table
      source_catalog: hive_metastore
      source_schema:  {SRC_SCHEMA_A}
      source_table:   customers
      target_catalog: hive_metastore
      target_schema:  {TGT_SCHEMA_TBL}
      target_table:   customers_copy

  exclude:
    catalogs:
      - system
      - samples
    schemas:
      - information_schema
      - __databricks_internal
    tables:
      - tmp_*
      - staging_*

control_tables:
  catalog: {META_CATALOG}
  schema:  {META_SCHEMA}

execution:
  max_retries: 2
  validation_enabled: true
  row_count_validation: false
  include_only_delta: true

batch:
  batch_id: {BATCH_ID}
  max_concurrent_chunks: 2
  parallel_threads_per_chunk: 2
  chunk_capacity_gb: 10
  min_executors_per_chunk: 2
"""

YAML_DBFS_PATH = f"/dbfs/deepclone_orchestrator/configs/test_migration_{BATCH_ID}.yaml"
os.makedirs(os.path.dirname(YAML_DBFS_PATH), exist_ok=True)
with open(YAML_DBFS_PATH, "w") as f:
    f.write(YAML_CONTENT)
print(f"✓ YAML written to {YAML_DBFS_PATH}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Upload updated orchestrator package to DBFS

# COMMAND ----------

# Copy the updated orchestrator package from workspace files to DBFS
import shutil, pathlib

SRC_ROOT   = "/Workspace/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion"
DBFS_ROOT  = "/dbfs/deepclone_orchestrator"

orch_files = list(pathlib.Path(f"{SRC_ROOT}/orchestrator").glob("*.py"))
for src in orch_files:
    dst = pathlib.Path(f"{DBFS_ROOT}/orchestrator/{src.name}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(src), str(dst))
    print(f"  ✓ {src.name}")

print(f"\nUploaded {len(orch_files)} orchestrator module(s) to DBFS")

# COMMAND ----------

# MAGIC %md ## Step 4 — Run orchestrator modes via dbutils.notebook.run

# COMMAND ----------

ORCH_NOTEBOOK = "/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion/notebooks/orchestrator_notebook"

def common_params():
    return {
        "meta_catalog":    META_CATALOG,
        "meta_schema":     META_SCHEMA,
        "clone_type":      CLONE_TYPE,
        "batch_id":        BATCH_ID,
        "input_type":      "YAML",
        "yaml_config_path": YAML_DBFS_PATH.replace("/dbfs", "dbfs:"),
        "validation_enabled": "true",
        "row_count_validation": "false",
        # Plain (non-secret) SQL warehouse id — required by orchestrator_
        # notebook.py's SqlClient/ApiClient. No client_id/client_secret needed
        # (native auth via databricks.sdk.core.Config()). Override via env var
        # AZ2AZ_TGT_WH_ID if this test notebook's default workspace differs.
        "target_warehouse_id": os.environ.get("AZ2AZ_TGT_WH_ID", "5fe1692f119e2528"),
    }

# No spark_env_vars/secrets needed — chunk_worker_notebook.py runs DEEP CLONE
# directly via Spark on its own cluster and never calls SqlClient/ApiClient.
WORKER_CLUSTER_JSON = json.dumps({
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_D4s_v3",
    "num_workers": 2,
    # REQUIRED for Unity Catalog access — without this, some workspaces
    # default new_cluster to a non-UC mode and fail on any UC table access.
    "data_security_mode": "DATA_SECURITY_MODE_AUTO",
    "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
    "azure_attributes": {"first_on_demand": 1, "availability": "ON_DEMAND_AZURE"},
})

results = {}

# ── DRY_RUN ──────────────────────────────────────────────────────────────────
print("=" * 60)
print("MODE: DRY_RUN")
print("=" * 60)
p = {**common_params(), "mode": "DRY_RUN"}
try:
    out = dbutils.notebook.run(ORCH_NOTEBOOK, timeout_seconds=600, arguments=p)
    results["DRY_RUN"] = {"status": "✓ PASSED", "output": out[:300] if out else ""}
    print(f"  Result: {out[:300] if out else 'OK'}")
except Exception as e:
    results["DRY_RUN"] = {"status": "✗ FAILED", "error": str(e)[:200]}
    print(f"  ERROR: {e}")

# COMMAND ----------

# ── INVENTORY ─────────────────────────────────────────────────────────────────
print("=" * 60)
print("MODE: INVENTORY")
print("=" * 60)
p = {**common_params(), "mode": "INVENTORY",
     "chunk_capacity_gb": "10", "max_concurrent_chunks": "2",
     "parallel_threads": "2", "min_executors": "2"}
try:
    out = dbutils.notebook.run(ORCH_NOTEBOOK, timeout_seconds=900, arguments=p)
    results["INVENTORY"] = {"status": "✓ PASSED", "output": out[:300] if out else ""}
    print(f"  Result: {out[:300] if out else 'OK'}")
except Exception as e:
    results["INVENTORY"] = {"status": "✗ FAILED", "error": str(e)[:200]}
    print(f"  ERROR: {e}")

# COMMAND ----------

# Check what was inventoried
inv_rows = spark.sql(f"""
SELECT source_schema, source_table, target_schema, target_table, status, chunk_id, size_gb
FROM {META_CATALOG}.{META_SCHEMA}.migration_control
WHERE batch_id = '{BATCH_ID}'
ORDER BY source_schema, source_table
""").collect()

print(f"\nInventoried {len(inv_rows)} tables for batch_id={BATCH_ID}:")
print(f"{'Source':50} {'Target':50} {'Status':15} chunk")
print("-" * 130)
for r in inv_rows:
    src = f"{r['source_schema']}.{r['source_table']}"
    tgt = f"{r['target_schema']}.{r['target_table']}"
    print(f"{src:50} {tgt:50} {r['status']:15} {r['chunk_id']}")

# Exclusion validation
excluded_found = [r for r in inv_rows if "tmp_" in r["source_table"] or "staging_" in r["source_table"]]
renamed_tbl    = [r for r in inv_rows if r["source_table"] == "customers" and r["target_table"] == "customers_copy"]
schema_renamed  = [r for r in inv_rows if r["target_schema"] == TGT_SCHEMA_B]

print(f"\n--- Exclusion checks ---")
print(f"  tmp_*/staging_* excluded: {'✓ YES' if not excluded_found else f'✗ NO — found: {[r[\"source_table\"] for r in excluded_found]}'}")
print(f"  TABLE mapping rename (customers→customers_copy): {'✓ YES' if renamed_tbl else '✗ NO'}")
print(f"  SCHEMA mapping renamed to {TGT_SCHEMA_B}: {'✓ YES' if schema_renamed else '✗ NO'}")

# COMMAND ----------

# ── DEEP_CLONE ────────────────────────────────────────────────────────────────
print("=" * 60)
print("MODE: DEEP_CLONE")
print("=" * 60)
p = {**common_params(), "mode": "DEEP_CLONE",
     "chunk_capacity_gb": "10", "max_concurrent_chunks": "2",
     "parallel_threads": "2", "min_executors": "2",
     "worker_cluster_json": WORKER_CLUSTER_JSON}
try:
    out = dbutils.notebook.run(ORCH_NOTEBOOK, timeout_seconds=1800, arguments=p)
    results["DEEP_CLONE"] = {"status": "✓ PASSED", "output": out[:300] if out else ""}
    print(f"  Result: {out[:300] if out else 'OK'}")
except Exception as e:
    results["DEEP_CLONE"] = {"status": "✗ FAILED", "error": str(e)[:200]}
    print(f"  ERROR: {e}")

# COMMAND ----------

# ── VALIDATE ──────────────────────────────────────────────────────────────────
print("=" * 60)
print("MODE: VALIDATE")
print("=" * 60)
p = {**common_params(), "mode": "VALIDATE"}
try:
    out = dbutils.notebook.run(ORCH_NOTEBOOK, timeout_seconds=900, arguments=p)
    results["VALIDATE"] = {"status": "✓ PASSED", "output": out[:300] if out else ""}
    print(f"  Result: {out[:300] if out else 'OK'}")
except Exception as e:
    results["VALIDATE"] = {"status": "✗ FAILED", "error": str(e)[:200]}
    print(f"  ERROR: {e}")

# COMMAND ----------

# ── RETRY (run only if there are failures) ────────────────────────────────────
retry_rows = spark.sql(f"""
SELECT COUNT(*) AS n FROM {META_CATALOG}.{META_SCHEMA}.migration_control
WHERE batch_id = '{BATCH_ID}' AND status IN ('FAILED','RETRY_PENDING','VALIDATION_FAILED')
""").collect()[0]["n"]

print("=" * 60)
print(f"MODE: RETRY  ({retry_rows} retryable rows)")
print("=" * 60)
if retry_rows > 0:
    p = {**common_params(), "mode": "RETRY",
         "chunk_capacity_gb": "10", "max_concurrent_chunks": "2",
         "parallel_threads": "2", "min_executors": "2",
         "worker_cluster_json": WORKER_CLUSTER_JSON}
    try:
        out = dbutils.notebook.run(ORCH_NOTEBOOK, timeout_seconds=1800, arguments=p)
        results["RETRY"] = {"status": "✓ PASSED", "output": out[:300] if out else ""}
        print(f"  Result: {out[:300] if out else 'OK'}")
    except Exception as e:
        results["RETRY"] = {"status": "✗ FAILED", "error": str(e)[:200]}
        print(f"  ERROR: {e}")
else:
    results["RETRY"] = {"status": "✓ SKIPPED — no failures to retry"}
    print("  No failures — RETRY mode skipped.")

# COMMAND ----------

# MAGIC %md ## Final Report

# COMMAND ----------

# ── Migration control snapshot ────────────────────────────────────────────────
final_rows = spark.sql(f"""
SELECT source_schema, source_table, target_schema, target_table,
       status, validation_status, ROUND(duration_seconds, 1) AS dur_s,
       error_code
FROM {META_CATALOG}.{META_SCHEMA}.migration_control
WHERE batch_id = '{BATCH_ID}'
ORDER BY source_schema, source_table
""").collect()

status_counts = {}
for r in final_rows:
    status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

print("\n" + "═"*80)
print("  YAML ALL-MODES TEST — FINAL REPORT")
print(f"  batch_id = {BATCH_ID}")
print("═"*80)

print("\n[1] MODE RESULTS")
for mode, res in results.items():
    err = res.get("error", "")
    print(f"  {mode:12} {res['status']}" + (f"  | {err[:60]}" if err else ""))

print(f"\n[2] STATUS SUMMARY")
for s, n in sorted(status_counts.items()):
    print(f"  {s:22}: {n}")

print(f"\n[3] PER-TABLE RESULTS")
print(f"  {'Source':40} {'Target':40} {'Status':20} {'Dur(s)':7}")
print(f"  {'-'*115}")
for r in final_rows:
    src = f"{r['source_schema']}.{r['source_table']}"
    tgt = f"{r['target_schema']}.{r['target_table']}"
    dur = str(r["dur_s"]) if r["dur_s"] else "-"
    icon = "✓" if r["status"] in ("VALIDATED","COMPLETED") else "✗"
    print(f"  {icon} {src:40} {tgt:40} {r['status']:20} {dur:7}")
    if r["error_code"]:
        print(f"    └─ error: {r['error_code']}")

print(f"\n[4] MAPPING TYPE VALIDATION")
# CATALOG mapping check
cat_rows = [r for r in final_rows if r["source_schema"] == SRC_SCHEMA_A and r["target_schema"] == TGT_SCHEMA_A]
print(f"  CATALOG mapping ({SRC_SCHEMA_A} → {TGT_SCHEMA_A}): {len(cat_rows)} tables")
# SCHEMA mapping check
sch_rows = [r for r in final_rows if r["source_schema"] == SRC_SCHEMA_B and r["target_schema"] == TGT_SCHEMA_B]
print(f"  SCHEMA mapping  ({SRC_SCHEMA_B} → {TGT_SCHEMA_B}): {len(sch_rows)} tables")
# TABLE mapping check (rename)
tbl_row = [r for r in final_rows if r["source_table"] == "customers" and r["target_table"] == "customers_copy"]
print(f"  TABLE mapping   (customers → customers_copy in {TGT_SCHEMA_TBL}): {'✓ found' if tbl_row else '✗ missing'}")

print(f"\n[5] EXCLUSION VALIDATION")
excluded = [r for r in final_rows if "tmp_" in r["source_table"] or "staging_" in r["source_table"]]
print(f"  tmp_*/staging_* tables excluded: {'✓ YES — none present' if not excluded else f'✗ NO — {[r[\"source_table\"] for r in excluded]}'}")

print("\n" + "═"*80)

# COMMAND ----------

# MAGIC %md ## Target table verification

# COMMAND ----------

# Spot-check a few target tables
for src_sch, src_tbl, tgt_sch, tgt_tbl in [
    (SRC_SCHEMA_A, "orders_2024",   TGT_SCHEMA_A, "orders_2024"),
    (SRC_SCHEMA_B, "transactions",  TGT_SCHEMA_B, "transactions"),
    (SRC_SCHEMA_A, "customers",     TGT_SCHEMA_TBL, "customers_copy"),
]:
    try:
        src_cnt = spark.sql(f"SELECT COUNT(*) AS n FROM hive_metastore.{src_sch}.{src_tbl}").collect()[0]["n"]
        tgt_cnt = spark.sql(f"SELECT COUNT(*) AS n FROM hive_metastore.{tgt_sch}.{tgt_tbl}").collect()[0]["n"]
        match = "✓ MATCH" if src_cnt == tgt_cnt else f"✗ MISMATCH (src={src_cnt}, tgt={tgt_cnt})"
        print(f"  {src_sch}.{src_tbl} → {tgt_sch}.{tgt_tbl}: {src_cnt} rows — {match}")
    except Exception as e:
        print(f"  {tgt_sch}.{tgt_tbl}: ✗ ERROR — {e}")
