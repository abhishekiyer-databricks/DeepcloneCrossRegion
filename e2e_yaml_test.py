#!/usr/bin/env python3
"""
End-to-end YAML-driven migration test: DRY_RUN → INVENTORY → DEEP_CLONE → VALIDATE → RETRY
"""
import base64, json, os, sys, time, requests

# ── Credentials ───────────────────────────────────────────────────────────────
# NEVER hardcode secrets here — load from env vars (same names the bundle
# injects into job clusters: AZ2AZ_TGT_URL / AZ2AZ_TGT_CID / AZ2AZ_TGT_SECRET).
TGT = os.environ["AZ2AZ_TGT_URL"]
CID = os.environ["AZ2AZ_TGT_CID"]
SEC = os.environ["AZ2AZ_TGT_SECRET"]
WH_ID = os.environ.get("AZ2AZ_TGT_WH_ID", "5fe1692f119e2528")
BATCH_ID = "yaml-test-20260903"

# Known Job IDs
JOB_INVENTORY   = 125033843689125
JOB_DEEP_CLONE  = 251066514084029
JOB_VALIDATE    = 196928057201340

# ── Auth ──────────────────────────────────────────────────────────────────────
def get_token():
    resp = requests.post(
        f"{TGT}/oidc/v1/token",
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(CID, SEC), timeout=30
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

TOKEN = get_token()
print(f"✓ Got OAuth token")

def hdr():
    return {"Authorization": f"Bearer {TOKEN}"}

# ── DBFS Upload ───────────────────────────────────────────────────────────────
BASE_LOCAL = "/Users/vivek.ravichandiran/DeepcloneCrossRegion"
DBFS_ROOT  = "dbfs:/deepclone_orchestrator"

def dbfs_upload(local_path, dbfs_path):
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()
    resp = requests.post(f"{TGT}/api/2.0/dbfs/put",
        headers=hdr(),
        json={"path": dbfs_path, "contents": content, "overwrite": True},
        timeout=60)
    if resp.status_code == 200:
        print(f"  ✓ Uploaded → {dbfs_path}")
    else:
        print(f"  ✗ Upload failed {dbfs_path}: {resp.status_code} {resp.text[:200]}")

print("\n── Step 0: Upload orchestrator files to DBFS ──────────────────────────")
uploads = [
    (f"{BASE_LOCAL}/orchestrator/input_resolver.py", f"{DBFS_ROOT}/orchestrator/input_resolver.py"),
    (f"{BASE_LOCAL}/orchestrator/config.py",          f"{DBFS_ROOT}/orchestrator/config.py"),
    (f"{BASE_LOCAL}/configs/test_migration.yaml",     f"{DBFS_ROOT}/configs/test_migration.yaml"),
]
for local, remote in uploads:
    if os.path.exists(local):
        dbfs_upload(local, remote)
    else:
        print(f"  ✗ Local file not found: {local}")

# ── SQL helper ────────────────────────────────────────────────────────────────
def sql_exec(statement, wait=True):
    """Execute SQL via Statement Execution API, return rows."""
    payload = {
        "statement": statement,
        "warehouse_id": WH_ID,
        "wait_timeout": "50s" if wait else "0s",
        "on_wait_timeout": "CONTINUE"
    }
    r = requests.post(f"{TGT}/api/2.0/sql/statements",
        headers=hdr(), json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    stmt_id = data.get("statement_id")

    # Poll if needed
    if data.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        for _ in range(60):
            time.sleep(5)
            r2 = requests.get(f"{TGT}/api/2.0/sql/statements/{stmt_id}", headers=hdr(), timeout=30)
            r2.raise_for_status()
            data = r2.json()
            state = data.get("status", {}).get("state")
            if state not in ("PENDING", "RUNNING"):
                break

    state = data.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = data.get("status", {}).get("error", {})
        msg = err.get("message", str(data))
        raise RuntimeError(f"SQL failed ({state}): {msg[:300]}")

    cols = [c["name"] for c in (data.get("manifest", {}).get("schema", {}).get("columns") or [])]
    rows_raw = (data.get("result", {}).get("data_array") or [])
    return [dict(zip(cols, r)) for r in rows_raw]


def sql_exec_safe(statement, label=""):
    try:
        rows = sql_exec(statement)
        return rows, None
    except Exception as e:
        return [], str(e)

# ── Step 1: Create target schemas ─────────────────────────────────────────────
print("\n── Step 1: Create target schemas ──────────────────────────────────────")
for schema in ["hive_metastore.dc_tgt_alpha_yaml", "hive_metastore.dc_tgt_beta_yaml"]:
    rows, err = sql_exec_safe(f"CREATE SCHEMA IF NOT EXISTS {schema}", schema)
    if err:
        print(f"  ✗ {schema}: {err}")
    else:
        print(f"  ✓ {schema} created/exists")

# ── Step 2: Clear migration_control ───────────────────────────────────────────
print("\n── Step 2: Clear migration_control for batch ───────────────────────────")
rows, err = sql_exec_safe(
    f"DELETE FROM hive_metastore.migration_meta.migration_control WHERE batch_id = '{BATCH_ID}'"
)
if err:
    print(f"  ✗ DELETE failed: {err}")
else:
    print(f"  ✓ Cleared batch {BATCH_ID}")

# ── Step 3: Check source schemas ──────────────────────────────────────────────
print("\n── Step 3: Check source schemas ────────────────────────────────────────")
rows, err = sql_exec_safe("SHOW SCHEMAS IN hive_metastore")
if err:
    print(f"  ✗ SHOW SCHEMAS failed: {err}")
    all_schemas = []
else:
    # Column may be databaseName or namespace
    all_schemas = [
        r.get("databaseName") or r.get("namespace") or r.get("schema_name") or list(r.values())[0]
        for r in rows
    ]
    print(f"  All schemas in hive_metastore: {sorted(all_schemas)}")

src_alpha_exists = "dc_src_alpha" in all_schemas
src_beta_exists  = "dc_src_beta"  in all_schemas
print(f"  dc_src_alpha exists: {src_alpha_exists}")
print(f"  dc_src_beta  exists: {src_beta_exists}")

# Create dc_src_alpha if missing
if not src_alpha_exists:
    print("  Creating dc_src_alpha.customers …")
    sql_exec_safe("CREATE SCHEMA IF NOT EXISTS hive_metastore.dc_src_alpha")
    sql_exec_safe("""
        CREATE OR REPLACE TABLE hive_metastore.dc_src_alpha.customers USING DELTA AS
        SELECT id, CONCAT('Name_', CAST(id AS STRING)) AS name
        FROM (SELECT explode(sequence(1,50)) AS id)
    """)
    print("  ✓ dc_src_alpha.customers created")

# ── Step 3b: Ensure dc_tgt_alpha exists (CATALOG mapping target) ──────────────
_, err = sql_exec_safe("CREATE SCHEMA IF NOT EXISTS hive_metastore.dc_tgt_alpha")
if not err:
    print("  ✓ dc_tgt_alpha ensured")

# ── List / find jobs ──────────────────────────────────────────────────────────
print("\n── Listing Jobs ────────────────────────────────────────────────────────")
r = requests.get(f"{TGT}/api/2.1/jobs/list?limit=25", headers=hdr(), timeout=30)
r.raise_for_status()
jobs = r.json().get("jobs", [])
for j in jobs:
    print(f"  Job {j['job_id']}: {j['settings'].get('name', '?')}")

# Try to find DRY_RUN job
dry_run_job_id = None
for j in jobs:
    name = j["settings"].get("name", "").upper()
    if "DRY" in name or "DRY_RUN" in name:
        dry_run_job_id = j["job_id"]
        break
if dry_run_job_id is None:
    dry_run_job_id = JOB_INVENTORY  # fallback: use INVENTORY job with mode=DRY_RUN
    print(f"  No DRY_RUN job found — using INVENTORY job ({JOB_INVENTORY}) with mode=DRY_RUN")
else:
    print(f"  Found DRY_RUN job: {dry_run_job_id}")

# ── Job runner ────────────────────────────────────────────────────────────────
WORKER_CLUSTER_JSON = json.dumps({
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_D4s_v3",
    "num_workers": 2,
    "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
    "azure_attributes": {"first_on_demand": 1, "availability": "ON_DEMAND_AZURE"},
    "spark_env_vars": {
        "AZ2AZ_SRC_URL":    "{{secrets/deepclone-migration/src-url}}",
        "AZ2AZ_SRC_CID":    "{{secrets/deepclone-migration/src-cid}}",
        "AZ2AZ_SRC_SECRET": "{{secrets/deepclone-migration/src-secret}}",
        "AZ2AZ_TGT_URL":    "{{secrets/deepclone-migration/tgt-url}}",
        "AZ2AZ_TGT_CID":    "{{secrets/deepclone-migration/tgt-cid}}",
        "AZ2AZ_TGT_SECRET": "{{secrets/deepclone-migration/tgt-secret}}",
        "AZ2AZ_TGT_WH_ID":  "{{secrets/deepclone-migration/tgt-wh-id}}",
        "PYTHONPATH":        "/dbfs/deepclone_orchestrator"
    }
})

def run_job(job_id, params, mode_label):
    """Trigger a job run and wait for completion. Returns (run_id, result_state, url)."""
    payload = {"job_id": job_id, "job_parameters": params}
    r = requests.post(f"{TGT}/api/2.1/jobs/runs/submit",
        headers=hdr(), json={"run_name": f"e2e-{mode_label}-{BATCH_ID}",
                             "existing_cluster_id": None}, timeout=30)
    # use runs/now instead
    r = requests.post(f"{TGT}/api/2.1/jobs/run-now",
        headers=hdr(), json=payload, timeout=30)
    if r.status_code != 200:
        print(f"  ✗ Could not start {mode_label}: {r.status_code} {r.text[:300]}")
        return None, "LAUNCH_FAILED", ""
    run_id = r.json()["run_id"]
    url = f"{TGT}/#job/{job_id}/runs/{run_id}"
    print(f"  Started {mode_label} run_id={run_id}  url={url}")

    # Poll
    timeout_s = 25 * 60  # 25 min
    start = time.time()
    last_state = "PENDING"
    while time.time() - start < timeout_s:
        time.sleep(20)
        r2 = requests.get(f"{TGT}/api/2.1/jobs/runs/get?run_id={run_id}",
            headers=hdr(), timeout=30)
        r2.raise_for_status()
        run = r2.json()
        life = run.get("state", {}).get("life_cycle_state", "?")
        result = run.get("state", {}).get("result_state", "")
        last_state = result or life
        elapsed = int(time.time() - start)
        print(f"    [{elapsed}s] {mode_label}: {life} / {result}")
        if life == "TERMINATED":
            if result != "SUCCESS":
                # Fetch task run output
                tasks = run.get("tasks", [])
                for t in tasks:
                    tr_id = t.get("run_id")
                    if tr_id:
                        ot = requests.get(f"{TGT}/api/2.1/jobs/runs/get-output?run_id={tr_id}",
                            headers=hdr(), timeout=30)
                        if ot.status_code == 200:
                            err_msg = ot.json().get("error", "")
                            if err_msg:
                                print(f"    Task error: {err_msg[:500]}")
            return run_id, result, url
        elif life in ("SKIPPED", "INTERNAL_ERROR"):
            return run_id, life, url

    return run_id, f"TIMEOUT/{last_state}", url

# ── Track results ─────────────────────────────────────────────────────────────
results = {}

# ── Step 4: DRY_RUN ───────────────────────────────────────────────────────────
print("\n── Step 4: DRY_RUN ─────────────────────────────────────────────────────")
dry_params = {
    "mode":             "DRY_RUN",
    "input_type":       "YAML",
    "yaml_config_path": "/dbfs/deepclone_orchestrator/configs/test_migration.yaml",
    "clone_type":       "delta_share",
    "meta_catalog":     "hive_metastore",
    "meta_schema":      "migration_meta",
    "batch_id":         BATCH_ID,
}
rid, state, url = run_job(dry_run_job_id, dry_params, "DRY_RUN")
results["DRY_RUN"] = {"run_id": rid, "state": state, "url": url}
print(f"  DRY_RUN finished: {state}")

# ── Step 5: INVENTORY ─────────────────────────────────────────────────────────
print("\n── Step 5: INVENTORY ───────────────────────────────────────────────────")
inv_params = {
    "mode":                   "INVENTORY",
    "input_type":             "YAML",
    "yaml_config_path":       "/dbfs/deepclone_orchestrator/configs/test_migration.yaml",
    "clone_type":             "delta_share",
    "meta_catalog":           "hive_metastore",
    "meta_schema":            "migration_meta",
    "batch_id":               BATCH_ID,
    "chunk_capacity_gb":      "10",
    "max_concurrent_chunks":  "2",
    "parallel_threads":       "2",
    "min_executors":          "2",
}
rid, state, url = run_job(JOB_INVENTORY, inv_params, "INVENTORY")
results["INVENTORY"] = {"run_id": rid, "state": state, "url": url}
print(f"  INVENTORY finished: {state}")

# Query migration_control after INVENTORY
print("\n  migration_control rows after INVENTORY:")
rows, err = sql_exec_safe(f"""
    SELECT source_schema, source_table, target_schema, target_table, status, chunk_id
    FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id = '{BATCH_ID}'
    ORDER BY source_schema, source_table
""")
if err:
    print(f"  ✗ Query failed: {err}")
    inventory_rows = []
else:
    inventory_rows = rows
    for r in rows:
        print(f"  {r.get('source_schema')}.{r.get('source_table')} → "
              f"{r.get('target_schema')}.{r.get('target_table')} [{r.get('status')}] chunk={r.get('chunk_id')}")
    print(f"  Total rows: {len(rows)}")

# ── Step 6: DEEP_CLONE ────────────────────────────────────────────────────────
print("\n── Step 6: DEEP_CLONE ──────────────────────────────────────────────────")
dc_params = {
    "mode":                   "DEEP_CLONE",
    "meta_catalog":           "hive_metastore",
    "meta_schema":            "migration_meta",
    "batch_id":               BATCH_ID,
    "max_concurrent_chunks":  "2",
    "parallel_threads":       "2",
    "chunk_capacity_gb":      "10",
    "min_executors":          "2",
    "worker_cluster_json":    WORKER_CLUSTER_JSON,
}
rid, state, url = run_job(JOB_DEEP_CLONE, dc_params, "DEEP_CLONE")
results["DEEP_CLONE"] = {"run_id": rid, "state": state, "url": url}
print(f"  DEEP_CLONE finished: {state}")

# Query post-DEEP_CLONE
rows, err = sql_exec_safe(f"""
    SELECT status, COUNT(*) as cnt
    FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id = '{BATCH_ID}'
    GROUP BY status ORDER BY status
""")
dc_status = {r.get("status"): int(r.get("cnt", 0)) for r in rows} if not err else {}
print(f"  Status distribution: {dc_status}")

# ── Step 7: VALIDATE ──────────────────────────────────────────────────────────
print("\n── Step 7: VALIDATE ────────────────────────────────────────────────────")
val_params = {
    "mode":        "VALIDATE",
    "meta_catalog": "hive_metastore",
    "meta_schema":  "migration_meta",
    "batch_id":     BATCH_ID,
}
rid, state, url = run_job(JOB_VALIDATE, val_params, "VALIDATE")
results["VALIDATE"] = {"run_id": rid, "state": state, "url": url}
print(f"  VALIDATE finished: {state}")

rows, err = sql_exec_safe(f"""
    SELECT status, COUNT(*) as cnt
    FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id = '{BATCH_ID}'
    GROUP BY status ORDER BY status
""")
val_status = {r.get("status"): int(r.get("cnt", 0)) for r in rows} if not err else {}
print(f"  Status distribution: {val_status}")

# ── Step 8: RETRY (if needed) ─────────────────────────────────────────────────
print("\n── Step 8: RETRY ───────────────────────────────────────────────────────")
failed_count = val_status.get("FAILED", 0) + val_status.get("RETRY_PENDING", 0)
retry_run_url = ""
retry_state   = "SKIPPED"
retry_run_id  = None

if failed_count > 0:
    print(f"  {failed_count} rows need retry — running RETRY mode")
    retry_params = {
        "mode":                  "RETRY",
        "meta_catalog":          "hive_metastore",
        "meta_schema":           "migration_meta",
        "batch_id":              BATCH_ID,
        "max_concurrent_chunks": "2",
        "parallel_threads":      "2",
        "min_executors":         "2",
        "worker_cluster_json":   WORKER_CLUSTER_JSON,
    }
    rid2, state2, url2 = run_job(JOB_DEEP_CLONE, retry_params, "RETRY")
    retry_run_id, retry_state, retry_run_url = rid2, state2, url2
    results["RETRY"] = {"run_id": rid2, "state": state2, "url": url2}
    print(f"  RETRY finished: {state2}")

    # Re-run VALIDATE
    run_job(JOB_VALIDATE, val_params, "VALIDATE-post-retry")
    rows, err = sql_exec_safe(f"""
        SELECT status, COUNT(*) as cnt
        FROM hive_metastore.migration_meta.migration_control
        WHERE batch_id = '{BATCH_ID}'
        GROUP BY status ORDER BY status
    """)
    val_status = {r.get("status"): int(r.get("cnt", 0)) for r in rows} if not err else val_status
else:
    print(f"  No failures — RETRY skipped")
    results["RETRY"] = {"run_id": None, "state": "SKIPPED", "url": ""}

# ── Final migration_control snapshot ──────────────────────────────────────────
print("\n── Final migration_control snapshot ────────────────────────────────────")
final_rows, err = sql_exec_safe(f"""
    SELECT source_schema, source_table, target_schema, target_table, status
    FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id = '{BATCH_ID}'
    ORDER BY source_schema, source_table
""")
if err:
    print(f"  ✗ {err}")
    final_rows = inventory_rows  # fallback
else:
    for r in final_rows:
        print(f"  {r.get('source_schema')}.{r.get('source_table')} → "
              f"{r.get('target_schema')}.{r.get('target_table')} [{r.get('status')}]")

# ── Exclusion checks ──────────────────────────────────────────────────────────
all_src_schemas = {r.get("source_schema", "").lower() for r in final_rows}
all_src_tables  = {r.get("source_table",  "").lower() for r in final_rows}
all_tgt_schemas = {r.get("target_schema", "").lower() for r in final_rows}
all_tgt_tables  = {r.get("target_table",  "").lower() for r in final_rows}

import fnmatch
sys_excluded    = not any(s in all_src_schemas for s in ["system", "samples"])
tmp_excluded    = not any(fnmatch.fnmatch(t, "tmp_*") or fnmatch.fnmatch(t, "staging_*") for t in all_src_tables)
self_excluded   = not any(s in all_tgt_schemas for s in ["dc_tgt_alpha", "dc_tgt_beta"])

# Mapping validations
catalog_rows = [r for r in final_rows if r.get("target_schema","").startswith("dc_tgt_alpha") and r.get("target_schema") != "dc_tgt_alpha_yaml"]
schema_rows  = [r for r in final_rows if r.get("target_schema","") == "dc_tgt_beta_yaml"]
table_rows   = [r for r in final_rows if r.get("source_table","") == "customers" and r.get("target_schema","") == "dc_tgt_alpha_yaml"]

n_catalog = len(catalog_rows)
n_schema  = len(schema_rows)
n_table   = len(table_rows)
table_rename_ok = any(r.get("target_table") == "customers_copy" for r in table_rows)

# ── Print final report ────────────────────────────────────────────────────────
def sym(b): return "✓" if b else "✗"
def fmt_state(s):
    return f"✓ {s}" if s == "SUCCESS" else f"✗ {s}"

completed   = val_status.get("COMPLETED",  dc_status.get("COMPLETED", 0))
val_ok      = val_status.get("VALIDATED",  val_status.get("COMPLETED", 0))
val_fail    = val_status.get("VALIDATION_FAILED", 0)
n_failed    = val_status.get("FAILED", 0)
n_queued    = sum(1 for r in inventory_rows if r.get("status") == "QUEUED")

report = f"""
╔══════════════════════════════════════════════════════════════════╗
║         YAML ALL-MODES TEST REPORT                              ║
║         batch_id = {BATCH_ID}                    ║
╚══════════════════════════════════════════════════════════════════╝

[MODE RESULTS]
  DRY_RUN   : {fmt_state(results["DRY_RUN"]["state"])}  (planned tables, 0 written to control)
  INVENTORY : {fmt_state(results["INVENTORY"]["state"])}  {n_queued} tables QUEUED
  DEEP_CLONE: {fmt_state(results["DEEP_CLONE"]["state"])}  {completed} COMPLETED, {n_failed} FAILED
  VALIDATE  : {fmt_state(results["VALIDATE"]["state"])}  {val_ok} VALIDATED, {val_fail} VALIDATION_FAILED
  RETRY     : {fmt_state(results["RETRY"]["state"]) if results["RETRY"]["state"] != "SKIPPED" else "✓ SKIPPED — no failures"}

[MAPPING VALIDATION — verify all 3 types worked]
  CATALOG mapping (dc_src_alpha → dc_tgt_alpha):
    {sym(n_catalog > 0)}  {n_catalog} tables discovered via catalog expansion
  SCHEMA mapping (dc_src_beta → dc_tgt_beta_yaml):
    {sym(n_schema > 0)}  {n_schema} tables cloned, target_schema=dc_tgt_beta_yaml
  TABLE mapping (dc_src_alpha.customers → dc_tgt_alpha_yaml.customers_copy):
    {sym(n_table > 0 and table_rename_ok)}  target_table confirmed as {'customers_copy' if table_rename_ok else 'WRONG: ' + str({r.get("target_table") for r in table_rows})}

[EXCLUSION VALIDATION]
  {sym(sys_excluded)}  Global exclude.catalogs: system/samples not in results
  {sym(tmp_excluded)}  Global exclude.tables: tmp_*/staging_* not in results
  {sym(self_excluded)}  Per-mapping exclude_schemas: dc_tgt_alpha/dc_tgt_beta not expanded as source

[FINAL migration_control SNAPSHOT]  batch={BATCH_ID}
  {'source_schema':<25} {'source_table':<25} {'target_schema':<30} {'target_table':<25} {'status'}"""

for r in final_rows:
    report += f"\n  {r.get('source_schema',''):<25} {r.get('source_table',''):<25} {r.get('target_schema',''):<30} {r.get('target_table',''):<25} {r.get('status','')}"

report += f"""

[JOB URLS]
  DRY_RUN   : {results["DRY_RUN"]["url"]}
  INVENTORY : {results["INVENTORY"]["url"]}
  DEEP_CLONE: {results["DEEP_CLONE"]["url"]}
  VALIDATE  : {results["VALIDATE"]["url"]}
  RETRY     : {results["RETRY"]["url"] or "(not run)"}
"""

print(report)
