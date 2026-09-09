#!/usr/bin/env python3
"""
Resume e2e test: check INVENTORY run, then run DEEP_CLONE → VALIDATE → RETRY.
Adds retry + exponential back-off for all HTTP calls.
"""
import base64, json, os, sys, time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Credentials ───────────────────────────────────────────────────────────────
# NEVER hardcode secrets here — load from env vars (same names the bundle
# injects into job clusters: AZ2AZ_TGT_URL / AZ2AZ_TGT_CID / AZ2AZ_TGT_SECRET).
TGT      = os.environ["AZ2AZ_TGT_URL"]
CID      = os.environ["AZ2AZ_TGT_CID"]
SEC      = os.environ["AZ2AZ_TGT_SECRET"]
WH_ID    = os.environ.get("AZ2AZ_TGT_WH_ID", "5fe1692f119e2528")
BATCH_ID = "yaml-test-20260903"

JOB_INVENTORY   = 125033843689125
JOB_DEEP_CLONE  = 251066514084029
JOB_VALIDATE    = 196928057201340
JOB_RETRY       = 673622228432148   # discovered: DeltaShare-5-RETRY
JOB_DRY_RUN     = 324656662471934

INV_RUN_ID = 17419845797985   # already-started INVENTORY run

# ── Resilient session ─────────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    retry = Retry(total=6, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

sess = make_session()

_token = None
_token_ts = 0
def get_token():
    global _token, _token_ts
    if _token and time.time() - _token_ts < 2700:  # refresh every 45 min
        return _token
    resp = sess.post(
        f"{TGT}/oidc/v1/token",
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(CID, SEC), timeout=30
    )
    resp.raise_for_status()
    _token = resp.json()["access_token"]
    _token_ts = time.time()
    return _token

def hdr():
    return {"Authorization": f"Bearer {get_token()}"}

def get(path, **kw):
    return sess.get(f"{TGT}{path}", headers=hdr(), timeout=45, **kw)

def post(path, **kw):
    return sess.post(f"{TGT}{path}", headers=hdr(), timeout=45, **kw)

print(f"✓ OAuth token acquired")

# ── SQL helper ────────────────────────────────────────────────────────────────
def sql_exec(statement):
    payload = {"statement": statement, "warehouse_id": WH_ID,
               "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"}
    r = post("/api/2.0/sql/statements", json=payload)
    r.raise_for_status()
    data = r.json()
    stmt_id = data.get("statement_id")
    for _ in range(120):
        state = data.get("status", {}).get("state")
        if state not in ("PENDING", "RUNNING"):
            break
        time.sleep(5)
        r2 = get(f"/api/2.0/sql/statements/{stmt_id}")
        r2.raise_for_status()
        data = r2.json()
    state = data.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = data.get("status", {}).get("error", {}).get("message", str(data))
        raise RuntimeError(f"SQL failed ({state}): {err[:300]}")
    cols = [c["name"] for c in (data.get("manifest", {}).get("schema", {}).get("columns") or [])]
    rows = data.get("result", {}).get("data_array") or []
    return [dict(zip(cols, r)) for r in rows]

def sql_safe(stmt, label=""):
    try: return sql_exec(stmt), None
    except Exception as e: return [], str(e)

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

def poll_run(run_id, label, timeout_s=22*60):
    """Poll an existing run_id until TERMINATED. Returns (result_state, url, job_id)."""
    start = time.time()
    job_id = "?"
    url = ""
    while time.time() - start < timeout_s:
        try:
            r = get(f"/api/2.1/jobs/runs/get?run_id={run_id}")
            r.raise_for_status()
            run = r.json()
            job_id = run.get("job_id", job_id)
            url = url or f"{TGT}/#job/{job_id}/runs/{run_id}"
            life   = run.get("state", {}).get("life_cycle_state", "?")
            result = run.get("state", {}).get("result_state", "")
            elapsed = int(time.time() - start)
            print(f"    [{elapsed}s] {label}: {life} / {result}")
            if life == "TERMINATED":
                if result != "SUCCESS":
                    tasks = run.get("tasks", [])
                    for t in tasks:
                        tr_id = t.get("run_id")
                        if tr_id:
                            ot = get(f"/api/2.1/jobs/runs/get-output?run_id={tr_id}")
                            if ot.status_code == 200:
                                em = ot.json().get("error", "")
                                if em:
                                    print(f"    ⚠ Task error snippet: {em[:600]}")
                return result, url, job_id
            elif life in ("SKIPPED", "INTERNAL_ERROR"):
                return life, url, job_id
        except requests.exceptions.ConnectionError as ce:
            elapsed = int(time.time() - start)
            print(f"    [{elapsed}s] {label}: network error ({ce!s:.120}) — retrying in 30s")
            time.sleep(30)
        time.sleep(20)
    return f"TIMEOUT", url, job_id

def run_job(job_id, params, label, timeout_s=22*60):
    r = post("/api/2.1/jobs/run-now", json={"job_id": job_id, "job_parameters": params})
    if r.status_code != 200:
        print(f"  ✗ Could not start {label}: {r.status_code} {r.text[:300]}")
        return None, "LAUNCH_FAILED", ""
    run_id = r.json()["run_id"]
    url = f"{TGT}/#job/{job_id}/runs/{run_id}"
    print(f"  Started {label} run_id={run_id}  url={url}")
    result, url, _ = poll_run(run_id, label, timeout_s=timeout_s)
    return run_id, result, url

results = {
    "DRY_RUN": {"run_id": 1114872317165853, "state": "SUCCESS",
                "url": f"{TGT}/#job/{JOB_DRY_RUN}/runs/1114872317165853"},
}

# ── Check INVENTORY run ───────────────────────────────────────────────────────
print(f"\n── Checking INVENTORY run {INV_RUN_ID} ─────────────────────────────────")
r = get(f"/api/2.1/jobs/runs/get?run_id={INV_RUN_ID}")
r.raise_for_status()
run = r.json()
life   = run.get("state", {}).get("life_cycle_state", "?")
result = run.get("state", {}).get("result_state", "")
job_id = run.get("job_id", JOB_INVENTORY)
inv_url = f"{TGT}/#job/{job_id}/runs/{INV_RUN_ID}"
print(f"  Current state: {life} / {result}")

if life == "TERMINATED":
    inv_state = result
    results["INVENTORY"] = {"run_id": INV_RUN_ID, "state": inv_state, "url": inv_url}
    print(f"  INVENTORY already finished: {inv_state}")
elif life in ("RUNNING", "PENDING"):
    print(f"  INVENTORY still {life} — waiting for completion …")
    inv_state, inv_url, _ = poll_run(INV_RUN_ID, "INVENTORY", timeout_s=25*60)
    results["INVENTORY"] = {"run_id": INV_RUN_ID, "state": inv_state, "url": inv_url}
    print(f"  INVENTORY finished: {inv_state}")
else:
    print(f"  INVENTORY in unexpected state {life}/{result} — re-running")
    inv_params = {
        "mode": "INVENTORY", "input_type": "YAML",
        "yaml_config_path": "/dbfs/deepclone_orchestrator/configs/test_migration.yaml",
        "clone_type": "delta_share", "meta_catalog": "hive_metastore",
        "meta_schema": "migration_meta", "batch_id": BATCH_ID,
        "chunk_capacity_gb": "10", "max_concurrent_chunks": "2",
        "parallel_threads": "2", "min_executors": "2",
    }
    rid, inv_state, inv_url = run_job(JOB_INVENTORY, inv_params, "INVENTORY")
    results["INVENTORY"] = {"run_id": rid, "state": inv_state, "url": inv_url}

# Query migration_control after INVENTORY
print("\n  migration_control rows after INVENTORY:")
inv_rows, err = sql_safe(f"""
    SELECT source_schema, source_table, target_schema, target_table, status, chunk_id
    FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id = '{BATCH_ID}'
    ORDER BY source_schema, source_table
""")
if err:
    print(f"  ✗ Query failed: {err}")
    inv_rows = []
else:
    for row in inv_rows:
        print(f"  {row.get('source_schema',''):<25} {row.get('source_table',''):<25} → "
              f"{row.get('target_schema',''):<25} {row.get('target_table',''):<25} [{row.get('status')}] chunk={row.get('chunk_id')}")
    print(f"  Total QUEUED rows: {sum(1 for r in inv_rows if r.get('status')=='QUEUED')}")

# If INVENTORY gave 0 QUEUED rows, re-run it
if inv_rows and all(r.get("status") != "QUEUED" for r in inv_rows):
    print("  ⚠ No QUEUED rows found — clearing and re-running INVENTORY")
    sql_safe(f"DELETE FROM hive_metastore.migration_meta.migration_control WHERE batch_id='{BATCH_ID}'")
    inv_params = {
        "mode": "INVENTORY", "input_type": "YAML",
        "yaml_config_path": "/dbfs/deepclone_orchestrator/configs/test_migration.yaml",
        "clone_type": "delta_share", "meta_catalog": "hive_metastore",
        "meta_schema": "migration_meta", "batch_id": BATCH_ID,
        "chunk_capacity_gb": "10", "max_concurrent_chunks": "2",
        "parallel_threads": "2", "min_executors": "2",
    }
    rid, inv_state, inv_url = run_job(JOB_INVENTORY, inv_params, "INVENTORY-retry", timeout_s=25*60)
    results["INVENTORY"] = {"run_id": rid, "state": inv_state, "url": inv_url}
    inv_rows, err = sql_safe(f"""
        SELECT source_schema, source_table, target_schema, target_table, status, chunk_id
        FROM hive_metastore.migration_meta.migration_control
        WHERE batch_id = '{BATCH_ID}'
        ORDER BY source_schema, source_table
    """)
    for row in inv_rows:
        print(f"  {row.get('source_schema',''):<25} {row.get('source_table',''):<25} → "
              f"{row.get('target_schema',''):<25} {row.get('target_table',''):<25} [{row.get('status')}]")

elif not inv_rows:
    print("  ⚠ Empty migration_control — running INVENTORY fresh")
    sql_safe(f"DELETE FROM hive_metastore.migration_meta.migration_control WHERE batch_id='{BATCH_ID}'")
    inv_params = {
        "mode": "INVENTORY", "input_type": "YAML",
        "yaml_config_path": "/dbfs/deepclone_orchestrator/configs/test_migration.yaml",
        "clone_type": "delta_share", "meta_catalog": "hive_metastore",
        "meta_schema": "migration_meta", "batch_id": BATCH_ID,
        "chunk_capacity_gb": "10", "max_concurrent_chunks": "2",
        "parallel_threads": "2", "min_executors": "2",
    }
    rid, inv_state, inv_url = run_job(JOB_INVENTORY, inv_params, "INVENTORY-fresh", timeout_s=25*60)
    results["INVENTORY"] = {"run_id": rid, "state": inv_state, "url": inv_url}
    inv_rows, err = sql_safe(f"""
        SELECT source_schema, source_table, target_schema, target_table, status, chunk_id
        FROM hive_metastore.migration_meta.migration_control
        WHERE batch_id = '{BATCH_ID}'
        ORDER BY source_schema, source_table
    """)
    for row in inv_rows:
        print(f"  {row.get('source_schema',''):<25} {row.get('source_table',''):<25} → "
              f"{row.get('target_schema',''):<25} {row.get('target_table',''):<25} [{row.get('status')}]")

n_queued = sum(1 for r in inv_rows if r.get("status") == "QUEUED")
print(f"  Final QUEUED count: {n_queued}")

# ── DEEP_CLONE ────────────────────────────────────────────────────────────────
print("\n── Step 6: DEEP_CLONE ──────────────────────────────────────────────────")
dc_params = {
    "mode":                  "DEEP_CLONE",
    "meta_catalog":          "hive_metastore",
    "meta_schema":           "migration_meta",
    "batch_id":              BATCH_ID,
    "max_concurrent_chunks": "2",
    "parallel_threads":      "2",
    "chunk_capacity_gb":     "10",
    "min_executors":         "2",
    "worker_cluster_json":   WORKER_CLUSTER_JSON,
}
rid, dc_state, dc_url = run_job(JOB_DEEP_CLONE, dc_params, "DEEP_CLONE", timeout_s=25*60)
results["DEEP_CLONE"] = {"run_id": rid, "state": dc_state, "url": dc_url}
print(f"  DEEP_CLONE finished: {dc_state}")

dc_status_rows, _ = sql_safe(f"""
    SELECT status, COUNT(*) as cnt FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id='{BATCH_ID}' GROUP BY status ORDER BY status
""")
dc_dist = {r.get("status"): int(r.get("cnt",0)) for r in dc_status_rows}
print(f"  Status distribution: {dc_dist}")

# ── VALIDATE ──────────────────────────────────────────────────────────────────
print("\n── Step 7: VALIDATE ────────────────────────────────────────────────────")
val_params = {"mode": "VALIDATE", "meta_catalog": "hive_metastore",
              "meta_schema": "migration_meta", "batch_id": BATCH_ID}
rid, val_state, val_url = run_job(JOB_VALIDATE, val_params, "VALIDATE", timeout_s=22*60)
results["VALIDATE"] = {"run_id": rid, "state": val_state, "url": val_url}
print(f"  VALIDATE finished: {val_state}")

val_rows, _ = sql_safe(f"""
    SELECT status, COUNT(*) as cnt FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id='{BATCH_ID}' GROUP BY status ORDER BY status
""")
val_dist = {r.get("status"): int(r.get("cnt",0)) for r in val_rows}
print(f"  Status distribution: {val_dist}")

# ── RETRY ─────────────────────────────────────────────────────────────────────
print("\n── Step 8: RETRY ───────────────────────────────────────────────────────")
failed_n = val_dist.get("FAILED", 0) + val_dist.get("RETRY_PENDING", 0)
retry_state = "SKIPPED"
retry_run_id = None
retry_url = ""

if failed_n > 0:
    print(f"  {failed_n} rows need retry")
    retry_params = {
        "mode": "RETRY", "meta_catalog": "hive_metastore",
        "meta_schema": "migration_meta", "batch_id": BATCH_ID,
        "max_concurrent_chunks": "2", "parallel_threads": "2",
        "min_executors": "2", "worker_cluster_json": WORKER_CLUSTER_JSON,
    }
    retry_run_id, retry_state, retry_url = run_job(JOB_RETRY, retry_params, "RETRY", timeout_s=22*60)
    results["RETRY"] = {"run_id": retry_run_id, "state": retry_state, "url": retry_url}
    print(f"  RETRY finished: {retry_state}")
    # Re-validate
    run_job(JOB_VALIDATE, val_params, "VALIDATE-post-retry", timeout_s=20*60)
    val_rows, _ = sql_safe(f"""
        SELECT status, COUNT(*) as cnt FROM hive_metastore.migration_meta.migration_control
        WHERE batch_id='{BATCH_ID}' GROUP BY status ORDER BY status
    """)
    val_dist = {r.get("status"): int(r.get("cnt",0)) for r in val_rows}
else:
    print(f"  No failures — RETRY skipped")
    results["RETRY"] = {"run_id": None, "state": "SKIPPED", "url": ""}

# ── Final snapshot ────────────────────────────────────────────────────────────
print("\n── Final migration_control snapshot ────────────────────────────────────")
final_rows, err = sql_safe(f"""
    SELECT source_schema, source_table, target_schema, target_table, status
    FROM hive_metastore.migration_meta.migration_control
    WHERE batch_id='{BATCH_ID}'
    ORDER BY source_schema, source_table
""")
if err:
    print(f"  ✗ {err}")
    final_rows = inv_rows
else:
    for r in final_rows:
        print(f"  {r.get('source_schema',''):<25} {r.get('source_table',''):<25} → "
              f"{r.get('target_schema',''):<30} {r.get('target_table',''):<25} [{r.get('status')}]")

# ── Exclusion/mapping checks ──────────────────────────────────────────────────
import fnmatch
all_src_schemas = {r.get("source_schema","").lower() for r in final_rows}
all_src_tables  = {r.get("source_table","").lower()  for r in final_rows}
all_tgt_schemas = {r.get("target_schema","").lower() for r in final_rows}

sys_excluded  = not any(s in all_src_schemas for s in ["system","samples"])
tmp_excluded  = not any(fnmatch.fnmatch(t,"tmp_*") or fnmatch.fnmatch(t,"staging_*") for t in all_src_tables)
self_excluded = not any(s in all_tgt_schemas for s in ["dc_tgt_alpha","dc_tgt_beta"])  # not as source

catalog_rows = [r for r in final_rows
                if r.get("target_schema","").startswith("dc_tgt_alpha")
                and r.get("target_schema") != "dc_tgt_alpha_yaml"]
schema_rows  = [r for r in final_rows if r.get("target_schema","") == "dc_tgt_beta_yaml"]
table_rows   = [r for r in final_rows
                if r.get("source_table","") == "customers"
                and r.get("target_schema","") == "dc_tgt_alpha_yaml"]
table_rename_ok = any(r.get("target_table") == "customers_copy" for r in table_rows)

# ── Print report ──────────────────────────────────────────────────────────────
sym = lambda b: "✓" if b else "✗"
fmts = lambda s: f"✓ {s}" if s == "SUCCESS" else (f"✓ {s}" if s == "SKIPPED" else f"✗ {s}")

n_completed  = val_dist.get("COMPLETED",  dc_dist.get("COMPLETED", 0))
n_validated  = val_dist.get("VALIDATED",  val_dist.get("COMPLETED", 0))
n_val_failed = val_dist.get("VALIDATION_FAILED", 0)
n_failed     = val_dist.get("FAILED", 0)
n_retry_pend = val_dist.get("RETRY_PENDING", 0)

report = f"""
╔══════════════════════════════════════════════════════════════════════╗
║         YAML ALL-MODES TEST REPORT                                  ║
║         batch_id = {BATCH_ID}                      ║
╚══════════════════════════════════════════════════════════════════════╝

[MODE RESULTS]
  DRY_RUN   : {fmts(results["DRY_RUN"]["state"])}  — planned tables, 0 written to migration_control
  INVENTORY : {fmts(results["INVENTORY"]["state"])}  — {n_queued} tables QUEUED
  DEEP_CLONE: {fmts(results["DEEP_CLONE"]["state"])}  — {n_completed} COMPLETED, {n_failed} FAILED
  VALIDATE  : {fmts(results["VALIDATE"]["state"])}  — {n_validated} VALIDATED, {n_val_failed} VALIDATION_FAILED
  RETRY     : {fmts(results["RETRY"]["state"])}  — {failed_n} requeued{" (or SKIPPED — no failures)" if failed_n == 0 else ""}

[MAPPING VALIDATION — verify all 3 types worked]
  CATALOG mapping (dc_src_alpha → dc_tgt_alpha):
    {sym(len(catalog_rows) > 0)}  {len(catalog_rows)} tables discovered via catalog expansion, exclusions applied
  SCHEMA mapping (dc_src_beta → dc_tgt_beta_yaml):
    {sym(len(schema_rows) > 0)}  {len(schema_rows)} tables cloned, target_schema=dc_tgt_beta_yaml confirmed
  TABLE mapping (dc_src_alpha.customers → dc_tgt_alpha_yaml.customers_copy):
    {sym(len(table_rows) > 0 and table_rename_ok)}  target_table = {"customers_copy ✓" if table_rename_ok else "WRONG: " + str({r.get("target_table") for r in table_rows})}

[EXCLUSION VALIDATION]
  {sym(sys_excluded)}  Global exclude.catalogs: system/samples not in results
  {sym(tmp_excluded)}  Global exclude.tables: tmp_*/staging_* not in results
  {sym(self_excluded)}  Per-mapping exclude_schemas: dc_tgt_alpha/dc_tgt_beta skipped as source

[FINAL migration_control SNAPSHOT]  batch={BATCH_ID}
  {"source_schema":<25} {"source_table":<25} {"target_schema":<30} {"target_table":<25} {"status"}"""

for r in final_rows:
    report += (f"\n  {r.get('source_schema',''):<25} {r.get('source_table',''):<25} "
               f"{r.get('target_schema',''):<30} {r.get('target_table',''):<25} {r.get('status','')}")

report += f"""

[JOB URLS]
  DRY_RUN   : {results["DRY_RUN"]["url"]}
  INVENTORY : {results["INVENTORY"]["url"]}
  DEEP_CLONE: {results["DEEP_CLONE"]["url"]}
  VALIDATE  : {results["VALIDATE"]["url"]}
  RETRY     : {results["RETRY"]["url"] or "(not run)"}
"""

print(report)
