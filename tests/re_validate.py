#!/usr/bin/env python3
"""re_validate.py — Re-run VALIDATE with clone_type=delta_share for both batches."""
import os, requests, time, json, threading

# NEVER hardcode secrets here — load from env vars (same names the bundle
# injects into job clusters: AZ2AZ_TGT_URL / AZ2AZ_TGT_CID / AZ2AZ_TGT_SECRET).
TGT_URL = os.environ["AZ2AZ_TGT_URL"]
TGT_CID = os.environ["AZ2AZ_TGT_CID"]
TGT_SECRET = os.environ["AZ2AZ_TGT_SECRET"]
TGT_WH = os.environ.get("AZ2AZ_TGT_WH_ID", "5fe1692f119e2528")
JOB_VAL = "196928057201340"
BATCH_A = "team-alpha-20260903"
BATCH_B = "team-beta-20260903"

_tok = {}
def get_token():
    if TGT_CID in _tok:
        t, e = _tok[TGT_CID]
        if time.time() < e - 30: return t
    r = requests.post(f"{TGT_URL}/oidc/v1/token",
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(TGT_CID, TGT_SECRET), timeout=30)
    r.raise_for_status()
    d = r.json()
    _tok[TGT_CID] = (d["access_token"], time.time() + d.get("expires_in", 3600))
    return d["access_token"]

def H():
    return {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

def run_sql(stmt, timeout=120):
    r = requests.post(f"{TGT_URL}/api/2.0/sql/statements", headers=H(),
        json={"statement": stmt, "warehouse_id": TGT_WH, "wait_timeout": "0s"}, timeout=30)
    r.raise_for_status()
    sid = r.json()["statement_id"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = requests.get(f"{TGT_URL}/api/2.0/sql/statements/{sid}", headers=H(), timeout=30).json()
        s = d.get("status", {}).get("state", "")
        if s == "SUCCEEDED": return d
        if s in ("FAILED", "CANCELED", "CLOSED"):
            raise RuntimeError(f"{s}: {d.get('status', {}).get('error', {})}")
        time.sleep(4)
    raise TimeoutError("SQL timed out")

def qry(stmt):
    d = run_sql(stmt)
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    return [dict(zip(cols, r)) for r in (d.get("result", {}).get("data_array", []) or [])]

# Reset VALIDATION_FAILED → COMPLETED
print("Resetting VALIDATION_FAILED → COMPLETED ...")
run_sql("""UPDATE hive_metastore.migration_meta.migration_control
SET status='COMPLETED', validation_status=NULL, validation_message=NULL
WHERE status='VALIDATION_FAILED'""")
rows = qry("SELECT batch_id, status, COUNT(*) AS cnt FROM hive_metastore.migration_meta.migration_control GROUP BY batch_id, status ORDER BY batch_id")
print("  Current state:")
for r in rows:
    print(f"    {r['batch_id']:30s} {r['status']:20s} {r['cnt']}")

# Launch both VALIDATE jobs with clone_type=delta_share
base_params = {
    "meta_catalog": "hive_metastore",
    "meta_schema":  "migration_meta",
    "clone_type":   "delta_share",   # ← KEY: ensures validator uses tgt_sql for source
}

def launch_job(batch_id, label):
    r = requests.post(f"{TGT_URL}/api/2.1/jobs/run-now", headers=H(),
        json={"job_id": int(JOB_VAL), "job_parameters": {**base_params, "batch_id": batch_id}},
        timeout=30)
    r.raise_for_status()
    rid = r.json()["run_id"]
    print(f"  ✅ {label} run_id={rid}  {TGT_URL}/#job/{JOB_VAL}/run/{rid}")
    return rid

def poll_job(run_id, label, interval=15, timeout=1800):
    deadline = time.time() + timeout
    start = time.time()
    while time.time() < deadline:
        d = requests.get(f"{TGT_URL}/api/2.1/jobs/runs/get?run_id={run_id}",
                         headers=H(), timeout=30).json()
        lc = d.get("state", {}).get("life_cycle_state", "")
        rs = d.get("state", {}).get("result_state", "")
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            elapsed = int(time.time() - start)
            print(f"  {'✅' if rs == 'SUCCESS' else '❌'} [{label}] run={run_id} {lc}/{rs} ({elapsed}s)")
            return d
        print(f"  ⏳ [{label}] run={run_id} {lc} ({int(time.time()-start)}s)")
        time.sleep(interval)
    raise TimeoutError(f"[{label}] timeout")

print("\nLaunching VALIDATE-A ...")
va = launch_job(BATCH_A, "VALIDATE-A")
print("Launching VALIDATE-B ...")
vb = launch_job(BATCH_B, "VALIDATE-B")

print("\nPolling in parallel ...")
res = {}
lock = threading.Lock()

def _poll(rid, lbl):
    try:
        d = poll_job(rid, lbl)
        with lock: res[rid] = d
    except Exception as e:
        with lock: res[rid] = {"_err": str(e)}

threads = [threading.Thread(target=_poll, args=(va, "VALIDATE-A"), daemon=True),
           threading.Thread(target=_poll, args=(vb, "VALIDATE-B"), daemon=True)]
[t.start() for t in threads]
[t.join() for t in threads]

print("\nFinal migration_control state:")
rows = qry("""SELECT batch_id, source_schema, source_table, target_schema, target_table,
    status, validation_status, validation_message
    FROM hive_metastore.migration_meta.migration_control
    ORDER BY batch_id, source_table""")
print(f"  Total rows: {len(rows)}")
for r in rows:
    batch = "alpha" if "alpha" in r["batch_id"] else "beta"
    src = f"{r['source_schema']}.{r['source_table']}"
    tgt = f"{r['target_schema']}.{r['target_table']}"
    sts = r["status"]
    vsts = r.get("validation_status", "-")
    vm = r.get("validation_message", "") or ""
    icon = "✓" if vsts == "VALIDATED" else "✗"
    print(f"  {icon} [{batch}] {src} → {tgt} | {sts} | {vsts}")
    if vm and vsts != "VALIDATED":
        print(f"       {vm[:250]}")

print(f"\nVAL-A: {TGT_URL}/#job/{JOB_VAL}/run/{va}")
print(f"VAL-B: {TGT_URL}/#job/{JOB_VAL}/run/{vb}")
