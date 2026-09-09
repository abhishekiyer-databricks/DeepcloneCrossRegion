"""
create_and_run_job.py
Creates the DeepClone CrossRegion Databricks Workflow Job on the SOURCE workspace,
runs it, monitors it, and prints the result + URL.
"""
import os, requests, time, json

SRC_URL    = os.environ["AZ2AZ_SRC_URL"]
SRC_CID    = os.environ["AZ2AZ_SRC_CID"]
SRC_SECRET = os.environ["AZ2AZ_SRC_SECRET"]
TGT_URL    = os.environ["AZ2AZ_TGT_URL"]
TGT_CID    = os.environ["AZ2AZ_TGT_CID"]
TGT_SECRET = os.environ["AZ2AZ_TGT_SECRET"]

WS_NB_PATH = "/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion/deepclone_main"
JOB_NAME   = "DeepClone CrossRegion — azure_uc_demo_region2 → azure_uc_demo_region1"

def tok():
    r = requests.post(f"{SRC_URL}/oidc/v1/token",
        data={"grant_type":"client_credentials","client_id":SRC_CID,
              "client_secret":SRC_SECRET,"scope":"all-apis"}, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]

def api(method, path, **kwargs):
    t = tok()
    fn = getattr(requests, method)
    r = fn(f"{SRC_URL}{path}", headers={"Authorization": f"Bearer {t}"}, timeout=60, **kwargs)
    return r

print("═"*70, flush=True)
print("  CREATING DATABRICKS WORKFLOW JOB", flush=True)
print("═"*70, flush=True)

# ── Delete existing job with same name (idempotent) ───────────────────────
existing = api("get", "/api/2.1/jobs/list", params={"name": JOB_NAME}).json()
for j in existing.get("jobs", []):
    api("post", "/api/2.1/jobs/delete", json={"job_id": j["job_id"]})
    print(f"  Deleted existing job: {j['job_id']}", flush=True)

# ── Job definition ────────────────────────────────────────────────────────
job_spec = {
    "name": JOB_NAME,
    "tags": {
        "project": "deepclone-crossregion",
        "owner": "vivek.ravichandiran@databricks.com",
        "env": "dr"
    },
    "tasks": [
        {
            "task_key": "deepclone_orchestrator",
            "description": "Reads onboarding table, resolves paths, runs DEEP CLONE, writes audit",
            "notebook_task": {
                "notebook_path": WS_NB_PATH,
                "source": "WORKSPACE"
            },
            "new_cluster": {
                "spark_version": "12.2.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 0,
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "spark_env_vars": {
                    "AZ2AZ_SRC_URL":    SRC_URL,
                    "AZ2AZ_SRC_CID":    SRC_CID,
                    "AZ2AZ_SRC_SECRET": SRC_SECRET,
                    "AZ2AZ_TGT_URL":    TGT_URL,
                    "AZ2AZ_TGT_CID":    TGT_CID,
                    "AZ2AZ_TGT_SECRET": TGT_SECRET,
                    "AZ2AZ_SRC_WH_ID":  "154fa0bd2b7f66fc",
                    "AZ2AZ_TGT_WH_ID":  "8c47421fdc710056",
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD"
            },
            "timeout_seconds": 1800,
            "max_retries": 1,
            "min_retry_interval_millis": 60000
        }
    ],
    "job_clusters": [],
    "email_notifications": {
        "on_failure": ["vivek.ravichandiran@databricks.com"],
        "no_alert_for_skipped_runs": True
    },
    "timeout_seconds": 3600,
    "max_concurrent_runs": 1,
    "format": "MULTI_TASK"
}

resp = api("post", "/api/2.1/jobs/create", json=job_spec)
if resp.status_code != 200:
    print(f"  [FAIL] Job create: {resp.status_code} {resp.text[:300]}", flush=True)
    raise SystemExit(1)

job_id = resp.json()["job_id"]
job_url = f"{SRC_URL}/jobs/{job_id}"
print(f"\n  ✓ Job created successfully!", flush=True)
print(f"  Job ID  : {job_id}", flush=True)
print(f"  Job URL : {job_url}", flush=True)

# ── Trigger run ───────────────────────────────────────────────────────────
print(f"\n  Triggering job run…", flush=True)
run_resp = api("post", "/api/2.1/jobs/run-now", json={"job_id": job_id})
if run_resp.status_code != 200:
    print(f"  [FAIL] Trigger: {run_resp.status_code} {run_resp.text[:300]}", flush=True)
    raise SystemExit(1)

run_id = run_resp.json()["run_id"]
run_url = f"{SRC_URL}/jobs/{job_id}/runs/{run_id}"
print(f"  ✓ Run triggered!", flush=True)
print(f"  Run ID  : {run_id}", flush=True)
print(f"  Run URL : {run_url}", flush=True)

# ── Monitor ───────────────────────────────────────────────────────────────
print(f"\n  Monitoring run (polling every 30s)…", flush=True)
print(f"  {'─'*66}", flush=True)

last_state = ""
elapsed = 0
while True:
    run_info = api("get", f"/api/2.1/jobs/runs/get", params={"run_id": run_id}).json()
    state    = run_info.get("state", {})
    lc       = state.get("life_cycle_state", "")
    rs       = state.get("result_state", "")
    msg      = state.get("state_message", "")
    tasks    = run_info.get("tasks", [])

    if lc != last_state:
        print(f"  [{elapsed:>3}s] state={lc}  result={rs}  {msg[:60]}", flush=True)
        for t in tasks:
            ts = t.get("state", {})
            print(f"         task={t['task_key']}  lc={ts.get('life_cycle_state','')}  rs={ts.get('result_state','')}", flush=True)
        last_state = lc

    if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        final_state = rs
        break

    if elapsed >= 1800:
        print("  TIMEOUT — job exceeded 30 min", flush=True)
        final_state = "TIMEOUT"
        break

    time.sleep(30)
    elapsed += 30

print(f"  {'─'*66}", flush=True)
print(f"\n  Final result: {final_state}", flush=True)

# If failed, get task output
if final_state not in ("SUCCESS",):
    for t in tasks:
        task_run_id = t.get("run_id")
        if task_run_id:
            output = api("get", "/api/2.1/jobs/runs/get-output",
                         params={"run_id": task_run_id}).json()
            nb_out = output.get("notebook_output", {}).get("result", "")
            err    = output.get("error", "")
            err_trace = output.get("error_trace", "")
            print(f"\n  Task output (last 3000 chars):", flush=True)
            print((nb_out or err or err_trace)[-3000:], flush=True)

print(f"\n{'═'*70}", flush=True)
print(f"  JOB COMPLETE", flush=True)
print(f"  Job URL : {job_url}", flush=True)
print(f"  Run URL : {run_url}", flush=True)
print(f"  Status  : {final_state}", flush=True)
print(f"{'═'*70}", flush=True)
