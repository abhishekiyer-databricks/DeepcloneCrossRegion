#!/usr/bin/env python3
"""
dual_batch_test_v4.py — Full fix: correct target schemas + validator fix
=========================================================================
Fixes applied to orchestrator:
  1. delta_share inventory → tgt_sql/tgt_api on TARGET workspace
  2. target_schema parameter → dc_src_alpha→dc_tgt_alpha, dc_src_beta→dc_tgt_beta
  3. delta_share validator → tgt_sql for source describes (source is on TARGET)

Source tables: hive_metastore.dc_src_alpha.* / dc_src_beta.* (already exist)
Target tables: hive_metastore.dc_tgt_alpha.* / dc_tgt_beta.* (created by DEEP_CLONE)
"""

import os, requests, time, json, threading
from datetime import datetime, timezone

# NEVER hardcode secrets here — load from env vars (same names the bundle
# injects into job clusters: AZ2AZ_TGT_URL / AZ2AZ_TGT_CID / AZ2AZ_TGT_SECRET).
TGT_URL    = os.environ["AZ2AZ_TGT_URL"]
TGT_CID    = os.environ["AZ2AZ_TGT_CID"]
TGT_SECRET = os.environ["AZ2AZ_TGT_SECRET"]
TGT_WH     = os.environ.get("AZ2AZ_TGT_WH_ID", "5fe1692f119e2528")

JOB_INV  = "125033843689125"
JOB_DC   = "251066514084029"
JOB_VAL  = "196928057201340"

BATCH_A  = "team-alpha-20260903"
BATCH_B  = "team-beta-20260903"

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
        "PYTHONPATH":       "/dbfs/deepclone_orchestrator"
    }
})

_tok_cache = {}

def get_token():
    if TGT_CID in _tok_cache:
        tok, exp = _tok_cache[TGT_CID]
        if time.time() < exp - 30: return tok
    r = requests.post(f"{TGT_URL}/oidc/v1/token",
        data={"grant_type":"client_credentials","scope":"all-apis"},
        auth=(TGT_CID, TGT_SECRET), timeout=30)
    r.raise_for_status()
    d = r.json()
    _tok_cache[TGT_CID] = (d["access_token"], time.time() + d.get("expires_in", 3600))
    return d["access_token"]

def H(): return {"Authorization":f"Bearer {get_token()}","Content-Type":"application/json"}

def sql(stmt, label="SQL", timeout=120):
    r = requests.post(f"{TGT_URL}/api/2.0/sql/statements", headers=H(),
        json={"statement":stmt,"warehouse_id":TGT_WH,"wait_timeout":"0s"}, timeout=30)
    r.raise_for_status()
    sid = r.json()["statement_id"]
    deadline = time.time()+timeout
    while time.time()<deadline:
        d = requests.get(f"{TGT_URL}/api/2.0/sql/statements/{sid}", headers=H(), timeout=30).json()
        s = d.get("status",{}).get("state","")
        if s=="SUCCEEDED": return d
        if s in ("FAILED","CANCELED","CLOSED"):
            raise RuntimeError(f"[{label}] {s}: {d.get('status',{}).get('error',{})}")
        time.sleep(4)
    raise TimeoutError(f"[{label}] timed out")

def qry(stmt, label="q"):
    d = sql(stmt, label)
    cols = [c["name"] for c in d.get("manifest",{}).get("schema",{}).get("columns",[])]
    return [dict(zip(cols,r)) for r in (d.get("result",{}).get("data_array",[]) or [])]

def launch(job_id, params, label):
    r = requests.post(f"{TGT_URL}/api/2.1/jobs/run-now", headers=H(),
        json={"job_id":int(job_id),"job_parameters":params}, timeout=30)
    r.raise_for_status()
    rid = r.json()["run_id"]
    print(f"  ✅ {label} run_id={rid}  {TGT_URL}/#job/{job_id}/run/{rid}")
    return rid

def poll(run_id, label, interval=20, timeout=5400):
    deadline=time.time()+timeout; start=time.time()
    while time.time()<deadline:
        d = requests.get(f"{TGT_URL}/api/2.1/jobs/runs/get?run_id={run_id}",
                         headers=H(), timeout=30).json()
        lc=d.get("state",{}).get("life_cycle_state","")
        rs=d.get("state",{}).get("result_state","")
        if lc in ("TERMINATED","SKIPPED","INTERNAL_ERROR"):
            elapsed=int(time.time()-start)
            print(f"    {'✅' if rs=='SUCCESS' else '❌'} [{label}] run={run_id} → {lc}/{rs} ({elapsed}s)")
            return d
        print(f"    ⏳ [{label}] run={run_id} life={lc} ({int(time.time()-start)}s)")
        time.sleep(interval)
    raise TimeoutError(f"[{label}] timeout")

def poll_par(pairs, interval=20, timeout=5400):
    res={}; lock=threading.Lock()
    def _p(rid,lbl):
        try: d=poll(rid,lbl,interval,timeout);lock.acquire();res[rid]=d;lock.release()
        except Exception as e: lock.acquire();res[rid]={"_err":str(e)};lock.release()
    ts=[threading.Thread(target=_p,args=(r,l),daemon=True) for r,l in pairs]
    [t.start() for t in ts]; [t.join() for t in ts]
    return res

def errors(run_id):
    tasks=requests.get(f"{TGT_URL}/api/2.1/jobs/runs/get?run_id={run_id}",headers=H(),timeout=30).json().get("tasks",[])
    out=[]
    for t in tasks:
        tid=t.get("run_id")
        if tid:
            od=requests.get(f"{TGT_URL}/api/2.1/jobs/runs/get-output?run_id={tid}",headers=H(),timeout=30).json()
            out.append({"task":t.get("task_key"),"error":od.get("error",""),
                        "trace":od.get("error_trace","")[:3000],"nb":od.get("notebook_output",{})})
    return out

def report_run(rid, label, results):
    d=results.get(rid,{})
    if "_err" in d: print(f"  ❌ {label} run={rid} ERROR: {d['_err']}"); return False
    rs=d.get("state",{}).get("result_state","?"); lc=d.get("state",{}).get("life_cycle_state","?")
    ok=rs=="SUCCESS"; print(f"  {'✅' if ok else '❌'} {label} run={rid}: {lc}/{rs}")
    if not ok:
        for e in errors(rid):
            print(f"    Task {e['task']}: {e['error'][:600]}")
            if e['trace']: print(f"    Trace:\n{e['trace'][:1500]}")
            nb=e.get('nb',{}); 
            if nb and nb.get('result'): print(f"    NB: {nb['result'][:400]}")
    return ok

def main():
    ts=datetime.now(timezone.utc).isoformat()
    print("\n"+"█"*70)
    print("  DUAL-BATCH PARALLEL TEST v4 — All fixes applied")
    print("  dc_src_alpha→dc_tgt_alpha / dc_src_beta→dc_tgt_beta")
    print(f"  Started: {ts}")
    print("█"*70)

    run_ids = {}

    # Verify source tables exist
    print("\n  Source tables on TARGET workspace:")
    for sch in ["dc_src_alpha","dc_src_beta"]:
        rows=qry(f"SHOW TABLES IN hive_metastore.{sch}")
        print(f"    {sch}: {sorted([r.get('tableName') for r in rows])}")

    # Step 2: Clear
    print("\n"+"═"*70)
    print("STEP 2: Clear migration_control")
    print("═"*70)
    sql("DELETE FROM hive_metastore.migration_meta.migration_control","clear",60)
    n=qry("SELECT COUNT(*) AS n FROM hive_metastore.migration_meta.migration_control")[0]["n"]
    print(f"  ✅ Cleared — {n} rows remaining")

    # Step 3: Parallel inventories with target_schema override
    print("\n"+"═"*70)
    print("STEP 3: PARALLEL INVENTORIES")
    print("  Batch A: dc_src_alpha → dc_tgt_alpha")
    print("  Batch B: dc_src_beta  → dc_tgt_beta")
    print("═"*70)

    base_inv = {
        "source_tables":"[]", "source_catalogs":"[]",
        "target_catalog":"hive_metastore",
        "clone_type":"delta_share",
        "meta_catalog":"hive_metastore", "meta_schema":"migration_meta",
        "chunk_capacity_gb":"1", "max_concurrent_chunks":"2",
        "parallel_threads":"2", "min_executors":"2",
    }

    print(f"\n  Launching INVENTORY-A ({BATCH_A}) ...")
    run_a = launch(JOB_INV, {**base_inv,
        "source_schemas":'["hive_metastore.dc_src_alpha"]',
        "target_schema":"dc_tgt_alpha",
        "batch_id":BATCH_A}, "INVENTORY-A")

    print(f"\n  Launching INVENTORY-B ({BATCH_B}) ...")
    run_b = launch(JOB_INV, {**base_inv,
        "source_schemas":'["hive_metastore.dc_src_beta"]',
        "target_schema":"dc_tgt_beta",
        "batch_id":BATCH_B}, "INVENTORY-B")

    print(f"\n  Polling both in parallel ...")
    res=poll_par([(run_a,"INVENTORY-A"),(run_b,"INVENTORY-B")],interval=15,timeout=1800)
    ok_a=report_run(run_a,"INVENTORY-A",res); ok_b=report_run(run_b,"INVENTORY-B",res)
    run_ids["INVENTORY-A"]=run_a; run_ids["INVENTORY-B"]=run_b

    print("\n  migration_control after inventories:")
    rows=qry("SELECT batch_id,target_schema,source_schema,status,COUNT(*) AS cnt FROM hive_metastore.migration_meta.migration_control GROUP BY batch_id,target_schema,source_schema,status ORDER BY batch_id")
    for r in rows:
        print(f"    batch={r['batch_id']:25s} {r['source_schema']}→{r['target_schema']} status={r['status']} cnt={r['cnt']}")
    if not rows:
        print("    ⚠️  Still 0 rows!")
        for rid,lbl in [(run_a,"INV-A"),(run_b,"INV-B")]:
            for e in errors(rid):
                print(f"    [{lbl}] {e['task']}: {e['error'][:400]}")
                nb=e.get('nb',{}); print(f"    NB: {nb}")

    bids=qry("SELECT DISTINCT batch_id,source_schema,target_schema FROM hive_metastore.migration_meta.migration_control ORDER BY batch_id")
    print(f"\n  Schema mappings in control:")
    for r in bids:
        print(f"    {r['batch_id']}: {r['source_schema']} → {r['target_schema']}")

    # Step 4: Parallel deep clones
    print("\n"+"═"*70)
    print("STEP 4: PARALLEL DEEP CLONEs")
    print("═"*70)

    base_dc = {
        "meta_catalog":"hive_metastore","meta_schema":"migration_meta",
        "validation_enabled":"true","row_count_validation":"false",
        "max_retries":"2","max_concurrent_chunks":"2",
        "parallel_threads":"2","chunk_capacity_gb":"1","min_executors":"2",
        "worker_cluster_json":WORKER_CLUSTER_JSON,
    }

    print(f"\n  Launching DEEP_CLONE-A ({BATCH_A}) ...")
    dc_a = launch(JOB_DC, {**base_dc,"batch_id":BATCH_A}, "DEEP_CLONE-A")
    print(f"\n  Launching DEEP_CLONE-B ({BATCH_B}) ...")
    dc_b = launch(JOB_DC, {**base_dc,"batch_id":BATCH_B}, "DEEP_CLONE-B")

    print(f"\n  Polling both in parallel ...")
    res=poll_par([(dc_a,"DEEP_CLONE-A"),(dc_b,"DEEP_CLONE-B")],interval=20,timeout=5400)
    ok_dc_a=report_run(dc_a,"DEEP_CLONE-A",res); ok_dc_b=report_run(dc_b,"DEEP_CLONE-B",res)
    run_ids["DEEP_CLONE-A"]=dc_a; run_ids["DEEP_CLONE-B"]=dc_b

    print("\n  migration_control after deep_clones:")
    rows=qry("SELECT batch_id,source_schema,target_schema,status,COUNT(*) AS cnt FROM hive_metastore.migration_meta.migration_control GROUP BY batch_id,source_schema,target_schema,status ORDER BY batch_id,status")
    for r in rows:
        print(f"    batch={r['batch_id']:25s} {r['source_schema']}→{r['target_schema']} status={r['status']} cnt={r['cnt']}")

    # Step 5: Parallel validates
    print("\n"+"═"*70)
    print("STEP 5: PARALLEL VALIDATEs")
    print("═"*70)

    base_val = {"meta_catalog":"hive_metastore","meta_schema":"migration_meta"}
    print(f"\n  Launching VALIDATE-A ({BATCH_A}) ...")
    val_a = launch(JOB_VAL, {**base_val,"batch_id":BATCH_A}, "VALIDATE-A")
    print(f"\n  Launching VALIDATE-B ({BATCH_B}) ...")
    val_b = launch(JOB_VAL, {**base_val,"batch_id":BATCH_B}, "VALIDATE-B")

    print(f"\n  Polling both in parallel ...")
    res=poll_par([(val_a,"VALIDATE-A"),(val_b,"VALIDATE-B")],interval=15,timeout=1800)
    ok_val_a=report_run(val_a,"VALIDATE-A",res); ok_val_b=report_run(val_b,"VALIDATE-B",res)
    run_ids["VALIDATE-A"]=val_a; run_ids["VALIDATE-B"]=val_b

    # Step 6: Final report
    print("\n"+"═"*70)
    print("DUAL-BATCH PARALLEL TEST — FINAL REPORT")
    print("═"*70)

    print("\n[1] BATCH SUMMARY")
    rows=qry("""SELECT batch_id,
        COUNT(*) AS tables, COUNT(DISTINCT chunk_id) AS chunks,
        SUM(CASE WHEN status='COMPLETED'  THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='FAILED'     THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN validation_status='VALIDATED' THEN 1 ELSE 0 END) AS validated
        FROM hive_metastore.migration_meta.migration_control GROUP BY batch_id ORDER BY batch_id""")
    print(f"  {'Batch':<25} │ {'Tables':>6} │ {'Chunks':>6} │ {'COMPLETED':>9} │ {'FAILED':>6} │ {'VALIDATED':>9}")
    print(f"  {'─'*25}─┼─{'─'*6}─┼─{'─'*6}─┼─{'─'*9}─┼─{'─'*6}─┼─{'─'*9}")
    for r in rows:
        print(f"  {str(r['batch_id']):<25} │ {str(r['tables']):>6} │ {str(r['chunks']):>6} │ "
              f"{str(r['completed']):>9} │ {str(r['failed']):>6} │ {str(r['validated']):>9}")

    print("\n[2] CHUNK BREAKDOWN (bin-packing)")
    rows=qry("SELECT batch_id,chunk_id,COUNT(*) AS tables,ROUND(SUM(COALESCE(size_gb,0)),4) AS gb,MAX(status) AS status FROM hive_metastore.migration_meta.migration_control GROUP BY batch_id,chunk_id ORDER BY batch_id,chunk_id")
    for r in rows:
        print(f"  batch={r['batch_id']}  chunk={r['chunk_id']}  tables={r['tables']}  gb={r['gb']}  status={r['status']}")

    print("\n[3] PER-TABLE RESULTS")
    rows=qry("SELECT batch_id,source_schema,source_table,target_schema,target_table,status,validation_status,COALESCE(duration_seconds,0) AS dur,error_message,validation_message FROM hive_metastore.migration_meta.migration_control ORDER BY batch_id,source_table")
    for r in rows:
        batch="alpha" if "alpha" in str(r['batch_id']) else "beta"
        src=f"{r['source_schema']}.{r['source_table']}"
        tgt=f"{r['target_schema']}.{r['target_table']}"
        sts=r['status']; vsts=r.get('validation_status','-')
        icon="✓" if sts in ("COMPLETED","VALIDATED") else "✗"
        vicon="✓" if vsts=="VALIDATED" else "✗"
        print(f"  {icon} [{batch}] {src} → {tgt} | {sts} | {vicon}{vsts} | {r['dur']}s")
        err=r.get("error_message") or ""; vm=r.get("validation_message") or ""
        if err: print(f"       ERR: {err[:200]}")
        if vm and vsts!="VALIDATED": print(f"       VAL: {vm[:200]}")

    print("\n[4] JOB URLS")
    jmap=[("INV-A",JOB_INV,"INVENTORY-A"),("INV-B",JOB_INV,"INVENTORY-B"),
          ("DC-A",JOB_DC,"DEEP_CLONE-A"),("DC-B",JOB_DC,"DEEP_CLONE-B"),
          ("VAL-A",JOB_VAL,"VALIDATE-A"),("VAL-B",JOB_VAL,"VALIDATE-B")]
    for short,jid,key in jmap:
        rid=run_ids.get(key,"N/A")
        url=f"{TGT_URL}/#job/{jid}/run/{rid}" if rid!="N/A" else "NOT LAUNCHED"
        print(f"  {short:<8}: {url}")

    print("\n[5] FINAL migration_control STATE (full)")
    rows=qry("SELECT batch_id,chunk_id,source_schema,source_table,target_schema,target_table,status,validation_status,size_gb,duration_seconds,error_code,error_message,validation_message FROM hive_metastore.migration_meta.migration_control ORDER BY batch_id,source_table")
    print(f"  Total rows: {len(rows)}")
    for r in rows:
        print(f"\n  ── {r['batch_id']} chunk={r['chunk_id']}")
        for k,v in r.items():
            if v is not None and str(v) not in ("","0","None"):
                print(f"    {k:25s}: {str(v)[:150]}")

    end_ts=datetime.now(timezone.utc).isoformat()
    all_ok = ok_dc_a and ok_dc_b and ok_val_a and ok_val_b
    print("\n"+"█"*70)
    print(f"  {'✅ PASSED' if all_ok else '⚠️  PARTIAL'}")
    print(f"  Started : {ts}")
    print(f"  Finished: {end_ts}")
    print("█"*70)

if __name__ == "__main__":
    main()
