"""
live_test.py — DeepClone CrossRegion live connectivity & dry-run test.
Runs entirely via REST API (no Databricks SDK needed).
Usage: python3 -u live_test.py
"""
import requests, json, os, sys, time

SRC_URL    = os.environ.get("AZ2AZ_SRC_URL","")
SRC_CID    = os.environ.get("AZ2AZ_SRC_CID","")
SRC_SECRET = os.environ.get("AZ2AZ_SRC_SECRET","")
TGT_URL    = os.environ.get("AZ2AZ_TGT_URL","")
TGT_CID    = os.environ.get("AZ2AZ_TGT_CID","")
TGT_SECRET = os.environ.get("AZ2AZ_TGT_SECRET","")

SRC_WH_ID   = "154fa0bd2b7f66fc"   # Serverless Starter Warehouse (fast cold start)
SRC_CATALOG = "hackathon"
SRC_SCHEMA  = "synthetic"
TGT_CATALOG = "rilmigration"
MAX_CLUSTERS = 3

print(f"SRC={SRC_URL}", flush=True)
print(f"TGT={TGT_URL}", flush=True)

warnings = []

# ── Auth ──────────────────────────────────────────────────────────────────
def get_token(url, cid, secret):
    r = requests.post(f"{url}/oidc/v1/token",
        data={"grant_type":"client_credentials","client_id":cid,
              "client_secret":secret,"scope":"all-apis"}, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]

def api_get(tok, url, path, params=None):
    r = requests.get(f"{url}{path}", headers={"Authorization":f"Bearer {tok}"},
        params=params, timeout=30)
    return r.json()

print("[Auth] Acquiring tokens…", flush=True)
src_tok = get_token(SRC_URL, SRC_CID, SRC_SECRET)
print("  ✓  Source token acquired", flush=True)
tgt_tok = get_token(TGT_URL, TGT_CID, TGT_SECRET)
print("  ✓  Target token acquired\n", flush=True)

# ── Step 1: Start warehouse ───────────────────────────────────────────────
print(f"[Step 1] Starting warehouse {SRC_WH_ID} (az2az-test-warehouse-2-x-small)…", flush=True)
r = requests.post(f"{SRC_URL}/api/2.0/sql/warehouses/{SRC_WH_ID}/start",
    headers={"Authorization":f"Bearer {src_tok}"}, timeout=30)
print(f"         HTTP {r.status_code}", flush=True)

for attempt in range(40):
    wh = api_get(src_tok, SRC_URL, f"/api/2.0/sql/warehouses/{SRC_WH_ID}")
    state = wh.get("state","?")
    print(f"         attempt {attempt+1:02d}: state={state}", flush=True)
    if state == "RUNNING":
        print("         ✓  Warehouse RUNNING\n", flush=True)
        break
    if state in ("DELETING","DELETED"):
        print(f"         ✗  Terminal state: {state}"); sys.exit(1)
    time.sleep(5)
else:
    print("         ⚠  Timed out waiting — attempting SQL anyway\n", flush=True)

# ── Step 2: Discover tables ───────────────────────────────────────────────
print(f"[Step 2] Listing tables in {SRC_CATALOG}.{SRC_SCHEMA}…", flush=True)
tbls_resp = api_get(src_tok, SRC_URL, "/api/2.1/unity-catalog/tables",
    params={"catalog_name": SRC_CATALOG, "schema_name": SRC_SCHEMA})
src_tables = [t for t in tbls_resp.get("tables",[]) if t.get("table_type")=="MANAGED"]
print(f"         Found {len(src_tables)} MANAGED tables\n", flush=True)

# ── Step 3: DESCRIBE DETAIL ───────────────────────────────────────────────
def run_sql(tok, url, sql, wh_id):
    payload = {"statement": sql, "warehouse_id": wh_id,
               "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"}
    r = requests.post(f"{url}/api/2.0/sql/statements",
        headers={"Authorization":f"Bearer {tok}","Content-Type":"application/json"},
        json=payload, timeout=60)
    resp = r.json()
    stmt_id = resp.get("statement_id")
    if not stmt_id:
        return None, resp
    for _ in range(60):
        poll = api_get(tok, url, f"/api/2.0/sql/statements/{stmt_id}")
        state = poll.get("status",{}).get("state","")
        if state == "SUCCEEDED":
            return poll, None
        if state in ("FAILED","CANCELED","CLOSED"):
            return None, poll.get("status",{}).get("error",{})
        time.sleep(2)
    return None, "timeout"

print("[Step 3] DESCRIBE DETAIL via SQL Statement API…", flush=True)
print("─"*72, flush=True)

table_details = []
for tbl in src_tables:
    tname    = tbl["name"]
    full_ref = f"`{SRC_CATALOG}`.`{SRC_SCHEMA}`.`{tname}`"
    print(f"  → {tname}…", end="  ", flush=True)
    result, err = run_sql(src_tok, SRC_URL, f"DESCRIBE DETAIL {full_ref}", SRC_WH_ID)
    if err or not result:
        print(f"ERROR: {err}", flush=True)
        warnings.append(f"DESCRIBE DETAIL failed: {tname}: {err}")
        table_details.append({"name":tname,"full_name":f"{SRC_CATALOG}.{SRC_SCHEMA}.{tname}",
            "type":"UNKNOWN","location":"","size_bytes":0,"num_files":0,"partitions":[],"error":str(err)})
        continue
    try:
        cols = [c["name"] for c in result["manifest"]["schema"]["columns"]]
        row  = dict(zip(cols, result["result"]["data_array"][0]))
        loc   = row.get("location","") or ""
        size  = int(row.get("sizeInBytes",0) or 0)
        nf    = int(row.get("numFiles",0) or 0)
        parts = row.get("partitionColumns",[]) or []
        ttype = (row.get("type","") or "MANAGED").upper()
        print(f"{ttype}  {size/1e6:.2f} MB  {nf} files  parts={parts}", flush=True)
        table_details.append({"name":tname,"full_name":f"{SRC_CATALOG}.{SRC_SCHEMA}.{tname}",
            "type":ttype,"location":loc,"size_bytes":size,"num_files":nf,
            "partitions":parts,"error":None})
    except Exception as ex:
        print(f"Parse error: {ex}", flush=True)
        table_details.append({"name":tname,"full_name":f"{SRC_CATALOG}.{SRC_SCHEMA}.{tname}",
            "type":"UNKNOWN","location":"","size_bytes":0,"num_files":0,"partitions":[],"error":str(ex)})

# ── Step 4: DEEP CLONE SQL ────────────────────────────────────────────────
print(f"\n[Step 4] Generated DEEP CLONE SQL (path_resolution mode)", flush=True)
print("─"*72, flush=True)
total_bytes = total_files = 0
sorted_tables = sorted(table_details, key=lambda t: t["size_bytes"], reverse=True)
for t in sorted_tables:
    if t.get("error"): continue
    total_bytes += t["size_bytes"]
    total_files += t["num_files"]
    tgt_ref = f"`{TGT_CATALOG}`.`{SRC_SCHEMA}`.`{t['name']}`"
    print(f"\n  -- {t['full_name']} ({t['size_bytes']/1e6:.2f} MB, {t['num_files']} files, partitions={t['partitions']})")
    print(f"  CREATE OR REPLACE TABLE {tgt_ref}")
    print(f"  DEEP CLONE delta.`{t['location']}`;")
    print(flush=True)

# ── Step 5: Load balancer ─────────────────────────────────────────────────
print(f"\n[Step 5] Load Balancer: {MAX_CLUSTERS} clusters", flush=True)
print("─"*72, flush=True)
groups = [[] for _ in range(MAX_CLUSTERS)]
loads  = [0]  * MAX_CLUSTERS
for t in sorted_tables:
    if t.get("error"): continue
    lightest = min(range(MAX_CLUSTERS), key=lambda i: loads[i])
    groups[lightest].append(t)
    loads[lightest] += t["size_bytes"]
for i, (grp, ld) in enumerate(zip(groups, loads)):
    names = ", ".join(t["name"] for t in grp)
    print(f"  Cluster-{i+1}:  {ld/1e6:>8.2f} MB  →  {len(grp)} tables: [{names}]", flush=True)

# ── Step 6: Target catalog check ─────────────────────────────────────────
print(f"\n[Step 6] Target catalog '{TGT_CATALOG}' check…", flush=True)
cc = api_get(tgt_tok, TGT_URL, f"/api/2.1/unity-catalog/catalogs/{TGT_CATALOG}")
if "name" in cc:
    print(f"  ✓  Catalog exists  owner={cc.get('owner','?')}", flush=True)
else:
    print(f"  ✗  Catalog not found — will need CREATE CATALOG on target", flush=True)
    warnings.append(f"Target catalog '{TGT_CATALOG}' not yet created")

# ── Summary ───────────────────────────────────────────────────────────────
resolved = sum(1 for t in table_details if not t.get("error"))
print(f"""
{"═"*72}
  DRY-RUN SUMMARY
{"═"*72}
  Clone Mode       : path_resolution (DESCRIBE DETAIL via SQL Statement API)
  Source           : {SRC_URL}
  Source Scope     : {SRC_CATALOG}.{SRC_SCHEMA}
  Target           : {TGT_URL}
  Target Catalog   : {TGT_CATALOG}
{"─"*72}
  Tables found     : {len(src_tables)}
  Paths resolved   : {resolved} / {len(src_tables)}
  Total data       : {total_bytes/1e9:.4f} GB
  Total files      : {total_files:,}
  Cluster groups   : {MAX_CLUSTERS}
{"─"*72}
  STATUS : {"✓  READY TO CLONE" if not warnings else "⚠  READY WITH WARNINGS"}
{"═"*72}""", flush=True)
for w in warnings:
    print(f"  ⚠  {w}", flush=True)
