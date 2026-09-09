"""
full_setup_and_test.py
Complete end-to-end setup and test for DeepClone CrossRegion utility
using Azure_UC_Demo_region1 (target) and Azure_UC_Demo_region2 (source).

Creates:
  SOURCE  (adb-7405609899028573): azure_uc_demo_region2.deepclone_src.*  — 5 test tables
  TARGET  (adb-7405606418658510): azure_uc_demo_region1.deepclone_meta.* — onboarding + audit tables

Grants vivek.ravichandiran@databricks.com full access on all created objects.
"""
import os, requests, time, uuid, json
from datetime import datetime, timedelta, timezone

SRC_URL    = os.environ["AZ2AZ_SRC_URL"]   # adb-7405609899028573
SRC_CID    = os.environ["AZ2AZ_SRC_CID"]
SRC_SECRET = os.environ["AZ2AZ_SRC_SECRET"]
TGT_URL    = os.environ["AZ2AZ_TGT_URL"]   # adb-7405606418658510
TGT_CID    = os.environ["AZ2AZ_TGT_CID"]
TGT_SECRET = os.environ["AZ2AZ_TGT_SECRET"]

SRC_WH_ID  = "154fa0bd2b7f66fc"   # Serverless Starter Warehouse — source
TGT_WH_ID  = "8c47421fdc710056"   # Serverless Starter Warehouse — target

SRC_CAT  = "azure_uc_demo_region2"
SRC_SCH  = "deepclone_src"
TGT_CAT  = "azure_uc_demo_region1"
META_SCH  = "deepclone_meta"
VIVEK     = "vivek.ravichandiran@databricks.com"
NOW       = datetime.now(tz=timezone.utc)

# ── Auth helpers ──────────────────────────────────────────────────────────
def get_token(url, cid, secret):
    r = requests.post(f"{url}/oidc/v1/token",
        data={"grant_type":"client_credentials","client_id":cid,
              "client_secret":secret,"scope":"all-apis"}, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]

def fresh(is_src=True):
    if is_src:
        return get_token(SRC_URL, SRC_CID, SRC_SECRET)
    return get_token(TGT_URL, TGT_CID, TGT_SECRET)

def sql_exec(url, wh_id, sql, label, is_src=True):
    """Submit SQL and poll until complete. Returns result dict or None."""
    tok = fresh(is_src)
    resp = requests.post(f"{url}/api/2.0/sql/statements",
        headers={"Authorization":f"Bearer {tok}","Content-Type":"application/json"},
        json={"statement": sql, "warehouse_id": wh_id,
              "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"}, timeout=65)
    if resp.status_code != 200:
        print(f"  [FAIL-HTTP] {label}: {resp.status_code} {resp.text[:300]}", flush=True)
        return None
    d = resp.json()
    stmt_id = d.get("statement_id")
    if not stmt_id:
        print(f"  [FAIL-NOID] {label}: {d}", flush=True)
        return None
    state = d.get("status",{}).get("state","")
    if state == "SUCCEEDED":
        return d
    for _ in range(60):
        tok = fresh(is_src)
        p = requests.get(f"{url}/api/2.0/sql/statements/{stmt_id}",
            headers={"Authorization":f"Bearer {tok}"}, timeout=30).json()
        state = p.get("status",{}).get("state","")
        if state == "SUCCEEDED":
            return p
        if state in ("FAILED","CANCELED","CLOSED"):
            err = p.get("status",{}).get("error",{})
            print(f"  [FAIL] {label}: {err.get('message','')[:200]}", flush=True)
            return None
        time.sleep(2)
    print(f"  [TIMEOUT] {label}", flush=True)
    return None

def exe(url, wh_id, sql, label, is_src=True):
    print(f"  → {label}… ", end="", flush=True)
    r = sql_exec(url, wh_id, sql, label, is_src)
    if r is not None:
        print("OK", flush=True)
    return r

def query(url, wh_id, sql, label, is_src=True):
    r = sql_exec(url, wh_id, sql, label, is_src)
    if r and r.get("result",{}).get("data_array"):
        cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in r["result"]["data_array"]]
    return []

def start_warehouse(url, wh_id, name, is_src):
    tok = fresh(is_src)
    requests.post(f"{url}/api/2.0/sql/warehouses/{wh_id}/start",
        headers={"Authorization":f"Bearer {tok}"}, timeout=30)
    for i in range(20):
        wh = requests.get(f"{url}/api/2.0/sql/warehouses/{wh_id}",
            headers={"Authorization":f"Bearer {fresh(is_src)}"}, timeout=20).json()
        if wh.get("state") == "RUNNING":
            print(f"  ✓ {name} RUNNING (attempt {i+1})", flush=True)
            return True
        print(f"  attempt {i+1}: {wh.get('state','?')}", flush=True)
        time.sleep(5)
    return False

B = "═"*72
D = "─"*72

print(flush=True)
print(B, flush=True)
print("  DEEPCLONE CROSSREGION — FULL SETUP & INTEGRATION TEST", flush=True)
print(f"  {NOW.strftime('%Y-%m-%d %H:%M:%S UTC')}", flush=True)
print(B, flush=True)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Start warehouses
# ═══════════════════════════════════════════════════════════════════════════
print("\n[STEP 1] Starting SQL Warehouses", flush=True)
print(D, flush=True)
start_warehouse(SRC_URL, SRC_WH_ID, f"SOURCE Serverless ({SRC_WH_ID})", is_src=True)
start_warehouse(TGT_URL, TGT_WH_ID, f"TARGET Serverless ({TGT_WH_ID})", is_src=False)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: Create SOURCE schema + test tables in azure_uc_demo_region2
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 2] SOURCE: Create {SRC_CAT}.{SRC_SCH} + test tables", flush=True)
print(D, flush=True)

exe(SRC_URL, SRC_WH_ID, f"CREATE SCHEMA IF NOT EXISTS {SRC_CAT}.{SRC_SCH}",
    f"CREATE SCHEMA {SRC_CAT}.{SRC_SCH}", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
CREATE TABLE IF NOT EXISTS {SRC_CAT}.{SRC_SCH}.patients (
  patient_id     BIGINT,
  first_name     STRING,
  last_name      STRING,
  date_of_birth  DATE,
  gender         STRING,
  insurance_id   STRING,
  region         STRING,
  created_at     TIMESTAMP
) USING DELTA COMMENT 'Patient master — DeepClone integration test'
""", f"CREATE TABLE {SRC_SCH}.patients", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
CREATE TABLE IF NOT EXISTS {SRC_CAT}.{SRC_SCH}.claims (
  claim_id       STRING,
  patient_id     BIGINT,
  provider_id    STRING,
  claim_date     DATE,
  diagnosis_code STRING,
  amount_billed  DOUBLE,
  amount_paid    DOUBLE,
  status         STRING,
  region         STRING
) USING DELTA COMMENT 'Claims records — DeepClone integration test'
""", f"CREATE TABLE {SRC_SCH}.claims", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
CREATE TABLE IF NOT EXISTS {SRC_CAT}.{SRC_SCH}.providers (
  provider_id   STRING,
  provider_name STRING,
  specialty     STRING,
  npi           STRING,
  state         STRING,
  active        BOOLEAN
) USING DELTA COMMENT 'Provider directory — DeepClone integration test'
""", f"CREATE TABLE {SRC_SCH}.providers", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
CREATE TABLE IF NOT EXISTS {SRC_CAT}.{SRC_SCH}.medications (
  medication_id   STRING,
  patient_id      BIGINT,
  drug_name       STRING,
  ndc_code        STRING,
  quantity        INT,
  days_supply     INT,
  fill_date       DATE,
  prescriber_id   STRING
) USING DELTA COMMENT 'Medication dispense records — DeepClone integration test'
""", f"CREATE TABLE {SRC_SCH}.medications", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
CREATE TABLE IF NOT EXISTS {SRC_CAT}.{SRC_SCH}.eligibility (
  member_id      STRING,
  patient_id     BIGINT,
  plan_id        STRING,
  effective_from DATE,
  effective_to   DATE,
  coverage_type  STRING,
  premium        DOUBLE
) USING DELTA COMMENT 'Member eligibility — DeepClone integration test'
""", f"CREATE TABLE {SRC_SCH}.eligibility", is_src=True)

# Insert sample rows
print("\n  Inserting sample rows…", flush=True)
exe(SRC_URL, SRC_WH_ID, f"""
INSERT INTO {SRC_CAT}.{SRC_SCH}.patients VALUES
  (1001,'Alice','Johnson','1985-04-12','F','INS-001','East',current_timestamp()),
  (1002,'Bob','Smith','1972-09-23','M','INS-002','West',current_timestamp()),
  (1003,'Clara','Davis','1990-01-30','F','INS-003','Central',current_timestamp()),
  (1004,'Daniel','Martinez','1965-07-08','M','INS-004','East',current_timestamp()),
  (1005,'Eva','Wilson','1988-11-15','F','INS-005','South',current_timestamp()),
  (1006,'Frank','Brown','1955-03-22','M','INS-006','North',current_timestamp()),
  (1007,'Grace','Taylor','1993-06-05','F','INS-007','West',current_timestamp()),
  (1008,'Henry','Anderson','1979-12-18','M','INS-008','Central',current_timestamp())
""", "INSERT patients rows", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
INSERT INTO {SRC_CAT}.{SRC_SCH}.claims VALUES
  (uuid(),'1001','PRV-001','2026-01-15','Z00.00',850.00,650.00,'PAID','East'),
  (uuid(),'1002','PRV-002','2026-02-10','I10',1200.00,900.00,'PAID','West'),
  (uuid(),'1003','PRV-003','2026-03-22','E11.9',450.00,320.00,'PENDING','Central'),
  (uuid(),'1004','PRV-001','2026-04-05','M54.5',2100.00,1680.00,'PAID','East'),
  (uuid(),'1005','PRV-004','2026-05-18','J06.9',340.00,0.00,'DENIED','South'),
  (uuid(),'1006','PRV-002','2026-06-01','K21.0',780.00,600.00,'PAID','North'),
  (uuid(),'1007','PRV-005','2026-06-28','F32.9',1560.00,1250.00,'PAID','West'),
  (uuid(),'1008','PRV-003','2026-07-01','Z12.31',3200.00,2800.00,'PAID','Central'),
  (uuid(),'1001','PRV-004','2026-07-05','Z00.00',720.00,550.00,'PAID','East'),
  (uuid(),'1003','PRV-001','2026-07-08','E11.9',980.00,740.00,'PENDING','Central')
""", "INSERT claims rows", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
INSERT INTO {SRC_CAT}.{SRC_SCH}.providers VALUES
  ('PRV-001','St. Mary Medical Center','Internal Medicine','1234567890','CA',true),
  ('PRV-002','Lakeside Cardiology','Cardiology','2345678901','NY',true),
  ('PRV-003','Central Hospital','General Surgery','3456789012','TX',true),
  ('PRV-004','Sunrise Family Clinic','Family Medicine','4567890123','FL',true),
  ('PRV-005','Mind & Wellness Center','Psychiatry','5678901234','WA',true)
""", "INSERT providers rows", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
INSERT INTO {SRC_CAT}.{SRC_SCH}.medications VALUES
  ('MED-001',1001,'Metformin','00006-3141-68',90,30,'2026-01-20','PRV-003'),
  ('MED-002',1002,'Lisinopril','00071-0222-24',30,30,'2026-02-15','PRV-002'),
  ('MED-003',1003,'Atorvastatin','00071-0155-23',90,90,'2026-03-25','PRV-001'),
  ('MED-004',1004,'Ibuprofen','49035-648-02',60,30,'2026-04-10','PRV-001'),
  ('MED-005',1005,'Amoxicillin','00093-4155-01',21,10,'2026-05-20','PRV-004'),
  ('MED-006',1007,'Sertraline','00173-0483-02',30,30,'2026-07-01','PRV-005')
""", "INSERT medications rows", is_src=True)

exe(SRC_URL, SRC_WH_ID, f"""
INSERT INTO {SRC_CAT}.{SRC_SCH}.eligibility VALUES
  ('MBR-1001',1001,'PLAN-GOLD','2026-01-01','2026-12-31','PPO',450.00),
  ('MBR-1002',1002,'PLAN-SILVER','2026-01-01','2026-12-31','HMO',320.00),
  ('MBR-1003',1003,'PLAN-GOLD','2026-04-01','2027-03-31','PPO',450.00),
  ('MBR-1004',1004,'PLAN-BRONZE','2026-01-01','2026-12-31','HMO',220.00),
  ('MBR-1005',1005,'PLAN-SILVER','2026-06-01','2027-05-31','PPO',320.00),
  ('MBR-1006',1006,'PLAN-GOLD','2026-01-01','2026-12-31','PPO',450.00),
  ('MBR-1007',1007,'PLAN-PREMIUM','2026-01-01','2026-12-31','EPO',580.00),
  ('MBR-1008',1008,'PLAN-SILVER','2026-03-01','2027-02-28','HMO',320.00)
""", "INSERT eligibility rows", is_src=True)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: Grant vivek full access on SOURCE objects
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 3] SOURCE: Grant VIVEK full access on {SRC_CAT}.{SRC_SCH}", flush=True)
print(D, flush=True)
for grant_sql, lbl in [
    (f"GRANT USE CATALOG ON CATALOG {SRC_CAT} TO `{VIVEK}`", f"GRANT USE CATALOG {SRC_CAT}"),
    (f"GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA {SRC_CAT}.{SRC_SCH} TO `{VIVEK}`", f"GRANT schema privs"),
    (f"GRANT SELECT, MODIFY ON TABLE {SRC_CAT}.{SRC_SCH}.patients TO `{VIVEK}`", "GRANT patients"),
    (f"GRANT SELECT, MODIFY ON TABLE {SRC_CAT}.{SRC_SCH}.claims TO `{VIVEK}`", "GRANT claims"),
    (f"GRANT SELECT, MODIFY ON TABLE {SRC_CAT}.{SRC_SCH}.providers TO `{VIVEK}`", "GRANT providers"),
    (f"GRANT SELECT, MODIFY ON TABLE {SRC_CAT}.{SRC_SCH}.medications TO `{VIVEK}`", "GRANT medications"),
    (f"GRANT SELECT, MODIFY ON TABLE {SRC_CAT}.{SRC_SCH}.eligibility TO `{VIVEK}`", "GRANT eligibility"),
]:
    exe(SRC_URL, SRC_WH_ID, grant_sql, lbl, is_src=True)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: Create TARGET metadata schema + tables in azure_uc_demo_region1
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 4] TARGET: Create {TGT_CAT}.{META_SCH} + metadata tables", flush=True)
print(D, flush=True)

exe(TGT_URL, TGT_WH_ID, f"CREATE SCHEMA IF NOT EXISTS {TGT_CAT}.{META_SCH}",
    f"CREATE SCHEMA {TGT_CAT}.{META_SCH}", is_src=False)

exe(TGT_URL, TGT_WH_ID, f"""
CREATE TABLE IF NOT EXISTS {TGT_CAT}.{META_SCH}.clone_onboarding (
  onboarding_id  STRING    NOT NULL,
  request_name   STRING    NOT NULL,
  scope_mode     STRING    NOT NULL COMMENT 'catalog | schema | table',
  clone_mode     STRING    NOT NULL COMMENT 'path_resolution | delta_share',
  src_catalog    STRING    NOT NULL,
  src_schema     STRING,
  src_table      STRING,
  tgt_catalog    STRING    NOT NULL,
  tgt_schema     STRING,
  priority       INT       NOT NULL COMMENT '1=High 2=Medium 3=Low',
  status         STRING    NOT NULL COMMENT 'PENDING|IN_PROGRESS|COMPLETED|FAILED|SKIPPED',
  requested_by   STRING    NOT NULL,
  requested_at   TIMESTAMP NOT NULL,
  scheduled_for  TIMESTAMP,
  execution_id   STRING,
  completed_at   TIMESTAMP,
  notes          STRING,
  tags           MAP<STRING,STRING>
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.autoOptimize.optimizeWrite'='true')
COMMENT 'DeepClone CrossRegion — self-service onboarding queue'
""", f"CREATE TABLE {META_SCH}.clone_onboarding", is_src=False)

exe(TGT_URL, TGT_WH_ID, f"""
CREATE TABLE IF NOT EXISTS {TGT_CAT}.{META_SCH}.deep_clone_audit_log (
  execution_id       STRING    NOT NULL,
  onboarding_id      STRING,
  batch_id           STRING,
  batch_sequence     INT,
  retry_attempt      INT,
  run_timestamp      TIMESTAMP NOT NULL,
  table_name         STRING    NOT NULL,
  src_catalog        STRING,
  src_schema         STRING,
  tgt_catalog        STRING,
  tgt_schema         STRING,
  table_type         STRING,
  clone_mode         STRING,
  src_location       STRING,
  tgt_location       STRING,
  records_cloned     BIGINT,
  files_copied       BIGINT,
  size_bytes         BIGINT,
  throughput_mbps    DOUBLE,
  start_time         TIMESTAMP,
  end_time           TIMESTAMP,
  duration_seconds   BIGINT,
  cluster_id         STRING,
  status             STRING    NOT NULL,
  error_message      STRING,
  error_stack_trace  STRING,
  dry_run            STRING
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.autoOptimize.optimizeWrite'='true')
COMMENT 'DeepClone CrossRegion — per-table clone execution audit with performance metrics'
""", f"CREATE TABLE {META_SCH}.deep_clone_audit_log", is_src=False)

# Also create target schema for cloned data
exe(TGT_URL, TGT_WH_ID, f"CREATE SCHEMA IF NOT EXISTS {TGT_CAT}.deepclone_tgt",
    f"CREATE SCHEMA {TGT_CAT}.deepclone_tgt", is_src=False)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: Populate onboarding table with sample data
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 5] TARGET: Populate {META_SCH}.clone_onboarding with sample requests", flush=True)
print(D, flush=True)

def ts(offset_h=0):
    return (NOW + timedelta(hours=offset_h)).strftime("'%Y-%m-%d %H:%M:%S'")

OB_ROWS = [
    # (name, scope, mode, src_cat, src_sch, src_tbl, tgt_cat, tgt_sch, pri, status, by, req_at, exec_id, comp_at, notes)
    ("Region2→Region1 Full Schema DR", "schema", "path_resolution",
     SRC_CAT, SRC_SCH, None, TGT_CAT, "deepclone_tgt", 1, "COMPLETED",
     VIVEK, ts(-3), "exec-20260709-0930", ts(-2),
     "Initial full schema disaster-recovery copy"),

    ("patients Table — Targeted Refresh", "table", "path_resolution",
     SRC_CAT, SRC_SCH, f"{SRC_CAT}.{SRC_SCH}.patients", TGT_CAT, "deepclone_tgt", 1, "COMPLETED",
     VIVEK, ts(-2), "exec-20260709-1015", ts(-1.5),
     "Refresh after patient master update"),

    ("claims Table — Targeted Refresh", "table", "path_resolution",
     SRC_CAT, SRC_SCH, f"{SRC_CAT}.{SRC_SCH}.claims", TGT_CAT, "deepclone_tgt", 1, "COMPLETED",
     VIVEK, ts(-2), "exec-20260709-1015", ts(-1.5),
     "Refresh after weekly claims batch load"),

    ("providers Table Sync", "table", "path_resolution",
     SRC_CAT, SRC_SCH, f"{SRC_CAT}.{SRC_SCH}.providers", TGT_CAT, "deepclone_tgt", 2, "COMPLETED",
     VIVEK, ts(-1.5), "exec-20260709-1015", ts(-1),
     "Weekly provider directory sync"),

    ("medications Refresh", "table", "path_resolution",
     SRC_CAT, SRC_SCH, f"{SRC_CAT}.{SRC_SCH}.medications", TGT_CAT, "deepclone_tgt", 2, "IN_PROGRESS",
     VIVEK, ts(-0.5), "exec-20260709-1058", None,
     "Daily medications dispense refresh"),

    ("eligibility Refresh", "table", "path_resolution",
     SRC_CAT, SRC_SCH, f"{SRC_CAT}.{SRC_SCH}.eligibility", TGT_CAT, "deepclone_tgt", 2, "IN_PROGRESS",
     VIVEK, ts(-0.5), "exec-20260709-1058", None,
     "Daily eligibility refresh"),

    ("Full Schema Catalog Backup via Delta Share", "schema", "delta_share",
     SRC_CAT, SRC_SCH, None, TGT_CAT, "deepclone_tgt", 3, "PENDING",
     VIVEK, ts(0), None, None,
     "Weekly catalog-level backup using Delta Sharing protocol"),

    ("azure_uc_demo_region2 Full Catalog DR", "catalog", "delta_share",
     SRC_CAT, None, None, TGT_CAT, None, 3, "PENDING",
     VIVEK, ts(0.1), None, None,
     "Full catalog DR — pending Delta Sharing provider setup"),
]

for row in OB_ROWS:
    name, scope, mode, scat, ssch, stbl, tcat, tsch, pri, status, by, req_at, exec_id, comp_at, notes = row
    ssch_val  = f"'{ssch}'"  if ssch  else "NULL"
    stbl_val  = f"'{stbl}'"  if stbl  else "NULL"
    tsch_val  = f"'{tsch}'"  if tsch  else "NULL"
    eid_val   = f"'{exec_id}'" if exec_id else "NULL"
    cat_val   = f"TIMESTAMP {comp_at}" if comp_at else "NULL"
    exe(TGT_URL, TGT_WH_ID, f"""
    INSERT INTO {TGT_CAT}.{META_SCH}.clone_onboarding VALUES (
      '{str(uuid.uuid4())}', '{name}', '{scope}', '{mode}',
      '{scat}', {ssch_val}, {stbl_val},
      '{tcat}', {tsch_val},
      {pri}, '{status}', '{by}',
      TIMESTAMP {req_at}, NULL,
      {eid_val}, {cat_val},
      '{notes}',
      map('env','dr','catalog','{scat}','owner','{by.split('@')[0]}')
    )""", f"INSERT onboarding: {name[:50]}", is_src=False)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6: Populate audit log with sample batch metrics
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 6] TARGET: Populate {META_SCH}.deep_clone_audit_log with batch metrics", flush=True)
print(D, flush=True)

# Get OB IDs we just inserted so we can reference them
ob_ids = query(TGT_URL, TGT_WH_ID,
    f"SELECT onboarding_id, execution_id, request_name FROM {TGT_CAT}.{META_SCH}.clone_onboarding ORDER BY requested_at",
    "fetch onboarding IDs", is_src=False)
ob_by_exec = {}
for r in ob_ids:
    eid = r.get("execution_id") or ""
    if eid and eid not in ob_by_exec:
        ob_by_exec[eid] = r.get("onboarding_id","")

AUDIT_ROWS = [
    # exec_id, ob_exec, batch, seq, retry, table_fqn, scat, ssch, tcat, tsch, ttype, cmode,
    # size_b, recs, files, tput, dur_s, cluster, status, err_msg
    ("exec-20260709-0930", "exec-20260709-0930", "batch-001", 1, 0,
     f"{SRC_CAT}.{SRC_SCH}.patients", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 2621440, 8, 1, 3.2, 1, "job-cluster-01", "SUCCESS", ""),
    ("exec-20260709-0930", "exec-20260709-0930", "batch-001", 2, 0,
     f"{SRC_CAT}.{SRC_SCH}.claims", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 4194304, 10, 1, 3.8, 1, "job-cluster-01", "SUCCESS", ""),
    ("exec-20260709-0930", "exec-20260709-0930", "batch-001", 3, 0,
     f"{SRC_CAT}.{SRC_SCH}.providers", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 1048576, 5, 1, 2.9, 0, "job-cluster-02", "SUCCESS", ""),
    ("exec-20260709-0930", "exec-20260709-0930", "batch-001", 4, 0,
     f"{SRC_CAT}.{SRC_SCH}.medications", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 786432, 6, 1, 2.7, 0, "job-cluster-02", "SUCCESS", ""),
    ("exec-20260709-0930", "exec-20260709-0930", "batch-001", 5, 0,
     f"{SRC_CAT}.{SRC_SCH}.eligibility", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 1572864, 8, 1, 3.1, 1, "job-cluster-02", "SUCCESS", ""),
    ("exec-20260709-1015", "exec-20260709-1015", "batch-002", 1, 0,
     f"{SRC_CAT}.{SRC_SCH}.patients", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 2621440, 8, 1, 4.1, 1, "job-cluster-01", "SUCCESS", ""),
    ("exec-20260709-1015", "exec-20260709-1015", "batch-002", 2, 0,
     f"{SRC_CAT}.{SRC_SCH}.claims", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 4194304, 10, 1, 4.5, 1, "job-cluster-01", "SUCCESS", ""),
    ("exec-20260709-1015", "exec-20260709-1015", "batch-002", 3, 0,
     f"{SRC_CAT}.{SRC_SCH}.providers", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 1048576, 5, 1, 3.8, 0, "job-cluster-02", "SUCCESS", ""),
    ("exec-20260709-1058", "exec-20260709-1058", "batch-003", 1, 0,
     f"{SRC_CAT}.{SRC_SCH}.medications", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 786432, 6, 1, 3.5, 0, "job-cluster-01", "SUCCESS", ""),
    ("exec-20260709-1058", "exec-20260709-1058", "batch-003", 2, 0,
     f"{SRC_CAT}.{SRC_SCH}.eligibility", SRC_CAT, SRC_SCH, TGT_CAT, "deepclone_tgt",
     "MANAGED", "path_resolution", 1572864, 8, 1, 3.6, 1, "job-cluster-01", "SUCCESS", ""),
]

for r in AUDIT_ROWS:
    (exec_id, ob_exec, batch, seq, retry, tname, scat, ssch, tcat, tsch, ttype, cmode,
     size_b, recs, files, tput, dur, cluster, status, err_msg) = r
    ob_id_val = f"'{ob_by_exec.get(ob_exec, str(uuid.uuid4()))}'" if ob_by_exec.get(ob_exec) else "NULL"
    err_val = f"'{err_msg}'" if err_msg else "NULL"
    start_ts = (NOW + timedelta(hours=-3, minutes=seq)).strftime("'%Y-%m-%d %H:%M:%S'")
    end_ts   = (NOW + timedelta(hours=-3, minutes=seq, seconds=dur+1)).strftime("'%Y-%m-%d %H:%M:%S'")
    src_loc  = f"abfss://unity-catalog-storage@dbstorageyrl36oejavi7w.dfs.core.windows.net/7405615611652340/{SRC_CAT}/__unitystorage/tables/..."
    tgt_loc  = f"abfss://unity-catalog-storage@dbstoraget5iivv4ym24pw.dfs.core.windows.net/7405606418658510/{TGT_CAT}/__unitystorage/tables/..."
    exe(TGT_URL, TGT_WH_ID, f"""
    INSERT INTO {TGT_CAT}.{META_SCH}.deep_clone_audit_log VALUES (
      '{exec_id}', {ob_id_val}, '{batch}', {seq}, {retry},
      current_timestamp(),
      '{tname}', '{scat}', '{ssch}', '{tcat}', '{tsch}',
      '{ttype}', '{cmode}',
      '{src_loc}', '{tgt_loc}',
      {recs}, {files}, {size_b}, {tput},
      TIMESTAMP {start_ts}, TIMESTAMP {end_ts}, {dur},
      '{cluster}', '{status}',
      {err_val}, NULL, 'false'
    )""", f"INSERT audit: {tname.split('.')[-1]} ({exec_id[-4:]})", is_src=False)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7: Grant VIVEK full access on TARGET objects
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 7] TARGET: Grant VIVEK full access on {TGT_CAT}.{META_SCH}", flush=True)
print(D, flush=True)
for grant_sql, lbl in [
    (f"GRANT USE CATALOG ON CATALOG {TGT_CAT} TO `{VIVEK}`", f"GRANT USE CATALOG {TGT_CAT}"),
    (f"GRANT ALL PRIVILEGES ON SCHEMA {TGT_CAT}.{META_SCH} TO `{VIVEK}`", f"GRANT ALL on {META_SCH}"),
    (f"GRANT ALL PRIVILEGES ON TABLE {TGT_CAT}.{META_SCH}.clone_onboarding TO `{VIVEK}`", "GRANT clone_onboarding"),
    (f"GRANT ALL PRIVILEGES ON TABLE {TGT_CAT}.{META_SCH}.deep_clone_audit_log TO `{VIVEK}`", "GRANT deep_clone_audit_log"),
    (f"GRANT ALL PRIVILEGES ON SCHEMA {TGT_CAT}.deepclone_tgt TO `{VIVEK}`", "GRANT deepclone_tgt schema"),
]:
    exe(TGT_URL, TGT_WH_ID, grant_sql, lbl, is_src=False)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8: Live DESCRIBE DETAIL on source tables → generate clone SQL
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 8] LIVE DRY-RUN: DESCRIBE DETAIL on {SRC_CAT}.{SRC_SCH} tables", flush=True)
print(D, flush=True)

tables_resp = requests.get(
    f"{SRC_URL}/api/2.1/unity-catalog/tables?catalog_name={SRC_CAT}&schema_name={SRC_SCH}&max_results=50",
    headers={"Authorization":f"Bearer {fresh(is_src=True)}"}, timeout=30).json()
source_tables = tables_resp.get("tables", [])
print(f"  Tables in {SRC_CAT}.{SRC_SCH}: {len(source_tables)}", flush=True)

table_details = []
for t in source_tables:
    fqn = t["full_name"]
    backtick_fqn = "`" + fqn.replace(".", "`.`") + "`"
    r = sql_exec(SRC_URL, SRC_WH_ID, f"DESCRIBE DETAIL {backtick_fqn}",
                 f"DESCRIBE DETAIL {fqn}", is_src=True)
    if r and r.get("result",{}).get("data_array"):
        cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
        row  = dict(zip(cols, r["result"]["data_array"][0]))
        size_b  = int(row.get("sizeInBytes") or 0)
        n_files = int(row.get("numFiles") or 0)
        loc     = row.get("location","")
        table_details.append({"fqn":fqn,"size_b":size_b,"n_files":n_files,"loc":loc})
        print(f"  ✓ {fqn:<60} {size_b/1024:.1f} KB  {n_files} file(s)", flush=True)
    else:
        print(f"  ✗ {fqn}  [DESCRIBE DETAIL failed]", flush=True)

# Load-balance across 2 clusters
buckets = [[], []]
b_sz = [0, 0]
for td in sorted(table_details, key=lambda x: -x["size_b"]):
    i = b_sz.index(min(b_sz))
    buckets[i].append(td)
    b_sz[i] += td["size_b"]

print(f"\n  Load-balanced across 2 clusters:", flush=True)
for ci, (bkt, sz) in enumerate(zip(buckets, b_sz)):
    print(f"  Cluster {ci+1} ({sz/1024:.1f} KB): {[td['fqn'].split('.')[-1] for td in bkt]}", flush=True)

print(f"\n  Generated DEEP CLONE SQL statements:", flush=True)
clone_sqls = []
for td in table_details:
    tgt_tbl = f"{TGT_CAT}.deepclone_tgt.{td['fqn'].split('.')[-1]}"
    sql = f"CREATE OR REPLACE TABLE `{tgt_tbl}` DEEP CLONE delta.`{td['loc']}`;"
    clone_sqls.append(sql)
    print(f"  {sql[:110]}", flush=True)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9: Verification queries
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n[STEP 9] VERIFICATION QUERIES", flush=True)
print(D, flush=True)

# Source row counts
print(f"\n  Source table row counts ({SRC_CAT}.{SRC_SCH}):", flush=True)
for tbl in ["patients","claims","providers","medications","eligibility"]:
    rows = query(SRC_URL, SRC_WH_ID, f"SELECT COUNT(*) AS cnt FROM {SRC_CAT}.{SRC_SCH}.{tbl}",
                 f"count {tbl}", is_src=True)
    cnt = rows[0]["cnt"] if rows else "?"
    print(f"    {tbl:<20} {cnt} rows", flush=True)

# Target onboarding table
print(f"\n  Onboarding queue ({TGT_CAT}.{META_SCH}.clone_onboarding):", flush=True)
ob_summary = query(TGT_URL, TGT_WH_ID,
    f"SELECT status, COUNT(*) AS cnt FROM {TGT_CAT}.{META_SCH}.clone_onboarding GROUP BY status ORDER BY cnt DESC",
    "onboarding summary", is_src=False)
for r in ob_summary:
    print(f"    {r['status']:<15} {r['cnt']} requests", flush=True)

# Target audit performance
print(f"\n  Audit log batch performance ({TGT_CAT}.{META_SCH}.deep_clone_audit_log):", flush=True)
perf = query(TGT_URL, TGT_WH_ID, f"""
  SELECT
    execution_id,
    COUNT(*)                               AS tables,
    SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS succeeded,
    ROUND(SUM(size_bytes)/1024.0/1024, 3)  AS total_mb,
    SUM(records_cloned)                    AS total_records,
    ROUND(AVG(throughput_mbps), 2)         AS avg_mbps,
    SUM(duration_seconds)                  AS total_dur_s
  FROM {TGT_CAT}.{META_SCH}.deep_clone_audit_log
  GROUP BY execution_id ORDER BY execution_id
""", "perf summary", is_src=False)
print(f"    {'EXEC_ID':<25} {'TABLES':>6} {'SUCCESS':>7} {'MB':>8} {'RECORDS':>9} {'AVG_MBPS':>9} {'DUR_S':>6}", flush=True)
for r in perf:
    print(f"    {r['execution_id']:<25} {r['tables']:>6} {r['succeeded']:>7} "
          f"{r['total_mb']:>8} {r['total_records']:>9} {r['avg_mbps']:>9} {r['total_dur_s']:>6}", flush=True)

total_mb = sum(td["size_b"] for td in table_details) / 1024 / 1024

# ═══════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
print(flush=True)
print(B, flush=True)
print("  SETUP COMPLETE — WORKSPACE REFERENCES", flush=True)
print(B, flush=True)
print(f"\n  SOURCE WORKSPACE: {SRC_URL}", flush=True)
print(f"  {'Table':<45} {'Explore URL'}", flush=True)
print(f"  {D[:45]} {D[:60]}", flush=True)
for tbl in ["patients","claims","providers","medications","eligibility"]:
    fq = f"{SRC_CAT}.{SRC_SCH}.{tbl}"
    url = f"{SRC_URL}/explore/data/{SRC_CAT}/{SRC_SCH}/{tbl}"
    print(f"  {fq:<45} {url}", flush=True)

print(f"\n  TARGET WORKSPACE: {TGT_URL}", flush=True)
print(f"  {'Table':<55} {'Explore URL'}", flush=True)
print(f"  {D[:55]} {D[:60]}", flush=True)
for tbl in ["clone_onboarding","deep_clone_audit_log"]:
    fq = f"{TGT_CAT}.{META_SCH}.{tbl}"
    url = f"{TGT_URL}/explore/data/{TGT_CAT}/{META_SCH}/{tbl}"
    print(f"  {fq:<55} {url}", flush=True)

print(f"\n  DRY-RUN SUMMARY:", flush=True)
print(f"    Clone Mode    : path_resolution (DESCRIBE DETAIL)", flush=True)
print(f"    Source scope  : {SRC_CAT}.{SRC_SCH} ({len(table_details)} tables)", flush=True)
print(f"    Target scope  : {TGT_CAT}.deepclone_tgt", flush=True)
print(f"    Total data    : {total_mb:.4f} MB", flush=True)
print(f"    SQL statements: {len(clone_sqls)} generated", flush=True)
print(f"    STATUS        : ✓ READY TO CLONE", flush=True)
print(B, flush=True)
