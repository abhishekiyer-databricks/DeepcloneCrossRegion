#!/usr/bin/env python3
"""
create_ril_tgt_catalogs.py
============================
Creates 3 new Unity Catalog catalogs on adb-7405616318078204.4.azuredatabricks.net,
using the EXACT SAME DDL shape as create_ril_bulk_catalogs.py (ril_bulk_02/03/04),
but with catalogs prefixed `ril_tgt_` instead of `ril_bulk_`. Schema names and
table names are identical to the ril_bulk_* set.

  - Catalogs: ril_tgt_02, ril_tgt_03, ril_tgt_04
  - 5 schemas per catalog: finance, iot, marketing, ops, sales
  - 26 managed Delta tables per schema (13 dim_*, 13 fact_*) = 130 tables/catalog
  - Same 8-column schema as ril_bulk.iot.dim_iot_00

No grants are applied in this script (not requested for this batch).

Auth: PAT token read from DBX_TOKEN env var (never hardcoded / committed).
"""

import os
import sys
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

DBX_HOST = os.environ.get("DBX_HOST", "https://adb-7405616318078204.4.azuredatabricks.net")
DBX_TOKEN = os.environ.get("DBX_TOKEN")
if not DBX_TOKEN:
    print("ERROR: set DBX_TOKEN env var before running this script.", file=sys.stderr)
    sys.exit(1)

WAREHOUSE_ID = os.environ.get("DBX_WAREHOUSE_ID", "5fe1692f119e2528")  # Serverless Starter Warehouse

NEW_CATALOGS = ["ril_tgt_02", "ril_tgt_03", "ril_tgt_04"]
SCHEMAS = ["finance", "iot", "marketing", "ops", "sales"]

TABLE_DDL_COLUMNS = """(
  row_id        BIGINT,
  business_key  STRING,
  name          STRING,
  category_id   INT,
  amount        DECIMAL(12,2),
  is_active     BOOLEAN,
  load_date     DATE,
  ingested_at   TIMESTAMP
) USING DELTA"""

H = {"Authorization": f"Bearer {DBX_TOKEN}", "Content-Type": "application/json"}


def sql_exec(stmt, label, timeout=120):
    r = requests.post(
        f"{DBX_HOST}/api/2.0/sql/statements",
        headers=H,
        json={"statement": stmt, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "0s"},
        timeout=30,
    )
    r.raise_for_status()
    sid = r.json()["statement_id"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = requests.get(f"{DBX_HOST}/api/2.0/sql/statements/{sid}", headers=H, timeout=30).json()
        state = d.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            return True, d
        if state in ("FAILED", "CANCELED", "CLOSED"):
            return False, d.get("status", {}).get("error", {})
        time.sleep(1.5)
    return False, {"message": "timeout"}


def run(stmt, label):
    ok, info = sql_exec(stmt, label)
    if ok:
        print(f"  OK   {label}")
    else:
        print(f"  FAIL {label}: {json.dumps(info)[:300]}")
    return ok


def start_warehouse():
    print(f"Starting warehouse {WAREHOUSE_ID} ...")
    requests.post(f"{DBX_HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}/start", headers=H, timeout=30)
    for i in range(20):
        wh = requests.get(f"{DBX_HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}", headers=H, timeout=20).json()
        state = wh.get("state")
        print(f"  attempt {i+1}: {state}")
        if state == "RUNNING":
            return True
        time.sleep(5)
    return False


def main():
    start_warehouse()

    print("\n=== STEP 1: Create catalogs ===")
    STORAGE_BASE = "abfss://unity-catalog-storage@dbstorageziwqzkb2dgooo.dfs.core.windows.net/7405616318078204"
    for cat in NEW_CATALOGS:
        run(
            f"CREATE CATALOG IF NOT EXISTS `{cat}` "
            f"MANAGED LOCATION '{STORAGE_BASE}/{cat}' "
            f"COMMENT 'Bulk tables for load-balancer, threading and state testing (extra capacity catalog)'",
            f"CREATE CATALOG {cat}",
        )

    print("\n=== STEP 2: Create schemas ===")
    schema_jobs = [(cat, sch) for cat in NEW_CATALOGS for sch in SCHEMAS]
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {
            ex.submit(run, f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`", f"CREATE SCHEMA {cat}.{sch}"): (cat, sch)
            for cat, sch in schema_jobs
        }
        for f in as_completed(futs):
            f.result()

    print("\n=== STEP 3: Create tables (130 per catalog = 390 total) ===")
    table_jobs = []
    for cat in NEW_CATALOGS:
        for sch in SCHEMAS:
            for i in range(1, 14):  # 13 dim_ tables
                table_jobs.append((cat, sch, f"dim_{sch}_{i:02d}"))
            for i in range(1, 14):  # 13 fact_ tables
                table_jobs.append((cat, sch, f"fact_{sch}_{i:02d}"))

    print(f"  Total tables to create: {len(table_jobs)}")

    def create_table(cat, sch, tbl):
        stmt = f"CREATE TABLE IF NOT EXISTS `{cat}`.`{sch}`.`{tbl}` {TABLE_DDL_COLUMNS}"
        return run(stmt, f"{cat}.{sch}.{tbl}")

    success = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(create_table, cat, sch, tbl): (cat, sch, tbl) for cat, sch, tbl in table_jobs}
        for f in as_completed(futs):
            if f.result():
                success += 1
            else:
                failed += 1

    print(f"\n  Tables created: {success} succeeded, {failed} failed")

    print("\n=== STEP 4: Verification ===")
    for cat in NEW_CATALOGS:
        for sch in SCHEMAS:
            ok, d = sql_exec(f"SHOW TABLES IN `{cat}`.`{sch}`", f"SHOW TABLES {cat}.{sch}")
            n = d.get("result", {}).get("row_count", 0) if ok else -1
            print(f"  {cat}.{sch}: {n} tables")

    print("\nDONE.")


if __name__ == "__main__":
    main()
