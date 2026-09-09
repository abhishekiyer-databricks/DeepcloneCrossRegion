#!/usr/bin/env python3
"""
load_sample_data_ril_bulk_02.py
================================
Loads synthetic sample rows into all 130 managed Delta tables in the
`ril_bulk_02` catalog (5 schemas x 26 tables) so that the DeepClone
INVENTORY / DEEP_CLONE pipeline has real (non-zero) data to work with.

Uses a single `INSERT INTO ... SELECT ... FROM range(N)` statement per
table (generated server-side by Spark SQL) — no client-side row shipping.

Auth: PAT token read from DBX_TOKEN env var (never hardcoded / committed).
"""

import os
import sys
import time
import json
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

DBX_HOST = os.environ.get("DBX_HOST", "https://adb-7405616318078204.4.azuredatabricks.net")
DBX_TOKEN = os.environ.get("DBX_TOKEN")
if not DBX_TOKEN:
    print("ERROR: set DBX_TOKEN env var before running this script.", file=sys.stderr)
    sys.exit(1)

WAREHOUSE_ID = os.environ.get("DBX_WAREHOUSE_ID", "5fe1692f119e2528")  # Serverless Starter Warehouse

CATALOG = "ril_bulk_02"
SCHEMAS = ["finance", "iot", "marketing", "ops", "sales"]
ROWS_PER_TABLE_MIN = 200
ROWS_PER_TABLE_MAX = 2000

H = {"Authorization": f"Bearer {DBX_TOKEN}", "Content-Type": "application/json"}


def sql_exec(stmt, timeout=180):
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
    ok, info = sql_exec(stmt)
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


def insert_stmt(cat, sch, tbl, n_rows):
    return f"""
    INSERT INTO `{cat}`.`{sch}`.`{tbl}`
    SELECT
      id                                                          AS row_id,
      concat('BK-{sch.upper()}-', lpad(cast(id AS STRING), 10, '0')) AS business_key,
      concat('{tbl} record ', cast(id AS STRING))                  AS name,
      cast(id % 20 AS INT)                                         AS category_id,
      cast(rand() * 100000 AS DECIMAL(12,2))                       AS amount,
      (id % 5 != 0)                                                AS is_active,
      date_add(DATE'2024-01-01', cast(id % 365 AS INT))            AS load_date,
      current_timestamp()                                         AS ingested_at
    FROM range({n_rows}) AS t(id)
    """


def main():
    start_warehouse()

    table_jobs = []
    for sch in SCHEMAS:
        for i in range(1, 14):
            table_jobs.append((CATALOG, sch, f"dim_{sch}_{i:02d}"))
        for i in range(1, 14):
            table_jobs.append((CATALOG, sch, f"fact_{sch}_{i:02d}"))

    print(f"\n=== Loading sample data into {len(table_jobs)} tables in {CATALOG} ===")

    def load_table(cat, sch, tbl):
        n_rows = random.randint(ROWS_PER_TABLE_MIN, ROWS_PER_TABLE_MAX)
        stmt = insert_stmt(cat, sch, tbl, n_rows)
        return run(stmt, f"{cat}.{sch}.{tbl} ({n_rows} rows)")

    success = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(load_table, cat, sch, tbl): (cat, sch, tbl) for cat, sch, tbl in table_jobs}
        for f in as_completed(futs):
            if f.result():
                success += 1
            else:
                failed += 1

    print(f"\n  Tables loaded: {success} succeeded, {failed} failed")

    print("\n=== Verification: total row count per schema ===")
    for sch in SCHEMAS:
        ok, d = sql_exec(
            f"SELECT COUNT(*) AS total_tables FROM information_schema.tables "
            f"WHERE table_catalog = '{CATALOG}' AND table_schema = '{sch}'"
        )
        print(f"  {sch}: query {'ok' if ok else 'failed'}")

    print("\nDONE.")


if __name__ == "__main__":
    main()
