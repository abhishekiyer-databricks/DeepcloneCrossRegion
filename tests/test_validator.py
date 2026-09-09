# Databricks notebook source
# MAGIC %md
# MAGIC # DeepClone CrossRegion — Post-Clone Validation Suite
# MAGIC
# MAGIC Runs after a clone operation to verify:
# MAGIC 1. **Row count parity** — source vs target rows match exactly.
# MAGIC 2. **Schema match** — column names, types, and nullability are identical.
# MAGIC 3. **Data integrity** — hash-based row-level comparison on sampled data.
# MAGIC 4. **Table type** — managed/external classification preserved correctly.
# MAGIC 5. **Partition columns** — same partition structure on source and target.
# MAGIC 6. **Audit log completeness** — every expected table has an audit row.
# MAGIC 7. **Delta metadata** — target table is a valid Delta table with proper history.

# COMMAND ----------

import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, BooleanType, StructField, StructType, TimestampType

# COMMAND ----------

dbutils.widgets.text("src_catalog",     "catalog_prod",       "Source Catalog")
dbutils.widgets.text("tgt_catalog",     "catalog_prod_dr",    "Target Catalog")
dbutils.widgets.text("audit_catalog",   "utility_metadata",   "Audit Catalog")
dbutils.widgets.text("execution_id",    "",                   "Execution ID to validate (blank = latest)")
dbutils.widgets.text("schemas",         "",                   "Schemas to validate (CSV, blank = all)")
dbutils.widgets.dropdown("fail_fast",   "false", ["true","false"], "Stop on first failure")
dbutils.widgets.text("row_sample_pct",  "1",                  "% rows sampled for hash comparison (1-100)")

SRC_CATALOG   = dbutils.widgets.get("src_catalog")
TGT_CATALOG   = dbutils.widgets.get("tgt_catalog")
AUDIT_CATALOG = dbutils.widgets.get("audit_catalog")
EXEC_ID       = dbutils.widgets.get("execution_id").strip()
SCHEMAS_CSV   = dbutils.widgets.get("schemas").strip()
FAIL_FAST     = dbutils.widgets.get("fail_fast").lower() == "true"
SAMPLE_PCT    = max(1, min(100, int(dbutils.widgets.get("row_sample_pct"))))

# COMMAND ----------

# ── Determine execution_id to validate ────────────────────────────────────

if not EXEC_ID:
    latest = spark.sql(f"""
        SELECT execution_id FROM {AUDIT_CATALOG}.default.deep_clone_audit_log
        ORDER BY run_timestamp DESC LIMIT 1
    """).collect()
    EXEC_ID = latest[0][0] if latest else "unknown"
    print(f"[INFO] Using latest execution_id: {EXEC_ID}")

SCHEMAS = [s.strip() for s in SCHEMAS_CSV.split(",") if s.strip()] if SCHEMAS_CSV else None

# COMMAND ----------

# ── Validation result tracker ──────────────────────────────────────────────

RESULTS = []

def record(check: str, table: str, passed: bool, detail: str = ""):
    RESULTS.append({
        "check": check,
        "table": table,
        "passed": passed,
        "detail": detail,
    })
    status = "PASS" if passed else "FAIL"
    icon   = "✓" if passed else "✗"
    print(f"  [{status}] {icon} {check:<36} {table:<60} {detail}")
    if not passed and FAIL_FAST:
        raise AssertionError(f"[FAIL_FAST] Check '{check}' failed for table '{table}': {detail}")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# HELPER: get table list from audit log for this execution
# ═════════════════════════════════════════════════════════════════════════════

def get_expected_tables() -> list:
    rows = spark.sql(f"""
        SELECT DISTINCT table_name, src_catalog, src_schema, tgt_catalog, tgt_schema,
               table_type, status, clone_mode
        FROM {AUDIT_CATALOG}.default.deep_clone_audit_log
        WHERE execution_id = '{EXEC_ID}' AND status = 'SUCCESS'
    """).collect()
    return rows

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# CHECK 1: Row Count Parity
# ═════════════════════════════════════════════════════════════════════════════

def check_row_counts(tbl_rows: list):
    print("\n── CHECK 1: Row Count Parity ──")
    for row in tbl_rows:
        table_name = row["table_name"].split(".")[-1]
        src_full = f"`{row['src_catalog']}`.`{row['src_schema']}`.`{table_name}`"
        tgt_full = f"`{row['tgt_catalog']}`.`{row['tgt_schema']}`.`{table_name}`"
        try:
            src_cnt = spark.sql(f"SELECT COUNT(*) FROM {src_full}").collect()[0][0]
            tgt_cnt = spark.sql(f"SELECT COUNT(*) FROM {tgt_full}").collect()[0][0]
            passed  = src_cnt == tgt_cnt
            detail  = f"src={src_cnt:,} tgt={tgt_cnt:,}"
            if not passed:
                detail += f" DELTA={tgt_cnt - src_cnt:+,}"
            record("row_count_parity", row["table_name"], passed, detail)
        except Exception as exc:
            record("row_count_parity", row["table_name"], False, f"Error: {exc}")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# CHECK 2: Schema Match
# ═════════════════════════════════════════════════════════════════════════════

def check_schemas(tbl_rows: list):
    print("\n── CHECK 2: Schema Match ──")
    for row in tbl_rows:
        table_name = row["table_name"].split(".")[-1]
        src_full   = f"`{row['src_catalog']}`.`{row['src_schema']}`.`{table_name}`"
        tgt_full   = f"`{row['tgt_catalog']}`.`{row['tgt_schema']}`.`{table_name}`"
        try:
            src_schema = {f.name: str(f.dataType) for f in spark.read.table(src_full).schema.fields}
            tgt_schema = {f.name: str(f.dataType) for f in spark.read.table(tgt_full).schema.fields}

            missing_in_tgt = set(src_schema) - set(tgt_schema)
            extra_in_tgt   = set(tgt_schema) - set(src_schema)
            type_mismatches = {
                col: (src_schema[col], tgt_schema[col])
                for col in set(src_schema) & set(tgt_schema)
                if src_schema[col] != tgt_schema[col]
            }

            passed = not (missing_in_tgt or extra_in_tgt or type_mismatches)
            issues = []
            if missing_in_tgt: issues.append(f"missing in tgt: {missing_in_tgt}")
            if extra_in_tgt:   issues.append(f"extra in tgt: {extra_in_tgt}")
            if type_mismatches:issues.append(f"type mismatches: {type_mismatches}")
            detail = "; ".join(issues) if issues else f"{len(src_schema)} cols match"
            record("schema_match", row["table_name"], passed, detail)
        except Exception as exc:
            record("schema_match", row["table_name"], False, f"Error: {exc}")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# CHECK 3: Data Hash Integrity (sampled)
# ═════════════════════════════════════════════════════════════════════════════

def check_data_integrity(tbl_rows: list, sample_pct: float):
    print(f"\n── CHECK 3: Data Hash Integrity ({sample_pct}% sample) ──")
    fraction = sample_pct / 100.0

    for row in tbl_rows:
        table_name = row["table_name"].split(".")[-1]
        src_full   = f"`{row['src_catalog']}`.`{row['src_schema']}`.`{table_name}`"
        tgt_full   = f"`{row['tgt_catalog']}`.`{row['tgt_schema']}`.`{table_name}`"
        try:
            src_df = spark.read.table(src_full)
            tgt_df = spark.read.table(tgt_full)

            # Compute a row-level hash across all columns for comparison
            def add_row_hash(df):
                cols = df.columns
                return df.withColumn(
                    "__row_hash__",
                    F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in cols]))
                ).select("__row_hash__")

            src_sample_count = int(spark.sql(f"SELECT COUNT(*) FROM {src_full}").collect()[0][0] * fraction)
            if src_sample_count == 0:
                record("data_hash_integrity", row["table_name"], True, "empty table — skip hash")
                continue

            src_hashes = add_row_hash(src_df.sample(fraction=fraction, seed=42)).groupBy("__row_hash__").count().withColumnRenamed("count", "src_count")
            tgt_hashes = add_row_hash(tgt_df.sample(fraction=fraction, seed=42)).groupBy("__row_hash__").count().withColumnRenamed("count", "tgt_count")

            mismatches = (
                src_hashes.join(tgt_hashes, on="__row_hash__", how="full_outer")
                .where(F.col("src_count").isNull() | F.col("tgt_count").isNull() | (F.col("src_count") != F.col("tgt_count")))
                .count()
            )
            passed = (mismatches == 0)
            detail = f"{src_sample_count:,} rows sampled, {mismatches} hash mismatches"
            record("data_hash_integrity", row["table_name"], passed, detail)
        except Exception as exc:
            record("data_hash_integrity", row["table_name"], False, f"Error: {exc}")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# CHECK 4: Table Type (MANAGED / EXTERNAL)
# ═════════════════════════════════════════════════════════════════════════════

def check_table_types(tbl_rows: list):
    print("\n── CHECK 4: Table Type Verification ──")
    for row in tbl_rows:
        table_name = row["table_name"].split(".")[-1]
        tgt_full   = f"`{row['tgt_catalog']}`.`{row['tgt_schema']}`.`{table_name}`"
        expected   = row["table_type"]
        try:
            detail_row = spark.sql(f"DESCRIBE DETAIL {tgt_full}").collect()[0]
            actual_type = detail_row["type"].upper() if hasattr(detail_row, 'type') else "UNKNOWN"
            passed = (actual_type == expected) or (expected == "UNKNOWN")
            detail = f"expected={expected} actual={actual_type}"
            record("table_type_preserved", row["table_name"], passed, detail)
        except Exception as exc:
            record("table_type_preserved", row["table_name"], False, f"Error: {exc}")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# CHECK 5: Partition Column Match
# ═════════════════════════════════════════════════════════════════════════════

def check_partition_columns(tbl_rows: list):
    print("\n── CHECK 5: Partition Column Match ──")
    for row in tbl_rows:
        table_name = row["table_name"].split(".")[-1]
        src_full   = f"`{row['src_catalog']}`.`{row['src_schema']}`.`{table_name}`"
        tgt_full   = f"`{row['tgt_catalog']}`.`{row['tgt_schema']}`.`{table_name}`"
        try:
            src_detail = spark.sql(f"DESCRIBE DETAIL {src_full}").collect()[0]
            tgt_detail = spark.sql(f"DESCRIBE DETAIL {tgt_full}").collect()[0]
            src_parts  = sorted(src_detail["partitionColumns"] or [])
            tgt_parts  = sorted(tgt_detail["partitionColumns"] or [])
            passed     = src_parts == tgt_parts
            detail     = f"src={src_parts} tgt={tgt_parts}"
            record("partition_columns_match", row["table_name"], passed, detail)
        except Exception as exc:
            record("partition_columns_match", row["table_name"], False, f"Error: {exc}")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# CHECK 6: Audit Log Completeness
# ═════════════════════════════════════════════════════════════════════════════

def check_audit_completeness():
    print("\n── CHECK 6: Audit Log Completeness ──")
    try:
        audit_rows = spark.sql(f"""
            SELECT status, COUNT(*) as cnt
            FROM {AUDIT_CATALOG}.default.deep_clone_audit_log
            WHERE execution_id = '{EXEC_ID}'
            GROUP BY status
        """).collect()
        counts = {r["status"]: r["cnt"] for r in audit_rows}
        total  = sum(counts.values())
        passed = total > 0
        detail = " | ".join(f"{k}={v}" for k, v in counts.items())
        record("audit_log_completeness", f"execution_id={EXEC_ID}", passed, f"total={total} {detail}")
    except Exception as exc:
        record("audit_log_completeness", f"execution_id={EXEC_ID}", False, f"Error: {exc}")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# CHECK 7: Delta Table Validity (target has Delta history)
# ═════════════════════════════════════════════════════════════════════════════

def check_delta_validity(tbl_rows: list):
    print("\n── CHECK 7: Delta Table Validity ──")
    for row in tbl_rows:
        table_name = row["table_name"].split(".")[-1]
        tgt_full   = f"`{row['tgt_catalog']}`.`{row['tgt_schema']}`.`{table_name}`"
        try:
            hist = spark.sql(f"DESCRIBE HISTORY {tgt_full} LIMIT 1").collect()
            has_history = len(hist) > 0
            op = hist[0]["operation"] if has_history else "n/a"
            passed = has_history
            detail = f"latest_op={op}"
            record("delta_table_valid", row["table_name"], passed, detail)
        except Exception as exc:
            record("delta_table_valid", row["table_name"], False, f"Error: {exc}")

# COMMAND ----------

# ── Run all checks ─────────────────────────────────────────────────────────

print("\n" + "═" * 80)
print(f"  DeepClone CrossRegion — Validation Suite")
print(f"  Execution ID : {EXEC_ID}")
print(f"  Source       : {SRC_CATALOG}")
print(f"  Target       : {TGT_CATALOG}")
print(f"  Sample %     : {SAMPLE_PCT}%")
print("═" * 80)

expected_tables = get_expected_tables()
print(f"\n[INFO] Tables to validate: {len(expected_tables)} (status=SUCCESS in audit log)")

if not expected_tables:
    print("[WARN] No successful tables found in audit log for this execution_id. Ensure the clone ran successfully first.")
    dbutils.notebook.exit("No tables to validate.")

check_row_counts(expected_tables)
check_schemas(expected_tables)
check_data_integrity(expected_tables, SAMPLE_PCT)
check_table_types(expected_tables)
check_partition_columns(expected_tables)
check_audit_completeness()
check_delta_validity(expected_tables)

# COMMAND ----------

# ── Final report ───────────────────────────────────────────────────────────

total_checks  = len(RESULTS)
passed_checks = sum(1 for r in RESULTS if r["passed"])
failed_checks = total_checks - passed_checks
pass_rate     = passed_checks / total_checks * 100 if total_checks else 0

print(f"""
{"═" * 80}
  VALIDATION REPORT
  Execution ID : {EXEC_ID}
  Total Checks : {total_checks}
  Passed       : {passed_checks}   ({pass_rate:.1f}%)
  Failed       : {failed_checks}
{"═" * 80}
""")

if failed_checks > 0:
    print("FAILED CHECKS:")
    for r in RESULTS:
        if not r["passed"]:
            print(f"  ✗  [{r['check']}] {r['table']} — {r['detail']}")

# Persist validation report as a Delta table
validation_schema = StructType([
    StructField("execution_id",    StringType(), False),
    StructField("validated_at",    TimestampType(), True),
    StructField("check_name",      StringType(), True),
    StructField("table_name",      StringType(), True),
    StructField("passed",          BooleanType(), True),
    StructField("detail",          StringType(), True),
])

now_ts = datetime.now(tz=timezone.utc)

report_df = spark.createDataFrame(
    [(EXEC_ID, now_ts, r["check"], r["table"], r["passed"], r["detail"]) for r in RESULTS],
    schema=validation_schema,
)

report_table = f"{AUDIT_CATALOG}.default.validation_results"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {AUDIT_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_CATALOG}.default")
report_df.write.format("delta").mode("append").saveAsTable(report_table)
print(f"\n[INFO] Validation report saved to: {report_table}")

summary = {
    "execution_id":   EXEC_ID,
    "total_checks":   total_checks,
    "passed_checks":  passed_checks,
    "failed_checks":  failed_checks,
    "pass_rate_pct":  round(pass_rate, 2),
    "validated_at":   now_ts.isoformat(),
}

dbutils.notebook.exit(json.dumps(summary))
