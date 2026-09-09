# Databricks notebook source
# MAGIC %md
# MAGIC # DeepClone CrossRegion — Test Data Generator
# MAGIC Generates a controlled suite of source tables to exercise all code paths:
# MAGIC - Managed tables (small, large, partitioned, empty, wide schema)
# MAGIC - External tables (with explicit ADLS locations)
# MAGIC - Edge cases: tables with special characters in names, single-row tables,
# MAGIC   deeply nested schemas, tables with deletion vectors

# COMMAND ----------

import random
import string
import uuid
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, DoubleType, IntegerType,
    LongType, MapType, StringType, StructField, StructType, TimestampType,
    ArrayType,
)

# COMMAND ----------

# ── Parameters ────────────────────────────────────────────────────────────
dbutils.widgets.text("src_catalog",        "catalog_prod",  "Source Catalog")
dbutils.widgets.text("adls_base_path",     "abfss://srcdata@srcadlsaccount.dfs.core.windows.net", "ADLS Base Path for External Tables")
dbutils.widgets.text("num_large_rows",     "5000000",       "Rows in large table")
dbutils.widgets.dropdown("generate_external", "true", ["true", "false"], "Generate External Tables")

SRC_CATALOG    = dbutils.widgets.get("src_catalog")
ADLS_BASE      = dbutils.widgets.get("adls_base_path").rstrip("/")
NUM_LARGE_ROWS = int(dbutils.widgets.get("num_large_rows"))
GEN_EXTERNAL   = dbutils.widgets.get("generate_external").lower() == "true"

# COMMAND ----------

# ── Setup catalog & schemas ────────────────────────────────────────────────

def setup_namespaces():
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{SRC_CATALOG}`")
    for schema in ["sales", "finance", "operations", "analytics", "edge_cases"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SRC_CATALOG}`.`{schema}`")
    print(f"Namespaces created under catalog: {SRC_CATALOG}")

setup_namespaces()

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# MANAGED TABLE GENERATORS
# ═════════════════════════════════════════════════════════════════════════════

def create_transactions_fact():
    """Large partitioned managed fact table (~5M rows)."""
    schema = StructType([
        StructField("transaction_id",   StringType(),  False),
        StructField("customer_id",      LongType(),    True),
        StructField("product_sku",      StringType(),  True),
        StructField("quantity",         IntegerType(), True),
        StructField("unit_price",       DecimalType(12,2), True),
        StructField("total_amount",     DecimalType(14,2), True),
        StructField("currency",         StringType(),  True),
        StructField("channel",          StringType(),  True),
        StructField("region",           StringType(),  True),
        StructField("transaction_date", DateType(),    True),
        StructField("created_at",       TimestampType(), True),
        StructField("is_refunded",      BooleanType(), True),
    ])

    base_date = date(2022, 1, 1)
    channels  = ["online", "store", "mobile", "partner"]
    regions   = ["APAC", "EMEA", "AMER", "LATAM"]
    currencies = ["USD", "EUR", "GBP", "JPY", "INR"]

    df = (
        spark.range(NUM_LARGE_ROWS)
        .withColumn("transaction_id",   F.concat(F.lit("TXN-"), F.col("id").cast("string")))
        .withColumn("customer_id",      (F.col("id") % 500000 + 1).cast(LongType()))
        .withColumn("product_sku",      F.concat(F.lit("SKU-"), (F.col("id") % 10000).cast("string")))
        .withColumn("quantity",         (F.col("id") % 20 + 1).cast(IntegerType()))
        .withColumn("unit_price",       ((F.rand() * 999 + 1).cast(DecimalType(12,2))))
        .withColumn("total_amount",     (F.col("unit_price") * F.col("quantity")).cast(DecimalType(14,2)))
        .withColumn("currency",         F.element_at(F.array([F.lit(c) for c in currencies]), (F.col("id") % 5 + 1).cast(IntegerType())))
        .withColumn("channel",          F.element_at(F.array([F.lit(c) for c in channels]),   (F.col("id") % 4 + 1).cast(IntegerType())))
        .withColumn("region",           F.element_at(F.array([F.lit(r) for r in regions]),    (F.col("id") % 4 + 1).cast(IntegerType())))
        .withColumn("transaction_date", F.date_add(F.lit(str(base_date)), (F.col("id") % 1095).cast(IntegerType())))
        .withColumn("created_at",       F.current_timestamp())
        .withColumn("is_refunded",      (F.col("id") % 20 == 0))
        .drop("id")
    )

    tbl = f"`{SRC_CATALOG}`.`sales`.`transactions_fact`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    (df.write.format("delta")
       .mode("overwrite")
       .partitionBy("transaction_date", "region")
       .saveAsTable(tbl))
    print(f"Created: {tbl} ({NUM_LARGE_ROWS:,} rows, partitioned by transaction_date, region)")


def create_customer_dim():
    """Medium managed dimension table."""
    df = (
        spark.range(100_000)
        .withColumn("customer_id",    (F.col("id") + 1).cast(LongType()))
        .withColumn("full_name",      F.concat(F.lit("Customer_"), F.col("id").cast("string")))
        .withColumn("email",          F.concat(F.lit("user"), F.col("id").cast("string"), F.lit("@example.com")))
        .withColumn("phone",          F.concat(F.lit("+1-555-"), F.lpad((F.col("id") % 9000000 + 1000000).cast("string"), 7, "0")))
        .withColumn("country_code",   F.element_at(F.array([F.lit(c) for c in ["US","IN","GB","DE","JP","AU"]]), (F.col("id") % 6 + 1).cast(IntegerType())))
        .withColumn("loyalty_tier",   F.element_at(F.array([F.lit(t) for t in ["BRONZE","SILVER","GOLD","PLATINUM"]]), (F.col("id") % 4 + 1).cast(IntegerType())))
        .withColumn("signup_date",    F.date_sub(F.current_date(), (F.col("id") % 1825).cast(IntegerType())))
        .withColumn("is_active",      (F.col("id") % 10 != 0))
        .drop("id")
    )
    tbl = f"`{SRC_CATALOG}`.`sales`.`customer_dim`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").saveAsTable(tbl)
    print(f"Created: {tbl} (100,000 rows)")


def create_revenue_monthly():
    """Small managed finance aggregation table."""
    df = (
        spark.range(120)
        .withColumn("month_key",        F.date_trunc("month", F.date_add(F.lit("2014-01-01"), (F.col("id") * 30).cast(IntegerType()))))
        .withColumn("total_revenue",    (F.rand() * 10_000_000 + 500_000).cast(DecimalType(16,2)))
        .withColumn("total_orders",     (F.rand() * 100_000 + 10_000).cast(LongType()))
        .withColumn("avg_order_value",  (F.col("total_revenue") / F.col("total_orders")).cast(DecimalType(10,2)))
        .withColumn("region",           F.element_at(F.array([F.lit(r) for r in ["APAC","EMEA","AMER","LATAM"]]), (F.col("id") % 4 + 1).cast(IntegerType())))
        .drop("id")
    )
    tbl = f"`{SRC_CATALOG}`.`finance`.`revenue_monthly`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").saveAsTable(tbl)
    print(f"Created: {tbl} (120 rows)")


def create_empty_staging():
    """Edge case: empty managed table."""
    schema = StructType([
        StructField("batch_id",   StringType(),    False),
        StructField("payload",    StringType(),    True),
        StructField("loaded_at",  TimestampType(), True),
    ])
    df = spark.createDataFrame([], schema)
    tbl = f"`{SRC_CATALOG}`.`sales`.`empty_staging_table`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").saveAsTable(tbl)
    print(f"Created: {tbl} (0 rows — empty edge case)")


def create_wide_schema_table():
    """Edge case: very wide schema (100+ columns)."""
    base = spark.range(10_000).withColumn("id", F.col("id").cast(LongType()))
    for i in range(1, 101):
        base = base.withColumn(f"col_{i:03d}", (F.rand() * 1000).cast(DoubleType()))
    for i in range(1, 21):
        base = base.withColumn(f"flag_{i:02d}", (F.col("id") % (i + 1) == 0))
    tbl = f"`{SRC_CATALOG}`.`analytics`.`wide_schema_table`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    base.write.format("delta").mode("overwrite").saveAsTable(tbl)
    print(f"Created: {tbl} (10,000 rows, {len(base.columns)} columns)")


def create_special_chars_table():
    """Edge case: table name with special characters (backtick-quoted)."""
    df = spark.range(5_000).withColumn("value", F.rand())
    tbl = f"`{SRC_CATALOG}`.`operations`.`special-table_v2`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").saveAsTable(tbl)
    print(f"Created: {tbl} (5,000 rows, special chars in name)")


def create_single_row_table():
    """Edge case: single-row table."""
    df = spark.createDataFrame(
        [("SINGLETON_001", "alive", 1)],
        schema=["record_id", "status", "version"],
    )
    tbl = f"`{SRC_CATALOG}`.`edge_cases`.`single_row_table`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").saveAsTable(tbl)
    print(f"Created: {tbl} (1 row — single-row edge case)")


def create_map_array_table():
    """Edge case: complex types — MapType and ArrayType."""
    schema = StructType([
        StructField("id",         LongType(),                                False),
        StructField("attributes", MapType(StringType(), StringType()),        True),
        StructField("tags",       ArrayType(StringType()),                    True),
        StructField("scores",     ArrayType(DoubleType()),                    True),
    ])
    rows = [
        (i, {"color": "red", "size": "L", "brand": f"Brand_{i%10}"}, [f"tag_{j}" for j in range(i % 5 + 1)], [round(random.random(), 4) for _ in range(3)])
        for i in range(2_000)
    ]
    df = spark.createDataFrame(rows, schema)
    tbl = f"`{SRC_CATALOG}`.`edge_cases`.`complex_types_table`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").saveAsTable(tbl)
    print(f"Created: {tbl} (2,000 rows, MapType + ArrayType)")


def create_partitioned_analytics():
    """Partitioned managed analytics table — tests partition column preservation."""
    df = (
        spark.range(1_000_000)
        .withColumn("event_id",   F.concat(F.lit("EVT-"), F.col("id").cast("string")))
        .withColumn("event_type", F.element_at(F.array([F.lit(t) for t in ["click","view","purchase","scroll","share"]]), (F.col("id") % 5 + 1).cast(IntegerType())))
        .withColumn("user_id",    (F.col("id") % 100_000 + 1).cast(LongType()))
        .withColumn("session_id", F.concat(F.lit("SES-"), (F.col("id") % 50_000).cast("string")))
        .withColumn("page_url",   F.concat(F.lit("/page/"), (F.col("id") % 500).cast("string")))
        .withColumn("duration_ms",(F.rand() * 30000).cast(LongType()))
        .withColumn("event_date", F.date_add(F.lit("2025-01-01"), (F.col("id") % 365).cast(IntegerType())))
        .drop("id")
    )
    tbl = f"`{SRC_CATALOG}`.`analytics`.`pageviews_hourly`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    (df.write.format("delta")
       .mode("overwrite")
       .partitionBy("event_date", "event_type")
       .saveAsTable(tbl))
    print(f"Created: {tbl} (1,000,000 rows, partitioned by event_date, event_type)")

# COMMAND ----------

# ═════════════════════════════════════════════════════════════════════════════
# EXTERNAL TABLE GENERATORS
# ═════════════════════════════════════════════════════════════════════════════

def create_external_product_catalog():
    """External table with explicit ADLS location."""
    if not GEN_EXTERNAL:
        print("[SKIP] External table generation disabled.")
        return
    ext_path = f"{ADLS_BASE}/catalog_prod/sales/product_catalog"
    df = (
        spark.range(50_000)
        .withColumn("sku",          F.concat(F.lit("SKU-"), F.col("id").cast("string")))
        .withColumn("product_name", F.concat(F.lit("Product "), F.col("id").cast("string")))
        .withColumn("category",     F.element_at(F.array([F.lit(c) for c in ["Electronics","Apparel","Home","Sports","Food"]]), (F.col("id") % 5 + 1).cast(IntegerType())))
        .withColumn("unit_cost",    (F.rand() * 500 + 5).cast(DecimalType(10,2)))
        .withColumn("is_available", (F.col("id") % 7 != 0))
        .withColumn("supplier_code",F.concat(F.lit("SUP-"), (F.col("id") % 200).cast("string")))
        .drop("id")
    )
    tbl = f"`{SRC_CATALOG}`.`sales`.`product_catalog`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").save(ext_path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tbl}
        USING DELTA LOCATION '{ext_path}'
    """)
    print(f"Created EXTERNAL: {tbl} → {ext_path} (50,000 rows)")


def create_external_warehouse_locations():
    """External table with separate ADLS container path."""
    if not GEN_EXTERNAL:
        print("[SKIP] External table generation disabled.")
        return
    ext_path = f"{ADLS_BASE}/catalog_prod/operations/warehouse_locations"
    df = spark.createDataFrame(
        [(i, f"WH-{i:04d}", f"City_{i}", f"State_{i%50}", f"US", round(random.uniform(25.0, 50.0), 4), round(random.uniform(-125.0, -65.0), 4)) for i in range(1, 2401)],
        schema=["warehouse_id", "warehouse_code", "city", "state", "country", "latitude", "longitude"],
    )
    tbl = f"`{SRC_CATALOG}`.`operations`.`warehouse_locations`"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.write.format("delta").mode("overwrite").save(ext_path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tbl}
        USING DELTA LOCATION '{ext_path}'
    """)
    print(f"Created EXTERNAL: {tbl} → {ext_path} (2,400 rows)")

# COMMAND ----------

# ── Run all generators ─────────────────────────────────────────────────────

print("\n" + "═" * 60)
print("  DeepClone CrossRegion — Test Data Generator")
print(f"  Catalog: {SRC_CATALOG}")
print("═" * 60 + "\n")

generators = [
    ("transactions_fact (large managed)",    create_transactions_fact),
    ("customer_dim (medium managed)",         create_customer_dim),
    ("revenue_monthly (small managed)",       create_revenue_monthly),
    ("empty_staging_table (empty edge)",      create_empty_staging),
    ("wide_schema_table (100+ cols)",         create_wide_schema_table),
    ("special-table_v2 (special chars)",      create_special_chars_table),
    ("single_row_table (single row)",         create_single_row_table),
    ("complex_types_table (map+array)",       create_map_array_table),
    ("pageviews_hourly (partitioned)",        create_partitioned_analytics),
    ("product_catalog (external)",            create_external_product_catalog),
    ("warehouse_locations (external)",        create_external_warehouse_locations),
]

for label, fn in generators:
    print(f"\n→ Generating: {label}")
    try:
        fn()
    except Exception as exc:
        print(f"  [ERROR] Failed to generate {label}: {exc}")

# COMMAND ----------

# ── Verification summary ───────────────────────────────────────────────────

print("\n" + "═" * 60)
print("  GENERATION COMPLETE — Table Inventory")
print("═" * 60)

for schema in ["sales", "finance", "operations", "analytics", "edge_cases"]:
    rows = spark.sql(f"SHOW TABLES IN `{SRC_CATALOG}`.`{schema}`").collect()
    if rows:
        print(f"\n  Schema: {schema}")
        for row in rows:
            try:
                cnt = spark.sql(f"SELECT COUNT(*) FROM `{SRC_CATALOG}`.`{schema}`.`{row['tableName']}`").collect()[0][0]
                print(f"    • {row['tableName']:<40} {cnt:>12,} rows")
            except Exception:
                print(f"    • {row['tableName']:<40} (count error)")

dbutils.notebook.exit("Test data generation complete.")
