# DeepClone CrossRegion — Data Copy Utility

> ⚠️ **STALE — describes a superseded pre-bundle architecture.** Everything
> below (`config.json`, `deepclone_main.py`, `auth_manager.py`,
> `clone_engine.py`, `wrapper_notebook.py`, the onboarding table, etc.)
> refers to files/tables that no longer exist in this repo. The current
> implementation is a Databricks Asset Bundle: `databricks.yml` +
> `resources/*.yml` define the jobs, `orchestrator/*.py` + `notebooks/*.py`
> hold the logic, and `configs/migration.yaml` is the single config file.
> For the current, accurate step-by-step procedure, see
> [`docs/SOP_CSV_Run.md`](docs/SOP_CSV_Run.md). Section 10 (File Reference)
> below has been corrected to the current tree; everything else in this file
> is kept only for historical/conceptual context (clone modes, validation
> options, troubleshooting categories are still conceptually relevant, just
> described against the old file names).

Production-grade tool for copying Delta Lake tables across Azure Databricks workspaces using **Delta DEEP CLONE**.  
Supports catalog, schema, or table-level scope, two clone strategies, and configurable post-clone validation.

---

## Table of Contents

1. [What this utility does](#1-what-this-utility-does)
2. [Pre-requisites](#2-pre-requisites)
3. [Clone modes — which one to pick?](#3-clone-modes)
4. [Scope options — what to copy](#4-scope-options)
5. [Validation options](#5-validation-options)
6. [Step-by-step: Run as a Databricks Job](#6-run-as-a-databricks-job)
7. [Onboarding table — self-service requests](#7-onboarding-table)
8. [Monitor a run](#8-monitor-a-run)
9. [Troubleshoot common errors](#9-troubleshoot-common-errors)
10. [File reference](#10-file-reference)

---

## 1. What this utility does

```
Source Workspace                        Target Workspace
─────────────────────────────────       ─────────────────────────────────
azure_uc_demo_region2                   azure_uc_demo_region1
  └─ deepclone_src                        └─ deepclone_tgt
       ├─ patients          ──────────────────► patients  (DEEP CLONE)
       ├─ claims            ──────────────────► claims    (DEEP CLONE)
       └─ providers         ──────────────────► providers (DEEP CLONE)

                     ▼ After every clone ▼
              Post-clone Validation
              (COUNT / CHECKSUM / COLUMN_SAMPLE)

                     ▼ All results recorded ▼
         azure_uc_demo_region1.deepclone_meta.deep_clone_audit_log
         azure_uc_demo_region1.deepclone_meta.clone_onboarding
```

**Key features**

| Feature | Detail |
|---------|--------|
| Clone mechanism | Delta `DEEP CLONE` — full, independent copy |
| Scope | Catalog / Schema / Explicit table list |
| Authentication | OAuth 2.0 M2M (Service Principal) |
| Clone modes | `path_resolution` (ADLS path) · `delta_share` (shared catalog) |
| Validation | COUNT · CHECKSUM · COLUMN_SAMPLE · ALL |
| Concurrency | Configurable `max_workers` (Python `concurrent.futures`) |
| Audit trail | Delta-backed `deep_clone_audit_log` |
| Input queue | `clone_onboarding` table (self-service, per-team) |
| Dry-run | Full execution blueprint, no data written |

---

## 2. Pre-requisites

### 2a. Service Principals (two SPs, or one if same tenant)

| SP | Workspace | Required permissions |
|----|-----------|---------------------|
| `sp-migrate-src` | Source | `USE CATALOG`, `USE SCHEMA`, `SELECT` on source tables |
| `sp-migrate-tgt` | Target | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY` on target; `ALL PRIVILEGES` on metadata schema |

### 2b. Set environment variables (local or in Job cluster config)

```bash
export AZ2AZ_SRC_URL="https://adb-<source-id>.azuredatabricks.net"
export AZ2AZ_SRC_CID="<source-sp-client-id>"
export AZ2AZ_SRC_SECRET="<source-sp-client-secret>"

export AZ2AZ_TGT_URL="https://adb-<target-id>.azuredatabricks.net"
export AZ2AZ_TGT_CID="<target-sp-client-id>"
export AZ2AZ_TGT_SECRET="<target-sp-client-secret>"

export AZ2AZ_SRC_WH_ID="<source-sql-warehouse-id>"
export AZ2AZ_TGT_WH_ID="<target-sql-warehouse-id>"
```

> **Never** store secrets in `config.json`. Use environment variables or Databricks Secrets.

### 2c. Metadata tables on target workspace

The utility needs two tables on the target. If they don't exist yet, run:

```sql
-- On target workspace
CREATE SCHEMA IF NOT EXISTS azure_uc_demo_region1.deepclone_meta;

CREATE TABLE IF NOT EXISTS azure_uc_demo_region1.deepclone_meta.clone_onboarding (
  onboarding_id        STRING,
  request_name         STRING,
  scope_mode           STRING,    -- catalog | schema | table
  clone_mode           STRING,    -- path_resolution | delta_share
  src_catalog          STRING,
  src_schema           STRING,
  src_table            STRING,
  tgt_catalog          STRING,
  tgt_schema           STRING,
  priority             INT,       -- 1=High 2=Medium 3=Low
  status               STRING,    -- PENDING | IN_PROGRESS | COMPLETED | VALIDATION_FAILED | FAILED | SKIPPED
  requested_by         STRING,
  requested_at         TIMESTAMP,
  scheduled_for        TIMESTAMP,
  execution_id         STRING,
  completed_at         TIMESTAMP,
  -- Validation parameters
  validation_mode      STRING,    -- NONE | COUNT | CHECKSUM | COLUMN_SAMPLE | ALL
  validation_columns   STRING,    -- comma-separated col names (NULL = all)
  count_tolerance_pct  DOUBLE,    -- 0.0 = exact match
  checksum_algorithm   STRING,    -- XXHASH64 | SHA256
  notes                STRING,
  tags                 MAP<STRING, STRING>
) USING DELTA;

CREATE TABLE IF NOT EXISTS azure_uc_demo_region1.deepclone_meta.deep_clone_audit_log (
  execution_id       STRING,
  onboarding_id      STRING,
  batch_id           STRING,
  batch_sequence     INT,
  retry_attempt      INT,
  run_timestamp      TIMESTAMP,
  table_name         STRING,
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
  status             STRING,
  error_message      STRING,
  validation_status  STRING,    -- PASS | FAIL | SKIPPED
  validation_notes   STRING
) USING DELTA;
```

---

## 3. Clone Modes

Choose the right mode for your architecture:

### `path_resolution` (recommended for most cases)

The utility calls `DESCRIBE DETAIL` on the source table via the Source SQL API to resolve the ADLS path (`abfss://…`), then runs:

```sql
CREATE OR REPLACE TABLE target_catalog.target_schema.my_table
DEEP CLONE delta.`abfss://container@storageaccount.dfs.core.windows.net/path/to/table`
```

**Requirement**: Target cluster/warehouse must have Storage Blob Data Reader on the source ADLS account.  
For **same-metastore** clones, the utility automatically uses the FQN form instead (avoids LOCATION_OVERLAP).

### `delta_share`

The source metastore shares a catalog via Delta Sharing. The target workspace consumes it as:

```sql
-- First, on target workspace (one-time setup):
CREATE CATALOG delta_share_catalog USING SHARE provider_name.share_name;

-- Then the utility runs:
CREATE OR REPLACE TABLE target_catalog.schema.my_table
DEEP CLONE delta_share_catalog.schema.my_table
```

**Requirement**: Delta Sharing provider configured on source; recipient on target.

---

## 4. Scope Options

Set `scope_mode` in the onboarding table row, or in `config.json`:

| Scope | What gets cloned | Onboarding table fields |
|-------|-----------------|------------------------|
| `catalog` | All schemas and tables in a catalog | `src_catalog` required |
| `schema` | All tables in one schema | `src_catalog` + `src_schema` required |
| `table` | One specific table | `src_catalog` + `src_schema` + `src_table` (FQN) |

---

## 5. Validation Options

Set `validation_mode` per request in the onboarding table:

| Mode | What it checks | When to use |
|------|---------------|-------------|
| `NONE` | Nothing | Large tables, low-risk, speed over accuracy |
| `COUNT` | Row count matches within tolerance % | Default for most tables |
| `CHECKSUM` | `SUM(xxhash64(*))` fingerprint matches | Critical tables, detect silent corruption |
| `COLUMN_SAMPLE` | Count/Min/Max/Avg/NullCount per column | Numeric columns, range validation |
| `ALL` | COUNT + CHECKSUM + COLUMN_SAMPLE | Financial, compliance, high-confidence required |

**Validation column examples** (`validation_columns` field):

```
patients table   → validation_mode=CHECKSUM, validation_columns=patient_id,dob,insurance_id
claims table     → validation_mode=ALL,      validation_columns=claim_id,amount_paid,status
large_fact       → validation_mode=COUNT,    count_tolerance_pct=0.001   (allow 0.001%)
pii_table        → validation_mode=NONE      (policy: no query on PII)
```

---

## 6. Run as a Databricks Job

### Step 1 — Add a copy request to the onboarding table

On the **target** workspace, insert a row into `clone_onboarding`:

```sql
-- Example: clone one schema with COUNT validation
INSERT INTO azure_uc_demo_region1.deepclone_meta.clone_onboarding VALUES (
  uuid(),                              -- onboarding_id (auto)
  'Finance Schema DR — July 2026',     -- request_name
  'schema',                            -- scope_mode: catalog | schema | table
  'path_resolution',                   -- clone_mode
  'azure_uc_demo_region2',             -- src_catalog
  'finance',                           -- src_schema
  NULL,                                -- src_table (NULL for schema scope)
  'azure_uc_demo_region1',             -- tgt_catalog
  'finance_dr',                        -- tgt_schema
  1,                                   -- priority (1=High)
  'PENDING',                           -- status — always PENDING for new requests
  'your.email@company.com',            -- requested_by
  current_timestamp(),                 -- requested_at
  NULL,                                -- scheduled_for
  NULL,                                -- execution_id (set by engine)
  NULL,                                -- completed_at (set by engine)
  'COUNT',                             -- validation_mode
  NULL,                                -- validation_columns (NULL = all)
  0.0,                                 -- count_tolerance_pct (0.0 = exact)
  'XXHASH64',                          -- checksum_algorithm
  'Finance DR run',                    -- notes
  map('team', 'finance', 'env', 'dr')  -- tags
);
```

**Quick reference — other scope examples:**

```sql
-- Single table with full validation
INSERT INTO azure_uc_demo_region1.deepclone_meta.clone_onboarding VALUES (
  uuid(), 'transactions_fact Refresh', 'table', 'path_resolution',
  'azure_uc_demo_region2', 'finance', 'azure_uc_demo_region2.finance.transactions_fact',
  'azure_uc_demo_region1', 'finance_dr', 2, 'PENDING',
  'your.email@company.com', current_timestamp(), NULL, NULL, NULL,
  'ALL', 'transaction_id,amount,currency', 0.0, 'SHA256',
  'Full validation on financial table', map('env','dr')
);

-- Full catalog clone, COUNT only
INSERT INTO azure_uc_demo_region1.deepclone_meta.clone_onboarding VALUES (
  uuid(), 'Full Catalog DR', 'catalog', 'path_resolution',
  'azure_uc_demo_region2', NULL, NULL,
  'azure_uc_demo_region1', NULL, 3, 'PENDING',
  'your.email@company.com', current_timestamp(), NULL, NULL, NULL,
  'COUNT', NULL, 0.01, 'XXHASH64', NULL, map('env','dr')
);
```

---

### Step 2 — Open the Databricks Jobs UI

Go to the **source workspace**:

```
https://adb-7405609899028573.13.azuredatabricks.net/jobs/346238815508224
```

Or navigate: **Workflows → Jobs → DeepClone CrossRegion**

---

### Step 3 — Trigger a manual run

1. Click **Run now** on the job page.
2. The job picks up all `PENDING` rows from the onboarding table automatically.
3. No parameters needed — credentials are pre-configured in the job cluster's `spark_env_vars`.

To trigger via REST API:

```bash
curl -X POST \
  https://adb-7405609899028573.13.azuredatabricks.net/api/2.1/jobs/run-now \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"job_id": 346238815508224}'
```

---

### Step 4 — Monitor the run

**In the UI**: Jobs → DeepClone CrossRegion → click the latest run.

**Via SQL** on the target workspace:

```sql
-- Live queue status
SELECT status, COUNT(*) AS n
FROM azure_uc_demo_region1.deepclone_meta.clone_onboarding
GROUP BY status;

-- Latest run results
SELECT
  a.table_name,
  a.status              AS clone_status,
  a.validation_status,
  a.duration_seconds,
  a.records_cloned,
  ROUND(a.throughput_mbps, 3) AS throughput_mbps,
  SUBSTRING(a.validation_notes, 1, 200) AS val_notes,
  a.error_message
FROM azure_uc_demo_region1.deepclone_meta.deep_clone_audit_log a
WHERE a.execution_id = (
  SELECT execution_id FROM azure_uc_demo_region1.deepclone_meta.deep_clone_audit_log
  ORDER BY run_timestamp DESC LIMIT 1
)
ORDER BY a.run_timestamp;

-- Batch performance summary
SELECT
  execution_id,
  COUNT(*)                             AS tables_attempted,
  SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS success,
  SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS failed,
  SUM(CASE WHEN validation_status='PASS' THEN 1 ELSE 0 END) AS val_pass,
  SUM(CASE WHEN validation_status='FAIL' THEN 1 ELSE 0 END) AS val_fail,
  ROUND(SUM(size_bytes)/1073741824.0, 3) AS total_gb,
  SUM(duration_seconds)                  AS total_seconds
FROM azure_uc_demo_region1.deepclone_meta.deep_clone_audit_log
GROUP BY execution_id
ORDER BY MIN(run_timestamp) DESC
LIMIT 10;
```

---

### Step 5 — Verify cloned tables

```sql
-- Row count comparison (same metastore example)
SELECT
  'patients'   AS tbl,
  (SELECT COUNT(*) FROM azure_uc_demo_region1.deepclone_src.patients)  AS src_rows,
  (SELECT COUNT(*) FROM azure_uc_demo_region1.deepclone_tgt.patients)  AS tgt_rows
UNION ALL
SELECT 'claims',
  (SELECT COUNT(*) FROM azure_uc_demo_region1.deepclone_src.claims),
  (SELECT COUNT(*) FROM azure_uc_demo_region1.deepclone_tgt.claims);

-- Spot-check schema match
DESCRIBE TABLE EXTENDED azure_uc_demo_region1.deepclone_tgt.patients;
```

---

### Step 6 — Schedule recurring runs (optional)

In the Job UI: **Edit → Schedule → Scheduled** → set your cron.

Or via API:

```bash
curl -X POST \
  https://adb-7405609899028573.13.azuredatabricks.net/api/2.1/jobs/update \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": 346238815508224,
    "new_settings": {
      "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED"
      }
    }
  }'
```

---

## 7. Onboarding Table

The `clone_onboarding` table is the **only input interface** to the utility.

| Field | Required | Description |
|-------|----------|-------------|
| `scope_mode` | Yes | `catalog` / `schema` / `table` |
| `clone_mode` | Yes | `path_resolution` (default) / `delta_share` |
| `src_catalog` | Yes | Source Unity Catalog name |
| `src_schema` | If schema/table scope | Source schema name |
| `src_table` | If table scope | Fully-qualified table name |
| `tgt_catalog` | Yes | Target Unity Catalog name |
| `tgt_schema` | Optional | Target schema (defaults to `deepclone_tgt`) |
| `priority` | Yes | `1`=High, `2`=Medium, `3`=Low (lower = runs first) |
| `status` | Yes | Always insert as `PENDING` |
| `validation_mode` | Optional | `NONE` / `COUNT` / `CHECKSUM` / `COLUMN_SAMPLE` / `ALL` |
| `validation_columns` | Optional | Comma-separated column names for validation |
| `count_tolerance_pct` | Optional | Row count tolerance %, default `0.0` (exact) |
| `checksum_algorithm` | Optional | `XXHASH64` (default) or `SHA256` |

**Re-queue a failed request:**

```sql
UPDATE azure_uc_demo_region1.deepclone_meta.clone_onboarding
SET status = 'PENDING', execution_id = NULL, completed_at = NULL,
    notes = 'Re-queued after root cause fix — July 2026'
WHERE onboarding_id = '<uuid>';
```

---

## 8. Monitor a Run

| What | Where |
|------|-------|
| Job UI | `https://adb-7405609899028573.13.azuredatabricks.net/jobs/346238815508224` |
| Audit log | `azure_uc_demo_region1.deepclone_meta.deep_clone_audit_log` |
| Onboarding queue | `azure_uc_demo_region1.deepclone_meta.clone_onboarding` |
| Cloned data | `azure_uc_demo_region1.deepclone_tgt.*` |
| Dashboard | Open `deepclone_dashboard.html` in any browser |

**Email alerts**: The job is configured to email `vivek.ravichandiran@databricks.com` on failure.

---

## 9. Troubleshoot Common Errors

| Error | Root cause | Fix |
|-------|-----------|-----|
| `TABLE_OR_VIEW_NOT_FOUND` for cross-workspace FQN | Target warehouse doesn't see source metastore | Switch to `path_resolution` mode; ensure ADLS external location registered on target |
| `LOCATION_OVERLAP` | Path-based clone on a managed UC table in same metastore | Utility auto-switches to FQN form for same-metastore; check `SRC_URL == TGT_URL` env |
| `CLOUD_PROVIDER_RESOURCE_STOCKOUT` | Azure VM SKU unavailable in region | Change `node_type_id` in job cluster config (e.g. `Standard_D4s_v3`) |
| `PERMISSION_DENIED on CREATE SCHEMA` | SP lacks privileges | Grant `USE CATALOG`, `CREATE SCHEMA` to SP on target catalog |
| `validation_status = FAIL` | Data drift detected post-clone | Query `validation_notes` in audit log for exact diff; re-run if transient |
| `TIMEOUT` | SQL warehouse cold-start > 50s | Pre-start warehouse before job or use Serverless warehouse |
| `NO_PENDING_REQUESTS` job exit | All rows are IN_PROGRESS or COMPLETED | Previous run marked rows IN_PROGRESS; reset stuck rows manually |

**Reset stuck IN_PROGRESS rows:**

```sql
UPDATE azure_uc_demo_region1.deepclone_meta.clone_onboarding
SET status = 'PENDING', execution_id = NULL
WHERE status = 'IN_PROGRESS'
  AND datediff(current_timestamp(), requested_at) > 1;  -- stuck > 1 day
```

---

## 10. File Reference

```
DeepcloneCrossRegion/
├── README.md                       ← This file (see stale-content banner above)
├── databricks.yml                  ← Databricks Asset Bundle root config (variables, targets, secrets doc)
├── configs/
│   ├── migration.yaml              ← THE canonical config file (CSV delegation or catalog/schema/table mappings)
│   └── csv_test_ril_bulk_02.csv    ← Example CSV table-mapping file
├── orchestrator/                   ← Core orchestration logic (importable package)
│   ├── config.py                   ← OrchestratorConfig, load_from_yaml, validate_config
│   ├── input_resolver.py           ← Resolves table selection: JOB / YAML / CSV
│   ├── inventory_manager.py        ← Onboards/upserts rows into migration_control
│   ├── audit_manager.py            ← State-machine transitions, attempt history, validation history
│   ├── validator.py                ← Post-clone validation (existence, size, row count)
│   ├── models.py                   ← Dataclasses (TableSelection, RunSummary, ValidationResult)
│   └── sql_client.py / api_client.py
├── notebooks/
│   ├── orchestrator_notebook.py    ← Main entrypoint (INVENTORY/DRY_RUN/DEEP_CLONE/VALIDATE/RETRY)
│   ├── chunk_worker_notebook.py    ← Ephemeral per-chunk cluster worker (runs the actual CLONE)
│   └── setup_control_tables.py     ← One-time DDL for migration_control / migration_attempts / migration_validation_history
├── resources/                      ← Bundle job definitions (included by databricks.yml)
│   ├── 00_setup_control_tables.yml
│   ├── 01_inventory_job.yml
│   ├── 02_dry_run_job.yml
│   ├── 03_deep_clone_job.yml
│   ├── 04_validate_job.yml
│   ├── 05_retry_job.yml
│   └── 06_full_migration_workflow.yml   ← End-to-end: inventory → deep_clone → validate
├── docs/
│   ├── SOP_CSV_Run.md               ← CURRENT step-by-step SOP (CSV + delta_share)
│   └── SOP_Onboarding.md            ← Superseded — see banner in that file
├── scripts/                         ← Ad-hoc/setup helpers (not part of the deployed bundle)
└── tests/                           ← Ad-hoc test/report scripts (not part of the deployed bundle)
```
