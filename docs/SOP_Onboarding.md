# DeepClone CrossRegion — Standard Operating Procedure (SOP)

> ⚠️ **SUPERSEDED.** This document describes an earlier `config.json`-based
> architecture (`clone_mode`, `scope_mode`, the onboarding table, etc.) that
> has since been replaced by the Databricks Asset Bundle in this repo
> (`databricks.yml` + `resources/*.yml` + `orchestrator/*.py`). `config.json`
> itself has been removed as unused/legacy. For the current procedure, see
> [`SOP_CSV_Run.md`](SOP_CSV_Run.md). Kept here for historical reference only.

**Document ID:** SOP-DCR-001  
**Version:** 2.5.0  
**Owner:** Data Platform Engineering  
**Last Reviewed:** July 2026  
**Classification:** Internal — Data Engineering

---

## Table of Contents

1. [Purpose & Scope](#1-purpose--scope)
2. [Prerequisites Checklist](#2-prerequisites-checklist)
3. [Clone Mode Selection Guide](#3-clone-mode-selection-guide)
4. [Scope Selection: Catalog vs Schema vs Table](#4-scope-selection-catalog-vs-schema-vs-table)
5. **[Onboarding Table — Self-Service Request Model](#5-onboarding-table--self-service-request-model)** *(New in v2.5)*
6. [SOP — Onboarding a Full Catalog](#6-sop--onboarding-a-full-catalog)
7. [SOP — Onboarding Specific Schemas](#7-sop--onboarding-specific-schemas)
8. [SOP — Onboarding an Explicit Table List](#8-sop--onboarding-an-explicit-table-list)
9. [External Table Onboarding](#9-external-table-onboarding)
10. [Delta Share Mode Setup (Option 1)](#10-delta-share-mode-setup-option-1)
11. [Path Resolution Mode Setup (Option 2)](#11-path-resolution-mode-setup-option-2)
12. [Dry-Run: Mandatory Pre-Flight Step](#12-dry-run-mandatory-pre-flight-step)
13. [Executing the Live Clone](#13-executing-the-live-clone)
14. [Post-Clone Validation](#14-post-clone-validation)
15. [Audit Log Reference](#15-audit-log-reference)
16. [Troubleshooting & FAQ](#16-troubleshooting--faq)
17. [Rollback Procedure](#17-rollback-procedure)
18. [Change Log](#18-change-log)

---

## 1. Purpose & Scope

This SOP defines the end-to-end procedure for onboarding new data assets — Catalogs, Schemas, or explicit Table lists — into the **DeepClone CrossRegion** utility for secure, production-grade data copy from a **Source Azure Databricks workspace** to a **Target Azure Databricks workspace** using Delta DEEP CLONE.

**Primary transfer mechanism:** Delta DEEP CLONE (full physical copy of Parquet files + Delta transaction log).

**This SOP covers:**
- Onboarding a new catalog for full replication
- Onboarding one or more schemas within a catalog
- Onboarding a hand-curated list of specific tables
- Handling managed and external tables
- Mandatory dry-run validation before any live execution
- Post-clone verification and audit sign-off

**This SOP does not cover:**
- Initial workspace setup or network/VNET configuration
- Unity Catalog metastore creation
- Storage account (ADLS Gen2) provisioning

---

## 2. Prerequisites Checklist

Complete all items before onboarding any scope. An incomplete checklist will result in failed or partial clones.

### 2.1 Source Workspace

| # | Requirement | How to Verify |
|---|-------------|---------------|
| 1 | Source Service Principal (`sp-migrate-src`) exists in Azure AD | Azure Portal → App Registrations |
| 2 | SP has **USE CATALOG** privilege on source catalog | `SHOW GRANTS ON CATALOG <name>` |
| 3 | SP has **USE SCHEMA** privilege on all source schemas to clone | `SHOW GRANTS ON SCHEMA <name>` |
| 4 | SP has **SELECT** privilege on all source tables | `SHOW GRANTS ON TABLE <name>` |
| 5 | SP has **CAN USE** on at least one running SQL Warehouse (Option 2 only) | Databricks SQL → Warehouses → Permissions |
| 6 | SP client secret is set as environment variable `AZ2AZ_SRC_SECRET` | `echo $AZ2AZ_SRC_SECRET` (non-empty) |
| 7 | Source tables are in **DELTA format** (not CSV/Parquet/ORC) | `DESCRIBE DETAIL catalog.schema.table` → format = DELTA |

### 2.2 Target Workspace

| # | Requirement | How to Verify |
|---|-------------|---------------|
| 1 | Target Service Principal (`sp-migrate-tgt`) exists in Azure AD | Azure Portal → App Registrations |
| 2 | SP has **CREATE CATALOG** privilege (if target catalog doesn't exist) | `SHOW GRANTS ON METASTORE` |
| 3 | SP has **CREATE SCHEMA** privilege on target catalog | `SHOW GRANTS ON CATALOG <tgt_catalog>` |
| 4 | SP has **CREATE TABLE** privilege on target schemas | Grant before run |
| 5 | SP has **CAN MANAGE RUNS** permission on Jobs (for job submission) | Workspace Settings → Service Principals |
| 6 | SP client secret is set as environment variable `AZ2AZ_TGT_SECRET` | `echo $AZ2AZ_TGT_SECRET` (non-empty) |
| 7 | Target ADLS Gen2 storage account is accessible by target SP | Test `abfss://` path read/write |
| 8 | Wrapper notebook uploaded to `/Shared/deepclone_utility/wrapper_notebook` | Databricks Workspace browser |

### 2.3 Shared Infrastructure

| # | Requirement | Notes |
|---|-------------|-------|
| 1 | Source ADLS Gen2 is readable by the **target** SP's managed identity / RBAC | Required for path_resolution mode |
| 2 | External path mappings configured in `config.json` | For external tables only |
| 3 | `utility_metadata` catalog exists (or SP can create it) on target | Audit logs write here |
| 4 | Python `requests` library available on the orchestration host | `python3 -c "import requests"` |

---

## 3. Clone Mode Selection Guide

Choose the correct clone mode **before** editing `config.json`. This is the most impactful architectural decision.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CLONE MODE DECISION TREE                             │
│                                                                         │
│   Is Delta Sharing already configured between the two metastores?       │
│                                                                         │
│       YES ──────────────────────────────────────────────────────►  ─── │
│                                                          Option 1:      │
│                                                          delta_share    │
│       NO  ──────────┐                                                   │
│                     │                                                   │
│   Does the target SP have ADLS RBAC read on source storage?             │
│                     │                                                   │
│       YES ──────────┴────────────────────────────────────────────►  ─── │
│                                                          Option 2:      │
│                                                          path_resolution│
│       NO  ──► Stop. Configure ADLS RBAC first.                          │
└─────────────────────────────────────────────────────────────────────────┘
```

| Criterion | Option 1: `delta_share` | Option 2: `path_resolution` |
|-----------|------------------------|------------------------------|
| **Source reference in SQL** | `` `delta_share_catalog`.`schema`.`table` `` | `delta.\`abfss://resolved-path\`` |
| **Path discovery** | None — Delta Sharing handles it | `DESCRIBE DETAIL` via SQL Statement API |
| **Requires SQL Warehouse** | No | Yes (source workspace) |
| **Raw ADLS path exposed** | No | Yes (in job task parameters) |
| **Best for** | Delta Sharing already set up | Direct storage access, no Sharing config |
| **`config.json` key** | `"clone_mode": "delta_share"` | `"clone_mode": "path_resolution"` |

---

## 4. Scope Selection: Catalog vs Schema vs Table

The utility supports three levels of granularity. Set `execution.scope_mode` in `config.json`.

### Scope Mode Summary

| `scope_mode` | What gets cloned | Required fields |
|---|---|---|
| `catalog` | Every non-system schema and table in the source catalog | `source.catalog` |
| `schema` | Specific schemas within a catalog | `source.catalog` + `source.schemas` (array) |
| `table` | An explicit curated list of fully-qualified tables | `source.tables` (array) |

> **Rule:** For `path_resolution` mode, only `scope_mode: table` is supported (each table needs individual `DESCRIBE DETAIL` resolution). For `delta_share` mode, all three scope modes are supported.

---

## 5. Onboarding Table — Self-Service Request Model

> **New in v2.5.** This is the preferred onboarding method for teams. It replaces direct `config.json` edits for routine copy requests.

### 5.1 Overview

Instead of editing `config.json` for every new clone scope, teams insert rows into a **Delta-backed onboarding table**. The orchestrator reads this table on each run, processes all `PENDING` requests in priority order, and writes status and execution details back to the same table.

```
utility_metadata.default.clone_onboarding   ← teams write here (INSERT)
utility_metadata.default.deep_clone_audit_log ← system writes here (metrics per table)
```

Both tables live in the **target workspace** under the `utility_metadata` catalog. Any team with `INSERT` privilege on `clone_onboarding` can self-serve a copy request.

### 5.2 Onboarding Table Schema

```sql
CREATE TABLE IF NOT EXISTS utility_metadata.default.clone_onboarding (
  onboarding_id   STRING      NOT NULL COMMENT 'UUID — auto-generated by requester',
  request_name    STRING      NOT NULL COMMENT 'Human-readable name for this copy request',
  scope_mode      STRING      NOT NULL COMMENT 'catalog | schema | table',
  clone_mode      STRING      NOT NULL COMMENT 'delta_share | path_resolution',
  src_catalog     STRING      NOT NULL COMMENT 'Source Unity Catalog name',
  src_schema      STRING               COMMENT 'Source schema — null for scope_mode=catalog',
  src_table       STRING               COMMENT 'Fully-qualified table — null for catalog/schema scope',
  tgt_catalog     STRING      NOT NULL COMMENT 'Target Unity Catalog name',
  tgt_schema      STRING               COMMENT 'Target schema override — null = mirror source schema name',
  priority        INT         NOT NULL COMMENT '1=High (runs first), 2=Medium, 3=Low',
  status          STRING      NOT NULL COMMENT 'PENDING | IN_PROGRESS | COMPLETED | FAILED | SKIPPED',
  requested_by    STRING      NOT NULL COMMENT 'Email or service account of requester',
  requested_at    TIMESTAMP   NOT NULL COMMENT 'Request submission timestamp',
  scheduled_for   TIMESTAMP            COMMENT 'Earliest run time — null = run immediately',
  execution_id    STRING               COMMENT 'Filled by orchestrator when run starts',
  completed_at    TIMESTAMP            COMMENT 'Filled when status reaches terminal state',
  notes           STRING               COMMENT 'Free-text notes, business justification',
  tags            MAP<STRING,STRING>   COMMENT 'Key-value labels: team, project, cost_centre, etc.'
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

### 5.3 How to Submit a Clone Request

#### Submitting a full catalog clone

```sql
INSERT INTO utility_metadata.default.clone_onboarding VALUES (
  uuid(),
  'Finance Catalog Q3 DR',           -- request_name
  'catalog',                          -- scope_mode
  'path_resolution',                  -- clone_mode
  'catalog_prod',                     -- src_catalog
  NULL,                               -- src_schema  (null = all schemas)
  NULL,                               -- src_table   (null = all tables)
  'catalog_prod_dr',                  -- tgt_catalog
  NULL,                               -- tgt_schema  (mirror source)
  1,                                  -- priority    (High)
  'PENDING',                          -- status
  'finance-team@company.com',         -- requested_by
  current_timestamp(),                -- requested_at
  NULL,                               -- scheduled_for (run immediately)
  NULL,                               -- execution_id  (filled by system)
  NULL,                               -- completed_at  (filled by system)
  'Q3 regulatory DR requirement',     -- notes
  map('team','finance','cost_centre','FIN-001')  -- tags
);
```

#### Submitting specific schemas

```sql
-- Insert one row per schema
INSERT INTO utility_metadata.default.clone_onboarding
SELECT
  uuid()               AS onboarding_id,
  concat('Schema DR: ', schema_name) AS request_name,
  'schema'             AS scope_mode,
  'path_resolution'    AS clone_mode,
  'catalog_prod'       AS src_catalog,
  schema_name          AS src_schema,
  NULL                 AS src_table,
  'catalog_prod_dr'    AS tgt_catalog,
  schema_name          AS tgt_schema,   -- mirror same name
  2                    AS priority,
  'PENDING'            AS status,
  'data-eng@company.com' AS requested_by,
  current_timestamp()  AS requested_at,
  NULL AS scheduled_for, NULL AS execution_id, NULL AS completed_at,
  'Schema-level DR batch' AS notes,
  map('team','data-eng') AS tags
FROM (VALUES ('sales'), ('finance'), ('operations')) t(schema_name);
```

#### Submitting an explicit table list

```sql
INSERT INTO utility_metadata.default.clone_onboarding
SELECT
  uuid(), 'Critical Tables Refresh', 'table', 'path_resolution',
  split(fqn, '\\.')[0],   -- src_catalog
  split(fqn, '\\.')[1],   -- src_schema
  fqn,                    -- src_table (fully qualified)
  'catalog_prod_dr',
  split(fqn, '\\.')[1],   -- mirror schema
  1, 'PENDING', 'ops-team@company.com', current_timestamp(),
  NULL, NULL, NULL, 'Weekly critical table refresh', map('team','ops')
FROM (
  VALUES
    ('catalog_prod.sales.transactions_fact'),
    ('catalog_prod.finance.revenue_monthly'),
    ('catalog_prod.operations.inventory_snapshot')
) t(fqn);
```

### 5.4 How the Orchestrator Processes the Queue

When the wrapper notebook runs in **orchestrator mode** (no `table_group` parameter), it:

1. Reads all `PENDING` rows from `clone_onboarding` ordered by `priority ASC, requested_at ASC`
2. Skips rows where `scheduled_for IS NOT NULL AND scheduled_for > current_timestamp()`
3. Marks each row `IN_PROGRESS` and sets `execution_id` (MERGE, idempotent)
4. Groups rows by `(scope_mode, clone_mode)` into execution batches
5. For each batch: discovers tables → resolves paths → load-balances across clusters → submits job
6. On completion: MERGEs `status = COMPLETED/FAILED` and `completed_at` back into the onboarding table
7. All per-table metrics are written to `deep_clone_audit_log` with `onboarding_id` linkage

### 5.5 Monitoring the Onboarding Queue

```sql
-- Current queue state
SELECT
  priority,
  status,
  COUNT(*)        AS request_count,
  MIN(requested_at) AS oldest_request
FROM utility_metadata.default.clone_onboarding
GROUP BY priority, status
ORDER BY priority, status;

-- Requests in flight right now
SELECT onboarding_id, request_name, requested_by, execution_id, requested_at
FROM utility_metadata.default.clone_onboarding
WHERE status = 'IN_PROGRESS';

-- Failed requests with error context (joined to audit)
SELECT
  o.onboarding_id, o.request_name, o.requested_by,
  a.table_name, a.error_message, a.error_stack_trace
FROM utility_metadata.default.clone_onboarding o
JOIN utility_metadata.default.deep_clone_audit_log a
  ON o.execution_id = a.execution_id
WHERE o.status = 'FAILED'
  AND a.status  = 'FAILED'
ORDER BY o.requested_at DESC;
```

### 5.6 Re-queuing Failed Requests

To re-run a failed onboarding request without inserting a new row:

```sql
UPDATE utility_metadata.default.clone_onboarding
SET status       = 'PENDING',
    execution_id = NULL,
    completed_at = NULL,
    notes        = concat(notes, ' | Re-queued: ', current_timestamp())
WHERE onboarding_id = '<failed-onboarding-id>';
```

### 5.7 Granting Team Access

```sql
-- Allow any team to submit clone requests (insert only)
GRANT INSERT ON TABLE utility_metadata.default.clone_onboarding
TO `team-finance`, `team-data-eng`, `team-ops`;

-- Allow read for monitoring dashboards
GRANT SELECT ON TABLE utility_metadata.default.clone_onboarding
TO `team-finance`, `team-data-eng`, `team-ops`;

-- Only the utility SP writes status back
-- sp-migrate-tgt already has full table access via catalog grant
```

---

## 6. SOP — Onboarding a Full Catalog

Use this procedure when you want to replicate an entire source catalog to the target workspace.

### Step 1: Edit `config.json`

```json
{
  "execution": {
    "scope_mode": "catalog",
    "clone_mode": "delta_share",
    "dry_run": true,
    "max_clusters": 8,
    "max_workers_per_cluster": 6
  },
  "source": {
    "workspace_url": "https://adb-<source-id>.azuredatabricks.net",
    "client_id": "<source-sp-client-id>",
    "client_secret_env": "AZ2AZ_SRC_SECRET",
    "catalog": "catalog_prod",
    "schemas": null,
    "tables": null
  },
  "target": {
    "workspace_url": "https://adb-<target-id>.azuredatabricks.net",
    "client_id": "<target-sp-client-id>",
    "client_secret_env": "AZ2AZ_TGT_SECRET",
    "catalog": "catalog_prod_dr"
  }
}
```

**Key fields:**
- `scope_mode` → `"catalog"`
- `source.catalog` → exact name of the source catalog
- `source.schemas` → **must be `null`** (all schemas discovered automatically)
- `source.tables` → **must be `null`**
- `target.catalog` → name of the catalog to create/populate on the target

### Step 2: Set environment variables

```bash
export AZ2AZ_SRC_SECRET="<source-sp-client-secret>"
export AZ2AZ_TGT_SECRET="<target-sp-client-secret>"
```

> **Security Rule:** Never paste secrets into `config.json`. Always use environment variables.

### Step 3: Run dry-run (mandatory — see §11)

### Step 4: Review blueprint, fix warnings, re-run dry-run if needed

### Step 5: Set `"dry_run": false` and execute live clone (see §12)

### Step 6: Run post-clone validation (see §13)

---

## 6. SOP — Onboarding Specific Schemas

Use this when you need to copy one or more schemas but not the entire catalog.

### Step 1: Edit `config.json`

```json
{
  "execution": {
    "scope_mode": "schema",
    "clone_mode": "delta_share",
    "dry_run": true
  },
  "source": {
    "catalog": "catalog_prod",
    "schemas": ["sales", "finance", "operations"],
    "tables": null
  },
  "target": {
    "catalog": "catalog_prod_dr"
  }
}
```

**Key fields:**
- `scope_mode` → `"schema"`
- `source.schemas` → JSON array of schema name strings — **exact case-sensitive names**
- `source.tables` → **must be `null`**

### Step 2: Verify schema names exist on source

```sql
-- Run on source workspace
SHOW SCHEMAS IN catalog_prod;
```

Confirm every name in `source.schemas` appears in the result before proceeding.

### Step 3: Check schema-level grants for source SP

```sql
-- Run for each schema being onboarded
SHOW GRANTS ON SCHEMA catalog_prod.sales;
-- Expected: sp-migrate-src has USE SCHEMA and SELECT
```

If missing, run as a metastore admin:

```sql
GRANT USE SCHEMA ON SCHEMA catalog_prod.sales TO `sp-migrate-src`;
GRANT SELECT ON SCHEMA catalog_prod.sales TO `sp-migrate-src`;
```

### Step 4: Run dry-run, review, then execute (§11 → §12 → §13)

---

## 7. SOP — Onboarding an Explicit Table List

Use this for targeted migrations: specific high-value tables, cherry-picked across schemas, or one-off back-fills.

### Step 1: Build the table list

Create your list of fully-qualified table names. Format: `catalog.schema.table`.

```json
{
  "execution": {
    "scope_mode": "table",
    "clone_mode": "path_resolution",
    "dry_run": true
  },
  "source": {
    "catalog": "catalog_prod",
    "schemas": null,
    "tables": [
      "catalog_prod.sales.transactions_fact",
      "catalog_prod.finance.revenue_monthly",
      "catalog_prod.operations.inventory_snapshot",
      "catalog_prod.analytics.customer_segments"
    ],
    "sql_warehouse_id": "<source-sql-warehouse-id>"
  },
  "target": {
    "catalog": "catalog_prod_dr"
  }
}
```

**Key fields:**
- `scope_mode` → `"table"`
- `source.tables` → array of fully-qualified names (must be exact, case-sensitive)
- `source.sql_warehouse_id` → **required** for `path_resolution` mode — the SQL Warehouse ID used to run `DESCRIBE DETAIL`
- `source.schemas` → **must be `null`**

### Step 2: Validate every table exists on source

```sql
-- Run for each table in the list
DESCRIBE DETAIL catalog_prod.sales.transactions_fact;
-- Expected: format = DELTA, location = abfss://...
```

Tables that return errors or show non-Delta format must be **excluded** from the list.

### Step 3: Find your SQL Warehouse ID (for path_resolution mode)

In the source workspace:

```
Databricks UI → SQL → SQL Warehouses → click your warehouse → copy ID from URL
# URL format: /sql/warehouses/<WAREHOUSE_ID>
```

Or via API:

```bash
curl -s "$SRC_URL/api/2.0/sql/warehouses" \
  -H "Authorization: Bearer $SRC_TOKEN" | python3 -m json.tool | grep -E '"id"|"name"'
```

Add the ID to `source.sql_warehouse_id` in `config.json`.

### Step 4: Run dry-run (§11) to validate all paths resolve

The dry-run for `table` + `path_resolution` mode calls `DESCRIBE DETAIL` on every listed table and outputs the full DEEP CLONE SQL with resolved `abfss://` paths. Review carefully.

### Step 5: Execute and validate (§12 → §13)

---

## 8. External Table Onboarding

External tables require additional configuration because their storage location must be remapped from source ADLS to target ADLS.

### Step 1: Identify which tables are EXTERNAL

```sql
-- Run on source workspace
SELECT table_name, table_type, location
FROM information_schema.tables
WHERE table_catalog = 'catalog_prod'
  AND table_schema  = 'sales'
  AND table_type    = 'EXTERNAL';
```

Or via `DESCRIBE DETAIL`:

```sql
DESCRIBE DETAIL catalog_prod.sales.product_catalog;
-- Check: "type" field = "EXTERNAL"
-- Check: "location" field = abfss://srcdata@srcaccount.dfs.core.windows.net/...
```

### Step 2: Configure path mapping rules

For every distinct source ADLS prefix used by external tables, add a mapping rule to `config.json`:

```json
"external_path_mappings": [
  {
    "src_prefix": "abfss://srcdata@srcadlsaccount.dfs.core.windows.net",
    "tgt_prefix": "abfss://tgtdata@tgtadlsaccount.dfs.core.windows.net"
  },
  {
    "src_prefix": "abfss://archive@srcadlsaccount.dfs.core.windows.net",
    "tgt_prefix": "abfss://archive@tgtadlsaccount.dfs.core.windows.net"
  }
]
```

**Rule:** The utility replaces the `src_prefix` with `tgt_prefix` in the resolved location to produce the `LOCATION` clause. If no rule matches, the table is cloned without an explicit `LOCATION` clause and a warning is logged.

### Step 3: Register target External Locations in Unity Catalog

Before cloning external tables, the target workspace must have External Locations registered for all target ADLS paths:

```sql
-- Run on TARGET workspace as metastore admin
CREATE EXTERNAL LOCATION IF NOT EXISTS tgt_data_location
URL 'abfss://tgtdata@tgtadlsaccount.dfs.core.windows.net'
WITH (STORAGE CREDENTIAL `tgt-storage-credential`);

-- Grant SP access
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION tgt_data_location
TO `sp-migrate-tgt`;
```

### Step 4: Verify target SP can write to external paths

```bash
# Simple test — upload a file
az storage blob upload \
  --account-name tgtadlsaccount \
  --container-name tgtdata \
  --name deepclone_test.txt \
  --data "test" \
  --auth-mode login
```

### Step 5: Proceed with dry-run (§11) — review the LOCATION clauses

In dry-run output, confirm every external table shows a correct `LOCATION` clause with the remapped target path.

---

## 9. Delta Share Mode Setup (Option 1)

Perform these steps once per catalog onboarding cycle when using `clone_mode: delta_share`.

### Step 9.1: Create a Delta Share on the source metastore

Run as a metastore admin or share owner on the **source workspace**:

```sql
-- Create the share
CREATE SHARE IF NOT EXISTS src_catalog_share
COMMENT 'Deep clone share for catalog_prod migration';

-- Add all tables in a schema to the share
ALTER SHARE src_catalog_share
ADD SCHEMA catalog_prod.sales;

-- Or add individual tables
ALTER SHARE src_catalog_share
ADD TABLE catalog_prod.finance.revenue_monthly;

-- Verify
SHOW ALL IN SHARE src_catalog_share;
```

### Step 9.2: Register the target metastore as a recipient

```sql
-- Get the target metastore sharing identifier
-- Run on TARGET workspace:
SELECT current_metastore();

-- Run on SOURCE workspace — create recipient using the target's metastore ID
CREATE RECIPIENT IF NOT EXISTS tgt_metastore_recipient
USING ID '<target-metastore-sharing-identifier>';

-- Grant recipient access to the share
GRANT SELECT ON SHARE src_catalog_share TO RECIPIENT tgt_metastore_recipient;
```

### Step 9.3: Mount the shared catalog on the target workspace

Run on the **target workspace**:

```sql
-- Create the shared catalog reference
CREATE CATALOG IF NOT EXISTS delta_share_catalog
USING SHARE <source-metastore-id>.src_catalog_share;

-- Verify tables are visible
SHOW SCHEMAS IN delta_share_catalog;
SHOW TABLES IN delta_share_catalog.sales;
```

### Step 9.4: Update `config.json`

```json
"delta_share": {
  "provider_name": "<source-metastore-id>",
  "share_name": "src_catalog_share",
  "shared_catalog_name": "delta_share_catalog"
}
```

### Step 9.5: Grant target SP access to the shared catalog

```sql
-- Run on target workspace
GRANT USE CATALOG ON CATALOG delta_share_catalog TO `sp-migrate-tgt`;
GRANT USE SCHEMA  ON CATALOG delta_share_catalog TO `sp-migrate-tgt`;
GRANT SELECT      ON CATALOG delta_share_catalog TO `sp-migrate-tgt`;
```

---

## 10. Path Resolution Mode Setup (Option 2)

Perform these steps when using `clone_mode: path_resolution`.

### Step 10.1: Identify and start the source SQL Warehouse

```bash
# List all warehouses and their states
curl -s "$AZ2AZ_SRC_URL/api/2.0/sql/warehouses" \
  -H "Authorization: Bearer $SRC_TOKEN" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
for w in d.get('warehouses', []):
    print(f\"{w['id']}  {w['name']:<35} {w['state']}\")
"
```

Choose the smallest warehouse sufficient for metadata queries (2X-Small is adequate for `DESCRIBE DETAIL`).

### Step 10.2: Grant source SP CAN USE on the warehouse

```
Databricks UI → SQL → Warehouses → <warehouse name> → Permissions → Add → sp-migrate-src → Can Use
```

### Step 10.3: Configure `source.sql_warehouse_id` in `config.json`

```json
"source": {
  "sql_warehouse_id": "f77ee3e31219a4e3"
}
```

### Step 10.4: Grant target SP ADLS RBAC read on source storage

In Azure Portal:

```
Storage Account (source) → Access Control (IAM) → Add role assignment
Role: Storage Blob Data Reader
Assign to: sp-migrate-tgt (managed identity or service principal)
```

This allows the **target cluster** to read Parquet files from the source ADLS path when executing DEEP CLONE.

### Step 10.5: Test connectivity

```bash
# Verify source SP can call DESCRIBE DETAIL
python3 - << 'EOF'
import requests, os

url    = os.environ["AZ2AZ_SRC_URL"]
cid    = os.environ["AZ2AZ_SRC_CID"]
secret = os.environ["AZ2AZ_SRC_SECRET"]
wh_id  = "<your-warehouse-id>"
table  = "catalog_prod.sales.transactions_fact"

tok = requests.post(f"{url}/oidc/v1/token",
    data={"grant_type":"client_credentials","client_id":cid,
          "client_secret":secret,"scope":"all-apis"}).json()["access_token"]

resp = requests.post(f"{url}/api/2.0/sql/statements",
    headers={"Authorization":f"Bearer {tok}","Content-Type":"application/json"},
    json={"statement":f"DESCRIBE DETAIL {table}","warehouse_id":wh_id,
          "wait_timeout":"30s"}).json()

print("State:", resp.get("status",{}).get("state"))
if resp.get("result",{}).get("data_array"):
    cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    row  = dict(zip(cols, resp["result"]["data_array"][0]))
    print("Location:", row.get("location"))
    print("Type:", row.get("type"))
EOF
```

Expected output:
```
State: SUCCEEDED
Location: abfss://srcdata@srcadlsaccount.dfs.core.windows.net/catalog_prod/sales/transactions_fact
Type: MANAGED
```

---

## 11. Dry-Run: Mandatory Pre-Flight Step

**A dry-run is mandatory before every live clone execution.** It does not modify any data.

### What the dry-run validates

| Check | What it does |
|---|---|
| Table discovery | Lists all tables in scope, logs count and names |
| Path resolution | Calls `DESCRIBE DETAIL` on every table (Option 2 only) |
| Type classification | Detects MANAGED vs EXTERNAL for each table |
| External location mapping | Validates every external table has a matching prefix rule |
| Target namespace | Verifies target catalog is accessible by target SP |
| Load balancing preview | Shows how tables will be distributed across clusters |
| DEEP CLONE SQL output | Prints the exact SQL that will be executed per table |

### Step 1: Enable dry-run in `config.json`

```json
{
  "execution": {
    "dry_run": true
  }
}
```

### Step 2: Run the wrapper notebook in dry-run mode

In the source Databricks workspace, open `wrapper_notebook` and set:

```
dry_run = "true"
```

Or trigger via API:

```bash
curl -X POST "$SRC_URL/api/2.1/jobs/runs/submit" \
  -H "Authorization: Bearer $SRC_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "run_name": "deepclone-dry-run",
    "tasks": [{
      "task_key": "dry_run",
      "notebook_task": {
        "notebook_path": "/Shared/deepclone_utility/wrapper_notebook",
        "base_parameters": {
          "dry_run": "true",
          "config_path": "/Shared/deepclone_utility/config.json"
        }
      },
      "existing_cluster_id": "<orchestrator-cluster-id>"
    }]
  }'
```

### Step 3: Review the dry-run blueprint

The blueprint is printed to notebook output **and** saved to:

```
utility_metadata.default.dry_run_blueprints
```

Review:

```sql
SELECT execution_id, generated_at, blueprint_json
FROM utility_metadata.default.dry_run_blueprints
ORDER BY generated_at DESC
LIMIT 1;
```

### Step 4: Sign-off criteria

Before proceeding to live execution, confirm **all** of the following:

- [ ] `paths_resolved == tables_discovered` (no unresolved paths)
- [ ] All MANAGED tables show correct source `abfss://` path
- [ ] All EXTERNAL tables show a `LOCATION` clause with the correct target path
- [ ] Cluster groups are evenly distributed (no single cluster > 2× others)
- [ ] No warnings in the summary output
- [ ] Total data size is within your network/storage budget

---

## 12. Executing the Live Clone

Only proceed after a successful, signed-off dry-run.

### Step 1: Set `"dry_run": false` in `config.json`

```json
{
  "execution": {
    "dry_run": false
  }
}
```

### Step 2: Set a meaningful execution ID (optional)

```json
{
  "execution": {
    "execution_id": "catalog_prod_dr_migration_2026_07_09"
  }
}
```

If left as `"auto"`, the utility generates `exec-YYYYMMDD-HHmmss-xxxxxx`.

### Step 3: Trigger the wrapper notebook (orchestrator mode)

Run with **no** `table_group` parameter (empty) — this activates orchestrator mode:

```
Notebook widget: table_group = <leave empty>
Notebook widget: dry_run = false
Notebook widget: config_path = /Shared/deepclone_utility/config.json
```

### Step 4: Monitor execution

**Option A — Databricks Jobs UI:**
Navigate to Workflows → Jobs → `deepclone_crossregion_<execution_id>` to see all cluster task states.

**Option B — Audit log:**

```sql
-- Live progress query (run repeatedly)
SELECT
  status,
  COUNT(*)                                   AS table_count,
  SUM(records_cloned)                        AS total_records,
  SUM(size_bytes) / 1e9                      AS total_gb,
  ROUND(AVG(duration_seconds) / 60.0, 1)    AS avg_duration_min
FROM utility_metadata.default.deep_clone_audit_log
WHERE execution_id = 'catalog_prod_dr_migration_2026_07_09'
GROUP BY status
ORDER BY status;
```

**Option C — Per-cluster breakdown:**

```sql
SELECT
  cluster_id,
  COUNT(*)        AS tables_assigned,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS done,
  SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failed,
  MIN(start_time) AS started_at,
  MAX(end_time)   AS last_update
FROM utility_metadata.default.deep_clone_audit_log
WHERE execution_id = 'catalog_prod_dr_migration_2026_07_09'
GROUP BY cluster_id
ORDER BY cluster_id;
```

### Step 5: Handle failures

Failed tables are automatically retried up to `retry_max_attempts` times. To re-run only failed tables after a completed job:

1. Query failed tables from audit log:

```sql
SELECT table_name
FROM utility_metadata.default.deep_clone_audit_log
WHERE execution_id = 'catalog_prod_dr_migration_2026_07_09'
  AND status = 'FAILED';
```

2. Add failed tables to a new `config.json` with `scope_mode: table`
3. Run a new execution (new `execution_id`) targeting only those tables

---

## 13. Post-Clone Validation

Run `test_validator.py` (or the equivalent notebook) on the **target workspace** after every live execution.

### Step 1: Run the validation notebook

```
Notebook: test_validator
Widget: execution_id = catalog_prod_dr_migration_2026_07_09
Widget: src_catalog  = catalog_prod
Widget: tgt_catalog  = catalog_prod_dr
Widget: row_sample_pct = 5
Widget: fail_fast = false
```

### Step 2: Check validation results

```sql
SELECT check_name, passed, COUNT(*) AS check_count,
       SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_count
FROM utility_metadata.default.validation_results
WHERE execution_id = 'catalog_prod_dr_migration_2026_07_09'
GROUP BY check_name, passed
ORDER BY check_name;
```

### Step 3: Acceptance criteria

| Check | Required Pass Rate |
|---|---|
| `row_count_parity` | 100% |
| `schema_match` | 100% |
| `data_hash_integrity` | ≥ 99% |
| `table_type_preserved` | 100% |
| `partition_columns_match` | 100% |
| `audit_log_completeness` | 100% |
| `delta_table_valid` | 100% |

Any `row_count_parity` or `schema_match` failure is a **blocker** — do not sign off until resolved.

### Step 4: Sign-off

Once all checks pass, record sign-off in your project tracker with:
- Execution ID
- Validation results link
- Approver name and date

---

## 14. Audit Log Reference

The audit table `utility_metadata.default.deep_clone_audit_log` is the system of record for all clone operations.

### Schema

| Column | Type | Description |
|---|---|---|
| `execution_id` | STRING | Unique run identifier |
| `run_timestamp` | TIMESTAMP | When the audit row was written |
| `table_name` | STRING | Fully-qualified source table name |
| `src_catalog` | STRING | Source catalog name |
| `src_schema` | STRING | Source schema name |
| `tgt_catalog` | STRING | Target catalog name |
| `tgt_schema` | STRING | Target schema name |
| `table_type` | STRING | MANAGED or EXTERNAL |
| `clone_mode` | STRING | delta_share or path_resolution |
| `src_location` | STRING | Resolved source abfss:// path |
| `tgt_location` | STRING | Remapped target abfss:// path |
| `records_cloned` | BIGINT | Row count from DEEP CLONE output |
| `files_copied` | BIGINT | File count from DEEP CLONE output |
| `size_bytes` | BIGINT | Bytes transferred |
| `start_time` | TIMESTAMP | Clone start time |
| `end_time` | TIMESTAMP | Clone end time |
| `duration_seconds` | BIGINT | Total elapsed seconds |
| `cluster_id` | STRING | Target cluster that ran the clone |
| `status` | STRING | SUCCESS / FAILED / DRY_RUN / SKIPPED |
| `error_message` | STRING | Short error (if FAILED) |
| `error_stack_trace` | STRING | Full Python traceback (if FAILED) |
| `dry_run` | STRING | "true" / "false" |

### Useful audit queries

```sql
-- Executive summary for an execution
SELECT
  COUNT(*)                                             AS total_tables,
  SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END)  AS succeeded,
  SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END)  AS failed,
  SUM(records_cloned)                                  AS total_records,
  ROUND(SUM(size_bytes)/1e9, 2)                       AS total_gb,
  ROUND(AVG(duration_seconds)/60.0, 1)                AS avg_min_per_table,
  MIN(start_time)                                      AS started_at,
  MAX(end_time)                                        AS completed_at
FROM utility_metadata.default.deep_clone_audit_log
WHERE execution_id = '<your-execution-id>';

-- All failed tables with error details
SELECT table_name, table_type, error_message, error_stack_trace
FROM utility_metadata.default.deep_clone_audit_log
WHERE execution_id = '<your-execution-id>'
  AND status = 'FAILED'
ORDER BY table_name;

-- Historical execution trend
SELECT
  DATE(run_timestamp)            AS run_date,
  execution_id,
  COUNT(*)                       AS total_tables,
  SUM(size_bytes)/1e9            AS total_gb,
  SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failures
FROM utility_metadata.default.deep_clone_audit_log
GROUP BY DATE(run_timestamp), execution_id
ORDER BY run_date DESC;
```

---

## 15. Troubleshooting & FAQ

### Q1: DESCRIBE DETAIL fails with `PERMISSION_DENIED`

**Symptom:** Step 3 of path_resolution mode logs `ERROR: {'error_code': 'PERMISSION_DENIED'}`

**Cause:** Source SP does not have SELECT on the table.

**Fix:**
```sql
GRANT SELECT ON TABLE catalog_prod.sales.transactions_fact TO `sp-migrate-src`;
```

---

### Q2: Target catalog not found after clone

**Symptom:** `utility_catalog.schema.table` does not exist on target after a SUCCESS audit row.

**Cause:** Target SP does not have CREATE SCHEMA or the target catalog wasn't created.

**Fix:**
```sql
-- Run on target workspace as metastore admin
CREATE CATALOG IF NOT EXISTS catalog_prod_dr;
GRANT CREATE SCHEMA ON CATALOG catalog_prod_dr TO `sp-migrate-tgt`;
```

---

### Q3: ExternalLocationNotFound during clone of external table

**Symptom:** `ExternalLocationNotFound: The specified location is not under any external location`

**Cause:** Target workspace does not have an External Location registered for the remapped path.

**Fix:** See §8 Step 3 — register the External Location and grant CREATE EXTERNAL TABLE to target SP.

---

### Q4: Warehouse takes too long to start

**Symptom:** Step 1 of live_test.py shows 40+ `STARTING` attempts.

**Cause:** Serverless or cold-start warehouse provisioning delay.

**Fix:** Use a pre-warmed or dedicated warehouse. Consider switching to a `CLASSIC` warehouse type for predictable startup. Set `"job_poll_interval_s": 60` to reduce API pressure.

---

### Q5: Load balancer puts all large tables on one cluster

**Symptom:** One cluster gets 90% of the data.

**Cause:** `scope_mode: catalog` with `path_resolution` does not have size metadata before grouping (size info comes from `DESCRIBE DETAIL` but is resolved per-table before grouping).

**Fix:** This is expected when tables have wildly different sizes. Increase `max_clusters` or manually split large catalogs into separate schema-level runs.

---

### Q6: DEEP CLONE fails with `AnalysisException: Table already exists`

**Symptom:** Clone fails even though `CREATE OR REPLACE TABLE` is used.

**Cause:** Incompatible schema change between source and cached target metadata.

**Fix:**
```sql
-- On target workspace
DROP TABLE IF EXISTS target_catalog.schema.table_name;
-- Re-run the clone
```

---

### Q7: `clone_mode: delta_share` table is not visible in shared catalog

**Symptom:** `delta_share_catalog.schema.table` returns `TABLE_NOT_FOUND`.

**Cause:** Table was not added to the share, or recipient hasn't refreshed.

**Fix:**
```sql
-- On source workspace — check what's in the share
SHOW ALL IN SHARE src_catalog_share;

-- Add missing table
ALTER SHARE src_catalog_share ADD TABLE catalog_prod.sales.missing_table;
```

---

## 16. Rollback Procedure

If the clone produces incorrect results or the target data is corrupted, use this rollback procedure.

### Option A: Drop and re-clone

```sql
-- On target workspace
DROP TABLE IF EXISTS catalog_prod_dr.sales.transactions_fact;
-- Then re-run the clone with a new execution_id
```

### Option B: Delta time-travel rollback

If the target table existed before the clone and was overwritten:

```sql
-- Check history
DESCRIBE HISTORY catalog_prod_dr.sales.transactions_fact;

-- Restore to previous version
RESTORE TABLE catalog_prod_dr.sales.transactions_fact
TO VERSION AS OF <version_before_clone>;
```

### Option C: Drop entire schema

```sql
-- Remove all tables in a schema (use with caution)
DROP SCHEMA IF EXISTS catalog_prod_dr.sales CASCADE;
```

### Rollback Checklist

- [ ] Identify affected tables from audit log (`execution_id` + `status = 'SUCCESS'`)
- [ ] Drop target tables or schemas as appropriate
- [ ] Document rollback in project tracker
- [ ] Update source table list to exclude rolled-back tables if re-running
- [ ] Re-run validation after any rollback

---

## 18. Change Log

| Version | Date | Author | Change |
|---|---|---|---|
| 2.5.0 | 2026-07-09 | Data Platform Engineering | Added §5 Onboarding Table (self-service model, full schema, INSERT examples, monitoring queries, re-queue procedure); added `onboarding_id`, `batch_id`, `throughput_mbps` to audit schema; updated all section numbers |
| 2.4.0 | 2026-07-09 | Data Platform Engineering | Initial SOP release; added §9 (delta_share setup) and §10 (path_resolution setup); both clone modes documented |
| 2.3.0 | 2026-06-01 | Data Platform Engineering | Added external table onboarding section |
| 2.2.0 | 2026-05-01 | Data Platform Engineering | Added dry-run mandatory policy |
| 2.0.0 | 2026-04-01 | Data Platform Engineering | Rewrite for dual clone-mode architecture |

---

*For questions or exceptions to this SOP, contact the Data Platform Engineering team via your internal ticketing system.*
