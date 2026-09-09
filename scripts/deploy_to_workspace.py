"""
deploy_to_workspace.py

DEPRECATED — superseded by `databricks bundle deploy` (see databricks.yml +
resources/*.yml). This raw-REST-upload script predates the Databricks Asset
Bundle setup and is kept only for historical reference; it is not part of the
current deploy workflow (see docs/SOP_CSV_Run.md for the current procedure)
and its RAW_FILES list below references configs/config.json and
configs/sample_config.yaml, both of which have since been removed as
unused/legacy — do not re-add them without checking they still exist.

Deploys all DeepClone CrossRegion files to both Databricks workspaces:
  SOURCE (adb-7405609899028573): /Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion/
  TARGET (adb-7405606418658510): /Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion/

Python files are uploaded as notebooks (SOURCE_PYTHON language).
Non-Python files (.html, .md) are uploaded as raw files via DBFS or workspace files API.
"""
import os, requests, base64, json, time

SRC_URL    = os.environ["AZ2AZ_SRC_URL"]
SRC_CID    = os.environ["AZ2AZ_SRC_CID"]
SRC_SECRET = os.environ["AZ2AZ_SRC_SECRET"]
TGT_URL    = os.environ["AZ2AZ_TGT_URL"]
TGT_CID    = os.environ["AZ2AZ_TGT_CID"]
TGT_SECRET = os.environ["AZ2AZ_TGT_SECRET"]

BASE_DIR = "/Users/vivek.ravichandiran/DeepcloneCrossRegion"
WS_PATH  = "/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion"

def get_token(url, cid, secret):
    r = requests.post(f"{url}/oidc/v1/token",
        data={"grant_type":"client_credentials","client_id":cid,
              "client_secret":secret,"scope":"all-apis"}, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]

def create_folder(url, tok, path):
    r = requests.post(f"{url}/api/2.0/workspace/mkdirs",
        headers={"Authorization":f"Bearer {tok}"},
        json={"path": path}, timeout=20)
    return r.status_code in (200, 409)

def upload_notebook(url, tok, local_path, ws_path):
    """Upload a .py file as a Python notebook."""
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")
    r = requests.post(f"{url}/api/2.0/workspace/import",
        headers={"Authorization":f"Bearer {tok}"},
        json={
            "path": ws_path,
            "language": "PYTHON",
            "format": "SOURCE",
            "overwrite": True,
            "content": content
        }, timeout=30)
    return r.status_code == 200, r.text

def upload_file(url, tok, local_path, ws_path):
    """Upload any file (JSON, HTML, MD) via workspace files API."""
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")
    r = requests.post(f"{url}/api/2.0/workspace/import",
        headers={"Authorization":f"Bearer {tok}"},
        json={
            "path": ws_path,
            "format": "AUTO",
            "overwrite": True,
            "content": content
        }, timeout=30)
    return r.status_code == 200, r.text

# ── New modular orchestrator package modules ──
ORCHESTRATOR_MODULES = [
    "orchestrator/__init__.py",
    "orchestrator/models.py",
    "orchestrator/config.py",
    "orchestrator/sql_client.py",
    "orchestrator/api_client.py",
    "orchestrator/parameter_parser.py",
    "orchestrator/input_resolver.py",
    "orchestrator/inventory_manager.py",
    "orchestrator/workload_classifier.py",
    "orchestrator/cluster_pool.py",
    "orchestrator/scheduler.py",
    "orchestrator/audit_manager.py",
    "orchestrator/clone_worker.py",
    "orchestrator/validator.py",
    "orchestrator/retry_manager.py",
]

# Files to deploy
NOTEBOOKS = [
    # Databricks notebooks (all under notebooks/)
    "notebooks/orchestrator_notebook.py",
    "notebooks/chunk_worker_notebook.py",
    "notebooks/setup_control_tables.py",
    # Test utilities
    "tests/test_data_generator.py",
    "tests/test_validator.py",
    "tests/live_test.py",
    "tests/dual_batch_test.py",
]
RAW_FILES = [
    "configs/migration.yaml",
    "docs/SOP_Onboarding.md",
    "README.md",
]

print("═"*68, flush=True)
print("  DEPLOYING DeepClone CrossRegion to Databricks Workspaces", flush=True)
print("═"*68, flush=True)

for workspace_name, url, cid, secret in [
    ("SOURCE — azure_uc_demo_region2 (adb-7405609899028573)", SRC_URL, SRC_CID, SRC_SECRET),
    ("TARGET — azure_uc_demo_region1 (adb-7405606418658510)", TGT_URL, TGT_CID, TGT_SECRET),
]:
    print(f"\n  Workspace: {workspace_name}", flush=True)
    print("  " + "─"*64, flush=True)
    tok = get_token(url, cid, secret)

    # Create folder
    ok = create_folder(url, tok, WS_PATH)
    print(f"  {'✓' if ok else '✗'} mkdir {WS_PATH}", flush=True)

    # Create orchestrator package sub-folder
    ok2 = create_folder(url, tok, f"{WS_PATH}/orchestrator")
    print(f"  {'✓' if ok2 else '✗'} mkdir {WS_PATH}/orchestrator", flush=True)

    # Upload orchestrator package modules (as raw files, not notebooks)
    for rel_path in ORCHESTRATOR_MODULES:
        local = os.path.join(BASE_DIR, rel_path)
        if not os.path.exists(local):
            print(f"  ⚠ MISSING {local}", flush=True)
            continue
        ws_file_path = f"{WS_PATH}/{rel_path}"
        tok = get_token(url, cid, secret)
        ok, msg = upload_file(url, tok, local, ws_file_path)
        status = "✓" if ok else "✗"
        print(f"  {status} {rel_path:<50} → uploaded", flush=True)

    # Upload notebooks
    for fname in NOTEBOOKS:
        local = os.path.join(BASE_DIR, fname)
        # Remove .py extension for notebook path
        nb_name = fname.replace(".py", "")
        ws_nb_path = f"{WS_PATH}/{nb_name}"
        tok = get_token(url, cid, secret)  # refresh
        ok, msg = upload_notebook(url, tok, local, ws_nb_path)
        if ok:
            print(f"  ✓ {fname:<40} → {ws_nb_path}", flush=True)
        else:
            # Try as raw file if notebook import fails
            ok2, msg2 = upload_file(url, tok, local, f"{WS_PATH}/{fname}")
            status = "✓" if ok2 else "✗"
            print(f"  {status} {fname:<40} [as file] {'' if ok2 else msg2[:80]}", flush=True)

    # Upload raw files
    for fname in RAW_FILES:
        local = os.path.join(BASE_DIR, fname)
        ws_file_path = f"{WS_PATH}/{fname}"
        tok = get_token(url, cid, secret)
        ok, msg = upload_file(url, tok, local, ws_file_path)
        if ok:
            print(f"  ✓ {fname:<40} → {ws_file_path}", flush=True)
        else:
            print(f"  ✗ {fname:<40} {msg[:80]}", flush=True)

    ws_ui_link = f"{url}#workspace{WS_PATH.replace('/','%2F')}"
    print(f"\n  Workspace folder URL:", flush=True)
    print(f"  {url}/browse/folders?o={WS_PATH}", flush=True)
    print(f"  {url}#workspace{WS_PATH}", flush=True)

print(flush=True)
print("═"*68, flush=True)
print("  DEPLOYMENT COMPLETE", flush=True)
print(f"  SOURCE: {SRC_URL}#workspace{WS_PATH}", flush=True)
print(f"  TARGET: {TGT_URL}#workspace{WS_PATH}", flush=True)
print("═"*68, flush=True)
