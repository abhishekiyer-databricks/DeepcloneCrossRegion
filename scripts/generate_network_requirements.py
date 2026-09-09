#!/usr/bin/env python3
"""
Network Requirements Generator: Central India → Jio India West
Azure Databricks Migration
Generates a comprehensive XLSX with real Azure/Databricks networking data.
"""

import subprocess, sys

# ── Install openpyxl if absent ─────────────────────────────────────────────
try:
    import openpyxl
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "-q"])
    import openpyxl

from openpyxl import Workbook
from openpyxl.styles import (
    PatternFill, Font, Alignment, Border, Side, GradientFill
)
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.formatting.rule import ColorScaleRule, CellIsRule, FormulaRule

# ── Colour palette ─────────────────────────────────────────────────────────
C = {
    "hdr_bg":     "1F3864",   # dark navy blue header bg
    "hdr_fg":     "FFFFFF",   # white text
    "hdr_accent": "C55A11",   # orange accent (secondary headers)
    "row_even":   "EBF1F9",   # light blue alternating row
    "row_odd":    "FFFFFF",   # white row
    "critical":   "FF0000",   # red – Critical priority
    "high":       "FF6600",   # orange – High
    "medium":     "FFD966",   # yellow – Medium
    "low":        "70AD47",   # green – Low
    "done":       "C6EFCE",   # pale green – Done
    "blocked":    "FFC7CE",   # pale red – Blocked
    "inprog":     "FFEB9C",   # pale yellow – In Progress
    "title_bg":   "17375E",   # deep navy for sheet titles
    "border":     "B4C6E7",   # light blue borders
}

def make_fill(hex_color):
    return PatternFill("solid", fgColor=hex_color)

def make_font(bold=False, color="000000", size=10, italic=False):
    return Font(bold=bold, color=color, size=size, italic=italic)

def make_border(color=C["border"]):
    s = Side(style="thin", color=color)
    return Border(left=s, right=s, top=s, bottom=s)

def make_align(wrap=True, h="left", v="center"):
    return Alignment(wrap_text=wrap, horizontal=h, vertical=v)

def write_header_row(ws, headers, row=1, bg=C["hdr_bg"], fg=C["hdr_fg"]):
    for col, text in enumerate(headers, 1):
        cell = ws.cell(row=row, column=col, value=text)
        cell.fill = make_fill(bg)
        cell.font = make_font(bold=True, color=fg, size=10)
        cell.alignment = make_align(h="center")
        cell.border = make_border()

def write_data_rows(ws, data, start_row=2, priority_col=None, status_col=None):
    prio_colors = {
        "Critical": C["critical"], "High": C["high"],
        "Medium": C["medium"], "Low": C["low"],
    }
    stat_colors = {
        "Done": C["done"], "Blocked": C["blocked"],
        "In Progress": C["inprog"], "Not Started": "FFFFFF",
    }
    for r_idx, row in enumerate(data):
        row_color = C["row_even"] if r_idx % 2 == 0 else C["row_odd"]
        for c_idx, val in enumerate(row, 1):
            cell = ws.cell(row=start_row + r_idx, column=c_idx, value=val)
            cell.border = make_border()
            cell.alignment = make_align()
            # Override fill for priority / status cells
            if priority_col and c_idx == priority_col and val in prio_colors:
                cell.fill = make_fill(prio_colors[val])
                cell.font = make_font(bold=True,
                    color="FFFFFF" if val in ("Critical","High") else "000000")
            elif status_col and c_idx == status_col and val in stat_colors:
                cell.fill = make_fill(stat_colors[val])
            else:
                cell.fill = make_fill(row_color)

def auto_width(ws, min_w=12, max_w=60):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_w, max(min_w, max_len + 2))

def add_status_validation(ws, col_letter, start_row, end_row):
    dv = DataValidation(
        type="list",
        formula1='"Not Started,In Progress,Done,Blocked"',
        allow_blank=True,
        showDropDown=False,
    )
    dv.sqref = f"{col_letter}{start_row}:{col_letter}{end_row}"
    ws.add_data_validation(dv)

def sheet_title(ws, title, subtitle="", colspan=12):
    ws.row_dimensions[1].height = 30
    cell = ws.cell(row=1, column=1, value=title)
    cell.fill = make_fill(C["title_bg"])
    cell.font = make_font(bold=True, color="FFFFFF", size=14)
    cell.alignment = make_align(h="center", v="center")
    ws.merge_cells(start_row=1, start_column=1,
                   end_row=1, end_column=min(colspan, ws.max_column or colspan))
    if subtitle:
        ws.row_dimensions[2].height = 18
        s = ws.cell(row=2, column=1, value=subtitle)
        s.fill = make_fill(C["hdr_accent"])
        s.font = make_font(bold=False, color="FFFFFF", size=10, italic=True)
        s.alignment = make_align(h="center")
        ws.merge_cells(start_row=2, start_column=1,
                       end_row=2, end_column=min(colspan, ws.max_column or colspan))

# ══════════════════════════════════════════════════════════════════════════════
wb = Workbook()
wb.remove(wb.active)   # remove default blank sheet

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 1 – Executive Summary
# ══════════════════════════════════════════════════════════════════════════════
ws1 = wb.create_sheet("Executive Summary")

# Title
ws1.row_dimensions[1].height = 35
title_cell = ws1.cell(row=1, column=1,
    value="Networking Requirements – Azure Databricks Migration: Central India → Jio India West")
title_cell.fill = make_fill(C["title_bg"])
title_cell.font = make_font(bold=True, color="FFFFFF", size=13)
title_cell.alignment = make_align(h="center", v="center")
ws1.merge_cells("A1:F1")

# Migration Overview
ws1.cell(row=3, column=1, value="MIGRATION OVERVIEW").fill = make_fill(C["hdr_bg"])
ws1.cell(row=3, column=1).font = make_font(bold=True, color="FFFFFF", size=11)
ws1.merge_cells("A3:F3")

overview_headers = ["Parameter", "Source (Central India)", "Target (Jio India West)"]
for c, h in enumerate(overview_headers, 1):
    cell = ws1.cell(row=4, column=c, value=h)
    cell.fill = make_fill(C["hdr_accent"])
    cell.font = make_font(bold=True, color="FFFFFF")
    cell.alignment = make_align(h="center")
    cell.border = make_border()

overview_data = [
    ["Azure Region Code",              "centralindia",                           "jioindiawest"],
    ["Azure Region Location",          "Pune, Maharashtra, India",               "Jamnagar, Gujarat, India"],
    ["Databricks Control Plane IPs",   "104.211.89.81/32, 20.235.199.64/28\n4.150.168.160/28",
                                       "135.235.2.133/32, 20.193.168.52/30"],
    ["SCC Relay FQDN",                 "tunnel.centralindia.azuredatabricks.net","tunnel.jioindiawest.azuredatabricks.net"],
    ["Control Plane NAT IPs",          "104.211.86.40/29, 40.80.55.104/29\n20.192.96.40/29 (+ 7 more)",
                                       "135.235.2.129/32"],
    ["Databricks Metastore Host",      "consolidated-centralindia-prod-metastore.mysql.database.azure.com",
                                       "consolidated-jioindiawest-prod-metastore-0.mysql.database.azure.com"],
    ["Artifact Blob Storage",          "dbartifactsprodcindia.blob.core.windows.net",
                                       "dbartifactsprodjiwest.blob.core.windows.net"],
    ["System Tables Storage (DFS)",    "ucstprdcindia.dfs.core.windows.net",     "ucstprdjiwest.dfs.core.windows.net"],
    ["Log Blob Storage",               "dblogprodcindia.blob.core.windows.net",  "dblogprodjiwest.blob.core.windows.net"],
    ["Event Hubs Endpoint",            "prod-centralindia-observabilityeventhubs.servicebus.windows.net",
                                       "prod-jioindiawest-observabilityeventhubs.servicebus.windows.net"],
    ["Workspace URL Pattern",          "adb-<id>.azuredatabricks.net",           "adb-<id>.azuredatabricks.net"],
    ["Private Link DNS Zone",          "privatelink.azuredatabricks.net",         "privatelink.azuredatabricks.net"],
    ["ADLS Gen2 Endpoint Suffix",      "*.dfs.core.windows.net",                 "*.dfs.core.windows.net"],
    ["Distance CI ↔ Jio West",         "~590 km (Pune → Jamnagar)",              "Expected RTT: 5–15 ms"],
    ["Inter-region Transfer Cost",     "Azure egress pricing applies",            "~₹2–4 / GB (ingress free)"],
]
for r, row in enumerate(overview_data):
    for c, val in enumerate(row, 1):
        cell = ws1.cell(row=5+r, column=c, value=val)
        cell.fill = make_fill(C["row_even"] if r % 2 == 0 else C["row_odd"])
        cell.border = make_border()
        cell.alignment = make_align()
ws1.row_dimensions[5+len(overview_data)].height = 8

# Critical Path
cp_row = 5 + len(overview_data) + 2
ws1.cell(row=cp_row, column=1, value="CRITICAL PATH ITEMS").fill = make_fill(C["hdr_bg"])
ws1.cell(row=cp_row, column=1).font = make_font(bold=True, color="FFFFFF", size=11)
ws1.merge_cells(f"A{cp_row}:F{cp_row}")

cp_headers = ["#", "Critical Item", "Why Critical", "Risk if Missed", "Owner", "Target Date"]
write_header_row(ws1, cp_headers, row=cp_row+1)
critical_items = [
    [1, "Deploy Private Endpoint for CI ADLS Gen2 in Jio West VNet",
        "Jio West clusters must reach CI storage without public internet", "Data reads will fail or be routed over public internet", "Network/Cloud Infra", "Before migration start"],
    [2, "Update CI Storage Account Firewall to allow Jio West subnet CIDRs",
        "ADLS Gen2 firewall blocks unknown subnets by default", "All abfss:// reads from Jio West will return 403 Forbidden", "Storage Admin", "Before migration start"],
    [3, "Configure Private DNS Zone privatelink.dfs.core.windows.net in Jio West VNet",
        "Name resolution for PE must return private IP not public IP", "Clusters resolve storage to public IP bypassing PE", "Network Admin", "Before PE creation"],
    [4, "Add NSG rules: Jio West subnets → CI ADLS private endpoint IP (443)",
        "NSG may block outbound 443 to cross-region PE IPs", "Storage access blocked at NSG layer", "Network Admin", "Same day as PE"],
    [5, "Whitelist Jio West control plane IPs in CI workspace IP access list",
        "Orchestrator REST API calls (port 443) from Jio West", "Job submission and SQL Warehouse queries fail", "Security Admin", "Pre-migration"],
    [6, "Register CI ADLS External Locations in Jio West Unity Catalog",
        "Required for Delta DEEP CLONE to write to target tables", "Clone engine cannot list/write target locations", "Data Engineer", "Post-workspace-setup"],
    [7, "Configure UDRs for Jio West ADB workspace (SCC + Storage + EventHub)",
        "VNet-injected workspace requires explicit routes", "Clusters fail to start or lose control-plane connectivity", "Network/Cloud Infra", "At workspace creation"],
    [8, "Validate ExpressRoute or VPN bandwidth for inter-region clone traffic",
        "Large DEEP CLONE transfers saturate links", "Migration SLA breach / partial clone failures", "Network/Infra", "Pre-migration load test"],
]
write_data_rows(ws1, critical_items, start_row=cp_row+2, priority_col=None)

# Bandwidth Summary
bw_row = cp_row + 2 + len(critical_items) + 2
ws1.cell(row=bw_row, column=1, value="ESTIMATED BANDWIDTH REQUIREMENTS").fill = make_fill(C["hdr_bg"])
ws1.cell(row=bw_row, column=1).font = make_font(bold=True, color="FFFFFF", size=11)
ws1.merge_cells(f"A{bw_row}:F{bw_row}")
bw_headers = ["Scenario", "Estimated Volume", "Bandwidth Needed", "Latency Sensitivity", "Duration Estimate", "Notes"]
write_header_row(ws1, bw_headers, row=bw_row+1)
bw_data = [
    ["DEEP CLONE – Bulk Delta Table Transfer", "1–50 TB (typical)", "≥ 1 Gbps sustained", "Low (throughput-bound)", "2–48 hours / TB @ 1Gbps", "Use ExpressRoute for >5 TB"],
    ["Metadata REST API calls (orchestrator → CI)", "< 1 MB / call", "< 1 Mbps", "High (< 200 ms acceptable)", "Continuous during migration", "Pure TCP/443 JSON payloads"],
    ["Unity Catalog metastore sync", "< 100 MB total", "< 10 Mbps", "Medium", "One-time + ongoing", "MySQL port 3306 not exposed cross-region; handled internally"],
    ["Delta log checkpoint reads (abfss://)", "10–500 MB / table", "Moderate", "Medium", "Per-table during CLONE", "Cached after first read per cluster lifecycle"],
    ["Spark shuffle / spill (stays local)", "N/A – stays in Jio West", "No cross-region", "N/A", "N/A", "Shuffle data never crosses region boundary"],
    ["Event Hub telemetry (logs)", "< 1 MB / min", "Negligible", "Low", "Continuous", "Region-local; no cross-region requirement"],
]
write_data_rows(ws1, bw_data, start_row=bw_row+2)

auto_width(ws1)
ws1.column_dimensions["A"].width = 36
ws1.column_dimensions["B"].width = 36
ws1.column_dimensions["C"].width = 36

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 2 – Workspace Connectivity
# ══════════════════════════════════════════════════════════════════════════════
ws2 = wb.create_sheet("Workspace Connectivity")
sheet_title(ws2, "Workspace Connectivity Requirements", "Databricks inter-workspace & control-plane connectivity for CI ↔ Jio West migration", 11)

hdrs2 = ["Req ID","Category","Source","Destination","Protocol","Port","Direction","Description","Priority","Action Required","Status"]
write_header_row(ws2, hdrs2, row=3)

data2 = [
    ["WC-001","Control Plane → Data Plane (Jio West)","ADB Control Plane\n(jioindiawest)","Jio West Data Plane VNet\n(Private/Public subnet)","TCP","443","Inbound to VNet","Control plane manages clusters via SCC relay. Uses SCC relay FQDN: tunnel.jioindiawest.azuredatabricks.net. Control Plane IPs: 135.235.2.133/32, 20.193.168.52/30","Critical","Add UDR + NSG Allow for SCC relay IP","Not Started"],
    ["WC-002","Control Plane → Data Plane (Central India)","ADB Control Plane\n(centralindia)","CI Data Plane VNet\n(worker subnets)","TCP","443","Inbound to VNet","Existing CI workspace control plane connectivity. IPs: 104.211.89.81/32, 20.235.199.64/28. SCC relay: tunnel.centralindia.azuredatabricks.net","Critical","Validate existing UDRs/NSGs still intact post-migration","Not Started"],
    ["WC-003","Orchestrator (Jio West) → Source Workspace REST API","Jio West ADB Cluster\n(orchestrator)","CI Workspace API\nadb-<ci-id>.azuredatabricks.net","HTTPS","443","Outbound from Jio West","Orchestrator calls CI workspace REST API (/api/2.0/jobs/*, /api/2.1/sql/*) to trigger SQL Warehouse queries and track job status. Requires NAT/firewall allow outbound 443 to *.azuredatabricks.net","Critical","Ensure Jio West firewall/NSG allows outbound 443 to all *.azuredatabricks.net","Not Started"],
    ["WC-004","Orchestrator (Jio West) → CI SQL Warehouse","Jio West Orchestrator\nCluster","CI SQL Warehouse\nHTTPS endpoint","HTTPS / JDBC","443","Outbound from Jio West","JDBC/ODBC or REST API SQL execution on CI SQL Warehouse from Jio West. Endpoint: <ci-workspace>.azuredatabricks.net/sql. Authentication via PAT or OAuth 2.0 (M2M)","High","Configure SQL Warehouse IP access list to include Jio West NAT IPs or use token auth over public 443","Not Started"],
    ["WC-005","OAuth Token Acquisition – Jio West Workspace","Jio West ADB Cluster","login.microsoftonline.com\n<tenant-id>/oauth2/token","HTTPS","443","Outbound","Databricks clusters and service principals fetch AAD/Entra ID OAuth tokens for storage and API authentication. Must be reachable from Jio West data plane.","Critical","Allow outbound 443 to login.microsoftonline.com in NSG + firewall","Not Started"],
    ["WC-006","OAuth Token Acquisition – CI Workspace","CI ADB Cluster","login.microsoftonline.com\n<tenant-id>/oauth2/token","HTTPS","443","Outbound","Same as WC-005 for CI workspace clusters. Required for managed identity token exchange for ADLS Gen2 access.","Critical","Validate existing CI outbound 443 to AAD endpoints","In Progress"],
    ["WC-007","Jio West Workspace → Azure Management API","Jio West Service Principal","management.azure.com","HTTPS","443","Outbound","Databricks cluster init scripts and SDK calls may invoke ARM APIs. Required for resource discovery, storage account key rotation checks.","Medium","Allow outbound 443 to management.azure.com from Jio West VNet","Not Started"],
    ["WC-008","Jio West Workspace → Databricks Account Console","Admin Users","accounts.azuredatabricks.com","HTTPS","443","Outbound (Browser)","Account-level operations: Unity Catalog metastore assignment, workspace linking, storage credential creation.","High","Allow outbound 443 to accounts.azuredatabricks.com","Not Started"],
    ["WC-009","Databricks Cluster Node ↔ Cluster Node (Jio West)","Jio West Worker Nodes","Jio West Worker Nodes","TCP","All (internal)","Intra-VNet","Spark inter-executor communication uses all ports within the private subnet. NSG must allow all traffic within VNet (intra-subnet).","Critical","NSG: Allow VirtualNetwork → VirtualNetwork, All ports","Not Started"],
    ["WC-010","Cluster to Databricks Metastore DB (Jio West)","Jio West Cluster","consolidated-jioindiawest-prod-metastore-0.mysql.database.azure.com","TCP","3306","Outbound","Hive metastore / catalog connectivity. Required for cluster startup and table resolution in Unity Catalog. Must allow outbound TCP 3306.","Critical","Allow outbound TCP 3306 to MySQL FQDN in firewall and UDR","Not Started"],
    ["WC-011","Cluster to Databricks Metastore DB (Central India)","CI Cluster","consolidated-centralindia-prod-metastore.mysql.database.azure.com","TCP","3306","Outbound","Same as WC-010 for CI workspace. Validate outbound TCP 3306 is allowed.","Critical","Validate CI outbound 3306 allow rules","In Progress"],
    ["WC-012","Event Hubs Telemetry – Jio West","Jio West Cluster","prod-jioindiawest-observabilityeventhubs.servicebus.windows.net","TCP / AMQP","9093","Outbound","Databricks observability and log streaming to Azure Event Hubs for Jio West workspace. Required for workspace metrics and diagnostics.","Medium","Allow outbound TCP 9093 to Event Hub FQDN or use EventHub service tag","Not Started"],
    ["WC-013","Event Hubs Telemetry – Central India","CI Cluster","prod-centralindia-observabilityeventhubs.servicebus.windows.net","TCP / AMQP","9093","Outbound","Same as WC-012 for CI workspace. Validate existing outbound 9093 rule.","Medium","Validate CI outbound 9093","In Progress"],
]
write_data_rows(ws2, data2, start_row=4, priority_col=9, status_col=11)
add_status_validation(ws2, "K", 4, 4+len(data2))
auto_width(ws2)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 3 – ADLS Storage Access
# ══════════════════════════════════════════════════════════════════════════════
ws3 = wb.create_sheet("ADLS Storage Access")
sheet_title(ws3, "ADLS Gen2 Storage Access Requirements",
    "Cross-region storage access: Jio West Databricks clusters reading CI ADLS Gen2 storage", 11)

hdrs3 = ["Req ID","Storage Account Region","Accessing From","Access Method","Endpoint Type","URL Pattern","Port","Auth Method","NSG Rule Needed","Storage Firewall Config","Notes"]
write_header_row(ws3, hdrs3, row=3)

data3 = [
    ["SA-001","Central India","Jio West ADB Clusters","abfss:// (ADLS Gen2 DFS)","Private Endpoint (Recommended)",
     "<ci-storage-account>.dfs.core.windows.net","443","Managed Identity (User/System Assigned)","Yes – Allow outbound 443 from Jio West private/host subnets to PE IP",
     "Add Jio West VNet subnet to 'Selected networks' or use PE and disable public access","STRONGLY RECOMMENDED: Deploy Private Endpoint for DFS sub-resource in Jio West VNet. Creates a NIC with private IP in Jio West subnet; DNS resolves to private IP."],
    ["SA-002","Central India","Jio West ADB Clusters","abfss:// (ADLS Gen2 DFS)","Service Endpoint (Alternative)",
     "<ci-storage-account>.dfs.core.windows.net","443","Managed Identity","Yes – Allow 443 outbound","Enable Microsoft.Storage service endpoint on Jio West subnets; add Jio West VNet/subnet to storage firewall allowed list",
     "Service Endpoints are VNet-bound but traffic still traverses Microsoft backbone. Less secure than Private Endpoints. Consider PE for production."],
    ["SA-003","Central India","Jio West ADB Clusters","Public Internet (Avoid)","Public Endpoint",
     "<ci-storage-account>.dfs.core.windows.net","443","Managed Identity / SAS / Account Key",
     "Allow outbound 443 to Storage service tag","Add Jio West public NAT IP to storage firewall 'Firewall > Add your client IP' or allow public access",
     "NOT RECOMMENDED for production. Traffic traverses public internet. Expose to data exfiltration risk."],
    ["SA-004","Central India","Unity Catalog (Jio West)","External Location registration","Private Endpoint + DNS",
     "<ci-storage-account>.dfs.core.windows.net/<container>","443","Storage Credential (Service Principal or Managed Identity)",
     "Same as SA-001","Storage account must allow the service principal used in storage credential","Register External Location in Jio West Unity Catalog metastore pointing to CI ADLS paths. Storage Credential must have Storage Blob Data Contributor role on CI storage account."],
    ["SA-005","Central India","Jio West ADB Clusters","BLOB endpoint (for DBFS/checkpoint)","Private Endpoint",
     "<ci-storage-account>.blob.core.windows.net","443","Managed Identity",
     "Allow outbound 443 from Jio West subnets to blob PE IP","Add blob sub-resource PE in Jio West VNet separately from dfs PE","ADLS Gen2 has two endpoints: dfs (ABFS) and blob. Private Endpoints are created per sub-resource. Create both if blob access needed (Delta log checkpoints sometimes use blob)."],
    ["SA-006","Jio West","Jio West ADB Clusters","abfss:// (local target storage)","Private Endpoint",
     "<jiwest-storage-account>.dfs.core.windows.net","443","Managed Identity","Yes – standard Jio West NSG","Standard configuration for Jio West storage account","DEEP CLONE target tables land in Jio West storage. This is local access – simpler networking. Ensure PE is deployed in same VNet as Jio West workspace."],
    ["SA-007","Central India","Azure Data Factory / Synapse (if used)","Integration Runtime","Private Endpoint",
     "<ci-storage-account>.dfs.core.windows.net","443","Service Principal / MSI","IR subnet NSG allow 443","Same PE as SA-001 can serve both ADF and ADB if same VNet","If ADF Integration Runtime in Jio West also needs CI storage access, share the Private Endpoint."],
    ["SA-008","Central India","CI Workspace (existing)","abfss:// local access","Private Endpoint (existing)",
     "<ci-storage-account>.dfs.core.windows.net","443","Managed Identity","Already configured","Already in storage firewall (CI VNet)","Validate existing CI storage PE configuration is not accidentally modified during migration. Keep CI access intact for parallel operations."],
    ["SA-009","Central India","Jio West ADB – DEEP CLONE Source Read","Delta DEEP CLONE source scan","Private Endpoint (SA-001)",
     "<ci-storage-account>.dfs.core.windows.net/<container>/<table-path>","443","Managed Identity","Same as SA-001","Same as SA-001","During DEEP CLONE: source files scanned from CI ADLS. Jio West executor reads Parquet files + Delta log JSON over ABFS. Bandwidth: up to 10 Gbps aggregate across cluster."],
    ["SA-010","Central India","Jio West ADB – DEEP CLONE Metadata Read","Delta log JSON / checkpoint reads","Private Endpoint (SA-001)",
     "<ci-storage-account>.dfs.core.windows.net/<container>/<table>/_delta_log/*.json","443","Managed Identity","Same as SA-001","Same as SA-001","Delta log files are read before data files. Ensure low-latency path (<15 ms RTT recommended) for efficient metadata reads. 5–15 ms expected CI ↔ Jio West."],
]
write_data_rows(ws3, data3, start_row=4)
auto_width(ws3)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 4 – DNS Requirements
# ══════════════════════════════════════════════════════════════════════════════
ws4 = wb.create_sheet("DNS Requirements")
sheet_title(ws4, "DNS Resolution Requirements",
    "Private DNS zones, FQDN patterns, and resolution methods for cross-region connectivity", 6)

hdrs4 = ["Req ID","Service","FQDN Pattern","Resolution Method","Private DNS Zone","Azure Region","Notes"]
write_header_row(ws4, hdrs4, row=3)

data4 = [
    ["DNS-001","ADLS Gen2 DFS (CI Storage – Private)","<account>.dfs.core.windows.net",
     "Azure Private DNS Zone linked to Jio West VNet","privatelink.dfs.core.windows.net","Jio West (PE deployed here)",
     "When PE is created with 'Integrate with private DNS zone', Azure auto-creates A record: <account>.dfs.core.windows.net → private IP. Link zone to Jio West workspace VNet."],
    ["DNS-002","ADLS Gen2 BLOB (CI Storage – Private)","<account>.blob.core.windows.net",
     "Azure Private DNS Zone linked to Jio West VNet","privatelink.blob.core.windows.net","Jio West",
     "Separate DNS zone for blob sub-resource. Required if blob endpoint PE is deployed (SA-005). A record maps account to PE NIC private IP."],
    ["DNS-003","Azure Databricks Workspace URL (Jio West)","adb-<workspace-id>.azuredatabricks.net",
     "Public DNS (default) or Private DNS Zone if Private Link enabled","privatelink.azuredatabricks.net","Jio West",
     "If Jio West workspace uses Private Link (databricks_ui_api PE), link privatelink.azuredatabricks.net zone to workspace VNet. Without PL, resolves to public IP via Azure public DNS."],
    ["DNS-004","Azure Databricks Workspace URL (Central India)","adb-<workspace-id>.azuredatabricks.net",
     "Public DNS (default) or Private DNS Zone if Private Link enabled","privatelink.azuredatabricks.net","Central India",
     "If CI workspace uses Private Link, ensure its privatelink.azuredatabricks.net zone does NOT conflict with Jio West zone. Use separate zones or hub-and-spoke DNS forwarding."],
    ["DNS-005","SCC Relay – Jio West","tunnel.jioindiawest.azuredatabricks.net",
     "Public DNS (always public – do NOT use Private DNS)","N/A – must resolve to public IP","Global (Azure public DNS)",
     "CRITICAL: SCC relay FQDN must ALWAYS resolve to public IP. Do not add private DNS records for tunnel.* FQDNs. Databricks uses certificate pinning; private resolution breaks TLS."],
    ["DNS-006","SCC Relay – Central India","tunnel.centralindia.azuredatabricks.net",
     "Public DNS (always public)","N/A","Global","Same as DNS-005 for CI SCC relay."],
    ["DNS-007","Azure Active Directory / Entra ID","login.microsoftonline.com","Public DNS","N/A","Global (Microsoft CDN)",
     "Must resolve over public DNS. Do not intercept with private DNS. Required for OAuth token acquisition on all clusters."],
    ["DNS-008","Azure Management API","management.azure.com","Public DNS","N/A","Global","ARM endpoint for cluster init scripts, SDK calls. Public DNS resolution only."],
    ["DNS-009","MySQL Metastore – Jio West","consolidated-jioindiawest-prod-metastore-0.mysql.database.azure.com",
     "Public DNS (Databricks-managed)","N/A – Databricks internal","Jio West",
     "Databricks-managed MySQL endpoint. Not customer-configurable. Cluster startup resolves this via public DNS; firewall must allow TCP 3306 outbound to this FQDN."],
    ["DNS-010","MySQL Metastore – Central India","consolidated-centralindia-prod-metastore.mysql.database.azure.com",
     "Public DNS (Databricks-managed)","N/A – Databricks internal","Central India",
     "Same as DNS-009 for CI region."],
    ["DNS-011","Event Hubs – Jio West","prod-jioindiawest-observabilityeventhubs.servicebus.windows.net",
     "Public DNS or Service Endpoint","N/A (public) or privatelink.servicebus.windows.net (PE)","Jio West",
     "Databricks sends cluster metrics to Event Hub. If using Service Endpoint, no DNS change needed. If PE, create privatelink.servicebus.windows.net zone."],
    ["DNS-012","Key Vault (if used)","<keyvault-name>.vault.azure.net","Public DNS or Private DNS Zone",
     "privatelink.vaultcore.azure.net","Where PE is deployed",
     "If Key Vault stores storage account keys or secrets, deploy PE in Jio West VNet and link privatelink.vaultcore.azure.net zone."],
    ["DNS-013","Unity Catalog Account Console","accounts.azuredatabricks.com","Public DNS","N/A","Global",
     "Account-level Databricks operations use this FQDN. Public DNS only. No private DNS override."],
    ["DNS-014","Hub DNS Forwarder (Recommended Architecture)","All *.dfs.core.windows.net, *.blob.core.windows.net",
     "Custom DNS forwarder in hub VNet → Azure Private DNS Resolver","privatelink.dfs.core.windows.net\nprivatelink.blob.core.windows.net","Hub VNet (if hub-spoke)",
     "If using hub-and-spoke VNet, deploy Azure Private DNS Resolver in hub. Jio West spoke VNet uses custom DNS pointing to hub resolver. Hub resolves private zones centrally."],
]
write_data_rows(ws4, data4, start_row=4)
auto_width(ws4)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 5 – Firewall & NSG Rules
# ══════════════════════════════════════════════════════════════════════════════
ws5 = wb.create_sheet("Firewall & NSG Rules")
sheet_title(ws5, "Firewall & NSG Rules",
    "Azure NSG + Azure Firewall rules for CI ↔ Jio West migration networking", 11)

hdrs5 = ["Req ID","Rule Name","Source CIDR / Service Tag","Destination CIDR / Service Tag",
         "Protocol","Port(s)","Direction","Action","Priority (NSG)","Component","Purpose"]
write_header_row(ws5, hdrs5, row=3)

data5 = [
    ["FW-001","ADB-JioWest-SCC-Outbound","Jio West ADB Private Subnet (10.x.x.x/26)","AzureDatabricks service tag",
     "TCP","443","Outbound","Allow","100","Jio West NSG – Private Subnet",
     "Allow Databricks clusters to reach control plane SCC relay (tunnel.jioindiawest.azuredatabricks.net). REQUIRED for SCC-enabled workspace."],
    ["FW-002","ADB-JioWest-SCC-Outbound-Public","Jio West ADB Public Subnet (10.x.x.x/26)","AzureDatabricks service tag",
     "TCP","443","Outbound","Allow","101","Jio West NSG – Public Subnet","Same SCC relay rule for public subnet."],
    ["FW-003","ADB-JioWest-Storage-Outbound","Jio West ADB Private Subnet","Storage.jioindiawest service tag",
     "TCP","443","Outbound","Allow","110","Jio West NSG","Allow access to DBFS root storage and artifact Blob storage in Jio West region."],
    ["FW-004","ADB-JioWest-CI-Storage-PE","Jio West ADB Private + Public Subnets","CI ADLS Private Endpoint IP\n(e.g. 10.y.y.y/32)","TCP","443",
     "Outbound","Allow","120","Jio West NSG","Allow Jio West clusters to reach the Private Endpoint NIC deployed in Jio West VNet for CI ADLS Gen2. IP = PE NIC private IP."],
    ["FW-005","ADB-JioWest-AAD-Outbound","Jio West ADB Subnets","AzureActiveDirectory service tag",
     "TCP","443","Outbound","Allow","130","Jio West NSG","OAuth token acquisition from login.microsoftonline.com. Required for Managed Identity and Service Principal auth."],
    ["FW-006","ADB-JioWest-EventHub-Outbound","Jio West ADB Subnets","EventHub.jioindiawest service tag",
     "TCP","9093","Outbound","Allow","140","Jio West NSG","Databricks cluster logs and observability metrics to Event Hubs in Jio West."],
    ["FW-007","ADB-JioWest-SQL-Metastore","Jio West ADB Subnets","Sql.jioindiawest service tag",
     "TCP","3306","Outbound","Allow","150","Jio West NSG","MySQL metastore for Databricks cluster catalog. REQUIRED for cluster start."],
    ["FW-008","ADB-JioWest-Intra-VNet","VirtualNetwork","VirtualNetwork",
     "All","All","Both","Allow","200","Jio West NSG","Allow all intra-VNet traffic for Spark inter-executor communication. REQUIRED by Databricks."],
    ["FW-009","ADB-JioWest-LB-Inbound","AzureLoadBalancer","Jio West ADB Subnets",
     "TCP","All","Inbound","Allow","100","Jio West NSG","Azure Load Balancer health probes. REQUIRED by Databricks NSG spec."],
    ["FW-010","ADB-CI-JioWest-InboundAPI","CI NSG: Source = Jio West NAT IPs\n135.235.2.129/32","CI Workspace Private Subnet",
     "TCP","443","Inbound","Allow","200","CI NSG / CI Workspace IP Access List",
     "Allow Jio West orchestrator REST API calls into CI workspace. Add Jio West Control Plane NAT IP (135.235.2.129/32) to CI workspace IP Access List."],
    ["FW-011","CI-Storage-JioWest-Subnet-Allow","CI ADLS Firewall: Jio West VNet / Subnet","CI Storage Account",
     "TCP","443","Inbound","Allow","N/A – Storage Firewall rule","CI ADLS Gen2 Storage Account Firewall",
     "Storage account firewall: add Jio West VNet/subnet (Selected Networks) or use Private Endpoint (disables public). This allows Jio West clusters through storage firewall."],
    ["FW-012","ADB-JioWest-Artifact-Storage","Jio West ADB Subnets","dbartifactsprodjiwest.blob.core.windows.net\nStorage.jioindiawest","TCP","443","Outbound","Allow","160","Jio West NSG",
     "Artifact Blob storage for Databricks cluster init and libraries. Uses Storage service tag or FQDN-based firewall rule."],
    ["FW-013","Deny-All-Other-Outbound","Jio West ADB Subnets","Internet","Any","Any","Outbound","Deny","4096","Jio West NSG – catch-all",
     "Deny all outbound traffic not matched by explicit allow rules (security hardening). Ensure UDR sends unmatched traffic to Azure Firewall for further inspection."],
    ["FW-014","AzFirewall-DNAT-None","N/A","N/A","N/A","N/A","N/A","N/A","N/A","Azure Firewall",
     "No DNAT rules required for this migration. All access is outbound-initiated from Jio West clusters."],
    ["FW-015","AzFirewall-AppRule-FQDN","Jio West ADB Subnet","*.azuredatabricks.net\nlogin.microsoftonline.com\nmanagement.azure.com\n*.dfs.core.windows.net\n*.blob.core.windows.net","HTTPS","443","Outbound","Allow","100","Azure Firewall – Application Rule Collection",
     "FQDN-based application rules in Azure Firewall for TLS-aware filtering. Do NOT enable TLS inspection for *.azuredatabricks.net (breaks SCC certificate pinning)."],
]
write_data_rows(ws5, data5, start_row=4)
auto_width(ws5)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 6 – Azure Private Link / Endpoints
# ══════════════════════════════════════════════════════════════════════════════
ws6 = wb.create_sheet("Private Link & Endpoints")
sheet_title(ws6, "Azure Private Endpoints & Private Link Requirements",
    "Private endpoint topology for secure cross-region access", 9)

hdrs6 = ["Req ID","Resource Type","Resource Region","PE Location","PE Subnet","DNS Zone","Approval Method","Use Case","Required?"]
write_header_row(ws6, hdrs6, row=3)

data6 = [
    ["PE-001","ADLS Gen2 – DFS sub-resource\n(CI Storage Account)","Central India\n(storage account resides here)","Jio West\n(PE NIC in Jio West VNet)","Jio West ADB Private Subnet\nor dedicated PE subnet /26","privatelink.dfs.core.windows.net","Auto-approved (same subscription) or manual approval","Allows Jio West Databricks clusters to read CI ADLS Gen2 via private IP. Eliminates public internet traversal for abfss:// reads.","YES – Strongly Recommended"],
    ["PE-002","ADLS Gen2 – BLOB sub-resource\n(CI Storage Account)","Central India","Jio West","Jio West dedicated PE subnet","privatelink.blob.core.windows.net","Auto-approved","Required if Databricks accesses blob endpoint (e.g. Delta checkpoint reads using blob URL). Separate PE from DFS.","Conditional (if blob endpoint used)"],
    ["PE-003","ADLS Gen2 – DFS sub-resource\n(Jio West Target Storage)","Jio West","Jio West","Jio West ADB Private Subnet","privatelink.dfs.core.windows.net","Auto-approved","Local storage access from Jio West clusters for target Delta tables. Standard workspace PE.","YES"],
    ["PE-004","Azure Databricks – databricks_ui_api\n(Jio West Workspace)","Jio West","Jio West","Dedicated PE subnet /27","privatelink.azuredatabricks.net","Auto-approved","Secures REST API and UI access to Jio West workspace from within VNet. Required for Private Link workspace deployment.","YES (if Private Link workspace)"],
    ["PE-005","Azure Databricks – browser_authentication\n(Jio West Workspace)","Jio West","Jio West","Dedicated PE subnet /27 (can share with PE-004)","privatelink.azuredatabricks.net","Auto-approved","Browser SSO authentication PE for Jio West workspace users.","Conditional (if Private Link browser auth required)"],
    ["PE-006","Azure Databricks – databricks_ui_api\n(CI Workspace)","Central India","Central India","CI dedicated PE subnet","privatelink.azuredatabricks.net","Auto-approved","Existing CI workspace Private Link PE. Do not modify during Jio West migration.","Existing – Validate"],
    ["PE-007","Azure Key Vault\n(CI Key Vault)","Central India","Jio West","Jio West PE subnet","privatelink.vaultcore.azure.net","Auto-approved or manual","If Jio West clusters need secrets from CI Key Vault (e.g. storage SAS tokens, connection strings), deploy KV PE in Jio West VNet.","Conditional (if KV secrets needed from Jio West)"],
    ["PE-008","Azure Key Vault\n(Jio West Key Vault)","Jio West","Jio West","Jio West PE subnet","privatelink.vaultcore.azure.net","Auto-approved","Jio West workspace secrets (e.g. Databricks PAT for CI, storage credentials). Standard Jio West KV PE.","YES (if KV used)"],
    ["PE-009","Event Hub Namespace\n(Jio West – optional)","Jio West","Jio West","Jio West PE subnet","privatelink.servicebus.windows.net","Auto-approved","If Databricks Event Hub telemetry must not traverse public internet. Optional – Databricks can use public Event Hub endpoint.","Optional"],
    ["PE-010","Azure Container Registry\n(if custom Docker images used)","Central India or Jio West","Jio West","Jio West PE subnet","privatelink.azurecr.io","Auto-approved","If Jio West clusters use custom Docker images from ACR, deploy ACR PE in Jio West.","Conditional (if Docker runtime)"],
]
write_data_rows(ws6, data6, start_row=4)
auto_width(ws6)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 7 – VNet & Peering
# ══════════════════════════════════════════════════════════════════════════════
ws7 = wb.create_sheet("VNet & Peering")
sheet_title(ws7, "VNet Topology & Peering Requirements",
    "VNet peering, Virtual WAN, and network topology analysis for CI ↔ Jio West", 10)

hdrs7 = ["Req ID","Source VNet","Source Region","Dest VNet","Dest Region","Peering Type","BGP Enabled","Use Case","Alternative","Notes"]
write_header_row(ws7, hdrs7, row=3)

data7 = [
    ["VN-001","Jio West ADB VNet\n(e.g. 10.1.0.0/16)","jioindiawest","CI ADB VNet\n(e.g. 10.0.0.0/16)","centralindia","Global VNet Peering","Optional (BGP not applicable for basic peering)","Cross-region cluster communication if needed. NOT required if ADLS access uses Private Endpoints (PE traffic doesn't require VNet peering).","Private Endpoints – Recommended: PE for ADLS in Jio West VNet can reach CI storage without VNet peering","IMPORTANT: VNet peering is NOT required for Private Endpoint-based ADLS access. The PE NIC is in the local VNet; traffic to CI storage goes over Microsoft backbone. Only add peering if direct VNet-to-VNet connectivity is needed for other services."],
    ["VN-002","Jio West ADB VNet","jioindiawest","Hub VNet\n(Transit/Connectivity)","jioindiawest","Local VNet Peering (Spoke→Hub)","No","Spoke-to-hub connectivity for centralized firewall, DNS, and PE management","Stand-alone VNet with direct peering to CI","Recommended hub-and-spoke architecture: Jio West ADB VNet peered to a Jio West hub VNet where Azure Firewall, DNS Resolver, and PE subnets reside."],
    ["VN-003","CI ADB VNet","centralindia","Hub VNet (CI)\n(Transit/Connectivity)","centralindia","Local VNet Peering (Spoke→Hub)","No","Existing CI spoke-to-hub peering for centralized network management","Stand-alone VNet","Leverage existing CI hub-spoke topology. Ensure Jio West hub can reach CI hub if inter-hub connectivity required."],
    ["VN-004","Jio West Hub VNet","jioindiawest","CI Hub VNet","centralindia","Global VNet Peering (Hub-to-Hub)","Optional","Inter-hub connectivity for hub-and-spoke model across regions","Azure Virtual WAN","If hub-spoke model: peer the two hub VNets. Allows transitive routing Jio West spoke → Jio West hub → CI hub → CI spoke. Address space must not overlap."],
    ["VN-005","Jio West ADB VNet","jioindiawest","Azure Virtual WAN Hub (jioindiawest)","jioindiawest","Virtual WAN Connection","Yes (automatically)","Centralized routing via vWAN for multi-region, multi-VNet topology","Manual hub-to-hub peering","If organization uses Azure Virtual WAN, connect Jio West VNet to vWAN hub. vWAN enables transitive routing automatically. Supports ExpressRoute, VPN, and VNet connections."],
    ["VN-006","CI ADB VNet","centralindia","Azure Virtual WAN Hub (centralindia)","centralindia","Virtual WAN Connection","Yes","Centralized routing for CI region via vWAN","Manual hub peering","Connect CI VNet to vWAN hub. With vWAN, Jio West ↔ CI connectivity handled automatically by any-to-any routing in vWAN."],
    ["VN-007","N/A – Private Endpoint approach","jioindiawest","N/A","centralindia","No Peering Required","N/A","ADLS Gen2 Private Endpoint in Jio West VNet routes traffic to CI storage over Microsoft backbone without VNet peering","This is the recommended approach","When a Private Endpoint for CI storage is deployed in the Jio West VNet, the PE NIC is local. Traffic flows: Jio West cluster → PE NIC (local) → Microsoft backbone → CI storage. No VNet peering needed."],
    ["VN-008","Jio West ADB VNet","jioindiawest","CI ADB VNet","centralindia","No direct Peering (Firewall-inspected)","N/A","If Azure Firewall must inspect all Jio West ↔ CI traffic","VNet peering (bypasses FW)","Use UDRs to route Jio West traffic through Azure Firewall before reaching CI VNet. Firewall can then apply FQDN and IP filtering rules."],
]
write_data_rows(ws7, data7, start_row=4)
auto_width(ws7)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 8 – Control Plane URLs
# ══════════════════════════════════════════════════════════════════════════════
ws8 = wb.create_sheet("Control Plane URLs")
sheet_title(ws8, "Databricks Control Plane URLs & Endpoints",
    "Exact FQDNs, IPs, and ports for CI and Jio West regions – used for firewall/NSG allow-listing", 8)

hdrs8 = ["Region","Service","FQDN / IP","Port","Protocol","Direction","Required For","Notes"]
write_header_row(ws8, hdrs8, row=3)

data8 = [
    # Central India
    ["Central India","Control Plane IPs (inbound to CP)","104.211.89.81/32","443","HTTPS","Outbound from data plane","Cluster startup, job management, notebook execution","SCC disabled: clusters connect to this IP. Allow outbound from CI subnets."],
    ["Central India","Control Plane IPs (inbound to CP)","20.235.199.64/28","443","HTTPS","Outbound from data plane","Cluster startup, job management","Additional CI control plane IP block."],
    ["Central India","Control Plane IPs (inbound to CP)","4.150.168.160/28","443","HTTPS","Outbound from data plane","Global Databricks shared CP services","Global shared IP block across multiple regions."],
    ["Central India","SCC Relay (inbound to relay)","tunnel.centralindia.azuredatabricks.net","443","HTTPS/WSS","Outbound from data plane (SCC enabled)","Secure Cluster Connectivity relay","ALWAYS use FQDN, never hardcode IP. IPs change periodically."],
    ["Central India","Control Plane NAT IPs (outbound from CP)","104.211.86.40/29, 40.80.55.104/29\n20.192.96.40/29, 104.211.101.14/32\n20.244.88.56/29, 98.70.76.16/29\n20.198.25.0/29, 98.70.75.40/29\n20.198.9.224/29, 98.70.91.64/29","443","HTTPS","Inbound to data plane (SCC disabled only)","Applicable only when SCC is DISABLED","If SCC disabled, control plane initiates inbound TCP to clusters. Add these IPs to NSG inbound allow rules."],
    ["Central India","MySQL Metastore","consolidated-centralindia-prod-metastore.mysql.database.azure.com","3306","TCP","Outbound from data plane","Cluster startup – Hive/catalog metadata","Databricks-managed MySQL. Allow outbound TCP 3306."],
    ["Central India","Artifact Blob Storage (primary)","dbartifactsprodcindia.blob.core.windows.net","443","HTTPS","Outbound","Cluster init scripts, library packages","Multiple: also arprodcindiaa1-a6.blob.core.windows.net"],
    ["Central India","Artifact Blob Storage (secondary – West India)","dbartifactsprodwindia.blob.core.windows.net","443","HTTPS","Outbound","Failover artifact storage","Secondary region is West India for Central India."],
    ["Central India","System Tables Storage (DFS)","ucstprdcindia.dfs.core.windows.net","443","HTTPS","Outbound","Unity Catalog system tables","Databricks-managed ADLS Gen2 for UC system tables."],
    ["Central India","Log Blob Storage","dblogprodcindia.blob.core.windows.net","443","HTTPS","Outbound","Cluster driver/executor logs","Databricks-managed Blob Storage for log aggregation."],
    ["Central India","Event Hubs","prod-centralindia-observabilityeventhubs.servicebus.windows.net","9093","TCP/AMQP","Outbound","Cluster metrics, observability","Allows cluster monitoring data egress."],
    ["Central India","Workspace URL","adb-<workspace-id>.azuredatabricks.net","443","HTTPS","Both (API + UI)","REST API, UI, JDBC/ODBC","Replace <workspace-id> with your actual workspace numeric ID."],
    # Jio India West
    ["Jio India West","Control Plane IPs (inbound to CP)","135.235.2.133/32","443","HTTPS","Outbound from data plane","Cluster startup, job management","Primary CI for Jio West control plane."],
    ["Jio India West","Control Plane IPs (inbound to CP)","20.193.168.52/30","443","HTTPS","Outbound from data plane","Cluster startup, additional CP IPs","Secondary Jio West control plane IPs (/30 = 4 IPs)."],
    ["Jio India West","SCC Relay","tunnel.jioindiawest.azuredatabricks.net","443","HTTPS/WSS","Outbound from data plane (SCC enabled)","Secure Cluster Connectivity relay","Use FQDN in firewall – IP changes periodically."],
    ["Jio India West","Control Plane NAT IP (outbound from CP)","135.235.2.129/32","443","HTTPS","Inbound to data plane (SCC disabled only)","Only if SCC is DISABLED","Single NAT IP for Jio West. Note: much smaller NAT pool than CI – less outbound capacity."],
    ["Jio India West","MySQL Metastore","consolidated-jioindiawest-prod-metastore-0.mysql.database.azure.com","3306","TCP","Outbound","Cluster startup","Databricks-managed MySQL for Jio West region."],
    ["Jio India West","Artifact Blob Storage","dbartifactsprodjiwest.blob.core.windows.net","443","HTTPS","Outbound","Cluster init scripts, libraries","Jio West artifact storage. Note: no secondary listed (smaller region)."],
    ["Jio India West","System Tables Storage (DFS)","ucstprdjiwest.dfs.core.windows.net","443","HTTPS","Outbound","Unity Catalog system tables","Databricks-managed DFS endpoint for UC system tables."],
    ["Jio India West","Log Blob Storage","dblogprodjiwest.blob.core.windows.net","443","HTTPS","Outbound","Cluster logs","Databricks-managed Blob for log aggregation in Jio West."],
    ["Jio India West","Event Hubs","prod-jioindiawest-observabilityeventhubs.servicebus.windows.net","9093","TCP/AMQP","Outbound","Cluster observability","Jio West specific Event Hub namespace."],
    ["Jio India West","Workspace URL","adb-<workspace-id>.azuredatabricks.net","443","HTTPS","Both","REST API, UI, JDBC","Same URL pattern, different workspace ID."],
    # Common Azure Endpoints
    ["Global","Azure Active Directory / Entra ID","login.microsoftonline.com","443","HTTPS","Outbound","OAuth token acquisition for all workspaces","Required by all clusters. Do not block or intercept TLS."],
    ["Global","Azure Active Directory / Entra ID","login.windows.net","443","HTTPS","Outbound","Legacy AAD token endpoint (some SDKs)","Some Azure SDKs may use this alias."],
    ["Global","Azure Management API","management.azure.com","443","HTTPS","Outbound","ARM API calls, resource management","Required if clusters use Azure SDK or init scripts."],
    ["Global","Azure Resource Manager","management.core.windows.net","443","HTTPS","Outbound","Legacy Azure management","Some older SDKs use this endpoint."],
    ["Global","Databricks Account Console","accounts.azuredatabricks.com","443","HTTPS","Outbound (browser/API)","Unity Catalog, workspace management","Admin operations for account-level Unity Catalog."],
    ["Global","NuGet / PyPI (library installs)","pypi.org, files.pythonhosted.org\nnuget.org","443","HTTPS","Outbound","Library installation on clusters","May be blocked by strict firewall – use artifact proxy or Databricks Library Manager instead."],
    ["Global","GitHub (optional – Repos/Notebooks)","github.com, api.github.com","443","HTTPS","Outbound","Databricks Repos Git integration","Only if Repos/Git integration used. Can be blocked for airgapped environments."],
]
write_data_rows(ws8, data8, start_row=4)

# Color code region rows
ci_fill = make_fill("DDEEFF")
jiw_fill = make_fill("FFF2CC")
glob_fill = make_fill("EBF9EB")
for row in ws8.iter_rows(min_row=4, max_row=4+len(data8)-1):
    region_val = row[0].value or ""
    if "Central India" in region_val:
        color = "DDEEFF"
    elif "Jio India West" in region_val:
        color = "FFF2CC"
    else:
        color = "EBF9EB"
    for cell in row:
        cell.fill = make_fill(color)
        cell.border = make_border()
        cell.alignment = make_align()

auto_width(ws8)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 9 – Bandwidth & Latency
# ══════════════════════════════════════════════════════════════════════════════
ws9 = wb.create_sheet("Bandwidth & Latency")
sheet_title(ws9, "Bandwidth & Latency Considerations",
    "Cross-region data transfer planning for CI → Jio West Databricks migration", 6)

hdrs9 = ["Transfer Scenario","Estimated Data Volume","Bandwidth Required","Expected Latency (CI↔JioWest)","Throttling / Quota Risk","Recommendation"]
write_header_row(ws9, hdrs9, row=3)

data9 = [
    ["Delta DEEP CLONE – Bulk Parquet data transfer (large tables)","1–100 TB per table\n(enterprise lakehouse)",
     "≥ 1 Gbps sustained per active clone job\n(Spark parallelism determines actual bandwidth)",
     "5–20 ms RTT (CI ↔ Jio West ~590 km)\nThroughput constrained by link, not latency","Azure inter-region egress: 20 Gbps soft limit per VNet Gateway; ADLS Gen2: no stated Gbps limit but per-account throttling applies",
     "Use ExpressRoute for >5 TB datasets. Throttle clone parallelism (maxCloneParallel) to avoid saturating shared link. Schedule during off-peak hours. Monitor ADLS throttling (429 errors)."],
    ["Delta log metadata reads (JSON + checkpoints)","10 MB – 2 GB per table\n(depends on table history depth)",
     "< 100 Mbps (bursty, read-intensive)","5–20 ms RTT – acceptable for sequential metadata reads; increase checkpoint interval to reduce log file count","Low throttling risk – small files, infrequent","Enable Delta checkpointing (OPTIMIZE) before clone to reduce number of log files that must be read. Set delta.checkpointInterval = 10."],
    ["Orchestrator REST API calls (Jio West → CI workspace)","< 1 KB – 10 MB per API call\n(job status, SQL results)",
     "< 10 Mbps (mostly latency-bound)","5–20 ms – excellent for API calls (< 200 ms round-trip expected)","Very low – API gateway handles bursting","Implement retry logic with exponential backoff. Use async job submission (runNow + getRunOutput) rather than synchronous blocking calls."],
    ["Spark task result delivery (small data)","< 100 MB per task result","< 50 Mbps","5–20 ms","Low","Keep driver/executor in same region. Only cross-region traffic is reading source data; all computation happens in Jio West."],
    ["Unity Catalog metadata sync (cross-region UC)","< 100 MB per sync batch","< 5 Mbps","5–20 ms","Low","UC metastore is per-region. For cross-region access, use Delta Sharing or replicate catalog metadata. No live cross-region metastore streaming."],
    ["ADLS Gen2 inter-region copy (AzCopy / azcopy sync)","Total dataset size (TB)","Max 10 Gbps (AzCopy parallel)","5–20 ms RTT; throughput ~1–5 Gbps typical","Storage account IOPS limits: 20,000 IOPS / account","Alternative to Databricks DEEP CLONE for initial seed: use AzCopy to copy raw Parquet/Delta files first, then run CLONE for incremental sync."],
    ["ExpressRoute – recommended for >5 TB migration","10 GB – 100 TB+","1 Gbps – 100 Gbps (ExpressRoute SKU dependent)","1–5 ms (ExpressRoute private peering through private backbone)","Depends on ER circuit bandwidth; upgrade SKU if needed","Use ExpressRoute with Private Peering for guaranteed bandwidth and lowest latency. Avoids public internet congestion. Requires ER circuit terminating in both CI and Jio West Azure regions."],
    ["Internet (public) path – not recommended for bulk","< 1 TB","Shared, variable: 100 Mbps – 2 Gbps","10–50 ms (variable; subject to internet congestion)","High – public internet bandwidth is best-effort","NOT RECOMMENDED for production migration. Use only for dev/test with small datasets. Acceptable for REST API and control-plane calls (low volume, latency-tolerant)."],
    ["Azure Storage replication (RA-GRS)","Automatic for geo-redundant storage","Asynchronous – near-real-time","~10–30 min lag (RA-GRS SLA)","N/A – Microsoft-managed","If source ADLS uses RA-GRS, Jio West can read from the secondary endpoint (<account>-secondary.dfs.core.windows.net). Read-only and eventual consistency; NOT suitable for active CLONE source."],
]
write_data_rows(ws9, data9, start_row=4)
auto_width(ws9)

# ══════════════════════════════════════════════════════════════════════════════
# SHEET 10 – Checklist & Action Items
# ══════════════════════════════════════════════════════════════════════════════
ws10 = wb.create_sheet("Checklist & Action Items")
sheet_title(ws10, "Pre-Migration Networking Checklist & Action Items",
    "Ordered action items for networking setup – CI to Jio West Databricks migration", 9)

hdrs10 = ["Seq","Action Item","Owner","Region","Dependency","Estimated Effort","Status","Notes"]
write_header_row(ws10, hdrs10, row=3)

data10 = [
    [1,"Confirm Jio West Azure region availability and Databricks service availability in subscription","Cloud Architect","Jio West","Azure subscription access","0.5 days","Not Started","Check if jioindiawest Databricks is available in your Azure subscription and tenant. Some subscriptions require region activation."],
    [2,"Design Jio West VNet address space (must not overlap with CI VNet)","Network Architect","Jio West","None","0.5 days","Not Started","Allocate non-overlapping CIDR blocks. CI: 10.0.0.0/16 example; Jio West: 10.1.0.0/16 example. Include subnets: private (10.1.1.0/24), public (10.1.2.0/24), PE (10.1.3.0/26)."],
    [3,"Create Jio West VNet with required subnets (private, public, PE, GatewaySubnet)","Network Engineer","Jio West","VNet design (Step 2)","1 day","Not Started","Databricks requires private subnet and public subnet (min /26 each). Add PE subnet /26 for private endpoints. Add GatewaySubnet /27 if ExpressRoute/VPN planned."],
    [4,"Create NSG for Jio West ADB private subnet with Databricks-required rules","Network Engineer","Jio West","VNet creation (Step 3)","0.5 days","Not Started","Required NSG rules: Allow VirtualNetwork inbound/outbound, Allow AzureLoadBalancer inbound, Allow AzureDatabricks outbound 443, Allow Storage outbound 443, Allow EventHub outbound 9093, Allow Sql outbound 3306."],
    [5,"Create NSG for Jio West ADB public subnet (same rules as private)","Network Engineer","Jio West","VNet creation (Step 3)","0.5 days","Not Started","Mirror NSG rules from private subnet NSG. Public subnet is used for Databricks cluster hosts in classic deployment."],
    [6,"Configure UDR (Route Table) for Jio West ADB subnets","Network Engineer","Jio West","NSG creation (Steps 4–5)","1 day","Not Started","Add routes: AzureDatabricks → Internet, Storage.jioindiawest → Internet, EventHub.jioindiawest → Internet, Sql.jioindiawest → Internet. If using Azure Firewall: set default route 0.0.0.0/0 → Firewall private IP."],
    [7,"Create Azure Databricks workspace in Jio West (VNet-injected)","Databricks Admin","Jio West","VNet + NSG + UDR ready (Steps 3–6)","1 day","Not Started","Deploy with: VNet injection enabled, SCC enabled (No Public IP), Premium SKU (for Unity Catalog). Assign private + public subnets from Step 3."],
    [8,"Assign Unity Catalog metastore to Jio West workspace","Databricks Admin","Jio West","Workspace created (Step 7)","0.5 days","Not Started","In Databricks Account Console: assign existing or new Unity Catalog metastore to Jio West workspace. Metastore location: jioindiawest."],
    [9,"Create Private Endpoint for CI ADLS Gen2 (DFS sub-resource) in Jio West VNet","Network Engineer","Jio West","Jio West VNet PE subnet ready","1 day","Not Started","In Azure Portal: CI storage account → Networking → Private endpoint connections → Create. Target: dfs sub-resource. VNet: Jio West VNet, Subnet: PE subnet. Enable private DNS integration (privatelink.dfs.core.windows.net)."],
    [10,"Create Private Endpoint for CI ADLS Gen2 (BLOB sub-resource) in Jio West VNet","Network Engineer","Jio West","Same as Step 9","0.5 days","Not Started","Repeat Step 9 for blob sub-resource if blob endpoint access is needed alongside DFS."],
    [11,"Link privatelink.dfs.core.windows.net DNS zone to Jio West workspace VNet","Network Engineer","Jio West","Private Endpoint created (Step 9)","0.5 days","Not Started","Verify auto-link was created when PE was deployed. If using custom DNS/hub DNS: link zone to hub VNet and configure DNS forwarder."],
    [12,"Link privatelink.blob.core.windows.net DNS zone to Jio West VNet","Network Engineer","Jio West","PE blob created (Step 10)","0.5 hours","Not Started","Same as Step 11 for blob zone."],
    [13,"Update CI ADLS Gen2 Storage Account firewall to allow Jio West PE connection","Storage Admin","Central India","PE approved (Step 9)","0.5 hours","Not Started","After PE approval, CI storage should auto-allow the PE. Verify under: Storage Account → Networking → Private endpoint connections → Approved. If 'Selected networks' mode: ensure PE is approved and disable public network access if desired."],
    [14,"Create Storage Credential in Jio West Unity Catalog (Service Principal or MSI)","Databricks Admin","Jio West","Unity Catalog metastore assigned (Step 8)","1 day","Not Started","In Databricks: Data → External Data → Storage Credentials → Create. Assign SP or MSI that has Storage Blob Data Contributor on CI storage account."],
    [15,"Grant Storage Blob Data Contributor role on CI storage account to Jio West service principal","Cloud IAM Admin","Central India","Storage Credential SP identified (Step 14)","0.5 hours","Not Started","In Azure IAM: CI storage account → Access Control → Add role assignment → Storage Blob Data Contributor → assign SP or MSI used in Step 14."],
    [16,"Register External Location(s) in Jio West Unity Catalog pointing to CI ADLS","Databricks Admin","Jio West","Storage Credential ready (Step 14–15)","1 day","Not Started","For each CI container/path: CREATE EXTERNAL LOCATION ci_source_data URL 'abfss://container@account.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL ci_storage_cred);"],
    [17,"Validate DNS resolution: test nslookup <ci-storage>.dfs.core.windows.net from Jio West cluster","Network Engineer","Jio West","DNS zone linked (Step 11)","1 hour","Not Started","From a Jio West Databricks cluster: %sh nslookup <ci-storage>.dfs.core.windows.net. Expected: private IP from PE NIC (e.g. 10.1.3.x). If public IP returned: DNS zone not linked correctly."],
    [18,"Validate ADLS Gen2 access from Jio West cluster (read test)","Data Engineer","Jio West","All Steps 9–16 complete","2 hours","Not Started","Run: dbutils.fs.ls('abfss://container@ci-storage.dfs.core.windows.net/'). Expected: file listing. If 403: check role assignment (Step 15). If 404/timeout: check PE and DNS."],
    [19,"Validate CI workspace REST API call from Jio West orchestrator","Data Engineer","Jio West","Jio West workspace ready","1 hour","Not Started","From Jio West: import requests; resp = requests.get('https://adb-<ci-id>.azuredatabricks.net/api/2.0/clusters/list', headers={'Authorization': 'Bearer <PAT>'}). Expected: 200 OK."],
    [20,"Add Jio West NAT/egress IP to CI workspace IP Access List (if IP access list enabled on CI)","Databricks Admin","Central India","Jio West cluster NAT IP identified","0.5 hours","Not Started","CI workspace → Settings → Security → IP Access List → Add Jio West cluster NAT IP or Jio West Control Plane NAT IP (135.235.2.129/32)."],
    [21,"Performance baseline: measure CI ↔ Jio West network throughput","Network Engineer / Data Engineer","Both","ADLS PE ready","1 day","Not Started","Use SpeedTest or azcopy bench to measure actual throughput. Compare against expected ≥1 Gbps for bulk clone. Escalate to Azure support if <500 Mbps sustained."],
    [22,"Run pilot DEEP CLONE (small table, <1 GB) end-to-end","Data Engineer","Jio West","All prerequisites complete","2 hours","Not Started","Clone one small Delta table from CI to Jio West. Verify: data integrity (row counts match), performance (time vs size), and no network errors in Spark logs."],
    [23,"Schedule bulk migration during low-traffic window","Migration Lead","Both","Pilot success (Step 22)","Planning","Not Started","Coordinate with business stakeholders. Migration window: typically weekend, 10 PM – 6 AM. Ensure monitoring alerts configured."],
    [24,"Post-migration: update DNS records / connection strings in downstream consumers to Jio West","Application Owners","Jio West","Migration complete","2–5 days","Not Started","Update all ETL pipelines, Power BI datasets, application connection strings to point to Jio West Databricks workspace and Jio West ADLS storage accounts."],
    [25,"Decommission cross-region network rules after migration cutover","Network Engineer","Both","All consumers migrated (Step 24)","1 day","Not Started","After full cutover: remove Jio West subnets from CI storage firewall, remove cross-region PE if no longer needed, update NSG rules. Retain rollback path for 30 days."],
]
write_data_rows(ws10, data10, start_row=4, status_col=7)
add_status_validation(ws10, "G", 4, 4+len(data10))
auto_width(ws10)

# ══════════════════════════════════════════════════════════════════════════════
# Final touches: freeze panes, tab colors, print settings
# ══════════════════════════════════════════════════════════════════════════════
tab_colors = {
    "Executive Summary":    "1F3864",
    "Workspace Connectivity": "2E75B6",
    "ADLS Storage Access":  "C55A11",
    "DNS Requirements":     "375623",
    "Firewall & NSG Rules": "843C0C",
    "Private Link & Endpoints": "7030A0",
    "VNet & Peering":       "00B0F0",
    "Control Plane URLs":   "1F3864",
    "Bandwidth & Latency":  "70AD47",
    "Checklist & Action Items": "FF0000",
}

for ws in wb.worksheets:
    ws.freeze_panes = ws.cell(row=4, column=1)
    ws.sheet_view.showGridLines = True
    if ws.title in tab_colors:
        ws.sheet_properties.tabColor = tab_colors[ws.title]
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToWidth = 1

# ══════════════════════════════════════════════════════════════════════════════
OUTPUT_PATH = "/Users/vivek.ravichandiran/DeepcloneCrossRegion/Network_Requirements_CI_to_JioWest.xlsx"
wb.save(OUTPUT_PATH)

print("\n" + "═"*72)
print("  XLSX CREATED SUCCESSFULLY")
print("═"*72)
print(f"  Path: {OUTPUT_PATH}")
print(f"  Sheets: {len(wb.sheetnames)}")
for i, name in enumerate(wb.sheetnames, 1):
    ws = wb[name]
    print(f"    {i:2}. {name:<35} ({ws.max_row} rows)")
print("═"*72)
print("\nKEY NETWORKING FACTS ENCODED IN THIS DOCUMENT:")
print("  ■ Central India Control Plane IPs:  104.211.89.81/32, 20.235.199.64/28, 4.150.168.160/28")
print("  ■ Jio West Control Plane IPs:       135.235.2.133/32, 20.193.168.52/30")
print("  ■ Jio West SCC Relay:               tunnel.jioindiawest.azuredatabricks.net")
print("  ■ Jio West Metastore FQDN:          consolidated-jioindiawest-prod-metastore-0.mysql.database.azure.com")
print("  ■ Jio West Event Hub:               prod-jioindiawest-observabilityeventhubs.servicebus.windows.net")
print("  ■ CI Metastore FQDN:                consolidated-centralindia-prod-metastore.mysql.database.azure.com")
print("  ■ CI SCC Relay:                     tunnel.centralindia.azuredatabricks.net")
print("  ■ ADLS access: Private Endpoint (dfs sub-resource) in Jio West VNet → CI storage")
print("  ■ DNS Zone required:                privatelink.dfs.core.windows.net (linked to Jio West VNet)")
print("  ■ Ports: 443 (HTTPS), 3306 (MySQL metastore), 9093 (Event Hubs AMQP)")
print("  ■ No VNet peering needed for PE-based ADLS access")
print("  ■ VNet peering only needed for direct VNet-to-VNet connectivity")
print("═"*72 + "\n")
