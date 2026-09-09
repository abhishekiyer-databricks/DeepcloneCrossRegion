"""
exclusion_manager.py — Global catalog/schema/table exclusion list.

Independent of input_type (JOB/YAML/CSV): loaded from
`OrchestratorConfig.exclusion_csv_path` and applied as a filter ON TOP OF
whatever InputResolver already resolved, right before InventoryManager
onboards the tables — so excluded tables NEVER reach migration_control and
are instead recorded into migration_exclusion_log for audit/review.

This is deliberately separate from the per-row `exclude_schemas`/
`exclude_tables` glob patterns already supported inside a single YAML
`mappings:` entry or a CSV Format-B row (see input_resolver.py) — those only
scope to the one mapping entry that declares them. This exclusion list is a
single, global, cross-cutting list that applies no matter which input_type
or CSV/YAML row produced the selection, which is what makes it suitable for
things like "never touch these legacy/sensitive schemas, regardless of what
any CSV/YAML says".

CSV format
----------
    exclude_type,catalog,schema,table
    catalog,ril_bulk_old,,
    schema,ril_bulk_02,iot,
    table,ril_bulk_02,finance,dim_finance_01

  - exclude_type : catalog | schema | table   (required)
  - catalog      : required for ALL types. Glob pattern allowed (fnmatch).
  - schema       : required for schema/table types (ignored for catalog).
                   Glob pattern allowed.
  - table        : required for table type only (ignored for catalog/schema).
                   Glob pattern allowed.

Matching is case-insensitive glob (fnmatch) matching, consistent with the
exclude_schemas/exclude_tables semantics already used in input_resolver.py.
A table is excluded if ANY rule matches it.
"""

from __future__ import annotations
import csv
import fnmatch
import logging
from typing import List, Tuple

from orchestrator.models import TableSelection

log = logging.getLogger(__name__)

_VALID_TYPES = ("catalog", "schema", "table")


class ExclusionRule:
    """One row of the exclusion CSV, resolved into a matcher."""

    __slots__ = ("exclude_type", "catalog", "schema", "table")

    def __init__(self, exclude_type: str, catalog: str, schema: str = "", table: str = ""):
        self.exclude_type = exclude_type
        self.catalog      = catalog
        self.schema       = schema
        self.table        = table

    def matches(self, sel: TableSelection) -> bool:
        if not fnmatch.fnmatch(sel.source_catalog.lower(), self.catalog.lower()):
            return False
        if self.exclude_type == "catalog":
            return True
        if not fnmatch.fnmatch(sel.source_schema.lower(), self.schema.lower()):
            return False
        if self.exclude_type == "schema":
            return True
        return fnmatch.fnmatch(sel.source_table.lower(), self.table.lower())

    def describe(self) -> str:
        if self.exclude_type == "catalog":
            return f"catalog:{self.catalog}"
        if self.exclude_type == "schema":
            return f"schema:{self.catalog}.{self.schema}"
        return f"table:{self.catalog}.{self.schema}.{self.table}"


def load_exclusion_rules(path: str) -> List[ExclusionRule]:
    """
    Parse the exclusion CSV at `path`. Returns [] if path is blank — this
    feature is fully opt-in; leaving exclusion_csv_path blank in
    databricks.yml (or the job parameter) means zero behavior change.
    """
    if not path or not path.strip():
        return []

    log.info("Loading global exclusion list from %s", path)
    rules: List[ExclusionRule] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for i, raw_row in enumerate(reader, start=2):  # header is row 1
            row = {(k or "").strip().lower(): (v or "").strip() for k, v in raw_row.items()}
            etype = (row.get("exclude_type") or "").lower()
            cat   = row.get("catalog", "")
            sch   = row.get("schema", "")
            tbl   = row.get("table", "")

            if etype not in _VALID_TYPES:
                log.warning("Exclusion CSV row %d has invalid/missing exclude_type=%r — skipping", i, etype)
                continue
            if not cat:
                log.warning("Exclusion CSV row %d missing catalog — skipping", i)
                continue
            if etype in ("schema", "table") and not sch:
                log.warning("Exclusion CSV row %d (type=%s) missing schema — skipping", i, etype)
                continue
            if etype == "table" and not tbl:
                log.warning("Exclusion CSV row %d (type=table) missing table — skipping", i)
                continue

            rules.append(ExclusionRule(etype, cat, sch, tbl))

    log.info("Loaded %d exclusion rule(s) from %s", len(rules), path)
    for r in rules:
        log.info("  Exclusion rule: %s", r.describe())
    return rules


def apply_exclusions(
    selections: List[TableSelection],
    rules:      List[ExclusionRule],
) -> Tuple[List[TableSelection], List[Tuple[TableSelection, ExclusionRule]]]:
    """
    Partition `selections` into (kept, excluded_with_rule).

    `excluded_with_rule` is a list of (TableSelection, matching ExclusionRule)
    pairs so the caller (orchestrator_notebook.py) can log/audit exactly WHY
    each table was skipped. Returns (selections, []) unchanged if there are
    no rules (common case — feature not configured).
    """
    if not rules:
        return selections, []

    kept:     List[TableSelection] = []
    excluded: List[Tuple[TableSelection, ExclusionRule]] = []
    for sel in selections:
        rule = next((r for r in rules if r.matches(sel)), None)
        if rule is not None:
            excluded.append((sel, rule))
        else:
            kept.append(sel)

    log.info(
        "Exclusion filter: %d input, %d excluded, %d kept",
        len(selections), len(excluded), len(kept),
    )
    return kept, excluded
