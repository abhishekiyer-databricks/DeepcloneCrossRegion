"""
predictive_optimization.py — INVENTORY-time Predictive Optimization (PO) gate.

Why this exists
---------------
Unity Catalog Predictive Optimization runs background OPTIMIZE / VACUUM on a
table. If PO is enabled on the TARGET (destination) table that a migration
clones into, those background rewrites mutate the target's files/Delta versions
underneath the clone — exactly the class of churn that causes DEEP CLONE version
drift and concurrent-write conflicts (especially on incremental / repeat clone
syncs into an existing target). The migration therefore requires PO to be
DISABLED on every target table before onboarding. INVENTORY enforces this: any
target whose *effective* PO is ENABLED (or whose PO status cannot be confirmed
disabled) is recorded with status=PO_BLOCKED and the INVENTORY step is failed.

Detection
---------
Uses the Unity Catalog REST API single-object GET (see
ApiClient.unity_catalog_get). A single table-level GET returns the fully
resolved `effective_predictive_optimization_flag` — catalog → schema → table
inheritance is resolved server-side — so one call per table covers the
"catalog, schema, or table level" requirement automatically, and
`inherited_from_type`/`inherited_from_name` tell us WHERE it was enabled (used
in the recorded error message).

Verified live (UC REST API 2.1):
  • catalog ENABLE  → child table effective ENABLE, inherited_from CATALOG
  • schema  ENABLE  → child table effective ENABLE, inherited_from SCHEMA
  • table   ENABLE  → table effective ENABLE (set directly, no inherited_from)
  • all DISABLE/INHERIT → effective DISABLE

Block policy (fail-OPEN except for a confirmed ENABLE)
------------------------------------------------------
Only a target whose effective PO resolves to **ENABLE** (directly or inherited
from its schema/catalog) is BLOCKED. Everything else continues:
  • A non-managed target (EXTERNAL / FOREIGN / delta-shared / view) → SKIPPED
    (PO_SKIPPED_CODE) — predictive optimization never runs on it.
  • A target securable that does not exist yet → SKIPPED (PO_SKIPPED_CODE).
  • An unreadable (permission/transport) or missing/unrecognised flag → treated
    as UNKNOWN (PO_UNKNOWN_CODE) and the migration CONTINUES, logging a warning.
This is an intentional operational requirement: an unconfirmable PO status must
NOT fail the migration (earlier revisions failed closed here). A delta-shared
external source/target, in particular, has no PO flag and must never block.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# error_code values persisted to migration_control.error_code for PO_BLOCKED rows
PO_ENABLED_CODE = "PREDICTIVE_OPTIMIZATION_ENABLED"
PO_UNKNOWN_CODE = "PREDICTIVE_OPTIMIZATION_UNKNOWN"
PO_SKIPPED_CODE = "PREDICTIVE_OPTIMIZATION_SKIPPED"   # PO not applicable (external/foreign/absent)


@dataclass
class POState:
    """Resolved Predictive Optimization verdict for one target table."""
    fqn:         str
    blocked:     bool                    # True ⇒ do NOT onboard; fail the step
    value:       Optional[str]           # "ENABLE" | "DISABLE" | None (unknown)
    level:       Optional[str]           # where enabled: "TABLE" | "SCHEMA" | "CATALOG"
    source_name: Optional[str]           # name of the inheriting securable (if any)
    reason:      str                     # human-readable message
    error_code:  str                     # "" when allowed; PO_*_CODE when blocked


class _NotFoundError(Exception):
    """Raised internally when the target table, its schema, AND its catalog all 404."""


def _is_not_found(exc: Exception) -> bool:
    """True if a UC GET failed purely because the object does not exist (404 /
    *_DOES_NOT_EXIST / *_NOT_FOUND) — as opposed to a permission/transport error."""
    if getattr(exc, "status_code", None) == 404:
        return True
    msg = str(exc).upper()
    return ("DOES_NOT_EXIST" in msg) or ("NOT_FOUND" in msg) or ("RESOURCE_DOES_NOT_EXIST" in msg)


class PredictiveOptimizationChecker:
    """
    Resolves effective PO for the TARGET (destination) table via the UC REST API.

    `api_client` must be the TARGET-workspace ApiClient (tgt_api) — the PO flag
    that matters is the one the cloned destination table has / will inherit,
    and that lives on the target workspace regardless of clone_type.

    The destination table may not exist yet at INVENTORY time (fresh migration,
    or require_target_precreated=false). In that case the table-level GET 404s,
    so we fall back to the target SCHEMA, then the target CATALOG, whose
    effective_predictive_optimization_flag is exactly what the freshly-cloned
    table would inherit. Only a 404 falls through to the parent; any other error
    (permission/transport) or an existing object with no resolvable flag fails
    closed (PREDICTIVE_OPTIMIZATION_UNKNOWN) — we never onboard a table whose PO
    we cannot prove is disabled.
    """

    def __init__(self, api_client):
        self._api = api_client

    def _resolve_securable(self, catalog: str, schema: str, table: str):
        """
        Return (uc_object, scope_label). Walk target table → schema → catalog,
        descending to the parent ONLY when the child does not exist (404).
        Raises `_NotFoundError` if every level 404s; re-raises any non-404
        (permission/transport) error for the caller to handle.
        """
        attempts = [
            ("tables",   f"{catalog}.{schema}.{table}", "table"),
            ("schemas",  f"{catalog}.{schema}",         "schema"),
            ("catalogs", f"{catalog}",                  "catalog"),
        ]
        last_not_found: Optional[Exception] = None
        for kind, name, label in attempts:
            try:
                obj = self._api.unity_catalog_get(kind, name)
            except Exception as e:
                if _is_not_found(e):
                    last_not_found = e
                    continue      # object missing — try the parent securable
                raise             # permission/transport error — handled by caller
            return obj, label
        raise _NotFoundError(str(last_not_found) if last_not_found else "target securable not found")

    def check(self, target_catalog: str, target_schema: str, target_table: str) -> POState:
        """
        Verdict policy (fail-OPEN except for a confirmed ENABLE):
          • effective PO == ENABLE on a managed target (or inherited from its
            schema/catalog)                    → BLOCKED (PO_ENABLED_CODE).
          • effective PO == DISABLE             → allowed.
          • target is a non-managed table
            (EXTERNAL / FOREIGN / shared / view) → allowed, SKIPPED — PO never
            runs on it (PO_SKIPPED_CODE).
          • target securable does not exist yet → allowed, SKIPPED — nothing to
            optimize on a table that isn't there (PO_SKIPPED_CODE).
          • status unreadable (permission/transport) or flag missing/unknown
                                                → allowed, continue (PO_UNKNOWN_CODE),
            logged as a warning. Does NOT fail the run (per operational
            requirement: an unconfirmable PO status must not block migration).
        """
        fqn = f"{target_catalog}.{target_schema}.{target_table}"
        try:
            obj, scope = self._resolve_securable(target_catalog, target_schema, target_table)
        except _NotFoundError:
            return POState(
                fqn=fqn, blocked=False, value=None, level=None, source_name=None,
                reason="Target securable does not exist yet; PO not applicable — skipped.",
                error_code=PO_SKIPPED_CODE,
            )
        except Exception as e:
            # Unknown (permission/transport). Policy: do NOT fail — continue.
            log.warning("PO status UNKNOWN for target %s (%s) — continuing without blocking.",
                        fqn, str(e)[:200])
            return POState(
                fqn=fqn, blocked=False, value=None, level=None, source_name=None,
                reason=f"PO status unknown ({str(e)[:200]}) — continuing without blocking.",
                error_code=PO_UNKNOWN_CODE,
            )

        # PO only ever runs on MANAGED Delta tables. If the resolved object is a
        # table that is NOT managed (external / foreign / shared / view), the gate
        # does not apply — skip it.
        if scope == "table":
            ttype = (obj.get("table_type") or "").upper()
            if ttype and ttype != "MANAGED":
                return POState(
                    fqn=fqn, blocked=False, value=None, level=None, source_name=None,
                    reason=f"Target is a {ttype} table; predictive optimization not "
                           f"applicable — skipped.",
                    error_code=PO_SKIPPED_CODE,
                )

        eff = obj.get("effective_predictive_optimization_flag") or {}
        value = eff.get("value")

        if value == "ENABLE":
            level = eff.get("inherited_from_type") or "TABLE"
            src   = eff.get("inherited_from_name")
            where = f"{level} {src}".strip() if src else f"set directly ({scope})"
            return POState(
                fqn=fqn, blocked=True, value="ENABLE", level=level, source_name=src,
                reason=f"Predictive optimization is ENABLED on the target ({where}) — "
                       f"disable it before migrating into this target table.",
                error_code=PO_ENABLED_CODE,
            )

        if value == "DISABLE":
            return POState(
                fqn=fqn, blocked=False, value="DISABLE", level=None, source_name=None,
                reason="Predictive optimization disabled.", error_code="",
            )

        # Missing / unrecognised effective flag → UNKNOWN. Policy: continue.
        log.warning("PO effective flag missing/unrecognised for target %s (%r) — "
                    "continuing without blocking.", fqn, eff)
        return POState(
            fqn=fqn, blocked=False, value=value, level=None, source_name=None,
            reason=f"PO status could not be confirmed (effective flag: {eff!r}) — "
                   f"continuing without blocking.",
            error_code=PO_UNKNOWN_CODE,
        )


def check_selections(
    checker: PredictiveOptimizationChecker,
    selections: List,
    threads: int = 4,
) -> Dict[str, POState]:
    """
    Run the PO check across many TableSelections in parallel (respecting the
    same thread budget as INVENTORY's reads). Returns {source_fqn: POState}.

    Used by the DRY_RUN path (which has no per-table onboarding loop of its
    own); the INVENTORY path calls `checker.check(...)` inline inside its
    existing ThreadPoolExecutor worker instead of adding a second pass.
    """
    results: Dict[str, POState] = {}
    if not selections:
        return results

    def _one(sel) -> Tuple[str, POState]:
        return sel.source_fqn, checker.check(
            sel.target_catalog, sel.target_schema, sel.target_table
        )

    with ThreadPoolExecutor(max_workers=max(1, threads), thread_name_prefix="po") as ex:
        for fut in as_completed([ex.submit(_one, s) for s in selections]):
            fqn, state = fut.result()
            results[fqn] = state
    return results
