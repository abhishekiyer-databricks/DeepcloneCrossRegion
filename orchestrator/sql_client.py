"""
sql_client.py — SQL Statement Execution API client.

Features
--------
• Transparent OAuth 2.0 M2M token acquisition and refresh (Databricks OIDC).
• Polls statement until SUCCEEDED / FAILED with configurable timeout.
• Returns typed rows as List[Dict[str, Any]].
• Separate source and target clients with independent token caches.
• Rate-limit awareness (backs off on 429).
"""

from __future__ import annotations
import time
import threading
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

log = logging.getLogger(__name__)


class TokenCache:
    """Thread-safe cached OAuth token with early expiry."""

    _REFRESH_MARGIN_S = 120  # refresh 2 minutes before expiry

    def __init__(self, workspace_url: str, client_id: str, client_secret: str):
        self._url    = workspace_url.rstrip("/")
        self._cid    = client_id
        self._secret = client_secret
        self._token: Optional[str] = None
        self._expires_at: float    = 0.0
        self._lock = threading.Lock()

    def get(self) -> str:
        with self._lock:
            if time.time() < self._expires_at - self._REFRESH_MARGIN_S and self._token:
                return self._token
            self._token, ttl = self._fetch()
            self._expires_at = time.time() + ttl
            return self._token

    def _fetch(self) -> Tuple[str, int]:
        resp = requests.post(
            f"{self._url}/oidc/v1/token",
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._cid,
                "client_secret": self._secret,
                "scope":         "all-apis",
            },
            timeout=20,
        )
        if not resp.ok:
            raise RuntimeError(
                f"{resp.status_code} POST /oidc/v1/token: {resp.text[:2000]}"
            )
        d = resp.json()
        return d["access_token"], d.get("expires_in", 3600)


class SqlClient:
    """
    Wraps the Databricks SQL Statement Execution API.

    Usage
    -----
    client = SqlClient(workspace_url, client_id, client_secret, warehouse_id)
    rows   = client.execute("SELECT * FROM t WHERE status = 'PENDING'")
    client.execute_ddl("CREATE SCHEMA IF NOT EXISTS cat.sch")
    """

    _POLL_INTERVAL_S = 2
    _MAX_WAIT_S      = 300   # 5 minutes per statement

    def __init__(
        self,
        workspace_url:  str,
        client_id:      str,
        client_secret:  str,
        warehouse_id:   str,
        throttle_s:     float = 0.2,
    ):
        self._url        = workspace_url.rstrip("/")
        self._wh         = warehouse_id
        self._throttle   = throttle_s
        self._tokens     = TokenCache(workspace_url, client_id, client_secret)

    # ── Public API ────────────────────────────────────────────────────────────

    def execute(self, sql: str, timeout_s: int = 300) -> List[Dict[str, Any]]:
        """
        Execute a SQL statement. Returns list of rows as dicts.
        For DDL / DML with no result set, returns [].
        Raises RuntimeError on failure.
        Handles multi-chunk (paginated) results transparently.
        """
        if self._throttle:
            time.sleep(self._throttle)

        stmt_id, initial = self._submit(sql)

        state = initial.get("status", {}).get("state", "")
        data  = initial

        deadline = time.time() + timeout_s
        while state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            if time.time() > deadline:
                self._cancel(stmt_id)
                raise TimeoutError(f"SQL statement timed out after {timeout_s}s: {sql[:80]}")
            time.sleep(self._POLL_INTERVAL_S)
            data  = self._poll(stmt_id)
            state = data.get("status", {}).get("state", "")

        if state == "SUCCEEDED":
            return self._extract_rows_paginated(stmt_id, data)

        err = data.get("status", {}).get("error", {})
        raise RuntimeError(
            f"[{err.get('error_code','SQL_ERROR')}] {err.get('message','Unknown SQL error')} "
            f"| SQL: {sql[:120]}"
        )

    def execute_ddl(self, sql: str) -> None:
        """Execute DDL / DML. Swallows the empty row result."""
        self.execute(sql)

    def execute_one(self, sql: str) -> Optional[Dict[str, Any]]:
        """Execute SQL that should return exactly one row. Returns None if empty."""
        rows = self.execute(sql)
        return rows[0] if rows else None

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._tokens.get()}",
            "Content-Type":  "application/json",
        }

    def _submit(self, sql: str) -> Tuple[str, Dict]:
        for attempt in range(3):
            resp = requests.post(
                f"{self._url}/api/2.0/sql/statements",
                headers=self._headers(),
                json={
                    "statement":       sql,
                    "warehouse_id":    self._wh,
                    "wait_timeout":    "50s",
                    "on_wait_timeout": "CONTINUE",
                },
                timeout=65,
            )
            if resp.status_code == 429:
                log.warning("Rate limited — backing off 30s")
                time.sleep(30)
                continue
            if not resp.ok:
                raise RuntimeError(
                    f"{resp.status_code} POST /api/2.0/sql/statements: {resp.text[:2000]}"
                )
            d = resp.json()
            return d["statement_id"], d
        raise RuntimeError("Failed to submit SQL after 3 retries (rate limited)")

    def _poll(self, stmt_id: str) -> Dict:
        resp = requests.get(
            f"{self._url}/api/2.0/sql/statements/{stmt_id}",
            headers=self._headers(),
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(
                f"{resp.status_code} GET /api/2.0/sql/statements/{stmt_id}: {resp.text[:2000]}"
            )
        return resp.json()

    def _cancel(self, stmt_id: str) -> None:
        try:
            requests.post(
                f"{self._url}/api/2.0/sql/statements/{stmt_id}/cancel",
                headers=self._headers(),
                timeout=15,
            )
        except Exception:
            pass

    @staticmethod
    def _extract_rows(data: Dict) -> List[Dict[str, Any]]:
        """Extract rows from the first chunk only (kept for legacy callers)."""
        result = data.get("result", {})
        if not result.get("data_array"):
            return []
        cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in result["data_array"]]

    def _extract_rows_paginated(self, stmt_id: str, data: Dict) -> List[Dict[str, Any]]:
        """
        Extract ALL rows across multiple result chunks.

        The SQL Statement Execution API paginates large result sets into chunks.
        Each chunk is fetched via:
          GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}
        """
        manifest = data.get("manifest", {})
        cols = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
        if not cols:
            return []

        total_chunks = manifest.get("total_chunk_count", 1)
        rows: List[Dict[str, Any]] = []

        # First chunk is embedded in the initial response
        first_result = data.get("result", {})
        if first_result.get("data_array"):
            rows.extend(dict(zip(cols, row)) for row in first_result["data_array"])

        # Fetch remaining chunks if any
        for chunk_idx in range(1, total_chunks):
            resp = requests.get(
                f"{self._url}/api/2.0/sql/statements/{stmt_id}/result/chunks/{chunk_idx}",
                headers=self._headers(),
                timeout=60,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"{resp.status_code} GET /api/2.0/sql/statements/{stmt_id}/result/chunks/{chunk_idx}: "
                    f"{resp.text[:2000]}"
                )
            chunk = resp.json()
            chunk_data = chunk.get("data_array", [])
            if chunk_data:
                rows.extend(dict(zip(cols, row)) for row in chunk_data)
            log.debug("Fetched chunk %d/%d — %d rows", chunk_idx + 1, total_chunks, len(chunk_data))

        return rows

    # ── Warehouse management ──────────────────────────────────────────────────

    def start_warehouse(self) -> None:
        """Start the warehouse if not already RUNNING."""
        tok = self._tokens.get()
        requests.post(
            f"{self._url}/api/2.0/sql/warehouses/{self._wh}/start",
            headers={"Authorization": f"Bearer {tok}"},
            timeout=30,
        )
        log.info("Warehouse %s start requested", self._wh)
        for _ in range(30):
            r = requests.get(
                f"{self._url}/api/2.0/sql/warehouses/{self._wh}",
                headers={"Authorization": f"Bearer {self._tokens.get()}"},
                timeout=20,
            ).json()
            if r.get("state") == "RUNNING":
                log.info("Warehouse %s is RUNNING", self._wh)
                return
            time.sleep(5)
        raise TimeoutError(f"Warehouse {self._wh} did not start within 150s")
