"""SQLite store for employees, actions, AI job logs, and simulated alerts (TigerGraph holds graph truth)."""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "ledger_shield.db"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextmanager
def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS employees (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                role TEXT,
                department TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS frozen_accounts (
                account_id TEXT PRIMARY KEY,
                reason TEXT,
                action TEXT NOT NULL,
                created_at TEXT NOT NULL,
                analyst TEXT
            );
            CREATE TABLE IF NOT EXISTS merge_runs (
                id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                payload TEXT,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS document_jobs (
                id TEXT PRIMARY KEY,
                filename TEXT,
                ocr_json TEXT,
                verification_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS internal_alerts (
                id TEXT PRIMARY KEY,
                employee_id TEXT,
                title TEXT NOT NULL,
                narrative TEXT NOT NULL,
                severity TEXT,
                created_at TEXT NOT NULL,
                acknowledged INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS uploaded_accounts (
                id TEXT PRIMARY KEY,
                row_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS salami_staging (
                batch_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                amount_inr REAL NOT NULL,
                source TEXT NOT NULL,
                destination TEXT NOT NULL,
                merchant_id TEXT NOT NULL,
                ref TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tx_notifications (
                id TEXT PRIMARY KEY,
                ts TEXT NOT NULL,
                amount_inr REAL NOT NULL,
                ref TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
    _run_migrations()


def seed_if_empty() -> None:
    init_db()
    with get_conn() as c:
        n = c.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
        if n > 0:
            return
        now = _utc_now()
        staff = [
            ("emp-001", "Priya Sharma", "priya@bank.internal", "Analyst", "Fraud Ops", now),
            ("emp-002", "James Okonkwo", "james@bank.internal", "Senior Analyst", "Fraud Ops", now),
            ("emp-003", "Maria Chen", "maria@bank.internal", "Compliance", "Internal Audit", now),
            ("emp-004", "Priya Singh", "psingh@bank.internal", "Operations Analyst", "Central Ops", now),
            ("emp-005", "Raj Sharma", "rsharma@bank.internal", "Teller", "Retail Banking", now),
        ]
        c.executemany(
            "INSERT INTO employees VALUES (?,?,?,?,?,?)",
            staff,
        )
        alerts = [
            (
                "ial-1",
                "emp-004",
                "Bulk Customer Data Export",
                "Priya Singh exported a large volume of customer records outside her normal working "
                "pattern — a significant deviation from typical behavior for her role. Correlation "
                "engine suggests alignment with prior insider data-exfiltration playbooks.",
                "critical",
                now,
                0,
            ),
            (
                "ial-2",
                "emp-005",
                "Accessed High-Value Dormant Account",
                "Raj Sharma accessed multiple dormant high-value accounts after branch hours without "
                "an active service ticket. Velocity of screen views and print jobs exceeds 99th "
                "percentile for teller peers at Branch #42.",
                "critical",
                now,
                0,
            ),
            (
                "ial-3",
                "emp-001",
                "Back-dated Transaction Entry",
                "A GL reversal was posted with an effective date 14 days in the past, bypassing "
                "dual-control approval. Linked movement into a newly onboarded external wallet was "
                "detected within the same session cluster.",
                "high",
                now,
                0,
            ),
        ]
        c.executemany(
            "INSERT INTO internal_alerts VALUES (?,?,?,?,?,?,?)",
            alerts,
        )


def list_employees() -> list[dict]:
    seed_if_empty()
    with get_conn() as c:
        rows = c.execute("SELECT * FROM employees ORDER BY name").fetchall()
        return [dict(r) for r in rows]


def add_employee(name: str, email: str, role: str, department: str) -> dict:
    seed_if_empty()
    eid = f"emp-{uuid.uuid4().hex[:8]}"
    now = _utc_now()
    with get_conn() as c:
        c.execute(
            "INSERT INTO employees VALUES (?,?,?,?,?,?)",
            (eid, name, email or "", role or "Staff", department or "General", now),
        )
    return {"id": eid, "name": name, "email": email, "role": role, "department": department}


def bulk_employees(rows: list[dict]) -> int:
    seed_if_empty()
    now = _utc_now()
    n = 0
    with get_conn() as c:
        for r in rows:
            eid = f"emp-{uuid.uuid4().hex[:8]}"
            c.execute(
                "INSERT INTO employees VALUES (?,?,?,?,?,?)",
                (
                    eid,
                    r.get("name", "Unknown"),
                    r.get("email", ""),
                    r.get("role", "Staff"),
                    r.get("department", "General"),
                    now,
                ),
            )
            n += 1
    return n


def _run_migrations() -> None:
    """Lightweight schema patches for existing DB files."""
    with get_conn() as c:
        cur = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='salami_staging'")
        if not cur.fetchone():
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS salami_staging (
                    batch_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    amount_inr REAL NOT NULL,
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    merchant_id TEXT NOT NULL,
                    ref TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tx_notifications (
                    id TEXT PRIMARY KEY,
                    ts TEXT NOT NULL,
                    amount_inr REAL NOT NULL,
                    ref TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )


SEVERITY_SCORE = {"critical": 95, "high": 88, "medium": 78, "low": 65}


def list_internal_alerts(include_dismissed: bool = False) -> list[dict]:
    seed_if_empty()
    with get_conn() as c:
        rows = c.execute(
            "SELECT * FROM internal_alerts ORDER BY created_at DESC"
        ).fetchall()
        out: list[dict] = []
        for row in rows:
            d = dict(row)
            if not include_dismissed and d.get("acknowledged") == 2:
                continue
            emp = c.execute(
                "SELECT name, role, department FROM employees WHERE id = ?",
                (d.get("employee_id") or "",),
            ).fetchone()
            d["employee_name"] = emp["name"] if emp else "Unknown user"
            d["job_title"] = (emp["role"] if emp else "") or "Staff"
            d["branch"] = f"Branch #{10 + (hash(d.get('employee_id') or '') % 89)}"
            d["risk_score"] = SEVERITY_SCORE.get((d.get("severity") or "").lower(), 82)
            ts = (d.get("created_at") or "").replace("T", " ")
            d["event_time"] = ts[:19] if len(ts) >= 19 else ts
            out.append(d)
        return out


def set_internal_alert_status(alert_id: str, action: str, analyst: str) -> bool:
    """action: 'confirm' -> acknowledged=1, 'dismiss' -> acknowledged=2"""
    seed_if_empty()
    val = 1 if action == "confirm" else 2 if action == "dismiss" else None
    if val is None:
        return False
    with get_conn() as c:
        cur = c.execute(
            "UPDATE internal_alerts SET acknowledged = ? WHERE id = ?",
            (val, alert_id),
        )
        return cur.rowcount > 0


def clear_salami_staging() -> None:
    seed_if_empty()
    with get_conn() as c:
        c.execute("DELETE FROM salami_staging")


def insert_salami_rows(batch_id: str, rows: list) -> None:
    seed_if_empty()
    with get_conn() as c:
        for r in rows:
            c.execute(
                """INSERT INTO salami_staging
                (batch_id, ts, amount_inr, source, destination, merchant_id, ref)
                VALUES (?,?,?,?,?,?,?)""",
                (
                    batch_id,
                    r.ts.isoformat(),
                    r.amount_inr,
                    r.source,
                    r.destination,
                    r.merchant_id,
                    r.ref,
                ),
            )


def insert_tx_notifications(rows: list) -> int:
    seed_if_empty()
    now = _utc_now()
    n = 0
    with get_conn() as c:
        for r in rows:
            nid = f"nf-{uuid.uuid4().hex[:10]}"
            msg = (
                f"Low-value alert: ₹{r.amount_inr:.2f} from {r.source} → {r.destination} "
                f"(threshold ≤₹3). Push notification simulated."
            )
            c.execute(
                "INSERT INTO tx_notifications VALUES (?,?,?,?,?,?)",
                (nid, r.ts.isoformat(), r.amount_inr, r.ref, msg, now),
            )
            n += 1
    return n


def load_salami_rows() -> list[dict]:
    seed_if_empty()
    with get_conn() as c:
        rows = c.execute(
            """SELECT batch_id, ts, amount_inr, source, destination, merchant_id, ref
               FROM salami_staging ORDER BY ts"""
        ).fetchall()
        return [dict(x) for x in rows]


def list_tx_notifications(limit: int = 30) -> list[dict]:
    seed_if_empty()
    with get_conn() as c:
        rows = c.execute(
            "SELECT * FROM tx_notifications ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def freeze_account(account_id: str, reason: str, analyst: str) -> None:
    seed_if_empty()
    with get_conn() as c:
        c.execute(
            """INSERT OR REPLACE INTO frozen_accounts VALUES (?,?,?,?,?)""",
            (account_id, reason, "freeze", _utc_now(), analyst),
        )


def refund_flag(account_id: str, reason: str, analyst: str) -> None:
    seed_if_empty()
    with get_conn() as c:
        c.execute(
            """INSERT OR REPLACE INTO frozen_accounts VALUES (?,?,?,?,?)""",
            (account_id, reason, "refund_initiated", _utc_now(), analyst),
        )


def list_frozen() -> list[dict]:
    seed_if_empty()
    with get_conn() as c:
        rows = c.execute("SELECT * FROM frozen_accounts ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]


def save_merge_run(mode: str, payload: dict | None, result: dict) -> str:
    seed_if_empty()
    rid = f"mr-{uuid.uuid4().hex[:10]}"
    with get_conn() as c:
        c.execute(
            "INSERT INTO merge_runs VALUES (?,?,?,?,?)",
            (
                rid,
                mode,
                json.dumps(payload) if payload else None,
                json.dumps(result),
                _utc_now(),
            ),
        )
    return rid


def save_document_job(filename: str, ocr: dict, verification: dict) -> str:
    seed_if_empty()
    jid = f"doc-{uuid.uuid4().hex[:10]}"
    with get_conn() as c:
        c.execute(
            "INSERT INTO document_jobs VALUES (?,?,?,?,?)",
            (jid, filename, json.dumps(ocr), json.dumps(verification), _utc_now()),
        )
    return jid


def save_uploaded_accounts(batch: list[dict]) -> int:
    seed_if_empty()
    now = _utc_now()
    with get_conn() as c:
        for row in batch:
            c.execute(
                "INSERT INTO uploaded_accounts VALUES (?,?,?)",
                (f"upl-{uuid.uuid4().hex[:12]}", json.dumps(row), now),
            )
    return len(batch)
