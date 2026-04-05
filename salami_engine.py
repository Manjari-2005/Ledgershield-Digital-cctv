"""
Salami-slicing detection: micro-transactions (≤₹10), 5-minute aggregation, 10-minute circuit breaker,
Z-score spikes on bucket counts, and low-value (≤₹3) notification log.

Optional upgrade: fit sklearn.ensemble.IsolationForest on per-bucket feature vectors
(count, unique_destinations, mean amount) for multivariate anomaly scoring alongside Z-score.
"""
from __future__ import annotations

import csv
import io
import statistics
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sim_db import (
    clear_salami_staging,
    insert_salami_rows,
    insert_tx_notifications,
    load_salami_rows,
)

MICRO_MAX_INR = 10.0
WINDOW_AGG_MINUTES = 5
CIRCUIT_WINDOW_MINUTES = 10
CIRCUIT_UNIQUE_SOURCES = 5000
NOTIFY_MAX_INR = 3.0
Z_SPIKE = 3.0


@dataclass
class SalamiRow:
    ts: datetime
    amount_inr: float
    source: str
    destination: str
    merchant_id: str
    ref: str


def _parse_ts(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _norm_header(h: str) -> str:
    return h.strip().lower().replace(" ", "_").replace("-", "_")


def parse_transaction_csv(text: str) -> tuple[list[SalamiRow], list[str]]:
    """Parse flexible CSV; returns rows + error messages."""
    errors: list[str] = []
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    if not reader.fieldnames:
        return [], ["Empty or invalid CSV"]
    colmap = {_norm_header(c): c for c in reader.fieldnames}

    def pick(*names: str) -> str | None:
        for n in names:
            k = _norm_header(n)
            if k in colmap:
                return colmap[k]
        return None

    c_ts = pick("timestamp", "txn_time", "time", "datetime", "transaction_time")
    c_amt = pick("amount", "amount_inr", "amt", "amt_inr", "value")
    c_src = pick("source_account", "source", "from_account", "payer", "sender")
    c_dst = pick("destination_id", "destination", "destination_account", "sink", "to_account")
    c_mer = pick("merchant_id", "merchant", "mid", "payee_merchant")
    c_ref = pick("txn_id", "id", "reference", "ref")

    if not c_ts or not c_amt:
        errors.append("CSV must include timestamp and amount columns.")
        return [], errors

    rows: list[SalamiRow] = []
    for i, raw in enumerate(reader):
        ts = _parse_ts(str(raw.get(c_ts or "", "")))
        if not ts:
            continue
        try:
            amt = float(str(raw.get(c_amt, "0")).replace(",", ""))
        except ValueError:
            continue
        src = str(raw.get(c_src or "", "") or f"SRC-{i}")
        dst_col = c_dst or c_mer
        dst = str(raw.get(dst_col or "", "") or "UNKNOWN_DEST")
        mer = str(raw.get(c_mer, "") or dst)
        ref = str(raw.get(c_ref or "", "") or f"TX-{i}")
        rows.append(SalamiRow(ts=ts, amount_inr=amt, source=src, destination=dst, merchant_id=mer, ref=ref))
    if not rows:
        errors.append("No parseable rows (check timestamp/amount formats).")
    return rows, errors


def ingest_csv_text(text: str) -> dict[str, Any]:
    rows, errs = parse_transaction_csv(text)
    if not rows:
        return {"ok": False, "errors": errs, "inserted": 0}
    batch = str(uuid.uuid4())
    clear_salami_staging()
    insert_salami_rows(batch, rows)
    notifs = [r for r in rows if r.amount_inr <= NOTIFY_MAX_INR]
    insert_tx_notifications(notifs)
    analysis = analyze_staged_rows(rows)
    analysis["ok"] = True
    analysis["inserted"] = len(rows)
    analysis["batch_id"] = batch
    analysis["errors"] = errs
    analysis["low_value_alerts"] = len(notifs)
    return analysis


def analyze_staged_rows(rows: list[SalamiRow]) -> dict[str, Any]:
    micro = [r for r in rows if 0 < r.amount_inr <= MICRO_MAX_INR]
    if not micro:
        return _empty_analysis("No micro-transactions (≤₹10) in dataset.")

    micro.sort(key=lambda r: r.ts)

    # 5-minute global buckets (line chart)
    bucket_counts: dict[str, int] = Counter()
    bucket_volume_inr: dict[str, float] = defaultdict(float)
    for r in micro:
        floored = r.ts.replace(second=0, microsecond=0)
        m = floored.minute - (floored.minute % WINDOW_AGG_MINUTES)
        bkt = floored.replace(minute=m).strftime("%Y-%m-%d %H:%M")
        bucket_counts[bkt] += 1
        bucket_volume_inr[bkt] += r.amount_inr

    labels = sorted(bucket_counts.keys())
    counts = [bucket_counts[k] for k in labels]
    volumes = [round(bucket_volume_inr[k], 2) for k in labels]

    z_spikes: list[str] = []
    if len(counts) >= 4:
        mu = statistics.mean(counts)
        sd = statistics.stdev(counts) or 1.0
        for k, v in zip(labels, counts):
            if sd and (v - mu) / sd >= Z_SPIKE:
                z_spikes.append(k)

    # Circuit breaker: unique sources → destination in CIRCUIT_WINDOW_MINUTES
    by_dest: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    for r in micro:
        key = r.destination or r.merchant_id
        by_dest[key].append((r.ts, r.source))

    sinks: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    win = timedelta(minutes=CIRCUIT_WINDOW_MINUTES)

    for dest, evs in by_dest.items():
        evs.sort(key=lambda x: x[0])
        dq: deque[tuple[datetime, str]] = deque()
        src_counts: dict[str, int] = defaultdict(int)
        max_unique = 0
        for t, src in evs:
            dq.append((t, src))
            src_counts[src] += 1
            while dq and t - dq[0][0] > win:
                ot, osrc = dq.popleft()
                src_counts[osrc] -= 1
                if src_counts[osrc] <= 0:
                    del src_counts[osrc]
            nu = len(src_counts)
            max_unique = max(max_unique, nu)
        tripped = max_unique >= CIRCUIT_UNIQUE_SOURCES
        sinks.append(
            {
                "destination_id": dest,
                "micro_count": len(evs),
                "unique_sources_max_window": max_unique,
                "tripped": tripped,
                "status": "Pending Verification" if tripped else "Monitoring",
            }
        )
        if tripped:
            pending.append(
                {
                    "destination_id": dest,
                    "reason": f"≥{CIRCUIT_UNIQUE_SOURCES} unique source accounts in a "
                    f"{CIRCUIT_WINDOW_MINUTES}-minute window (micro ≤₹{MICRO_MAX_INR:.0f}).",
                }
            )

    sinks.sort(key=lambda x: (-int(x["tripped"]), -x["micro_count"]))

    return {
        "line_labels": labels,
        "line_counts": counts,
        "line_volume_inr": volumes,
        "z_score_spike_buckets": z_spikes,
        "sinks": sinks[:50],
        "pending_verification": pending,
        "params": {
            "micro_max_inr": MICRO_MAX_INR,
            "agg_window_min": WINDOW_AGG_MINUTES,
            "circuit_window_min": CIRCUIT_WINDOW_MINUTES,
            "circuit_unique_sources": CIRCUIT_UNIQUE_SOURCES,
            "notify_max_inr": NOTIFY_MAX_INR,
        },
        "source": "upload",
    }


def _empty_analysis(msg: str) -> dict[str, Any]:
    return {
        "line_labels": [],
        "line_counts": [],
        "line_volume_inr": [],
        "z_score_spike_buckets": [],
        "sinks": [],
        "pending_verification": [],
        "note": msg,
        "source": "empty",
    }


def analyze_from_db() -> dict[str, Any]:
    raw = load_salami_rows()
    if not raw:
        return _empty_analysis("Upload a transaction spreadsheet to enable velocity analytics.")
    rows = []
    for r in raw:
        ts_raw = str(r["ts"]).replace("Z", "+00:00")
        try:
            ts = datetime.fromisoformat(ts_raw)
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        rows.append(
            SalamiRow(
                ts=ts,
                amount_inr=float(r["amount_inr"]),
                source=r["source"],
                destination=r["destination"],
                merchant_id=r["merchant_id"],
                ref=r["ref"],
            )
        )
    return analyze_staged_rows(rows)


def merge_with_tigergraph(tg_payload: dict[str, Any]) -> dict[str, Any]:
    """Prefer upload line chart if present; merge sink lists."""
    up = analyze_from_db()
    has_upload = bool(up.get("line_labels"))
    out: dict[str, Any] = {
        "upload": up if has_upload or up.get("sinks") else None,
        "tigergraph": tg_payload,
        "line_chart": {
            "labels": up["line_labels"] if has_upload else tg_payload.get("labels", []),
            "counts": up["line_counts"] if has_upload else tg_payload.get("values", []),
            "unit": "inr_micro_volume" if has_upload else "tg_sample_buckets",
        },
        "sinks": up.get("sinks") or [],
        "tg_sinks": tg_payload.get("sinks") or [],
        "pending_verification": up.get("pending_verification") or [],
        "z_score_spike_buckets": up.get("z_score_spike_buckets") or [],
        "params": up.get("params")
        or {
            "micro_max_inr": MICRO_MAX_INR,
            "agg_window_min": WINDOW_AGG_MINUTES,
            "circuit_window_min": CIRCUIT_WINDOW_MINUTES,
            "circuit_unique_sources": CIRCUIT_UNIQUE_SOURCES,
        },
        "notifications_preview": [],
    }
    if not out["line_chart"]["labels"] and tg_payload.get("labels"):
        out["line_chart"]["labels"] = tg_payload["labels"]
        out["line_chart"]["counts"] = tg_payload.get("values", [])
        out["line_chart"]["unit"] = "tg_sample_buckets"
    return out
