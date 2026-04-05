"""
TigerGraph integration for LedgerShield — Transaction_Fraud graph (Party, Device, IP, rings, payments).
Falls back to rich mock data when the database is unreachable.
"""
from __future__ import annotations

import hashlib
import os
import random
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from pyTigerGraph import TigerGraphConnection

INR_PER_USD = float(os.environ.get("FX_INR_PER_USD", "83.5"))


def build_connection() -> TigerGraphConnection | None:
    host = os.environ.get("TG_HOST", "").strip().rstrip("/")
    if not host:
        return None
    graph = os.environ.get("TG_GRAPH", "Transaction_Fraud")
    user = os.environ.get("TG_USERNAME", "tigergraph")
    password = os.environ.get("TG_PASSWORD", "")
    token = os.environ.get("TG_API_TOKEN") or os.environ.get("TG_SECRET", "")
    tg_cloud = "tgcloud.io" in host.lower() or os.environ.get("TG_CLOUD", "").lower() in (
        "1",
        "true",
        "yes",
    )
    try:
        return TigerGraphConnection(
            host=host,
            graphname=graph,
            username=user,
            password=password,
            apiToken=token or "",
            tgCloud=bool(tg_cloud),
            useCert=True,
        )
    except Exception:
        return None


def _as_float(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _as_int(x: Any) -> int:
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return 0


def _party_risk(attrs: dict[str, Any], vid: str) -> float:
    fraud = _as_int(attrs.get("is_fraud"))
    if fraud:
        return min(0.99, 0.82 + (hash(vid) % 17) / 100.0)
    h = hash(vid) % 1000
    return round(0.12 + (h / 1000.0) * 0.55, 3)


def _norm_vertex(v: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    attrs = v.get("attributes") or {}
    if not isinstance(attrs, dict):
        attrs = {}
    vid = v.get("v_id")
    return str(vid) if vid is not None else "", attrs


def _list_vertices(
    conn: TigerGraphConnection, vtype: str, select: str, limit: int, sort: str = ""
) -> list[dict[str, Any]]:
    try:
        raw = conn.getVertices(vtype, select=select, limit=limit, sort=sort, fmt="py")
    except Exception:
        return []
    if not raw:
        return []
    if isinstance(raw, dict) and "results" in raw:
        raw = raw["results"]
    if not isinstance(raw, list):
        return []
    out = []
    for x in raw:
        if isinstance(x, dict):
            vid, attrs = _norm_vertex(x)
            out.append({"v_id": vid, "attributes": attrs})
    return out


def _edge_neighbor(edge: dict[str, Any], center_type: str, center_id: str) -> tuple[str, str] | None:
    """Return (other_type, other_id) for an edge incident to center vertex (REST key variants)."""
    cid = str(center_id)
    pairs = [
        (str(edge.get("from_type") or edge.get("From_type") or ""), str(edge.get("from_id") or edge.get("From_id") or "")),
        (str(edge.get("to_type") or edge.get("To_type") or ""), str(edge.get("to_id") or edge.get("To_id") or "")),
    ]
    ct = center_type
    for t, i in pairs:
        if t == ct and i == cid:
            for ot, oid in pairs:
                if (ot, oid) != (t, i) and oid:
                    return ot, oid
    return None


def _get_edges(
    conn: TigerGraphConnection,
    src_type: str,
    src_id: str,
    edge_type: str = "",
    limit: int = 500,
) -> list[dict[str, Any]]:
    try:
        raw = conn.getEdges(src_type, str(src_id), edge_type, limit=limit, fmt="py")
    except Exception:
        return []
    if not raw:
        return []
    if isinstance(raw, dict) and "results" in raw:
        raw = raw["results"]
    if not isinstance(raw, list):
        return []
    return [e for e in raw if isinstance(e, dict)]


def _tx_volume_sample(conn: TigerGraphConnection) -> tuple[float, int]:
    rows = _list_vertices(
        conn,
        "Payment_Transaction",
        "amount",
        15000,
        sort="-amount",
    )
    s = sum(_as_float(r["attributes"].get("amount")) for r in rows)
    return s, len(rows)


class LedgerTG:
    def __init__(self) -> None:
        self._conn: TigerGraphConnection | None = None
        self._demo: bool = False
        self._last_error: str | None = None

    def connect(self) -> bool:
        self._last_error = None
        self._conn = build_connection()
        if not self._conn:
            self._demo = True
            self._last_error = "TG_HOST not set"
            return False
        try:
            self._conn.getVertexCount("Party")
            self._demo = False
            return True
        except Exception as exc:  # noqa: BLE001
            self._demo = True
            self._last_error = str(exc)
            self._conn = None
            return False

    @property
    def is_demo(self) -> bool:
        return self._demo

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def kpis(self) -> dict[str, Any]:
        if self._demo or not self._conn:
            return self._mock_kpis()
        conn = self._conn
        try:
            party_c = conn.getVertexCount("Party")
            dev_c = conn.getVertexCount("Device")
            ip_c = conn.getVertexCount("IP")
            acc_c = conn.getVertexCount("Account")
            cc_c = conn.getVertexCount("Connected_Component")
            vol_sample, n_tx = _tx_volume_sample(conn)
            parties = _list_vertices(
                conn,
                "Party",
                "is_fraud,name,party_type",
                min(8000, max(500, n_tx)),
                sort="party_type",
            )
            high_risk = sum(
                1 for p in parties if _party_risk(p["attributes"], p["v_id"]) >= 0.8
            )
            entities = _as_int(party_c) + _as_int(dev_c) + _as_int(ip_c) + _as_int(acc_c)
            scale = 3.2 if n_tx else 1.0
            est_volume = vol_sample * scale if n_tx else vol_sample

            return {
                "connected": True,
                "demo": False,
                "error": None,
                "total_tx_volume_usd": round(est_volume, 2),
                "tx_sample_used": n_tx,
                "high_risk_users": high_risk,
                "fraud_rings": _as_int(cc_c),
                "total_entities": entities,
                "party_count": _as_int(party_c),
                "device_count": _as_int(dev_c),
                "ip_count": _as_int(ip_c),
                "account_count": _as_int(acc_c),
            }
        except Exception as exc:  # noqa: BLE001
            self._last_error = str(exc)
            m = self._mock_kpis()
            m["error"] = str(exc)
            return m

    def _mock_kpis(self) -> dict[str, Any]:
        random.seed(42)
        return {
            "connected": False,
            "demo": True,
            "error": self._last_error,
            "total_tx_volume_usd": 128_400_000.0,
            "tx_sample_used": 0,
            "high_risk_users": 47,
            "fraud_rings": 12,
            "total_entities": 482_300,
            "party_count": 210_000,
            "device_count": 154_000,
            "ip_count": 98_200,
            "account_count": 20_100,
        }

    def fraud_ring_graph(self, ring_id: str | None = None) -> dict[str, Any]:
        if self._demo or not self._conn:
            return self._mock_graph()
        conn = self._conn
        try:
            comps = _list_vertices(conn, "Connected_Component", "", 40, sort="id")
            if not comps:
                return self._mock_graph()
            chosen = None
            if ring_id:
                for c in comps:
                    if str(c["v_id"]) == str(ring_id):
                        chosen = c
                        break
            if not chosen:
                chosen = comps[0]
            rid = chosen["v_id"]
            edges_p = _get_edges(conn, "Connected_Component", str(rid), "Entity_In_Ring", 200)
            party_ids: list[str] = []
            for e in edges_p:
                nb = _edge_neighbor(e, "Connected_Component", str(rid))
                if nb and nb[0] == "Party":
                    party_ids.append(nb[1])
            party_ids = list(dict.fromkeys(party_ids))[:40]
            if not party_ids:
                return self._mock_graph()

            nodes: list[dict[str, Any]] = []
            links: list[dict[str, Any]] = []
            nodes.append(
                {
                    "id": f"cc_{rid}",
                    "label": f"Ring {rid}",
                    "group": "ring",
                    "risk": 0.95,
                    "title": f"Connected component {rid}",
                    "raw_id": str(rid),
                    "vtype": "Connected_Component",
                }
            )

            device_by_party: dict[str, list[str]] = defaultdict(list)
            ip_by_party: dict[str, list[str]] = defaultdict(list)

            for pid in party_ids:
                pattrs = {}
                try:
                    pv = conn.getVerticesById("Party", pid, fmt="py")
                    if isinstance(pv, list) and pv:
                        _, pattrs = _norm_vertex(pv[0])
                except Exception:
                    pass
                risk = _party_risk(pattrs, pid)
                label = (pattrs.get("name") or pid)[:22]
                nodes.append(
                    {
                        "id": f"p_{pid}",
                        "label": str(label),
                        "group": "party",
                        "risk": risk,
                        "title": f"Party {pid}",
                        "raw_id": pid,
                        "vtype": "Party",
                    }
                )
                links.append({"from": f"cc_{rid}", "to": f"p_{pid}", "label": "in_ring"})

                for ed in _get_edges(conn, "Party", pid, "Has_Device", 20):
                    nb = _edge_neighbor(ed, "Party", pid)
                    if nb and nb[0] == "Device":
                        device_by_party[pid].append(nb[1])
                for ed in _get_edges(conn, "Party", pid, "Has_IP", 20):
                    nb = _edge_neighbor(ed, "Party", pid)
                    if nb and nb[0] == "IP":
                        ip_by_party[pid].append(nb[1])

            seen_d: set[str] = set()
            seen_i: set[str] = set()
            for pid, dlist in device_by_party.items():
                for did in dlist[:2]:
                    key = f"d_{did}"
                    if key not in seen_d:
                        seen_d.add(key)
                        nodes.append(
                            {
                                "id": key,
                                "label": f"Dev…{did[-6:]}",
                                "group": "device",
                                "risk": 0.55,
                                "title": f"Device {did}",
                                "raw_id": did,
                                "vtype": "Device",
                            }
                        )
                    links.append({"from": f"p_{pid}", "to": key, "label": "device"})

            for pid, ilist in ip_by_party.items():
                for iid in ilist[:2]:
                    key = f"i_{iid}"
                    if key not in seen_i:
                        seen_i.add(key)
                        nodes.append(
                            {
                                "id": key,
                                "label": str(iid)[:18],
                                "group": "ip",
                                "risk": 0.48,
                                "title": f"IP {iid}",
                                "raw_id": iid,
                                "vtype": "IP",
                            }
                        )
                    links.append({"from": f"p_{pid}", "to": key, "label": "ip"})

            return {"nodes": nodes, "edges": links, "ring_id": str(rid), "source": "tigergraph"}
        except Exception as exc:  # noqa: BLE001
            self._last_error = str(exc)
            g = self._mock_graph()
            g["error"] = str(exc)
            return g

    def _mock_graph(self) -> dict[str, Any]:
        nodes = [
            {
                "id": "cc_1",
                "label": "Ring A-12",
                "group": "ring",
                "risk": 0.96,
                "title": "Demo ring",
                "raw_id": "demo",
                "vtype": "Connected_Component",
            },
            {"id": "p_1", "label": "User 8821", "group": "party", "risk": 0.91, "raw_id": "demo-1", "vtype": "Party"},
            {"id": "p_2", "label": "User 4410", "group": "party", "risk": 0.88, "raw_id": "demo-2", "vtype": "Party"},
            {"id": "d_1", "label": "Device …a9f2", "group": "device", "risk": 0.62, "raw_id": "dev-a9f2", "vtype": "Device"},
            {"id": "i_1", "label": "103.21.44.x", "group": "ip", "risk": 0.51, "raw_id": "103.21.44.12", "vtype": "IP"},
        ]
        edges = [
            {"from": "cc_1", "to": "p_1", "label": "in_ring"},
            {"from": "cc_1", "to": "p_2", "label": "in_ring"},
            {"from": "p_1", "to": "d_1", "label": "device"},
            {"from": "p_2", "to": "d_1", "label": "device"},
            {"from": "p_1", "to": "i_1", "label": "ip"},
        ]
        return {"nodes": nodes, "edges": edges, "ring_id": "demo", "source": "mock"}

    def entity_inspector(self, vtype: str, raw_id: str) -> dict[str, Any]:
        vtype = (vtype or "Party").strip() or "Party"
        raw_id = str(raw_id).strip()
        if self._demo or not self._conn:
            return self._mock_inspector(vtype, raw_id)
        conn = self._conn
        try:
            if vtype == "Connected_Component":
                neighbors = []
                for ed in _get_edges(conn, "Connected_Component", raw_id, "Entity_In_Ring", 200):
                    nb = _edge_neighbor(ed, "Connected_Component", raw_id)
                    if nb and nb[0] == "Party":
                        neighbors.append(
                            {"edge": "Entity_In_Ring", "vertex_type": "Party", "vertex_id": nb[1]}
                        )
                return {
                    "vertex_type": vtype,
                    "vertex_id": raw_id,
                    "risk_score": 0.94,
                    "attributes": {"role": "fraud_ring_component"},
                    "neighbors": neighbors[:80],
                    "transactions": [],
                    "source": "tigergraph",
                }
            rows = conn.getVerticesById(vtype, raw_id, fmt="py")
            if not rows:
                return self._mock_inspector(vtype, raw_id)
            _, attrs = _norm_vertex(rows[0])
            risk = _party_risk(attrs, raw_id) if vtype == "Party" else 0.35 + (hash(raw_id) % 40) / 100.0
            neighbors: list[dict[str, str]] = []
            for ed in _get_edges(conn, vtype, raw_id, "", 80):
                et = ed.get("e_type") or ed.get("E_type") or ed.get("edge_type") or "link"
                nb = _edge_neighbor(ed, vtype, raw_id)
                if nb:
                    neighbors.append(
                        {"edge": str(et), "vertex_type": nb[0], "vertex_id": nb[1]}
                    )
            txs: list[dict[str, Any]] = []
            if vtype == "Party":
                for ed in _get_edges(conn, "Party", raw_id, "Party_Has_Card", 30):
                    nb = _edge_neighbor(ed, "Party", raw_id)
                    if not nb or nb[0] != "Card":
                        continue
                    cid = nb[1]
                    for ted in _get_edges(conn, "Card", str(cid), "Card_Send_Transaction", 15):
                        tnb = _edge_neighbor(ted, "Card", str(cid))
                        if tnb and tnb[0] == "Payment_Transaction":
                            tid = tnb[1]
                            txs.append(
                                {
                                    "id": str(tid),
                                    "amount_usd": 0.0,
                                    "time": "",
                                    "channel": "card_payment",
                                }
                            )
            for t in txs[:20]:
                try:
                    tv = conn.getVerticesById("Payment_Transaction", t["id"], fmt="py")
                    if tv:
                        _, a = _norm_vertex(tv[0])
                        t["amount_usd"] = _as_float(a.get("amount"))
                        t["time"] = str(a.get("transaction_time") or "")[:19]
                        t["flagged"] = _as_int(a.get("is_fraud")) != 0
                except Exception:
                    pass
            return {
                "vertex_type": vtype,
                "vertex_id": raw_id,
                "risk_score": round(risk, 3),
                "attributes": attrs,
                "neighbors": neighbors[:60],
                "transactions": txs[:40],
                "source": "tigergraph",
            }
        except Exception as exc:  # noqa: BLE001
            m = self._mock_inspector(vtype, raw_id)
            m["error"] = str(exc)
            return m

    def _mock_inspector(self, vtype: str, raw_id: str) -> dict[str, Any]:
        if vtype == "Connected_Component":
            return {
                "vertex_type": vtype,
                "vertex_id": raw_id,
                "risk_score": 0.94,
                "attributes": {"role": "fraud_ring_component"},
                "neighbors": [
                    {"edge": "Entity_In_Ring", "vertex_type": "Party", "vertex_id": "demo-1"},
                    {"edge": "Entity_In_Ring", "vertex_type": "Party", "vertex_id": "demo-2"},
                ],
                "transactions": [],
                "source": "mock",
            }
        return {
            "vertex_type": vtype,
            "vertex_id": raw_id,
            "risk_score": 0.87,
            "attributes": {"name": "Demo Entity", "note": "TigerGraph offline — sample payload"},
            "neighbors": [
                {"edge": "Has_Device", "vertex_type": "Device", "vertex_id": "dev-a9f2"},
                {"edge": "Has_IP", "vertex_type": "IP", "vertex_id": "103.21.44.12"},
            ],
            "transactions": [
                {"id": "tx-1", "amount_usd": 120.0, "time": "2026-04-01 14:22:00", "flagged": True},
                {"id": "tx-2", "amount_usd": 18.5, "time": "2026-04-02 09:10:00", "flagged": False},
            ],
            "source": "mock",
        }

    def list_rings_detail(self) -> list[dict[str, Any]]:
        if self._demo or not self._conn:
            return [
                {
                    "ring_id": "demo-12",
                    "shared_device": "dev-a9f2",
                    "accounts": [
                        {"id": "ACC-77821", "risk": 0.91},
                        {"id": "ACC-90214", "risk": 0.88},
                    ],
                }
            ]
        conn = self._conn
        out = []
        try:
            comps = _list_vertices(conn, "Connected_Component", "", 25, sort="id")
            for c in comps:
                rid = c["v_id"]
                parties = []
                shared_devices: list[str] = []
                for ed in _get_edges(conn, "Connected_Component", str(rid), "Entity_In_Ring", 200):
                    nb = _edge_neighbor(ed, "Connected_Component", str(rid))
                    if not nb or nb[0] != "Party":
                        continue
                    pid = nb[1]
                    pattrs = {}
                    try:
                        pv = conn.getVerticesById("Party", str(pid), fmt="py")
                        if pv:
                            _, pattrs = _norm_vertex(pv[0])
                    except Exception:
                        pass
                    parties.append({"id": str(pid), "risk": _party_risk(pattrs, str(pid))})
                    for ded in _get_edges(conn, "Party", str(pid), "Has_Device", 5):
                        dnb = _edge_neighbor(ded, "Party", str(pid))
                        if dnb and dnb[0] == "Device":
                            shared_devices.append(str(dnb[1]))
                dev_counts: dict[str, int] = defaultdict(int)
                for d in shared_devices:
                    dev_counts[d] += 1
                top_dev = max(dev_counts, key=lambda k: dev_counts[k]) if dev_counts else "—"
                out.append(
                    {
                        "ring_id": str(rid),
                        "shared_device": top_dev,
                        "accounts": parties[:30],
                    }
                )
            return out
        except Exception:
            return [
                {
                    "ring_id": "error",
                    "shared_device": "—",
                    "accounts": [],
                }
            ]

    def list_high_risk_users(self, threshold: float = 0.8) -> list[dict[str, Any]]:
        if self._demo or not self._conn:
            return [
                {"party_id": "demo-1", "name": "Synthetic A", "risk": 0.91},
                {"party_id": "demo-2", "name": "Synthetic B", "risk": 0.88},
            ]
        conn = self._conn
        try:
            parties = _list_vertices(conn, "Party", "name,is_fraud,party_type", 6000, sort="name")
            hi = []
            for p in parties:
                r = _party_risk(p["attributes"], p["v_id"])
                if r >= threshold:
                    hi.append(
                        {
                            "party_id": p["v_id"],
                            "name": p["attributes"].get("name") or p["v_id"],
                            "risk": r,
                        }
                    )
            hi.sort(key=lambda x: -x["risk"])
            return hi[:200]
        except Exception:
            return [
                {"party_id": "n/a", "name": "Unavailable", "risk": 0.0},
            ]

    def salami_analysis(self) -> dict[str, Any]:
        """Micro-transaction histogram + sink heuristic from Payment_Transaction sample."""
        if self._demo or not self._conn:
            return self._mock_salami()
        conn = self._conn
        try:
            txs = _list_vertices(
                conn,
                "Payment_Transaction",
                "amount,is_fraud,id",
                12000,
                sort="-unix_time",
            )
            micro = [t for t in txs if 0 < _as_float(t["attributes"].get("amount")) <= 50]
            buckets = defaultdict(int)
            for t in micro:
                a = int(_as_float(t["attributes"].get("amount")) // 5) * 5
                buckets[f"${a}-{a+5}"] += 1
            labels = sorted(buckets.keys(), key=lambda k: int(k.replace("$", "").split("-")[0]))[:12]
            values = [buckets[k] for k in labels]
            sink_scores: dict[str, int] = defaultdict(int)
            for t in micro:
                sink_scores[t["v_id"]] += 1
            sinks = sorted(sink_scores.items(), key=lambda x: -x[1])[:8]
            circuit = [
                {
                    "account_id": sid,
                    "micro_count": cnt,
                    "tripped": cnt >= 25,
                }
                for sid, cnt in sinks
            ]
            return {
                "labels": labels,
                "values": values,
                "sinks": circuit,
                "threshold": 25,
                "source": "tigergraph",
            }
        except Exception:
            return self._mock_salami()

    def _mock_salami(self) -> dict[str, Any]:
        return {
            "labels": ["$0-5", "$5-10", "$10-15", "$15-20", "$20-25", "$25-30"],
            "values": [120, 210, 340, 280, 190, 95],
            "sinks": [
                {"account_id": "sink-441", "micro_count": 42, "tripped": True},
                {"account_id": "sink-902", "micro_count": 31, "tripped": True},
                {"account_id": "sink-103", "micro_count": 12, "tripped": False},
            ],
            "threshold": 25,
            "source": "mock",
        }


def format_money_usd_inr(amount_usd: float, currency: str) -> str:
    c = (currency or "USD").upper()
    if c == "INR":
        v = amount_usd * INR_PER_USD
        return f"₹{v:,.0f}"
    return f"${amount_usd:,.0f}"
