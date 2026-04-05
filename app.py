"""
LedgerShield Digital CCTV — Flask hub with TigerGraph-backed fraud intelligence.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from flask import (
    Flask,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from ai_stubs import (
    ai_suggest_synthetic_identity_merge,
    assistant_flow,
    ghost_buster_verify,
    parse_csv_accounts,
    parse_employee_csv,
    simulate_ocr_text,
)
from ledger_tg import LedgerTG, format_money_usd_inr, INR_PER_USD
from salami_engine import ingest_csv_text, merge_with_tigergraph
from sim_db import (
    add_employee,
    bulk_employees,
    freeze_account,
    list_employees,
    list_frozen,
    list_internal_alerts,
    list_tx_notifications,
    refund_flag,
    save_document_job,
    save_merge_run,
    save_uploaded_accounts,
    seed_if_empty,
    set_internal_alert_status,
)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "ledger-shield-dev-secret-change-me")
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024


@app.context_processor
def inject_fx() -> dict:
    return {"fx_inr_per_usd": INR_PER_USD}


@app.before_request
def _attach_ledger() -> None:
    seed_if_empty()
    g.ledger = LedgerTG()
    g.ledger.connect()


def _user() -> str | None:
    return session.get("user_name")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if name:
            session["user_name"] = name
            return redirect(url_for("dashboard"))
    if _user():
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


def _require_login():
    if not _user():
        return redirect(url_for("login"))
    return None


@app.route("/")
def dashboard():
    redir = _require_login()
    if redir:
        return redir
    return render_template("dashboard.html", user=_user())


@app.route("/graph")
def graph_live():
    redir = _require_login()
    if redir:
        return redir
    return render_template("graph_live.html", user=_user())


@app.route("/upload")
def page_upload():
    redir = _require_login()
    if redir:
        return redir
    return render_template("data_upload.html", user=_user())


@app.route("/analysis")
def page_analysis():
    redir = _require_login()
    if redir:
        return redir
    return render_template("analysis.html", user=_user())


@app.route("/documents")
def page_documents():
    redir = _require_login()
    if redir:
        return redir
    return render_template("documents.html", user=_user())


@app.route("/reports/rings")
def report_rings():
    redir = _require_login()
    if redir:
        return redir
    return render_template("report_rings.html", user=_user())


@app.route("/reports/high-risk")
def report_high_risk():
    redir = _require_login()
    if redir:
        return redir
    return render_template("report_high_risk.html", user=_user())


@app.route("/reports/suspicious")
def report_suspicious():
    redir = _require_login()
    if redir:
        return redir
    return render_template("report_suspicious.html", user=_user())


@app.route("/reports/salami")
def report_salami():
    redir = _require_login()
    if redir:
        return redir
    return render_template("report_salami.html", user=_user())


@app.route("/employees")
def page_employees():
    redir = _require_login()
    if redir:
        return redir
    return render_template("employees.html", user=_user())


# --- API ---


@app.route("/api/meta")
def api_meta():
    return jsonify(
        {
            "user": _user(),
            "fx_inr_per_usd": INR_PER_USD,
            "tg_demo": g.ledger.is_demo,
            "tg_error": g.ledger.last_error,
        }
    )


@app.route("/api/kpis")
def api_kpis():
    k = g.ledger.kpis()
    return jsonify(k)


@app.route("/api/graph/ring")
def api_graph_ring():
    rid = request.args.get("ring_id")
    return jsonify(g.ledger.fraud_ring_graph(rid))


@app.route("/api/entity/<vtype>/<path:vid>")
def api_entity(vtype: str, vid: str):
    return jsonify(g.ledger.entity_inspector(vtype, vid))


@app.route("/api/reports/rings")
def api_reports_rings():
    return jsonify({"rings": g.ledger.list_rings_detail()})


@app.route("/api/reports/high-risk")
def api_reports_high_risk():
    return jsonify({"users": g.ledger.list_high_risk_users(0.8)})


@app.route("/api/reports/suspicious")
def api_reports_suspicious():
    return jsonify({"alerts": list_internal_alerts(include_dismissed=False)})


@app.route("/api/reports/suspicious/<alert_id>/action", methods=["POST"])
def api_suspicious_action(alert_id: str):
    data = request.get_json(force=True, silent=True) or {}
    action = str(data.get("action", "")).lower()
    if action not in ("confirm", "dismiss"):
        return jsonify({"ok": False, "error": "action must be confirm or dismiss"}), 400
    ok = set_internal_alert_status(alert_id, action, _user() or "analyst")
    return jsonify({"ok": ok})


@app.route("/api/reports/salami")
def api_reports_salami():
    tg = g.ledger.salami_analysis()
    merged = merge_with_tigergraph(tg)
    merged["notifications_preview"] = list_tx_notifications(25)
    return jsonify(merged)


@app.route("/api/reports/salami/upload", methods=["POST"])
def api_salami_upload():
    text = request.form.get("csv") or ""
    f = request.files.get("file")
    if f:
        text = f.read().decode("utf-8", errors="replace")
    result = ingest_csv_text(text)
    return jsonify(result)


@app.route("/api/format-money", methods=["POST"])
def api_format_money():
    data = request.get_json(force=True, silent=True) or {}
    amt = float(data.get("amount_usd", 0))
    cur = data.get("currency", "USD")
    return jsonify({"formatted": format_money_usd_inr(amt, cur)})


@app.route("/api/upload/accounts", methods=["POST"])
def api_upload_accounts():
    text = request.form.get("csv") or ""
    f = request.files.get("file")
    if f:
        text = f.read().decode("utf-8", errors="replace")
    rows = parse_csv_accounts(text)
    n = save_uploaded_accounts(rows)
    return jsonify({"saved": n, "preview": rows[:5]})


@app.route("/api/ai/merge-suggestions", methods=["POST"])
def api_merge_suggestions():
    data = request.get_json(force=True, silent=True) or {}
    manual = bool(data.get("manual", True))
    ids = data.get("account_ids") or []
    auto = bool(data.get("auto_mode", False))
    uploaded = data.get("uploaded_context")
    result = ai_suggest_synthetic_identity_merge(
        account_ids=ids if isinstance(ids, list) else [],
        uploaded_context=uploaded if isinstance(uploaded, list) else None,
        auto_mode=auto and not manual,
    )
    save_merge_run("auto" if auto else "manual", data, result)
    return jsonify(result)


@app.route("/api/documents/verify", methods=["POST"])
def api_documents_verify():
    f = request.files.get("file")
    filename = f.filename if f else "paste.txt"
    ocr = simulate_ocr_text(filename)
    ring_hits = 1 if "deed" in filename.lower() else 0
    verification = ghost_buster_verify(ocr, ring_hits)
    save_document_job(filename, ocr, verification)
    return jsonify({"ocr": ocr, "verification": verification})


@app.route("/api/assistant", methods=["POST"])
def api_assistant():
    data = request.get_json(force=True, silent=True) or {}
    msg = data.get("message", "")
    return jsonify({"reply": assistant_flow(str(msg)), "flow": "assistant-flow"})


@app.route("/api/employees", methods=["GET"])
def api_employees_get():
    return jsonify({"employees": list_employees()})


@app.route("/api/employees", methods=["POST"])
def api_employees_post():
    data = request.get_json(force=True, silent=True) or {}
    if data.get("bulk_text"):
        rows = parse_employee_csv(str(data["bulk_text"]))
        n = bulk_employees(rows)
        return jsonify({"added": n})
    e = add_employee(
        data.get("name", ""),
        data.get("email", ""),
        data.get("role", ""),
        data.get("department", ""),
    )
    return jsonify(e)


@app.route("/api/actions/freeze", methods=["POST"])
def api_freeze():
    data = request.get_json(force=True, silent=True) or {}
    freeze_account(
        str(data.get("account_id", "")),
        str(data.get("reason", "")),
        _user() or "analyst",
    )
    return jsonify({"ok": True})


@app.route("/api/actions/refund", methods=["POST"])
def api_refund():
    data = request.get_json(force=True, silent=True) or {}
    refund_flag(
        str(data.get("account_id", "")),
        str(data.get("reason", "")),
        _user() or "analyst",
    )
    return jsonify({"ok": True})


@app.route("/api/actions/frozen")
def api_frozen_list():
    return jsonify({"items": list_frozen()})


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "1") in ("1", "true", "True")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5050")), debug=debug)
