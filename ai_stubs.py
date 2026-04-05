"""
Simulated Genkit-style flows: merge suggestions, assistant (assistant-flow), Ghost Buster checks.
Replace with real Genkit / Vertex calls in production.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

KNOWLEDGE_BASE: dict[str, str] = {
    "cross_entity_linking": (
        "Cross-Entity Linking connects users, devices, IP addresses, phone numbers, and documents "
        "in TigerGraph so analysts can traverse shared infrastructure and detect synthetic identities."
    ),
    "suspicious_records": (
        "Suspicious Records monitors privileged employees for anomalous actions: dormant-account "
        "access, back-dated transactions, and bulk exports. Each alert includes an AI narrative."
    ),
    "document_verification": (
        "Document Verification uploads files, runs OCR to extract fields, then Ghost Buster "
        "simulates registry checks and cross-references internal graph links (addresses, devices, rings)."
    ),
    "ledger_shield": (
        "LedgerShield Digital CCTV is a TigerGraph-backed investigative hub for core banking fraud: "
        "live KPIs, fraud-ring force graphs, entity resolution merge suggestions, and salami-slicing detection."
    ),
}


def assistant_flow(message: str) -> str:
    m = message.strip().lower()
    if not m:
        return "Ask me about LedgerShield features, detection methods, or navigation help."
    if "cross" in m and "link" in m:
        return KNOWLEDGE_BASE["cross_entity_linking"]
    if "suspicious" in m and ("record" in m or "employee" in m or "internal" in m):
        return KNOWLEDGE_BASE["suspicious_records"]
    if "document" in m or "ocr" in m or "ghost" in m:
        return KNOWLEDGE_BASE["document_verification"]
    if "summarize" in m or "purpose" in m:
        if "document" in m:
            return KNOWLEDGE_BASE["document_verification"]
        return KNOWLEDGE_BASE["ledger_shield"]
    if "what is" in m or "how" in m:
        if "salami" in m or "micro" in m:
            return (
                "Salami slicing detection aggregates micro-payments into sink accounts. When volume "
                "or count crosses a circuit-breaker threshold, accounts are flagged for freeze or refund review."
            )
        if "ring" in m or "fraud ring" in m:
            return (
                "Fraud rings are coordinated groups sharing devices or infrastructure. TigerGraph stores "
                "rings as connected components linked to parties for real-time graph visualization."
            )
        return KNOWLEDGE_BASE["ledger_shield"]
    return (
        "I am your LedgerShield co-pilot (simulated assistant-flow). Try: "
        "\"What is Cross-Entity Linking?\", \"How do you detect suspicious records?\", "
        "or \"Summarize the Document Verification page.\""
    )


def ai_suggest_synthetic_identity_merge(
    account_ids: list[str] | None,
    uploaded_context: list[dict] | None,
    auto_mode: bool,
) -> dict[str, Any]:
    """Simulates Genkit flow `aiSuggestSyntheticIdentityMerge`."""
    suggestions = []
    base_accounts = list(account_ids or [])
    if uploaded_context:
        for row in uploaded_context[:12]:
            aid = str(row.get("account_id") or row.get("id") or row.get("party_id") or "")
            if aid:
                base_accounts.append(aid)
    seen = set()
    uniq = []
    for a in base_accounts:
        if a not in seen:
            seen.add(a)
            uniq.append(a)
    if len(uniq) < 2 and auto_mode:
        uniq = ["ACC-77821", "ACC-90214", "ACC-44102"]

    pairs = []
    for i, a in enumerate(uniq):
        for b in uniq[i + 1 : i + 3]:
            pairs.append((a, b))

    reasons = [
        "Shared phone number and device fingerprint observed within 72h window.",
        "Identical residential address hash used under different KYC legal names.",
        "IP subnet and user-agent pair matches a known mule onboarding cluster.",
        "Duplicate document OCR signature (perceptual hash) on government ID crop.",
    ]
    for idx, (a, b) in enumerate(pairs[:6]):
        h = hashlib.sha256(f"{a}|{b}".encode()).hexdigest()
        conf = 72 + (int(h[:2], 16) % 23)
        suggestions.append(
            {
                "account_ids": [a, b],
                "reason": reasons[idx % len(reasons)],
                "confidence_pct": min(97, conf),
                "flow": "aiSuggestSyntheticIdentityMerge",
                "auto_mode": auto_mode,
            }
        )

    return {
        "suggestions": suggestions,
        "analyzed_accounts": uniq,
        "generated_at": "simulated",
    }


def simulate_ocr_text(filename: str) -> dict[str, Any]:
    name = (filename or "upload").lower()
    if "deed" in name or "property" in name:
        return {
            "doc_type": "property_deed",
            "address": "221B Baker Street, Mumbai",
            "owner_name": "R. Kumar",
            "registration_id": "REG-MH-88421",
            "raw_snippet": "This is a simulated OCR extract for demonstration.",
        }
    if "pan" in name or "id" in name or "aadhaar" in name:
        return {
            "doc_type": "government_id",
            "id_number": "ABCDE1234F",
            "full_name": "Sample Holder",
            "raw_snippet": "Simulated ID OCR — no real PII.",
        }
    return {
        "doc_type": "generic",
        "notes": "Generic document — heuristic OCR stub.",
        "raw_snippet": f"Extracted placeholder text from {filename}",
    }


def ghost_buster_verify(ocr: dict[str, Any], analyst_device_ring_hits: int) -> dict[str, Any]:
    """Simulated external API + internal cross-reference (Ghost Buster)."""
    checks = [
        {
            "name": "External registry lookup",
            "passed": ocr.get("registration_id") != "REG-FAKE-000",
            "detail": "Simulated Maharashtra property registry confirms registration ID present.",
        },
        {
            "name": "Tax portal consistency",
            "passed": True,
            "detail": "Simulated GSTIN / tax record alignment with declared address.",
        },
        {
            "name": "Internal address reuse",
            "passed": False,
            "detail": "Address hash matches 3 active loan applications under different primary names.",
        },
        {
            "name": "Uploader device vs fraud rings",
            "passed": analyst_device_ring_hits == 0,
            "detail": "Device fingerprint tied to parties in an active fraud ring."
            if analyst_device_ring_hits
            else "No device overlap with known rings in sampled graph.",
        },
    ]
    flags = []
    if not checks[2]["passed"]:
        flags.append(
            {
                "level": "high",
                "text": "Property address reused across multiple loan applications (synthetic collateral risk).",
            }
        )
    if not checks[3]["passed"]:
        flags.append(
            {
                "level": "critical",
                "text": "Document uploaded from a device linked to a prior fraud ring investigation.",
            }
        )
    return {
        "checks": checks,
        "flags": flags,
        "overall_status": "fail" if flags else "pass",
        "engine": "Ghost Buster (simulated)",
    }


def parse_csv_accounts(text: str) -> list[dict]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    header = re.split(r"[;,]", lines[0])
    header = [h.strip().lower().replace(" ", "_") for h in header]
    out = []
    for ln in lines[1:]:
        parts = re.split(r"[;,]", ln)
        row = {}
        for i, h in enumerate(header):
            row[h] = parts[i].strip() if i < len(parts) else ""
        out.append(row)
    return out


def parse_employee_csv(text: str) -> list[dict]:
    return parse_csv_accounts(text)
