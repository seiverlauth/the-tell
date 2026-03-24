#!/usr/bin/env python3
"""
fetch_lda.py — LDA lobbying disclosure pipeline for HARPY

Polls https://lda.gov/api/v1/filings/ for new registrations (filing_type=RR)
posted since yesterday. Filters to high-signal records:
  - client.country != "US", OR
  - foreign_entities is non-empty, OR
  - any lobbying_activity general_issue_code in {DEF, FOR, TRD, ENE, HCR}

Deduplicates by filing_uuid. Appends to data/lda_signals.json.
No API key required — anonymous rate limit (15 req/min) is sufficient.
"""

import json
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LDA_BASE          = "https://lda.gov/api/v1/filings"
LOOKBACK_DAYS     = 1
HIGH_SIGNAL_CODES = {"DEF", "FOR", "TRD", "ENE", "HCR"}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "lda_signals.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def load_profile_score(iso2: str):
    p = REPO_ROOT / "data" / "profiles" / f"{iso2}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text()).get("structural_interest_score")
    except Exception:
        return None


def raw_score_for(iso2: str) -> float:
    if not iso2 or iso2 == "XX":
        return 0.0
    score = load_profile_score(iso2)
    return float(score) if score is not None else 0.0


def pick_iso(filing: dict) -> str:
    """
    ISO priority:
    1. client.country if non-US
    2. first foreign_entity.country
    3. registrant.country if non-US
    4. "XX"
    """
    client_country = ((filing.get("client") or {}).get("country") or "").upper()
    if client_country and client_country != "US":
        return client_country

    foreign_entities = filing.get("foreign_entities") or []
    if foreign_entities:
        fe_country = (foreign_entities[0].get("country") or "").upper()
        if fe_country:
            return fe_country

    reg_country = ((filing.get("registrant") or {}).get("country") or "").upper()
    if reg_country and reg_country != "US":
        return reg_country

    return "XX"


def is_high_signal(filing: dict) -> bool:
    client_country = ((filing.get("client") or {}).get("country") or "US").upper()
    if client_country != "US":
        return True
    if filing.get("foreign_entities"):
        return True
    for act in (filing.get("lobbying_activities") or []):
        if (act.get("general_issue_code") or "").upper() in HIGH_SIGNAL_CODES:
            return True
    return False


def build_description(filing: dict) -> str:
    activities = filing.get("lobbying_activities") or []
    codes = []
    first_desc = ""
    for act in activities:
        code = (act.get("general_issue_code") or "").strip()
        if code:
            codes.append(code)
        if not first_desc:
            first_desc = (act.get("description") or "").strip()

    client = filing.get("client") or {}
    client_country_display = (client.get("country_display") or client.get("country") or "").strip()

    parts = []
    if codes:
        parts.append(", ".join(sorted(set(codes))))
    if client_country_display and client_country_display.upper() not in (
        "UNITED STATES OF AMERICA", "US", "UNITED STATES"
    ):
        parts.append(client_country_display)
    if first_desc:
        parts.append(first_desc[:100] + ("…" if len(first_desc) > 100 else ""))

    return "; ".join(parts)


def to_signal(filing: dict) -> dict:
    registrant_name = ((filing.get("registrant") or {}).get("name") or "").strip()
    client_name     = ((filing.get("client") or {}).get("name") or "").strip()
    title           = f"{registrant_name} — {client_name}" if client_name else registrant_name

    dt_posted   = filing.get("dt_posted") or ""
    signal_date = dt_posted[:10] if dt_posted else ""
    iso         = pick_iso(filing)

    return {
        "filing_uuid": filing.get("filing_uuid"),
        "iso":         iso,
        "source":      "lda",
        "signal_date": signal_date,
        "title":       title,
        "value_usd":   None,
        "description": build_description(filing),
        "url":         filing.get("filing_document_url"),
        "raw_score":   raw_score_for(iso),
        "weight":      1.0,
    }


def fetch_filings(after_date: str) -> list:
    """Fetch all RR filings posted after after_date, paginating through results."""
    params = {
        "filing_type":            "RR",
        "ordering":               "-dt_posted",
        "filing_dt_posted_after": after_date,
    }
    url = LDA_BASE + "/?" + urllib.parse.urlencode(params)
    filings = []

    while url:
        print(f"[lda] GET {url}")
        data = api_get(url)
        filings.extend(data.get("results") or [])
        url = data.get("next")  # None when exhausted

    return filings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today     = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=LOOKBACK_DAYS)
    after_str = yesterday.strftime("%Y-%m-%d")

    print(f"[lda] Fetching RR filings posted after {after_str}")

    try:
        filings = fetch_filings(after_str)
    except Exception as e:
        print(f"[lda] ERROR: {e}", file=sys.stderr)
        _write_error(str(e))
        sys.exit(0)

    print(f"[lda] {len(filings)} raw filing(s) returned")

    if SIGNALS_PATH.exists():
        try:
            existing = json.loads(SIGNALS_PATH.read_text())
        except Exception:
            existing = {"generated_at": None, "sources": ["lda"], "signals": []}
    else:
        existing = {"generated_at": None, "sources": ["lda"], "signals": []}

    known_uuids = {
        s.get("filing_uuid")
        for s in existing.get("signals", [])
        if s.get("filing_uuid")
    }

    new_signals = []
    for filing in filings:
        uuid = filing.get("filing_uuid")
        if uuid in known_uuids:
            continue
        if not is_high_signal(filing):
            continue
        sig = to_signal(filing)
        new_signals.append(sig)
        known_uuids.add(uuid)
        print(f"[lda] + {sig['iso']}  {sig['title'][:60]}")

    print(f"[lda] {len(new_signals)} new signal(s) after filter")

    all_signals = existing.get("signals", []) + new_signals
    all_signals.sort(key=lambda s: s.get("signal_date") or "")

    SIGNALS_PATH.write_text(json.dumps({
        "generated_at": today.isoformat(),
        "sources":      ["lda"],
        "signals":      all_signals,
    }, indent=2))
    print(f"[lda] Wrote {len(all_signals)} total signals ({len(new_signals)} new) → {SIGNALS_PATH}")


def _write_error(error: str):
    try:
        existing = json.loads(SIGNALS_PATH.read_text()) if SIGNALS_PATH.exists() else {}
    except Exception:
        existing = {}
    existing.update({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources":      ["lda"],
        "error":        error,
    })
    existing.setdefault("signals", [])
    SIGNALS_PATH.write_text(json.dumps(existing, indent=2))


if __name__ == "__main__":
    main()
