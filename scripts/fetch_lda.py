#!/usr/bin/env python3
"""
fetch_lda.py — LDA lobbying disclosure pipeline for HARPY

Polls https://lda.gov/api/v1/filings/ for new registrations (filing_type=RR)
posted since yesterday. Filters to high-signal records:
  - foreign_entities is non-empty, OR
  - any lobbying_activity general_issue_code in {DEF, FOR, TRD, ENE, SCI, HOM}, OR
  - client country profile_score >= 6 (high-interest country — any lobbying matters)

Deduplicates by filing_uuid. Appends to data/lda_signals.json.
No API key required — anonymous rate limit (15 req/min) is sufficient.
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import profile_score, load_existing, append_and_write, write_error

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LDA_BASE               = "https://lda.gov/api/v1/filings"
LOOKBACK_DAYS          = 1
LOOKBACK_DAYS_BACKFILL = 365
# Note: LDA API uses "ENG" for energy, not "ENE". Both included defensively.
HIGH_SIGNAL_CODES      = {"DEF", "FOR", "TRD", "ENE", "ENG", "SCI", "HOM"}

# LDA general issue area codes → human-readable labels.
# Full list: https://lda.congress.gov/api/v1/constants/filing/issue_area_codes/
ISSUE_CODE_LABELS: dict[str, str] = {
    "ACC": "Accounting",       "ADV": "Advertising",     "AER": "Aerospace",
    "AGR": "Agriculture",      "ALC": "Alcohol/Drugs",   "ANI": "Animals",
    "APP": "Appropriations",   "AUT": "Automotive",      "AVI": "Aviation",
    "BAN": "Banking",          "BEV": "Beverage",        "BUD": "Budget",
    "CAW": "Clean Air/Water",  "CDT": "Debt/Deficit",    "CHM": "Chemicals",
    "CIV": "Civil Rights",     "COM": "Communications",  "CON": "Constitution",
    "CPT": "Technology",       "CPI": "Consumer Issues", "CSP": "Consumer Safety",
    "DEF": "Defense",          "DIS": "Disaster/Emergency", "ECN": "Economics",
    "EDU": "Education",        "ENG": "Energy",          "ENE": "Energy",
    "ENV": "Environment",      "FAM": "Family",          "FIN": "Finance",
    "FIR": "Firearms",         "FOO": "Food",            "FOR": "Foreign Affairs",
    "FUE": "Fuel/Oil",         "GAM": "Gaming",          "GOV": "Government",
    "HCR": "Health",           "HOM": "Homeland Security", "HOU": "Housing",
    "IMM": "Immigration",      "INS": "Insurance",       "INT": "Intelligence",
    "LAW": "Law Enforcement",  "LBR": "Labor",           "MAR": "Maritime",
    "MED": "Media",            "MIA": "Medicare/Medicaid", "MIA": "Medicare",
    "MAN": "Manufacturing",    "MMM": "Manufacturing",
    "NAT": "Natural Resources", "PHA": "Pharma",
    "POS": "Postal",           "RES": "R&D",             "ROD": "Roads/Highway",
    "RRR": "Railroads",        "SCI": "Science/Technology", "SMB": "Small Business",
    "SPO": "Sports",           "TAR": "Tariffs",         "TAX": "Taxation",
    "TEC": "Telecom",          "TOB": "Tobacco",         "TOU": "Tourism",
    "TRA": "Transportation",   "TRD": "Trade",           "TRU": "Trucking",
    "URB": "Urban Development", "UTI": "Utilities",      "VET": "Veterans",
    "WAS": "Waste",            "WEL": "Welfare",
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "lda_signals.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_get(url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                wait = 15 * (attempt + 1)
                print(f"[lda] 429 rate limit — sleeping {wait}s")
                time.sleep(wait)
                continue
            raise


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
    if filing.get("foreign_entities"):
        return True
    for act in (filing.get("lobbying_activities") or []):
        if (act.get("general_issue_code") or "").upper() in HIGH_SIGNAL_CODES:
            return True
    iso = pick_iso(filing)
    if iso and iso != "XX" and (profile_score(iso) or 0) >= 6:
        return True
    return False


def _expand_codes(codes: list[str]) -> str:
    """Convert raw LDA issue codes to human-readable labels."""
    labels = [ISSUE_CODE_LABELS.get(c, c) for c in codes]
    # Deduplicate while preserving order (ENG and ENE both map to "Energy")
    seen: set[str] = set()
    unique = [l for l in labels if not (l in seen or seen.add(l))]
    return ", ".join(unique)


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
        parts.append(_expand_codes(sorted(set(codes))))
    if client_country_display and client_country_display.upper() not in (
        "UNITED STATES OF AMERICA", "US", "UNITED STATES"
    ):
        parts.append(client_country_display)
    if first_desc:
        parts.append(first_desc[:100] + ("…" if len(first_desc) > 100 else ""))

    return "; ".join(parts)


def to_signal(filing: dict) -> dict:
    registrant_name = ((filing.get("registrant") or {}).get("name") or "").strip().title()
    client_name     = ((filing.get("client") or {}).get("name") or "").strip().title()
    # Surface the top issue domains in the title so the reader knows why this
    # entry is in the feed without having to read the description.
    activities  = filing.get("lobbying_activities") or []
    raw_codes   = sorted(set(
        (act.get("general_issue_code") or "").upper()
        for act in activities
        if act.get("general_issue_code")
    ))
    # Limit to 3 most-signal-relevant codes: HIGH_SIGNAL first, then others
    priority = [c for c in raw_codes if c in HIGH_SIGNAL_CODES]
    others   = [c for c in raw_codes if c not in HIGH_SIGNAL_CODES]
    top_codes = (priority + others)[:3]
    domain_str = _expand_codes(top_codes) if top_codes else ""
    if client_name and domain_str:
        title = f"{client_name} ({domain_str}) — via {registrant_name}"
    elif client_name:
        title = f"{client_name} — via {registrant_name}"
    else:
        title = registrant_name

    dt_posted   = filing.get("dt_posted") or ""
    signal_date = dt_posted[:10] if dt_posted else ""
    iso         = pick_iso(filing)

    issue_codes = sorted(set(
        (act.get("general_issue_code") or "").upper()
        for act in (filing.get("lobbying_activities") or [])
        if act.get("general_issue_code")
    ))

    return {
        "filing_uuid":   filing.get("filing_uuid"),
        "iso":           iso,
        "source":        "lda",
        "signal_date":   signal_date,
        "title":         title,
        "value_usd":     None,
        "description":   build_description(filing),
        "lobbying_firm": registrant_name or None,
        "issue_codes":   issue_codes,
        "url":           filing.get("filing_document_url"),
        "raw_score":     profile_score(iso),
        "weight":        1.0,
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
        if url:
            time.sleep(4.5)  # 15 req/min = 1 req per 4s

    return filings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true",
                        help=f"Fetch {LOOKBACK_DAYS_BACKFILL} days of history")
    args = parser.parse_args()

    today     = datetime.now(timezone.utc)
    days      = LOOKBACK_DAYS_BACKFILL if args.backfill else LOOKBACK_DAYS
    after_str = (today - timedelta(days=days)).strftime("%Y-%m-%d")

    print(f"[lda] Fetching RR filings posted after {after_str} ({'backfill' if args.backfill else 'daily'})")

    try:
        filings = fetch_filings(after_str)
    except Exception as e:
        print(f"[lda] ERROR: {e}", file=sys.stderr)
        write_error(SIGNALS_PATH, "lda", str(e))
        sys.exit(0)

    print(f"[lda] {len(filings)} raw filing(s) returned")

    existing = load_existing(SIGNALS_PATH, "lda")
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

    added = append_and_write(SIGNALS_PATH, "lda", new_signals, lambda s: s.get("filing_uuid"))
    print(f"[lda] {added} new signal(s) written → {SIGNALS_PATH}")


if __name__ == "__main__":
    main()
