#!/usr/bin/env python3
"""
fetch_sam.py — SAM.gov procurement signal pipeline for HARPY

Single API call, last 45 days, limit=10.
Filters to defense/state/USAID agencies.
Writes data/sam_signals.json.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import urllib.request
import urllib.parse

sys.path.insert(0, str(Path(__file__).parent))
from utils import COUNTRY_NAME_TO_ISO2, ALPHA3_TO_ALPHA2, profile_score, append_and_write, write_error

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SAM_API_BASE = "https://api.sam.gov/opportunities/v2/search"
LOOKBACK_DAYS = 45
BATCH_SIZE = 10

AGENCY_KEYWORDS = [
    "DEFENSE, DEPARTMENT OF",
    "ARMY, DEPARTMENT OF",
    "NAVY, DEPARTMENT OF",
    "AIR FORCE, DEPARTMENT OF",
    "STATE, DEPARTMENT OF",
    "AGENCY FOR INTERNATIONAL DEVELOPMENT",
]

_COUNTRY_NAMES_SORTED = sorted(COUNTRY_NAME_TO_ISO2.keys(), key=len, reverse=True)
_COUNTRY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in _COUNTRY_NAMES_SORTED) + r")\b",
    re.IGNORECASE,
)

# Adjective forms not in the country name map — expand title/description matching.
ADJECTIVE_TO_ISO = {
    "afghan": "AF", "iraqi": "IQ", "syrian": "SY", "iranian": "IR",
    "pakistani": "PK", "saudi": "SA", "yemeni": "YE", "ukrainian": "UA",
    "taiwanese": "TW", "israeli": "IL", "palestinian": "PS", "lebanese": "LB",
    "libyan": "LY", "sudanese": "SD", "somali": "SO", "malian": "ML",
    "congolese": "CD", "venezuelan": "VE", "cuban": "CU", "burmese": "MM",
    "haitian": "HT", "belarusian": "BY", "georgian": "GE", "azerbaijani": "AZ",
    "kosovar": "XK", "kenyan": "KE", "ethiopian": "ET", "ugandan": "UG",
    "rwandan": "RW", "nigerian": "NG", "ghanaian": "GH", "senegalese": "SN",
    "moroccan": "MA", "tunisian": "TN", "algerian": "DZ", "egyptian": "EG",
    "jordanian": "JO", "kuwaiti": "KW", "bahraini": "BH", "emirati": "AE",
    "qatari": "QA", "omani": "OM", "indonesian": "ID", "filipino": "PH",
    "vietnamese": "VN", "thai": "TH", "cambodian": "KH", "bangladeshi": "BD",
    "nepali": "NP", "colombian": "CO", "peruvian": "PE", "bolivian": "BO",
    "ecuadorian": "EC", "guatemalan": "GT", "honduran": "HN",
    "salvadoran": "SV", "nicaraguan": "NI", "rwandese": "RW",
}
_ADJECTIVE_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in ADJECTIVE_TO_ISO) + r")\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_api_key():
    key = os.environ.get("SAM_API_KEY", "")
    if not key:
        env_path = Path(__file__).parent.parent / ".env"
        try:
            with open(env_path) as f:
                for line in f:
                    if line.startswith("SAM_API_KEY="):
                        key = line.strip().split("=", 1)[1]
                        break
        except FileNotFoundError:
            pass
    return key


MAINTENANCE_PATTERNS = re.compile(
    r"\b(corrugated|roofing|flooring|painting|janitorial|landscaping|"
    r"elevator|hvac|plumbing|electrical repair|window|carpet|custodial)\b",
    re.IGNORECASE,
)


def is_agency_match(record):
    path = (record.get("fullParentPathName") or "").upper()
    return any(kw in path for kw in AGENCY_KEYWORDS)


def is_maintenance(record):
    title = record.get("title") or ""
    return bool(MAINTENANCE_PATTERNS.search(title))


def extract_country(record):
    pop = record.get("placeOfPerformance") or {}
    country_obj = pop.get("country") or {}
    code = (country_obj.get("code") or "").strip().upper()

    if code:
        if len(code) == 3:
            iso2 = ALPHA3_TO_ALPHA2.get(code)
            if iso2:
                return iso2
        elif len(code) == 2:
            return code

    # Scan title + description for country names, then adjective forms
    text = " ".join(filter(None, [record.get("title"), record.get("description")]))
    m = _COUNTRY_PATTERN.search(text)
    if m:
        return COUNTRY_NAME_TO_ISO2.get(m.group(1).lower())

    m = _ADJECTIVE_PATTERN.search(text)
    if m:
        return ADJECTIVE_TO_ISO.get(m.group(1).lower())

    return None


def build_description(record):
    award = record.get("award") or {}
    parts = []
    notice_type = (record.get("type") or record.get("baseType") or "").strip()
    psc = (record.get("classificationCode") or "").strip()
    awardee = ((award.get("awardee") or {}).get("name") or "").strip()
    if notice_type:
        parts.append(notice_type)
    if psc:
        parts.append(psc)
    if awardee:
        parts.append(awardee)
    return " · ".join(parts) if parts else None


def to_signal(record):
    award = record.get("award") or {}
    value = award.get("amount")
    try:
        value = float(value) if value is not None else None
    except (TypeError, ValueError):
        value = None

    iso = extract_country(record)
    return {
        "iso": iso,
        "source": "sam",
        "signal_date": record.get("postedDate"),
        "title": record.get("title"),
        "value_usd": value,
        "description": build_description(record),
        "page_url": record.get("uiLink"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    out_path = Path(__file__).parent.parent / "data" / "sam_signals.json"

    key = load_api_key()
    if not key:
        print("ERROR: SAM_API_KEY not set", file=sys.stderr)
        write_error(out_path, "sam", "missing SAM_API_KEY")
        sys.exit(0)

    today = datetime.now(timezone.utc)
    from_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    to_date = today.strftime("%m/%d/%Y")

    params = {
        "api_key": key,
        "limit": BATCH_SIZE,
        "offset": 0,
        "postedFrom": from_date,
        "postedTo": to_date,
    }
    url = SAM_API_BASE + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"ERROR: API call failed: {e}", file=sys.stderr)
        write_error(out_path, "sam", str(e))
        sys.exit(0)

    batch = data.get("opportunitiesData") or []
    new_signals = [to_signal(r) for r in batch if is_agency_match(r) and not is_maintenance(r)]

    def dedup_key(sig):
        url = sig.get("page_url") or ""
        return url or f"{sig.get('iso')}|{sig.get('signal_date')}|{sig.get('title')}"

    added = append_and_write(out_path, "sam", new_signals, dedup_key)
    print(f"Fetched {len(new_signals)} candidates, added {added} new signals → {out_path}")


if __name__ == "__main__":
    main()
