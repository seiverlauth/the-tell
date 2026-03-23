#!/usr/bin/env python3
"""
fetch_ofac.py — OFAC SDN sanctions designation pipeline for HARPY

Source: https://www.treasury.gov/ofac/downloads/sdn.xml

First run (data/ofac_known_uids.json absent):
  Download SDN list, record all current UIDs as baseline in ofac_known_uids.json,
  write ofac_signals.json with empty signals array. Emits zero signals.

Subsequent runs:
  Diff current UIDs against ofac_known_uids.json. Emit one signal per unique
  country per new entry, filtered to structural_interest_score >= 4. Profiles
  that don't exist are kept (unknown = potentially interesting). Append to
  ofac_signals.json and update ofac_known_uids.json.

signal_date = Publish_Date from XML header (no per-entry date exists in the feed).
"""

import json
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.xml"
MIN_SCORE = 4  # profiles below this are dropped; missing profiles are kept

# ---------------------------------------------------------------------------
# Country name → ISO alpha-2
# Covers the full-name strings that appear in OFAC address/country fields.
# ---------------------------------------------------------------------------

COUNTRY_NAME_TO_ISO2 = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "angola": "AO",
    "armenia": "AM", "australia": "AU", "austria": "AT", "azerbaijan": "AZ",
    "bahrain": "BH", "bangladesh": "BD", "belarus": "BY", "belgium": "BE",
    "belize": "BZ", "benin": "BJ", "bolivia": "BO",
    "bosnia": "BA", "bosnia and herzegovina": "BA",
    "botswana": "BW", "brazil": "BR", "bulgaria": "BG", "burkina faso": "BF",
    "burma": "MM", "burundi": "BI", "cambodia": "KH", "cameroon": "CM",
    "canada": "CA", "central african republic": "CF", "chad": "TD",
    "chile": "CL", "china": "CN", "colombia": "CO", "comoros": "KM",
    "congo": "CG", "democratic republic of congo": "CD",
    "democratic republic of the congo": "CD", "drc": "CD",
    "costa rica": "CR", "croatia": "HR", "cuba": "CU", "cyprus": "CY",
    "czech republic": "CZ", "denmark": "DK", "djibouti": "DJ",
    "dominican republic": "DO", "ecuador": "EC", "egypt": "EG",
    "el salvador": "SV", "equatorial guinea": "GQ", "eritrea": "ER",
    "estonia": "EE", "ethiopia": "ET", "finland": "FI", "france": "FR",
    "gabon": "GA", "gambia": "GM", "georgia": "GE", "germany": "DE",
    "ghana": "GH", "greece": "GR", "guatemala": "GT", "guinea": "GN",
    "guinea-bissau": "GW", "haiti": "HT", "honduras": "HN", "hong kong": "HK",
    "hungary": "HU", "india": "IN", "indonesia": "ID", "iran": "IR",
    "iraq": "IQ", "ireland": "IE", "israel": "IL", "italy": "IT",
    "ivory coast": "CI", "cote d'ivoire": "CI", "jamaica": "JM",
    "japan": "JP", "jordan": "JO", "kazakhstan": "KZ", "kenya": "KE",
    "kosovo": "XK", "kuwait": "KW", "kyrgyzstan": "KG", "laos": "LA",
    "latvia": "LV", "lebanon": "LB", "liberia": "LR", "libya": "LY",
    "lithuania": "LT", "luxembourg": "LU", "madagascar": "MG", "malawi": "MW",
    "malaysia": "MY", "mali": "ML", "mauritania": "MR", "mauritius": "MU",
    "mexico": "MX", "moldova": "MD", "mongolia": "MN", "montenegro": "ME",
    "morocco": "MA", "mozambique": "MZ", "myanmar": "MM", "namibia": "NA",
    "nepal": "NP", "netherlands": "NL", "nicaragua": "NI", "niger": "NE",
    "nigeria": "NG", "north korea": "KP", "north macedonia": "MK",
    "norway": "NO", "oman": "OM", "pakistan": "PK", "panama": "PA",
    "papua new guinea": "PG", "paraguay": "PY", "peru": "PE",
    "philippines": "PH", "poland": "PL", "portugal": "PT", "qatar": "QA",
    "romania": "RO", "russia": "RU", "russian federation": "RU",
    "rwanda": "RW", "saudi arabia": "SA", "senegal": "SN", "serbia": "RS",
    "sierra leone": "SL", "singapore": "SG", "slovakia": "SK",
    "slovenia": "SI", "somalia": "SO", "south africa": "ZA",
    "south korea": "KR", "south sudan": "SS", "spain": "ES",
    "sri lanka": "LK", "sudan": "SD", "sweden": "SE", "switzerland": "CH",
    "syria": "SY", "taiwan": "TW", "tajikistan": "TJ", "tanzania": "TZ",
    "thailand": "TH", "timor-leste": "TL", "east timor": "TL", "togo": "TG",
    "tunisia": "TN", "turkey": "TR", "turkmenistan": "TM", "uganda": "UG",
    "ukraine": "UA", "united arab emirates": "AE", "uae": "AE",
    "united kingdom": "GB", "united states": "US", "usa": "US",
    "uruguay": "UY", "uzbekistan": "UZ", "venezuela": "VE", "vietnam": "VN",
    "viet nam": "VN", "west bank": "PS", "gaza": "PS", "palestine": "PS",
    "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
    # Common OFAC alternates
    "korea, north": "KP", "korea, south": "KR",
    "iran, islamic republic of": "IR",
    "syrian arab republic": "SY",
    "libyan arab jamahiriya": "LY",
    "lao people's democratic republic": "LA",
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "ofac_signals.json"
KNOWN_UIDS_PATH = REPO_ROOT / "data" / "ofac_known_uids.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def country_to_iso2(name: str):
    return COUNTRY_NAME_TO_ISO2.get(name.strip().lower())


def parse_publish_date(raw: str) -> str:
    """'MM/DD/YYYY' → 'YYYY-MM-DD'"""
    try:
        return datetime.strptime(raw.strip(), "%m/%d/%Y").date().isoformat()
    except ValueError:
        return raw.strip()


def load_known_uids():
    """Return set of known UIDs, or None if the file doesn't exist (first run)."""
    if not KNOWN_UIDS_PATH.exists():
        return None
    try:
        return set(json.loads(KNOWN_UIDS_PATH.read_text()))
    except Exception:
        return None


def save_known_uids(uids: set[int]):
    KNOWN_UIDS_PATH.write_text(json.dumps(sorted(uids)))


def load_profile_score(iso2: str):
    p = REPO_ROOT / "data" / "profiles" / f"{iso2}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text()).get("structural_interest_score")
    except Exception:
        return None


def should_include(iso2) -> bool:
    """Keep if: no iso (unknown), no profile (unknown), or score >= MIN_SCORE."""
    if iso2 is None:
        return True
    score = load_profile_score(iso2)
    if score is None:
        return True
    return score >= MIN_SCORE


def parse_xml(data: bytes) -> tuple[str, list[dict]]:
    """
    Parse sdn.xml. Returns (publish_date_iso, list_of_entry_dicts).
    Each entry dict has: uid, lastName, firstName, sdnType, programs, remarks, countries.
    """
    root = ET.fromstring(data)
    ns = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
    p = f"{{{ns}}}" if ns else ""

    pubinfo = root.find(f"{p}publshInformation")
    publish_date = ""
    if pubinfo is not None:
        pd_el = pubinfo.find(f"{p}Publish_Date")
        if pd_el is not None and pd_el.text:
            publish_date = parse_publish_date(pd_el.text)

    entries = []
    for entry in root.findall(f"{p}sdnEntry"):
        def text(tag):
            el = entry.find(f"{p}{tag}")
            return el.text.strip() if el is not None and el.text else None

        uid_raw = text("uid")
        if uid_raw is None:
            continue
        try:
            uid = int(uid_raw)
        except ValueError:
            continue

        last = text("lastName") or ""
        first = text("firstName")
        sdn_type = text("sdnType") or "Entity"
        remarks = text("remarks")

        programs = [
            el.text.strip()
            for el in entry.findall(f".//{p}program")
            if el.text
        ]

        countries = []
        seen_iso = set()
        for addr in entry.findall(f".//{p}address"):
            c_el = addr.find(f"{p}country")
            if c_el is not None and c_el.text:
                name = c_el.text.strip()
                iso2 = country_to_iso2(name)
                key = iso2 if iso2 else name.lower()
                if key not in seen_iso:
                    seen_iso.add(key)
                    countries.append((name, iso2))

        entries.append({
            "uid": uid,
            "lastName": last,
            "firstName": first,
            "sdnType": sdn_type,
            "programs": programs,
            "remarks": remarks,
            "countries": countries,
        })

    return publish_date, entries


def build_title(entry: dict) -> str:
    first = entry.get("firstName")
    last = entry.get("lastName") or ""
    sdn_type = entry.get("sdnType") or "Entity"
    name = f"{first} {last}".strip() if first else last
    return f"{name} ({sdn_type})"


def entry_to_signals(entry: dict, signal_date: str) -> list[dict]:
    title = build_title(entry)
    description = entry.get("remarks")
    programs = entry.get("programs") or []
    uid = entry["uid"]

    countries = entry.get("countries") or []
    if not countries:
        countries = [(None, None)]

    signals = []
    for _country_name, iso2 in countries:
        if not should_include(iso2):
            continue
        signals.append({
            "uid": uid,
            "iso": iso2,
            "source": "ofac",
            "signal_date": signal_date,
            "title": title,
            "value_usd": None,
            "description": description,
            "programs": programs,
            "raw_score": 1.0,
        })

    return signals


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    known_uids = load_known_uids()
    first_run = known_uids is None

    # Download
    try:
        print(f"[ofac] Downloading {SDN_URL}")
        with urllib.request.urlopen(SDN_URL, timeout=120) as resp:
            data = resp.read()
        print(f"[ofac] Downloaded {len(data):,} bytes")
    except Exception as e:
        print(f"[ofac] ERROR: download failed: {e}", file=sys.stderr)
        _write_error(str(e))
        sys.exit(0)

    # Parse
    try:
        publish_date, entries = parse_xml(data)
        print(f"[ofac] Publish_Date: {publish_date}, total entries: {len(entries)}")
    except Exception as e:
        print(f"[ofac] ERROR: XML parse failed: {e}", file=sys.stderr)
        _write_error(str(e))
        sys.exit(0)

    current_uids = {e["uid"] for e in entries}

    if first_run:
        # Establish baseline — emit zero signals
        print(f"[ofac] First run: recording {len(current_uids)} UIDs as baseline")
        save_known_uids(current_uids)
        SIGNALS_PATH.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": ["ofac"],
            "signals": [],
        }, indent=2))
        print(f"[ofac] Baseline set. Future runs will diff against these {len(current_uids)} UIDs.")
        return

    # Subsequent run — diff
    new_uids = current_uids - known_uids
    print(f"[ofac] {len(new_uids)} new UIDs since last run")

    if not new_uids:
        print("[ofac] No new designations — nothing to do")
        # Update known UIDs in case any were removed (list is sometimes corrected)
        save_known_uids(current_uids)
        return

    new_entries = [e for e in entries if e["uid"] in new_uids]

    # Load existing signals
    try:
        existing = json.loads(SIGNALS_PATH.read_text())
    except Exception:
        existing = {"generated_at": None, "sources": ["ofac"], "signals": []}

    new_signals = []
    for entry in new_entries:
        new_signals.extend(entry_to_signals(entry, publish_date))

    filtered = len(new_entries) - len({s["uid"] for s in new_signals})
    print(f"[ofac] {len(new_signals)} signals from {len(new_entries)} new entries "
          f"({filtered} entries filtered by score < {MIN_SCORE})")

    for s in new_signals:
        iso = s.get("iso") or "—"
        print(f"[ofac] + uid={s['uid']}  {iso}  {s['title'][:60]}")

    all_signals = existing.get("signals", []) + new_signals
    all_signals.sort(key=lambda s: s.get("signal_date") or "")

    SIGNALS_PATH.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["ofac"],
        "signals": all_signals,
    }, indent=2))
    print(f"[ofac] Wrote {len(all_signals)} total signals ({len(new_signals)} new) → {SIGNALS_PATH}")

    # Update known UIDs (include current full set to handle OFAC removals)
    save_known_uids(current_uids)


def _write_error(error: str):
    """On failure, preserve existing signals if any and record error state."""
    try:
        existing = json.loads(SIGNALS_PATH.read_text()) if SIGNALS_PATH.exists() else {}
    except Exception:
        existing = {}
    existing.update({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["ofac"],
        "error": error,
    })
    existing.setdefault("signals", [])
    SIGNALS_PATH.write_text(json.dumps(existing, indent=2))


if __name__ == "__main__":
    main()
