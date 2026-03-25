#!/usr/bin/env python3
"""
fetch_federalregister.py — Federal Register signal pipeline for HARPY

Fetches recent documents from agencies that generate apparatus actions:
State Dept, DoD, BIS (export controls), OFAC (sanctions), Treasury, USAID.

No API key required.
Writes data/federalregister_signals.json.
"""

import http.client
import json
import re
import ssl
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FR_HOST = "www.federalregister.gov"
FR_PATH = "/api/v1/documents.json"
LOOKBACK_DAYS = 30
PER_PAGE = 100

# Correct agency slugs from /api/v1/agencies.json
AGENCIES = [
    "state-department",
    "defense-department",
    "industry-and-security-bureau",
    "foreign-assets-control-office",
    "treasury-department",
    "agency-for-international-development",
    "army-department",
    "navy-department",
    "air-force-department",
]

FIELDS = [
    "title",
    "abstract",
    "document_number",
    "html_url",
    "publication_date",
    "type",
    "action",
]

INCLUDE_TYPES = {"Rule", "Proposed Rule", "Notice", "Presidential Document"}

NOISE_PATTERNS = re.compile(
    r"\b(meeting|sunshine act|comment period|vacancy|nomination|"
    r"privacy act|records management|information collection|"
    r"pay scale|federal holiday|office hours|"
    r"viticultural area|winegrowing|alcohol|tobacco|"
    r"flood insurance|flood plain|flood map|"
    r"small business|disadvantaged business|"
    r"environmental impact|environmental assessment)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Country name → ISO alpha-2
# ---------------------------------------------------------------------------

COUNTRY_NAME_TO_ISO2 = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "angola": "AO",
    "armenia": "AM", "australia": "AU", "austria": "AT", "azerbaijan": "AZ",
    "bahrain": "BH", "bangladesh": "BD", "belarus": "BY", "belgium": "BE",
    "belize": "BZ", "benin": "BJ", "bosnia": "BA",
    "bosnia and herzegovina": "BA", "botswana": "BW", "brazil": "BR",
    "bulgaria": "BG", "burkina faso": "BF", "burma": "MM", "myanmar": "MM",
    "burundi": "BI", "cambodia": "KH", "cameroon": "CM",
    "central african republic": "CF", "chad": "TD", "colombia": "CO",
    "comoros": "KM", "congo": "CG", "democratic republic of congo": "CD",
    "democratic republic of the congo": "CD", "drc": "CD",
    "costa rica": "CR", "croatia": "HR", "cuba": "CU", "cyprus": "CY",
    "czech republic": "CZ", "denmark": "DK", "djibouti": "DJ",
    "dominican republic": "DO", "ecuador": "EC", "egypt": "EG",
    "el salvador": "SV", "eritrea": "ER", "estonia": "EE", "ethiopia": "ET",
    "finland": "FI", "france": "FR", "gabon": "GA", "gambia": "GM",
    "georgia": "GE", "germany": "DE", "ghana": "GH", "greece": "GR",
    "guatemala": "GT", "guinea": "GN", "guinea-bissau": "GW", "haiti": "HT",
    "honduras": "HN", "hungary": "HU", "india": "IN", "indonesia": "ID",
    "iran": "IR", "iraq": "IQ", "ireland": "IE", "israel": "IL",
    "italy": "IT", "ivory coast": "CI", "cote d'ivoire": "CI",
    "jamaica": "JM", "japan": "JP", "jordan": "JO", "kazakhstan": "KZ",
    "kenya": "KE", "kosovo": "XK", "kuwait": "KW", "kyrgyzstan": "KG",
    "laos": "LA", "latvia": "LV", "lebanon": "LB", "liberia": "LR",
    "libya": "LY", "lithuania": "LT", "madagascar": "MG", "malawi": "MW",
    "malaysia": "MY", "mali": "ML", "mauritania": "MR", "mexico": "MX",
    "moldova": "MD", "mongolia": "MN", "montenegro": "ME", "morocco": "MA",
    "mozambique": "MZ", "namibia": "NA", "nepal": "NP",
    "netherlands": "NL", "nicaragua": "NI", "niger": "NE", "nigeria": "NG",
    "north korea": "KP", "north macedonia": "MK", "norway": "NO",
    "oman": "OM", "pakistan": "PK", "panama": "PA",
    "papua new guinea": "PG", "paraguay": "PY", "peru": "PE",
    "philippines": "PH", "poland": "PL", "portugal": "PT", "qatar": "QA",
    "romania": "RO", "russia": "RU", "russian federation": "RU",
    "rwanda": "RW", "saudi arabia": "SA", "senegal": "SN", "serbia": "RS",
    "sierra leone": "SL", "slovakia": "SK", "slovenia": "SI",
    "somalia": "SO", "south africa": "ZA", "south korea": "KR",
    "south sudan": "SS", "spain": "ES", "sri lanka": "LK", "sudan": "SD",
    "sweden": "SE", "switzerland": "CH", "syria": "SY", "taiwan": "TW",
    "tajikistan": "TJ", "tanzania": "TZ", "thailand": "TH",
    "timor-leste": "TL", "east timor": "TL", "togo": "TG", "tunisia": "TN",
    "turkey": "TR", "turkmenistan": "TM", "uganda": "UG", "ukraine": "UA",
    "united arab emirates": "AE", "uae": "AE", "united kingdom": "GB",
    "uruguay": "UY", "uzbekistan": "UZ", "venezuela": "VE", "vietnam": "VN",
    "viet nam": "VN", "west bank": "PS", "gaza": "PS", "palestine": "PS",
    "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
}

_COUNTRY_NAMES_SORTED = sorted(COUNTRY_NAME_TO_ISO2.keys(), key=len, reverse=True)
_COUNTRY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in _COUNTRY_NAMES_SORTED) + r")\b",
    re.IGNORECASE,
)


def extract_country(title, abstract):
    text = f"{title or ''} {abstract or ''}"
    m = _COUNTRY_PATTERN.search(text)
    if m:
        return COUNTRY_NAME_TO_ISO2.get(m.group(1).lower())
    return None


# ---------------------------------------------------------------------------
# Fetching — use http.client directly; urllib re-encodes %5B%5D in query strings
# ---------------------------------------------------------------------------

def _build_path(from_date_str, page):
    # Build query string with percent-encoded brackets for nested params
    parts = [
        f"conditions%5Bpublication_date%5D%5Bgte%5D={from_date_str}",
        f"per_page={PER_PAGE}",
        f"page={page}",
        "order=newest",
    ]
    for agency in AGENCIES:
        parts.append(f"conditions%5Bagencies%5D%5B%5D={agency}")
    for field in FIELDS:
        parts.append(f"fields%5B%5D={field}")
    return FR_PATH + "?" + "&".join(parts)


def _get(path):
    ctx = ssl.create_default_context()
    conn = http.client.HTTPSConnection(FR_HOST, context=ctx, timeout=30)
    conn.request("GET", path, headers={"User-Agent": "harpy/1.0"})
    resp = conn.getresponse()
    if resp.status != 200:
        body = resp.read(512).decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {resp.status}: {body}")
    return json.loads(resp.read())


def fetch_all(from_date_str):
    results = []
    page = 1
    while True:
        data = _get(_build_path(from_date_str, page))
        batch = data.get("results") or []
        results.extend(batch)
        total = data.get("count", 0)
        if not batch or len(results) >= total or len(results) >= 500:
            break
        page += 1
    return results


# ---------------------------------------------------------------------------
# Signal conversion
# ---------------------------------------------------------------------------

def is_noise(doc):
    text = f"{doc.get('title') or ''} {doc.get('abstract') or ''}"
    return bool(NOISE_PATTERNS.search(text))


def to_signal(doc):
    title = (doc.get("title") or "").strip()
    abstract = (doc.get("abstract") or "").strip()
    doc_type = (doc.get("type") or "").strip()
    doc_number = (doc.get("document_number") or "").strip()

    desc_parts = []
    if doc_type:
        desc_parts.append(doc_type)
    if doc_number:
        desc_parts.append(doc_number)
    if abstract:
        desc_parts.append(abstract[:300] + ("…" if len(abstract) > 300 else ""))
    description = " · ".join(desc_parts) if desc_parts else None

    return {
        "iso": extract_country(title, abstract),
        "source": "federalregister",
        "signal_date": doc.get("publication_date"),
        "title": title,
        "value_usd": None,
        "description": description,
        "raw_score": 1.0,
        "page_url": doc.get("html_url"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    out_path = Path(__file__).parent.parent / "data" / "federalregister_signals.json"
    today = datetime.now(timezone.utc)
    from_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    print(f"Fetching Federal Register documents since {from_date}…")

    try:
        docs = fetch_all(from_date)
    except Exception as e:
        print(f"ERROR: fetch failed: {e}", file=sys.stderr)
        out_path.write_text(json.dumps({
            "generated_at": today.isoformat(),
            "sources": ["federalregister"],
            "error": str(e),
            "signals": [],
        }, indent=2))
        sys.exit(0)

    print(f"  Retrieved {len(docs)} raw documents")

    signals = []
    for doc in docs:
        if (doc.get("type") or "").strip() not in INCLUDE_TYPES:
            continue
        if is_noise(doc):
            continue
        signals.append(to_signal(doc))

    output = {
        "generated_at": today.isoformat(),
        "sources": ["federalregister"],
        "signals": signals,
    }
    out_path.write_text(json.dumps(output, indent=2))
    print(f"Wrote {len(signals)} signals to {out_path}")


if __name__ == "__main__":
    main()
