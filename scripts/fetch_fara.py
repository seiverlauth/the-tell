#!/usr/bin/env python3
"""
fetch_fara.py — FARA new registrant pipeline for HARPY

Flow:
  1. GET /api/v1/Registrants/json/New?from=...&to=... → list of new registration numbers
  2. For each new reg number, GET /api/v1/ForeignPrincipals/json/Active/{regNumber}
     → registrant name, foreign principal name, country
  3. Download registration PDF; extract Item 5h (business description),
     Item 9a/10a (compensation), Item 16 (target groups).
  4. Emit one signal per registration. Append to data/fara_signals.json.
     Deduplicate by registration_number across runs.

Rate limit: 5 requests / 10 seconds. Delay between enrichment calls: 2s.
"""

import io
import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import curl_cffi.requests as cffi_requests
    _CFFI_SESSION = cffi_requests.Session()
    def _http_get_bytes(url: str, timeout: int = 30) -> bytes:
        r = _CFFI_SESSION.get(url, timeout=timeout, impersonate="chrome120")
        r.raise_for_status()
        return r.content
except ImportError:
    import urllib.request as _urllib_req
    def _http_get_bytes(url: str, timeout: int = 30) -> bytes:
        with _urllib_req.urlopen(url, timeout=timeout) as r:
            return r.read()

try:
    from pypdf import PdfReader
    _PYPDF_OK = True
except ImportError:
    _PYPDF_OK = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FARA_BASE        = "https://efile.fara.gov/api/v1"
LOOKBACK_DAYS    = 1
ENRICH_DELAY     = 2.0  # seconds between ForeignPrincipals calls

# ---------------------------------------------------------------------------
# Country name → ISO alpha-2
# ---------------------------------------------------------------------------

COUNTRY_NAME_TO_ISO2 = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "angola": "AO",
    "armenia": "AM", "australia": "AU", "austria": "AT", "azerbaijan": "AZ",
    "bahrain": "BH", "bangladesh": "BD", "belarus": "BY", "belgium": "BE",
    "belize": "BZ", "benin": "BJ", "bolivia": "BO",
    "bosnia and herzegovina": "BA", "bosnia": "BA",
    "botswana": "BW", "brazil": "BR", "bulgaria": "BG", "burkina faso": "BF",
    "burma": "MM", "burundi": "BI", "cambodia": "KH", "cameroon": "CM",
    "canada": "CA", "central african republic": "CF", "chad": "TD",
    "chile": "CL", "china": "CN", "colombia": "CO", "comoros": "KM",
    "congo": "CG", "democratic republic of the congo": "CD", "drc": "CD",
    "costa rica": "CR", "croatia": "HR", "cuba": "CU", "cyprus": "CY",
    "czech republic": "CZ", "denmark": "DK", "djibouti": "DJ",
    "dominican republic": "DO", "ecuador": "EC", "egypt": "EG",
    "el salvador": "SV", "equatorial guinea": "GQ", "eritrea": "ER",
    "estonia": "EE", "ethiopia": "ET", "finland": "FI", "france": "FR",
    "gabon": "GA", "gambia": "GM", "georgia": "GE", "germany": "DE",
    "ghana": "GH", "greece": "GR", "guatemala": "GT", "guinea": "GN",
    "guinea-bissau": "GW", "haiti": "HT", "honduras": "HN", "hungary": "HU",
    "india": "IN", "indonesia": "ID", "iran": "IR", "iraq": "IQ",
    "ireland": "IE", "israel": "IL", "italy": "IT", "ivory coast": "CI",
    "cote d'ivoire": "CI", "jamaica": "JM", "japan": "JP", "jordan": "JO",
    "kazakhstan": "KZ", "kenya": "KE", "kosovo": "XK", "kuwait": "KW",
    "kyrgyzstan": "KG", "laos": "LA", "latvia": "LV", "lebanon": "LB",
    "liberia": "LR", "libya": "LY", "lithuania": "LT", "madagascar": "MG",
    "malawi": "MW", "malaysia": "MY", "mali": "ML", "mauritania": "MR",
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
    "thailand": "TH", "timor-leste": "TL", "togo": "TG", "tunisia": "TN",
    "turkey": "TR", "turkmenistan": "TM", "uganda": "UG", "ukraine": "UA",
    "united arab emirates": "AE", "uae": "AE", "united kingdom": "GB",
    "united states": "US", "usa": "US", "uruguay": "UY", "uzbekistan": "UZ",
    "venezuela": "VE", "vietnam": "VN", "viet nam": "VN",
    "west bank": "PS", "gaza": "PS", "palestine": "PS",
    "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "fara_signals.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_get(url: str) -> dict:
    """Fetch a FARA API URL, follow redirects, return parsed JSON."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def unwrap_rowset(data: dict) -> list:
    """
    FARA API wraps all responses in {"ROWSET": {"ROW": ...}}.
    ROW is a dict for a single result, a list for multiple.
    Returns a plain list (possibly empty).
    """
    rowset = data.get("ROWSET") or {}
    row = rowset.get("ROW")
    if row is None:
        return []
    return row if isinstance(row, list) else [row]


def parse_date(raw: str) -> str:
    """'2026-03-23T00:00:00' → '2026-03-23'"""
    return raw[:10] if raw else ""


def country_to_iso2(name: str) -> str:
    if not name:
        return "XX"
    return COUNTRY_NAME_TO_ISO2.get(name.strip().lower(), "XX")


def load_profile_score(iso2: str):
    p = REPO_ROOT / "data" / "profiles" / f"{iso2}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text()).get("structural_interest_score")
    except Exception:
        return None


def raw_score_for(iso2: str) -> float:
    if iso2 == "XX":
        return 0.0
    score = load_profile_score(iso2)
    return float(score) if score is not None else 0.0


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def _pdf_text(url: str) -> str:
    """Download a FARA PDF and return all page text concatenated."""
    if not _PYPDF_OK:
        return ""
    try:
        raw = _http_get_bytes(url, timeout=45)
        reader = PdfReader(io.BytesIO(raw))
        pages = []
        for page in reader.pages:
            t = page.extract_text() or ""
            pages.append(t)
        return "\n".join(pages)
    except Exception as e:
        print(f"[fara] WARNING: PDF fetch/parse failed ({url}): {e}")
        return ""


def _extract_item(text: str, header_pattern: str, stop_pattern: str, maxlen: int = 600) -> str:
    """
    Extract text after a form item header up to the next item header.
    Returns stripped string or "".
    """
    m = re.search(header_pattern, text, re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    body = text[m.end():]
    stop = re.search(stop_pattern, body, re.IGNORECASE)
    if stop:
        body = body[:stop.start()]
    body = re.sub(r"\s+", " ", body).strip()
    if len(body) > maxlen:
        body = body[:maxlen].rsplit(" ", 1)[0] + "\u2026"
    return body


def enrich_from_pdf(pdf_url: str) -> dict:
    """
    Download and parse a FARA Form NSD-1 PDF.
    Returns dict with keys: description, value_usd, target_groups (list[str]).
    All fields may be None/empty on failure.
    """
    result = {"description": None, "value_usd": None, "target_groups": []}
    if not _PYPDF_OK:
        return result

    text = _pdf_text(pdf_url)
    if not text:
        return result

    # ── Item 5h — nature of registrant's business ───────────────────────────
    # PDF header: "(h) Describe the nature of the registrant's regular business..."
    desc = _extract_item(
        text,
        header_pattern=r"\(h\)\s+Describe[^.\n]+\.\s*",
        stop_pattern=r"\(i\)|\b6\s*[\.\(]|\bitem\s+6\b",
        maxlen=600,
    )
    # Fallback: "5. (h)" style (older form versions)
    if not desc:
        desc = _extract_item(
            text,
            header_pattern=r"5\s*[\.\)]\s*\(?h\)?[\s:]+",
            stop_pattern=r"\(i\)|\b6\s*[\.\(]",
            maxlen=600,
        )
    result["description"] = desc or None

    # ── Items 9a / 10a — compensation received ──────────────────────────────
    # Look for dollar amounts near "9" or "10" (receipts/disbursements)
    # Pattern: $X,XXX,XXX or $X million
    money_section = ""
    m9 = re.search(r"9\s*[\.\(]\s*\(?a\)?[^$]{0,200}", text, re.IGNORECASE | re.DOTALL)
    if m9:
        money_section = text[m9.start():m9.start() + 500]

    value = None
    # Try spelled-out amounts first: $1,500,000 or $1.5 million/billion
    amount_m = re.search(
        r"\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion)?",
        money_section,
        re.IGNORECASE,
    )
    if amount_m:
        raw_num = float(amount_m.group(1).replace(",", ""))
        multiplier = amount_m.group(2)
        if multiplier:
            raw_num *= 1e9 if multiplier.lower() == "billion" else 1e6
        if raw_num > 0:
            value = raw_num
    result["value_usd"] = value

    # ── Item 16 — target groups ──────────────────────────────────────────────
    target_section = ""
    m16 = re.search(r"\b16\s*[\.\(]", text, re.IGNORECASE)
    if m16:
        target_section = text[m16.start():m16.start() + 600]

    KNOWN_TARGETS = [
        "Public officials", "Legislators", "Government agencies",
        "Newspapers", "Magazines", "Radio", "Television",
        "Editors", "Foreign nationals", "Academic institutions",
        "Libraries", "Business associations", "Civic organizations",
        "Religious organizations", "Labor unions",
    ]
    found_targets = [t for t in KNOWN_TARGETS if t.lower() in target_section.lower()]
    result["target_groups"] = found_targets

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today     = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=LOOKBACK_DAYS)
    from_str  = yesterday.strftime("%m-%d-%Y")
    to_str    = today.strftime("%m-%d-%Y")

    # Step 1: fetch new registration numbers
    url = (f"{FARA_BASE}/Registrants/json/New"
           f"?{urllib.parse.urlencode({'from': from_str, 'to': to_str})}")
    print(f"[fara] GET {url}")
    try:
        data = api_get(url)
    except Exception as e:
        print(f"[fara] ERROR: request failed: {e}", file=sys.stderr)
        _write_error(str(e))
        sys.exit(0)

    new_rows = unwrap_rowset(data)
    print(f"[fara] {len(new_rows)} new registration(s)")

    # Load existing signals; build dedup set
    if SIGNALS_PATH.exists():
        try:
            existing = json.loads(SIGNALS_PATH.read_text())
        except Exception:
            existing = {"generated_at": None, "sources": ["fara"], "signals": []}
    else:
        existing = {"generated_at": None, "sources": ["fara"], "signals": []}

    known_reg_numbers = {
        s.get("registration_number")
        for s in existing.get("signals", [])
        if s.get("registration_number") is not None
    }

    # Step 2: enrich each new registration via ForeignPrincipals endpoint
    new_signals = []
    for row in new_rows:
        reg_number = row.get("REGISTRATION_x0020_NUMBER")
        if reg_number in known_reg_numbers:
            print(f"[fara] reg {reg_number} already present — skip")
            continue

        filed_date = parse_date(row.get("REGISTRATION_x0020_DATE") or "")

        time.sleep(ENRICH_DELAY)
        fp_url = f"{FARA_BASE}/ForeignPrincipals/json/Active/{reg_number}"
        print(f"[fara] GET {fp_url}")
        try:
            fp_data = api_get(fp_url)
            fp_rows = unwrap_rowset(fp_data)
        except Exception as e:
            print(f"[fara] WARNING: ForeignPrincipals fetch failed for {reg_number}: {e}")
            fp_rows = []

        if fp_rows:
            fp = fp_rows[0]
            registrant     = (fp.get("REGISTRANT_NAME") or row.get("NAME") or "").strip()
            fp_name        = (fp.get("FP_NAME") or "").strip()
            country_name   = (fp.get("COUNTRY_NAME") or "").strip()
            filed_date     = filed_date or parse_date(fp.get("REG_DATE") or "")
        else:
            registrant   = (row.get("NAME") or "").strip()
            fp_name      = ""
            country_name = ""

        iso         = country_to_iso2(country_name)
        fp_short    = fp_name.split(",")[0].strip() if fp_name else ""
        title       = f"{registrant} — {fp_short}" if fp_short else registrant

        # Direct PDF link: efile.fara.gov/docs/{reg}-Registration-Statement-{YYYYMMDD}-1.pdf
        raw_date    = (row.get("REGISTRATION_x0020_DATE") or "")[:10].replace("-", "")
        page_url    = (f"https://efile.fara.gov/docs/{reg_number}"
                       f"-Registration-Statement-{raw_date}-1.pdf")

        # PDF enrichment — extract business description, compensation, target groups
        time.sleep(ENRICH_DELAY)
        print(f"[fara] PDF {page_url}")
        pdf_data = enrich_from_pdf(page_url)

        # Description: prefer PDF Item 5h; fall back to API fields
        desc_fallback = ", ".join(filter(None, [registrant, fp_name, country_name]))
        description   = pdf_data["description"] or desc_fallback

        # Append target groups to description if found
        if pdf_data["target_groups"]:
            targets_str = ", ".join(pdf_data["target_groups"])
            description = f"{description} — targets: {targets_str}"

        sig = {
            "registration_number": reg_number,
            "iso":         iso,
            "source":      "fara",
            "signal_date": filed_date,
            "title":       title,
            "value_usd":   pdf_data["value_usd"],
            "description": description,
            "raw_score":   raw_score_for(iso),
            "weight":      1.0,
            "page_url":    page_url,
        }
        new_signals.append(sig)
        known_reg_numbers.add(reg_number)
        print(f"[fara] + reg {reg_number}  {iso}  {title[:60]}")

    print(f"[fara] {len(new_signals)} new signal(s)")

    all_signals = existing.get("signals", []) + new_signals
    all_signals.sort(key=lambda s: s.get("signal_date") or "")

    SIGNALS_PATH.write_text(json.dumps({
        "generated_at": today.isoformat(),
        "sources":      ["fara"],
        "signals":      all_signals,
    }, indent=2))
    print(f"[fara] Wrote {len(all_signals)} total signals ({len(new_signals)} new) → {SIGNALS_PATH}")


def _write_error(error: str):
    try:
        existing = json.loads(SIGNALS_PATH.read_text()) if SIGNALS_PATH.exists() else {}
    except Exception:
        existing = {}
    existing.update({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["fara"],
        "error": error,
    })
    existing.setdefault("signals", [])
    SIGNALS_PATH.write_text(json.dumps(existing, indent=2))


if __name__ == "__main__":
    main()
