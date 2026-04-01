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

import argparse
import io
import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import country_to_iso2, profile_score, load_existing, append_and_write, write_error

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

FARA_BASE              = "https://efile.fara.gov/api/v1"
LOOKBACK_DAYS          = 1
LOOKBACK_DAYS_BACKFILL = 365
ENRICH_DELAY           = 2.0  # seconds between enrichment calls

# Sub-state entities and FARA-specific country name variants that
# country_to_iso2 (utils.py) does not cover. Keys are lowercase.
FARA_ISO_OVERRIDES = {
    "republika srpska":                 "BA",
    "republic of srpska":               "BA",
    "government of republika srpska":   "BA",
    "bermuda":                          "BM",
    "republic of the congo":            "CG",
    "congo, republic of":               "CG",
    "congo, republic of the":           "CG",
    "democratic republic of the congo": "CD",
    "congo, democratic republic of the": "CD",
    "cayman islands":                   "KY",
    "british virgin islands":           "VG",
    "turks and caicos islands":         "TC",
    "turks and caicos":                 "TC",
    "isle of man":                      "IM",
    "jersey":                           "JE",
    "guernsey":                         "GG",
    "macau":                            "MO",
    "macao":                            "MO",
    "western sahara":                   "EH",
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "fara_signals.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fara_country_to_iso2(name: str):
    """Resolve FARA country name to ISO alpha-2, checking local overrides first."""
    if not name:
        return None
    key = name.strip().lower()
    if key in FARA_ISO_OVERRIDES:
        return FARA_ISO_OVERRIDES[key]
    return country_to_iso2(name)


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true",
                        help=f"Fetch {LOOKBACK_DAYS_BACKFILL} days of history")
    args = parser.parse_args()

    today    = datetime.now(timezone.utc)
    days     = LOOKBACK_DAYS_BACKFILL if args.backfill else LOOKBACK_DAYS
    start    = today - timedelta(days=days)
    from_str = start.strftime("%m-%d-%Y")
    to_str   = today.strftime("%m-%d-%Y")

    # Step 1: fetch new registration numbers
    url = (f"{FARA_BASE}/Registrants/json/New"
           f"?{urllib.parse.urlencode({'from': from_str, 'to': to_str})}")
    print(f"[fara] GET {url}")
    try:
        data = api_get(url)
    except Exception as e:
        print(f"[fara] ERROR: request failed: {e}", file=sys.stderr)
        write_error(SIGNALS_PATH, "fara", str(e))
        sys.exit(0)

    new_rows = unwrap_rowset(data)
    print(f"[fara] {len(new_rows)} new registration(s)")

    existing = load_existing(SIGNALS_PATH, "fara")
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

        iso         = fara_country_to_iso2(country_name)
        fp_short    = fp_name.split(",")[0].strip() if fp_name else ""
        # Action-first: "Registrant registered to represent Principal" makes clear
        # what happened, rather than just listing parties.
        title       = (f"{registrant} registered to represent {fp_short}"
                       if fp_short else registrant)

        # Direct PDF link: efile.fara.gov/docs/{reg}-Registration-Statement-{YYYYMMDD}-1.pdf
        raw_date    = (row.get("REGISTRATION_x0020_DATE") or "")[:10].replace("-", "")
        page_url    = (f"https://efile.fara.gov/docs/{reg_number}"
                       f"-Registration-Statement-{raw_date}-1.pdf")

        # PDF enrichment — extract business description, compensation, target groups
        time.sleep(ENRICH_DELAY)
        print(f"[fara] PDF {page_url}")
        pdf_data = enrich_from_pdf(page_url)

        # Description: prefer PDF Item 5h (nature of registrant's business).
        # Fallback when PDF parse fails: construct from API fields in a readable form.
        if fp_name and country_name:
            desc_fallback = f"Registered to represent {fp_name} ({country_name})"
        elif fp_name:
            desc_fallback = f"Registered to represent {fp_name}"
        elif country_name:
            desc_fallback = f"{registrant} — foreign agent for {country_name}"
        else:
            desc_fallback = registrant
        description = pdf_data["description"] or desc_fallback

        sig = {
            "registration_number": reg_number,
            "iso":          iso,
            "source":       "fara",
            "signal_date":  filed_date,
            "title":        title,
            "value_usd":    pdf_data["value_usd"],
            "description":  description,
            "registrant":   registrant or None,
            "principal":    fp_name or None,
            "target_groups": pdf_data["target_groups"],
            "raw_score":    profile_score(iso),
            "weight":       1.0,
            "page_url":     page_url,
        }
        new_signals.append(sig)
        known_reg_numbers.add(reg_number)
        print(f"[fara] + reg {reg_number}  {iso}  {title[:60]}")

    print(f"[fara] {len(new_signals)} new signal(s)")

    added = append_and_write(SIGNALS_PATH, "fara", new_signals, lambda s: s.get("registration_number"))
    print(f"[fara] {added} new signal(s) written → {SIGNALS_PATH}")


if __name__ == "__main__":
    main()
