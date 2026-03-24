#!/usr/bin/env python3
"""
fetch_dsca.py — DSCA Major Arms Sales congressional notification scraper

Source: https://www.dsca.mil/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library
Pagination: ?igpage=N (last page discovered at runtime from "LAST" link)
Strategy: scrape every page, collect all records, filter by target country client-side.

Usage:
  python fetch_dsca.py --probe              # dump page 1 HTML + pagination info, exit
  python fetch_dsca.py                      # scrape all pages, write data/dsca_notifications.json
  python fetch_dsca.py --backtest           # filter saved data to 2021-02-24→2022-02-24, print
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DSCA_LIBRARY_URL = "https://www.dsca.mil/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library"
DSCA_MAIN_URL    = "https://www.dsca.mil/Press-Media/Major-Arms-Sales"
REQUEST_DELAY    = 1.5  # seconds between library page requests
ENRICH_DELAY     = 1.0  # seconds between article page fetches (be polite)

# Comprehensive country name → ISO alpha-2 map.
# Covers all countries that commonly appear in DSCA arms sale notifications.
# Keys are uppercase as they appear in DSCA filenames.
DSCA_COUNTRY_MAP = {
    "AFGHANISTAN": "AF", "ALBANIA": "AL", "ALGERIA": "DZ", "ANGOLA": "AO",
    "ARGENTINA": "AR", "ARMENIA": "AM", "AUSTRALIA": "AU", "AUSTRIA": "AT",
    "AZERBAIJAN": "AZ", "BAHRAIN": "BH", "BANGLADESH": "BD", "BELGIUM": "BE",
    "BELIZE": "BZ", "BENIN": "BJ", "BOTSWANA": "BW", "BRAZIL": "BR",
    "BULGARIA": "BG", "BURKINA FASO": "BF", "CAMBODIA": "KH", "CAMEROON": "CM",
    "CANADA": "CA", "CHAD": "TD", "CHILE": "CL", "COLOMBIA": "CO",
    "CROATIA": "HR", "CZECH REPUBLIC": "CZ", "DENMARK": "DK", "DJIBOUTI": "DJ",
    "DOMINICAN REPUBLIC": "DO", "ECUADOR": "EC", "EGYPT": "EG",
    "EL SALVADOR": "SV", "ERITREA": "ER", "ESTONIA": "EE", "ETHIOPIA": "ET",
    "FINLAND": "FI", "FRANCE": "FR", "GEORGIA": "GE", "GERMANY": "DE",
    "GHANA": "GH", "GREECE": "GR", "GUATEMALA": "GT", "HONDURAS": "HN",
    "HUNGARY": "HU", "INDIA": "IN", "INDONESIA": "ID", "IRAQ": "IQ",
    "IRELAND": "IE", "ISRAEL": "IL", "ITALY": "IT", "JAMAICA": "JM",
    "JAPAN": "JP", "JORDAN": "JO", "KAZAKHSTAN": "KZ", "KENYA": "KE",
    "KOSOVO": "XK", "KUWAIT": "KW", "KYRGYZSTAN": "KG", "LATVIA": "LV",
    "LEBANON": "LB", "LIBERIA": "LR", "LIBYA": "LY", "LITHUANIA": "LT",
    "LUXEMBOURG": "LU", "MALAYSIA": "MY", "MALI": "ML", "MAURITANIA": "MR",
    "MEXICO": "MX", "MOLDOVA": "MD", "MONGOLIA": "MN", "MONTENEGRO": "ME",
    "MOROCCO": "MA", "MOZAMBIQUE": "MZ", "NAMIBIA": "NA", "NETHERLANDS": "NL",
    "NEW ZEALAND": "NZ", "NIGER": "NE", "NIGERIA": "NG", "NORTH MACEDONIA": "MK",
    "NORWAY": "NO", "OMAN": "OM", "PAKISTAN": "PK", "PANAMA": "PA",
    "PAPUA NEW GUINEA": "PG", "PERU": "PE", "PHILIPPINES": "PH", "POLAND": "PL",
    "PORTUGAL": "PT", "QATAR": "QA", "ROMANIA": "RO", "RWANDA": "RW",
    "SAUDI ARABIA": "SA", "SENEGAL": "SN", "SERBIA": "RS", "SINGAPORE": "SG",
    "SLOVAKIA": "SK", "SLOVENIA": "SI", "SOMALIA": "SO", "SOUTH AFRICA": "ZA",
    "SOUTH KOREA": "KR", "SOUTH SUDAN": "SS", "SPAIN": "ES", "SRI LANKA": "LK",
    "SWEDEN": "SE", "SWITZERLAND": "CH", "TAIWAN": "TW", "TAJIKISTAN": "TJ",
    "TANZANIA": "TZ", "THAILAND": "TH", "TIMOR-LESTE": "TL", "TOGO": "TG",
    "TRINIDAD AND TOBAGO": "TT", "TUNISIA": "TN", "TURKEY": "TR",
    "TURKMENISTAN": "TM", "UGANDA": "UG", "UKRAINE": "UA",
    "UNITED ARAB EMIRATES": "AE", "UAE": "AE", "UNITED KINGDOM": "GB",
    "URUGUAY": "UY", "UZBEKISTAN": "UZ", "VIETNAM": "VN", "YEMEN": "YE",
    "ZAMBIA": "ZM", "ZIMBABWE": "ZW",
    # DSCA-specific abbreviations and alternate forms
    "REPUBLIC OF KOREA": "KR", "ROK": "KR",
    "REPUBLIC OF THE PHILIPPINES": "PH",
    "CZECH": "CZ",
    "KINGDOM OF SAUDI ARABIA": "SA",
    "KINGDOM OF BAHRAIN": "BH",
    "HASHEMITE KINGDOM OF JORDAN": "JO",
    "NSPA": "XN",   # NATO Support and Procurement Agency — no ISO, use XN
    "NATO": "XN",
}

# Sorted longest-first so multi-word names match before their substrings
_DSCA_NAMES_SORTED = sorted(DSCA_COUNTRY_MAP.keys(), key=len, reverse=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "Referer": "https://www.dsca.mil/Press-Media/Major-Arms-Sales",
}

DAILY_LOOKBACK_DAYS = 45  # fetch articles published in last N days

BACKTEST_START = "2021-02-24"
BACKTEST_END   = "2022-02-24"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_session():
    s = requests.Session(impersonate="chrome120")
    s.headers.update(HEADERS)
    return s


def parse_date_from_url(url):
    """Extract date from media.defense.gov URL: /YYYY/Mon/DD/"""
    m = re.search(r"/(\d{4})/([A-Za-z]{3})/(\d{2})/", url)
    if m:
        try:
            return datetime.strptime(
                f"{m.group(1)}/{m.group(2)}/{m.group(3)}", "%Y/%b/%d"
            ).date().isoformat()
        except ValueError:
            pass
    return None


def parse_cn_from_text(text):
    """Extract CN number like '21-45' from a filename or title."""
    m = re.search(r"\b(\d{2,4}-\d{1,3})\b", text)
    return m.group(1) if m else None


def country_from_filename(filename):
    """
    Extract country name and ISO alpha-2 from a DSCA filename.

    Filename patterns observed:
      'PRESS RELEASE - UKRAINE 25-105 CN.PDF'
      'PRESS RELEASE - SAUDI ARABIA 25-103 CN.PDF'
      'GEORGIA_17-59.PDF'

    Strategy:
      1. Strip 'PRESS RELEASE - ' prefix and file extension.
      2. Extract the text before the first CN number (NN-NNN pattern).
      3. Look up the resulting name in DSCA_COUNTRY_MAP (longest match first).

    Returns (country_name, iso2) or (None, None).
    """
    text = filename.upper()
    text = re.sub(r"\.PDF$", "", text).strip()
    text = re.sub(r"^PRESS\s+RELEASE\s*[-–]\s*", "", text).strip()
    # Replace underscores (older format: GEORGIA_17-59)
    text = text.replace("_", " ").strip()
    # Extract text before the CN number
    m = re.match(r"^(.+?)\s+\d{2,4}-\d{1,3}", text)
    if not m:
        candidate = text
    else:
        candidate = m.group(1).strip()

    # Longest-match lookup
    for name in _DSCA_NAMES_SORTED:
        if candidate == name or candidate.startswith(name + " "):
            return name.title(), DSCA_COUNTRY_MAP[name]

    return None, None


def parse_page(html):
    """
    Parse one library page. Returns:
      records  — list of notification dicts for target countries
      last_page — int, highest page number found in pagination (or None)
    """
    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Find last page from pagination
    last_page = None
    for a in soup.find_all("a", href=True):
        if "igpage=" in a["href"]:
            try:
                n = int(re.search(r"igpage=(\d+)", a["href"]).group(1))
                if last_page is None or n > last_page:
                    last_page = n
            except (AttributeError, ValueError):
                pass

    # Collect all media.defense.gov PDF links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "media.defense.gov" not in href:
            continue
        if not href.lower().endswith(".pdf"):
            continue

        title = a.get_text(strip=True) or href.split("/")[-1]
        filename = href.split("/")[-1]

        country_name, iso2 = country_from_filename(filename)
        if not iso2:
            country_name, iso2 = country_from_filename(title)
        if not iso2:
            continue  # couldn't identify country — skip

        date_str = parse_date_from_url(href)
        cn_number = parse_cn_from_text(filename) or parse_cn_from_text(title)

        records.append({
            "cn_number": cn_number,
            "country": country_name,
            "country_iso2": iso2,
            "date": date_str,
            "pdf_url": href,
            "title": title,
        })

    return records, last_page


# ---------------------------------------------------------------------------
# Article enrichment — page_url discovery + article page parsing
# ---------------------------------------------------------------------------

def parse_listing_date(date_str):
    """Parse 'Feb. 6, 2026' or 'Feb 6, 2026' → '2026-02-06'."""
    date_str = re.sub(r"\.", "", date_str.strip())   # strip trailing dots on month abbrev
    date_str = re.sub(r"\s+", " ", date_str)
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def country_iso_from_title(title):
    """
    Extract ISO alpha-2 from an article title like 'Ukraine – Class IX Spare Parts'
    or 'Kingdom of Saudi Arabia – F-15 Sustainment'.
    """
    m = re.match(r"^(.+?)\s*[–—\-]\s*.+", title)
    if not m:
        return None
    candidate = m.group(1).strip().upper()
    if candidate in DSCA_COUNTRY_MAP:
        return DSCA_COUNTRY_MAP[candidate]
    for name in _DSCA_NAMES_SORTED:
        if candidate == name or candidate.startswith(name + " "):
            return DSCA_COUNTRY_MAP[name]
    return None


def scrape_listing_page(html):
    """
    Parse one page of the main DSCA arms sales listing.
    Returns (items, last_page) where items = [{article_url, date_str, title}, ...].

    The library page (used by the main scraper) only has PDF links — no article URLs.
    This function scrapes the separate listing page that does have article links.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for div in soup.find_all("div", class_="item"):
        date_el  = div.find("p", class_="date")
        title_el = div.find("p", class_="title")
        if not date_el or not title_el:
            continue
        a = title_el.find("a", href=True)
        if not a:
            continue
        date_str    = parse_listing_date(date_el.get_text(strip=True))
        title       = a.get_text(strip=True)
        article_url = a["href"]
        if not article_url.startswith("http"):
            article_url = "https://www.dsca.mil" + article_url
        items.append({"article_url": article_url, "date_str": date_str, "title": title})

    last_page = None
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]Page=(\d+)", a["href"])
        if m:
            n = int(m.group(1))
            if last_page is None or n > last_page:
                last_page = n

    return items, last_page


def parse_article_page(html):
    """
    Extract description, value_usd, quantity, and cn_number from a DSCA article page.
    Returns a dict; any field is None if not found.

    - description: weapon system name from <h1> '{Country} – {Weapon System}'
    - value_usd:   float in USD from 'estimated cost of $X million/billion'
    - quantity:    int from 'requested to buy ... (N)'; None if not applicable
    - cn_number:   from 'Transmittal No. YY-NNN' in body text (used for disambiguation)
    """
    soup = BeautifulSoup(html, "html.parser")

    description = None
    h1 = soup.find("h1")
    if h1:
        h1_text = h1.get_text(strip=True)
        m = re.match(r"^.+?\s*[–—\-]\s*(.+)$", h1_text)
        if m:
            description = m.group(1).strip()

    body = soup.select_one(".article-body")
    body_text = body.get_text(" ", strip=True) if body else ""

    cn_number = None
    m = re.search(r"Transmittal No\.?\s+(\d{2,4}-\d{1,3})", body_text)
    if m:
        cn_number = m.group(1)

    value_usd = None
    m = re.search(
        r"estimated(?:\s+total)?\s+cost(?:\s+(?:of|is))?\s+\$([0-9,.]+)\s*(million|billion)",
        body_text, re.IGNORECASE,
    )
    if m:
        raw        = float(m.group(1).replace(",", ""))
        multiplier = 1_000_000_000 if m.group(2).lower() == "billion" else 1_000_000
        value_usd  = raw * multiplier

    quantity = None
    m = re.search(r"requested to buy\s+[\w\s\-]+?\((\d+)\)", body_text, re.IGNORECASE)
    if m:
        quantity = int(m.group(1))

    return {
        "description": description,
        "value_usd":   value_usd,
        "quantity":    quantity,
        "cn_number":   cn_number,
    }


def build_article_url_map(target_signals, session):
    """
    Scrape the main DSCA listing page (with pagination) until we've covered the
    date range of target_signals.

    Returns {(date_str, iso2): [article_url, ...]} — one or more articles per
    date+country combination (multiple sales to the same country on the same date
    are common).
    """
    if not target_signals:
        return {}

    oldest_date = min(s.get("signal_date") or "9999" for s in target_signals)
    by_date_iso = {}
    page = 1

    while True:
        url = DSCA_MAIN_URL if page == 1 else f"{DSCA_MAIN_URL}?Page={page}"
        print(f"[enrich] Fetching listing page {page}: {url}")
        if page > 1:
            time.sleep(ENRICH_DELAY)

        try:
            resp = session.get(url, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"[enrich] Listing page {page} error: {e} — stopping")
            break

        if resp.status_code != 200:
            print(f"[enrich] Listing page {page} HTTP {resp.status_code} — stopping")
            break

        items, last_page = scrape_listing_page(resp.text)
        if not items:
            break

        for item in items:
            iso2 = country_iso_from_title(item["title"])
            if item["date_str"] and iso2:
                key = (item["date_str"], iso2)
                by_date_iso.setdefault(key, []).append(item["article_url"])

        page_dates = [item["date_str"] for item in items if item["date_str"]]
        if page_dates and min(page_dates) < oldest_date:
            break
        if last_page and page >= last_page:
            break
        page += 1

    return by_date_iso


def find_page_url_for_signal(signal, by_date_iso, session):
    """
    Resolve the DSCA article page URL for a signal.

    Matches by (signal_date, iso2). When multiple articles share the same date
    and country, fetches each candidate to check for the matching CN number.
    Returns the article URL string, or None if not found.
    """
    key        = (signal.get("signal_date"), signal.get("iso"))
    candidates = by_date_iso.get(key, [])

    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    # Disambiguate by CN number
    cn = signal.get("cn_number")
    if not cn:
        return candidates[0]

    for article_url in candidates:
        time.sleep(ENRICH_DELAY)
        try:
            resp = session.get(article_url, timeout=30)
        except requests.exceptions.RequestException:
            continue
        if resp.status_code != 200:
            continue
        parsed = parse_article_page(resp.text)
        if parsed.get("cn_number") == cn:
            return article_url

    return candidates[0]   # fallback: first candidate


def enrich_signals(signals_path, session, test_n=None):
    """
    Populate description, value_usd, quantity, and page_url for signals that
    currently have description=null.

    Skips any signal where description is already set.
    Rate-limits article fetches to ENRICH_DELAY seconds.

    test_n: if set, process only the N most recent unenriched signals, print
            results, and do NOT write to disk.
    """
    data    = json.loads(signals_path.read_text())
    signals = data["signals"]

    to_enrich = [s for s in signals if not s.get("description")]
    to_enrich.sort(key=lambda s: s.get("signal_date") or "", reverse=True)

    if test_n is not None:
        to_enrich = to_enrich[:test_n]
        print(f"[test-enrich] {len(to_enrich)} most recent unenriched signals")

    if not to_enrich:
        print("[enrich] All signals already have descriptions — nothing to do")
        return

    by_date_iso = build_article_url_map(to_enrich, session)
    enriched    = 0

    for signal in to_enrich:
        page_url = signal.get("page_url") or find_page_url_for_signal(
            signal, by_date_iso, session
        )
        if not page_url:
            iso = signal.get("iso")
            cn  = signal.get("cn_number")
            dt  = signal.get("signal_date")
            print(f"[enrich] No article URL found for {iso} CN {cn} ({dt}) — skipping")
            continue

        time.sleep(ENRICH_DELAY)
        print(f"[enrich] Fetching {page_url}")
        try:
            resp = session.get(page_url, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"[enrich] Error: {e}")
            continue
        if resp.status_code != 200:
            print(f"[enrich] HTTP {resp.status_code}")
            continue

        parsed = parse_article_page(resp.text)

        if test_n is not None:
            iso = signal.get("iso")
            cn  = signal.get("cn_number")
            dt  = signal.get("signal_date")
            print(f"\n  {iso}  CN {cn}  {dt}")
            print(f"    page_url:    {page_url}")
            print(f"    description: {parsed['description']}")
            print(f"    value_usd:   {parsed['value_usd']}")
            print(f"    quantity:    {parsed['quantity']}")
        else:
            signal["page_url"]    = page_url
            signal["description"] = parsed["description"]
            signal["value_usd"]   = parsed["value_usd"]
            if parsed.get("quantity") is not None:
                signal["quantity"] = parsed["quantity"]
            enriched += 1

    if test_n is None:
        signals_path.write_text(json.dumps(data, indent=2))
        print(f"[enrich] Enriched {enriched} signals → {signals_path}")


# ---------------------------------------------------------------------------
# Daily scrape — listing page only, append new records, dedupe by cn_number
# ---------------------------------------------------------------------------

def scrape_daily(signals_path, session):
    """
    Daily update mode. Scrapes the main DSCA listing page (not the library
    pagination) for articles published in the last DAILY_LOOKBACK_DAYS days.
    Fetches each article page, dedupes by cn_number against existing
    dsca_signals.json, and appends any new records.

    This is the default mode run by GitHub Actions.
    """
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=DAILY_LOOKBACK_DAYS)).isoformat()

    # Load existing signals; build cn_number index for deduplication
    if signals_path.exists():
        data = json.loads(signals_path.read_text())
        signals = data.get("signals", [])
    else:
        data = {"generated_at": None, "sources": ["dsca"], "signals": []}
        signals = []

    known_cns = {s["cn_number"] for s in signals if s.get("cn_number")}

    # Collect listing items within lookback window
    recent_items = []
    page = 1
    while True:
        url = DSCA_MAIN_URL if page == 1 else f"{DSCA_MAIN_URL}?Page={page}"
        print(f"[daily] Listing page {page}: {url}")
        if page > 1:
            time.sleep(ENRICH_DELAY)

        try:
            resp = session.get(url, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Listing page {page} request error: {e}", file=sys.stderr)
            sys.exit(1)

        if resp.status_code != 200:
            print(f"[ERROR] Listing page {page} HTTP {resp.status_code}", file=sys.stderr)
            sys.exit(1)

        items, last_page = scrape_listing_page(resp.text)
        if not items:
            print("[daily] No items found on listing page — stopping")
            break

        in_window = [i for i in items if i.get("date_str") and i["date_str"] >= cutoff]
        recent_items.extend(in_window)

        oldest = min((i["date_str"] for i in items if i.get("date_str")), default="9999")
        if oldest < cutoff or (last_page and page >= last_page):
            break
        page += 1

    print(f"[daily] {len(recent_items)} articles in last {DAILY_LOOKBACK_DAYS} days")

    # Fetch each article page; skip records we already have
    added = 0
    for item in recent_items:
        iso2 = country_iso_from_title(item["title"])
        if not iso2:
            print(f"[daily] No ISO match: {item['title']!r} — skipping")
            continue

        time.sleep(ENRICH_DELAY)
        print(f"[daily] GET {item['article_url']}")
        try:
            resp = session.get(item["article_url"], timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"[daily] Request error: {e} — skipping")
            continue
        if resp.status_code != 200:
            print(f"[daily] HTTP {resp.status_code} — skipping")
            continue

        parsed = parse_article_page(resp.text)
        cn = parsed.get("cn_number")

        if cn and cn in known_cns:
            print(f"[daily] CN {cn} already present — skip")
            continue

        entry = {
            "iso":         iso2,
            "source":      "dsca",
            "signal_date": item["date_str"],
            "title":       item["title"],
            "value_usd":   parsed.get("value_usd"),
            "description": parsed.get("description"),
            "raw_score":   None,
            "cn_number":   cn,
            "pdf_url":     None,
            "page_url":    item["article_url"],
        }
        if parsed.get("quantity") is not None:
            entry["quantity"] = parsed["quantity"]

        signals.append(entry)
        if cn:
            known_cns.add(cn)
        added += 1
        desc = parsed.get("description") or "—"
        print(f"[daily] + {iso2}  CN {cn}  {item['date_str']}  {desc}")

    data["generated_at"] = datetime.now(timezone.utc).isoformat()
    data["signals"] = sorted(signals, key=lambda s: s.get("signal_date") or "")
    signals_path.write_text(json.dumps(data, indent=2))
    print(f"[daily] {added} new record(s) added → {signals_path}")


# ---------------------------------------------------------------------------
# Probe mode
# ---------------------------------------------------------------------------

def probe():
    """Fetch page 1, print pagination summary and first 10 target-country records."""
    session = get_session()
    print(f"[probe] GET {DSCA_LIBRARY_URL}", file=sys.stderr)
    resp = session.get(DSCA_LIBRARY_URL, timeout=30)
    print(f"[probe] HTTP {resp.status_code}", file=sys.stderr)

    records, last_page = parse_page(resp.text)
    print(f"[probe] Last page: {last_page}", file=sys.stderr)
    print(f"[probe] Target-country records on page 1: {len(records)}", file=sys.stderr)

    print(json.dumps({"last_page": last_page, "page_1_records": records}, indent=2))


# ---------------------------------------------------------------------------
# Full scrape
# ---------------------------------------------------------------------------

def scrape(output_path):
    """Paginate all pages, collect target-country records, write JSON."""
    session = get_session()
    all_records = []
    seen_urls = set()

    # Page 1 — also discover last_page
    print(f"[scrape] Fetching page 1...")
    resp = session.get(DSCA_LIBRARY_URL, timeout=30)
    if resp.status_code != 200:
        print(f"[ERROR] Page 1 returned HTTP {resp.status_code}", file=sys.stderr)
        sys.exit(1)

    records, last_page = parse_page(resp.text)
    if last_page is None:
        print(f"[ERROR] Could not determine last page from pagination", file=sys.stderr)
        sys.exit(1)

    for r in records:
        if r["pdf_url"] not in seen_urls:
            seen_urls.add(r["pdf_url"])
            all_records.append(r)

    print(f"[scrape] Page 1/{last_page} — {len(records)} target records, {last_page} pages total")

    # Pages 2..last_page
    for page_num in range(2, last_page + 1):
        time.sleep(REQUEST_DELAY)
        url = f"{DSCA_LIBRARY_URL}?igpage={page_num}"

        # Retry up to 2 times on timeout/error
        resp = None
        for attempt in range(3):
            try:
                resp = session.get(url, timeout=60)
                break
            except requests.exceptions.Timeout:
                wait = 5 * (attempt + 1)
                print(f"[scrape] Page {page_num} timeout (attempt {attempt+1}/3) — waiting {wait}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                print(f"[scrape] Page {page_num} error: {e} — skipping")
                break

        if resp is None or resp.status_code != 200:
            status = resp.status_code if resp is not None else "timeout"
            print(f"[scrape] Page {page_num} HTTP {status} — skipping")
            continue

        records, _ = parse_page(resp.text)
        added = 0
        for r in records:
            if r["pdf_url"] not in seen_urls:
                seen_urls.add(r["pdf_url"])
                all_records.append(r)
                added += 1

        print(f"[scrape] Page {page_num}/{last_page} — {added} new target records ({len(all_records)} total)")

        # Save incrementally so a crash doesn't lose progress
        if added > 0:
            _write(all_records, output_path)

    _write(all_records, output_path)
    print(f"[done] {len(all_records)} notifications written to {output_path}")
    signals_path = output_path.parent / "dsca_signals.json"
    write_signals(output_path, signals_path)




def _write(records, output_path):
    sorted_records = sorted(records, key=lambda r: r.get("date") or "9999-99-99")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(sorted_records, indent=2))


def write_signals(notifications_path, signals_path):
    """
    Transform dsca_notifications.json → dsca_signals.json (and dsca_nato.json).

    NATO/NSPA collective notifications (iso2 == "XN") are written to
    dsca_nato.json using the same schema. All other records go to dsca_signals.json.
    """
    records = json.loads(notifications_path.read_text())
    generated_at = datetime.now(timezone.utc).isoformat()

    signals = []
    nato = []

    for r in records:
        iso2 = r.get("country_iso2")
        if not iso2:
            continue
        entry = {
            "iso":         iso2,
            "source":      "dsca",
            "signal_date": r.get("date"),
            "title":       r.get("title"),
            "value_usd":   None,
            "description": None,
            "quantity":    None,
            "raw_score":   None,
            "cn_number":   r.get("cn_number"),
            "pdf_url":     r.get("pdf_url"),
            "page_url":    None,
        }
        if iso2 == "XN":
            nato.append(entry)
        else:
            signals.append(entry)

    def _bundle(entries):
        return {
            "generated_at": generated_at,
            "sources":      ["dsca"],
            "signals":      entries,
        }

    signals_path.parent.mkdir(parents=True, exist_ok=True)
    signals_path.write_text(json.dumps(_bundle(signals), indent=2))
    print(f"[signals] {len(signals)} signals → {signals_path}")

    nato_path = signals_path.parent / "dsca_nato.json"
    nato_path.write_text(json.dumps(_bundle(nato), indent=2))
    print(f"[signals] {len(nato)} NATO/collective signals → {nato_path}")


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------

def backtest(data_path, start=BACKTEST_START, end=BACKTEST_END):
    """Filter dsca_notifications.json to a date window and print chronologically."""
    if not data_path.exists():
        print(f"[ERROR] {data_path} not found — run without flags first.", file=sys.stderr)
        sys.exit(1)

    records = json.loads(data_path.read_text())
    in_window = [r for r in records if r.get("date") and start <= r["date"] <= end]
    in_window.sort(key=lambda r: r["date"])

    print(f"[backtest] {start} → {end}  ({len(in_window)} of {len(records)} total)\n")
    for r in in_window:
        cn = r.get("cn_number") or "—"
        print(f"{r['date']}  {r['country']:<12}  CN {cn:<8}  {r['title']}")
        print(f"           {r['pdf_url']}")
        print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DSCA Major Arms Sales scraper")
    parser.add_argument("--probe", action="store_true",
                        help="Dump page 1 records and pagination info, exit")
    parser.add_argument("--full-scrape", action="store_true",
                        help="Bulk-load all pages of the library (historical import only)")
    parser.add_argument("--backtest", action="store_true",
                        help=f"Print {BACKTEST_START}→{BACKTEST_END} records from saved notifications")
    parser.add_argument("--enrich", action="store_true",
                        help="Fetch article pages to populate description, value_usd, quantity, page_url")
    parser.add_argument("--test-enrich", action="store_true",
                        help="Dry-run enrich on 5 most recent records — print results, do not write")
    args = parser.parse_args()

    repo_root    = Path(__file__).parent.parent
    signals_path = repo_root / "data" / "dsca_signals.json"

    if args.probe:
        probe()
        sys.exit(0)

    if args.backtest:
        notifications_path = repo_root / "data" / "dsca_notifications.json"
        backtest(notifications_path)
        sys.exit(0)

    if args.test_enrich:
        enrich_signals(signals_path, get_session(), test_n=5)
        sys.exit(0)

    if args.enrich:
        enrich_signals(signals_path, get_session())
        sys.exit(0)

    if args.full_scrape:
        # Historical bulk load via library pagination — writes dsca_notifications.json
        # then converts to dsca_signals.json. Run manually, not in CI.
        notifications_path = repo_root / "data" / "dsca_notifications.json"
        scrape(notifications_path)
        if not signals_path.exists():
            write_signals(notifications_path, signals_path)
        sys.exit(0)

    # Default: daily incremental update from listing page
    scrape_daily(signals_path, get_session())


if __name__ == "__main__":
    main()
