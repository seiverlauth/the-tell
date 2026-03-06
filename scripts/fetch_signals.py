#!/usr/bin/env python3
"""
fetch_signals.py
Fetch RSS feeds, count country mentions, write data/signals.json.
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import feedparser

# ── Feeds ──────────────────────────────────────────────────────────────────────
FEEDS = [
    ("AP",        "https://news.google.com/rss/search?q=site%3Aapnews.com&hl=en-US&gl=US&ceid=US%3Aen"),
    ("Reuters",   "https://news.google.com/rss/search?q=site%3Areuters.com&hl=en-US&gl=US&ceid=US%3Aen"),
    ("AFP",       "https://news.google.com/rss/search?q=site%3Aafp.com&hl=en-US&gl=US&ceid=US%3Aen"),
    ("BBC",       "http://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Al Jazeera","https://www.aljazeera.com/xml/rss/all.xml"),
]

# ── Country name → ISO alpha-2 ─────────────────────────────────────────────────
# Longer/more specific names first to avoid substring false matches.
COUNTRY_NAMES = {
    "Afghanistan": "AF",
    "Albania": "AL",
    "Algeria": "DZ",
    "Angola": "AO",
    "Argentina": "AR",
    "Armenia": "AM",
    "Australia": "AU",
    "Austria": "AT",
    "Azerbaijan": "AZ",
    "Bangladesh": "BD",
    "Belarus": "BY",
    "Belgium": "BE",
    "Bolivia": "BO",
    "Bosnia": "BA",
    "Brazil": "BR",
    "Bulgaria": "BG",
    "Cambodia": "KH",
    "Cameroon": "CM",
    "Canada": "CA",
    "Central African Republic": "CF",
    "Chad": "TD",
    "Chile": "CL",
    "China": "CN",
    "Colombia": "CO",
    "Congo": "CD",
    "Croatia": "HR",
    "Cuba": "CU",
    "Czech Republic": "CZ",
    "Czechia": "CZ",
    "Denmark": "DK",
    "Ecuador": "EC",
    "Egypt": "EG",
    "El Salvador": "SV",
    "Eritrea": "ER",
    "Estonia": "EE",
    "Ethiopia": "ET",
    "Finland": "FI",
    "France": "FR",
    "Gaza": "PS",
    "Georgia": "GE",
    "Germany": "DE",
    "Ghana": "GH",
    "Greece": "GR",
    "Guatemala": "GT",
    "Guinea": "GN",
    "Haiti": "HT",
    "Honduras": "HN",
    "Hungary": "HU",
    "Iceland": "IS",
    "India": "IN",
    "Indonesia": "ID",
    "Iran": "IR",
    "Iraq": "IQ",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Japan": "JP",
    "Jordan": "JO",
    "Kazakhstan": "KZ",
    "Kenya": "KE",
    "North Korea": "KP",
    "South Korea": "KR",
    "Kosovo": "XK",
    "Kuwait": "KW",
    "Kyrgyzstan": "KG",
    "Laos": "LA",
    "Latvia": "LV",
    "Lebanon": "LB",
    "Libya": "LY",
    "Lithuania": "LT",
    "Malaysia": "MY",
    "Mali": "ML",
    "Mexico": "MX",
    "Moldova": "MD",
    "Mongolia": "MN",
    "Montenegro": "ME",
    "Morocco": "MA",
    "Mozambique": "MZ",
    "Myanmar": "MM",
    "Burma": "MM",
    "Namibia": "NA",
    "Nepal": "NP",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Nicaragua": "NI",
    "Niger": "NE",
    "Nigeria": "NG",
    "North Macedonia": "MK",
    "Norway": "NO",
    "Oman": "OM",
    "Pakistan": "PK",
    "Palestine": "PS",
    "West Bank": "PS",
    "Panama": "PA",
    "Paraguay": "PY",
    "Peru": "PE",
    "Philippines": "PH",
    "Poland": "PL",
    "Portugal": "PT",
    "Qatar": "QA",
    "Romania": "RO",
    "Russia": "RU",
    "Rwanda": "RW",
    "Saudi Arabia": "SA",
    "Senegal": "SN",
    "Serbia": "RS",
    "Sierra Leone": "SL",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "Somalia": "SO",
    "South Africa": "ZA",
    "South Sudan": "SS",
    "Spain": "ES",
    "Sri Lanka": "LK",
    "Sudan": "SD",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Syria": "SY",
    "Taiwan": "TW",
    "Tajikistan": "TJ",
    "Tanzania": "TZ",
    "Thailand": "TH",
    "Timor-Leste": "TL",
    "Togo": "TG",
    "Tunisia": "TN",
    "Turkey": "TR",
    "Turkmenistan": "TM",
    "Uganda": "UG",
    "Ukraine": "UA",
    "United Arab Emirates": "AE",
    "UAE": "AE",
    "United Kingdom": "GB",
    "UK": "GB",
    "United States": "US",
    "USA": "US",
    "America": "US",
    "Uruguay": "UY",
    "Uzbekistan": "UZ",
    "Venezuela": "VE",
    "Vietnam": "VN",
    "West Africa": None,   # region, skip
    "Yemen": "YE",
    "Zambia": "ZM",
    "Zimbabwe": "ZW",
}

# Pre-compile one regex per country name (word-boundary, case-insensitive).
# Sort longest names first so "Saudi Arabia" matches before "Arabia".
_PATTERNS = [
    (re.compile(r'\b' + re.escape(name) + r'\b', re.IGNORECASE), iso)
    for name, iso in sorted(COUNTRY_NAMES.items(), key=lambda item: -len(item[0]))
    if iso is not None
]


def mentions_in(text):
    """Return list of ISO codes mentioned in text (duplicates included)."""
    found = []
    for pattern, iso in _PATTERNS:
        for _ in pattern.finditer(text):
            found.append(iso)
    return found


def fetch_feed(name, url):
    """Fetch one RSS feed, return flat list of ISO codes from all articles."""
    codes = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            text = " ".join([
                entry.get("title", ""),
                entry.get("summary", ""),
                entry.get("description", ""),
            ])
            codes.extend(mentions_in(text))
        print("  {}: {} articles, {} country mentions".format(
            name, len(feed.entries), len(codes)))
    except Exception as exc:
        print("  {}: FAILED — {}".format(name, exc), file=sys.stderr)
    return codes


def normalize(counts):
    if not counts:
        return {}
    lo, hi = min(counts.values()), max(counts.values())
    span = hi - lo or 1
    return {iso: round((v - lo) / span * 100) for iso, v in counts.items()}


def main():
    print("Fetching feeds...")
    all_codes = []
    for name, url in FEEDS:
        all_codes.extend(fetch_feed(name, url))

    # Count mentions per country
    counts = {}
    for iso in all_codes:
        counts[iso] = counts.get(iso, 0) + 1

    scores = normalize(counts)

    # Write output
    out_path = Path(__file__).parent.parent / "data" / "signals.json"
    out_path.parent.mkdir(exist_ok=True)
    payload = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "scores": scores,
    }
    out_path.write_text(json.dumps(payload, indent=2))

    # Summary
    print(f"\nDone. {len(counts)} countries scored.")
    top = sorted(scores.items(), key=lambda x: -x[1])[:10]
    print("Top 10:")
    for iso, score in top:
        bar = "█" * (score // 5)
        print(f"  {iso}  {score:>3}  {bar}")
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
