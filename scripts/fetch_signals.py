#!/usr/bin/env python3
"""
fetch_signals.py
Fetch RSS feeds, count country mentions, write data/signals.json.
Outputs layered scores: wire, think_tank, government, plus a weighted composite.
"""

import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import feedparser

# ── Browser User-Agent to avoid blocks ─────────────────────────────────────────
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}

# ── Feeds by layer ─────────────────────────────────────────────────────────────
WIRE_FEEDS = [
    ("AP",         "https://news.google.com/rss/search?q=site%3Aapnews.com&hl=en-US&gl=US&ceid=US%3Aen"),
    ("Reuters",    "https://news.google.com/rss/search?q=site%3Areuters.com&hl=en-US&gl=US&ceid=US%3Aen"),
    ("AFP",        "https://news.google.com/rss/search?q=site%3Aafp.com&hl=en-US&gl=US&ceid=US%3Aen"),
    ("BBC",        "http://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
]

THINK_TANK_FEEDS = [
    ("Atlantic Council", "https://www.atlanticcouncil.org/feed/"),
]

GOVERNMENT_FEEDS = [
    ("Congressional Record",        "https://www.congress.gov/rss/congressional-record.xml"),
    ("GAO Reports",                 "https://www.gao.gov/rss/reports.xml"),
    ("State Dept Press Releases",   "https://www.state.gov/rss-feed/press-releases/feed/"),
    ("State Dept Sec Remarks",      "https://www.state.gov/rss-feed/secretarys-remarks/feed/"),
    ("State Dept Travel Advisories","https://travel.state.gov/_res/rss/TAsTWs.xml"),
]

# Layer weights for composite score
LAYER_WEIGHTS = {
    "wire":       1,
    "think_tank": 2,
    "government": 3,
}

# Minimum raw mention count to enter a layer's scores
LAYER_THRESHOLDS = {
    "wire":       3,
    "think_tank": 2,
    "government": 3,
}

# ── Country name → ISO alpha-2 ─────────────────────────────────────────────────
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
    "Greenland": "GL",
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


def load_feeds():
    """
    Returns all feed configs as a list of dicts:
    [{ 'url', 'name', 'source_type', 'weight' }]
    weight = desc_weight for description mentions (0.0 for wire/think_tank, 0.5 for government).
    """
    feeds = []
    for name, url in WIRE_FEEDS:
        feeds.append({'name': name, 'url': url, 'source_type': 'wire', 'weight': 0.0})
    for name, url in THINK_TANK_FEEDS:
        feeds.append({'name': name, 'url': url, 'source_type': 'think_tank', 'weight': 0.0})
    for name, url in GOVERNMENT_FEEDS:
        feeds.append({'name': name, 'url': url, 'source_type': 'government', 'weight': 0.5})
    return feeds


def fetch_feed(feed):
    """
    Fetches one RSS feed. feed = { url, name, source_type, weight }.
    Returns: [{ 'title', 'description', 'published', 'feed_name', 'source_type', 'weight' }]
    Prints per-feed article count. On failure, prints to stderr and returns [].
    """
    articles = []
    try:
        parsed = feedparser.parse(feed['url'], request_headers=REQUEST_HEADERS)
        for entry in parsed.entries:
            articles.append({
                'title':       entry.get('title', ''),
                'description': entry.get('summary', '') or entry.get('description', ''),
                'published':   entry.get('published', ''),
                'feed_name':   feed['name'],
                'source_type': feed['source_type'],
                'weight':      feed['weight'],
            })
        print("  {}: {} articles".format(feed['name'], len(parsed.entries)))
    except Exception as exc:
        print("  {}: FAILED — {}".format(feed['name'], exc), file=sys.stderr)
    return articles


def extract_mentions(article, country_lookup):
    """
    Takes one article dict, returns { iso: weighted_score }.
    Title mentions: weight 1.0.
    Description mentions: weight = article['weight'] (skipped when 0.0).
    country_lookup = _PATTERNS (pre-compiled regex list).
    """
    scores = {}
    title = article['title']
    for pattern, iso in country_lookup:
        if pattern.search(title):
            scores[iso] = scores.get(iso, 0) + 1.0
    if article['weight'] > 0:
        desc = article['description']
        for pattern, iso in country_lookup:
            if pattern.search(desc):
                scores[iso] = scores.get(iso, 0) + article['weight']
    return scores


def aggregate_mentions(articles, country_lookup):
    """
    Runs extract_mentions on all articles, sums scores by ISO per source_type.
    Applies LAYER_THRESHOLDS filter and prints top-10 raw counts per layer.
    Returns:
        layer_counts  — { 'wire': {iso: count}, 'think_tank': {iso: count}, 'government': {iso: count} }
        layer_sources — { 'wire': {iso: [feed_names]}, ... } (filtered to above-threshold isos)
    """
    layer_counts = {'wire': {}, 'think_tank': {}, 'government': {}}
    layer_sources_sets = {'wire': {}, 'think_tank': {}, 'government': {}}

    for article in articles:
        st = article['source_type']
        for iso, score in extract_mentions(article, country_lookup).items():
            layer_counts[st][iso] = layer_counts[st].get(iso, 0) + score
            layer_sources_sets[st].setdefault(iso, set()).add(article['feed_name'])

    layer_sources = {}
    for label in ('wire', 'think_tank', 'government'):
        counts = layer_counts[label]
        if counts:
            top = sorted(counts.items(), key=lambda x: -x[1])[:10]
            print("  raw counts ({}): {}".format(
                label, ", ".join("{}={:.1f}".format(iso, v) for iso, v in top)))
        threshold = LAYER_THRESHOLDS[label]
        filtered = {iso: v for iso, v in counts.items() if v >= threshold}
        print("  {} countries above threshold ({})".format(len(filtered), threshold))
        layer_counts[label] = filtered
        layer_sources[label] = {
            iso: sorted(feeds)
            for iso, feeds in layer_sources_sets[label].items()
            if iso in filtered
        }

    return layer_counts, layer_sources


def normalize(counts, min_mentions=3):
    """
    Log normalization with minimum threshold.
    Returns { iso: 0-100 score }.
    """
    if not counts:
        return {}
    filtered = {iso: v for iso, v in counts.items() if v >= min_mentions}
    if not filtered:
        return {}
    log_counts = {iso: math.log(v + 1) for iso, v in filtered.items()}
    lo = min(log_counts.values())
    hi = max(log_counts.values())
    if hi == lo:
        return {iso: 50 for iso in filtered}
    return {iso: round((lv - lo) / (hi - lo) * 100)
            for iso, lv in log_counts.items()}


def build_composite(layers):
    """
    Blends wire×1 + think_tank×2 + government×3.
    Missing layers contribute 0. Renormalizes to 0-100 with log scale.
    layers = { 'wire': {iso: score}, 'think_tank': {iso: score}, 'government': {iso: score} }
    Returns { iso: score }.
    """
    all_isos = set()
    for scores in layers.values():
        all_isos.update(scores.keys())

    raw = {}
    for iso in all_isos:
        total = 0
        for layer, weight in LAYER_WEIGHTS.items():
            total += layers.get(layer, {}).get(iso, 0) * weight
        raw[iso] = total

    return normalize(raw)


def write_output(composite, layers, layer_sources):
    """
    Writes data/signals.json in current format.
    Returns the output path.
    """
    out_path = Path(__file__).parent.parent / "data" / "signals.json"
    out_path.parent.mkdir(exist_ok=True)
    payload = {
        "updated":   datetime.now(timezone.utc).isoformat(),
        "layers":    layers,
        "sources":   layer_sources,
        "composite": composite,
        "scores":    composite,  # backwards compat: index.html reads data.scores
    }
    out_path.write_text(json.dumps(payload, indent=2))
    return out_path


def main():
    feeds = load_feeds()

    articles = []
    current_type = None
    for feed in feeds:
        if feed['source_type'] != current_type:
            current_type = feed['source_type']
            print("Fetching {} feeds...".format(current_type))
        articles.extend(fetch_feed(feed))

    layer_counts, layer_sources = aggregate_mentions(articles, _PATTERNS)

    layer_scores = {}
    for layer, counts in layer_counts.items():
        min_mentions = 2 if layer == 'think_tank' else 3
        layer_scores[layer] = normalize(counts, min_mentions=min_mentions)

    composite = build_composite(layer_scores)
    out_path = write_output(composite, layer_scores, layer_sources)

    print("\nDone. Composite: {} countries scored.".format(len(composite)))
    top = sorted(composite.items(), key=lambda x: -x[1])[:10]
    print("Top 10 composite:")
    for iso, score in top:
        bar = "█" * (score // 5)
        print("  {}  {:>3}  {}".format(iso, score, bar))
    print("\nWrote {}".format(out_path))


if __name__ == "__main__":
    main()
