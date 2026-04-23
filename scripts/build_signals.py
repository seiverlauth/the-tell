#!/usr/bin/env python3
"""
build_signals.py — Aggregation and enrichment layer for HARPY.

Reads all data/*_signals.json source files, then:
  1. Merges all records
  2. Filters: drops iso null/US, drops records before DATE_CUTOFF (XX passes through as "unresolved")
  3. Deduplicates: cn_number (DSCA) or iso|signal_date|description composite key
  4. Enriches each signal with precomputed fields:
       profile      — nested {name, score, factors, rationale} from country profile
       layer        — source taxonomy layer (military, influence, regulatory, ...)
       quality      — source quality weight (0.0–1.0)
       dollar_mod   — log-scaled dollar bonus (DSCA/SAM only)
       is_fr_policy — True if federalregister signal matches policy regex
  5. Sorts by signal_date descending
  6. Writes data/signals.json

Fields stripped from source records: raw_score, weight (both unused downstream).
"""

import json
import glob
import hashlib
import math
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    import anthropic as _anthropic
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False

sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    load_profile, SOURCE_LAYER_MAP, source_quality, dollar_modifier, FR_POLICY_RE,
)

DATA_DIR         = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE      = os.path.join(DATA_DIR, "signals.json")
PROSE_CACHE_FILE = os.path.join(DATA_DIR, "prose_cache.json")

DATE_CUTOFF = "2025-01-01"   # signals before this date are excluded from output

STRIP_FIELDS = {"raw_score", "weight"}


def build_profile_block(iso: str):
    """Return nested profile dict for iso, or None if no profile exists."""
    if not iso:
        return None
    p = load_profile(iso)
    if p is None:
        return None
    return {
        "name":      p.get("name"),
        "score":     p.get("structural_interest_score"),
        "factors":   p.get("key_structural_interests"),
        "rationale": p.get("score_rationale"),
    }


def enrich(sig: dict) -> dict:
    """Add precomputed fields; strip dead fields. Returns new dict."""
    out = {k: v for k, v in sig.items() if k not in STRIP_FIELDS}

    iso    = sig.get("iso") or ""
    source = sig.get("source") or ""
    title  = sig.get("title") or ""

    out["profile"]      = build_profile_block(iso)
    out["layer"]        = SOURCE_LAYER_MAP.get(source.lower())
    out["quality"]      = source_quality(source, title)
    out["dollar_mod"]   = dollar_modifier(sig.get("value_usd"), source)
    out["is_fr_policy"] = bool(FR_POLICY_RE.search(title)) if source.lower() == "federalregister" else None

    return out


def dedup_key(sig: dict):
    cn = sig.get("cn_number")
    if cn:
        return f"cn:{cn}"
    return f"{sig.get('iso')}|{sig.get('signal_date')}|{sig.get('description')}"


def _signal_key(sig: dict) -> str:
    """Stable unique key for a signal, used in theme signal_keys arrays."""
    src = sig.get("source", "")
    unique = {
        "fara":           sig.get("registration_number"),
        "lda":            sig.get("filing_uuid"),
        "dsca":           sig.get("cn_number"),
        "cftc":           sig.get("commodity"),
        "imf":            sig.get("imf_id"),
        "federalregister":sig.get("document_number"),
        "anchor_budget":  sig.get("accession"),
    }.get(src) or (sig.get("title") or "")[:40]
    return f"{src}:{unique}"


# ── Commodity→sector mapping for CFTC basket overlap ────────────────────────
# A country ISO only counts toward cftc_overlap if at least one of its
# non-CFTC signals in the window has a title containing a commodity-sector
# keyword. Wheat positioning does not pull in pipeline lobbying.

COMMODITY_SECTORS: dict = {
    "WTI Crude":    {"oil", "petroleum", "crude", "barrel", "refin", "pipeline",
                     "energy", "fuel", "lng", "opec", "hydrocarbon"},
    "Brent Crude":  {"oil", "petroleum", "crude", "barrel", "refin", "pipeline",
                     "energy", "fuel", "lng", "opec", "hydrocarbon"},
    "Natural Gas":  {"gas", "pipeline", "lng", "energy", "fuel", "petroleum",
                     "hydrocarbon", "methane"},
    "Heating Oil":  {"oil", "petroleum", "fuel", "refin", "energy", "diesel"},
    "Gold":         {"gold", "metal", "mining", "reserve", "bullion", "precious"},
    "Copper":       {"copper", "metal", "mining", "mineral", "ore"},
    "Palladium":    {"palladium", "platinum", "metal", "mining", "mineral",
                     "catalytic", "autocatalyst"},
    "Wheat":        {"wheat", "grain", "agriculture", "food", "crop", "farm",
                     "cereal", "flour", "milling", "harvest"},
    "Corn":         {"corn", "maize", "grain", "agriculture", "food", "crop",
                     "farm", "ethanol", "cereal"},
    "Soybeans":     {"soybean", "soy", "grain", "agriculture", "food", "crop",
                     "farm", "oilseed", "meal"},
    "Cocoa":        {"cocoa", "chocolate", "agriculture", "food", "crop",
                     "commodity", "cacao"},
    "RUB Futures":  {"ruble", "rouble", "russia", "russian", "currency",
                     "forex", "monetary"},
    "CNH Futures":  {"yuan", "renminbi", "china", "chinese", "currency",
                     "forex", "cnh", "monetary"},
}

# Stopwords for context signal keyword filter used in generate_prose_for_themes.

_CONTEXT_STOPWORDS: set = {
    "with", "that", "this", "from", "have", "will", "been", "they", "their",
    "were", "said", "than", "then", "when", "also", "into", "more", "some",
    "would", "about", "there", "which", "other", "after", "first", "could",
    "these", "those", "through", "between", "registered", "represent",
    "united", "states", "kingdom", "north", "south", "east", "west",
    "republic", "democratic", "federal", "national", "international",
    "government", "ministry", "department", "company", "corporation",
    "systems", "group", "services", "partners", "management", "holdings",
    "limited", "llc", "inc", "corp", "hedge", "fund", "funds",
}


def compute_themes(enriched: list) -> list:
    """
    Compute narrative themes from enriched signals.

    Three algorithms, all based on surprise relative to baseline:
      1. first_appearance        — country appears in a source after ≥180 days of silence
      2. influence_before_action — FARA/LDA precedes DSCA/OFAC/SAM/BIS within 90 days,
                                   and this sequence didn't exist in the prior 180 days
      3. cftc_overlap            — CFTC positioning anomaly precedes country signals
                                   within 30 days (CFTC is the leading indicator)
    """
    today = datetime.now(timezone.utc).date()

    def days_ago(date_str: str) -> int:
        if not date_str:
            return 99999
        try:
            d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            return (today - d).days
        except ValueError:
            return 99999

    def sig_date(s: dict):
        ds = s.get("signal_date", "")
        if not ds:
            return None
        try:
            return datetime.strptime(ds[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    def prof_score(sig: dict) -> int:
        p = sig.get("profile")
        return (p.get("score") or 0) if p else 0

    def profile_name(sig: dict) -> str:
        p = sig.get("profile")
        if p and p.get("name"):
            return p["name"]
        return sig.get("iso") or "Unknown"

    themes: list = []

    active = [s for s in enriched
              if s.get("iso") and s.get("iso") not in ("XX", "US")
              and s.get("signal_date")]

    # ── Algorithm 1: First Appearance ────────────────────────────────────────
    #
    # Country X receives a signal from source S, and has not appeared in source S
    # in the prior 180 days. The apparatus starts doing paperwork it wasn't doing.
    # Saudi Arabia never fires (always in DSCA). Morocco firing after 14 months is news.

    SILENCE_WINDOW = 180
    CURRENT_WINDOW = 30

    by_iso_src: dict = {}
    for s in active:
        by_iso_src.setdefault((s["iso"], s.get("source", "")), []).append(s)

    for (iso, src), sigs in by_iso_src.items():
        current = [s for s in sigs if days_ago(s["signal_date"]) < CURRENT_WINDOW]
        if not current:
            continue
        # Appeared within the silence window → not actually silent
        if any(CURRENT_WINDOW <= days_ago(s["signal_date"]) < CURRENT_WINDOW + SILENCE_WINDOW
               for s in sigs):
            continue

        best = max(current, key=lambda s: s.get("quality", 0))
        ps = prof_score(best)
        if ps < 2:
            continue

        all_prior = [s for s in sigs if days_ago(s["signal_date"]) >= CURRENT_WINDOW]
        if all_prior:
            last_days = min(days_ago(s["signal_date"]) for s in all_prior)
            silence_label = f"{last_days}d"
        else:
            last_days = 999
            silence_label = "first recorded"

        # "First recorded" is only meaningful for high-quality sources with enough
        # history to trust the absence. SAM/IMF/LDA data is thin — skip.
        if last_days == 999 and best.get("quality", 0) < 0.85:
            continue

        recency_w = math.exp(-min(days_ago(best["signal_date"]), 30) / 15)
        score = ps * best.get("quality", 0.5) * recency_w * math.log2(last_days / 30 + 2)

        country_name = profile_name(best)
        themes.append({
            "type": "first_appearance",
            "title": f"{country_name} — {src} after {silence_label} silence",
            "score": round(score, 3),
            "countries": [iso],
            "signal_keys": [_signal_key(s) for s in current],
            "why": (
                f"{src} filed for {country_name} after {silence_label} of silence "
                f"(profile score: {ps}). Signal: {best.get('title', '')[:80]}"
            ),
        })

    # ── Algorithm 2: Influence Before Action ─────────────────────────────────
    #
    # FARA or LDA for country X, followed within 90 days by DSCA/OFAC/SAM/BIS.
    # Influence must come first. Only fires if this pattern wasn't present in the
    # prior 180 days — standing relationships (Saudi Arabia, Israel) are excluded.

    INFLUENCE_SOURCES = {"fara", "lda"}
    ACTION_SOURCES    = {"dsca", "ofac", "sam", "bis"}
    IBA_WINDOW        = 90   # max days between influence and action
    IBA_LOOKBACK      = 180  # prior history window for standing-relationship check

    by_iso: dict = {}
    for s in active:
        by_iso.setdefault(s["iso"], []).append(s)

    for iso, sigs in by_iso.items():
        current = [s for s in sigs if days_ago(s.get("signal_date", "")) < IBA_WINDOW]
        inf_sigs = [s for s in current if s.get("source") in INFLUENCE_SOURCES]
        act_sigs = [s for s in current if s.get("source") in ACTION_SOURCES]

        if not inf_sigs or not act_sigs:
            continue

        best_pair = None
        best_score = 0.0
        for inf in inf_sigs:
            d_inf = sig_date(inf)
            if not d_inf:
                continue
            for act in act_sigs:
                d_act = sig_date(act)
                if not d_act:
                    continue
                gap = (d_act - d_inf).days
                if gap < 7 or gap > IBA_WINDOW:
                    continue
                ps = prof_score(inf) or prof_score(act)
                pair_score = (
                    ps *
                    math.log2(ps + 2) *
                    math.exp(-gap / 45) *
                    (inf.get("quality", 0) + act.get("quality", 0))
                )
                if pair_score > best_score:
                    best_score = pair_score
                    best_pair = (inf, act, gap)

        if not best_pair:
            continue

        inf_sig, act_sig, gap = best_pair
        ps = prof_score(inf_sig) or prof_score(act_sig)
        if ps < 3:
            continue

        # Standing-relationship check: both layers present in the prior window too
        prior = [s for s in sigs
                 if IBA_WINDOW <= days_ago(s.get("signal_date", "")) < IBA_WINDOW + IBA_LOOKBACK]
        if (any(s.get("source") in INFLUENCE_SOURCES for s in prior) and
                any(s.get("source") in ACTION_SOURCES for s in prior)):
            continue

        country_name = profile_name(inf_sig)
        themes.append({
            "type": "influence_before_action",
            "title": (
                f"{country_name} — {inf_sig.get('source')} → "
                f"{act_sig.get('source')} ({gap}d)"
            ),
            "score": round(best_score, 3),
            "countries": [iso],
            "signal_keys": list({_signal_key(inf_sig), _signal_key(act_sig)}),
            "why": (
                f"{inf_sig.get('source')} on {inf_sig.get('signal_date')} "
                f"→ {act_sig.get('source')} on {act_sig.get('signal_date')} "
                f"({gap}d gap). No prior {inf_sig.get('source')}+action sequence "
                f"in preceding {IBA_LOOKBACK}d."
            ),
        })

    # ── Algorithm 3: CFTC Basket Overlap ─────────────────────────────────────
    #
    # CFTC positioning anomaly fires first; country signals follow within 30 days.
    # CFTC is the leading indicator — country action must come after, not before.

    iso_ps: dict = {}
    for s in enriched:
        i = s.get("iso")
        if i and i not in iso_ps:
            p = s.get("profile")
            iso_ps[i] = (p.get("score") if p else None) or 0

    non_cftc = [s for s in enriched if s.get("source") != "cftc"
                and s.get("iso") not in (None, "XX", "US")
                and s.get("signal_date")]

    for cftc_sig in enriched:
        if cftc_sig.get("source") != "cftc":
            continue
        cftc_date_str = cftc_sig.get("signal_date", "")
        if not cftc_date_str:
            continue
        try:
            cftc_date = datetime.strptime(cftc_date_str[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        commodity = cftc_sig.get("commodity") or "Unknown"
        basket = set(cftc_sig.get("basket") or [])
        if not basket:
            continue

        sector_kw = COMMODITY_SECTORS.get(commodity, set())
        active_isos: set = set()
        for s in non_cftc:
            try:
                sd = datetime.strptime(s["signal_date"][:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            # Country signal must come AFTER the CFTC anomaly (CFTC is the leading indicator)
            gap_days = (sd - cftc_date).days
            if not (0 <= gap_days <= 30):
                continue
            if sector_kw:
                if not any(kw in (s.get("title") or "").lower() for kw in sector_kw):
                    continue
            active_isos.add(s["iso"])

        overlap = basket & active_isos
        if not overlap:
            continue

        overlap_score = sum(iso_ps.get(i, 0) for i in overlap)
        z = cftc_sig.get("z_score") or 0
        score = abs(z) * math.log2(len(overlap) + 1) * overlap_score / len(basket)
        themes.append({
            "type": "cftc_overlap",
            "title": f"{commodity} positioning anomaly — {len(overlap)} exposed countries active",
            "score": round(score, 3),
            "countries": sorted(overlap),
            "signal_keys": [_signal_key(cftc_sig)],
            "why": (
                f"{commodity} z={z:+.1f}σ on {cftc_date_str}. "
                f"Countries with subsequent signals: {', '.join(sorted(overlap))} "
                f"({len(overlap)}/{len(basket)} basket exposed)."
            ),
        })

    themes.sort(key=lambda t: t["score"], reverse=True)

    # ── Convergence collapse ─────────────────────────────────────────────────
    # Two independent algorithms firing on the same country is a stronger signal
    # than either alone. Collapse into a single convergence theme.
    PROSE_FLOOR_FOR_COLLAPSE = 5.0
    single_country_above_floor = defaultdict(list)
    for t in themes:
        if t["score"] >= PROSE_FLOOR_FOR_COLLAPSE and len(t.get("countries", [])) == 1:
            single_country_above_floor[t["countries"][0]].append(t)

    collapsed_isos = set()
    convergence_themes = []
    for iso, matching in single_country_above_floor.items():
        if len(matching) < 2:
            continue
        collapsed_isos.add(iso)
        combined_score = sum(t["score"] for t in matching)
        combined_keys = list({k for t in matching for k in t.get("signal_keys", [])})
        type_labels = " + ".join(
            t["type"].replace("first_appearance", "first appearance")
                     .replace("influence_before_action", "influence → action")
                     .replace("cftc_overlap", "CFTC overlap")
            for t in matching
        )
        country_name = matching[0]["title"].split(" —")[0]

        why_parts = [f"[{t['type']}] {t['why']}" for t in matching]
        convergence_themes.append({
            "type": "convergence",
            "title": f"{country_name} — {type_labels}",
            "score": round(combined_score, 3),
            "countries": [iso],
            "signal_keys": combined_keys,
            "why": " | ".join(why_parts),
            "_components": matching,
        })

    if convergence_themes:
        themes = [t for t in themes if not (
            len(t.get("countries", [])) == 1 and t["countries"][0] in collapsed_isos
        )]
        themes.extend(convergence_themes)
        themes.sort(key=lambda t: t["score"], reverse=True)

    return themes[:20]


def _load_anthropic_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        env_path = Path(__file__).parent.parent / ".env"
        try:
            with open(env_path) as f:
                for line in f:
                    if line.startswith("ANTHROPIC_API_KEY="):
                        key = line.strip().split("=", 1)[1]
                        break
        except FileNotFoundError:
            pass
    return key


def _prose_cache_key(theme: dict) -> str:
    # Include sorted signal_keys in the key so that prose regenerates when the
    # underlying signal composition changes — e.g. after grouping logic fixes or
    # new signals arriving for the country. Title alone is not sufficient.
    keys_fingerprint = hashlib.md5(
        "|".join(sorted(theme.get("signal_keys", []))).encode()
    ).hexdigest()[:8]
    raw = f"{theme['type']}|{','.join(sorted(theme['countries']))}|{theme['title']}|{keys_fingerprint}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


_PROSE_SYSTEM = (
    "You are a coherence filter. Your primary output is a rejection. Most patterns fail.\n\n"
    "STEP 1 — COHERENCE GATE\n"
    "Before writing anything, assess whether the contributing signals share a plausible causal mechanism. "
    "Valid mechanisms: the same named entity appears across multiple signals; "
    "signals belong to the same sector with directional logic connecting them; "
    "signals form a traceable transaction chain (e.g. policy → procurement → arms sale); "
    "signals share the same policy domain with identifiable cause-and-effect.\n"
    "Country co-occurrence alone FAILS. Layer co-occurrence alone FAILS. "
    "Temporal overlap alone FAILS.\n"
    "If you cannot name the causal mechanism in one sentence, output:\n"
    '  {"coherent": false, "reason": "<one sentence>", "headline": null, "body": null, "prompt": null}\n'
    "and stop.\n\n"
    "STEP 2 — NARRATIVE (only if coherent)\n"
    "State what happened, what the mechanism is, and why it is upstream of public knowledge. "
    "No hedging. No 'this could suggest.' "
    "Sentence 1: what the data shows. Sentence 2: what the mechanism is. "
    "Sentence 3 (optional): what comes next if the pattern holds. "
    "Max 60 words total.\n\n"
    "STEP 3 — RESEARCH PROMPT (only if coherent)\n"
    "Write a prompt a non-expert reader would paste into Claude immediately after reading the headline. "
    "Lead with the so-what: why does this matter, what is at stake, who benefits or loses. "
    "Then: who are the named entities, how do they connect, is this pattern routine or anomalous. "
    "End with: what specifically to watch for next and why. "
    "Name only the entities in the causal mechanism — not every entity in the country bucket. "
    "First person ('I just saw...'). Max 130 words.\n\n"
    "Return valid JSON only with exactly these fields:\n"
    "- coherent: boolean\n"
    "- reason: string (rejection reason if coherent=false, otherwise null)\n"
    "- headline: string (if coherent=true: max 12 words, states the mechanism — not a description; null if false)\n"
    "- body: string (if coherent=true: 2 sentences max, ~40 words. Sentence 1: what the data shows. Sentence 2: what the mechanism means or what comes next; null if false)\n"
    "- prompt: string (research prompt if coherent=true, otherwise null)\n"
    "No other fields."
)


def generate_prose_for_themes(themes: list, enriched: list) -> list:
    """Add narrative dict to each theme via Claude API. Cache results.

    Top 10 themes (by score) get narrative generated. Themes ranked 10+ get narrative: null.
    Cache entries with old schema keys (narrative_prose, watch_for) are discarded as stale.
    """
    # Default all themes to null; will be filled in for top 10 below
    for t in themes:
        t["narrative"] = None

    if not _ANTHROPIC_AVAILABLE:
        print("  anthropic package not installed — skipping prose generation", file=sys.stderr)
        return themes

    api_key = _load_anthropic_key()
    if not api_key:
        print("  ANTHROPIC_API_KEY not set — skipping prose generation", file=sys.stderr)
        return themes

    # Signal lookup by key
    sig_lookup: dict = {_signal_key(s): s for s in enriched}

    # Build iso → list-of-signals index for context lookup
    by_iso: dict = {}
    for s in enriched:
        iso = s.get("iso")
        if iso and iso not in ("XX", "US"):
            by_iso.setdefault(iso, []).append(s)

    # Load cache — discard entries with old schema
    cache: dict = {}
    try:
        with open(PROSE_CACHE_FILE) as f:
            raw_cache = json.load(f)
        for k, v in raw_cache.items():
            if isinstance(v, dict) and "narrative" in v and "narrative_prose" not in v and "watch_for" not in v and "prompt" in v.get("narrative", {}):
                cache[k] = v
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    client = _anthropic.Anthropic(api_key=api_key)
    cache_updated = False

    today = datetime.now(timezone.utc).date()

    # Only generate prose for themes with score above floor — weak themes stay null
    # and are invisible in the narratives panel. Better 3 real narratives than 10 noisy ones.
    PROSE_SCORE_FLOOR = 3.0

    for theme in [t for t in themes[:10] if t.get("score", 0) >= PROSE_SCORE_FLOOR]:
        ck = _prose_cache_key(theme)
        if ck in cache:
            theme["narrative"] = cache[ck]["narrative"]
            continue

        # Build contributing signal details (up to 5 from signal_keys)
        theme_signal_keys = set(theme.get("signal_keys", []))
        contrib_lines = []
        for sk in theme.get("signal_keys", [])[:5]:
            sig = sig_lookup.get(sk)
            if not sig:
                continue
            parts = [f"[{sig.get('source', '?')}] {sig.get('title', '')}"]
            if sig.get("signal_date"):
                parts.append(f"date={sig['signal_date']}")
            if sig.get("value_usd"):
                parts.append(f"value=${sig['value_usd']:,.0f}")
            if sig.get("description"):
                parts.append(f"description={sig['description'][:200]}")
            contrib_lines.append("- " + " | ".join(parts))

        # Extract domain keywords from contributing signals for context filtering
        contrib_kw: set = set()
        for sk in theme.get("signal_keys", []):
            csig = sig_lookup.get(sk)
            if not csig:
                continue
            words = set(re.findall(r'\b[a-z]{4,}\b', (csig.get("title") or "").lower()))
            contrib_kw |= (words - _CONTEXT_STOPWORDS)

        # Build context signals: same countries, last 90 days, thematically relevant (up to 10)
        context_lines = []
        theme_countries = theme.get("countries", [])
        context_seen: set = set()
        for iso in theme_countries:
            for s in by_iso.get(iso, []):
                sk = _signal_key(s)
                if sk in theme_signal_keys or sk in context_seen:
                    continue
                date_str = s.get("signal_date", "")
                try:
                    sd = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
                    if (today - sd).days > 90:
                        continue
                except (ValueError, TypeError):
                    continue
                if contrib_kw:
                    ctx_words = set(re.findall(r'\b[a-z]{4,}\b', (s.get("title") or "").lower()))
                    if not (ctx_words & contrib_kw):
                        continue
                context_seen.add(sk)
                context_lines.append(
                    f"- [{s.get('source', '?')}] {s.get('title', '')} ({date_str})"
                )
                if len(context_lines) >= 10:
                    break
            if len(context_lines) >= 10:
                break

        if theme["type"] == "convergence" and theme.get("_components"):
            component_summaries = "\n".join(
                f"- [{c['type']}] {c['title']}: {c['why']}"
                for c in theme["_components"]
            )
            user_msg = (
                f"Pattern type: convergence (multiple independent algorithms fired on same country)\n"
                f"Pattern title: {theme['title']}\n"
                f"Score: {theme['score']}\n"
                f"Countries involved: {', '.join(theme_countries)}\n"
                f"Component findings:\n{component_summaries}\n\n"
                f"Contributing signals (primary):\n"
                + ("\n".join(contrib_lines) if contrib_lines else "- (none resolved)")
                + "\n\nOther signals from same countries (last 90 days):\n"
                + ("\n".join(context_lines) if context_lines else "- (none)")
            )
        else:
            user_msg = (
                f"Pattern type: {theme['type']}\n"
                f"Pattern title: {theme['title']}\n"
                f"Score: {theme['score']}\n"
                f"Countries involved: {', '.join(theme_countries)}\n"
                f"Algorithm finding: {theme['why']}\n\n"
                f"Contributing signals (primary):\n"
                + ("\n".join(contrib_lines) if contrib_lines else "- (none resolved)")
                + "\n\nOther signals from same countries (last 90 days):\n"
                + ("\n".join(context_lines) if context_lines else "- (none)")
            )

        try:
            resp = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=500,
                temperature=0,
                system=_PROSE_SYSTEM,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip().rstrip("`").strip()
            parsed = json.loads(raw)
            if not parsed.get("coherent", True):
                reason = parsed.get("reason", "no reason given")
                print(f"  Prose rejected (incoherent): {theme['title'][:60]} [score={theme.get('score', 0):.1f}] — {reason}", file=sys.stderr)
                theme["narrative"] = None
                continue
            narrative = {
                "headline": parsed.get("headline") or "",
                "body":     parsed.get("body") or "",
                "prompt":   parsed.get("prompt") or "",
            }
            theme["narrative"] = narrative
            cache[ck] = {"narrative": narrative}
            cache_updated = True
            print(f"  Prose: {theme['title'][:60]}")
        except Exception as e:
            print(f"  Prose generation failed for '{theme['title'][:60]}': {e}", file=sys.stderr)
            theme["narrative"] = None

    if cache_updated:
        with open(PROSE_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
        print(f"  Prose cache updated ({len(cache)} entries → {PROSE_CACHE_FILE})")

    return themes


def main():
    pattern      = os.path.join(DATA_DIR, "*_signals.json")
    source_files = sorted(glob.glob(pattern))

    raw_signals      = []
    sources_found    = []
    counts_per_source = {}

    for path in source_files:
        if os.path.abspath(path) == os.path.abspath(OUTPUT_FILE):
            continue

        filename    = os.path.basename(path)
        source_name = filename.replace("_signals.json", "")

        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ERROR reading {filename}: {e}")
            continue

        signals = data.get("signals", [])
        signals = [s for s in signals if s.get("title")]

        raw_signals.extend(signals)
        sources_found.append(source_name)
        counts_per_source[source_name] = len(signals)
        print(f"  Found: {filename}  ({len(signals)} records)")

    # Filter — XX records pass through (rendered as "unresolved" in feed, excluded from map/scoring)
    filtered = [
        s for s in raw_signals
        if s.get("iso")
        and s.get("iso") != "US"
        and (s.get("signal_date") or "") >= DATE_CUTOFF
    ]
    xx_count = sum(1 for s in filtered if s.get("iso") == "XX")
    print(f"\n  After filter (iso valid, date >= {DATE_CUTOFF}): {len(filtered)} / {len(raw_signals)} ({xx_count} unresolved XX)")

    # Deduplicate
    seen = set()
    deduped = []
    for s in filtered:
        key = dedup_key(s)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(s)
    print(f"  After dedup: {len(deduped)}")

    # Enrich
    enriched = [enrich(s) for s in deduped]

    # Sort newest first
    enriched.sort(key=lambda s: s.get("signal_date") or "", reverse=True)

    themes = compute_themes(enriched)
    themes = generate_prose_for_themes(themes, enriched)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources":      sources_found,
        "signals":      enriched,
        "themes":       themes,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nTotal records: {len(enriched)}")
    print(f"Sources: {sources_found}")
    print("Records per source (pre-filter):")
    for src, count in counts_per_source.items():
        print(f"  {src}: {count}")

    # Theme summary
    type_counts = Counter(t["type"] for t in themes)
    print(
        f"\nThemes: {len(themes)} computed "
        f"(first_appearance: {type_counts.get('first_appearance', 0)}, "
        f"influence_before_action: {type_counts.get('influence_before_action', 0)}, "
        f"cftc: {type_counts.get('cftc_overlap', 0)}, "
        f"convergence: {type_counts.get('convergence', 0)})"
    )

    print("\nFirst 3 records:")
    for i, rec in enumerate(enriched[:3]):
        print(f"  [{i}] {json.dumps(rec)}")

    print(f"\nWrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
