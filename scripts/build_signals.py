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
import statistics
import sys
from collections import Counter
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


def compute_themes(enriched: list) -> list:
    """
    Compute algorithmic narrative themes from enriched signals.

    Four algorithms:
      1. actor_concentration — FARA multi-firm principals / LDA multi-firm lobbying per iso
      2. velocity_anomaly    — current-30d signal count vs rolling baseline per country
      3. layer_sequence      — meaningful cross-layer ordered pairs within 90 days
      4. cftc_overlap        — CFTC basket vs active non-CFTC signals in same window
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

    def prof_score(sig: dict) -> int:
        p = sig.get("profile")
        if not p:
            return 0
        return p.get("score") or 0

    def profile_name(sig: dict) -> str:
        p = sig.get("profile")
        if p and p.get("name"):
            return p["name"]
        return sig.get("iso") or "Unknown"

    themes: list = []

    # ── Algorithm 1: Actor Concentration ────────────────────────────────────

    # FARA: group by principal
    fara_sigs = [s for s in enriched if s.get("source") == "fara"]
    by_principal: dict = {}
    for s in fara_sigs:
        p = s.get("principal")
        if p:
            by_principal.setdefault(p, []).append(s)

    for principal, sigs in by_principal.items():
        registrants = {s.get("registrant") for s in sigs if s.get("registrant")}
        if len(registrants) < 3:
            continue
        isos = [s.get("iso") for s in sigs if s.get("iso") and s.get("iso") not in ("XX", "US")]
        iso = isos[0] if isos else "XX"
        ps = prof_score(sigs[0])
        most_recent_days = min((days_ago(s.get("signal_date")) for s in sigs), default=9999)
        recency_w = math.exp(-most_recent_days / 60)
        firm_count = len(registrants)
        score = math.log2(firm_count + 1) * (ps + 1) * recency_w
        if score <= 1.0:
            continue
        dates = sorted([s.get("signal_date", "") for s in sigs if s.get("signal_date")])
        try:
            window_days = (
                datetime.strptime(dates[-1][:10], "%Y-%m-%d").date() -
                datetime.strptime(dates[0][:10], "%Y-%m-%d").date()
            ).days if len(dates) >= 2 else 0
        except ValueError:
            window_days = 0
        themes.append({
            "type": "actor_concentration",
            "title": f"{firm_count} firms registered to represent {principal}",
            "score": round(score, 3),
            "countries": [iso] if iso and iso != "XX" else [],
            "signal_keys": [_signal_key(s) for s in sigs],
            "why": f"{firm_count} distinct registrants for {principal} in {window_days}d. Baseline: 1–2 per principal.",
        })

    # LDA: group by lobbying_firm — flag a single firm representing multiple foreign-country clients
    # "Multiple firms on one country" is meaningless (that's just an active lobbying ecosystem).
    # "One firm with multiple foreign-country clients simultaneously" is actual concentration.
    lda_sigs = [s for s in enriched if s.get("source") == "lda"
                and s.get("iso") not in (None, "XX", "US")
                and s.get("lobbying_firm")]
    by_firm: dict = {}
    for s in lda_sigs:
        by_firm.setdefault(s["lobbying_firm"], []).append(s)

    for firm, sigs in by_firm.items():
        # Count distinct foreign ISOs this firm is representing
        isos_represented = {s["iso"] for s in sigs}
        # Established multi-country lobbying shops (Akin Gump, Holland & Knight, etc.)
        # always represent many countries — that's their business model, not a signal.
        # Only flag firms whose total foreign-country footprint in our data is ≤ 6.
        if len(isos_represented) > 6:
            continue
        if len(isos_represented) < 3:
            continue
        # Require at least 2 represented countries to have structural score ≥ 5
        # "Firm lobbies for Australia, Canada, New Zealand" is not a signal
        high_interest = sum(
            1 for iso in isos_represented
            for s in [next((x for x in sigs if x.get("iso") == iso), None)]
            if s and (s.get("profile") or {}).get("score", 0) >= 5
        )
        if high_interest < 2:
            continue
        most_recent_days = min((days_ago(s.get("signal_date")) for s in sigs), default=9999)
        recency_w = math.exp(-most_recent_days / 60)
        # Weight by sum of profile scores of represented countries
        ps_sum = sum(
            max((s.get("profile") or {}).get("score") or 0 for s in sigs if s.get("iso") == iso)
            for iso in isos_represented
        )
        iso_count = len(isos_represented)
        score = math.log2(iso_count + 1) * (ps_sum / iso_count + 1) * recency_w
        if score <= 1.0:
            continue
        country_names = []
        for iso in sorted(isos_represented):
            iso_sigs = [s for s in sigs if s.get("iso") == iso]
            country_names.append(profile_name(iso_sigs[0]) if iso_sigs else iso)
        themes.append({
            "type": "actor_concentration",
            "title": f"{firm} lobbying for {iso_count} countries simultaneously",
            "score": round(score, 3),
            "countries": sorted(isos_represented),
            "signal_keys": [_signal_key(s) for s in sigs],
            "why": f"{firm} filed LDA disclosures for {', '.join(country_names[:5])}{'...' if iso_count > 5 else ''} in the same period.",
        })

    # ── Algorithm 2: Velocity Anomaly ────────────────────────────────────────

    dated = [s for s in enriched if s.get("iso") and s.get("iso") not in ("XX", "US")
             and s.get("signal_date")]
    by_iso_vel: dict = {}
    for s in dated:
        by_iso_vel.setdefault(s["iso"], []).append(s)

    for iso, sigs in by_iso_vel.items():
        oldest_days = max(days_ago(s["signal_date"]) for s in sigs)
        n_prior = oldest_days // 30
        if n_prior < 3:
            continue

        current_sigs = [s for s in sigs if days_ago(s["signal_date"]) < 30]
        current_count = len(current_sigs)
        if current_count < 3:
            continue

        weighted_count = sum(s.get("quality", 0) for s in current_sigs)

        prior_counts = []
        for w in range(1, n_prior + 1):
            c = sum(s.get("quality", 0) for s in sigs if w * 30 <= days_ago(s["signal_date"]) < (w + 1) * 30)
            prior_counts.append(c)

        baseline_mean = statistics.mean(prior_counts)
        baseline_std = statistics.stdev(prior_counts) if len(prior_counts) >= 3 else 1.0
        velocity_z = (weighted_count - baseline_mean) / max(baseline_std, 0.5)

        if velocity_z < 2.0:
            continue

        ps = prof_score(sigs[0])
        country_name = profile_name(sigs[0])
        score = velocity_z * math.log2(ps + 2) * weighted_count

        src_counts = Counter(s.get("source", "?") for s in current_sigs)
        source_breakdown = ", ".join(f"{src}×{n}" for src, n in sorted(src_counts.items()))

        themes.append({
            "type": "velocity_anomaly",
            "title": f"{country_name} — {current_count} signals in 30 days ({velocity_z:+.1f}\u03c3)",
            "score": round(score, 3),
            "countries": [iso],
            "signal_keys": [_signal_key(s) for s in current_sigs],
            "why": (
                f"{weighted_count:.1f} weighted signals in 30 days vs {baseline_mean:.1f} avg "
                f"({velocity_z:+.1f}\u03c3). Sources: {source_breakdown}"
            ),
        })

    # ── Algorithm 3: Layer Sequence Detection ────────────────────────────────
    #
    # Approach: for each country, count quality-weighted cross-layer pairs in each
    # historical 30-day window. Flag only when the current window is anomalously
    # high vs. the country's own baseline. This prevents countries that are simply
    # active (many signals → many chance pairs) from dominating.
    #
    # Window tightened from 90d to 30d: a FARA filing and DSCA award 3 months apart
    # is not a meaningful sequence. 30 days is tight enough to suggest temporal coupling.

    MEANINGFUL_SEQ = {
        ("influence", "military"),
        ("influence", "regulatory"),
        ("financial", "regulatory"),
        ("financial", "military"),
        ("regulatory", "military"),
    }

    seq_sigs = [s for s in enriched if s.get("iso") and s.get("iso") not in ("XX", "US")
                and s.get("signal_date") and s.get("layer")]
    by_iso_seq: dict = {}
    for s in seq_sigs:
        by_iso_seq.setdefault(s["iso"], []).append(s)

    def count_pairs_in_window(sigs_sorted: list, window_start_days: int, window_end_days: int) -> float:
        """Quality-weighted pair count for signals whose signal_date falls in [window_start_days, window_end_days) ago."""
        window_sigs = [s for s in sigs_sorted
                       if window_end_days > days_ago(s["signal_date"]) >= window_start_days]
        total = 0.0
        for i, sig_a in enumerate(window_sigs):
            try:
                da = datetime.strptime(sig_a["signal_date"][:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            for sig_b in window_sigs[i + 1:]:
                try:
                    db = datetime.strptime(sig_b["signal_date"][:10], "%Y-%m-%d").date()
                except ValueError:
                    continue
                if db <= da:
                    continue
                gap = (db - da).days
                if gap > 30:
                    break
                layer_a, layer_b = sig_a.get("layer"), sig_b.get("layer")
                if not layer_a or not layer_b or layer_a == layer_b:
                    continue
                if (layer_a, layer_b) not in MEANINGFUL_SEQ:
                    continue
                total += sig_a.get("quality", 0) * sig_b.get("quality", 0) * math.exp(-gap / 15)
        return total

    for iso, sigs in by_iso_seq.items():
        sigs_sorted = sorted(sigs, key=lambda s: s["signal_date"])

        oldest_days = max(days_ago(s["signal_date"]) for s in sigs)
        n_prior = oldest_days // 30
        if n_prior < 3:
            continue  # not enough history for a baseline

        current_pairs = count_pairs_in_window(sigs_sorted, 0, 30)
        if current_pairs <= 0:
            continue

        prior_counts = [count_pairs_in_window(sigs_sorted, w * 30, (w + 1) * 30)
                        for w in range(1, n_prior + 1)]
        baseline_mean = statistics.mean(prior_counts)
        baseline_std = statistics.stdev(prior_counts) if len(prior_counts) >= 3 else 1.0
        seq_z = (current_pairs - baseline_mean) / max(baseline_std, 0.1)

        if seq_z < 1.5:
            continue  # not anomalous vs this country's own history

        # Find the best pair in the current window for the title/why
        current_sigs = [s for s in sigs_sorted if days_ago(s["signal_date"]) < 30]
        valid_pairs = []
        for i, sig_a in enumerate(current_sigs):
            try:
                da = datetime.strptime(sig_a["signal_date"][:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            for sig_b in current_sigs[i + 1:]:
                try:
                    db = datetime.strptime(sig_b["signal_date"][:10], "%Y-%m-%d").date()
                except ValueError:
                    continue
                if db <= da:
                    continue
                gap = (db - da).days
                if gap > 30:
                    break
                layer_a, layer_b = sig_a.get("layer"), sig_b.get("layer")
                if not layer_a or not layer_b or layer_a == layer_b:
                    continue
                if (layer_a, layer_b) not in MEANINGFUL_SEQ:
                    continue
                p = sig_a.get("profile") or sig_b.get("profile")
                ps = (p.get("score") if p else None) or 0
                w_score = sig_a.get("quality", 0) * sig_b.get("quality", 0) * math.log2(ps + 2)
                valid_pairs.append({
                    "sig_a": sig_a, "sig_b": sig_b,
                    "gap": gap, "w_score": w_score,
                    "layer_a": layer_a, "layer_b": layer_b,
                })

        if not valid_pairs:
            continue

        best = max(valid_pairs, key=lambda p: p["w_score"])
        p = sigs_sorted[0].get("profile")
        ps = (p.get("score") if p else None) or 0
        country_name = profile_name(sigs_sorted[0])
        score = seq_z * math.log2(ps + 2) * current_pairs

        involved = list({_signal_key(p["sig_a"]) for p in valid_pairs} |
                        {_signal_key(p["sig_b"]) for p in valid_pairs})
        themes.append({
            "type": "layer_sequence",
            "title": f"{country_name} \u2014 {best['layer_a']} \u2192 {best['layer_b']} sequence ({best['gap']}d gap)",
            "score": round(score, 3),
            "countries": [iso],
            "signal_keys": involved,
            "why": (
                f"{best['sig_a'].get('source')} on {best['sig_a']['signal_date']} "
                f"→ {best['sig_b'].get('source')} on {best['sig_b']['signal_date']} "
                f"({best['gap']}d). Current window {current_pairs:.2f} weighted pairs "
                f"vs {baseline_mean:.2f} baseline ({seq_z:+.1f}\u03c3)."
            ),
        })

    # ── Algorithm 4: CFTC Basket Overlap ────────────────────────────────────

    # Build iso→profile_score map once
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
        basket = set(cftc_sig.get("basket") or [])
        if not basket:
            continue

        active_isos: set = set()
        for s in non_cftc:
            try:
                sd = datetime.strptime(s["signal_date"][:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            if 0 <= (cftc_date - sd).days <= 30:
                active_isos.add(s["iso"])

        overlap = basket & active_isos
        if not overlap:
            continue

        overlap_score = sum(iso_ps.get(i, 0) for i in overlap)
        z = cftc_sig.get("z_score") or 0
        score = abs(z) * math.log2(len(overlap) + 1) * overlap_score / len(basket)

        commodity = cftc_sig.get("commodity") or "Unknown"
        themes.append({
            "type": "cftc_overlap",
            "title": f"{commodity} positioning anomaly \u2014 {len(overlap)} exposed countries active",
            "score": round(score, 3),
            "countries": sorted(overlap),
            "signal_keys": [_signal_key(cftc_sig)],
            "why": (
                f"{commodity} z={z:+.1f}\u03c3. Active basket: {', '.join(sorted(overlap))} "
                f"({len(overlap)}/{len(basket)} exposed countries have recent signals)."
            ),
        })

    themes.sort(key=lambda t: t["score"], reverse=True)

    # ── Convergence collapse ─────────────────────────────────────────────────
    # If the same single country appears in 2+ themes above the prose floor,
    # collapse them into one convergence theme. Two algorithms firing on the
    # same country independently is a stronger signal than either alone.
    PROSE_FLOOR_FOR_COLLAPSE = 15.0
    from collections import defaultdict
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
            t["type"].replace("velocity_anomaly", "velocity spike")
                     .replace("layer_sequence", "layer sequence")
                     .replace("actor_concentration", "actor concentration")
                     .replace("cftc_overlap", "CFTC overlap")
            for t in matching
        )
        country_name = profile_name(matching[0].get("countries", [iso]) and
                                    next((s for s in enriched if s.get("iso") == iso), {}) or {})
        # Fallback name from profile if available
        for t in matching:
            for sk in t.get("signal_keys", []):
                pass  # just need country_name
        # Get name from first theme title (before the em-dash)
        country_name = matching[0]["title"].split(" \u2014")[0].split(" \u2192")[0]

        why_parts = [f"[{t['type']}] {t['why']}" for t in matching]
        convergence_themes.append({
            "type": "convergence",
            "title": f"{country_name} \u2014 {type_labels}",
            "score": round(combined_score, 3),
            "countries": [iso],
            "signal_keys": combined_keys,
            "why": " | ".join(why_parts),
            "_components": matching,  # preserved for prose generation context
        })

    # Replace individual themes with convergence themes where applicable
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
    raw = f"{theme['type']}|{','.join(sorted(theme['countries']))}|{theme['title']}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


_PROSE_SYSTEM = (
    "You are an intelligence analyst writing terse feed entries for a single expert reader. "
    "Your job is to say what you think is actually happening — not just what is documented. "
    "Use the filing data as evidence. Make a call. Label speculation as such ('likely', 'suggests', 'probably'). "
    "One confident interpretation beats five caveats. "
    "Return valid JSON only with exactly these fields:\n"
    "- headline: one sentence, max 12 words, states your interpretation — not just a description\n"
    "- body: 2 sentences max, ~40 words total. Sentence 1: what the data shows. Sentence 2: what it probably means.\n"
    "- prompt: a ready-to-paste Claude prompt that acts as a rabbit hole entrance. "
    "The reader has just seen the headline and body above and knows nothing else. "
    "The prompt should ask Claude to: (1) explain who each named entity is and what they actually do, "
    "(2) describe the relationships between them and the broader context they operate in, "
    "(3) assess whether this kind of activity is routine or genuinely anomalous for these actors, "
    "and (4) help the reader decide if this is worth paying closer attention to. "
    "Write it in first person as the reader ('I just saw...'). "
    "Name every entity from the signals explicitly so Claude has full context. "
    "End with the question: is this worth paying attention to, and why or why not? Max 120 words.\n"
    "No filler. No other fields."
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
    PROSE_SCORE_FLOOR = 18.0

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

        # Build context signals: same countries, last 90 days, not already in signal_keys (up to 10)
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
                max_tokens=400,
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
            narrative = {
                "headline": parsed.get("headline", ""),
                "body":     parsed.get("body", ""),
                "prompt":   parsed.get("prompt", ""),
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
        f"(actor_concentration: {type_counts.get('actor_concentration', 0)}, "
        f"velocity: {type_counts.get('velocity_anomaly', 0)}, "
        f"sequence: {type_counts.get('layer_sequence', 0)}, "
        f"cftc: {type_counts.get('cftc_overlap', 0)})"
    )

    print("\nFirst 3 records:")
    for i, rec in enumerate(enriched[:3]):
        print(f"  [{i}] {json.dumps(rec)}")

    print(f"\nWrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
