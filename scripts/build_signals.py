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
import math
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    load_profile, SOURCE_LAYER_MAP, source_quality, dollar_modifier, FR_POLICY_RE,
)

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "signals.json")

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
        if len(registrants) < 2:
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

    # LDA: group by iso
    lda_sigs = [s for s in enriched if s.get("source") == "lda"
                and s.get("iso") not in (None, "XX", "US")]
    by_iso_lda: dict = {}
    for s in lda_sigs:
        by_iso_lda.setdefault(s["iso"], []).append(s)

    for iso, sigs in by_iso_lda.items():
        firms = {s.get("lobbying_firm") for s in sigs if s.get("lobbying_firm")}
        if len(firms) < 2:
            continue
        ps = prof_score(sigs[0])
        most_recent_days = min((days_ago(s.get("signal_date")) for s in sigs), default=9999)
        recency_w = math.exp(-most_recent_days / 60)
        firm_count = len(firms)
        score = math.log2(firm_count + 1) * (ps + 1) * recency_w * 0.5
        if score <= 1.0:
            continue
        country_name = profile_name(sigs[0])
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
            "title": f"{firm_count} firms lobbying on {country_name} issues",
            "score": round(score, 3),
            "countries": [iso],
            "signal_keys": [_signal_key(s) for s in sigs],
            "why": f"{firm_count} distinct lobbying firms on {iso} in {window_days}d.",
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

        current_count = sum(1 for s in sigs if days_ago(s["signal_date"]) < 30)
        if current_count < 3:
            continue

        prior_counts = []
        for w in range(1, n_prior + 1):
            c = sum(1 for s in sigs if w * 30 <= days_ago(s["signal_date"]) < (w + 1) * 30)
            prior_counts.append(c)

        baseline_mean = statistics.mean(prior_counts)
        baseline_std = statistics.stdev(prior_counts) if len(prior_counts) >= 3 else 1.0
        velocity_z = (current_count - baseline_mean) / max(baseline_std, 0.5)

        if velocity_z < 2.0:
            continue

        ps = prof_score(sigs[0])
        country_name = profile_name(sigs[0])
        score = velocity_z * math.log2(ps + 2) * current_count
        current_sigs = [s for s in sigs if days_ago(s["signal_date"]) < 30]

        themes.append({
            "type": "velocity_anomaly",
            "title": f"{country_name} — {current_count} signals in 30 days ({velocity_z:+.1f}\u03c3)",
            "score": round(score, 3),
            "countries": [iso],
            "signal_keys": [_signal_key(s) for s in current_sigs],
            "why": (
                f"{current_count} signals in 30 days vs {baseline_mean:.1f} avg "
                f"({baseline_std:.1f}\u03c3 baseline, {len(prior_counts)} windows)"
            ),
        })

    # ── Algorithm 3: Layer Sequence Detection ────────────────────────────────

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

    for iso, sigs in by_iso_seq.items():
        sigs_sorted = sorted(sigs, key=lambda s: s["signal_date"])
        valid_pairs = []
        country_score = 0.0

        for i, sig_a in enumerate(sigs_sorted):
            da_str = sig_a["signal_date"]
            try:
                da = datetime.strptime(da_str[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            for sig_b in sigs_sorted[i + 1:]:
                db_str = sig_b["signal_date"]
                try:
                    db = datetime.strptime(db_str[:10], "%Y-%m-%d").date()
                except ValueError:
                    continue
                if db <= da:
                    continue
                gap = (db - da).days
                if gap > 90:
                    break  # sorted, so all further sig_b are also too far
                layer_a, layer_b = sig_a.get("layer"), sig_b.get("layer")
                if not layer_a or not layer_b or layer_a == layer_b:
                    continue
                if (layer_a, layer_b) not in MEANINGFUL_SEQ:
                    continue
                pair_score = sig_a.get("quality", 0) * sig_b.get("quality", 0) * math.exp(-gap / 30)
                p = sig_a.get("profile") or sig_b.get("profile")
                ps = (p.get("score") if p else None) or 0
                w_score = pair_score * math.log2(ps + 2)
                valid_pairs.append({
                    "sig_a": sig_a, "sig_b": sig_b,
                    "gap": gap, "pair_score": pair_score, "w_score": w_score,
                    "layer_a": layer_a, "layer_b": layer_b,
                })
                country_score += pair_score

        if country_score <= 0.3 or not valid_pairs:
            continue

        best = max(valid_pairs, key=lambda p: p["w_score"])
        p = sigs_sorted[0].get("profile")
        ps = (p.get("score") if p else None) or 0
        country_name = profile_name(sigs_sorted[0])
        score = country_score * math.log2(ps + 2)

        involved = list({_signal_key(p["sig_a"]) for p in valid_pairs} |
                        {_signal_key(p["sig_b"]) for p in valid_pairs})
        themes.append({
            "type": "layer_sequence",
            "title": f"{country_name} \u2014 {best['layer_a']} \u2192 {best['layer_b']} sequence ({best['gap']}d gap)",
            "score": round(score, 3),
            "countries": [iso],
            "signal_keys": involved,
            "why": (
                f"{best['sig_a'].get('source')} filing on {best['sig_a']['signal_date']} "
                f"preceded {best['sig_b'].get('source')} on {best['sig_b']['signal_date']} "
                f"by {best['gap']}d."
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
    return themes[:20]


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
    from collections import Counter
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
