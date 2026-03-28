# ROADMAP.md — HARPY
_Last updated: 2026-03-28 (quality scores done)_

One thing at a time. Update this file when a task completes, a new idea gets approved, or priorities shift. Nothing moves to Done without a commit and push.

---

## Now

- **fetch_cftc.py** — CFTC COT anomaly pipeline, financial layer
  - Weekly COT report, 52-week z-score baseline
  - One signal per commodity when managed money positioning is anomalous
  - Schema extras: `z_score`, `basket` (list of associated ISOs)
  - `iso` = highest-scoring country in basket, `value_usd` = null
  - Commodities: WTI Crude, Brent, Natural Gas, Heating Oil, Gold, Copper, Palladium, Wheat, Corn, Soybeans, RUB futures, CNH futures, Cocoa

---

## Next (approved, in order)

1. **Feed UI rebuild** — `index.html` primary view should be the feed, not the map. Ranked by recency × structural score. Map stays but secondary.
2. **SAM monitoring** — watch whether adjective-matching fix actually surfaces signals after a few CI runs. Reassess extraction approach if still empty after 2 weeks.

---

## Ideas (not yet approved)

- Congress.gov hearing schedules — institutional attention signal, legislative layer
- USASpending modifications — drawdowns and terminations that SAM misses
- SIPRI / ACLED / zakupki.gov.ru — non-US wavelengths, adversarial layer
- ADS-B / AIS / Sentinel-2 — physical world signals (complex, post-admin-stack)
- TIC / EDGAR / CDS spreads — financial layer additions (complex)
- dsca_nato.json — NATO/NSPA collective notifications not yet in pipeline

---

## Done

- **Quality score corrections** — `utils.py`: anchor_budget 1.0→0.75, bis 0.85, cftc 0.55, imf 0.5 explicit

- `scripts/utils.py` — canonical shared utilities, all fetch scripts import from here
- All fetch scripts refactored to use utils (removed ~300 lines of duplicated maps/helpers)
- `scripts/build_signals.py` — enriches every signal at build time: profile, layer, quality, dollar_mod, is_fr_policy; strips raw_score/weight
- `index.html` refactor — removed 144+ HTTP profile fetches, precomputed fields wired, dead JS removed (computePercentileLift, weeklyLayerCounts, allScored, baselineStd, SOURCE_LAYER_MAP, sigQuality, dollarModifier, FR_POLICY_RE)
- Layer-based colors (`LAYER_COLORS` / `layerColor`) replacing per-source color map
- `scripts/fetch_bis.py` — BIS Entity List diff pipeline
- `scripts/fetch_imf.py` — IMF program monitoring pipeline
- `scripts/fetch_ofac.py` — OFAC SDN diff pipeline
- `scripts/fetch_fara.py` — FARA foreign agent registrations
- `scripts/fetch_lda.py` — LDA lobbying disclosures
- `scripts/fetch_federalregister.py` — Federal Register policy actions
- `scripts/fetch_anchor.py` — Elbit Systems SEC 6-K
- `data/dsca_signals.json` — backfilled profile scores for all 1098 records
- SAM fix — append_and_write so signals accumulate, adjective-based country extraction
