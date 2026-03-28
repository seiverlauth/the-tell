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

1. **Map highlight color bug** — clicking a country zooms/pans the map but the active highlight (red for convergence, yellow for notable) gets overwritten by the default active color. Regression from a prior session. Fix `syncMapFilter` to preserve zone colors on active country.

2. **Signal click → feed sync** — clicking a notable signal or convergence cluster entry should scroll to and expand that signal's row in the feed. Currently filters the feed by country but doesn't jump to or expand a specific item.

3. **Source link audit** — IMF signals link to the IMF search page, not the individual document. Audit all sources: every feed item must link to the specific document/page, not a landing page. Fix `page_url` / `url` fields in fetch scripts as needed.

4. **FARA/LDA structured fields + signal quality** — prior audit found both sources are opaque to scoring/rendering because structured data is flattened into strings. Specific issues:
   - `fetch_lda.py`: `is_high_signal` OR gate too broad — "non-US client" accepts all Canadian tariff/pharma noise. HCR in HIGH_SIGNAL_CODES is wrong. Issue codes captured but not stored as a field. Lobbying firm not a separate field. XX records dropped entirely.
   - `fetch_fara.py`: Sub-state entities (Republika Srpska, DRC, Bermuda) resolve to XX and vanish from map/convergence — 30 of 114 records. `target_groups`, `registrant`, `principal` buried in title/description strings instead of separate fields.
   - Fix: store `issue_codes`, `registrant`, `principal`, `lobbying_firm`, `target_groups` as separate signal fields. Fix XX resolution for known sub-state entities. Narrow LDA filter to DEF/FOR/TRD/ENE/SCI/HOM only (drop HCR, tighten the non-US client branch).
   - Context: Republika Srpska hired 6 DC firms in ~12 months — most anomalous influence pattern in the dataset. Currently invisible.

5. **UI surfacing / callouts** — the feed is flat and doesn't explain why anything matters. High-score signals look identical to low-quality noise. Needs interpretive context pulled from profile and signal structure — "why this matters" inline, stemmed from FARA/LDA conversation. Republika Srpska hiring 6 firms should read as alarming, not identical to a SAM filing.

6. **Better feed filters** — filter by layer, source, or structural score threshold, not just by country. Low priority.

7. **SAM monitoring** — watch whether adjective-matching fix actually surfaces signals after a few CI runs. Reassess extraction approach if still empty after 2 weeks.

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
