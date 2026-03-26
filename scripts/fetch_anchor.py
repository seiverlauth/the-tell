#!/usr/bin/env python3
"""
fetch_anchor.py — Israeli defense budget modifications pipeline for HARPY

Source: OpenBudget API (next.obudget.org) — aggregates from official Israeli
        government budget change filings approved by the Knesset Finance Committee.

Filters to changes affecting budget code 0031 (Ministry of Defense).
Each record represents a formal budget modification — money moved in or out of
the defense budget, approved by the Knesset Finance Committee. These appear in
the database when approved, before any mainstream coverage.

No API key required.

Usage:
  python fetch_israel.py             # last 45 days
  python fetch_israel.py --backfill  # last 365 days
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OBUDGET_API        = "https://next.obudget.org/api/query"
LOOKBACK_DAYS      = 45
LOOKBACK_DAYS_BACKFILL = 365
PAGE_SIZE          = 50
MAX_PAGES          = 20          # 1000 records max
REQUEST_DELAY      = 1.0         # seconds between requests
ILS_TO_USD         = 1 / 3.7    # approximate NIS → USD conversion rate

# Budget codes to track (text LIKE match against budget_code_title array)
# 0031 = Ministry of Defense
# 0010 = PM's Office (includes National Security Staff 00105101)
DEFENSE_CODE_PATTERNS = ["0031", "00105101"]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "anchor_signals.json"
IL_PROFILE   = REPO_ROOT / "data" / "profiles" / "IL.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_il_score() -> float:
    if IL_PROFILE.exists():
        try:
            return float(json.loads(IL_PROFILE.read_text()).get("structural_interest_score", 8))
        except Exception:
            pass
    return 8.0


def load_existing(path: Path):
    if path.exists():
        try:
            data = json.loads(path.read_text())
            signals = data.get("signals", [])
            seen = {s["transaction_id"] for s in signals if "transaction_id" in s}
            return signals, seen
        except Exception:
            pass
    return [], set()


def api_query(sql: str) -> dict:
    params = urllib.parse.urlencode({"query": sql})
    url = f"{OBUDGET_API}?{params}"
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "harpy/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def to_usd(ils):
    if ils is None:
        return None
    try:
        v = float(ils)
    except (TypeError, ValueError):
        return None
    if v == 0:
        return None
    return round(abs(v) * ILS_TO_USD, 2)


def pick_date(row: dict):
    dates = row.get("date") or []
    if dates:
        return str(dates[0])[:10]
    return None


def is_defense_related(row: dict) -> bool:
    """True if any budget code in the change touches the MoD or NSS."""
    codes_str = json.dumps(row.get("budget_code_title") or [])
    return any(p in codes_str for p in DEFENSE_CODE_PATTERNS)


def extract_defense_items(row: dict) -> list:
    """Return change_list entries that touch defense budget codes."""
    items = []
    for cl in (row.get("change_list") or []):
        code_title = cl.get("budget_code_title") or ""
        if any(p in code_title for p in DEFENSE_CODE_PATTERNS):
            items.append(cl)
    return items


def make_title(row: dict, defense_items: list) -> str:
    change_type = ", ".join(row.get("change_title") or []) or "budget modification"
    # Use the most significant defense item title
    if defense_items:
        item_title = defense_items[0].get("budget_code_title", "").split(":")[-1].strip()
    else:
        # Fall back to any defense code in budget_code_title
        codes = row.get("budget_code_title") or []
        item_title = next(
            (c.split(":")[-1].strip() for c in codes if any(p in c for p in DEFENSE_CODE_PATTERNS)),
            "defense budget"
        )
    return f"IL — {item_title}: {change_type}"


def make_description(row: dict, defense_items: list) -> str:
    parts = []
    # AI explanation for defense-specific items
    for item in defense_items[:2]:
        expl = (item.get("ai_change_explanation") or "").strip()
        if expl:
            parts.append(expl)
    # Fallback to full explanation
    if not parts:
        expl = (row.get("explanation") or "").strip()
        if expl:
            parts.append(expl[:300])
    # Net expense diff for defense codes
    for item in defense_items[:1]:
        diff = item.get("net_expense_diff")
        if diff:
            sign = "+" if diff > 0 else ""
            ils_m = round(diff / 1_000_000, 1)
            parts.append(f"Net expense diff: {sign}{ils_m}M ILS")
    # Committee approval
    committee_type = ", ".join(row.get("change_type_name") or [])
    if committee_type:
        parts.append(f"Approved by: {committee_type}")
    return " | ".join(parts) if parts else None


def signal_from_row(row: dict, il_score: float) -> dict:
    defense_items = extract_defense_items(row)
    sig_date = pick_date(row)
    value_usd = to_usd(row.get("amount"))

    return {
        "iso": "IL",
        "source": "anchor_budget",
        "signal_date": sig_date,
        "title": make_title(row, defense_items),
        "value_usd": value_usd,
        "description": make_description(row, defense_items),
        "raw_score": il_score,
        "weight": 1.0,
        "transaction_id": row.get("transaction_id"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HARPY anchor defense budget change scraper")
    parser.add_argument("--backfill", action="store_true", help="Extend lookback to 365 days")
    args = parser.parse_args()

    lookback = LOOKBACK_DAYS_BACKFILL if args.backfill else LOOKBACK_DAYS
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=lookback)
    cutoff_str = cutoff_dt.strftime("%Y-%m-%d")
    print(f"[anchor] lookback={lookback}d  cutoff={cutoff_str}")

    il_score = load_il_score()
    existing_signals, seen_ids = load_existing(SIGNALS_PATH)
    print(f"[anchor] existing records: {len(existing_signals)}, seen ids: {len(seen_ids)}")

    new_signals = []
    offset = 0
    done = False

    for page in range(MAX_PAGES):
        if done:
            break

        # Query for any budget change touching 0031 (MoD) or 00105101 (NSS)
        # Use OR conditions since we can't easily query array contains in this API
        sql = (
            f"SELECT * FROM budget_changes "
            f"WHERE (budget_code_title::text LIKE '%0031%' "
            f"   OR budget_code_title::text LIKE '%00105101%') "
            f"ORDER BY date DESC "
            f"LIMIT {PAGE_SIZE} OFFSET {offset}"
        )

        try:
            data = api_query(sql)
        except Exception as e:
            print(f"[anchor] ERROR on page {page}: {e}", file=sys.stderr)
            break

        if not data.get("success"):
            print(f"[anchor] API error: {data.get('error')}", file=sys.stderr)
            break

        rows = data.get("rows", [])
        print(f"[anchor] page {page}  offset={offset}  rows={len(rows)}")

        if not rows:
            break

        for row in rows:
            sig_date = pick_date(row)
            if not sig_date:
                continue

            # Stop once we pass the lookback window
            if sig_date < cutoff_str:
                done = True
                break

            txn_id = row.get("transaction_id")
            if txn_id and txn_id in seen_ids:
                continue

            if not is_defense_related(row):
                continue

            sig = signal_from_row(row, il_score)
            if sig.get("signal_date"):
                new_signals.append(sig)
                if txn_id:
                    seen_ids.add(txn_id)

        offset += PAGE_SIZE
        if len(rows) < PAGE_SIZE:
            break

        time.sleep(REQUEST_DELAY)

    print(f"[anchor] new signals: {len(new_signals)}")

    if not new_signals:
        print("[anchor] nothing new — exiting")
        return

    all_signals = existing_signals + new_signals
    SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SIGNALS_PATH.write_text(json.dumps({"signals": all_signals}, indent=2, ensure_ascii=False))
    print(f"[anchor] wrote {len(all_signals)} total records to {SIGNALS_PATH}")


if __name__ == "__main__":
    main()
