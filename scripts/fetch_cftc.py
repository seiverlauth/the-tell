#!/usr/bin/env python3
"""
fetch_cftc.py — CFTC COT anomaly pipeline for HARPY

Weekly Commitments of Traders (COT) report, disaggregated futures and
Traders in Financial Futures (TFF). Emits one signal per commodity when
managed money net positioning crosses ±2σ vs the 52-week rolling baseline.

Schema extras: z_score, basket (list of ISOs), commodity, net_position
iso = basket country with highest structural profile score
value_usd = null (positioning data, not a notional dollar amount)

Data sources (CFTC public ZIPs):
  Disaggregated futures: https://www.cftc.gov/dea/newcot/f_disagg_fut.zip
  TFF futures:           https://www.cftc.gov/dea/newcot/f_fin_fut.zip
  Historical (by year):  .../HistoricalCompressed/fut_{disagg|fin}_txt_{year}.zip
"""

from __future__ import annotations

import csv
import io
import re
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path
from statistics import mean, stdev

sys.path.insert(0, str(Path(__file__).parent))
from utils import append_and_write, profile_score, write_error

try:
    from curl_cffi import requests as cffi_requests
    _HAS_CFFI = True
except ImportError:
    _HAS_CFFI = False

try:
    import requests as _stdlib_requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

SOURCE   = "cftc"
OUT_PATH = Path(__file__).parent.parent / "data" / "cftc_signals.json"

Z_THRESHOLD  = 2.0   # |z| ≥ this → signal
MIN_HISTORY  = 12    # minimum weeks required to compute a meaningful z-score
WINDOW_WEEKS = 52    # rolling window for baseline mean/σ

# ---------------------------------------------------------------------------
# Commodity definitions
# report:  "disaggregated" (physical) | "tff" (financial/FX)
# pattern: regex matched against Market_and_Exchange_Names in CFTC CSV
# basket:  ISO2 codes for countries directly exposed to this commodity
# ---------------------------------------------------------------------------

COMMODITIES: list[dict] = [
    {
        "name": "WTI Crude",
        "report": "disaggregated",
        # WTI-PHYSICAL is the main NYMEX CL contract (managed money data)
        "pattern": re.compile(r"WTI-PHYSICAL|WTI FINANCIAL CRUDE OIL", re.I),
        "basket": ["SA", "RU", "IQ", "IR", "AE", "NG", "VE"],
    },
    {
        "name": "Brent Crude",
        "report": "disaggregated",
        # BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE
        "pattern": re.compile(r"BRENT LAST DAY", re.I),
        "basket": ["SA", "RU", "IQ", "IR", "NO", "AE", "NG"],
    },
    {
        "name": "Natural Gas",
        "report": "disaggregated",
        # HENRY HUB - NEW YORK MERCANTILE EXCHANGE (main NG contract)
        "pattern": re.compile(r"^HENRY HUB - NEW YORK MERCANTILE EXCHANGE$", re.I),
        "basket": ["RU", "QA", "AU", "NG", "DZ"],
    },
    {
        "name": "Heating Oil",
        "report": "disaggregated",
        # NY HARBOR ULSD (former Heating Oil #2) - NEW YORK MERCANTILE EXCHANGE
        "pattern": re.compile(r"NY HARBOR ULSD", re.I),
        "basket": ["SA", "RU", "IQ", "IR", "VE"],
    },
    {
        "name": "Gold",
        "report": "disaggregated",
        # GOLD - COMMODITY EXCHANGE INC. (COMEX gold)
        "pattern": re.compile(r"^GOLD - COMMODITY EXCHANGE INC\.", re.I),
        "basket": ["CN", "RU", "AU", "ZA", "IN"],
    },
    {
        "name": "Copper",
        "report": "disaggregated",
        # COPPER- #1 - COMMODITY EXCHANGE INC.
        "pattern": re.compile(r"^COPPER.*COMMODITY EXCHANGE", re.I),
        "basket": ["CL", "PE", "CN", "CD", "ZM"],
    },
    {
        "name": "Palladium",
        "report": "disaggregated",
        # PALLADIUM - NEW YORK MERCANTILE EXCHANGE
        "pattern": re.compile(r"^PALLADIUM - NEW YORK MERCANTILE EXCHANGE$", re.I),
        "basket": ["RU", "ZA", "CN"],
    },
    {
        "name": "Wheat",
        "report": "disaggregated",
        # WHEAT-SRW - CHICAGO BOARD OF TRADE (primary Chicago SRW wheat)
        "pattern": re.compile(r"^WHEAT-SRW - CHICAGO BOARD OF TRADE$", re.I),
        "basket": ["RU", "UA", "US", "AU", "CA", "IN"],
    },
    {
        "name": "Corn",
        "report": "disaggregated",
        # CORN - CHICAGO BOARD OF TRADE
        "pattern": re.compile(r"^CORN - CHICAGO BOARD OF TRADE$", re.I),
        "basket": ["US", "UA", "BR", "AR", "CN"],
    },
    {
        "name": "Soybeans",
        "report": "disaggregated",
        # SOYBEANS - CHICAGO BOARD OF TRADE
        "pattern": re.compile(r"^SOYBEANS - CHICAGO BOARD OF TRADE$", re.I),
        "basket": ["US", "BR", "AR", "CN"],
    },
    {
        "name": "RUB Futures",
        "report": "tff",
        # Russian Ruble futures — suspended by CME post-2022 sanctions; kept for future resumption
        "pattern": re.compile(r"russian ruble|ruble", re.I),
        "basket": ["RU"],
    },
    {
        "name": "CNH Futures",
        "report": "tff",
        # Offshore Chinese Yuan futures on CME
        "pattern": re.compile(r"chinese renminbi|yuan.*chicago|cnh", re.I),
        "basket": ["CN"],
    },
    {
        "name": "Cocoa",
        "report": "disaggregated",
        # COCOA - ICE FUTURES U.S.
        "pattern": re.compile(r"^COCOA - ICE FUTURES U\.S\.$", re.I),
        "basket": ["CI", "GH", "CM", "NG"],
    },
]

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

_COT_BASE = "https://www.cftc.gov/sites/default/files/files/dea/history"

def _url_for_year(report: str, year: int) -> str:
    """Year-based COT data URL (works for all years including current)."""
    tag = "disagg" if report == "disaggregated" else "fin"
    return f"{_COT_BASE}/fut_{tag}_txt_{year}.zip"

# ---------------------------------------------------------------------------
# HTTP
# CFTC uses Cloudflare bot protection that blocks Python HTTP clients.
# curl_cffi is tried first; if it gets a non-200, we fall back to subprocess
# curl which consistently returns 200 for static CFTC zip files.
# ---------------------------------------------------------------------------

import subprocess as _subprocess

def _fetch(url: str, timeout: int = 90) -> bytes:
    # Try curl_cffi with browser impersonation
    if _HAS_CFFI:
        try:
            sess = cffi_requests.Session()
            resp = sess.get(url, impersonate="chrome120", timeout=timeout)
            if resp.status_code == 200:
                return resp.content
        except Exception:
            pass

    # Fall back to subprocess curl — bypasses Cloudflare bot detection
    result = _subprocess.run(
        ["curl", "-sL", "--max-time", str(timeout), "--fail", "-o", "-", url],
        capture_output=True, timeout=timeout + 10,
    )
    if result.returncode != 0:
        raise IOError(
            f"curl exited {result.returncode}: {result.stderr.decode(errors='replace')[:200]}"
        )
    if len(result.stdout) < 512:
        raise IOError(f"Response too small ({len(result.stdout)} bytes) for {url}")
    return result.stdout

# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def _parse_zip_csv(content: bytes) -> list[dict]:
    """Unzip and parse the first .txt/.csv found; strips all field-name whitespace."""
    zf   = zipfile.ZipFile(io.BytesIO(content))
    name = next(
        n for n in zf.namelist()
        if n.lower().endswith((".txt", ".csv")) and not n.endswith("/")
    )
    text   = zf.read(name).decode("latin-1")
    reader = csv.DictReader(io.StringIO(text))
    rows   = []
    for row in reader:
        # CFTC files occasionally pad column names with whitespace
        clean = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
        rows.append(clean)
    return rows


def _parse_date(row: dict) -> str | None:
    """
    Return ISO date string from a CFTC COT row.
    Current CFTC format: Report_Date_as_YYYY-MM-DD (already ISO).
    Fallback: As_of_Date_In_Form_YYMMDD (YYMMDD).
    """
    # Primary: ISO date column (current CFTC format)
    raw = row.get("Report_Date_as_YYYY-MM-DD", "").strip()
    if raw and len(raw) == 10:
        return raw   # already YYYY-MM-DD

    # Fallback: YYMMDD  (older files / legacy format)
    raw = row.get("As_of_Date_In_Form_YYMMDD", "").strip()
    if raw and len(raw) == 6:
        try:
            yy, mm, dd = int(raw[:2]), raw[2:4], raw[4:6]
            year = 2000 + yy if yy < 50 else 1900 + yy
            return f"{year}-{mm}-{dd}"
        except ValueError:
            pass

    # Legacy MM/DD/YYYY column name variant
    raw = row.get("Report_Date_as_MM_DD_YYYY", "").strip()
    if raw:
        try:
            return datetime.strptime(raw, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def _net_mm(row: dict) -> int | None:
    """
    Managed money net position (long − short). Returns None if missing.

    Disaggregated COT:  M_Money_Positions_Long_All  / _Short_All
    TFF report:         Lev_Money_Positions_Long_All / _Short_All
                        (Leveraged Funds = speculative proxy for managed money)
    Legacy fallback:    *_ALL uppercase column names (older CFTC files)
    """
    def _val(col: str) -> str:
        return row.get(col, "").replace(",", "").strip()

    long_: str = (
        _val("M_Money_Positions_Long_All")
        or _val("Lev_Money_Positions_Long_All")
        or _val("M_Money_Positions_Long_ALL")
    )
    short_: str = (
        _val("M_Money_Positions_Short_All")
        or _val("Lev_Money_Positions_Short_All")
        or _val("M_Money_Positions_Short_ALL")
    )

    if not long_ or not short_ or long_ == "." or short_ == ".":
        return None
    try:
        return int(float(long_)) - int(float(short_))
    except (ValueError, TypeError):
        return None

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_report(report: str, today: date) -> list[dict]:
    """
    Download previous year + current year COT data.
    Both use the year-based URL (current year file is updated throughout the year).
    Returns all rows combined; gracefully skips failures.
    """
    rows: list[dict] = []

    for year in (today.year - 1, today.year):
        url = _url_for_year(report, year)
        try:
            print(f"  Fetching {report} {year} ...")
            fetched = _parse_zip_csv(_fetch(url))
            rows.extend(fetched)
            print(f"    +{len(fetched)} rows")
        except Exception as exc:
            print(f"    WARN: {report} {year} fetch failed: {exc}")

    return rows

# ---------------------------------------------------------------------------
# Index building
# ---------------------------------------------------------------------------

def index_by_market(rows: list[dict]) -> dict[str, list[tuple[str, int]]]:
    """
    Returns {market_name: [(date_iso, net_mm), ...]} sorted ascending by date.
    Rows with missing date or net position are skipped.
    """
    idx: dict[str, list] = {}
    for row in rows:
        market = row.get("Market_and_Exchange_Names", "").strip()
        if not market:
            continue
        dt  = _parse_date(row)
        net = _net_mm(row)
        if dt is None or net is None:
            continue
        idx.setdefault(market, []).append((dt, net))

    for k in idx:
        # Deduplicate same-date entries (keep last seen = most recent file)
        seen: dict[str, int] = {}
        for dt, net in idx[k]:
            seen[dt] = net
        idx[k] = sorted(seen.items())   # [(date_iso, net), ...] asc

    return idx

# ---------------------------------------------------------------------------
# Z-score
# ---------------------------------------------------------------------------

def compute_z(series: list[tuple[str, int]]) -> tuple[str | None, int | None, float | None]:
    """
    Given [(date_iso, net), ...] sorted ascending,
    return (latest_date, latest_net, z_score) using WINDOW_WEEKS rolling window.
    Returns (None, None, None) if insufficient history.
    """
    if len(series) < MIN_HISTORY:
        return None, None, None

    window   = series[-WINDOW_WEEKS:]
    ld, ln   = window[-1]
    nets     = [n for _, n in window]

    if len(nets) < 2:
        return ld, ln, None
    mu    = mean(nets)
    sigma = stdev(nets)
    if sigma == 0:
        return ld, ln, None

    return ld, ln, round((ln - mu) / sigma, 3)

# ---------------------------------------------------------------------------
# Signal assembly
# ---------------------------------------------------------------------------

_SKIP_ISO = {"US", "XX", None}

def _best_iso(basket: list[str]) -> str | None:
    """
    Return the highest-scoring non-US/XX ISO in basket.
    US and XX are excluded because build_signals.py drops them anyway,
    and they have no geopolitical signal value in this context.
    """
    best_iso_  = None
    best_score = -1.0
    for iso in basket:
        if iso in _SKIP_ISO:
            continue
        s = profile_score(iso)
        if s is not None and s > best_score:
            best_score = s
            best_iso_  = iso
    return best_iso_


def _build_signal(
    *,
    name: str,
    iso: str | None,
    basket: list[str],
    report_date: str,
    net_position: int,
    z_score: float,
) -> dict:
    direction = "long" if net_position > 0 else "short"
    z_abs     = abs(z_score)
    title = (
        f"{name} — managed money net {direction} anomaly "
        f"(z={z_score:+.1f})"
    )
    desc = (
        f"CFTC COT: managed money net {direction} "
        f"{abs(net_position):,} contracts. "
        f"52-week z-score {z_score:+.2f} ({z_abs:.1f}σ "
        f"{'above' if z_score > 0 else 'below'} baseline). "
        f"Basket: {', '.join(basket)}."
    )
    return {
        "iso":          iso,
        "source":       SOURCE,
        "signal_date":  report_date,
        "title":        title,
        "value_usd":    None,
        "description":  desc,
        "z_score":      z_score,
        "basket":       basket,
        "commodity":    name,
        "net_position": net_position,
    }


def _dedup_key(sig: dict):
    return f"{sig.get('commodity')}|{sig.get('signal_date')}"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    today = date.today()
    print(f"fetch_cftc.py — {today.isoformat()}")
    print(f"  Threshold: |z| ≥ {Z_THRESHOLD}   Window: {WINDOW_WEEKS} weeks   Min history: {MIN_HISTORY} weeks")

    disagg_commodities = [c for c in COMMODITIES if c["report"] == "disaggregated"]
    tff_commodities    = [c for c in COMMODITIES if c["report"] == "tff"]

    disagg_rows: list[dict] = []
    tff_rows:    list[dict] = []

    if disagg_commodities:
        disagg_rows = load_report("disaggregated", today)
    if tff_commodities:
        tff_rows = load_report("tff", today)

    if not disagg_rows and not tff_rows:
        write_error(OUT_PATH, SOURCE, "No COT data fetched")
        print("  ERROR: no data loaded — exiting")
        return

    disagg_idx = index_by_market(disagg_rows)
    tff_idx    = index_by_market(tff_rows)
    print(
        f"  Indexed {len(disagg_idx)} disaggregated markets, "
        f"{len(tff_idx)} TFF markets"
    )

    signals: list[dict] = []

    for comm in COMMODITIES:
        idx     = disagg_idx if comm["report"] == "disaggregated" else tff_idx
        pattern = comm["pattern"]
        name    = comm["name"]

        matches = [k for k in idx if pattern.search(k)]
        if not matches:
            print(f"  [{name}] no market match")
            continue

        # Prefer the market with the most data (avoids mini-contract noise)
        market = max(matches, key=lambda k: len(idx[k]))
        if len(matches) > 1:
            print(f"  [{name}] {len(matches)} matches → '{market}'")

        series = idx[market]
        ld, ln, z = compute_z(series)

        if ld is None:
            print(f"  [{name}] insufficient history ({len(series)} weeks)")
            continue

        z_str = f"{z:+.2f}" if z is not None else "n/a"
        print(f"  [{name}] {ld}  net={ln:+,}  z={z_str}  ({len(series)} weeks)")

        if z is None or abs(z) < Z_THRESHOLD:
            continue

        iso = _best_iso(comm["basket"])
        sig = _build_signal(
            name=name,
            iso=iso,
            basket=comm["basket"],
            report_date=ld,
            net_position=ln,
            z_score=z,
        )
        signals.append(sig)
        print(f"    *** SIGNAL → {sig['title']}")

    added = append_and_write(OUT_PATH, SOURCE, signals, _dedup_key)
    print(f"\n  Signals this run: {len(signals)}  new (added): {added}")
    print(f"  Output: {OUT_PATH.name}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        write_error(OUT_PATH, SOURCE, str(exc))
        traceback.print_exc()
        sys.exit(1)
