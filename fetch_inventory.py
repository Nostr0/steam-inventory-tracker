#!/usr/bin/env python3
"""
CS:GO Inventory Value Tracker
Fetches Steam Market prices for all configured accounts, records daily
totals to values.csv and accounts.csv (overwriting same-day rows on re-runs),
and writes top items to items.json.

Requires: pip install requests
"""
import csv
import datetime as dt
import json
import re
import time
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ROOT   = Path(__file__).parent
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))

STEAM_IDS      = CONFIG.get("steam_ids", [])
CURRENCY       = CONFIG.get("currency", "EUR").upper()
ACCOUNT_LABELS = CONFIG.get("account_labels", {})
SLEEP_MS       = int(CONFIG.get("sleep_between_price_requests_ms", 4000))
DEBUG          = CONFIG.get("debug", True)
TOP_ITEMS      = int(CONFIG.get("top_items_count", 25))

CURRENCY_MAP = {
    "USD": 1,  "GBP": 2,  "EUR": 3,  "CHF": 4,  "RUB": 5,
    "BRL": 7,  "JPY": 8,  "NOK": 9,  "IDR": 10, "MYR": 11,
    "PHP": 12, "SGD": 13, "THB": 14, "VND": 15, "KRW": 16,
    "TRY": 17, "UAH": 18, "MXN": 19, "CAD": 20, "AUD": 21,
    "NZD": 22, "PLN": 23, "DKK": 24, "SEK": 25, "CNY": 27,
    "INR": 28, "CLP": 29, "PEN": 30, "COP": 31, "ZAR": 32,
    "HKD": 34, "TWD": 35,
}
CURRENCY_CODE = CURRENCY_MAP.get(CURRENCY, 3)

VALUES_CSV   = ROOT / "values.csv"
ACCOUNTS_CSV = ROOT / "accounts.csv"
ITEMS_JSON   = ROOT / "items.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[DEBUG] {msg}")

# ---------------------------------------------------------------------------
# CSV helpers — read/write as dicts, upsert by key
# ---------------------------------------------------------------------------

def read_csv_dicts(path: Path):
    """Returns (fieldnames: list[str], rows: list[dict])."""
    if not path.exists():
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def write_csv_dicts(path: Path, fieldnames: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def upsert_rows(path: Path, fieldnames: list, new_rows: list, key_fields: list) -> None:
    """
    Write new_rows into path, overwriting any existing rows whose key_fields
    match. New rows are appended at the end. Safe to call multiple times per day.
    """
    existing_fields, existing = read_csv_dicts(path)
    all_fields = existing_fields or fieldnames
    new_keys = {tuple(r[k] for k in key_fields) for r in new_rows}
    kept = [r for r in existing
            if tuple(r.get(k, "") for k in key_fields) not in new_keys]
    write_csv_dicts(path, all_fields, kept + new_rows)


def ensure_headers() -> None:
    if not VALUES_CSV.exists():
        write_csv_dicts(VALUES_CSV, ["date", "lowest", "median"], [])
    if not ACCOUNTS_CSV.exists():
        write_csv_dicts(ACCOUNTS_CSV, ["date", "steam_id", "value_eur"], [])

# ---------------------------------------------------------------------------
# HTTP with exponential backoff retry
# ---------------------------------------------------------------------------

def get_with_retry(
    url: str,
    params: Optional[dict] = None,
    max_retries: int = 6,
    base_delay: float = 2.0,
    timeout: int = 30,
) -> requests.Response:
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = base_delay * (2 ** attempt)
                dbg(f"429 rate-limited → waiting {wait:.0f}s "
                    f"(attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise
            wait = base_delay * (2 ** attempt)
            dbg(f"Request error ({exc}), retrying in {wait:.0f}s")
            time.sleep(wait)
    raise RuntimeError(f"All {max_retries} retries exhausted for {url}")

# ---------------------------------------------------------------------------
# Steam inventory — paginated
# ---------------------------------------------------------------------------

def fetch_inventory(steam_id: str) -> dict:
    """
    Returns the full CS2 inventory dict, handling Steam's 2000-item
    pagination automatically.
    """
    assets: list = []
    descriptions: list = []
    last_assetid: Optional[str] = None

    while True:
        params: dict = {"l": "english", "count": 2000}
        if last_assetid:
            params["start_assetid"] = last_assetid

        r = get_with_retry(
            f"https://steamcommunity.com/inventory/{steam_id}/730/2",
            params=params,
        )
        data = r.json()
        assets.extend(data.get("assets", []))
        descriptions.extend(data.get("descriptions", []))

        if not data.get("more_items"):
            break

        last_assetid = data.get("last_assetid")
        dbg(f"  Paginating… {len(assets)} assets fetched so far")
        time.sleep(1.5)

    return {"assets": assets, "descriptions": descriptions}


def get_marketable_counts(inv: dict) -> dict:
    """
    Returns {market_hash_name: quantity} for every marketable item.
    Non-marketable items (e.g. untradeable skins, event items) are skipped.
    """
    desc_map = {
        (str(d["classid"]), str(d.get("instanceid", "0"))): d
        for d in inv.get("descriptions", [])
    }
    counts: dict = {}
    skipped = 0
    for asset in inv.get("assets", []):
        key = (str(asset["classid"]), str(asset.get("instanceid", "0")))
        desc = desc_map.get(key)
        if not desc:
            continue
        if not desc.get("marketable", 0):
            skipped += 1
            continue
        name = desc.get("market_hash_name")
        if name:
            counts[name] = counts.get(name, 0) + 1

    if skipped:
        dbg(f"  Skipped {skipped} non-marketable item(s)")
    return counts

# ---------------------------------------------------------------------------
# Price parsing — handles EUR/USD/mixed formats
# ---------------------------------------------------------------------------

_PRICE_RE = re.compile(r"(\d[\d.,]*\d|\d)")


def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    # Normalise whitespace variants
    text = text.replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    m = _PRICE_RE.search(text)
    if not m:
        return None
    raw = m.group(1)
    # Detect European "1.234,56" vs American "1,234.56"
    if "." in raw and "," in raw:
        if raw.rindex(".") < raw.rindex(","):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        raw = raw.replace(",", ".")
    try:
        v = float(raw)
        return v if v > 0 else None
    except ValueError:
        return None


def fetch_price(name: str) -> tuple:
    """Returns (lowest: Optional[float], median: Optional[float])."""
    try:
        r = get_with_retry(
            "https://steamcommunity.com/market/priceoverview/",
            params={
                "appid": "730",
                "market_hash_name": name,
                "currency": CURRENCY_CODE,
            },
        )
        data = r.json()
        if not data.get("success"):
            dbg(f"  success=false for '{name}'")
            return None, None
        return (
            parse_price(data.get("lowest_price")),
            parse_price(data.get("median_price")),
        )
    except Exception as exc:
        dbg(f"  Price fetch failed for '{name}': {exc}")
        return None, None

# ---------------------------------------------------------------------------
# Core per-account logic
# ---------------------------------------------------------------------------

def value_for_account(steam_id: str) -> list:
    """Returns list of item dicts: {name, qty, lowest, median}."""
    label = ACCOUNT_LABELS.get(steam_id, steam_id)
    dbg(f"Fetching inventory: {label} ({steam_id})")

    inv    = fetch_inventory(steam_id)
    counts = get_marketable_counts(inv)
    total  = len(counts)
    dbg(f"  {total} unique marketable items")

    items = []
    for i, (name, qty) in enumerate(counts.items(), 1):
        lowest, median = fetch_price(name)
        items.append({"name": name, "qty": qty, "lowest": lowest, "median": median})

        lo_s  = f"{lowest:.2f}"      if lowest  is not None else "N/A"
        med_s = f"{median:.2f}"      if median  is not None else "N/A"
        tot_s = f"{lowest * qty:.2f}" if lowest is not None else "N/A"
        dbg(f"  [{i}/{total}] {name!r} ×{qty} → L:{lo_s} M:{med_s} total:{tot_s}")

        time.sleep(SLEEP_MS / 1000.0)

    return items


def sum_values(items: list) -> tuple:
    """Returns (total_lowest, total_median) as rounded floats."""
    lowest = sum((d["lowest"] or 0.0) * d["qty"] for d in items)
    median = sum((d["median"] or 0.0) * d["qty"] for d in items)
    return round(lowest, 2), round(median, 2)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    today = dt.date.today().isoformat()
    ensure_headers()

    all_items:    list = []
    acc_rows:     list = []
    total_lowest: float = 0.0
    total_median: float = 0.0

    for sid in STEAM_IDS:
        try:
            items = value_for_account(sid)
            all_items.extend(items)
            lo, med = sum_values(items)
            total_lowest += lo
            total_median += med
            acc_rows.append({"date": today, "steam_id": sid, "value_eur": round(lo, 2)})
            print(f"  {ACCOUNT_LABELS.get(sid, sid)}: "
                  f"lowest={lo} {CURRENCY}, median={med} {CURRENCY}")
        except Exception as exc:
            # Do NOT write a 0-row — skip this account for today so we don't
            # corrupt the historical record with a transient API failure.
            print(f"ERROR processing {sid}: {exc}")

    total_lowest = round(total_lowest, 2)
    total_median = round(total_median, 2)

    # --- values.csv ---
    upsert_rows(
        VALUES_CSV,
        ["date", "lowest", "median"],
        [{"date": today, "lowest": total_lowest, "median": total_median}],
        key_fields=["date"],
    )

    # --- accounts.csv ---
    if acc_rows:
        upsert_rows(
            ACCOUNTS_CSV,
            ["date", "steam_id", "value_eur"],
            acc_rows,
            key_fields=["date", "steam_id"],
        )

    # --- items.json: aggregate across all accounts, sort by total value ---
    merged: dict = {}
    for d in all_items:
        n = d["name"]
        if n not in merged:
            merged[n] = {
                "name": n, "qty": 0,
                "lowest": d["lowest"], "median": d["median"],
            }
        else:
            # Keep the freshest non-None price
            if d["lowest"]  is not None: merged[n]["lowest"]  = d["lowest"]
            if d["median"]  is not None: merged[n]["median"]  = d["median"]
        merged[n]["qty"] += d["qty"]

    sorted_items = sorted(
        merged.values(),
        key=lambda x: (x["lowest"] or 0) * x["qty"],
        reverse=True,
    )

    ITEMS_JSON.write_text(
        json.dumps(
            {
                "date":     today,
                "currency": CURRENCY,
                "items": [
                    {
                        "name":         it["name"],
                        "qty":          it["qty"],
                        "lowest":       it["lowest"],
                        "median":       it["median"],
                        "total_lowest": round((it["lowest"] or 0) * it["qty"], 2),
                        "total_median": round((it["median"] or 0) * it["qty"], 2),
                    }
                    for it in sorted_items[:TOP_ITEMS]
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"\n✓ {today}: "
          f"lowest={total_lowest} {CURRENCY}, median={total_median} {CURRENCY}")


if __name__ == "__main__":
    main()