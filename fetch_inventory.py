#!/usr/bin/env python3
"""
CS2 Inventory Value Tracker

Fetches Steam inventory data via Steam Web API:
    IEconService/GetInventoryItemsWithDescriptions/v1

Then:
- counts marketable CS2 items correctly using asset["amount"]
- deduplicates price requests across all configured accounts
- caches Steam Market price lookups
- records daily totals to values.csv
- records per-account values to accounts.csv
- writes top items to items.json

Requires:
    pip install requests

Recommended:
    Set STEAM_API_KEY as an environment variable.
"""

import csv
import datetime as dt
import json
import os
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))

STEAM_IDS = [str(sid) for sid in CONFIG.get("steam_ids", [])]
CURRENCY = CONFIG.get("currency", "EUR").upper()
ACCOUNT_LABELS = {str(k): str(v) for k, v in CONFIG.get("account_labels", {}).items()}

# Prefer GitHub Actions / local env var. Do not commit your API key.
STEAM_API_KEY = os.environ.get("STEAM_API_KEY") or CONFIG.get("steam_api_key")

APPID = int(CONFIG.get("appid", 730))          # CS2
CONTEXTID = int(CONFIG.get("contextid", 2))    # CS2 inventory context
LANGUAGE = CONFIG.get("language", "english")

SLEEP_MS = int(CONFIG.get("sleep_between_price_requests_ms", 4000))
DEBUG = bool(CONFIG.get("debug", True))
TOP_ITEMS = int(CONFIG.get("top_items_count", 25))

INVENTORY_PAGE_SIZE = int(CONFIG.get("inventory_page_size", 5000))

# Price cache prevents repeated same-day / same-run requests.
PRICE_CACHE_TTL_HOURS = float(CONFIG.get("price_cache_ttl_hours", 12))

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

VALUES_CSV = ROOT / "values.csv"
ACCOUNTS_CSV = ROOT / "accounts.csv"
ITEMS_JSON = ROOT / "items.json"
PRICE_CACHE_JSON = ROOT / "price_cache.json"

ACCOUNT_VALUE_FIELD = f"value_{CURRENCY.lower()}"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "cs2-inventory-value-tracker/2.0"
})

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[DEBUG] {msg}")

# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_boolish(value: Any) -> bool:
    return value in (1, "1", True, "true", "True", "yes", "Yes")


def parse_amount(value: Any) -> int:
    try:
        amount = int(value)
        return amount if amount > 0 else 1
    except (TypeError, ValueError):
        return 1


# ---------------------------------------------------------------------------
# CSV helpers — read/write as dicts, upsert by key
# ---------------------------------------------------------------------------

def read_csv_dicts(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Returns (fieldnames, rows)."""
    if not path.exists():
        return [], []

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def write_csv_dicts(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    """Atomically write CSV by staging to a .tmp file first."""
    tmp = path.with_suffix(".tmp")

    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    tmp.replace(path)


def upsert_rows(
    path: Path,
    fieldnames: list[str],
    new_rows: list[dict[str, Any]],
    key_fields: list[str],
) -> None:
    """
    Write new_rows into path, overwriting existing rows whose key_fields match.
    New rows are appended at the end.
    """
    existing_fields, existing = read_csv_dicts(path)

    all_fields = list(dict.fromkeys((existing_fields or fieldnames) + fieldnames))

    new_keys = {
        tuple(str(row.get(k, "")) for k in key_fields)
        for row in new_rows
    }

    kept = [
        row for row in existing
        if tuple(str(row.get(k, "")) for k in key_fields) not in new_keys
    ]

    write_csv_dicts(path, all_fields, kept + new_rows)

# ---------------------------------------------------------------------------
# HTTP with exponential backoff retry
# ---------------------------------------------------------------------------

def get_with_retry(
    url: str,
    params: Optional[dict[str, Any]] = None,
    max_retries: int = 6,
    base_delay: float = 2.0,
    timeout: int = 30,
) -> requests.Response:
    for attempt in range(max_retries):
        try:
            response = SESSION.get(url, params=params, timeout=timeout)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else base_delay * (2 ** attempt)

                dbg(
                    f"429 rate-limited → waiting {wait:.0f}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )

                time.sleep(wait)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise

            wait = base_delay * (2 ** attempt)
            dbg(f"Request error ({exc}), retrying in {wait:.0f}s")
            time.sleep(wait)

    raise RuntimeError(f"All {max_retries} retries exhausted for {url}")

# ---------------------------------------------------------------------------
# Steam inventory — Web API with community fallback
# ---------------------------------------------------------------------------

def fetch_inventory(steam_id: str) -> dict[str, list[dict[str, Any]]]:
    """
    Tries Steam Web API first.
    If it returns 0 assets, falls back to Steam Community inventory endpoint.

    This is intentional: CS2 inventories often work more reliably through
    steamcommunity.com/inventory than through IEconService.
    """
    try:
        inv = fetch_inventory_web_api(steam_id)

        amount = sum(parse_amount(asset.get("amount", 1)) for asset in inv.get("assets", []))

        if amount > 0:
            dbg(f"  Web API inventory accepted: {len(inv['assets'])} asset rows, amount={amount}")
            return inv

        dbg("  Web API returned 0 assets; falling back to Steam Community inventory endpoint")

    except Exception as exc:
        dbg(f"  Web API inventory failed: {exc}")
        dbg("  Falling back to Steam Community inventory endpoint")

    return fetch_inventory_community(steam_id)


def fetch_inventory_web_api(steam_id: str) -> dict[str, list[dict[str, Any]]]:
    """
    Fetches inventory via IEconService/GetInventoryItemsWithDescriptions.

    This may return 0 assets for CS2 inventories even when the public
    community inventory endpoint works.
    """
    api_key = require_steam_api_key()

    assets: list[dict[str, Any]] = []
    descriptions_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    last_assetid: Optional[str] = None
    previous_last_assetid: Optional[str] = None

    while True:
        request_body: dict[str, Any] = {
            "steamid": steam_id,
            "appid": APPID,
            "contextid": CONTEXTID,
            "get_descriptions": True,
            "language": LANGUAGE,
            "count": INVENTORY_PAGE_SIZE,
        }

        if last_assetid:
            request_body["start_assetid"] = last_assetid

        # IEconService is a service interface. Use input_json.
        response = get_with_retry(
            "https://api.steampowered.com/IEconService/GetInventoryItemsWithDescriptions/v1/",
            params={
                "key": api_key,
                "input_json": json.dumps(request_body),
            },
        )

        raw = response.json()
        data = raw.get("response", raw)

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected Steam API response shape: {raw}")

        if data.get("error"):
            raise RuntimeError(f"Steam API error for {steam_id}: {data.get('error')}")

        page_assets = data.get("assets", [])
        page_descriptions = data.get("descriptions", [])

        if not isinstance(page_assets, list):
            raise RuntimeError(f"Unexpected assets value from Steam API: {page_assets}")

        if not isinstance(page_descriptions, list):
            raise RuntimeError(f"Unexpected descriptions value from Steam API: {page_descriptions}")

        assets.extend(page_assets)

        for desc in page_descriptions:
            classid = str(desc.get("classid", ""))
            instanceid = str(desc.get("instanceid", "0"))

            if classid:
                descriptions_by_key[(classid, instanceid)] = desc

        page_amount = sum(parse_amount(asset.get("amount", 1)) for asset in page_assets)
        total_amount = sum(parse_amount(asset.get("amount", 1)) for asset in assets)

        dbg(
            f"  Web API page: +{len(page_assets)} asset rows, "
            f"+{page_amount} item amount, total_amount={total_amount}"
        )

        more_items = parse_boolish(data.get("more_items", False))

        if not more_items:
            break

        previous_last_assetid = last_assetid
        last_assetid = data.get("last_assetid")

        if not last_assetid and page_assets:
            last_assetid = str(page_assets[-1].get("assetid", ""))

        if not last_assetid:
            raise RuntimeError("Steam API returned more_items=true but no last_assetid.")

        if previous_last_assetid == last_assetid:
            raise RuntimeError(
                f"Steam API pagination did not advance. last_assetid={last_assetid}"
            )

        time.sleep(1.0)

    return {
        "assets": assets,
        "descriptions": list(descriptions_by_key.values()),
    }


def fetch_inventory_community(steam_id: str) -> dict[str, list[dict[str, Any]]]:
    """
    Fetches full CS2 inventory from the Steam Community inventory endpoint.

    This is usually the more reliable endpoint for public CS2 inventories.
    """
    assets: list[dict[str, Any]] = []
    descriptions_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    last_assetid: Optional[str] = None
    previous_last_assetid: Optional[str] = None

    while True:
        params: dict[str, Any] = {
            "l": LANGUAGE,
            "count": 2000,
        }

        if last_assetid:
            params["start_assetid"] = last_assetid

        response = get_with_retry(
            f"https://steamcommunity.com/inventory/{steam_id}/{APPID}/{CONTEXTID}",
            params=params,
        )

        data = response.json()

        if data.get("success") not in (1, True, None):
            raise RuntimeError(f"Steam Community inventory request failed: {data}")

        page_assets = data.get("assets", [])
        page_descriptions = data.get("descriptions", [])

        if not isinstance(page_assets, list):
            raise RuntimeError(f"Unexpected community assets value: {page_assets}")

        if not isinstance(page_descriptions, list):
            raise RuntimeError(f"Unexpected community descriptions value: {page_descriptions}")

        assets.extend(page_assets)

        for desc in page_descriptions:
            classid = str(desc.get("classid", ""))
            instanceid = str(desc.get("instanceid", "0"))

            if classid:
                descriptions_by_key[(classid, instanceid)] = desc

        page_amount = sum(parse_amount(asset.get("amount", 1)) for asset in page_assets)
        total_amount = sum(parse_amount(asset.get("amount", 1)) for asset in assets)

        dbg(
            f"  Community page: +{len(page_assets)} asset rows, "
            f"+{page_amount} item amount, "
            f"total_amount={total_amount}, "
            f"reported_total={data.get('total_inventory_count')}"
        )

        more_items = parse_boolish(data.get("more_items", False))

        if not more_items:
            break

        previous_last_assetid = last_assetid
        last_assetid = data.get("last_assetid")

        if not last_assetid and page_assets:
            last_assetid = str(page_assets[-1].get("assetid", ""))

        if not last_assetid:
            raise RuntimeError("Community endpoint returned more_items=true but no last_assetid.")

        if previous_last_assetid == last_assetid:
            raise RuntimeError(
                f"Community pagination did not advance. last_assetid={last_assetid}"
            )

        dbg(f"  Paginating community inventory after assetid={last_assetid}")
        time.sleep(1.5)

    return {
        "assets": assets,
        "descriptions": list(descriptions_by_key.values()),
    }

def require_steam_api_key() -> str:
    if not STEAM_API_KEY:
        raise RuntimeError(
            "STEAM_API_KEY is missing. Add it as an environment variable "
            "or GitHub Actions secret."
        )

    return str(STEAM_API_KEY)

def get_marketable_counts(inv: dict[str, list[dict[str, Any]]]) -> Counter[str]:
    """
    Returns Counter({market_hash_name: quantity}).

    Important:
        Uses asset["amount"], not just one count per asset row.
    """
    desc_map = {
        (str(desc.get("classid", "")), str(desc.get("instanceid", "0"))): desc
        for desc in inv.get("descriptions", [])
    }

    counts: Counter[str] = Counter()
    skipped_non_marketable = 0
    skipped_no_description = 0

    for asset in inv.get("assets", []):
        classid = str(asset.get("classid", ""))
        instanceid = str(asset.get("instanceid", "0"))
        amount = parse_amount(asset.get("amount", 1))

        desc = desc_map.get((classid, instanceid))

        if not desc:
            skipped_no_description += amount
            continue

        if not parse_boolish(desc.get("marketable", 0)):
            skipped_non_marketable += amount
            continue

        name = desc.get("market_hash_name")

        if name:
            counts[str(name)] += amount

    if skipped_no_description:
        dbg(f"  Skipped {skipped_no_description} item(s) without description")

    if skipped_non_marketable:
        dbg(f"  Skipped {skipped_non_marketable} non-marketable item(s)")

    return counts


def counts_for_account(steam_id: str) -> Counter[str]:
    label = ACCOUNT_LABELS.get(steam_id, steam_id)
    dbg(f"Fetching inventory: {label} ({steam_id})")

    inv = fetch_inventory(steam_id)
    counts = get_marketable_counts(inv)

    dbg(f"  {sum(counts.values())} total marketable item amount")
    dbg(f"  {len(counts)} unique marketable items")

    return counts

# ---------------------------------------------------------------------------
# Price parsing — handles EUR/USD/mixed formats
# ---------------------------------------------------------------------------

_PRICE_RE = re.compile(r"(\d[\d.,]*\d|\d)")


def parse_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None

    text = text.replace("\u202f", "").replace("\xa0", "").replace(" ", "")

    match = _PRICE_RE.search(text)

    if not match:
        return None

    raw = match.group(1)

    # European "1.234,56" vs American "1,234.56"
    if "." in raw and "," in raw:
        if raw.rindex(".") < raw.rindex(","):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        raw = raw.replace(",", ".")

    try:
        value = float(raw)
        return value if value > 0 else None
    except ValueError:
        return None


def fetch_price_from_steam(name: str) -> tuple[Optional[float], Optional[float]]:
    """
    Returns (lowest, median) from Steam Community Market priceoverview.

    Steam does not provide a convenient public bulk price endpoint here,
    so this script deduplicates and caches these requests.
    """
    try:
        response = get_with_retry(
            "https://steamcommunity.com/market/priceoverview/",
            params={
                "appid": str(APPID),
                "market_hash_name": name,
                "currency": CURRENCY_CODE,
            },
        )

        data = response.json()

        if not data.get("success"):
            dbg(f"  success=false for price '{name}'")
            return None, None

        return (
            parse_price(data.get("lowest_price")),
            parse_price(data.get("median_price")),
        )

    except Exception as exc:
        dbg(f"  Price fetch failed for '{name}': {exc}")
        return None, None

# ---------------------------------------------------------------------------
# Price cache
# ---------------------------------------------------------------------------

def read_price_cache() -> dict[str, dict[str, Any]]:
    if not PRICE_CACHE_JSON.exists():
        return {}

    try:
        data = json.loads(PRICE_CACHE_JSON.read_text(encoding="utf-8"))

        if not isinstance(data, dict):
            return {}

        return data

    except Exception as exc:
        dbg(f"Could not read price cache: {exc}")
        return {}


def write_price_cache(cache: dict[str, dict[str, Any]]) -> None:
    tmp = PRICE_CACHE_JSON.with_suffix(".tmp")

    tmp.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    tmp.replace(PRICE_CACHE_JSON)


def get_cached_price(
    name: str,
    cache: dict[str, dict[str, Any]],
) -> Optional[dict[str, Optional[float]]]:
    if PRICE_CACHE_TTL_HOURS <= 0:
        return None

    entry = cache.get(name)

    if not isinstance(entry, dict):
        return None

    if entry.get("currency") != CURRENCY:
        return None

    fetched_at = entry.get("fetched_at")

    if not fetched_at:
        return None

    try:
        fetched_dt = dt.datetime.fromisoformat(str(fetched_at).replace("Z", "+00:00"))

        if fetched_dt.tzinfo is None:
            fetched_dt = fetched_dt.replace(tzinfo=dt.timezone.utc)

    except ValueError:
        return None

    age_hours = (now_utc() - fetched_dt).total_seconds() / 3600

    if age_hours > PRICE_CACHE_TTL_HOURS:
        return None

    return {
        "lowest": entry.get("lowest"),
        "median": entry.get("median"),
    }


def fetch_prices_for_items(item_names: list[str]) -> dict[str, dict[str, Optional[float]]]:
    """
    Fetches prices once per unique item name.
    Uses price_cache.json where possible.
    """
    cache = read_price_cache()
    price_map: dict[str, dict[str, Optional[float]]] = {}

    unique_names = sorted(set(item_names))
    fresh_requests = 0

    dbg(f"Fetching prices for {len(unique_names)} unique items")

    for index, name in enumerate(unique_names, 1):
        cached = get_cached_price(name, cache)

        if cached is not None:
            price_map[name] = cached
            dbg(f"  [{index}/{len(unique_names)}] cached {name!r}")
            continue

        lowest, median = fetch_price_from_steam(name)

        price_map[name] = {
            "lowest": lowest,
            "median": median,
        }

        cache[name] = {
            "currency": CURRENCY,
            "lowest": lowest,
            "median": median,
            "fetched_at": now_utc().isoformat(),
        }

        fresh_requests += 1

        lo_s = f"{lowest:.2f}" if lowest is not None else "N/A"
        med_s = f"{median:.2f}" if median is not None else "N/A"

        dbg(f"  [{index}/{len(unique_names)}] fetched {name!r} → L:{lo_s} M:{med_s}")

        # Persist occasionally so reruns do not lose everything if interrupted.
        if fresh_requests % 10 == 0:
            write_price_cache(cache)

        time.sleep(SLEEP_MS / 1000.0)

    write_price_cache(cache)

    return price_map

# ---------------------------------------------------------------------------
# Value calculations
# ---------------------------------------------------------------------------

def calculate_totals(
    counts: Counter[str],
    price_map: dict[str, dict[str, Optional[float]]],
) -> tuple[float, float]:
    lowest_total = 0.0
    median_total = 0.0

    missing_lowest: list[str] = []

    for name, qty in counts.items():
        price = price_map.get(name, {})
        lowest = price.get("lowest")
        median = price.get("median")

        if lowest is None:
            missing_lowest.append(name)
        else:
            lowest_total += lowest * qty

        if median is not None:
            median_total += median * qty

    if missing_lowest:
        print(
            f"  WARNING: {len(missing_lowest)} item(s) had no lowest price and "
            f"are excluded from the lowest total: "
            f"{', '.join(missing_lowest[:5])}"
            f"{'…' if len(missing_lowest) > 5 else ''}"
        )

    return round(lowest_total, 2), round(median_total, 2)


def build_top_items(
    merged_counts: Counter[str],
    price_map: dict[str, dict[str, Optional[float]]],
) -> list[dict[str, Any]]:
    sorted_items = sorted(
        merged_counts.items(),
        key=lambda item: (price_map.get(item[0], {}).get("lowest") or 0) * item[1],
        reverse=True,
    )

    top = sorted_items[:TOP_ITEMS]

    result: list[dict[str, Any]] = []

    for name, qty in top:
        price = price_map.get(name, {})
        lowest = price.get("lowest")
        median = price.get("median")

        result.append({
            "name": name,
            "qty": qty,
            "lowest": lowest,
            "median": median,
            "total_lowest": round((lowest or 0) * qty, 2),
            "total_median": round((median or 0) * qty, 2),
        })

    return result

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    today = dt.date.today().isoformat()

    if not STEAM_IDS:
        raise RuntimeError("No steam_ids configured in config.json")

    require_steam_api_key()

    account_counts: dict[str, Counter[str]] = {}
    merged_counts: Counter[str] = Counter()
    failed_ids: list[str] = []

    # 1. Fetch all inventories first.
    for steam_id in STEAM_IDS:
        try:
            counts = counts_for_account(steam_id)

            account_counts[steam_id] = counts
            merged_counts.update(counts)

            label = ACCOUNT_LABELS.get(steam_id, steam_id)
            print(
                f"  {label}: "
                f"{sum(counts.values())} marketable item amount, "
                f"{len(counts)} unique marketable items"
            )

        except Exception as exc:
            failed_ids.append(steam_id)
            print(f"ERROR processing inventory {steam_id}: {exc}")

    if not account_counts:
        raise RuntimeError("No account inventory could be processed.")

    # 2. Fetch each unique market price only once.
    price_map = fetch_prices_for_items(list(merged_counts.keys()))

    # 3. Calculate per-account values.
    account_rows: list[dict[str, Any]] = []
    total_lowest = 0.0
    total_median = 0.0

    for steam_id, counts in account_counts.items():
        label = ACCOUNT_LABELS.get(steam_id, steam_id)

        lowest, median = calculate_totals(counts, price_map)

        total_lowest += lowest
        total_median += median

        account_rows.append({
            "date": today,
            "steam_id": steam_id,
            ACCOUNT_VALUE_FIELD: round(lowest, 2),
            "currency": CURRENCY,
        })

        print(f"  {label}: lowest={lowest} {CURRENCY}, median={median} {CURRENCY}")

    total_lowest = round(total_lowest, 2)
    total_median = round(total_median, 2)

    partial = len(failed_ids) > 0

    # 4. Write values.csv.
    # Do not write combined total if any account failed.
    if partial:
        print(
            f"\nWARNING: Skipping values.csv write — "
            f"{len(failed_ids)} account(s) failed: {failed_ids}"
        )
    else:
        upsert_rows(
            VALUES_CSV,
            ["date", "lowest", "median", "currency"],
            [{
                "date": today,
                "lowest": total_lowest,
                "median": total_median,
                "currency": CURRENCY,
            }],
            key_fields=["date"],
        )

    # 5. Write accounts.csv for successful accounts.
    if account_rows:
        upsert_rows(
            ACCOUNTS_CSV,
            ["date", "steam_id", ACCOUNT_VALUE_FIELD, "currency"],
            account_rows,
            key_fields=["date", "steam_id"],
        )

    # 6. Write items.json.
    top_items = build_top_items(merged_counts, price_map)

    ITEMS_JSON.write_text(
        json.dumps(
            {
                "date": today,
                "currency": CURRENCY,
                "partial": partial,
                "failed_ids": failed_ids,
                "total_lowest": total_lowest,
                "total_median": total_median,
                "total_marketable_item_amount": sum(merged_counts.values()),
                "unique_marketable_items": len(merged_counts),
                "items": top_items,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    # 7. Console summary.
    print(
        f"\n✓ {today}: "
        f"lowest={total_lowest} {CURRENCY}, median={total_median} {CURRENCY}"
        + (" [PARTIAL]" if partial else "")
    )

    print(f"\nTop {len(top_items)} items by lowest total value:")

    for item in top_items:
        lowest = item["lowest"]
        lo_s = f"{lowest:.2f}" if lowest is not None else "N/A"

        print(
            f"  {item['name']!r:60s} "
            f"×{item['qty']}  "
            f"unit={lo_s}  "
            f"total={item['total_lowest']:.2f} {CURRENCY}"
        )


if __name__ == "__main__":
    main()