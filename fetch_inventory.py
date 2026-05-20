#!/usr/bin/env python3
"""
CS2 Inventory Value Tracker
Fetches Steam Market prices for all configured accounts, records daily
totals to values.csv and accounts.csv (overwriting same-day rows on re-runs),
and writes top items to items.json.

Requires: pip install requests
"""
import csv
import datetime as dt
import argparse
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote

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
PRICE_TTL_HRS  = float(CONFIG.get("price_cache_ttl_hours", 12))
HISTORY_SLEEP_MIN_MS = int(CONFIG.get("history_sleep_min_ms", max(SLEEP_MS, 12000)))
HISTORY_SLEEP_MAX_MS = int(CONFIG.get("history_sleep_max_ms", max(SLEEP_MS * 3, 30000)))

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
PRICE_CACHE  = ROOT / "price_cache.json"
HISTORY_CACHE = ROOT / "item_history_cache.json"
BACKTRACK_CSV = ROOT / "backtracked_values.csv"
ITEM_SNAPSHOTS_CSV = ROOT / "item_snapshots.csv"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 inventory-value-tracker/1.0",
    "Accept": "application/json,text/javascript,*/*;q=0.01",
})

if os.environ.get("STEAM_SESSIONID"):
    SESSION.cookies.set("sessionid", os.environ["STEAM_SESSIONID"], domain="steamcommunity.com")
if os.environ.get("STEAM_LOGIN_SECURE"):
    SESSION.cookies.set("steamLoginSecure", os.environ["STEAM_LOGIN_SECURE"], domain="steamcommunity.com")
if os.environ.get("STEAM_COOKIE"):
    SESSION.headers["Cookie"] = os.environ["STEAM_COOKIE"]

HAS_STEAM_AUTH = bool(
    os.environ.get("STEAM_COOKIE")
    or os.environ.get("STEAM_LOGIN_SECURE")
)
PRICEHISTORY_AUTH_BLOCKED = False

try:
    PRICE_CACHE_DATA = json.loads(PRICE_CACHE.read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    PRICE_CACHE_DATA = {}

try:
    HISTORY_CACHE_DATA = json.loads(HISTORY_CACHE.read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    HISTORY_CACHE_DATA = {}

# ---------------------------------------------------------------------------
# Logging and generic helpers
# ---------------------------------------------------------------------------

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[DEBUG] {msg}")


def history_sleep_seconds() -> float:
    low = max(0, min(HISTORY_SLEEP_MIN_MS, HISTORY_SLEEP_MAX_MS))
    high = max(low, max(HISTORY_SLEEP_MIN_MS, HISTORY_SLEEP_MAX_MS))
    return random.uniform(low / 1000.0, high / 1000.0)


def parse_amount(value) -> int:
    """Steam inventory assets may represent stackable items via amount."""
    try:
        amount = int(value)
        return amount if amount > 0 else 1
    except (TypeError, ValueError):
        return 1


def parse_csv_float(value) -> Optional[float]:
    try:
        number = float(str(value).strip())
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def format_csv_float(value: float) -> str:
    return f"{value:.2f}"


def cents_to_currency(value) -> Optional[float]:
    try:
        cents = int(value)
        return round(cents / 100.0, 2) if cents > 0 else None
    except (TypeError, ValueError):
        return None


def now_ts() -> float:
    return time.time()


def cache_key(name: str) -> str:
    return f"{CURRENCY_CODE}:{name}"


def get_cached_price(name: str) -> Optional[tuple]:
    entry = PRICE_CACHE_DATA.get(cache_key(name))
    if not isinstance(entry, dict):
        return None
    fetched_at = float(entry.get("fetched_at", 0) or 0)
    if fetched_at <= 0 or (now_ts() - fetched_at) > PRICE_TTL_HRS * 3600:
        return None
    return entry.get("lowest"), entry.get("median")


def set_cached_price(name: str, lowest: Optional[float], median: Optional[float]) -> None:
    PRICE_CACHE_DATA[cache_key(name)] = {
        "currency": CURRENCY,
        "lowest": lowest,
        "median": median,
        "fetched_at": now_ts(),
    }


def write_price_cache() -> None:
    PRICE_CACHE.write_text(
        json.dumps(PRICE_CACHE_DATA, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def write_history_cache() -> None:
    HISTORY_CACHE.write_text(
        json.dumps(HISTORY_CACHE_DATA, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def parse_steam_history_date(value: str) -> Optional[str]:
    # Steam embeds dates like "Feb 07 2024 01: +0" in listing pages.
    parts = str(value).split()
    if len(parts) < 3:
        return None
    try:
        parsed = dt.datetime.strptime(" ".join(parts[:3]), "%b %d %Y")
        return parsed.date().isoformat()
    except ValueError:
        return None


def normalize_history_points(raw_points: list) -> list[dict]:
    by_date: dict[str, dict] = {}
    for point in raw_points:
        if not isinstance(point, list) or len(point) < 2:
            continue
        date_str = parse_steam_history_date(point[0])
        if not date_str:
            continue
        try:
            price = float(point[1])
        except (TypeError, ValueError):
            continue
        try:
            volume = int(str(point[2]).replace(",", "")) if len(point) > 2 else 0
        except (TypeError, ValueError):
            volume = 0
        if price > 0:
            by_date[date_str] = {"date": date_str, "price": round(price, 2), "volume": volume}
    return [by_date[k] for k in sorted(by_date)]


def history_cache_key(name: str) -> str:
    return f"{CURRENCY_CODE}:{name}"


def tag_lookup(desc: dict) -> dict:
    tags = {}
    for tag in desc.get("tags", []) or []:
        if not isinstance(tag, dict):
            continue
        category = tag.get("category") or tag.get("category_name") or "tag"
        tags[category] = {
            "name": tag.get("localized_tag_name") or tag.get("name") or tag.get("internal_name"),
            "internal": tag.get("internal_name"),
            "category_name": tag.get("localized_category_name") or tag.get("category_name") or category,
            "color": tag.get("color"),
        }
    return tags


def tag_name(tags: dict, category: str) -> Optional[str]:
    tag = tags.get(category)
    return tag.get("name") if isinstance(tag, dict) else None


def infer_exterior(name: str) -> Optional[str]:
    m = re.search(r"\((Factory New|Minimal Wear|Field-Tested|Well-Worn|Battle-Scarred)\)", name)
    return m.group(1) if m else None


def infer_family(name: str, item_type: Optional[str]) -> str:
    base = re.sub(r"^(StatTrak™|StatTrak\u2122|Souvenir)\s+", "", name)
    base = re.sub(r"\s+\((Factory New|Minimal Wear|Field-Tested|Well-Worn|Battle-Scarred)\)$", "", base)
    if "|" in base:
        return base.split("|", 1)[0].strip()
    if item_type:
        return item_type
    return "Other"


def market_url(name: str, language: str = "german") -> str:
    return f"https://steamcommunity.com/market/listings/730/{quote(name, safe='')}?l={language}"


def inspect_url(desc: dict, assetid: str, owner_steamid: str) -> Optional[str]:
    for action in desc.get("market_actions", []) or desc.get("actions", []) or []:
        link = action.get("link") if isinstance(action, dict) else None
        if not link:
            continue
        return (
            link.replace("%assetid%", str(assetid))
                .replace("%owner_steamid%", str(owner_steamid))
                .replace("%listingid%", "")
        )
    return None


def describe_item(desc: dict, asset: dict, owner_steamid: str) -> dict:
    name = desc.get("market_hash_name") or desc.get("name") or "Unknown item"
    tags = tag_lookup(desc)
    item_type = tag_name(tags, "Type") or desc.get("type")
    exterior = tag_name(tags, "Exterior") or infer_exterior(name)
    quality = tag_name(tags, "Quality")
    rarity = tag_name(tags, "Rarity")
    weapon = tag_name(tags, "Weapon")
    collection = tag_name(tags, "ItemSet")
    tournament = tag_name(tags, "Tournament")

    return {
        "name": name,
        "display_name": desc.get("name") or name,
        "type": item_type,
        "exterior": exterior,
        "quality": quality,
        "rarity": rarity,
        "weapon": weapon,
        "collection": collection,
        "tournament": tournament,
        "family": infer_family(name, item_type),
        "commodity": bool(desc.get("commodity")),
        "tradable": bool(desc.get("tradable")),
        "marketable": bool(desc.get("marketable")),
        "name_color": desc.get("name_color"),
        "background_color": desc.get("background_color"),
        "icon_url": (
            f"https://community.cloudflare.steamstatic.com/economy/image/{desc.get('icon_url')}"
            if desc.get("icon_url") else None
        ),
        "icon_url_large": (
            f"https://community.cloudflare.steamstatic.com/economy/image/{desc.get('icon_url_large')}"
            if desc.get("icon_url_large") else None
        ),
        "inspect_url": inspect_url(desc, str(asset.get("assetid", "")), owner_steamid),
        "market_url": market_url(name),
        "tags": tags,
    }


# ---------------------------------------------------------------------------
# CSV helpers — read/write as dicts, upsert by key
# ---------------------------------------------------------------------------

def read_csv_dicts(path: Path):
    """Returns (fieldnames: list[str], rows: list[dict])."""
    if not path.exists():
        return [], []
    with open(path, newline="", encoding="utf-8-sig") as f:  # utf-8-sig strips BOM
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def write_csv_dicts(path: Path, fieldnames: list, rows: list) -> None:
    """Atomically write CSV by staging to a .tmp file first."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)  # atomic on POSIX; best-effort on Windows


def upsert_rows(path: Path, fieldnames: list, new_rows: list, key_fields: list) -> None:
    """
    Write new_rows into path, overwriting any existing rows whose key_fields
    match. New rows are appended at the end. Safe to call multiple times per day.
    """
    existing_fields, existing = read_csv_dicts(path)
    # Always include every required fieldname; existing columns keep their order.
    all_fields = list(dict.fromkeys((existing_fields or fieldnames) + fieldnames))
    new_keys = {tuple(r[k] for k in key_fields) for r in new_rows}
    kept = [r for r in existing
            if tuple(r.get(k, "") for k in key_fields) not in new_keys]
    write_csv_dicts(path, all_fields, kept + new_rows)


def backfill_missing_daily_rows(path: Path, numeric_fields: tuple[str, ...]) -> None:
    """
    Fill date gaps by linear interpolation between neighboring real rows.

    This is only used for display continuity after skipped workflow days. It does
    not invent future values and only fills dates that are bracketed by existing
    earlier and later rows.
    """
    fieldnames, rows = read_csv_dicts(path)
    if not rows or "date" not in fieldnames:
        return

    by_date = {}
    for row in rows:
        date_str = str(row.get("date", "")).strip()
        if date_str:
            by_date[date_str] = row

    dated_rows = []
    for date_str, row in by_date.items():
        try:
            dated_rows.append((dt.date.fromisoformat(date_str), row))
        except ValueError:
            continue

    dated_rows.sort(key=lambda pair: pair[0])
    if len(dated_rows) < 2:
        return

    out = []
    added = 0
    all_fields = list(dict.fromkeys(fieldnames + ["currency"]))

    for idx, (left_date, left_row) in enumerate(dated_rows[:-1]):
        right_date, right_row = dated_rows[idx + 1]
        out.append(left_row)

        gap = (right_date - left_date).days
        if gap <= 1:
            continue

        for offset in range(1, gap):
            ratio = offset / gap
            new_row = {field: "" for field in all_fields}
            new_row["date"] = (left_date + dt.timedelta(days=offset)).isoformat()

            for field in numeric_fields:
                left_value = parse_csv_float(left_row.get(field))
                right_value = parse_csv_float(right_row.get(field))
                if left_value is None or right_value is None:
                    continue
                new_row[field] = format_csv_float(left_value + (right_value - left_value) * ratio)

            if "currency" in all_fields:
                new_row["currency"] = right_row.get("currency") or left_row.get("currency") or CURRENCY

            out.append(new_row)
            added += 1

    out.append(dated_rows[-1][1])

    if added:
        write_csv_dicts(path, all_fields, out)
        dbg(f"Backfilled {added} missing daily row(s) in {path.name}")

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
            r = SESSION.get(url, params=params, timeout=timeout)
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


def get_marketable_items(inv: dict, steam_id: str) -> dict:
    """
    Returns aggregated marketable item metadata keyed by market_hash_name.
    Non-marketable items (e.g. untradeable skins, event items) are skipped.
    """
    desc_map = {
        (str(d["classid"]), str(d.get("instanceid", "0"))): d
        for d in inv.get("descriptions", [])
    }
    items: dict = {}
    skipped = 0
    for asset in inv.get("assets", []):
        key = (str(asset["classid"]), str(asset.get("instanceid", "0")))
        amount = parse_amount(asset.get("amount", 1))
        desc = desc_map.get(key)
        if not desc:
            continue
        if not desc.get("marketable", 0):
            skipped += amount
            continue
        meta = describe_item(desc, asset, steam_id)
        name = meta["name"]
        if name not in items:
            items[name] = {**meta, "qty": 0}
        elif not items[name].get("inspect_url") and meta.get("inspect_url"):
            items[name]["inspect_url"] = meta["inspect_url"]
        items[name]["qty"] += amount

    if skipped:
        dbg(f"  Skipped {skipped} non-marketable item(s)")
    return items

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


def fetch_lowest_from_listing_render(name: str) -> Optional[float]:
    """
    Fallback for missing priceoverview lowest_price.

    priceoverview can return success=true with median_price but no lowest_price.
    The listings render endpoint exposes active sell listings; we take the
    cheapest converted listing price including Steam fees.
    """
    try:
        url_name = quote(name, safe="")
        r = get_with_retry(
            f"https://steamcommunity.com/market/listings/730/{url_name}/render/",
            params={
                "query": "",
                "start": 0,
                "count": 10,
                "country": "DE",
                "language": "english",
                "currency": CURRENCY_CODE,
            },
            max_retries=3,
            base_delay=3.0,
        )
        data = r.json()
        listinginfo = data.get("listinginfo", {})
        if not isinstance(listinginfo, dict) or not listinginfo:
            dbg(f"  listings fallback found no active sell listings for '{name}'")
            return None

        prices = []
        for listing in listinginfo.values():
            if not isinstance(listing, dict):
                continue

            price = (
                listing.get("converted_price_per_unit")
                or listing.get("converted_price")
                or listing.get("price")
            )
            fee = (
                listing.get("converted_fee_per_unit")
                or listing.get("converted_fee")
                or listing.get("fee")
                or 0
            )

            try:
                total = int(price) + int(fee)
            except (TypeError, ValueError):
                continue

            parsed = cents_to_currency(total)
            if parsed is not None:
                prices.append(parsed)

        if not prices:
            dbg(f"  listings fallback had no parseable prices for '{name}'")
            return None

        lowest = min(prices)
        dbg(f"  lowest fallback from active listings for '{name}': {lowest:.2f}")
        return lowest

    except Exception as exc:
        dbg(f"  listings fallback failed for '{name}': {exc}")
        return None


def fetch_price(name: str) -> tuple:
    """Returns (lowest: Optional[float], median: Optional[float])."""
    cached = get_cached_price(name)
    if cached is not None:
        return cached

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
            set_cached_price(name, None, None)
            return None, None

        lowest = parse_price(data.get("lowest_price"))
        median = parse_price(data.get("median_price"))

        if lowest is None:
            lowest = fetch_lowest_from_listing_render(name)

        set_cached_price(name, lowest, median)
        return lowest, median
    except Exception as exc:
        dbg(f"  Price fetch failed for '{name}': {exc}")
        return None, None


def fetch_market_history(name: str) -> list[dict]:
    """
    Fetch public Steam Market graph data from the listing page.

    Steam exposes the chart as a line1 JavaScript array on public listing pages.
    Each point is [date, median-ish sale price, volume]. This gives enough data
    to reconstruct what the current inventory would have been worth historically.
    """
    key = history_cache_key(name)
    entry = HISTORY_CACHE_DATA.get(key)
    if isinstance(entry, dict) and entry.get("points"):
        fetched_at = float(entry.get("fetched_at", 0) or 0)
        if fetched_at > 0 and (now_ts() - fetched_at) <= PRICE_TTL_HRS * 3600:
            return entry["points"]

    global PRICEHISTORY_AUTH_BLOCKED

    try:
        url_name = quote(name, safe="")
        listing_url = f"https://steamcommunity.com/market/listings/730/{url_name}"

        # Open the listing first so Steam sets sessionid/steamCountry cookies.
        # The pricehistory endpoint often returns 400 without those cookies.
        listing = get_with_retry(
            listing_url,
            params={"l": "english"},
            max_retries=3,
            base_delay=3.0,
            timeout=30,
        )

        raw_points = []
        if not PRICEHISTORY_AUTH_BLOCKED:
            hist = SESSION.get(
                "https://steamcommunity.com/market/pricehistory/",
                params={
                    "country": "DE",
                    "currency": CURRENCY_CODE,
                    "appid": "730",
                    "market_hash_name": name,
                },
                timeout=30,
                headers={"Referer": listing.url},
            )
            if hist.status_code == 400:
                PRICEHISTORY_AUTH_BLOCKED = True
                if not HAS_STEAM_AUTH:
                    dbg("  Steam pricehistory returned 400. This endpoint now requires logged-in Steam cookies.")
                else:
                    dbg("  Steam pricehistory returned 400 even with supplied cookies. Refresh/copy cookies again.")
            else:
                hist.raise_for_status()
                data = hist.json()
                if isinstance(data, dict) and data.get("success") and isinstance(data.get("prices"), list):
                    raw_points = data["prices"]
                else:
                    dbg(f"  pricehistory endpoint returned no prices for '{name}': {data}")

        if not raw_points:
            match = re.search(r"(?:var\s+)?line1\s*=\s*(\[.*?\]);", listing.text, re.S)
            if match:
                raw_points = json.loads(match.group(1))

        points = normalize_history_points(raw_points)
        if not points:
            dbg(f"  No public graph history found for '{name}'")
            return []

        HISTORY_CACHE_DATA[key] = {"currency": CURRENCY, "points": points, "fetched_at": now_ts()}
        return points
    except Exception as exc:
        dbg(f"  Market history fetch failed for '{name}': {exc}")
        return entry.get("points", []) if isinstance(entry, dict) else []


def has_cached_market_history(name: str) -> bool:
    entry = HISTORY_CACHE_DATA.get(history_cache_key(name))
    return isinstance(entry, dict) and bool(entry.get("points"))


def backfill_market_history_once(items: list) -> None:
    """
    One-time all-item market history import.

    Existing item histories are never refreshed here. Progress is written after
    every successful item, so interrupted runs can be resumed without repeating
    completed requests.
    """
    global PRICEHISTORY_AUTH_BLOCKED
    PRICEHISTORY_AUTH_BLOCKED = False

    candidates = [
        item for item in items
        if item.get("name") and item.get("qty", 0) > 0
    ]
    if not candidates:
        print("No current items available for market-history backfill.")
        return

    missing = [item for item in candidates if not has_cached_market_history(item["name"])]
    if not missing:
        print(f"Market-history cache already contains all {len(candidates)} current item(s).")
        return

    print(
        f"Backfilling all-time Steam Market history for {len(missing)} missing "
        f"item(s); {len(candidates) - len(missing)} already cached."
    )

    consecutive_empty = 0
    for idx, item in enumerate(missing, 1):
        name = item["name"]
        dbg(f"  Backfill history [{idx}/{len(missing)}] {name!r}")
        points = fetch_market_history(name)
        if points:
            write_history_cache()
            consecutive_empty = 0
            print(f"  cached {len(points)} point(s): {name}")
        else:
            consecutive_empty += 1
            print(f"  no history cached: {name}")
            if PRICEHISTORY_AUTH_BLOCKED and not HAS_STEAM_AUTH and consecutive_empty >= 3:
                print(
                    "\nStopping history backfill: Steam returned 400 for pricehistory "
                    "and no public graph data was exposed. Re-run once with STEAM_COOKIE "
                    "from your logged-in browser session if you want old history."
                )
                break

        delay = history_sleep_seconds()
        dbg(f"  Sleeping {delay:.1f}s before next history request")
        time.sleep(delay)


def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def write_backtracked_values(items: list) -> None:
    """
    Write a reconstructed portfolio series for the current holdings.

    The series answers: "what would the items currently in the inventory have
    been worth on each historical date?" It does not claim the inventory had the
    same contents on those dates.
    """
    histories = []
    candidates = [item for item in items if item.get("name") and item.get("qty", 0) > 0]
    for item in candidates:
        entry = HISTORY_CACHE_DATA.get(history_cache_key(item["name"]))
        points = entry.get("points", []) if isinstance(entry, dict) else []
        if points:
            histories.append({"item": item, "points": points})

    if not histories:
        return

    first_date = min(dt.date.fromisoformat(h["points"][0]["date"]) for h in histories)
    today = dt.date.today()
    rows = []
    cursors = [0 for _ in histories]
    latest_prices: list[Optional[float]] = [None for _ in histories]

    for day in daterange(first_date, today):
        day_str = day.isoformat()
        total = 0.0
        coverage_items = 0
        coverage_qty = 0
        volume = 0

        for idx, hist in enumerate(histories):
            points = hist["points"]
            while cursors[idx] < len(points) and points[cursors[idx]]["date"] <= day_str:
                latest_prices[idx] = points[cursors[idx]]["price"]
                volume += points[cursors[idx]].get("volume", 0)
                cursors[idx] += 1

            price = latest_prices[idx]
            if price is None:
                continue

            qty = int(hist["item"].get("qty", 0) or 0)
            total += price * qty
            coverage_items += 1
            coverage_qty += qty

        if coverage_items:
            rows.append({
                "date": day_str,
                "value": round(total, 2),
                "coverage_items": coverage_items,
                "coverage_qty": coverage_qty,
                "tracked_items": len(histories),
                "volume": volume,
                "currency": CURRENCY,
            })

    write_csv_dicts(
        BACKTRACK_CSV,
        ["date", "value", "coverage_items", "coverage_qty", "tracked_items", "volume", "currency"],
        rows,
    )


def write_item_snapshots(today: str, items: list) -> None:
    """
    Store daily item-level holdings and prices.

    This is the free, durable history source going forward: once snapshots exist
    for past dates, we can analyze what was actually owned then without relying
    on a third-party market-history service.
    """
    rows = []
    for item in items:
        qty = int(item.get("qty", 0) or 0)
        lowest = item.get("lowest")
        median = item.get("median")
        rows.append({
            "date": today,
            "account_id": item.get("account_id", ""),
            "account_label": item.get("account_label", ""),
            "name": item.get("name", ""),
            "qty": qty,
            "lowest": "" if lowest is None else lowest,
            "median": "" if median is None else median,
            "total_lowest": round((lowest or 0) * qty, 2),
            "total_median": round((median or 0) * qty, 2),
            "type": item.get("type") or "",
            "exterior": item.get("exterior") or "",
            "rarity": item.get("rarity") or "",
            "quality": item.get("quality") or "",
            "family": item.get("family") or "",
            "market_url": item.get("market_url") or market_url(item.get("name", "")),
            "currency": CURRENCY,
        })

    if rows:
        upsert_rows(
            ITEM_SNAPSHOTS_CSV,
            [
                "date", "account_id", "account_label", "name", "qty",
                "lowest", "median", "total_lowest", "total_median",
                "type", "exterior", "rarity", "quality", "family",
                "market_url", "currency",
            ],
            rows,
            key_fields=["date", "account_id", "name"],
        )

# ---------------------------------------------------------------------------
# Core per-account logic
# ---------------------------------------------------------------------------

def value_for_account(steam_id: str) -> list:
    """Returns list of item dicts: {name, qty, lowest, median}."""
    label = ACCOUNT_LABELS.get(steam_id, steam_id)
    dbg(f"Fetching inventory: {label} ({steam_id})")

    inv    = fetch_inventory(steam_id)
    market_items = get_marketable_items(inv, steam_id)
    total  = len(market_items)
    dbg(f"  {total} unique marketable items")

    items = []
    for i, (name, meta) in enumerate(market_items.items(), 1):
        qty = meta["qty"]
        cached_before = get_cached_price(name) is not None
        lowest, median = fetch_price(name)
        items.append({
            **meta,
            "account_id": steam_id,
            "account_label": label,
            "lowest": lowest,
            "median": median,
        })

        lo_s  = f"{lowest:.2f}"       if lowest  is not None else "N/A"
        med_s = f"{median:.2f}"       if median  is not None else "N/A"
        tot_s = f"median={median * qty:.2f}" if median is not None else "median=N/A"
        dbg(f"  [{i}/{total}] {name!r} ×{qty} → L:{lo_s} M:{med_s} {tot_s}")

        if not cached_before:
            time.sleep(SLEEP_MS / 1000.0)

    return items


def sum_values(items: list) -> tuple:
    """
    Returns (total_lowest, total_median) as rounded floats.
    Lowest uses priceoverview first, then active listing fallback.
    Median remains the primary value used by dashboard and account totals.
    """
    no_lowest = [d["name"] for d in items if d["lowest"] is None]
    no_median = [d["name"] for d in items if d["median"] is None]

    if no_lowest:
        print(f"  NOTE: {len(no_lowest)} item(s) still had no active listing lowest price: "
              f"{', '.join(no_lowest[:5])}"
              f"{'…' if len(no_lowest) > 5 else ''}")

    if no_median:
        print(f"  WARNING: {len(no_median)} item(s) had no median price and are excluded "
              f"from the median total: {', '.join(no_median[:5])}"
              f"{'…' if len(no_median) > 5 else ''}")

    lowest = sum(d["lowest"] * d["qty"] for d in items if d["lowest"] is not None)
    median = sum(d["median"] * d["qty"] for d in items if d["median"] is not None)
    return round(lowest, 2), round(median, 2)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(backfill_history: bool = False) -> None:
    today = dt.date.today().isoformat()

    all_items:    list = []
    acc_rows:     list = []
    failed_ids:   list = []
    total_lowest: float = 0.0
    total_median: float = 0.0

    for sid in STEAM_IDS:
        try:
            items = value_for_account(sid)
            all_items.extend(items)
            lo, med = sum_values(items)
            total_lowest += lo
            total_median += med
            acc_rows.append({"date": today, "steam_id": sid, "value_eur": round(med, 2)})
            print(f"  {ACCOUNT_LABELS.get(sid, sid)}: "
                  f"median={med} {CURRENCY}, lowest={lo} {CURRENCY}")
        except Exception as exc:
            failed_ids.append(sid)
            print(f"ERROR processing {sid}: {exc}")

    total_lowest = round(total_lowest, 2)
    total_median = round(total_median, 2)
    partial = len(failed_ids) > 0

    # --- values.csv ---
    # Do not write a combined total if any account failed — a partial sum
    # would silently corrupt the historical record.
    if partial:
        print(f"\nWARNING: Skipping values.csv write — "
              f"{len(failed_ids)} account(s) failed: {failed_ids}")
    else:
        upsert_rows(
            VALUES_CSV,
            ["date", "lowest", "median", "currency"],
            [{"date": today, "lowest": total_lowest, "median": total_median, "currency": CURRENCY}],
            key_fields=["date"],
        )
        backfill_missing_daily_rows(VALUES_CSV, numeric_fields=("lowest", "median"))

    # --- accounts.csv — write only the rows that succeeded ---
    if acc_rows:
        upsert_rows(
            ACCOUNTS_CSV,
            ["date", "steam_id", "value_eur", "currency"],
            [{**row, "currency": CURRENCY} for row in acc_rows],
            key_fields=["date", "steam_id"],
        )

    if not all_items:
        print("\nWARNING: Skipping items.json write — no inventory items were fetched successfully")
        write_price_cache()
        write_history_cache()
        return

    # --- items.json: aggregate across all accounts, sort by median total value ---
    merged: dict = {}
    for d in all_items:
        n = d["name"]
        if n not in merged:
            merged[n] = {
                **{k: d.get(k) for k in (
                    "name", "display_name", "type", "exterior", "quality", "rarity",
                    "weapon", "collection", "tournament", "family", "commodity",
                    "tradable", "marketable", "name_color", "background_color",
                    "icon_url", "icon_url_large", "inspect_url", "market_url", "tags",
                )},
                "qty": 0,
                "accounts": {},
                "lowest": d["lowest"], "median": d["median"],
            }
        else:
            # Keep the freshest non-None price
            if d["lowest"] is not None: merged[n]["lowest"] = d["lowest"]
            if d["median"] is not None: merged[n]["median"] = d["median"]
            if not merged[n].get("inspect_url") and d.get("inspect_url"):
                merged[n]["inspect_url"] = d["inspect_url"]
        merged[n]["qty"] += d["qty"]
        merged[n]["accounts"][d["account_label"]] = merged[n]["accounts"].get(d["account_label"], 0) + d["qty"]

    sorted_items = sorted(
        merged.values(),
        key=lambda x: (x["median"] or x["lowest"] or 0) * x["qty"],
        reverse=True,
    )
    write_item_snapshots(today, all_items)
    if backfill_history:
        backfill_market_history_once(sorted_items)
    write_backtracked_values(sorted_items)

    ITEMS_JSON.write_text(
        json.dumps(
            {
                "date":     today,
                "currency": CURRENCY,
                "partial":  partial,
                "primary_value": "median",
                "items": [
                    {
                        "name":         it["name"],
                        "qty":          it["qty"],
                        "lowest":       it["lowest"],
                        "median":       it["median"],
                        "total_lowest": round((it["lowest"] or 0) * it["qty"], 2),
                        "total_median": round((it["median"] or 0) * it["qty"], 2),
                        "type":         it.get("type"),
                        "exterior":     it.get("exterior"),
                        "quality":      it.get("quality"),
                        "rarity":       it.get("rarity"),
                        "weapon":       it.get("weapon"),
                        "collection":   it.get("collection"),
                        "tournament":   it.get("tournament"),
                        "family":       it.get("family"),
                        "commodity":    it.get("commodity"),
                        "tradable":     it.get("tradable"),
                        "icon_url":     it.get("icon_url"),
                        "icon_url_large": it.get("icon_url_large"),
                        "inspect_url":  it.get("inspect_url"),
                        "market_url":   it.get("market_url"),
                        "accounts":     it.get("accounts", {}),
                        "tags":         it.get("tags", {}),
                    }
                    for it in sorted_items
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_price_cache()
    write_history_cache()

    # --- Console summary ---
    print(f"\n✓ {today}: "
          f"median={total_median} {CURRENCY}, lowest={total_lowest} {CURRENCY}"
          + (" [PARTIAL]" if partial else ""))

    print(f"\nTop {min(TOP_ITEMS, len(sorted_items))} items by median total value:")
    for it in sorted_items[:TOP_ITEMS]:
        med_total = (it["median"] or 0) * it["qty"]
        med_s = f"{it['median']:.2f}" if it["median"] is not None else "N/A"
        print(f"  {it['name']!r:60s} ×{it['qty']}  "
              f"median_unit={med_s}  median_total={med_total:.2f} {CURRENCY}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and value CS2 Steam inventory.")
    parser.add_argument(
        "--backfill-history",
        action="store_true",
        help="one-time import of missing all-time market history for every current item",
    )
    args = parser.parse_args()
    main(backfill_history=args.backfill_history)
