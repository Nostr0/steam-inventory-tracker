#!/usr/bin/env python3
import csv, datetime as dt, json, os, re, time
from pathlib import Path
import requests

ROOT = Path(__file__).parent
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
STEAM_IDS = CONFIG.get("steam_ids", [])
CURRENCY = CONFIG.get("currency", "EUR")
SLEEP_MS = int(CONFIG.get("sleep_between_price_requests_ms", 5000))
DEBUG = CONFIG.get("debug", False)

VALUES_CSV = ROOT / "values.csv"         # date,total_value
ACCOUNTS_CSV = ROOT / "accounts.csv"     # date,steam_id,value

# --- Helpers ---------------------------------------------------------------

def log_debug(msg):
    if DEBUG:
        print("[DEBUG]", msg)


def ensure_csv_headers():
    if not VALUES_CSV.exists():
        with open(VALUES_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["date", f"value_{CURRENCY.lower()}"])
    if not ACCOUNTS_CSV.exists():
        with open(ACCOUNTS_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["date", "steam_id", f"value_{CURRENCY.lower()}"])

PRICE_RE = re.compile(r"(\d+[.,]?\d*(?:[.,]\d{2})?)")

def parse_price(text: str) -> float:
    if not text:
        return 0.0
    m = PRICE_RE.search(text.replace("\u202f", " ").replace(" ", ""))
    if not m:
        return 0.0
    raw = m.group(1)
    if "." in raw and "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw and "." not in raw:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return 0.0

def get_inventory(steam_id: str):
    url = f"https://steamcommunity.com/inventory/{steam_id}/730/2?l=english&count=2000"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def get_item_counts(inv_json):
    desc_map = {}
    for d in inv_json.get("descriptions", []):
        key = (str(d.get("classid")), str(d.get("instanceid", "0")))
        desc_map[key] = d

    counts = {}
    for a in inv_json.get("assets", []):
        key = (a.get("classid"), a.get("instanceid", "0"))
        d = desc_map.get((str(key[0]), str(key[1])))
        if not d:
            d = next((x for x in inv_json.get("descriptions", []) if str(x.get("classid")) == str(key[0])), None)
        name = d.get("market_hash_name") if d else None
        if not name:
            continue
        counts[name] = counts.get(name, 0) + 1
    return counts

def priceoverview(name: str) -> float:
    url = "https://steamcommunity.com/market/priceoverview/"
    r = requests.get(url, params={"appid": "730", "market_hash_name": name}, timeout=30)
    if r.status_code != 200:
        return 0.0
    data = r.json()
    price_str = data.get("lowest_price") or data.get("median_price")
    return parse_price(price_str)

def try_csgobackpack_value(steam_id: str, currency: str) -> float | None:
    url = "https://csgobackpack.net/api/GetInventoryValue/"
    log_debug(f"Fetching total value from CSGOBackpack for {steam_id}")
    try:
        r = requests.get(url, params={"id": steam_id, "currency": currency}, timeout=30)
        r.raise_for_status()
        data = r.json()
        val = data.get("value")
        log_debug(f"CSGOBackpack response: {data}")
        return float(val) if val is not None else None
    except Exception as e:
        log_debug(f"CSGOBackpack failed: {e}")
        return None

def try_csfloat_value(steam_id: str) -> float | None:
    url = f"https://api.csfloat.com/market/value/{steam_id}"
    log_debug(f"Fetching total value from CSFloat for {steam_id}")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        val = data.get("data", {}).get("value")
        log_debug(f"CSFloat response: {data}")
        return float(val) if val is not None else None
    except Exception as e:
        log_debug(f"CSFloat failed: {e}")
        return None

def compute_value_via_market(steam_id: str) -> float:
    log_debug(f"Falling back to Steam Market for {steam_id}")
    inv = get_inventory(steam_id)
    counts = get_item_counts(inv)
    total = 0.0
    for i, (name, qty) in enumerate(counts.items(), start=1):
        price = priceoverview(name)
        if price == 0.0:
            log_debug(f"[{i}] '{name}' x{qty} → FAILED to fetch price")
        else:
            log_debug(f"[{i}] '{name}' x{qty} → {price:.2f} each → {price * qty:.2f} total")
        total += price * qty
        time.sleep(SLEEP_MS / 1000.0)
    return round(total, 2)

def get_value_for_account(steam_id: str) -> float:
    # Try in order: CSFloat → CSGOBackpack → Steam Market
    v = try_csfloat_value(steam_id)
    if isinstance(v, float) and v > 0:
        log_debug(f"Using CSFloat total: {v:.2f} {CURRENCY}")
        return round(v, 2)

    v = try_csgobackpack_value(steam_id, CURRENCY)
    if isinstance(v, float) and v > 0:
        log_debug(f"Using CSGOBackpack total: {v:.2f} {CURRENCY}")
        return round(v, 2)

    log_debug("CSFloat and CSGOBackpack returned no value, using Steam Market fallback")
    return compute_value_via_market(steam_id)

# --- Run -------------------------------------------------------------------

def main():
    today = dt.date.today().isoformat()
    ensure_csv_headers()

    per_account = []
    for sid in STEAM_IDS:
        try:
            val = get_value_for_account(sid)
        except Exception as e:
            val = 0.0
        per_account.append((sid, val))

    total = round(sum(v for _, v in per_account), 2)

    with open(ACCOUNTS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for sid, val in per_account:
            w.writerow([today, sid, val])

    with open(VALUES_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([today, total])

    print(f"Recorded {today} total={total} {CURRENCY}; accounts={per_account}")

if __name__ == "__main__":
    main()
