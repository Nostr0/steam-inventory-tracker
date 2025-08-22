#!/usr/bin/env python3
import csv, datetime as dt, json, os, re, time
from pathlib import Path
import requests

ROOT = Path(__file__).parent
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
STEAM_IDS = CONFIG.get("steam_ids", [])
CURRENCY = CONFIG.get("currency", "EUR")
SLEEP_MS = int(CONFIG.get("sleep_between_price_requests_ms", 5000))
DEBUG = CONFIG.get("debug", True)

VALUES_CSV = ROOT / "values.csv"
ACCOUNTS_CSV = ROOT / "accounts.csv"

# --- Helpers ---------------------------------------------------------------

def log_debug(msg):
    if DEBUG:
        print("[DEBUG]", msg)

def ensure_csv_headers():
    if not VALUES_CSV.exists():
        with open(VALUES_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "date",
                "lowest",
                "median"
            ])


PRICE_RE = re.compile(r"(\d+[.,]?\d*(?:[.,]\d{2})?)")

def parse_price(text: str) -> float:
    if not text: return 0.0
    m = PRICE_RE.search(text.replace("\u202f", "").replace(" ", ""))
    if not m: return 0.0
    raw = m.group(1)
    if "." in raw and "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw and "." not in raw:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return 0.0

# --- Steam inventory & prices ----------------------------------------------

def get_inventory(steam_id: str):
    url = f"https://steamcommunity.com/inventory/{steam_id}/730/2?l=english&count=2000"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def get_item_counts(inv_json):
    desc_map = {(str(d.get("classid")), str(d.get("instanceid", "0"))): d
                for d in inv_json.get("descriptions", [])}
    counts = {}
    for a in inv_json.get("assets", []):
        key = (str(a.get("classid")), str(a.get("instanceid", "0")))
        d = desc_map.get(key)
        if not d:
            d = next((x for x in inv_json.get("descriptions", []) if str(x.get("classid")) == key[0]), None)
        name = d.get("market_hash_name") if d else None
        if name:
            counts[name] = counts.get(name, 0) + 1
    return counts

def get_item_price(name: str):
    url = "https://steamcommunity.com/market/priceoverview/"
    r = requests.get(url, params={"appid": "730", "market_hash_name": name, "currency": 3}, timeout=30)
    if r.status_code != 200: 
        return 0.0, 0.0
    data = r.json()
    lowest = parse_price(data.get("lowest_price"))
    median = parse_price(data.get("median_price"))
    return lowest, median

def compute_inventory_value(steam_id: str):
    log_debug(f"Computing Steam Market value for {steam_id}")
    inv = get_inventory(steam_id)
    counts = get_item_counts(inv)

    lowest_values = []
    median_values = []

    for i, (name, qty) in enumerate(counts.items(), start=1):
        lowest, median = get_item_price(name)
        lowest_values.extend([lowest] * qty)
        median_values.extend([median] * qty)
        log_debug(f"[{i}] '{name}' x{qty} → L:{lowest:.2f} M:{median:.2f} → {lowest * qty:.2f} total")
        time.sleep(SLEEP_MS / 1000.0)

    return lowest_values, median_values


# --- Main ---------------------------------------------------------------

def main():
    today = dt.date.today().isoformat()
    ensure_csv_headers()

    per_account = []
    all_lowest = []
    all_median = []

    for sid in STEAM_IDS:
        try:
            val, lowest_vals, median_vals = compute_inventory_value(sid)
            all_lowest.extend(lowest_vals)
            all_median.extend(median_vals)
        except Exception as e:
            log_debug(f"Error fetching {sid}: {e}")
            val = 0.0
        per_account.append((sid, val))

    # Durchschnittswerte berechnen
    lowest = round(sum(all_lowest) / len(all_lowest), 2) if all_lowest else 0.0
    median = round(sum(all_median) / len(all_median), 2) if all_median else 0.0

    with open(ACCOUNTS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for sid, val in per_account:
            w.writerow([today, sid, val])

    with open(VALUES_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([today, lowest, median])

    print(f"Recorded {today} lowest={lowest}; median={median}; accounts={per_account}")

if __name__ == "__main__":
    main()
