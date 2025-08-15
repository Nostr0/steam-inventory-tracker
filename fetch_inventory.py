import csv
import os
from datetime import datetime
import requests

STEAM_ID = "76561198059817397"  # your SteamID64
CURRENCY = "eur"  # lowercase for API
CURRENCY_SYMBOL = "€"

VALUES_CSV = "values.csv"
ACCOUNTS_CSV = "accounts.csv"
ITEMS_CSV = "items.csv"

def fetch_inventory(steam_id):
    """Fetch inventory JSON from Steam API."""
    url = f"https://steamcommunity.com/inventory/{steam_id}/730/2?l=english&count=1000"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def get_item_counts(inv):
    """Aggregate counts per market hash name."""
    counts = {}
    assets = {a['assetid']: a for a in inv.get("assets", [])}
    for desc in inv.get("descriptions", []):
        name = desc["market_hash_name"]
        # Find how many assets match this classid/instanceid
        quantity = sum(1 for a in assets.values()
                       if a["classid"] == desc["classid"] and a["instanceid"] == desc["instanceid"])
        counts[name] = quantity
    return counts

def fetch_price(name):
    """Fetch item price from Steam Market."""
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {"appid": 730, "currency": 3, "market_hash_name": name}  # currency=3 for EUR
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        return None
    lowest_price = data.get("lowest_price") or data.get("median_price")
    if not lowest_price:
        return None
    clean = lowest_price.replace(CURRENCY_SYMBOL, "").replace(",", ".").strip()
    try:
        return float(clean)
    except ValueError:
        return None

def append_csv(filename, header, row):
    """Append row to CSV, create file if not exists."""
    exists = os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(header)
        writer.writerow(row)

# === Main script ===
today = datetime.utcnow().strftime("%Y-%m-%d")

print(f"[DEBUG] Fetching inventory for {STEAM_ID}")
inv = fetch_inventory(STEAM_ID)
counts = get_item_counts(inv)

total_value = 0.0
item_rows = []

for name, qty in counts.items():
    price = fetch_price(name)
    if price is None:
        print(f"[WARN] No price for {name}")
        continue
    total = price * qty
    total_value += total
    item_rows.append([today, STEAM_ID, name, qty, f"{price:.2f}", f"{total:.2f}"])
    print(f"[DEBUG] {name} | {qty}x @ {price:.2f}{CURRENCY_SYMBOL} → {total:.2f}{CURRENCY_SYMBOL}")

# Save totals
append_csv(
    VALUES_CSV,
    ["date", f"value_{CURRENCY}"],
    [today, f"{total_value:.2f} {CURRENCY_SYMBOL}"]
)

# Save per-item snapshot
for row in item_rows:
    append_csv(
        ITEMS_CSV,
        ["date", "steam_id", "item_name", "quantity", f"price_{CURRENCY}", f"total_value_{CURRENCY}"],
        row
    )

print(f"[INFO] Total inventory value: {total_value:.2f}{CURRENCY_SYMBOL}")
