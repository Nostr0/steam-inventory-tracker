#!/usr/bin/env python3
import csv
import datetime as dt
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# Constants
CURRENCY_MAP = {
    "USD": 1,
    "EUR": 3,
    "GBP": 2,
    "RUB": 5,
    "BRL": 7,
    "JPY": 8,
    "NOK": 9,
    "IDR": 10,
    "MYR": 11,
    "PHP": 12,
    "SGD": 13,
    "THB": 14,
    "VND": 15,
    "KRW": 16,
    "TRY": 17,
    "UAH": 18,
    "MXN": 19,
    "CAD": 20,
    "AUD": 21,
    "NZD": 22,
    "CNY": 23,
    "INR": 24,
    "CLP": 25,
    "PEN": 26,
    "COP": 27,
    "ZAR": 28,
    "HKD": 29,
    "TWD": 30,
    "SAR": 31,
    "AED": 32,
    "ARS": 34,
    "ILS": 35,
    "BYN": 36,
    "KZT": 37,
    "KWD": 38,
    "QAR": 39,
    "CRC": 40,
    "UYU": 41,
}

class SteamInventoryTracker:
    def __init__(self, config_path: Path):
        self.root = config_path.parent
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        self.steam_ids = config.get("steam_ids", [])
        self.currency = config.get("currency", "EUR")
        self.sleep_ms = int(config.get("sleep_between_price_requests_ms", 5000))
        self.debug = config.get("debug", True)
        
        # Set up logging
        logging.basicConfig(level=logging.DEBUG if self.debug else logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.values_csv = self.root / "values.csv"
        self.accounts_csv = self.root / "accounts.csv"
        self.currency_code = CURRENCY_MAP.get(self.currency.upper(), 3)  # Default to EUR

    def log_debug(self, msg: str) -> None:
        if self.debug:
            self.logger.debug(msg)

    def ensure_csv_headers(self) -> None:
        if not self.values_csv.exists():
            with open(self.values_csv, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["date", "lowest", "median"])
        if not self.accounts_csv.exists():
            with open(self.accounts_csv, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["date", "steam_id", "value_eur"])

    def parse_price(self, text: str) -> float:
        """Parse a price string into a float, handling various formats."""
        if not text:
            return 0.0
        price_re = re.compile(r"(\d+[.,]?\d*(?:[.,]\d{2})?)")
        m = price_re.search(text.replace("\u202f", "").replace(" ", ""))
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

    def get_inventory(self, steam_id: str) -> dict:
        """Fetch inventory JSON from Steam API."""
        url = f"https://steamcommunity.com/inventory/{steam_id}/730/2?l=english&count=2000"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_item_counts(self, inv_json: dict) -> Dict[str, int]:
        """Extract item counts from inventory JSON."""
        desc_map = {
            (str(d.get("classid")), str(d.get("instanceid", "0"))): d
            for d in inv_json.get("descriptions", [])
        }
        counts = {}
        for asset in inv_json.get("assets", []):
            key = (str(asset.get("classid")), str(asset.get("instanceid", "0")))
            desc = desc_map.get(key)
            if desc:
                name = desc.get("market_hash_name")
                if name:
                    counts[name] = counts.get(name, 0) + 1
        return counts

    def get_item_price(self, name: str) -> Tuple[float, float]:
        """Fetch lowest and median prices for an item."""
        url = "https://steamcommunity.com/market/priceoverview/"
        response = requests.get(
            url,
            params={"appid": "730", "market_hash_name": name, "currency": self.currency_code},
            timeout=30
        )
        if response.status_code != 200:
            return 0.0, 0.0
        data = response.json()
        lowest = self.parse_price(data.get("lowest_price", ""))
        median = self.parse_price(data.get("median_price", ""))
        return lowest, median

    def compute_inventory_value(self, steam_id: str) -> Tuple[List[float], List[float]]:
        """Compute lowest and median values for an inventory."""
        self.log_debug(f"Computing Steam Market value for {steam_id}")
        inv = self.get_inventory(steam_id)
        counts = self.get_item_counts(inv)

        lowest_values = []
        median_values = []

        for i, (name, qty) in enumerate(counts.items(), start=1):
            lowest, median = self.get_item_price(name)
            lowest_values.extend([lowest] * qty)
            median_values.extend([median] * qty)
            self.log_debug(
                f"[{i}] '{name}' x{qty} → L:{lowest:.2f} M:{median:.2f} → {lowest * qty:.2f} total"
            )
            time.sleep(self.sleep_ms / 1000.0)

        return lowest_values, median_values

    def run(self) -> None:
        """Main execution: fetch values and write to CSVs."""
        today = dt.date.today().isoformat()
        self.ensure_csv_headers()

        per_account = []
        all_lowest = []
        all_median = []

        for sid in self.steam_ids:
            try:
                lowest_vals, median_vals = self.compute_inventory_value(sid)
                val = round(sum(lowest_vals), 2)
                all_lowest.extend(lowest_vals)
                all_median.extend(median_vals)
            except Exception as e:
                self.log_debug(f"Error fetching {sid}: {e}")
                val = 0.0
            per_account.append((sid, val))

        lowest = round(sum(all_lowest), 2)
        median = round(sum(all_median), 2)

        with open(self.accounts_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for sid, val in per_account:
                writer.writerow([today, sid, val])

        with open(self.values_csv, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([today, lowest, median])

        print(f"Recorded {today} lowest={lowest}; median={median}; accounts={per_account}")


def main():
    config_path = Path(__file__).parent / "config.json"
    tracker = SteamInventoryTracker(config_path)
    tracker.run()


if __name__ == "__main__":
    main()
