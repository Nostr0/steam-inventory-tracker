# steam-inventory-tracker

Tracks a CS2 Steam inventory value over time and renders a local market-style
portfolio dashboard.

## Setup
1. Install Python 3.x.
2. Install dependencies: `pip install -r requirements.txt`
3. Edit `config.json` with your Steam IDs and settings.
4. Run `python fetch_inventory.py` to update values.
5. Serve the folder and open the dashboard:
   `python3 -m http.server 8000`
6. Open `http://localhost:8000/index.html`.

## Usage
- `fetch_inventory.py` fetches inventory data, Steam Market prices, item tags,
  images, inspect links, and market links.
- `values.csv` stores daily total history.
- `accounts.csv` stores per-account values.
- `item_snapshots.csv` stores daily item-level holdings and prices going
  forward. This is the free durable history source.
- `items.json` stores the latest top marketable items for the dashboard.
- `backtracked_values.csv` reconstructs what the currently tracked inventory
  would have been worth historically from public Steam Market graph data.
- `price_cache.json` caches market prices according to
  `price_cache_ttl_hours` in `config.json`.
- `item_history_cache.json` caches per-item Steam Market graph history.
- The dashboard supports search, type/exterior/rarity/account filters, price
  basis switching, table/card views, watchlist, CSV export, market-link copy,
  allocation breakdowns, backtracked value charts, point-of-interest event
  markers, and zoomable value charts.

## Backtracking
The backtracked chart is a reconstruction of the current inventory, not a
record of what was owned in the past. It answers: "what would the items I hold
now have been worth on that date?"

Normal runs do not fetch historical market graphs. They only use history that is
already saved in `item_history_cache.json`.

To do a one-time import for every current item that is not already cached:

```bash
python3 fetch_inventory.py --backfill-history
```

The import saves progress after every successful item and skips items already in
the cache, so interrupted runs can be resumed. Request sleeps are randomized
between `history_sleep_min_ms` and `history_sleep_max_ms` in `config.json`.

Steam's `pricehistory` endpoint may require a logged-in Steam web session. If
the import prints `400 Bad Request`, copy the full `Cookie` request header from
a logged-in browser request to `steamcommunity.com` and pass it without storing
it in the repo:

```bash
STEAM_COOKIE='sessionid=...; steamLoginSecure=...; steamCountry=...' python3 fetch_inventory.py --backfill-history
```

Alternatively, pass the two important cookie values separately:

```bash
STEAM_SESSIONID='...' STEAM_LOGIN_SECURE='...' python3 fetch_inventory.py --backfill-history
```

No paid third-party market API is used. Historical data before you started
tracking can only come from Steam's session-gated market graph data. Future
history is stored locally for free in `item_snapshots.csv`.
