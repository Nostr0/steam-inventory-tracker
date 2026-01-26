# steam-inventory-tracker
Tracks my csgo inventory value over time :3

## Setup
1. Install Python 3.x.
2. Install dependencies: `pip install -r requirements.txt`
3. Edit `config.json` with your Steam IDs and settings.
4. Run `python fetch_inventory.py` to update values.
5. Open `index.html` in a browser to view the chart.

## Usage
- The script fetches inventory data and prices, logging to CSVs.
- The HTML page visualizes the data with Chart.js.
- Supports zooming/panning on the chart.
