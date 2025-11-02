import pandas as pd
import sqlite3
from pathlib import Path

# Paths
# Go up just one level (project root = folder that contains this script's parent)
ROOT = Path(__file__).resolve().parent.parent / "RiskPipelineProject"
DATA = ROOT / "raw_data"
DB_PATH = ROOT / "db" / "risk.sqlite"

# CSV files inside raw_data
POSITION_FILES = [
    ("Prop", DATA / "Position_Prop.csv"),
    ("Copper", DATA / "Position_Copper.csv"),
    ("ZincLead", DATA / "Position_ZincLead.csv"),
]
PRICES_FILE = DATA / "Historical Price.csv"

# --- Ingestion process ---
all_positions = []
for name, path in POSITION_FILES:
    df = pd.read_csv(path)
    df["business_line"] = name
    all_positions.append(df)
positions = pd.concat(all_positions, ignore_index=True)

prices = pd.read_csv(PRICES_FILE)

conn = sqlite3.connect(DB_PATH)
positions.to_sql("raw_positions", conn, if_exists="replace", index=False)
prices.to_sql("raw_prices", conn, if_exists="replace", index=False)
conn.close()

print(f"✅ Database created at: {DB_PATH}")
print(f"   raw_positions: {len(positions)} rows")
print(f"   raw_prices:    {len(prices)} rows")

