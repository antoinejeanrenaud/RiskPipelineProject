import pandas as pd
import sqlite3
import numpy as np
from pathlib import Path

# Paths
ROOT = Path(__file__).resolve().parent.parent / "RiskPipelineProject"
DB_PATH = ROOT / "db" / "risk.sqlite"

# ---- Define column types ----
POS_TYPES = {
    "MATURITY": "date",
    "CONTRACTTYPE": "str",
    "BUSINESS LINE": "str",
    "STRATEGY": "str",
    "METAL": "str",
    "EXCHANGE": "str",
    "CURRENCY": "str",
    "LONGSHORT": "str",
    "VOLUME": "float",
    "UNIT": "str",
    "business_line": "str"
}

PRICE_TYPES = {
    "Price Date": "date",
    "Maturity": "date",
    "QuoteValue": "float",
    "Metal": "str",
    "Exchange": "str",
    "Unit": "str"
}

DATE_FMT = "%d/%m/%Y"


def clean_table(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
    """Convert DataFrame columns according to the given type mapping."""

    # 🔹 Clean column names first (remove spaces, keep consistent case)
    df.columns = df.columns.str.strip()

    for col, dtype in type_map.items():
        if col in df.columns:
            if dtype == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime(DATE_FMT)
            elif dtype == "float":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .astype(float)
                )
            elif dtype == "str":
                df[col] = df[col].astype(str).str.strip()
    return df



def load_and_clean_data(db_path: Path = DB_PATH):
    """Load both raw tables, clean them, and return as pandas DataFrames."""
    conn = sqlite3.connect(db_path)

    # Load tables
    # Path to the SQL file
    #sql_path = ROOT / "sql/prices_query.txt"

    # Read the query from the file
    #with open(sql_path, "r") as f:
    #   sql = f.read()


    pos = pd.read_sql_query("SELECT * FROM raw_positions", conn)  # <- use read_sql_query for clarity

    prices = pd.read_sql("SELECT * FROM raw_prices", conn)
    conn.close()

    # Clean each table
    pos_clean = clean_table(pos, POS_TYPES)
    prices_clean = clean_table(prices, PRICE_TYPES)

    print("✅ Data loaded and cleaned.")
    print(f"   Positions rows: {len(pos_clean)}")
    print(f"   Prices rows:    {len(prices_clean)}")

    return pos_clean, prices_clean


def transform_position(df):
    """
    Convert position volumes to metric tons (MT) and apply long/short sign.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: ['VOLUME', 'UNIT', 'LONGSHORT']

    Returns
    -------
    pd.DataFrame
        Original dataframe with two new columns:
        - MT_Volume: volume converted to metric tons
        - Net_Volume: signed volume (+ for L, - for S)
    """

    # conversion factors
    unit_transform = {
        "LB": 0.0004536,  # pounds -> metric tons
        "MT": 1.0,        # already in metric tons
    }

    final_df = df.copy()

    # convert to MT
    final_df["MT_Volume"] = final_df["VOLUME"] * final_df["UNIT"].map(unit_transform).fillna(1)

    # apply long/short sign
    final_df["Net_Volume"] = np.where(final_df["LONGSHORT"] == "L",
                                      final_df["MT_Volume"],
                                      -final_df["MT_Volume"])

    final_df["MATURITY"] = pd.to_datetime(final_df["MATURITY"], dayfirst=True, errors="coerce")
    final_df["MaturityMonth"] = final_df["MATURITY"].dt.strftime("%b-%Y")

    return final_df

def transform_prices(df):
    # conversion factors
    unit_transform = {
        "USD/LB": 0.0004536,  # pounds -> metric tons
        "USD/MT": 1.0,  # already in metric tons
    }

    final_df = df.copy()

    # convert to MT
    final_df["MTQuote"] = final_df["QuoteValue"] / final_df["Unit"].map(unit_transform).fillna(1)
    final_df["Maturity"] = pd.to_datetime(final_df["Maturity"], dayfirst=True, errors="coerce")
    final_df["MaturityMonth"] = final_df["Maturity"].dt.strftime("%b-%Y")

    return final_df

def get_data():
    positions_df, prices_df = load_and_clean_data()
    positions_df = transform_position(positions_df)
    prices_df = transform_prices(prices_df)

    return positions_df, prices_df


#positions_df, prices_df = load_and_clean_data()


#positions_df = transform_position(positions_df)
#prices_df = transform_prices(prices_df)

#rint(positions_df)
#rint(prices_df.head())





