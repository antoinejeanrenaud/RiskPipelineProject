import Clean_process as cp
import var_utils as var
#import Ingest as ingest
import pandas as pd
from datetime import timedelta
from scipy.stats import norm
import sqlite3
import numpy as np
from pathlib import Path

from var_utils import calculate_parametric_var


def merge_latest_prices(positions_df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each row in positions_df, add the latest available QuoteValue
    from prices_df, matched by METAL, MATURITY, and EXCHANGE.

    Returns
    -------
    pd.DataFrame : positions_df with 'QuoteValue' and 'Price Date' columns added
    """
    # Normalize column names for consistent merge keys
    prices = prices_df.rename(columns={
        "Metal": "METAL",
        "Exchange": "EXCHANGE",
        "MTQuote": "QUOTE",
        "Price Date": "PRICE_DATE"
    }).copy()

    # Convert to datetime using exact format
    prices["PRICE_DATE"] = pd.to_datetime(prices["PRICE_DATE"], dayfirst=True, errors="coerce")


    # Drop rows with missing dates (invalid for grouping)
    prices = prices.dropna(subset=["PRICE_DATE"])

    # Get the latest price per (METAL, MATURITY, EXCHANGE)
    latest_prices = (
        prices.sort_values("PRICE_DATE")
              .groupby(["METAL", "MaturityMonth", "EXCHANGE"], as_index=False)
              .last()
    )

    # Keep only relevant columns to merge
    latest_prices = latest_prices[["METAL", "MaturityMonth", "EXCHANGE", "QUOTE", "PRICE_DATE"]]

    # Merge latest prices into positions
    merged = positions_df.merge(
        latest_prices,
        on=["METAL", "MaturityMonth", "EXCHANGE"],
        how="left"
    )

    # Rename for clarity
    merged = merged.rename(columns={
        "QUOTE": "QuoteValue",
        "PRICE_DATE": "Price Date"
    })

    return merged

def calculate_VaR_From_portfolio(positions_df, prices_df, conf=0.99, lookback = 365):
    positions_df = merge_latest_prices(positions_df, prices_df)
    # 1. Get unique combinations from positions_df
    valid_keys = positions_df[["METAL", "EXCHANGE", "MaturityMonth"]].drop_duplicates()

    # 2. Merge or filter prices_df to keep only matching rows
    filtered_prices = prices_df.rename(columns={
        "Metal": "METAL",
        "Exchange": "EXCHANGE",
    }).copy()
    filtered_prices = filtered_prices.merge(valid_keys, on=["METAL", "EXCHANGE", "MaturityMonth"], how="inner")

    filtered_prices = filtered_prices.rename(columns={
        "METAL": "Metal",
        "EXCHANGE": "Exchange",
    })

    VaR = var.calculate_parametric_var(positions_df, filtered_prices, conf, lookback)

    return VaR

positions_df, prices_df = cp.get_data()
calculate_VaR_From_portfolio(positions_df, prices_df)
