import Clean_process as cp
import var_utils as var
import Ingest as ingest
import pandas as pd

from var_utils import calculate_parametric_var

print("Step 1: Importing Raw Data in DB")
ingest.save_raw_data_to_db()

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

def calculate_VaR_from_portfolio(positions_df, prices_df, conf=0.99, lookback = 365):
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

    VaR = round(var.calculate_parametric_var(positions_df, filtered_prices, conf, lookback),2)

    return VaR


def calculate_VaR(levels=["Total", "BUSINESS LINE"], conf=0.99, lookback=365, T=1):
    print("Step 2: Loading Data positions and prices from DB to perform VaR calculation.")
    positions_df, prices_df = cp.get_data()

    VaR_Total = "error"
    VaR_Levels = {}

    for level in levels:
        if level == "Total":
            try:
                VaR_Total = calculate_VaR_from_portfolio(positions_df, prices_df, conf, lookback) * (T ** 0.5)
            except Exception as e:
                print(f" Error calculating Total VaR: {e}")
        else:
            try:
                if level not in positions_df.columns:
                    print(f" Column '{level}' not found in positions data.")
                    continue

                VaR_Levels[level] = {}
                elements = list(positions_df[level].unique())

                for el in elements:
                    sub_df = positions_df[positions_df[level] == el].copy()
                    VaR_el = calculate_VaR_from_portfolio(sub_df, prices_df, conf, lookback) * (T ** 0.5)
                    VaR_Levels[level][el] = VaR_el

            except Exception as e:
                print(f" Error calculating VaR for level '{level}': {e}")

    return VaR_Total, VaR_Levels



def export_var_to_excel(var_total, var_levels, output_path="results_var_summary.xlsx"):
    """
    Export VaR results to an Excel file with:
    - Sheet 1: Total Portfolio VaR
    - Sheet 2+: Business line-level VaRs (one sheet per level)

    Parameters
    ----------
    var_total : float
        The total portfolio VaR.

    var_levels : dict
        Dictionary where keys are levels (e.g., "BUSINESS LINE") and values are
        dictionaries of individual VaRs per group.

    output_path : str
        Path to save the Excel file.
    """

    # Create Excel writer
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Total VaR sheet
        df_total = pd.DataFrame({"VaR": [var_total]})
        df_total.to_excel(writer, sheet_name="Total_VaR", index=False)

        # VaR by level sheets
        for level_name, level_dict in var_levels.items():
            df_level = pd.DataFrame.from_dict(level_dict, orient="index", columns=["VaR"])
            df_level.index.name = level_name
            df_level.reset_index(inplace=True)
            sheet_name = f"VaR_by_{level_name.replace(' ', '_')[:31]}"  # Excel sheet name limit
            df_level.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ VaR results exported to: {output_path}")


positions_df, prices_df = cp.get_data()
#calculate_VaR_from_portfolio(positions_df, prices_df)

result = calculate_VaR(T=1)

print("Saving Results to Excel...")
export_var_to_excel(result[0],result[1])

