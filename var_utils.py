import pandas as pd
from datetime import timedelta
from scipy.stats import norm
import numpy as np


def compute_covariance_matrix(prices_df: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """

    Compute the covariance matrix of daily returns for all instruments
    defined by (Metal, MaturityMonth, Exchange), over a given lookback period.

    Parameters
    ----------
    prices_df : pd.DataFrame
        Must contain columns: ['Price Date', 'Metal', 'QuoteValue', 'MaturityMonth', 'Exchange']
    lookback_days : int
        Number of days to look back from the most recent date

    Returns
    -------
    cov_matrix : pd.DataFrame
        Covariance matrix of daily returns (rows and cols = instruments)
    """

    # Clean column names
    df = prices_df.rename(columns={
        "Price Date": "date",
        "Metal": "metal",
        "MTQuote": "price",
        "MaturityMonth": "maturity",
        "Exchange": "exchange"
    }).copy()

    # Convert date
    df["date"] = pd.to_datetime(df["date"],dayfirst=True, errors="coerce")
    df = df.dropna(subset=["date", "price", "metal", "maturity", "exchange"])

    # Create a unique instrument ID: e.g. Copper_Oct-2024_LME
    df["instrument"] = df["metal"] + "_" + df["maturity"] + "_" + df["exchange"]

    # Filter to the lookback period
    max_date = df["date"].max()
    start_date = max_date - timedelta(days=lookback_days)
    df = df[df["date"] >= start_date]

    # Pivot prices into time series matrix
    pivoted = df.pivot(index="date", columns="instrument", values="price").sort_index()
    pivoted = pivoted.ffill()
    pivoted.dropna(inplace=True,axis=0)
    # Compute daily returns
    returns = pivoted.pct_change().dropna(how="all")

    # Covariance matrix of returns
    cov_matrix = returns.cov()

    return cov_matrix


def compute_portfolio_value(positions_df: pd.DataFrame) -> float:
    """
    Compute the total value of the portfolio in monetary terms.

    Assumes that 'Net_Volume' and 'QuoteValue' columns exist.

    Returns
    -------
    float : total value of the portfolio
    """
    df = positions_df.copy()
    df["position_value"] = df["Net_Volume"].abs() * df["QuoteValue"]
    return df["position_value"].sum()

def compute_asset_weights(positions_df: pd.DataFrame) -> pd.Series:
    """
    Compute portfolio weights that are signed (positive for long, negative for short)
    but normalized by the total gross exposure to avoid division by zero.

    Returns
    -------
    pd.Series : index = (METAL, MaturityMonth, EXCHANGE), values = signed weights
    """
    df = positions_df.copy()

    # Signed position values
    df["signed_position_value"] = df["Net_Volume"] * df["QuoteValue"]

    # Gross portfolio value (denominator)
    gross_value = df["signed_position_value"].abs().sum()

    if gross_value == 0:
        raise ValueError("Total gross portfolio value is zero — cannot compute weights")

    # Group by instrument
    grouped = df.groupby(["METAL", "MaturityMonth", "EXCHANGE"])["signed_position_value"].sum()

    # Normalize by gross value (not net!)
    weights = grouped / gross_value

    return weights

def compute_z_score(confidence_level: float) -> float:
    """
    Get the Z-score corresponding to a given confidence level (e.g., 0.99).

    Parameters
    ----------
    confidence_level : float
        Confidence level (between 0 and 1)

    Returns
    -------
    float : corresponding z-value from standard normal distribution
    """
    return norm.ppf(confidence_level)


def compute_parametric_var(weights: pd.Series, cov_matrix: pd.DataFrame, z_score: float, portfolio_value: float) -> float:
    """
    Compute the parametric (variance-covariance) VaR.

    Parameters
    ----------
    weights : pd.Series
        Portfolio weights indexed by instrument
    cov_matrix : pd.DataFrame
        Covariance matrix of returns
    z_score : float
        Z value from standard normal for desired confidence level
    portfolio_value : float
        Total portfolio value in currency

    Returns
    -------
    float : VaR in monetary terms
    """
    # Reformat the index of weights to match the instrument names in cov_matrix
    weights.index = [
        f"{metal}_{maturity}_{exchange}"
        for metal, maturity, exchange in weights.index
    ]

    w = weights.reindex(cov_matrix.columns).fillna(0).values.reshape(-1, 1)
    var_pct = z_score * np.sqrt((w.T @ cov_matrix.values @ w).item())
    return portfolio_value * var_pct


def calculate_parametric_var(
    positions_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    confidence_level: float,
    lookback_days: int
) -> float:
    """
    Full pipeline to compute parametric VaR from positions and prices.

    Parameters
    ----------
    positions_df : pd.DataFrame
        Must contain columns: ['METAL', 'MATURITY', 'EXCHANGE', 'Net_Volume', 'QuoteValue']
    prices_df : pd.DataFrame
        Must contain columns: ['Price Date', 'Metal', 'QuoteValue']
    confidence_level : float
        e.g. 0.99 for 99% confidence
    lookback_days : int
        Number of days to look back for return covariance

    Returns
    -------
    float : Value-at-Risk in monetary terms
    """
    # Step 1: Get Z-score
    z = compute_z_score(confidence_level)

    # Step 2: Compute covariance matrix from historical prices
    cov_matrix = compute_covariance_matrix(prices_df, lookback_days)

    # Step 3: Compute portfolio value
    portfolio_value = compute_portfolio_value(positions_df)

    # Step 4: Compute weights based on absolute position value
    weights_series = compute_asset_weights(positions_df)

    # Step 6: Compute final VaR
    var = compute_parametric_var(weights_series, cov_matrix, z, portfolio_value)
    return var


############# Historical Helpers

def merge_prices_on_date(positions_df: pd.DataFrame, prices_df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    For each row in positions_df, add the QuoteValue from prices_df
    for the specified date, matched by METAL, MaturityMonth, and EXCHANGE.

    Parameters
    ----------
    positions_df : pd.DataFrame
        Portfolio with columns: METAL, MaturityMonth, EXCHANGE, Net_Volume, etc.

    prices_df : pd.DataFrame
        Historical prices with columns: Metal, MaturityMonth, Exchange, Price Date, MTQuote

    date : str
        Target pricing date (format: 'YYYY-MM-DD')

    Returns
    -------
    pd.DataFrame
        positions_df with 'QuoteValue' and 'Price Date' (if match exists)
    """

    # Normalize column names
    prices = prices_df.rename(columns={
        "Metal": "METAL",
        "Exchange": "EXCHANGE",
        "MTQuote": "QUOTE",
        "Price Date": "PRICE_DATE"
    }).copy()

    # Ensure date column is datetime
    prices["PRICE_DATE"] = pd.to_datetime(prices["PRICE_DATE"], dayfirst=True, errors="coerce")

    # Target date as datetime
    target_date = pd.to_datetime(date)

    # Filter for target date only
    prices_on_date = prices[prices["PRICE_DATE"] == target_date]

    # Merge on exact match: METAL, MaturityMonth, EXCHANGE
    merged = positions_df.merge(
        prices_on_date[["METAL", "MaturityMonth", "EXCHANGE", "QUOTE", "PRICE_DATE"]],
        on=["METAL", "MaturityMonth", "EXCHANGE"],
        how="left"
    )

    # Rename for clarity
    merged = merged.rename(columns={
        "QUOTE": "QuoteValue",
        "PRICE_DATE": "Price Date"
    })

    return merged

def compute_portfolio_value_time_series(positions_df, prices_df, lookback_days=365):
    """
    Compute time series of total portfolio value using historical prices.

    Returns
    -------
    pd.DataFrame with columns: ['Date', 'PortfolioValue']
    """

    # Normalize column names
    prices = prices_df.rename(columns={
        "Metal": "METAL",
        "Exchange": "EXCHANGE",
        "MTQuote": "QUOTE",
        "Price Date": "PRICE_DATE"
    }).copy()

    prices["PRICE_DATE"] = pd.to_datetime(prices["PRICE_DATE"], dayfirst=True, errors="coerce")
    prices = prices.dropna(subset=["PRICE_DATE"])

    # Get unique dates within lookback period
    max_date = prices["PRICE_DATE"].max()
    start_date = max_date - timedelta(days=lookback_days)
    unique_dates = prices[(prices["PRICE_DATE"] >= start_date)]["PRICE_DATE"].drop_duplicates().sort_values()

    # Prepare base position data
    pos = positions_df.copy()

    all_results = []

    for date in unique_dates:
        # Get prices for this date
        day_prices = prices[prices["PRICE_DATE"] == date][
            ["METAL", "MaturityMonth", "EXCHANGE", "QUOTE"]
        ].copy()

        # Merge into positions
        merged = pos.merge(
            day_prices,
            how="left",
            on=["METAL", "MaturityMonth", "EXCHANGE"]
        )

        # Check how many prices are missing
        missing_count = merged["QUOTE"].isna().sum()

        if missing_count > 0:
            print(f"⚠️ {missing_count} missing prices on {date.date()}, skipping that day.")
            continue  # skip day with incomplete pricing

        # Compute position value
        merged["position_value"] = merged["Net_Volume"] * merged["QUOTE"]
        total_value = merged["position_value"].sum()

        all_results.append({"Date": date, "PortfolioValue": total_value})

    return pd.DataFrame(all_results)

# def compute_historical_var_pct(positions_df: pd.DataFrame,
#                            prices_df: pd.DataFrame,
#                            lookback_days: int = 365,
#                            confidence: float = 0.99) -> float:
#     """
#     Compute Historical (non-parametric) Value-at-Risk for a given portfolio.
#
#     Parameters
#     ----------
#     positions_df : pd.DataFrame
#         Current portfolio with Net_Volume, METAL, EXCHANGE, MaturityMonth, etc.
#     prices_df : pd.DataFrame
#         Historical prices with columns: Metal, Exchange, MaturityMonth, MTQuote, Price Date.
#     lookback_days : int, default=365
#         Number of calendar days to look back for the historical simulation.
#     confidence : float, default=0.99
#         Confidence level (e.g. 0.99 for 99% VaR).
#
#     Returns
#     -------
#     float : Historical VaR (positive number, monetary units)
#     """
#
#     ts_df = compute_portfolio_value_time_series(positions_df, prices_df, lookback_days)
#
#     if ts_df.empty or "PortfolioValue" not in ts_df.columns:
#         print("⚠️ Portfolio value time series is empty or missing required columns.")
#         return float("nan")
#
#     ts_df = ts_df.sort_values("Date").reset_index(drop=True)
#
#     # Compute returns manually
#     returns = []
#     for i in range(1, len(ts_df)):
#         prev = ts_df.loc[i - 1, "PortfolioValue"]
#         curr = ts_df.loc[i, "PortfolioValue"]
#
#         if prev == 0 and curr == 0:
#             ret = 0.0
#         elif prev == 0:
#             # Undefined return, skip
#             ret = float("nan")
#         else:
#             ret = (curr - prev) / abs(prev)
#
#         returns.append(ret)
#
#     # Insert returns into DataFrame (align by trimming first row)
#     ts_df = ts_df.iloc[1:].copy()
#     ts_df["Return"] = returns
#
#     # Clean up invalid returns
#     ts_df = ts_df.replace([float("inf"), float("-inf")], float("nan"))
#     ts_df = ts_df.dropna(subset=["Return"])
#
#     if ts_df["Return"].empty:
#         print("⚠️ No valid returns after cleanup.")
#         return float("nan")
#
#     # Compute VaR
#     var_quantile = 1 - confidence
#     var_value = -ts_df["Return"].quantile(var_quantile)
#
#     latest_value = abs(ts_df["PortfolioValue"].iloc[-1])
#     var_monetary = latest_value * var_value
#
#     print(f"Historical {int(confidence * 100)}% VaR over {lookback_days} days: ${var_monetary:,.0f}")
#     return var_monetary

def compute_historical_var(positions_df: pd.DataFrame,
                           prices_df: pd.DataFrame,
                           lookback_days: int = 365,
                           confidence: float = 0.99) -> float:
    """
    Compute Historical (non-parametric) Value-at-Risk for a given portfolio using P&L differences.

    Parameters
    ----------
    positions_df : pd.DataFrame
        Current portfolio with Net_Volume, METAL, EXCHANGE, MaturityMonth, etc.
    prices_df : pd.DataFrame
        Historical prices with columns: Metal, Exchange, MaturityMonth, MTQuote, Price Date.
    lookback_days : int, default=365
        Number of calendar days to look back for the historical simulation.
    confidence : float, default=0.99
        Confidence level (e.g. 0.99 for 99% VaR).

    Returns
    -------
    float : Historical VaR (positive number, monetary units)
    """

    ts_df = compute_portfolio_value_time_series(positions_df, prices_df, lookback_days)

    if ts_df.empty or "PortfolioValue" not in ts_df.columns:
        print("⚠️ Portfolio value time series is empty or missing required columns.")
        return float("nan")

    ts_df = ts_df.sort_values("Date").reset_index(drop=True)

    # Compute daily P&L (differences in absolute portfolio value)
    ts_df["PnL"] = ts_df["PortfolioValue"].diff()

    # Drop NaN in PnL (first day)
    ts_df = ts_df.dropna(subset=["PnL"])

    if ts_df["PnL"].empty:
        print("⚠️ No valid P&L values.")
        return float("nan")

    # Compute quantile of P&L distribution
    var_quantile = 1 - confidence
    var_value = -ts_df["PnL"].quantile(var_quantile)  # Take negative to get loss

    print(f"Historical {int(confidence * 100)}% VaR over {lookback_days} days (P&L): ${var_value:,.0f}")
    return var_value
