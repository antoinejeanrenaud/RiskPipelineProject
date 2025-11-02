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
