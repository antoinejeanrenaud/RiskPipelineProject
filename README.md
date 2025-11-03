# Risk Analytics Pipeline — VaR Calculation & Reporting

## Overview

This project implements a modular Python pipeline to calculate historical Value-at-Risk (VaR) using position and price data. It demonstrates, data cleaning and handling, time series analysis, and financial risk analytics using Python.

## Structure

- `Ingest.py`: Loads raw CSV files and saves them to a SQLite database (`db/risk.sqlite`). The database (risk.sqlite) is built using the save_raw_data_to_db() function in Ingest.py, which loads data from CSV and writes it to the database. Therefore, there is no separate .sql script — it is fully managed via Python
- `Clean_process.py`: Loads data from the database and applies cleaning/standardization (e.g., fixing types, removing whitespace).
- `var_utils.py`: Contains the full set of utilities to compute:
  - Covariance matrix of returns
  - Portfolio value and weights
  - Z-score for confidence level
  - Parametric VaR
- `VaR.py`: Coordinates the pipeline — loads data, processes it, calculates VaR (by business line and total), and writes a basic Excel report.

## Data

- Input files:
  - `raw_data/Position_Prop.csv`
  - `raw_data/Position_Copper.csv`
  - `raw_data/Position_ZincLead.csv`
  - `raw_data/Historical Price.csv`

- Output:
  - `db/risk.sqlite` — contains `raw_positions` and `raw_prices`
  - Excel report with total and business line VaR

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the VaR script

Select if you want Parametric VaR or historical version
Select confidence level wanted for the VaR (0.95, 0.99)
Select the number of days looking forward for the VaR using T as variable set by default to 1 (Only for parametric)
Select the lookbback period that is set by default to 365 days

```

This will process the data, calculate VaR, and export an Excel report.

## Assumptions & Logic

- Historical VaR: 1-day, 99% confidence, 1-year lookback (customizable)
Weights computed using **signed monetary exposure**
- Handles futures contracts differentiated by Metal, Maturity, and Exchange
- Covariance matrix built using filtered historical prices
- FOr the Historical method it uses realized past P&L

## Reporting

The pipeline produces:
- VaR at total portfolio level
- VaR by business line (3 groups)
- VaR by Metal (2 groups)

## Results

- The results are easy to reproduce as there is no monte carlo simulation. From parametric VaR you get the same results without specifying a random fixed seed.
- The pipeline produces a simple Excel file that can be then opened manually.
