"""
scripts/inventory_forecast.py
══════════════════════════════════════════════════════════════════
Feature 2 — Inventory Forecasting Model
──────────────────────────────────────────────────────────────────
What it does
  • Loads 90 days of historical sales from SQL Server
  • Fills missing dates with zero demand per product
  • Forecasts the next 7 days of demand using TWO models:
      – Simple Moving Average (SMA, 7-day window)
      – Linear Regression    (NumPy polyfit, clips negatives to 0)
  • Exports a 2-sheet Excel workbook (detail + pivot)

Design note
  All forecast math lives in pure functions (no DB, no SQLAlchemy).
  DB access is deferred to run_forecast() so the module is fully
  importable and testable without a database.

Schedule:
  • python scheduler.py
  • cron: 30 23 * * *  python -m scripts.inventory_forecast
"""

import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from config.logger import setup_logger

load_dotenv()
logger = setup_logger("inventory_forecast")


# ══════════════════════════════════════════════════════════════════
# 1. DATA EXTRACTION  (DB-dependent)
# ══════════════════════════════════════════════════════════════════

def fetch_historical_sales(session, days: int = 90) -> pd.DataFrame:
    """
    Pull per-product daily sales for the past *days* days.

    Returns:
        DataFrame: order_date, product_id, product_name, sku, quantity
    """
    from config.db import Order, OrderItem, Product

    cutoff = datetime.now() - timedelta(days=days)

    rows = (
        session.query(
            Order.order_date,
            Product.id.label("product_id"),
            Product.name.label("product_name"),
            Product.sku,
            OrderItem.quantity,
        )
        .join(OrderItem, Order.id == OrderItem.order_id)
        .join(Product,   OrderItem.product_id == Product.id)
        .filter(Order.order_date >= cutoff)
        .all()
    )

    if not rows:
        logger.warning("No sales data in last %d days.", days)
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "order_date", "product_id", "product_name", "sku", "quantity",
    ])
    df["sale_date"] = pd.to_datetime(df["order_date"]).dt.date
    return df


# ══════════════════════════════════════════════════════════════════
# 2. FEATURE ENGINEERING  (pure pandas)
# ══════════════════════════════════════════════════════════════════

def build_daily_demand(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate raw sales to daily units-sold per product.
    Gaps (days with no sales) are filled with 0.

    Args:
        df: Output of fetch_historical_sales.

    Returns:
        DataFrame: sale_date, product_id, product_name, sku, units_sold
    """
    if df.empty:
        return pd.DataFrame()

    daily = (
        df.groupby(["sale_date", "product_id", "product_name", "sku"])["quantity"]
        .sum()
        .reset_index()
        .rename(columns={"quantity": "units_sold"})
    )

    products   = daily[["product_id", "product_name", "sku"]].drop_duplicates()
    date_range = pd.DataFrame({
        "sale_date": pd.date_range(
            start=str(daily["sale_date"].min()),
            end=str(daily["sale_date"].max()),
            freq="D",
        ).date
    })

    frames = []
    for _, prod in products.iterrows():
        prod_daily = daily[daily["product_id"] == prod["product_id"]]
        merged = date_range.merge(
            prod_daily[["sale_date", "units_sold"]], on="sale_date", how="left"
        )
        merged["units_sold"]   = merged["units_sold"].fillna(0).astype(int)
        merged["product_id"]   = prod["product_id"]
        merged["product_name"] = prod["product_name"]
        merged["sku"]          = prod["sku"]
        frames.append(merged)

    return pd.concat(frames, ignore_index=True)


# ══════════════════════════════════════════════════════════════════
# 3. FORECASTING MODELS  (pure numpy/pandas)
# ══════════════════════════════════════════════════════════════════

def moving_average_forecast(series: pd.Series, window: int = 7) -> float:
    """
    Simple Moving Average of the last *window* observations.

    Falls back to the full-series mean when len(series) < window.
    Returns 0.0 for an empty series.
    """
    if series.empty:
        return 0.0
    tail = series.iloc[-window:] if len(series) >= window else series
    return float(tail.mean())


def linear_regression_forecast(
    series: pd.Series, horizon: int = 7
) -> list[float]:
    """
    Fit a linear trend to *series*, project *horizon* steps forward.

    Uses numpy.polyfit (degree 1).  Predictions are clipped to >= 0
    because negative demand is not meaningful.

    Returns:
        List of *horizon* predicted daily-demand values.
    """
    if len(series) < 2:
        # Not enough data for a trend → fall back to constant SMA
        val = moving_average_forecast(series)
        return [round(val, 2)] * horizon

    x      = np.arange(len(series), dtype=float)
    coeffs = np.polyfit(x, series.values.astype(float), deg=1)
    poly   = np.poly1d(coeffs)

    future = np.arange(len(series), len(series) + horizon, dtype=float)
    return [max(0.0, round(float(v), 2)) for v in poly(future)]


def forecast_all_products(
    daily_df: pd.DataFrame, horizon: int = 7
) -> pd.DataFrame:
    """
    Generate SMA + LR forecasts for every product.

    Args:
        daily_df: Output of build_daily_demand.
        horizon:  Number of future days to predict.

    Returns:
        DataFrame with columns:
        product_id, product_name, sku, forecast_date,
        sma_forecast_units, lr_forecast_units, recommended_reorder
    """
    if daily_df.empty:
        return pd.DataFrame()

    start  = date.today() + timedelta(days=1)
    rows   = []

    for pid, group in daily_df.groupby("product_id"):
        group  = group.sort_values("sale_date")
        series = group["units_sold"].reset_index(drop=True)
        name   = group["product_name"].iloc[0]
        sku    = group["sku"].iloc[0]

        sma_val  = moving_average_forecast(series)
        lr_vals  = linear_regression_forecast(series, horizon=horizon)

        for i in range(horizon):
            rows.append({
                "product_id":          pid,
                "product_name":        name,
                "sku":                 sku,
                "forecast_date":       start + timedelta(days=i),
                "sma_forecast_units":  round(sma_val, 2),
                "lr_forecast_units":   lr_vals[i],
                # Conservative: take the higher of the two models
                "recommended_reorder": max(sma_val, lr_vals[i]),
            })

    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════
# 4. EXCEL EXPORT  (pure openpyxl)
# ══════════════════════════════════════════════════════════════════

def export_forecast(forecast_df: pd.DataFrame) -> str:
    """
    Write forecast to Excel (two sheets: detail + pivot).

    Returns:
        Absolute path of the written file.
    """
    output_dir = os.getenv("OUTPUT_DIR", "./output")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(
        output_dir,
        f"inventory_forecast_{date.today().strftime('%Y%m%d')}.xlsx",
    )

    pivot = forecast_df.pivot_table(
        index=["product_id", "product_name", "sku"],
        columns="forecast_date",
        values="recommended_reorder",
    ).reset_index()

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        forecast_df.to_excel(writer, sheet_name="7-Day Forecast", index=False)
        pivot.to_excel(writer,       sheet_name="Forecast Pivot",  index=False)

    logger.info("Forecast exported → %s", path)
    return path


# ══════════════════════════════════════════════════════════════════
# 5. PIPELINE ENTRYPOINT
# ══════════════════════════════════════════════════════════════════

def run_forecast(lookback_days: int = 90, horizon: int = 7) -> str | None:
    """
    Full pipeline: DB extract → aggregate → forecast → export.

    Returns:
        Path to the generated Excel file, or None on failure.
    """
    from config.db import create_db_engine, get_session_factory

    logger.info(
        "Starting inventory forecast (lookback=%dd, horizon=%dd) …",
        lookback_days, horizon,
    )
    try:
        engine  = create_db_engine()
        Session = get_session_factory(engine)
        with Session() as session:
            raw_df = fetch_historical_sales(session, days=lookback_days)

        if raw_df.empty:
            logger.warning("No data — forecast aborted.")
            return None

        daily_df    = build_daily_demand(raw_df)
        forecast_df = forecast_all_products(daily_df, horizon=horizon)
        path        = export_forecast(forecast_df)

        logger.info(
            "Forecast complete — %d products × %d days.",
            forecast_df["product_id"].nunique(), horizon,
        )
        return path

    except RuntimeError as exc:
        logger.error("Forecast failed: %s", exc)
        return None


if __name__ == "__main__":
    run_forecast()
