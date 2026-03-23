"""
scripts/anomaly_detection.py
══════════════════════════════════════════════════════════════════
Feature 4 — Data Anomaly Detection
──────────────────────────────────────────────────────────────────
What it does
  Scans the database for three anomaly categories:

  1. HIGH QUANTITY ORDERS
     Per product, compute the Z-score of each order's quantity.
     Flag any order line where Z-score > ANOMALY_Z_THRESHOLD (default 3).

  2. NEGATIVE STOCK
     Any product whose current stock_quantity < 0.
     (Should be blocked by the API but acts as a secondary safety check.)

  3. SALES SPIKES
     Per product, compute daily revenue Z-score across ANOMALY_LOOKBACK_DAYS.
     Flag any day where revenue deviates > threshold SDs from the norm.

  All anomalies are:
    • Logged to anomaly_detection.log (WARNING level)
    • Exported to anomaly_report_YYYYMMDD.xlsx  +  .csv

Design note
  DB access deferred to run_anomaly_detection().
  All three detect_* functions are pure pandas and fully testable
  without a database connection.

Schedule:
  • python scheduler.py
  • cron: 0 2 * * *  python -m scripts.anomaly_detection
"""

import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from config.logger import setup_logger

load_dotenv()
logger = setup_logger("anomaly_detection", log_file="anomaly_detection.log")

Z_THRESHOLD   = float(os.getenv("ANOMALY_Z_THRESHOLD",   "3.0"))
LOOKBACK_DAYS = int(  os.getenv("ANOMALY_LOOKBACK_DAYS", "90"))


# ══════════════════════════════════════════════════════════════════
# 1. DATA FETCHERS  (DB-dependent)
# ══════════════════════════════════════════════════════════════════

def fetch_order_items(session, days: int = LOOKBACK_DAYS) -> pd.DataFrame:
    """
    Fetch all order lines from the past *days* days.

    Returns:
        DataFrame: order_id, order_date, product_id, product_name,
                   sku, quantity, unit_price, line_total, sale_date
    """
    from config.db import Order, OrderItem, Product

    cutoff = datetime.now() - timedelta(days=days)

    rows = (
        session.query(
            Order.id.label("order_id"),
            Order.order_date,
            Product.id.label("product_id"),
            Product.name.label("product_name"),
            Product.sku,
            OrderItem.quantity,
            OrderItem.unit_price,
        )
        .join(OrderItem, Order.id == OrderItem.order_id)
        .join(Product,   OrderItem.product_id == Product.id)
        .filter(Order.order_date >= cutoff)
        .all()
    )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "order_id", "order_date", "product_id", "product_name",
        "sku", "quantity", "unit_price",
    ])
    df["line_total"] = df["quantity"] * df["unit_price"]
    df["sale_date"]  = pd.to_datetime(df["order_date"]).dt.date
    return df


def fetch_all_products(session) -> pd.DataFrame:
    """
    Fetch current stock levels for all products.

    Returns:
        DataFrame: product_id, product_name, sku, stock_quantity
    """
    from config.db import Product

    rows = session.query(
        Product.id, Product.name, Product.sku, Product.stock_quantity
    ).all()
    return pd.DataFrame(
        rows, columns=["product_id", "product_name", "sku", "stock_quantity"]
    )


# ══════════════════════════════════════════════════════════════════
# 2. ANOMALY DETECTORS  (pure pandas/numpy)
# ══════════════════════════════════════════════════════════════════

def detect_high_quantity_orders(
    df: pd.DataFrame, z_threshold: float = Z_THRESHOLD
) -> pd.DataFrame:
    """
    Flag order lines whose quantity Z-score exceeds *z_threshold*
    within that product's distribution.

    Args:
        df:          Output of fetch_order_items.
        z_threshold: Standard deviations above mean to flag.

    Returns:
        DataFrame of flagged rows with z_score, anomaly_type, detail columns.
        Empty DataFrame if no anomalies found.
    """
    if df.empty:
        return pd.DataFrame()

    flagged = []
    for _, group in df.groupby("product_id"):
        std = group["quantity"].std()
        if std == 0 or np.isnan(std):
            continue                   # uniform quantities → no outliers possible
        mean   = group["quantity"].mean()
        g      = group.copy()
        g["z_score"] = (g["quantity"] - mean) / std
        hits   = g[g["z_score"] > z_threshold].copy()
        hits["anomaly_type"] = "High Quantity Order"
        hits["detail"] = hits.apply(
            lambda r: f"qty={r['quantity']} (μ={mean:.1f}, σ={std:.1f}, z={r['z_score']:.2f})",
            axis=1,
        )
        flagged.append(hits)

    if not flagged:
        return pd.DataFrame()

    cols = ["order_id", "order_date", "product_id", "product_name",
            "sku", "quantity", "z_score", "anomaly_type", "detail"]
    return pd.concat(flagged, ignore_index=True)[cols]


def detect_negative_stock(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag any product whose current stock_quantity is negative.

    Args:
        products_df: Output of fetch_all_products.

    Returns:
        DataFrame of flagged products with anomaly_type + detail columns.
    """
    if products_df.empty:
        return pd.DataFrame()

    hits = products_df[products_df["stock_quantity"] < 0].copy()
    if hits.empty:
        return pd.DataFrame()

    hits["anomaly_type"] = "Negative Stock"
    hits["detail"]       = hits["stock_quantity"].apply(
        lambda q: f"stock={q} (must be ≥ 0)"
    )
    hits["detected_at"]  = datetime.now().isoformat()
    return hits[["product_id", "product_name", "sku",
                 "stock_quantity", "anomaly_type", "detail", "detected_at"]]


def detect_sales_spikes(
    df: pd.DataFrame, z_threshold: float = Z_THRESHOLD
) -> pd.DataFrame:
    """
    Flag days where a product's daily revenue deviates more than
    *z_threshold* standard deviations from its historical norm.

    Args:
        df:          Output of fetch_order_items.
        z_threshold: Standard deviations above mean to flag.

    Returns:
        DataFrame of flagged (product, date) pairs with z_score + detail.
    """
    if df.empty:
        return pd.DataFrame()

    daily = (
        df.groupby(["sale_date", "product_id", "product_name", "sku"])["line_total"]
        .sum()
        .reset_index()
        .rename(columns={"line_total": "daily_revenue"})
    )

    flagged = []
    for _, group in daily.groupby("product_id"):
        std = group["daily_revenue"].std()
        if std == 0 or np.isnan(std):
            continue
        mean   = group["daily_revenue"].mean()
        g      = group.copy()
        g["z_score"] = (g["daily_revenue"] - mean) / std
        hits   = g[g["z_score"] > z_threshold].copy()
        hits["anomaly_type"] = "Sales Spike"
        hits["detail"] = hits.apply(
            lambda r: (
                f"revenue=${r['daily_revenue']:.2f} "
                f"(μ=${mean:.2f}, σ=${std:.2f}, z={r['z_score']:.2f})"
            ),
            axis=1,
        )
        flagged.append(hits)

    if not flagged:
        return pd.DataFrame()

    cols = ["sale_date", "product_id", "product_name", "sku",
            "daily_revenue", "z_score", "anomaly_type", "detail"]
    return pd.concat(flagged, ignore_index=True)[cols]


# ══════════════════════════════════════════════════════════════════
# 3. LOG SUMMARY
# ══════════════════════════════════════════════════════════════════

def log_summary(anomalies: dict[str, pd.DataFrame]) -> None:
    """Print a concise per-category summary to the log."""
    total = sum(len(df) for df in anomalies.values())
    logger.info("─── Anomaly Detection Summary ───")
    for category, df in anomalies.items():
        logger.info("  %-28s %d anomaly/ies", category + ":", len(df))
        if not df.empty and "detail" in df.columns:
            for _, row in df.iterrows():
                name = row.get("product_name", "?")
                logger.warning("    ↳ %-20s  %s", name, row["detail"])
    logger.info("  %-28s %d", "TOTAL:", total)
    logger.info("─────────────────────────────────")


# ══════════════════════════════════════════════════════════════════
# 4. EXPORT  (pure openpyxl)
# ══════════════════════════════════════════════════════════════════

def export_anomalies(anomalies: dict[str, pd.DataFrame]) -> str:
    """
    Write all anomaly DataFrames to:
      • anomaly_report_YYYYMMDD.xlsx  (one sheet per category)
      • anomaly_report_YYYYMMDD.csv   (combined flat file)

    Returns:
        Path to the Excel file.
    """
    output_dir = os.getenv("OUTPUT_DIR", "./output")
    os.makedirs(output_dir, exist_ok=True)
    today     = date.today().strftime("%Y%m%d")
    xlsx_path = os.path.join(output_dir, f"anomaly_report_{today}.xlsx")
    csv_path  = os.path.join(output_dir, f"anomaly_report_{today}.csv")

    all_frames = []
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for sheet, df in anomalies.items():
            out = df if not df.empty else pd.DataFrame({"message": ["No anomalies detected."]})
            out.to_excel(writer, sheet_name=sheet[:31], index=False)
            if not df.empty:
                all_frames.append(df.assign(anomaly_category=sheet))

    if all_frames:
        pd.concat(all_frames, ignore_index=True).to_csv(csv_path, index=False)
        logger.info("Anomaly CSV  → %s", csv_path)

    logger.info("Anomaly XLSX → %s", xlsx_path)
    return xlsx_path


# ══════════════════════════════════════════════════════════════════
# 5. PIPELINE ENTRYPOINT
# ══════════════════════════════════════════════════════════════════

def run_anomaly_detection() -> str | None:
    """
    Full pipeline: DB fetch → detect → log summary → export.

    Returns:
        Path to the generated Excel file, or None on failure.
    """
    from config.db import create_db_engine, get_session_factory

    logger.info(
        "Anomaly detection — lookback=%dd, z=%.1f …",
        LOOKBACK_DAYS, Z_THRESHOLD,
    )
    try:
        engine  = create_db_engine()
        Session = get_session_factory(engine)
        with Session() as session:
            orders_df   = fetch_order_items(session, days=LOOKBACK_DAYS)
            products_df = fetch_all_products(session)

        anomalies = {
            "High Quantity Orders": detect_high_quantity_orders(orders_df),
            "Negative Stock":       detect_negative_stock(products_df),
            "Sales Spikes":         detect_sales_spikes(orders_df),
        }

        log_summary(anomalies)
        return export_anomalies(anomalies)

    except RuntimeError as exc:
        logger.error("Anomaly detection failed: %s", exc)
        return None


if __name__ == "__main__":
    run_anomaly_detection()
