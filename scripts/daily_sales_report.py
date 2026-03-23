"""
scripts/daily_sales_report.py
══════════════════════════════════════════════════════════════════
Feature 1 — Automated Daily Sales Report
──────────────────────────────────────────────────────────────────
What it does
  • Connects to SQL Server and pulls every order line for a given date
  • Calculates: total revenue, order count, top-10 products by revenue
  • Exports a 3-sheet Excel workbook (Summary / Top Products / Raw Orders)
  • Simulates emailing the report to the store manager

Design note
  DB imports (SQLAlchemy) are done INSIDE the functions that need them,
  not at module level.  This means calculate_metrics / export_to_excel /
  simulate_email are importable and testable without SQLAlchemy installed.

Schedule (choose one):
  • python scheduler.py            (uses the `schedule` library)
  • cron: 0 23 * * *  python -m scripts.daily_sales_report
"""

import os
import smtplib
from datetime import date, datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from dotenv import load_dotenv

from config.logger import setup_logger

load_dotenv()
logger = setup_logger("daily_sales_report")


# ══════════════════════════════════════════════════════════════════
# 1. DATA EXTRACTION  (DB-dependent — SQLAlchemy imported here only)
# ══════════════════════════════════════════════════════════════════

def fetch_orders_for_date(session, target_date: date) -> pd.DataFrame:
    """
    Query all order-item rows joined with Orders + Products for *target_date*.

    Args:
        session:     Active SQLAlchemy session.
        target_date: The calendar date to report on.

    Returns:
        DataFrame with columns:
        order_id, order_date, customer_name, product_id, product_name,
        sku, quantity, unit_price, line_total
    """
    # Lazy import keeps the module usable without SQLAlchemy
    from config.db import Order, OrderItem, Product

    start = datetime.combine(target_date, datetime.min.time())
    end   = start + timedelta(days=1)

    rows = (
        session.query(
            Order.id.label("order_id"),
            Order.order_date,
            Order.customer_name,
            Product.id.label("product_id"),
            Product.name.label("product_name"),
            Product.sku,
            OrderItem.quantity,
            OrderItem.unit_price,
        )
        .join(OrderItem, Order.id == OrderItem.order_id)
        .join(Product,   OrderItem.product_id == Product.id)
        .filter(Order.order_date >= start, Order.order_date < end)
        .all()
    )

    if not rows:
        logger.warning("No orders found for %s.", target_date)
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "order_id", "order_date", "customer_name",
        "product_id", "product_name", "sku",
        "quantity", "unit_price",
    ])
    df["line_total"] = df["quantity"] * df["unit_price"]
    return df


# ══════════════════════════════════════════════════════════════════
# 2. METRIC CALCULATION  (pure pandas — no DB needed)
# ══════════════════════════════════════════════════════════════════

def calculate_metrics(df: pd.DataFrame) -> dict:
    """
    Derive KPIs from a flat order DataFrame.

    Args:
        df: Output of fetch_orders_for_date (must have line_total column).

    Returns:
        {
          "total_revenue": float,
          "total_orders":  int,
          "top_products":  DataFrame  ← top 10 products sorted by revenue
        }
    """
    if df.empty:
        return {
            "total_revenue": 0.0,
            "total_orders":  0,
            "top_products":  pd.DataFrame(),
        }

    total_revenue = round(float(df["line_total"].sum()), 2)
    total_orders  = int(df["order_id"].nunique())

    top_products = (
        df.groupby(["product_id", "product_name", "sku"])
        .agg(units_sold=("quantity", "sum"), revenue=("line_total", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
        .head(10)
        .round({"revenue": 2})
    )

    return {
        "total_revenue": total_revenue,
        "total_orders":  total_orders,
        "top_products":  top_products,
    }


# ══════════════════════════════════════════════════════════════════
# 3. EXCEL EXPORT  (pure openpyxl — no DB needed)
# ══════════════════════════════════════════════════════════════════

def export_to_excel(df_raw: pd.DataFrame, metrics: dict, target_date: date) -> str:
    """
    Write a multi-sheet Excel workbook to OUTPUT_DIR.

    Sheets
    ------
    Summary      →  headline KPIs (one row)
    Top Products →  top 10 products ranked by revenue
    Raw Orders   →  full line-level order detail

    Returns:
        Absolute path of the written file.
    """
    output_dir = os.getenv("OUTPUT_DIR", "./output")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(
        output_dir,
        f"daily_sales_report_{target_date.strftime('%Y%m%d')}.xlsx",
    )

    avg_order = (
        round(metrics["total_revenue"] / metrics["total_orders"], 2)
        if metrics["total_orders"] else 0.0
    )

    summary_df = pd.DataFrame([{
        "Report Date":         target_date.strftime("%Y-%m-%d"),
        "Total Revenue ($)":   metrics["total_revenue"],
        "Total Orders":        metrics["total_orders"],
        "Avg Order Value ($)": avg_order,
        "Generated At":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary",      index=False)
        metrics["top_products"].to_excel(writer, sheet_name="Top Products", index=False)
        if not df_raw.empty:
            df_raw.to_excel(writer, sheet_name="Raw Orders", index=False)

    logger.info("Report exported → %s", path)
    return path


# ══════════════════════════════════════════════════════════════════
# 4. SIMULATED EMAIL  (stdlib only — no DB needed)
# ══════════════════════════════════════════════════════════════════

def simulate_email(filepath: str, metrics: dict, target_date: date) -> None:
    """
    Attempt to send the report via SMTP.
    Falls back to simulation-mode logging when SMTP_HOST is not configured.

    Required env vars for real sending:
        SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
        REPORT_SENDER_EMAIL, REPORT_RECIPIENT_EMAIL
    """
    sender    = os.getenv("REPORT_SENDER_EMAIL",    "analytics@store.com")
    recipient = os.getenv("REPORT_RECIPIENT_EMAIL", "manager@store.com")
    subject   = f"Daily Sales Report — {target_date.strftime('%B %d, %Y')}"

    body = (
        f"Hi,\n\n"
        f"Here is today's sales summary:\n\n"
        f"  Date:           {target_date}\n"
        f"  Total Revenue:  ${metrics['total_revenue']:,.2f}\n"
        f"  Total Orders:   {metrics['total_orders']}\n\n"
        f"The full report is attached.\n\n"
        f"Regards,\nRetail Analytics Bot"
    )

    msg             = MIMEMultipart()
    msg["From"]     = sender
    msg["To"]       = recipient
    msg["Subject"]  = subject
    msg.attach(MIMEText(body, "plain"))

    with open(filepath, "rb") as fh:
        part = MIMEApplication(fh.read(), Name=os.path.basename(filepath))
        part["Content-Disposition"] = (
            f'attachment; filename="{os.path.basename(filepath)}"'
        )
        msg.attach(part)

    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_user = os.getenv("SMTP_USER", "")

    if smtp_host and smtp_user:
        try:
            with smtplib.SMTP(smtp_host, int(os.getenv("SMTP_PORT", "587"))) as srv:
                srv.starttls()
                srv.login(smtp_user, os.getenv("SMTP_PASSWORD", ""))
                srv.sendmail(sender, [recipient], msg.as_string())
            logger.info("Report emailed → %s", recipient)
        except smtplib.SMTPException as exc:
            logger.error("Email send failed: %s", exc)
    else:
        logger.info(
            "[SIMULATED EMAIL] To: %s | Subject: %s | File: %s",
            recipient, subject, os.path.basename(filepath),
        )


# ══════════════════════════════════════════════════════════════════
# 5. PIPELINE ENTRYPOINT
# ══════════════════════════════════════════════════════════════════

def run_report(target_date: date | None = None) -> str | None:
    """
    Full pipeline: DB extract → calculate → export → email.

    Args:
        target_date: Date to report on. Defaults to yesterday.

    Returns:
        Path to the generated Excel file, or None on failure.
    """
    from config.db import create_db_engine, get_session_factory

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    logger.info("Starting daily sales report for %s …", target_date)
    try:
        engine  = create_db_engine()
        Session = get_session_factory(engine)
        with Session() as session:
            df = fetch_orders_for_date(session, target_date)

        metrics  = calculate_metrics(df)
        filepath = export_to_excel(df, metrics, target_date)
        simulate_email(filepath, metrics, target_date)

        logger.info(
            "Report complete — Revenue: $%.2f | Orders: %d",
            metrics["total_revenue"], metrics["total_orders"],
        )
        return filepath

    except RuntimeError as exc:
        logger.error("Report generation failed: %s", exc)
        return None


if __name__ == "__main__":
    run_report()
