"""
scripts/low_stock_alert.py
══════════════════════════════════════════════════════════════════
Feature 3 — Low Stock Alert Automation
──────────────────────────────────────────────────────────────────
What it does
  • Polls the .NET API  GET /api/products  (JWT-authenticated)
  • Any product with stock < LOW_STOCK_THRESHOLD triggers an alert
  • Alerts are:
      1. Written to a rotating log file
      2. Posted to Slack via Incoming Webhook  (or simulated if
         SLACK_WEBHOOK_URL is not configured)

Design note
  No SQLAlchemy.  All external calls use the `requests` library only.
  Fully testable without a running API (mock the requests.get call).

Schedule:
  • python scheduler.py            (runs every 30 minutes)
  • cron: */30 * * * *  python -m scripts.low_stock_alert
"""

import json
import os
from datetime import datetime

import requests
from dotenv import load_dotenv

from config.logger import setup_logger

load_dotenv()
logger = setup_logger("low_stock_alert", log_file="low_stock_alerts.log")

# ── Config (all from .env) ────────────────────────────────────────────────────
LOW_STOCK_THRESHOLD = int(os.getenv("LOW_STOCK_THRESHOLD", "10"))
API_BASE_URL        = os.getenv("API_BASE_URL",   "http://localhost:5000/api")
API_JWT_TOKEN       = os.getenv("API_JWT_TOKEN",  "")
SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL", "")
REQUEST_TIMEOUT     = 10   # seconds


# ══════════════════════════════════════════════════════════════════
# 1. API CLIENT
# ══════════════════════════════════════════════════════════════════

def fetch_inventory_from_api() -> list[dict]:
    """
    Call GET /api/products with JWT Bearer auth.

    The function handles the .NET unified envelope:
        { "success": true, "data": [...], "message": "" }
    as well as a bare list response.

    Returns:
        List of product dicts.

    Raises:
        RuntimeError: On connection error, timeout, or HTTP error.
    """
    url     = f"{API_BASE_URL}/products"
    headers = {
        "Authorization": f"Bearer {API_JWT_TOKEN}",
        "Content-Type":  "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()

        if isinstance(body, dict) and "data" in body:
            return body["data"]
        if isinstance(body, list):
            return body

        logger.warning("Unexpected API response shape: %s", type(body).__name__)
        return []

    except requests.exceptions.ConnectionError:
        raise RuntimeError(f"Cannot reach API at {url}. Is the .NET backend running?")
    except requests.exceptions.Timeout:
        raise RuntimeError(f"API timed out after {REQUEST_TIMEOUT}s.")
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"API error {exc.response.status_code}: {exc.response.text[:200]}"
        )


# ══════════════════════════════════════════════════════════════════
# 2. ALERT DETECTION  (pure logic — no I/O)
# ══════════════════════════════════════════════════════════════════

def detect_low_stock(
    products: list[dict], threshold: int = LOW_STOCK_THRESHOLD
) -> list[dict]:
    """
    Return only products whose stock is strictly below *threshold*.

    Accepts both camelCase (stockQuantity) and snake_case (stock_quantity)
    field names to handle different API response formats.

    Args:
        products:  List of product dicts from the API.
        threshold: Alert if stock < threshold (NOT <=).

    Returns:
        List of alert dicts with: id, name, sku, stock_quantity,
        threshold, detected_at.
    """
    alerts = []
    for product in products:
        qty = product.get("stockQuantity", product.get("stock_quantity"))
        if qty is None:
            logger.warning(
                "Product id=%s is missing a stock quantity field — skipped.",
                product.get("id", "?"),
            )
            continue
        if qty < threshold:
            alerts.append({
                "id":             product.get("id"),
                "name":           product.get("name", "Unknown"),
                "sku":            product.get("sku",  "N/A"),
                "stock_quantity": qty,
                "threshold":      threshold,
                "detected_at":    datetime.now().isoformat(),
            })
    return alerts


# ══════════════════════════════════════════════════════════════════
# 3. NOTIFICATIONS
# ══════════════════════════════════════════════════════════════════

def log_alerts(alerts: list[dict]) -> None:
    """Write each alert as a WARNING line in the log."""
    for a in alerts:
        logger.warning(
            "LOW STOCK — '%s' (SKU: %s) | Stock: %d | Threshold: %d",
            a["name"], a["sku"], a["stock_quantity"], a["threshold"],
        )


def send_slack_alert(alerts: list[dict]) -> None:
    """
    Post a Slack Block Kit message via Incoming Webhook.

    If SLACK_WEBHOOK_URL is absent or still the placeholder value,
    the function logs a simulation instead of making a real HTTP call.
    This makes the feature testable without a live Slack workspace.
    """
    if not alerts:
        return

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "⚠️  Low Stock Alert", "emoji": True},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{len(alerts)} product(s)* are below the reorder threshold "
                    f"as of {datetime.now().strftime('%Y-%m-%d %H:%M')}."
                ),
            },
        },
        {"type": "divider"},
    ]

    for a in alerts:
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Product:*\n{a['name']}"},
                {"type": "mrkdwn", "text": f"*SKU:*\n{a['sku']}"},
                {"type": "mrkdwn", "text": f"*Current Stock:*\n{a['stock_quantity']} units"},
                {"type": "mrkdwn", "text": f"*Threshold:*\n{a['threshold']} units"},
            ],
        })

    payload = {"blocks": blocks}

    is_placeholder = (
        not SLACK_WEBHOOK_URL
        or SLACK_WEBHOOK_URL.startswith("https://hooks.slack.com/services/YOUR")
    )

    if is_placeholder:
        logger.info(
            "[SIMULATED SLACK] Payload:\n%s", json.dumps(payload, indent=2)
        )
        return

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL, json=payload, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        logger.info("Slack alert posted — %d product(s).", len(alerts))
    except requests.exceptions.RequestException as exc:
        logger.error("Slack post failed: %s", exc)


# ══════════════════════════════════════════════════════════════════
# 4. PIPELINE ENTRYPOINT
# ══════════════════════════════════════════════════════════════════

def run_alerts() -> list[dict]:
    """
    Full pipeline: fetch inventory → detect → log → notify Slack.

    Returns:
        List of alert dicts (empty if all stock is healthy).
    """
    logger.info("Low stock check — threshold: %d …", LOW_STOCK_THRESHOLD)
    try:
        products = fetch_inventory_from_api()
        logger.info("Fetched %d products from API.", len(products))

        alerts = detect_low_stock(products)

        if not alerts:
            logger.info("All products sufficiently stocked. ✓")
            return []

        log_alerts(alerts)
        send_slack_alert(alerts)
        logger.info("Check complete — %d alert(s) raised.", len(alerts))
        return alerts

    except RuntimeError as exc:
        logger.error("Low stock check failed: %s", exc)
        return []


if __name__ == "__main__":
    run_alerts()
