"""
scheduler.py
══════════════════════════════════════════════════════════════════
Master scheduler — runs all four scripts on a time-based cadence
using the `schedule` library.

Usage:
    python scheduler.py          # runs forever
    Ctrl-C to stop

Individual scripts can also be triggered manually:
    python -m scripts.daily_sales_report
    python -m scripts.inventory_forecast
    python -m scripts.low_stock_alert
    python -m scripts.anomaly_detection
"""

import time

import schedule

from config.logger import setup_logger
from scripts.anomaly_detection import run_anomaly_detection
from scripts.daily_sales_report import run_report
from scripts.inventory_forecast import run_forecast
from scripts.low_stock_alert import run_alerts

logger = setup_logger("scheduler")


def _run(name, fn):
    logger.info("Scheduler: starting %s …", name)
    fn()


schedule.every().day.at("23:00").do(_run, "Daily Sales Report",  run_report)
schedule.every().day.at("23:30").do(_run, "Inventory Forecast",  run_forecast)
schedule.every(30).minutes.do(      _run, "Low Stock Alert",     run_alerts)
schedule.every().day.at("02:00").do(_run, "Anomaly Detection",   run_anomaly_detection)

if __name__ == "__main__":
    logger.info("Scheduler started.")
    logger.info("  Daily Sales Report  → 23:00 daily")
    logger.info("  Inventory Forecast  → 23:30 daily")
    logger.info("  Low Stock Alert     → every 30 minutes")
    logger.info("  Anomaly Detection   → 02:00 daily")
    logger.info("Press Ctrl-C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(30)
