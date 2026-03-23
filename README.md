# Retail Data Analytics & Automation
### Python Extension for the .NET Retail Inventory & Order Management API

---

## What This Project Does

This project adds a Python analytics layer on top of your existing .NET Core backend. Four automated scripts connect to the same SQL Server database and REST API to deliver:

| # | Script | What it produces |
|---|--------|-----------------|
| 1 | `daily_sales_report.py` | Daily Excel report — revenue, order count, top 10 products |
| 2 | `inventory_forecast.py` | 7-day demand forecast per product (SMA + Linear Regression) |
| 3 | `low_stock_alert.py` | Real-time Slack alerts when stock drops below threshold |
| 4 | `anomaly_detection.py` | Flags unusual orders, negative stock, and revenue spikes |

All scripts are scheduled via `scheduler.py` and write logs + Excel/CSV output to `./output/`.

---

## Project Structure

```
retail_analytics/
│
├── config/
│   ├── db.py               # SQLAlchemy engine, ORM models (Products/Orders/OrderItems)
│   └── logger.py           # Rotating file + console logger used by all scripts
│
├── scripts/
│   ├── daily_sales_report.py   # Feature 1
│   ├── inventory_forecast.py   # Feature 2
│   ├── low_stock_alert.py      # Feature 3
│   └── anomaly_detection.py    # Feature 4
│
├── tests/
│   └── run_tests.py        # 72 unit tests — no database required to run
│
├── sample_output/          # Pre-generated sample Excel/CSV files (see below)
│   ├── daily_sales_report_20240314.xlsx
│   ├── inventory_forecast_20260323.xlsx
│   ├── anomaly_report_20260323.xlsx
│   └── anomaly_report_20260323.csv
│
├── output/                 # Live output lands here (git-ignored)
├── scheduler.py            # Master scheduler — runs all 4 scripts on a timer
├── requirements.txt
├── .env.example            # Copy to .env and fill in your credentials
└── README.md
```

---

## Prerequisites

Before you start, make sure you have:

- **Python 3.9+**
	- https://www.python.org/ftp/python/3.13.0/python-3.13.0-amd64.exe
- **Microsoft ODBC Driver 17 for SQL Server**
  - Windows: [Download here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
  - Ubuntu/Debian: `sudo apt-get install msodbcsql17`
  - macOS: `brew install msodbcsql17`
- Your **.NET Retail API running** and accessible
- (Optional) A **Slack Incoming Webhook URL** — if not set, alerts are logged in simulation mode

---

## Setup — Step by Step

### 1. Clone and enter the project

```bash
git clone <your-repo-url>
cd retail_analytics
```

### 2. Create a virtual environment

```bash
# Create
python -m venv venv

# Activate — Windows
venv\Scripts\activate
or  : source venv/Scripts/activate if using bash on windows

# Activate — macOS / Linux
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
# Copy the example file
cp .env.example .env

# Open .env and fill in your real values
notepad .env          # Windows
nano .env             # macOS / Linux
```

**Minimum required values:**

```env
DB_SERVER=localhost          # Your SQL Server host
DB_PORT=1433
DB_NAME=RetailDB             # Your database name
DB_USER=sa
DB_PASSWORD=your_password

API_BASE_URL=http://localhost:5000/api
API_JWT_TOKEN=your_jwt_token
```

**Optional (features degrade gracefully without these):**

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/...   # Real Slack webhook
SMTP_HOST=smtp.gmail.com                         # For real email sending
```

---

## Running Each Script

Every script can be run directly or via the master scheduler.

### Run manually (one-off)

```bash
# Feature 1 — Generate yesterday's sales report
python -m scripts.daily_sales_report

# Feature 2 — Generate 7-day inventory forecast
python -m scripts.inventory_forecast

# Feature 3 — Run a low stock check right now
python -m scripts.low_stock_alert

# Feature 4 — Scan for data anomalies
python -m scripts.anomaly_detection
```

### Run all scripts on a schedule

```bash
python scheduler.py
```

This starts a process that runs:
- Sales report → every day at **23:00**
- Inventory forecast → every day at **23:30**
- Low stock check → **every 30 minutes**
- Anomaly detection → every day at **02:00**

Press `Ctrl-C` to stop.

### Schedule via cron (Linux/macOS)

```bash
crontab -e
```

Add these lines (adjust paths):

```cron
# Daily sales report at 23:00
0 23 * * * /path/to/venv/bin/python -m scripts.daily_sales_report >> /path/to/output/cron.log 2>&1

# Inventory forecast at 23:30
30 23 * * * /path/to/venv/bin/python -m scripts.inventory_forecast >> /path/to/output/cron.log 2>&1

# Low stock check every 30 minutes
*/30 * * * * /path/to/venv/bin/python -m scripts.low_stock_alert >> /path/to/output/cron.log 2>&1

# Anomaly detection at 02:00
0 2 * * * /path/to/venv/bin/python -m scripts.anomaly_detection >> /path/to/output/cron.log 2>&1
```

---

## Output Files

All files are written to `./output/` (configurable via `OUTPUT_DIR` in `.env`).

| File | Generated by | Contents |
|------|-------------|---------|
| `daily_sales_report_YYYYMMDD.xlsx` | Feature 1 | 3 sheets: Summary KPIs, Top 10 Products, Raw Orders |
| `inventory_forecast_YYYYMMDD.xlsx` | Feature 2 | 2 sheets: 7-Day Detail, Pivot by product × date |
| `anomaly_report_YYYYMMDD.xlsx` | Feature 4 | 3 sheets: one per anomaly category |
| `anomaly_report_YYYYMMDD.csv` | Feature 4 | Combined flat file for further analysis |
| `retail_analytics.log` | All scripts | General activity log (rotating, 5 MB max) |
| `low_stock_alerts.log` | Feature 3 | Alert-specific log |
| `anomaly_detection.log` | Feature 4 | Anomaly-specific log |

Sample output files are included in `sample_output/` so you can inspect the format before connecting a real database.

---

## Running the Tests

### On your local machine (recommended)

```bash
# Standard pytest
pytest tests/run_tests.py -v

# With coverage report
pytest tests/run_tests.py -v --cov=scripts --cov-report=term-missing
```

### Without pytest installed

```bash
python tests/run_tests.py
```

### What the tests cover

The test suite has **72 tests** and runs without a database or live API.

| Class | Tests | What is verified |
|-------|-------|-----------------|
| `TestCalculateMetrics` | 8 | Revenue sums, order counts, top-product ranking, edge cases |
| `TestExportToExcel` | 4 | File creation, date in filename, correct sheets, column headers |
| `TestMovingAverageForecast` | 6 | Window logic, fallback on short series, empty series |
| `TestLinearRegressionForecast` | 6 | Horizon length, trend direction, negative-clipping, edge cases |
| `TestBuildDailyDemand` | 5 | Gap-filling, multi-product, same-day aggregation |
| `TestForecastAllProducts` | 5 | Row count, future dates, max-of-two-models logic |
| `TestDetectLowStock` | 11 | Threshold boundary, camelCase vs snake_case field, missing fields |
| `TestSendSlackAlert` | 4 | No-call on empty, simulation mode, real webhook path, payload content |
| `TestDetectHighQuantityOrders` | 7 | Z-score outlier detection, uniform data, column validation |
| `TestDetectNegativeStock` | 7 | Negative/zero/positive stock, anomaly label, empty input |
| `TestDetectSalesSpikes` | 6 | Revenue spike detection, uniform data, column validation |
| `TestExportAnomalies` | 3 | File creation, correct sheets, empty-data handling |

### Tests skipped in this sandbox (require real infrastructure)

These tests need a live database or API and are **not** in the automated suite. Run them manually on your machine after setup:

```bash
# 1. Test real DB connection (requires SQL Server + .env configured)
python -c "
from config.db import create_db_engine
engine = create_db_engine()
print('DB connection OK:', engine.url)
"

# 2. Test real API connection (requires .NET API running)
python -c "
from scripts.low_stock_alert import fetch_inventory_from_api
products = fetch_inventory_from_api()
print(f'API OK — {len(products)} products returned')
"

# 3. Test full daily report pipeline end-to-end
python -c "
from scripts.daily_sales_report import run_report
from datetime import date
path = run_report(target_date=date.today())
print('Report saved to:', path)
"

# 4. Test full forecast pipeline end-to-end
python -c "
from scripts.inventory_forecast import run_forecast
path = run_forecast(lookback_days=30, horizon=7)
print('Forecast saved to:', path)
"
```

---
