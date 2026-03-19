# 🔄 Web ETL Pipeline

> Automatic **Extract → Transform → Load** pipeline for web data, written in Python.
> Scrapes HTML tables, REST APIs, product listings, and articles — cleans the data automatically — and exports to CSV, Excel, or a database.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

---

## ✨ Features

- **4 extractor types** — HTML tables, REST APIs/JSON, product listings, article text
- **Automatic data cleaning** — deduplication, type coercion, date parsing, price extraction
- **Multiple outputs** — CSV, Excel (.xlsx), SQLite, PostgreSQL
- **Web dashboard** — trigger runs, stream live logs, download files from the browser
- **Notifications** — Slack and email alerts after every run
- **Scheduling** — built-in scheduler or cron/Task Scheduler
- **Polite scraping** — request delays, automatic retry with backoff, structured logging

---

## 📁 Project Structure

```
etl_pipeline/
├── pipeline.py                 ← Main entry point
├── config.py                   ← All source definitions & settings
├── requirements.txt
│
├── extractors/
│   ├── table_extractor.py      ← HTML <table> scraping (Wikipedia, etc.)
│   ├── api_extractor.py        ← REST API / JSON endpoints
│   ├── product_extractor.py    ← E-commerce product listings
│   └── article_extractor.py    ← Blog / news article text
│
├── transformers/
│   └── cleaner.py              ← 8-step automated data cleaning
│
├── loaders/
│   ├── file_loader.py          ← CSV + Excel output
│   └── db_loader.py            ← SQLite / PostgreSQL output
│
├── dashboard/
│   └── app.py                  ← Flask web UI
│
└── utils/
    ├── http.py                 ← Shared HTTP session with retry
    ├── logger.py               ← File + console logging
    └── notifier.py             ← Email & Slack notifications
```

---

## 🚀 Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/your-username/etl-pipeline.git
cd etl-pipeline
```

### 2. Create a virtual environment

```bash
python -m venv venv

# Mac / Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure your sources

Open `config.py` and add your data sources. Ready-to-use public examples:

```python
# Free REST API — no key required
API_SOURCES = [
    APISource(
        name="world_countries",
        url="https://restcountries.com/v3.1/all",
        params={"fields": "name,population,area,capital,region"},
    ),
    APISource(
        name="crypto_top50",
        url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 50},
    ),
]

# Wikipedia table
TABLE_SOURCES = [
    TableSource(
        name="sp500_companies",
        url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        table_index=0,
    ),
]

# Product scraping (books.toscrape.com is built for this)
PRODUCT_SOURCES = [
    ProductSource(
        name="books",
        url="https://books.toscrape.com/catalogue/page-1.html",
        product_selector="article.product_pod",
        name_selector="h3 a",
        price_selector=".price_color",
        max_pages=3,
        next_page_selector="li.next a",
    ),
]

# Article scraping
ARTICLE_SOURCES = [
    ArticleSource(
        name="hacker_news",
        url="https://news.ycombinator.com/",
        article_list_selector="tr.athing",
        title_selector=".titleline a",
        link_selector=".titleline a",
        max_articles=30,
    ),
]
```

### 5. Run the pipeline

```bash
python pipeline.py
```

Output files are saved to the `output/` folder:

| File | Description |
|------|-------------|
| `{source_name}_{timestamp}.csv` | One CSV per data source |
| `etl_output_{timestamp}.xlsx` | All sources in one Excel workbook |
| `run_summary_{timestamp}.json` | Row counts, columns, file paths |

---

## 🖥️ Web Dashboard

Start the dashboard and trigger runs from your browser:

```bash
python dashboard/app.py
```

Open **http://localhost:5000**

**Dashboard features:**
- View all configured sources at a glance
- Click ▶ Run Pipeline — watch live logs stream in real time
- Browse run history with row counts and durations
- Download any output file directly

---

## 🗄️ Database Output

Enable in `config.py`:

**SQLite** (zero config):
```python
DB_ENABLED  = True
DB_BACKEND  = "sqlite"
SQLITE_PATH = "etl_data.db"
```

**PostgreSQL:**
```python
DB_ENABLED    = True
DB_BACKEND    = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_DB   = "mydb"
POSTGRES_USER = "postgres"
POSTGRES_PASS = "yourpassword"
```

---

## 🔔 Notifications

Enable in `config.py`:

**Slack:**
```python
NOTIFICATIONS_ENABLED = True
SLACK_ENABLED         = True
SLACK_WEBHOOK_URL     = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
SLACK_CHANNEL         = "#data-ops"
```

**Email (Gmail):**
```python
NOTIFICATIONS_ENABLED = True
EMAIL_ENABLED         = True
EMAIL_USERNAME        = "you@gmail.com"
EMAIL_PASSWORD        = "your-app-password"
EMAIL_TO              = ["teammate@company.com"]
```

> **Gmail tip:** generate an App Password at **myaccount.google.com → Security → App Passwords**

---

## ⏰ Scheduling

**Built-in scheduler:**
```bash
python pipeline.py --schedule 60   # every 60 minutes
```

**Cron (Mac / Linux):**
```bash
crontab -e
# Add:
0 * * * * /path/to/venv/bin/python /path/to/etl_pipeline/pipeline.py
```

**Windows:** use Task Scheduler pointing to `python pipeline.py`

---

## 🧹 Cleaning Steps Applied Automatically

| Step | What it does |
|------|-------------|
| Normalise column names | Lowercase, underscores, strip special characters |
| Strip whitespace | Trim all string values |
| Drop empty columns | Remove columns with zero non-null values |
| Deduplicate rows | Drop exact duplicate rows |
| Coerce numerics | Convert numeric-looking strings to float/int |
| Parse dates | Auto-detect and parse date/time columns |
| Extract price numerics | Add `{col}_numeric` for price strings |
| Fill string nulls | Replace remaining NaN strings with "" |

---

## ⚙️ CLI Options

```
python pipeline.py --help

  --schedule MINUTES    Run every N minutes automatically
  --log-level LEVEL     DEBUG | INFO | WARNING | ERROR
```

---

## 🔧 Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` inside your venv |
| No products matched selector | Inspect the page with browser DevTools → update the CSS selector in `config.py` |
| Empty DataFrame | Check the URL is publicly accessible |
| Dashboard won't start | `pip install flask` |
| Gmail auth error | Use an App Password, not your account password |
| Rate limited / blocked | Increase `REQUEST_DELAY` in `config.py` |

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `requests` | HTTP requests |
| `beautifulsoup4` + `lxml` | HTML parsing |
| `pandas` | Data processing |
| `openpyxl` | Excel output |
| `sqlalchemy` | Database abstraction |
| `flask` | Web dashboard |
| `apscheduler` | Built-in scheduling |
| `psycopg2-binary` | PostgreSQL driver (optional) |

---

## 📄 License

MIT — free to use, modify, and distribute.

---

## 🙌 Contributing

Pull requests are welcome! For major changes, open an issue first.

1. Fork the repo
2. Create your branch: `git checkout -b feature/my-extractor`
3. Commit: `git commit -m "Add sitemap extractor"`
4. Push: `git push origin feature/my-extractor`
5. Open a Pull Request
