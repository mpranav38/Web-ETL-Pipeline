# Web ETL Pipeline

Automatic Extract → Transform → Load pipeline for web data, written in Python.
Supports HTML tables, REST APIs/JSON, product listings, and article/blog text.
Outputs to CSV and/or Excel with timestamped filenames.

---

## Project Structure

```
etl_pipeline/
├── config.py                  ← Configure your sources here
├── pipeline.py                ← Main entry point
├── requirements.txt
├── extractors/
│   ├── table_extractor.py     ← HTML <table> scraping
│   ├── api_extractor.py       ← REST API / JSON
│   ├── product_extractor.py   ← Product listing scraping
│   └── article_extractor.py   ← Article / blog text
├── transformers/
│   └── cleaner.py             ← Data cleaning pipeline
├── loaders/
│   └── file_loader.py         ← CSV + Excel output
└── utils/
    ├── http.py                ← Shared HTTP session with retry
    └── logger.py              ← File + console logging
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure your sources
Edit `config.py`. Sources are organised by type:

```python
# REST API example
API_SOURCES = [
    APISource(
        name="products_api",
        url="https://api.example.com/products",
        data_key="results",          # JSON key holding the list
    ),
]

# HTML table example
TABLE_SOURCES = [
    TableSource(
        name="market_data",
        url="https://example.com/stats",
        table_index=0,               # First table on the page
    ),
]

# Product listing example
PRODUCT_SOURCES = [
    ProductSource(
        name="shop_products",
        url="https://shop.example.com/all",
        product_selector=".product-card",
        price_selector=".price",
        max_pages=3,
    ),
]

# Article / blog example
ARTICLE_SOURCES = [
    ArticleSource(
        name="tech_news",
        url="https://blog.example.com",
        article_list_selector="article",
        title_selector="h2",
        body_selector=".entry-content p",
        max_articles=50,
    ),
]
```

### 3. Run the pipeline
```bash
# Single run
python pipeline.py

# Run every 60 minutes (requires apscheduler)
python pipeline.py --schedule 60

# Verbose output
python pipeline.py --log-level DEBUG
```

---

## Output Files

Each run produces files in `output/`:

| File | Description |
|------|-------------|
| `{source_name}_{timestamp}.csv` | One CSV per data source |
| `etl_output_{timestamp}.xlsx` | All sources, one sheet each |
| `run_summary_{timestamp}.json` | Row counts, column lists, file paths |

Logs are written to `logs/etl_run_{timestamp}.log`.

---

## Cleaning Steps Applied

The `DataCleaner` applies these transformations automatically:

1. **Normalise column names** — lowercase, underscores, no special characters
2. **Strip whitespace** — all string columns trimmed
3. **Drop empty columns** — columns with zero non-null values removed
4. **Deduplicate rows** — exact duplicates dropped
5. **Coerce numerics** — columns that look numeric are converted
6. **Parse dates** — columns with "date", "time", "published" in name parsed to `datetime`
7. **Extract price numerics** — price columns get an additional `{col}_numeric` column
8. **Fill string NaN** — remaining string nulls become empty string

---

## Scheduling with Cron (Alternative)

Instead of `--schedule`, add a cron entry:
```bash
# Run every hour at :00
0 * * * * /usr/bin/python3 /path/to/etl_pipeline/pipeline.py >> /path/to/logs/cron.log 2>&1
```

On Windows, use Task Scheduler pointing to `python pipeline.py`.

---

## Adding a New Extractor

1. Add a dataclass to `config.py` (e.g. `SitemapSource`)
2. Create `extractors/sitemap_extractor.py` — return a `pd.DataFrame`
3. Add it to `pipeline.py` in the Extract section
4. Add source instances to `config.py`

The `DataCleaner` and `FileLoader` work on any DataFrame — no changes needed there.
