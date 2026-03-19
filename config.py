"""
ETL Pipeline Configuration — with real pre-configured public data sources.
Replace the contents of your config.py with this file.
"""

from dataclasses import dataclass, field
from typing import List, Optional


# ─── Output Settings ─────────────────────────────────────────────────────────

OUTPUT_DIR      = "output"
LOG_DIR         = "logs"
SAVE_CSV        = True
SAVE_EXCEL      = True
TIMESTAMP_FILES = True
REQUEST_TIMEOUT = 15
REQUEST_DELAY   = 1.5   # polite delay — don't hammer public sites
MAX_RETRIES     = 3


# ─── Source Dataclasses ───────────────────────────────────────────────────────

@dataclass
class TableSource:
    name: str
    url: str
    table_index: int = 0
    headers: Optional[List[str]] = None

@dataclass
class APISource:
    name: str
    url: str
    data_key: Optional[str] = None
    params: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)

@dataclass
class ProductSource:
    name: str
    url: str
    product_selector: str = ".product"
    name_selector: str = ".product-name, h2, h3"
    price_selector: str = ".price, .product-price"
    image_selector: str = "img"
    link_selector: str = "a"
    max_pages: int = 1
    next_page_selector: str = ".next-page, a[rel='next']"

@dataclass
class ArticleSource:
    name: str
    url: str
    article_list_selector: str = "article, .post, .article"
    title_selector: str = "h1, h2, .title"
    body_selector: str = "p, .content, .body"
    date_selector: str = "time, .date, .published"
    author_selector: str = ".author, .byline"
    link_selector: str = "a"
    max_articles: int = 20


# ─── TABLE SOURCES ────────────────────────────────────────────────────────────
# Scrapes real HTML <table> elements from Wikipedia

TABLE_SOURCES: List[TableSource] = [

    # World's largest countries by area (Wikipedia)
    TableSource(
        name="countries_by_area",
        url="https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_area",
        table_index=0,
    ),

    # World's most populous cities (Wikipedia)
    TableSource(
        name="largest_cities",
        url="https://en.wikipedia.org/wiki/List_of_largest_cities",
        table_index=0,
    ),

    # S&P 500 companies list (Wikipedia)
    TableSource(
        name="sp500_companies",
        url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        table_index=0,
    ),

]


# ─── API SOURCES ──────────────────────────────────────────────────────────────
# Free public REST APIs — no API key required

API_SOURCES: List[APISource] = [

    # Open-Meteo: current weather for major world cities
    # Returns hourly temperature, wind speed, precipitation
    APISource(
        name="weather_new_york",
        url="https://api.open-meteo.com/v1/forecast",
        data_key=None,
        params={
            "latitude": 40.71,
            "longitude": -74.01,
            "hourly": "temperature_2m,precipitation,windspeed_10m",
            "forecast_days": 3,
            "timezone": "America/New_York",
        },
    ),

    # REST Countries: facts about every country in the world
    # Returns name, population, area, capital, region, languages, currencies
    APISource(
        name="world_countries",
        url="https://restcountries.com/v3.1/all",
        data_key=None,
        params={"fields": "name,population,area,capital,region,subregion,languages,currencies,flags"},
    ),

    # Open Library: search results for programming books
    APISource(
        name="books_programming",
        url="https://openlibrary.org/search.json",
        data_key="docs",
        params={
            "subject": "programming",
            "limit": 50,
            "fields": "title,author_name,first_publish_year,publisher,language,subject",
        },
    ),

    # CoinGecko: top 50 cryptocurrencies by market cap (no key needed)
    APISource(
        name="crypto_top50",
        url="https://api.coingecko.com/api/v3/coins/markets",
        data_key=None,
        params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False,
        },
    ),

    # NASA Astronomy Picture of the Day archive (last 20 entries)
    APISource(
        name="nasa_apod",
        url="https://api.nasa.gov/planetary/apod",
        data_key=None,
        params={
            "api_key": "DEMO_KEY",   # DEMO_KEY works for light usage; get a free key at api.nasa.gov
            "count": 20,
        },
    ),

]


# ─── PRODUCT SOURCES ──────────────────────────────────────────────────────────
# Books to Scrape — a site built specifically for practicing web scraping

PRODUCT_SOURCES: List[ProductSource] = [

    # All books — scrapes 3 pages of listings (~60 books)
    ProductSource(
        name="books_to_scrape",
        url="https://books.toscrape.com/catalogue/page-1.html",
        product_selector="article.product_pod",
        name_selector="h3 a",
        price_selector=".price_color",
        image_selector="img",
        link_selector="h3 a",
        max_pages=3,
        next_page_selector="li.next a",
    ),

    # Mystery genre only
    ProductSource(
        name="books_mystery",
        url="https://books.toscrape.com/catalogue/category/books/mystery_3/index.html",
        product_selector="article.product_pod",
        name_selector="h3 a",
        price_selector=".price_color",
        image_selector="img",
        link_selector="h3 a",
        max_pages=2,
        next_page_selector="li.next a",
    ),

    # Science Fiction genre only
    ProductSource(
        name="books_scifi",
        url="https://books.toscrape.com/catalogue/category/books/science-fiction_16/index.html",
        product_selector="article.product_pod",
        name_selector="h3 a",
        price_selector=".price_color",
        image_selector="img",
        link_selector="h3 a",
        max_pages=2,
        next_page_selector="li.next a",
    ),

]


# ─── ARTICLE SOURCES ──────────────────────────────────────────────────────────
# Hacker News and public blogs with clean HTML

ARTICLE_SOURCES: List[ArticleSource] = [

    # Hacker News front page stories
    ArticleSource(
        name="hacker_news",
        url="https://news.ycombinator.com/",
        article_list_selector="tr.athing",
        title_selector=".titleline a",
        body_selector=".subtext",
        date_selector=".age a",
        author_selector=".hnuser",
        link_selector=".titleline a",
        max_articles=30,
    ),

    # Python.org blog
    ArticleSource(
        name="python_blog",
        url="https://www.python.org/blogs/",
        article_list_selector="li.list-recent-posts-no-header",
        title_selector="h3 a",
        body_selector="p",
        date_selector="time",
        author_selector=".name",
        link_selector="h3 a",
        max_articles=15,
    ),

]


# ─── Database Output (optional) ───────────────────────────────────────────────

DB_ENABLED  = False
DB_BACKEND  = "sqlite"
DB_IF_EXISTS = "append"
SQLITE_PATH = "etl_data.db"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB   = "etl"
POSTGRES_USER = "postgres"
POSTGRES_PASS = ""


# ─── Notifications (optional) ─────────────────────────────────────────────────

NOTIFICATIONS_ENABLED = False

EMAIL_ENABLED   = False
EMAIL_SMTP_HOST = "smtp.gmail.com"
EMAIL_SMTP_PORT = 587
EMAIL_USERNAME  = ""
EMAIL_PASSWORD  = ""
EMAIL_FROM      = ""
EMAIL_TO        = []

SLACK_ENABLED     = False
SLACK_WEBHOOK_URL = ""
SLACK_CHANNEL     = ""