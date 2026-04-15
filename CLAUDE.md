# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Research and data pipeline project for BVC (Bourse des Valeurs de Casablanca) companies. The goal is to enrich a list of listed companies with structured data via AI-powered web research, run sentiment analysis on financial news, and store results in a Neo4j graph database.

## Directory Structure

```
pfa_bvc/
├── Data/                    # Raw input data
│   ├── companies.csv        # Master company list
│   └── companies_detail.csv
├── Pipelines/               # Python data pipeline (uv-managed project)
│   ├── agents/
│   │   └── agent.py         # LangChain research + extraction agent
│   ├── config/
│   │   ├── search_config.yaml   # Field definitions, LLM, search settings (agent pipeline)
│   │   └── rss_feeds.yaml       # RSS feed list + LLM/fetcher settings (RSS pipeline)
│   ├── data/                # Pipeline input/output data
│   │   ├── companies.csv
│   │   ├── companies_detail.csv
│   │   ├── companies_research.json   # Agent output (gitignored)
│   │   └── lematin_companies.json    # LeMatin scraper output
│   ├── db/db.py             # Neo4j connection helper
│   ├── rss/                 # RSS sentiment pipeline module
│   │   ├── __init__.py
│   │   ├── pipeline.py      # Entry point: fetch → analyze → store
│   │   ├── rss_fetcher.py   # feedparser-based fetcher + full-text scrapers
│   │   ├── analyzer.py      # LLM sentiment analyzer (ArticleAnalyzer protocol)
│   │   ├── models.py        # Pydantic models: Article, ArticleSentiment
│   │   └── neo4j_loader.py  # Neo4j schema + storage functions
│   ├── scrapers/            # Scrapy spiders
│   │   ├── settings.py
│   │   └── spiders/
│   │       ├── bkam_spider.py     # BKAM foreign exchange rates
│   │       └── lematin_spider.py  # LeMatin company news
│   ├── exp.ipynb            # Exploration notebook
│   ├── main.py
│   └── pyproject.toml
├── Services/                # Docker infrastructure
│   ├── docker-compose.yml   # Neo4j + Postgres services
│   └── postgres/init.sql
└── cypher/                  # Neo4j Cypher queries (reserved for future use)
```

## Pipelines Project

**Runtime:** Python 3.12, managed with `uv`.

**Key dependencies:** LangChain, LangChain-OpenAI, LangChain-Ollama, Tavily search, Neo4j driver, Scrapy + Playwright, feedparser, BeautifulSoup4, html5lib, bvcscrap, pandas, PyYAML, Pydantic.

---

### Agent Pipeline (`agents/agent.py`)

Enriches the company list with structured data via LLM-powered web research.

1. Reads company list from `data/companies.csv` (columns: `ticker`, `company_name`).
2. For each company, runs a two-phase pipeline:
   - **Phase 1 (research):** LangChain agent uses Tavily web search to gather raw info.
   - **Phase 2 (extraction):** LLM with structured output (Pydantic model) extracts typed fields.
3. Results are appended to `data/companies_research.json` after each company (resume-safe).
4. Fields are fully configurable via `config/search_config.yaml` — no code changes needed to add/remove fields.

```bash
cd Pipelines
uv run python agents/agent.py
```

---

### RSS Sentiment Pipeline (`rss/pipeline.py`)

Fetches Moroccan financial news from RSS feeds, scores sentiment with an LLM, and stores results in Neo4j. Resumable — articles already in Neo4j are skipped.

**Flow:**
1. Load feeds from `config/rss_feeds.yaml` + companies from `data/companies.csv`.
2. `rss_fetcher.py` — fetch all enabled feeds via `feedparser`, filter by `lookback_days`. For feeds with a `scraper:` key, fetch the full article body (LeMatin, Challenge scrapers are registered in `SCRAPERS` dict).
3. `analyzer.py` — `LLMArticleAnalyzer` sends each article to an OpenRouter LLM via `langchain-openai`, returning structured `ArticleSentiment` (sentiment label + score + confidence + reasoning + mentioned tickers + mentioned sectors).
4. `neo4j_loader.py` — stores Feed, Article, SentimentScore nodes and MENTIONS/HAS_SENTIMENT/PUBLISHED relationships.

```bash
cd Pipelines
uv run python -m rss.pipeline
```

**Adding a new full-text scraper:** Register a `(url, timeout) -> str` function in `SCRAPERS` dict in `rss/rss_fetcher.py`, then set `scraper: <key>` in the feed entry in `rss_feeds.yaml`.

**Adding a new analyzer implementation:** Create a class with `analyze(article) -> ArticleSentiment`, add an `elif` branch in `build_analyzer()` in `rss/analyzer.py`, and set `llm.analyzer: <key>` in `rss_feeds.yaml`.

---

### Scrapy Spiders (`scrapers/spiders/`)

- **`bkam_spider.py`** — scrapes BKAM foreign exchange (banknote) rates for a date range. Run with `uv run scrapy crawl bkam -a start_d=01-01-2024 -a end=31-12-2024`.
- **`lematin_spider.py`** — scrapes LeMatin company news.

---

## Neo4j Graph Schema

Managed by `rss/neo4j_loader.py`:

```
(:Feed {url, name, language})
(:Article {url, title, published_at, full_text, language, feed_url})
(:Company {ticker, name})
(:Sector {name})
(:SentimentScore {article_url, sentiment, score, confidence, reasoning, analyzed_at})

(Feed)-[:PUBLISHED]->(Article)
(Article)-[:MENTIONS]->(Company)
(Article)-[:MENTIONS]->(Sector)
(Article)-[:HAS_SENTIMENT]->(SentimentScore)
```

Unique constraints are auto-created on startup via `ensure_constraints()`.

---

## Services (Docker)

```bash
cd Services
docker compose up -d
```

- **Neo4j** — `bolt://localhost:7687`, `http://localhost:7474`.
- **Postgres** — `localhost:5432`, db `pfa_bvc`.

---

## Required Environment Variables

Set in `Pipelines/.env`:

| Variable            | Used by                        |
|---------------------|--------------------------------|
| `TAVILY_API_KEY`    | Agent pipeline (web search)    |
| `OPENROUTER_API_KEY`| Agent pipeline + RSS pipeline  |
| `NEO4J_PASSWORD`    | RSS pipeline (checked at startup) |

**LLM:** OpenRouter API (`https://openrouter.ai/api/v1`), model configurable per pipeline in `search_config.yaml` / `rss_feeds.yaml`.
