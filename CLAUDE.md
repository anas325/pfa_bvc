# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Research and data pipeline project for BVC (Bourse des Valeurs de Casablanca) companies. The goal is to enrich a list of listed companies with structured data via AI-powered web research, and store results in a Neo4j graph database.

## Directory Structure

```
pfa_bvc/
├── cypher/                  # Neo4j Cypher queries (currently empty)
├── Data/                    # Raw input data
│   ├── companies.csv        # Master company list
│   └── companies_detail.csv
├── Pipelines/               # Python data pipeline (uv-managed project)
│   ├── agents/agent.py      # LangChain research + extraction agent
│   ├── config/search_config.yaml  # Field definitions, LLM, search settings
│   ├── data/                # Pipeline input/output data
│   │   ├── companies.csv
│   │   ├── companies_detail.csv
│   │   └── companies_research.json  # Agent output (gitignored)
│   ├── db/db.py             # Neo4j connection helper
│   ├── scrapers/            # Scrapy spider scaffolding
│   ├── main.py
│   └── pyproject.toml
```

## Pipelines Project

**Runtime:** Python 3.12, managed with `uv`.

**Key dependencies:** LangChain, LangChain-OpenAI, Tavily search, Neo4j driver, Scrapy + Playwright, PyYAML, Pydantic.

**How the agent pipeline works (`agents/agent.py`):**
1. Reads company list from `data/companies.csv` (columns: `ticker`, `company_name`).
2. For each company, runs a two-phase pipeline:
   - **Phase 1 (research):** LangChain agent uses Tavily web search to gather raw info.
   - **Phase 2 (extraction):** LLM with structured output (Pydantic model) extracts typed fields.
3. Results are appended to `data/companies_research.json` after each company (resume-safe).
4. Fields are fully configurable via `config/search_config.yaml` — no code changes needed to add/remove fields.

**Required env vars** (set in `Pipelines/.env`):
- `TAVILY_API_KEY`
- `OPENROUTER_API_KEY`

**LLM:** OpenRouter API (`https://openrouter.ai/api/v1`), model configurable in `search_config.yaml`.

**Database:** Neo4j at `bolt://localhost:7687` (credentials in `db/db.py`).

## Running the Pipeline

```bash
cd Pipelines
uv run python agents/agent.py
```

## Cypher Directory

The `cypher/` directory is reserved for Neo4j Cypher scripts to load/query the researched data. Currently empty — to be populated as the graph schema is defined.
