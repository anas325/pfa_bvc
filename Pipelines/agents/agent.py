"""
Company research agent.
Reads companies from Data/companies.csv, searches the web for each one,
and saves results to the path configured in search_config.yaml.

Required env vars:
  TAVILY_API_KEY
  OPENROUTER_API_KEY
"""

import csv
import json
import os
import sys
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

import yaml
from pydantic import BaseModel, create_model
from langchain_openai import ChatOpenAI
from langchain_community.tools.tavily_search import TavilySearchResults
from langchain.agents import create_agent

load_dotenv()

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from monitoring import PipelineLogger
CONFIG_FILE = ROOT / "config" / "search_config.yaml"
COMPANIES_CSV = ROOT / "data" / "companies.csv"

TYPE_MAP = {
    "string":  Optional[str],
    "integer": Optional[int],
    "number":  Optional[float],
}


def load_config() -> dict:
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f)


def load_companies() -> list[dict]:
    with open(COMPANIES_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_output_model(fields: list[dict]) -> type[BaseModel]:
    """Dynamically build a Pydantic model from the config field definitions."""
    field_defs = {
        f["name"]: (TYPE_MAP.get(f.get("type", "string"), Optional[str]), None)
        for f in fields
    }
    return create_model("CompanyResearch", **field_defs)


def build_research_prompt(company_name: str, fields: list[dict]) -> str:
    questions = "\n".join(
        f"- {field['prompt'].format(company=company_name)}"
        for field in fields
    )
    return (
        f"Research the company '{company_name}' using web search.\n"
        f"Find answers to the following:\n{questions}\n\n"
        f"Gather as much reliable information as possible before concluding."
    )


def build_extraction_prompt(company_name: str, research: str, fields: list[dict]) -> str:
    questions = "\n".join(
        f"- {field['name']}: {field['prompt'].format(company=company_name)}"
        for field in fields
    )
    return (
        f"Based on the research below, extract structured information about '{company_name}'.\n\n"
        f"Research:\n{research}\n\n"
        f"Fields to extract:\n{questions}"
    )


def research_company(
    research_agent,
    extraction_llm,
    company: dict,
    fields: list[dict],
    output_model: type[BaseModel],
) -> dict:
    company_name = company["company_name"]
    print(f"  Researching: {company_name}")

    # Phase 1: agent gathers raw information via web search
    research_prompt = build_research_prompt(company_name, fields)
    response = research_agent.invoke({"messages": [("human", research_prompt)]})
    gathered_info = response["messages"][-1].content

    # Phase 2: structured extraction enforced by Pydantic schema
    extraction_prompt = build_extraction_prompt(company_name, gathered_info, fields)
    structured = extraction_llm.invoke(extraction_prompt)

    return {
        "ticker": company["ticker"],
        "company_name": company_name,
        **structured.model_dump(),
    }


def main():
    config = load_config()
    fields = config["fields"]
    search_cfg = config.get("search", {})
    llm_cfg = config.get("llm", {})
    output_path = ROOT / config["output"]["file"]
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_model = build_output_model(fields)

    llm = ChatOpenAI(
        base_url=llm_cfg.get("base_url", "https://openrouter.ai/api/v1"),
        api_key=os.getenv("OPENROUTER_API_KEY"),
        model=llm_cfg.get("model", "openrouter/auto"),
        temperature=0,
    )
    search_tool = TavilySearchResults(
        max_results=search_cfg.get("max_results", 5),
        search_depth=search_cfg.get("search_depth", "advanced"),
    )

    research_agent = create_agent(llm, tools=[search_tool])
    extraction_llm = llm.with_structured_output(output_model)

    companies = load_companies()
    results = []

    if output_path.exists():
        with open(output_path, encoding="utf-8") as f:
            results = json.load(f)
        done_tickers = {r["ticker"] for r in results}
        companies = [c for c in companies if c["ticker"] not in done_tickers]
        print(f"Resuming — {len(done_tickers)} already done, {len(companies)} remaining.\n")

    with PipelineLogger("agent") as log:
        log.metric("companies_total", len(companies), stage="setup")
        for i, company in enumerate(companies, 1):
            ticker = company["ticker"]
            print(f"[{i}/{len(companies)}]", end=" ")
            try:
                result = research_company(research_agent, extraction_llm, company, fields, output_model)
                results.append(result)
                log.increment_processed()
                log.event(
                    f"researched {company['company_name']}",
                    stage="research",
                    item_key=ticker,
                )
            except Exception as e:
                print(f"  ERROR: {e}")
                null_fields = {f["name"]: None for f in fields}
                results.append({"ticker": ticker, "company_name": company["company_name"], "error": str(e), **null_fields})
                log.increment_failed()
                log.event(
                    f"research failed for {ticker}: {e}",
                    level="error",
                    stage="research",
                    item_key=ticker,
                )

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\nDone. Results saved to {output_path}")


if __name__ == "__main__":
    missing = [v for v in ("TAVILY_API_KEY", "OPENROUTER_API_KEY") if not os.getenv(v)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    main()
