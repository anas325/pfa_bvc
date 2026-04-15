"""
Neo4j storage for RSS articles and sentiment scores.

Schema:
  (:Feed {url, name, language})
  (:Article {url, title, published_at, full_text, language, feed_url})
  (:Company {ticker, name})
  (:Sector {name})
  (:SentimentScore {article_url, sentiment, score, confidence, reasoning, analyzed_at})

  (Feed)-[:PUBLISHED]->(Article)
  (Article)-[:MENTIONS]->(Company)
  (Article)-[:MENTIONS]->(Sector)
  (Article)-[:HAS_SENTIMENT]->(SentimentScore)
"""

import re
from datetime import datetime, timezone

from neo4j import Driver

from .models import ArticleSentiment
from .rss_fetcher import Article

_CONSTRAINTS = [
    "CREATE CONSTRAINT article_url IF NOT EXISTS FOR (a:Article) REQUIRE a.url IS UNIQUE",
    "CREATE CONSTRAINT feed_url IF NOT EXISTS FOR (f:Feed) REQUIRE f.url IS UNIQUE",
    "CREATE CONSTRAINT company_ticker IF NOT EXISTS FOR (c:Company) REQUIRE c.ticker IS UNIQUE",
    "CREATE CONSTRAINT sector_name IF NOT EXISTS FOR (s:Sector) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT sentiment_article_url IF NOT EXISTS FOR (s:SentimentScore) REQUIRE s.article_url IS UNIQUE",
]


def ensure_constraints(driver: Driver) -> None:
    for query in _CONSTRAINTS:
        driver.execute_query(query, database_="neo4j")


def seed_companies(driver: Driver, companies: list[dict]) -> None:
    """MERGE all companies from companies.csv as :Company nodes."""
    for c in companies:
        ticker = c.get("ticker", "").strip()
        name = c.get("libelle", "").strip()
        if not ticker:
            continue
        driver.execute_query(
            "MERGE (c:Company {ticker: $ticker}) SET c.name = $name",
            parameters_={"ticker": ticker, "name": name},
            database_="neo4j",
        )


def seed_sectors(driver: Driver, companies: list[dict]) -> None:
    """MERGE unique :Sector nodes derived from the secteur column."""
    seen: set[str] = set()
    for c in companies:
        raw = c.get("secteur", "").strip()
        sector = re.sub(r"^MASI\s+", "", raw).strip()
        if not sector or sector in seen:
            continue
        seen.add(sector)
        driver.execute_query(
            "MERGE (s:Sector {name: $name})",
            parameters_={"name": sector},
            database_="neo4j",
        )


# ---------------------------------------------------------------------------
# Phase 1 — raw article storage
# ---------------------------------------------------------------------------

def get_stored_article_urls(driver: Driver) -> set[str]:
    """Return URLs of all Article nodes already in Neo4j."""
    result = driver.execute_query(
        "MATCH (a:Article) RETURN a.url AS url",
        database_="neo4j",
    )
    return {record["url"] for record in result.records}


def store_articles_raw(driver: Driver, articles: list[Article]) -> None:
    """Store Feed, Article, and PUBLISHED relationship — no sentiment."""
    for article in articles:
        published_at = article.published_at.isoformat()

        driver.execute_query(
            "MERGE (f:Feed {url: $url}) SET f.name = $name, f.language = $language",
            parameters_={"url": article.feed_url, "name": article.feed_name, "language": article.language},
            database_="neo4j",
        )

        driver.execute_query(
            """
            MERGE (a:Article {url: $url})
            SET a.title = $title,
                a.published_at = $published_at,
                a.full_text = $full_text,
                a.language = $language,
                a.feed_url = $feed_url
            """,
            parameters_={
                "url": article.url,
                "title": article.title,
                "published_at": published_at,
                "full_text": article.full_text[:2000],
                "language": article.language,
                "feed_url": article.feed_url,
            },
            database_="neo4j",
        )

        driver.execute_query(
            """
            MATCH (f:Feed {url: $feed_url})
            MATCH (a:Article {url: $article_url})
            MERGE (f)-[:PUBLISHED]->(a)
            """,
            parameters_={"feed_url": article.feed_url, "article_url": article.url},
            database_="neo4j",
        )


# ---------------------------------------------------------------------------
# Phase 2 — LLM analysis + sentiment storage
# ---------------------------------------------------------------------------

def get_unanalyzed_articles(driver: Driver) -> list[Article]:
    """Return Article nodes that have no SentimentScore yet."""
    result = driver.execute_query(
        """
        MATCH (f:Feed)-[:PUBLISHED]->(a:Article)
        WHERE NOT (a)-[:HAS_SENTIMENT]->()
        RETURN a.url AS url, a.title AS title, a.published_at AS published_at,
               a.full_text AS full_text, a.language AS language,
               a.feed_url AS feed_url, f.name AS feed_name
        """,
        database_="neo4j",
    )
    articles = []
    for record in result.records:
        published_at = datetime.fromisoformat(record["published_at"])
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        articles.append(Article(
            url=record["url"],
            feed_name=record["feed_name"],
            feed_url=record["feed_url"],
            title=record["title"],
            published_at=published_at,
            full_text=record["full_text"] or "",
            language=record["language"] or "fr",
        ))
    return articles


def store_sentiment(
    driver: Driver,
    article_url: str,
    sentiment: ArticleSentiment,
) -> None:
    """Store SentimentScore and MENTIONS relationships for an already-stored article."""
    analyzed_at = datetime.now(timezone.utc).isoformat()

    driver.execute_query(
        """
        MERGE (s:SentimentScore {article_url: $article_url})
        SET s.sentiment = $sentiment,
            s.score = $score,
            s.confidence = $confidence,
            s.reasoning = $reasoning,
            s.analyzed_at = $analyzed_at
        """,
        parameters_={
            "article_url": article_url,
            "sentiment": sentiment.sentiment,
            "score": sentiment.score,
            "confidence": sentiment.confidence,
            "reasoning": sentiment.reasoning,
            "analyzed_at": analyzed_at,
        },
        database_="neo4j",
    )

    driver.execute_query(
        """
        MATCH (a:Article {url: $article_url})
        MATCH (s:SentimentScore {article_url: $article_url})
        MERGE (a)-[:HAS_SENTIMENT]->(s)
        """,
        parameters_={"article_url": article_url},
        database_="neo4j",
    )

    for ticker in sentiment.mentioned_tickers:
        driver.execute_query(
            """
            MATCH (a:Article {url: $article_url})
            MATCH (c:Company {ticker: $ticker})
            MERGE (a)-[:MENTIONS]->(c)
            """,
            parameters_={"article_url": article_url, "ticker": ticker},
            database_="neo4j",
        )

    for sector in sentiment.mentioned_sectors:
        driver.execute_query(
            """
            MATCH (a:Article {url: $article_url})
            MATCH (s:Sector {name: $name})
            MERGE (a)-[:MENTIONS]->(s)
            """,
            parameters_={"article_url": article_url, "name": sector},
            database_="neo4j",
        )


def store_sentiment_batch(
    driver: Driver,
    results: list[tuple[str, ArticleSentiment]],
) -> None:
    for article_url, sentiment in results:
        store_sentiment(driver, article_url, sentiment)
