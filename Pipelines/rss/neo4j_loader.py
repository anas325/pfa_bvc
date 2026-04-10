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
    import re
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
    import re
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


def get_already_processed_urls(driver: Driver) -> set[str]:
    """Return the set of article URLs that already have a SentimentScore node."""
    result = driver.execute_query(
        "MATCH (s:SentimentScore) RETURN s.article_url AS url",
        database_="neo4j",
    )
    return {record["url"] for record in result.records}


def store_article_with_sentiment(
    driver: Driver,
    article: Article,
    sentiment: ArticleSentiment,
) -> None:
    """
    Single call that MERGEs Feed, Article, SentimentScore, and all relationships.
    Tickers and sectors are read directly from sentiment.mentioned_tickers/sectors.
    """
    analyzed_at = datetime.now(timezone.utc).isoformat()
    published_at = article.published_at.isoformat()

    # Upsert Feed
    driver.execute_query(
        "MERGE (f:Feed {url: $url}) SET f.name = $name, f.language = $language",
        parameters_={"url": article.feed_url, "name": article.feed_name, "language": article.language},
        database_="neo4j",
    )

    # Upsert Article
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
            "full_text": article.full_text[:2000],  # cap to avoid huge nodes
            "language": article.language,
            "feed_url": article.feed_url,
        },
        database_="neo4j",
    )

    # Feed -> Article
    driver.execute_query(
        """
        MATCH (f:Feed {url: $feed_url})
        MATCH (a:Article {url: $article_url})
        MERGE (f)-[:PUBLISHED]->(a)
        """,
        parameters_={"feed_url": article.feed_url, "article_url": article.url},
        database_="neo4j",
    )

    # Upsert SentimentScore
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
            "article_url": article.url,
            "sentiment": sentiment.sentiment,
            "score": sentiment.score,
            "confidence": sentiment.confidence,
            "reasoning": sentiment.reasoning,
            "analyzed_at": analyzed_at,
        },
        database_="neo4j",
    )

    # Article -> SentimentScore
    driver.execute_query(
        """
        MATCH (a:Article {url: $article_url})
        MATCH (s:SentimentScore {article_url: $article_url})
        MERGE (a)-[:HAS_SENTIMENT]->(s)
        """,
        parameters_={"article_url": article.url},
        database_="neo4j",
    )

    # Article -> Company MENTIONS
    for ticker in sentiment.mentioned_tickers:
        driver.execute_query(
            """
            MATCH (a:Article {url: $article_url})
            MATCH (c:Company {ticker: $ticker})
            MERGE (a)-[:MENTIONS]->(c)
            """,
            parameters_={"article_url": article.url, "ticker": ticker},
            database_="neo4j",
        )

    # Article -> Sector MENTIONS
    for sector in sentiment.mentioned_sectors:
        driver.execute_query(
            """
            MATCH (a:Article {url: $article_url})
            MATCH (s:Sector {name: $name})
            MERGE (a)-[:MENTIONS]->(s)
            """,
            parameters_={"article_url": article.url, "name": sector},
            database_="neo4j",
        )


def store_batch(
    driver: Driver,
    results: list[tuple[Article, ArticleSentiment]],
) -> None:
    for article, sentiment in results:
        store_article_with_sentiment(driver, article, sentiment)
