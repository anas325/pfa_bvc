INSERT INTO sentiment_scores (article_url, sentiment, score, confidence, reasoning, analyzed_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (article_url) DO UPDATE SET
    sentiment   = EXCLUDED.sentiment,
    score       = EXCLUDED.score,
    confidence  = EXCLUDED.confidence,
    reasoning   = EXCLUDED.reasoning,
    analyzed_at = EXCLUDED.analyzed_at;
