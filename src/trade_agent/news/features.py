from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from trade_agent.news.normalize import NormalizedNews


@dataclass
class NewsFeatures:
    sentiment: float
    keyword_flags: dict[str, bool]
    source_weight: float
    language: str
    extracted_at: str


_ANALYZER = SentimentIntensityAnalyzer()


def _detect_language(text: str) -> str:
    if any(ord(ch) > 127 for ch in text):
        return "non_en"
    return "en"


def _sentiment_score(text: str, language: str) -> float:
    if language != "en" or not text:
        return 0.0
    return float(_ANALYZER.polarity_scores(text)["compound"])


def extract_features(
    news: NormalizedNews, keyword_flags: list[str], source_weights: dict[str, float]
) -> NewsFeatures:
    language = _detect_language(news.title)
    sentiment = _sentiment_score(news.title, language)
    flags: dict[str, bool] = {}
    title_lower = news.title.lower()
    for keyword in keyword_flags:
        flags[keyword] = keyword.lower() in title_lower
    source_weight = float(source_weights.get(news.source, 1.0))
    extracted_at = datetime.now(timezone.utc).isoformat()
    return NewsFeatures(
        sentiment=sentiment,
        keyword_flags=flags,
        source_weight=source_weight,
        language=language,
        extracted_at=extracted_at,
    )


def aggregate_sentiment(features: list[dict[str, Any]]) -> float:
    if not features:
        return 0.0
    weighted = [f["sentiment"] * f["source_weight"] for f in features]
    return sum(weighted) / max(sum(abs(f["source_weight"]) for f in features), 1.0)
