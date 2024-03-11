"""
Save the market news to a file so that students can choose
to not create an Alpaca account.

Usage:
python -m examples.alpaca.historical_news_create_file

cat market-news.txt | rpk topic produce market-news -f '%k %v\n'
"""
import json

from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

from config import (ADDITIONAL_TEXT_FILTERS, BACKFILL_END, BACKFILL_START,
                    SYMBOLS)
from data.providers import ALPACA, DATA_PROVIDER_KEY
from utils import alpaca_utils

# Create a sentiment analyzer
sia = SIA()

# Script-level onfigs
OUTPUT_FILE = "market-news.txt"

# Pull live data from Alpaca
print(f"Pulling historical news data for symbols: {SYMBOLS}")

# Pull news articles for the provided symbol
news = alpaca_utils.get_historical_news_data(
    SYMBOLS, BACKFILL_START, BACKFILL_END, limit=10_000
)


def get_sentiment(text):
    scores = sia.polarity_scores(text)
    return scores["compound"]


success_count = 0
error_count = 0

with open(OUTPUT_FILE, mode="w", newline="") as txt_file:
    # Iterate through the news articles and produce each record to Redpanda
    for i, row in enumerate(news):
        # The SDK returns a NewsV2 object. row._raw allows us to access the record
        # in dictionary form
        article = row._raw

        # Apply additional text filters to reduce the noise. e.g. if the additional
        # text filters are set to ["Apple", "Tesla"], then the headline must also
        # include the words "Apple" or "Tesla".
        should_proceed = False
        for term in ADDITIONAL_TEXT_FILTERS:
            if term in article["headline"]:
                should_proceed = True

        if not should_proceed:
            continue

        # Covert the timestamp to milliseconds
        timestamp_ms = int(row.created_at.timestamp() * 1000)
        article["timestamp_ms"] = timestamp_ms

        # Add an identifier for the data provider
        article[DATA_PROVIDER_KEY] = ALPACA

        # Calculate the sentiment
        article["sentiment"] = get_sentiment(article["headline"])

        # The article may relate to multiple symbols. Produce a separate record
        # for each matched search symbol.
        article_symbols = article.pop("symbols")

        for search_symbol in SYMBOLS:
            if not search_symbol in article_symbols:
                continue

            article["symbol"] = search_symbol

            # Rename keys that are reserved words in Flink since
            # we need to port this to Killercoda and there's a bug
            # with escaping reserved words
            article['article_url'] = article.pop('url')

            # Produce the news article to Redpanda
            try:
                # Convert the JSON record to a string
                json_record = json.dumps(article)

                # Write the line to the text file
                txt_file.write(f"{search_symbol} {json_record}\n")

                if success_count > 0 and success_count % 100 == 0:
                    print(f"Produced {i} records")

                success_count += 1
            except Exception as e:
                error_count += 1
                print(e)

print(f"Produced {success_count} records to file: {OUTPUT_FILE}")
print(f"Encountered {error_count} errors")
