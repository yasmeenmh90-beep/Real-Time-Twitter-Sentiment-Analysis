import json
from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
collection = client["sentiment_db"]["tweets"]

# Path to your .jsonl file
file_path = "twitter_scraper/logs/tweets.jsonl"

# Load each line as a separate JSON object
with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        try:
            tweet = json.loads(line.strip())

            # Add fallback timestamp if missing
            if "created_at" not in tweet:
                tweet["created_at"] = datetime.utcnow()

            collection.insert_one(tweet)
        except Exception as e:
            print("Error:", e)
