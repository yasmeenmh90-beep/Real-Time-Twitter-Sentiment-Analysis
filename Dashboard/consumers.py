# dashboard/websockets/consumers.py
import json, asyncio
from collections import Counter

from channels.generic.websocket import AsyncJsonWebsocketConsumer
from aiokafka import AIOKafkaConsumer
from pymongo import MongoClient

BOOTSTRAP = "localhost:9092"
TOPIC     = "twitter_sentiment"

class SentimentConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("sentiment", self.channel_name)
        await self.accept()

        self.kc = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id="dashboard_frontend",
            value_deserializer=lambda b: json.loads(b.decode())
        )
        await self.kc.start()
        self.c_task = asyncio.create_task(self.stream())

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("sentiment", self.channel_name)
        self.c_task.cancel()
        await self.kc.stop()

    async def receive_json(self, content):
        if content.get("type") == "sentiment.update":
            try:
                client = MongoClient("mongodb://localhost:27017/")
                db = client["sentiment_db"]
                collection = db["tweets"]

                # Count recent tweets (e.g., last 30 minutes if needed)
                pipeline = [
                    {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
                ]
                result = collection.aggregate(pipeline)
                count_map = Counter({doc["_id"]: doc["count"] for doc in result})

                await self.send_json({
                    "type": "stats",
                    "payload": {
                        "pos": count_map.get("pos", 0),
                        "neu": count_map.get("neu", 0),
                        "neg": count_map.get("neg", 0),
                    }
                })

            except Exception as e:
                print(f"[MongoDB error] {e}")
                await self.send_json({
                    "type": "stats",
                    "payload": {
                        "positive": 0,
                        "neutral": 0,
                        "negative": 0
                    }
                })

    async def stream(self):
        counts = {"positive": 0, "neutral": 0, "negative": 0}
        while True:
            try:
                async for msg in self.kc:
                    d = msg.value
                    label = d.get("sentiment") or d.get("sentiment_label")

                    if label is None:
                        # skip if there's no sentiment info at all
                        return  # or: continue / log a warning

                    counts[label] += 1

                    await self.send_json({
                        "type": "tweet",
                        "payload": {
                            "id": d.get("ids", ""),
                            "text": d.get("text", ""),
                            "sentiment": label,
                            "prediction": d.get("prediction"),
                            "timestamp": str(d.get("created_at") or d.get("date_ts", "")),
                            "hashtags": d.get("hashtags", []),
                            "processed_text": d.get("processed_text", ""),
                            "vader_score": d.get("vader_score", None),
                            "hour": d.get("hour", None),
                            "day_of_week": d.get("day_of_week", None),
                            "is_weekend": d.get("is_weekend", None),
                            "has_mentions": d.get("has_mentions", None),
                            "has_hashtags": d.get("has_hashtags", None),
                            "has_links": d.get("has_links", None),
                            "is_question": d.get("is_question", None),
                            "text_length": d.get("text_length", None),
                            "word_count": d.get("word_count", None),
                            "char_density": d.get("char_density", None),
                            "capital_word_count": d.get("capital_word_count", None),
                            "negation_count": d.get("negation_count", None),
                            "emoji_count": d.get("emoji_count", None),
                            "sentiment_keyword_count": d.get("sentiment_keyword_count", None)
                        }
                    })

                    if sum(counts.values()) % 5 == 0:
                        await self.send_json({
                            "type": "stats",
                            "payload": counts,
                        })
            except Exception as e:
                print(f"[Kafka error] {e}")
                await asyncio.sleep(1)

    async def sentiment_update(self, event):
        await self.send(text_data=json.dumps({
            "ids": event["message"]["ids"],
            "text": event["message"]["text"],
            "sentiment": event["message"]["sentiment"],
            "date_ts": event["message"]["date_ts"],
            "vader_score": event["message"].get("vader_score"),
            "emoji_count": event["message"].get("emoji_count"),
        }))
