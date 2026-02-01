import json
import time
import argparse
from kafka import KafkaProducer

def replay_log_to_kafka(
    jsonl_path: str,
    kafka_topic: str = "tweets",
    bootstrap_servers: str = "localhost:9092",
    sleep_ms: int = 100
):
    # Kafka setup
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

    print(f"🚀 Replaying tweets from {jsonl_path} to topic '{kafka_topic}'")

    sent = 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                tweet = json.loads(line.strip())
                if not tweet.get("text") or not tweet.get("ids"):
                    continue  # Skip invalid
                producer.send(kafka_topic, tweet)
                sent += 1
                if sent % 100 == 0:
                    print(f"✅ Sent {sent} tweets…")
                if sleep_ms > 0:
                    time.sleep(sleep_ms / 1000)
            except Exception as e:
                print(f"[❌ ERROR] Failed to send tweet: {e}")

    producer.flush()
    producer.close()
    print(f"✅ Done. Replayed {sent} tweets to topic '{kafka_topic}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay tweets from JSONL to Kafka")
    parser.add_argument("--path", type=str, required=True, help="Path to tweets.jsonl")
    parser.add_argument("--topic", type=str, default="tweets", help="Kafka topic name")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Kafka broker")
    parser.add_argument("--sleep", type=int, default=100, help="Delay between messages (ms)")

    args = parser.parse_args()

    replay_log_to_kafka(
        jsonl_path=args.path,
        kafka_topic=args.topic,
        bootstrap_servers=args.broker,
        sleep_ms=args.sleep
    )
