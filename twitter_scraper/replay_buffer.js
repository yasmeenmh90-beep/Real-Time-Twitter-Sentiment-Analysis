import Redis from 'ioredis';
import { Kafka } from 'kafkajs';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const kafka = new Kafka({ clientId: 'replay', brokers: [process.env.KAFKA_BROKER || 'localhost:9092'] });
const prod = kafka.producer();

(async () => {
  await prod.connect();
  const tweets = await redis.lrange('tweets:buffer', 0, -1);
  console.log(`🌀 Replaying ${tweets.length} buffered tweets…`);

  for (const t of tweets.reverse()) {
    try {
      const tweet = JSON.parse(t);
      await prod.send({
        topic: 'tweets',
        messages: [{ key: tweet.ids, value: t }],
      });
      await redis.lrem('tweets:buffer', 0, t);
      await redis.lpush('replayed:ids', tweet.ids);
      await redis.ltrim('replayed:ids', 0, 19); // keep only latest 20

      console.log(`✅ Replayed: ${tweet.ids}`);
    } catch (err) {
      console.error('❌ Failed to replay tweet:', err.message);
    }
  }

  await prod.disconnect();
  process.exit(0);
})();
