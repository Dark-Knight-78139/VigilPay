import redis.asyncio as redis
from loguru import logger

class RedisStateStore:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client = None

    async def connect(self):
        self.client = redis.from_url(self.redis_url, decode_responses=True)
        # Verify connection
        await self.client.ping()
        logger.info(f"Connected to Redis at {self.redis_url}")

    async def disconnect(self):
        if self.client:
            await self.client.aclose()
            logger.info("Disconnected from Redis")
