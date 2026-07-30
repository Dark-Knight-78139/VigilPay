import time
from src.infrastructure.redis_client import RedisStateStore
from src.domain.events import TransactionPayload, FeaturesPayload

class FeatureComputer:
    def __init__(self, store: RedisStateStore):
        self.store = store

    async def compute_features(self, payload: TransactionPayload) -> FeaturesPayload:
        """
        Computes stateful features using Redis.
        Uses Redis Pipelines for atomicity and latency reduction.
        """
        if not self.store.client:
            raise RuntimeError("Redis client is not connected.")

        customer_id = payload.customer_id
        current_time = int(payload.timestamp.timestamp())
        amount = payload.amount

        # Keys
        velocity_key = f"customer:{customer_id}:tx_velocity"
        amounts_key = f"customer:{customer_id}:tx_amounts"
        location_key = f"customer:{customer_id}:last_location"

        one_hour_ago = current_time - 3600
        twenty_four_hours_ago = current_time - 86400

        async with self.store.client.pipeline(transaction=True) as pipe:
            # 1. Velocity (1h)
            pipe.zremrangebyscore(velocity_key, 0, one_hour_ago)
            pipe.zadd(velocity_key, {payload.transaction_id: current_time})
            pipe.zcard(velocity_key)
            pipe.expire(velocity_key, 3600 * 2)

            # 2. Average Amount (24h)
            pipe.zremrangebyscore(amounts_key, 0, twenty_four_hours_ago)
            member_value = f"{amount}:{payload.transaction_id}"
            pipe.zadd(amounts_key, {member_value: current_time})
            pipe.zrange(amounts_key, 0, -1)
            pipe.expire(amounts_key, 86400 * 2)

            # 3. Location Change
            pipe.get(location_key)
            pipe.set(location_key, payload.location if payload.location else "UNKNOWN")

            # Execute
            results = await pipe.execute()

        velocity_count = results[2]
        
        amounts_members = results[6]
        total_amount = sum(float(m.split(":")[0]) for m in amounts_members)
        avg_amount = total_amount / len(amounts_members) if amounts_members else amount

        last_location = results[8]
        current_location = payload.location if payload.location else "UNKNOWN"
        country_change = bool(last_location and last_location != current_location)

        return FeaturesPayload(
            velocity_1h=velocity_count,
            average_amount_24h=round(avg_amount, 2),
            country_change=country_change
        )
