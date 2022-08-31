from configs.config import settings
from services.event_hub import EventHubManager


def configure_dependencies(binder):
    from injector import singleton
    from redis.client import Redis
    from services.redice_client import RedisDAO

    redis = Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=0,
        password=settings.REDIS_PASSWORD,
        ssl=settings.REDIC_SSL,
        decode_responses=True,
    )

    binder.bind(Redis, redis, singleton)
    binder.bind(RedisDAO, RedisDAO(redis), singleton)
    binder.bind(EventHubManager, EventHubManager(), singleton)
