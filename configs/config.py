from dynaconf import Dynaconf

settings = Dynaconf(
    environments=True,
    envvar_prefix="DYNACONF",
    settings_files=['configs/settings.toml', 'configs/.secrets.toml'],
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.


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

