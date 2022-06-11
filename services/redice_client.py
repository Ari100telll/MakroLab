import json
from enum import Enum

from redis import Redis

from configs.config import settings


class RedisBody:
    def get_json(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
        )


class RedisInfoBody(RedisBody):
    def __init__(self, status, last_modify=None, timestamp=None):
        self.status = status
        self.timestamp = timestamp
        self.last_modify = last_modify


class RedisErrorBody(RedisBody):
    def __init__(self, status, error):
        self.status = status
        self.error = error


class RedisTryBody(RedisBody):
    def __init__(self, timestamp=None, url=None):
        self.url = url
        self.timestamp = timestamp


class RedisDAO:
    class DatasetStatus(Enum):
        IN_PROGRESS = 'in_progress'
        COMPLETED = 'completed'
        FAILED = 'failed'

        @classmethod
        def list(cls):
            return list(map(lambda c: c.value, cls))

    DATASET_NAME = settings.REDIC_DATASET_NAME
    UPLOADS_NAME = settings.REDIC_UPLOADS_NAME
    UPLOADS_NAME_ID = settings.REDIC_UPLOADS_NAME_ID

    def __init__(self, redis: Redis):
        self._redis_client = redis
        super(RedisDAO, self).__init__()

    def get_info(self, data_url):
        result = self._redis_client.hget(self.DATASET_NAME, data_url)
        if not result:
            return None
        result = json.loads(result)
        if result.get("status") == self.DatasetStatus.FAILED.value:
            return RedisErrorBody(**result)

        elif result.get("status") in [
            self.DatasetStatus.COMPLETED.value,
            self.DatasetStatus.IN_PROGRESS.value
        ]:
            return RedisInfoBody(**result)

    def save_info(self, data_url, body: RedisBody):
        self._redis_client.hset(self.DATASET_NAME, data_url, body.get_json())

    def delete_info(self, data_url):
        self._redis_client.hdel(self.DATASET_NAME, data_url)

    def save_try(self, data_url, body: RedisTryBody):
        id_ = self._redis_client.incr(self.UPLOADS_NAME_ID)
        body.url = data_url
        self._redis_client.hset(self.UPLOADS_NAME, id_, body.get_json())
