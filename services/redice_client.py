import json
from enum import Enum

from redis import Redis

from configs.config import settings


class RedisInfoBody:
    def __init__(self, status, last_modify):
        self.status = status
        self.last_modify = last_modify

    def get_json(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=4
        )


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

    def __init__(self, redis: Redis):
        self._redis_client = redis
        super(RedisDAO, self).__init__()

    def get_info(self, data_url):
        result = self._redis_client.hget(self.DATASET_NAME, data_url)
        if result:
            result = json.loads(result)
        return result

    def save_info(self, data_url, body):
        self._redis_client.hset(self.DATASET_NAME, data_url, json.dumps(body))

    def delete_info(self, data_url):
        self._redis_client.hdel(self.DATASET_NAME, data_url)

    def save_try(self, data_url, body):
        self._redis_client.hset(self.UPLOADS_NAME, data_url, json.dumps(body))
