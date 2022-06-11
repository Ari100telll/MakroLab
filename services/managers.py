import requests

from configs.config import settings


class DataManager:
    def __init__(self, url: str, limit: int = settings.DEFAULT_CHUNK_SIZE):
        self.url = url
        self.offset = 0
        if limit < 1:
            raise ValueError("limit cannot be lesser than 1")
        self.limit = limit
        super(DataManager, self).__init__()

    def get_chunks(self):
        result = True
        while result:
            request_url = f"{self.url}?$limit={self.limit}&$offset={self.offset}"
            req = requests.get(url=request_url)
            if req.status_code != 200:
                raise ConnectionError(
                    f"Cannot fetch data from url - {self.url} with $limit={self.limit},"
                    f" $offset={self.offset}. \n Error: {req.text}"
                )
            result = req.json()
            if result:
                yield result

            self.offset += self.limit
        self.offset = 0

    def get_last_modify(self):
        request_url = f"{self.url}?$limit=1&$offset=0"
        req = requests.get(url=request_url)
        if req.status_code != 200:
            raise ConnectionError(
                f"Cannot fetch data from url - {self.url}. \n Error: {req.text}"
            )
        return req.headers.get("Last-Modified")
