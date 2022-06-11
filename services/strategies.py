import datetime
from typing import List, Dict
import tabulate
from services.managers import DataManager
from services.redice_client import RedisDAO, RedisTryBody, RedisInfoBody, RedisErrorBody
from utils.errors import OperationError


class DataOperationStrategy:
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager

    def operate(self):
        raise NotImplementedError


class ConsoleDataOperationStrategy(DataOperationStrategy):
    def operate(self):
        chunk = 1
        for chunk_of_data in self.data_manager.get_chunks():
            chunk_printable = self.operate_chunk(chunk_of_data)
            print(f"CHUNK {chunk}")
            print(chunk_printable)
            chunk += 1

    @staticmethod
    def operate_chunk(chunk_of_data: List[Dict]) -> str:
        header = list(chunk_of_data[0].keys())
        rows = [row.values() for row in chunk_of_data]
        return tabulate.tabulate(rows, header)


class CloudDataOperationStrategy(DataOperationStrategy):
    def __init__(self, redis_dao: RedisDAO, *args, **kwargs):
        self.redis_dao = redis_dao
        super(CloudDataOperationStrategy, self).__init__(*args, **kwargs)

    def operate(self):
        is_operation_needed = self.check_is_operation_needed()
        self.save_try()
        if not is_operation_needed:
            raise OperationError("This URL already operated.")
        self.operate_chunks()

    def operate_chunks(self):
        self.save_start_status()
        try:
            for chunk in self.data_manager.get_chunks():
                self.send_chunk(chunk)
        except Exception as e:
            self.save_error(e)
            raise OperationError("Error with saving data to cloud")

        self.save_completed_status()
        return False

    def send_chunk(self, chunk):
        ...

    def check_is_operation_needed(self):
        info = self.redis_dao.get_info(self.data_manager.url)
        if isinstance(info, RedisInfoBody):
            if info.status == self.redis_dao.DatasetStatus.COMPLETED.value:
                return self.can_be_updated(info)
            if info.status == self.redis_dao.DatasetStatus.IN_PROGRESS.value:
                return False
        if isinstance(info, RedisErrorBody):
            return True
        return True

    def can_be_updated(self, info: RedisInfoBody):
        new_last_modify = self.data_manager.get_last_modify()
        return new_last_modify != info.last_modify

    def save_start_status(self):
        last_modify = self.data_manager.get_last_modify()
        redis_info_body = RedisInfoBody(
            status=RedisDAO.DatasetStatus.IN_PROGRESS.value,
            timestamp=str(datetime.datetime.utcnow()),
            last_modify=last_modify,
        )
        self.redis_dao.save_info(
            data_url=self.data_manager.url,
            body=redis_info_body
        )

    def save_completed_status(self):
        info = self.redis_dao.get_info(self.data_manager.url)
        if not isinstance(info, RedisInfoBody):
            raise OperationError("Something went wrong")
        info.status = self.redis_dao.DatasetStatus.COMPLETED.value
        self.redis_dao.save_info(
            data_url=self.data_manager.url,
            body=info
        )

    def save_error(self, error):
        redis_error_body = RedisErrorBody(
            status=self.redis_dao.DatasetStatus.FAILED.value,
            error=error
        )
        self.redis_dao.save_info(
            data_url=self.data_manager.url,
            body=redis_error_body
        )

    def save_try(self):
        info = RedisTryBody(timestamp=str(datetime.datetime.utcnow()))
        self.redis_dao.save_try(self.data_manager.url, info)
