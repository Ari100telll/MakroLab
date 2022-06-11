from typing import List, Dict
import tabulate
from services.managers import DataManager
from services.redice_client import RedisDAO
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

    def get_status(self, data_url):
        result = self.redis_dao.get_info(data_url)
        if result:
            status = result.get("status")
            return status

    def check_is_operation_needed(self):
        status = self.get_status(self.data_manager.url)
        if not status or status == self.redis_dao.DatasetStatus.FAILED.value:
            return True
        if status == self.redis_dao.DatasetStatus.COMPLETED.value:
            return self.can_be_updated()
        return False

    def can_be_updated(self):
        old_last_modify = self.data_manager.get_last_modify()
        info = self.redis_dao.get_info(self.data_manager.url)
        new_last_modify = info.get("last_modify")
        return old_last_modify != new_last_modify

    def save_start_status(self):
        last_modify = self.data_manager.get_last_modify()
        body = {
            "status": RedisDAO.DatasetStatus.IN_PROGRESS.value,
            "last_modify": last_modify
        }
        self.redis_dao.save_info(
            data_url=self.data_manager.url,
            body=body
        )

    def save_completed_status(self):
        info = self.redis_dao.get_info(self.data_manager.url)
        last_modify = info.get("last_modify")
        body = {
            "status": self.redis_dao.DatasetStatus.COMPLETED.value,
            "last_modify": last_modify,
        }
        self.redis_dao.save_info(
            data_url=self.data_manager.url,
            body=body
        )

    def save_error(self, error):
        body = {
            "status": self.redis_dao.DatasetStatus.FAILED.value,
            "error": error
        }
        self.redis_dao.save_info(
            data_url=self.data_manager.url,
            body=body
        )

    def save_try(self):
        info = self.redis_dao.get_info(self.data_manager.url)

