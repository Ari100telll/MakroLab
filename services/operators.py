from services.strategies import DataOperationStrategy
from utils.errors import OperationError


class DataOperator:
    def __init__(self, strategy: DataOperationStrategy = None, ):
        self.strategy = strategy
        super(DataOperator, self).__init__()

    def set_strategy(self, strategy: DataOperationStrategy):
        self.strategy = strategy

    def operate_data(self):
        if not self.strategy:
            raise OperationError("strategy not set.")

        self.strategy.operate()
