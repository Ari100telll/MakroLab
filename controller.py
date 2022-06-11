from http.client import NO_CONTENT, BAD_REQUEST

import flask
from flask import Blueprint, request
from injector import Injector

from configs.config import settings, configure_dependencies
from services.managers import DataManager
from services.operators import DataOperator
from services.redice_client import RedisDAO
from services.strategies import ConsoleDataOperationStrategy, CloudDataOperationStrategy
from utils.errors import OperationError

process_bp = Blueprint("process_blueprint", __name__, url_prefix="")


@process_bp.route("/", methods=["POST"])
def process():
    url = request.values.get("url")
    data_manager = DataManager(url=url)
    data_operator = DataOperator()
    PROCESS_TYPE = settings.PROCESS_TYPE
    if PROCESS_TYPE == settings.CONSOLE_PROCESS:
        data_operator.set_strategy(
            ConsoleDataOperationStrategy(data_manager)
        )
    else:
        injector = Injector([configure_dependencies])
        redis_dao = injector.get(RedisDAO)
        data_operator.set_strategy(
            CloudDataOperationStrategy(redis_dao=redis_dao, data_manager=data_manager)
        )
    try:
        data_operator.operate_data()
    except OperationError as e:
        return flask.Response(status=BAD_REQUEST, response=str(e))
    return flask.Response(status=NO_CONTENT)
