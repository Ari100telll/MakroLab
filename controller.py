from http.client import NO_CONTENT, BAD_REQUEST

import flask
from flask import Blueprint, request

from configs.config import settings
from services.managers import DataManager
from services.operators import DataOperator
from services.strategies import ConsoleDataOperationStrategy, CloudDataOperationStrategy
from utils.errors import OperationError

process_bp = Blueprint("process_blueprint", __name__, url_prefix="")


@process_bp.route("/", methods=["POST"])
async def process():
    url = request.values.get("url")
    data_manager = DataManager(url=url)
    data_operator = DataOperator()
    PROCESS_TYPE = settings.PROCESS_TYPE

    if PROCESS_TYPE == settings.CONSOLE_PROCESS:
        data_operator.set_strategy(
            ConsoleDataOperationStrategy(data_manager)
        )
    else:
        data_operator.set_strategy(
            CloudDataOperationStrategy(data_manager=data_manager)
        )
    try:
        await data_operator.operate_data()
    except OperationError as e:
        return flask.Response(status=BAD_REQUEST, response=str(e))
    return flask.Response(status=NO_CONTENT)
