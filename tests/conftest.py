# fixture taken from https://loguru.readthedocs.io/en/stable/resources/migration.html#making-things-work-with-pytest-and-caplog
# whith a small addition to remove all handlers instead of just one

import logging
import pytest
from _pytest.logging import caplog as _caplog
from loguru import logger


@pytest.fixture
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    logger.add(PropogateHandler(), format="{message} {extra}")
    yield _caplog
    # iterate over and remove all registered handlers
    for handler_id in logger._core.handlers:
        logger.remove(handler_id)
