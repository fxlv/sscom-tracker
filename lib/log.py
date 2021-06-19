import logging
import sys
import time
from loguru import logger


def func_log(function_name):
    """Decorator for logging and timing function execution."""

    def log_it(*args, **kwargs):
        """Log function and its args, execute the function and return the result."""
        t_start = time.time()
        result = function_name(*args, **kwargs)
        t_end = time.time() - t_start
        msg = f"Function call: {function_name.__name__}"
        if args:
            msg += f" with args: {args}"
        if kwargs:
            msg += f" with kwargs {args}"
        msg += f" executed in: {t_end:5.5f} sec"
        logger.debug(msg)
        return result

    return log_it


def set_up_logging(debug=False, log_file_name="tracker.log"):
    logger.remove()  # remove the default logger output
    logger.add(
        "tracker.log", rotation="10 MB", retention="1 week", compression="zip"
    )  # always log to a file
    logger.add(sys.stderr, level="WARNING")
    if debug:
        logger.add(sys.stdout, level="DEBUG")
        logger.debug("DEBUG Logging started.")
