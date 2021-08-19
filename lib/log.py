import sys
import time
from pathlib import Path

from loguru import logger

import lib.settings
import unicodedata


def normalize(string):
    """Strip unicode characters"""
    return unicodedata.normalize("NFKD", string).encode("ascii", "ignore")

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
        logger.trace(msg)
        return result

    return log_it


def set_up_logging(settings: lib.settings.Settings, debug=False):
    logger.remove()  # remove the default logger output

    log_file_name = Path(f"{settings.log_dir}/{settings.log_file_name}").absolute()
    if not log_file_name.parent.exists():
        log_file_name.parent.mkdir()
    if not log_file_name.parent.exists():
        print(
            f"Log file parent directory: {log_file_name.parent} does not exist and cannot be created."
        )
        sys.exit(1)

    logger.add(
        log_file_name,
        rotation=settings.log_rotation,
        retention="1 week",
        compression="zip",
        level="TRACE",
    )  # always log to a file
    logger.add(sys.stderr, level="WARNING")
    if debug:
        logger.add(sys.stderr, level="DEBUG")
        logger.debug("DEBUG Logging started.")
