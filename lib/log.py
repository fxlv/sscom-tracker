import logging
import time


def func_log(function_name):
    """Decorator for logging and timing function execution."""

    def log_it(*args, **kwargs):
        """Log function and its args, execute the function and return the result."""
        t_start = time.time()
        result = function_name(*args, **kwargs)
        t_end = time.time() - t_start
        msg = "Function call: {}".format(function_name.__name__)
        if args:
            msg += " with args: {}".format(args)
        if kwargs:
            msg += " with kwargs {}".format(args, kwargs)
        msg += " executed in: {:5.5f} sec".format(t_end)
        logging.debug(msg)
        return result

    return log_it


def set_up_logging(debug=False, log_file="debug.log"):
    log_format = "%(asctime)s %(levelname)s %(name)s " \
                 "%(filename)s %(lineno)d >> %(message)s"
    if debug:
        logging.basicConfig(level=logging.DEBUG,
                            filename=log_file,
                            format=log_format)
        logging.debug('Logging started.')
    else:
        logging.basicConfig(level=logging.INFO)