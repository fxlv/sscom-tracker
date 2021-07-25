from loguru import logger
import pytest
import lib.log
import datetime
import os

now = datetime.datetime.now()

test_string = f"Test {now}"
log_file_name = "tests_debug1.log"


def setup_module():
    if os.path.exists(log_file_name):
        print(f"Deleting old log file {log_file_name}")
        os.unlink(log_file_name)


@pytest.fixture
def set_up_logging():
    lib.log.set_up_logging(debug=True, log_file_name=log_file_name, rotation="15KB")


def test_before_set_up_logging(caplog):
    """By default all logs should be written to stderr."""
    logger.debug(test_string)
    assert test_string in caplog.text


def test_after_set_up_logging(caplog, set_up_logging):
    """Once logging is configured logging output changes.

    Everything should be logged to file,
    Warnings and above should be also logged to stderr.
    """
    logger.debug(test_string)
    # now, anything less than warning should go to file and not to stderr
    assert test_string not in caplog.text


def find_string_in_logs(string):
    line_found = False
    with open(log_file_name) as logfile:
        for line in logfile:
            if string in line:
                line_found = True
                continue
    return line_found


def test_log_line_is_logged_to_file():
    # let's check that file
    # test string should be found in the file
    assert find_string_in_logs(test_string) is True


def test_warning_is_always_logged_to_stderr(caplog):
    # now, let's make sure anything >= warning is stil logged to stderr
    logger.warning(test_string)
    assert test_string in caplog.text


@lib.log.func_log
def dummy_function(*args, **kvargs):
    logger.debug("Dummy function executing")


def test_func_log_without_args(set_up_logging):
    dummy_function()
    assert find_string_in_logs("dummy_function executed in:")


def test_func_log_with_args(set_up_logging):
    dummy_function("val1")
    assert find_string_in_logs("dummy_function with args: ('val1',) executed in")


def test_func_log_with_kvargs(set_up_logging):
    dummy_function(argument="val1")
    assert find_string_in_logs("dummy_function with kwargs () executed in:")


def get_log_size():
    return os.stat(log_file_name).st_size


def test_log_rotation_works_as_expected(set_up_logging):
    print(f"Log size now is: {get_log_size()}")
    # at this point the log should be below 300 bytes
    assert get_log_size() < 3000
    # write more logs and expect it gets rotated
    # rotation should happen at 15 KB, so write until 14,5
    while get_log_size() < 14500:
        logger.debug("Filling up logs")
    print(f"Log size now is: {get_log_size()}")
    assert get_log_size() >= 14500
    for i in range(1, 10):
        logger.debug("Filling up logs some more")
    # at this point rotation should have happened
    print(f"Log size now is: {get_log_size()}")
    assert get_log_size() < 1000
