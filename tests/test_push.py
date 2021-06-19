import lib.push
import pytest
import lib.log

lib.log.set_up_logging(debug=True, log_file="tests_debug.log")


@pytest.fixture
def push_settings_enabled():
    settings = {
        "pushover-enabled": True,
        "pushover_user_key": "dummy_key",
        "pushover_api_token": "dummy_api_token",
    }
    return settings


@pytest.fixture
def push_settings_disabled():
    settings = {
        "pushover-enabled": False,
        "pushover_user_key": "dummy_key",
        "pushover_api_token": "dummy_api_token",
    }
    return settings


def test_push_enabled(push_settings_enabled):
    p = lib.push.Push(push_settings_enabled)
    assert isinstance(p, lib.push.Push)
    assert p.enabled


def test_push_disabled(push_settings_disabled):
    p = lib.push.Push(push_settings_disabled)
    assert isinstance(p, lib.push.Push)
    assert p.enabled is False


def test_send_pushover_message_disabled(push_settings_disabled, capsys):
    p = lib.push.Push(push_settings_disabled)
    m = lib.push.PushMessage("Nice dog", "Dog")
    p.send_pushover_message(m)
    out, err = capsys.readouterr()
    assert "Push messages not enabled! [Title: New Dog found Message: Nice dog]" in out
