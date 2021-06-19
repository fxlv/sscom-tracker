from pushover import Client
from loguru import logger
from lib.log import func_log


class PushMessage:
    """Push message class."""

    def __init__(self, message, message_type=None):
        """Construct a message based on arguments."""
        self.message = message
        # use message type in title, or use word 'match' if type not provided
        self.message_type = message_type if message_type else "match"
        self.title = f"New {message_type} found"


class Push:
    """Send push notifications using Pushover service."""

    def __init__(self, settings):
        """Construct the object based on settings."""
        self.client = Client(
            settings["pushover_user_key"], api_token=settings["pushover_api_token"]
        )
        if settings["pushover-enabled"]:
            self.enabled = True
        else:
            self.enabled = False

    @func_log
    def send_pushover_message(self, message: PushMessage):
        """Send a message, if push is enabled."""
        if self.enabled:
            logger.debug("Sending push message")
            self.client.send_message(message=message.message, title=message.title)
        else:
            print(
                "Push messages not enabled! [Title: {} Message: {}]".format(
                    message.title, message.message
                )
            )
