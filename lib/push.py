from pushover import Client


class PushMessage:
    """Push message class."""

    def __init__(self, message, message_type=None):
        """Construct a message based on arguments."""
        self.message = message
        # use message type in title, or use word 'match' if type not provided
        self.message_type = message_type if message_type else "match"
        self.title = "New {} found".format(message_type)


class Push:
    """Send push notifications using Pushover service."""

    def __init__(self, settings):
        """Construct the object based on settings."""
        self.client = Client(settings["pushover_user_key"],
                             api_token=settings["pushover_api_token"])
        if settings["pushover-enabled"]:
            self.enabled = True
        else:
            self.enabled = False

    def send_pushover_message(self, message: PushMessage):
        """Send a message, if push is enabled."""
        if self.enabled:
            print("Sending push message")
            self.client.send_message(
                message=message.message, title=message.title)
        else:
            print("Push messages not enabled! [Title: {} Message: {}]".format(message.title, message.message))
