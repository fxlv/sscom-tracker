from pushover import Client


class Push:
    """Send push notifications using Pushover service."""
    def __init__(self, settings):
        self.client = Client(settings["pushover_user_key"],
                             api_token=settings["pushover_api_token"])
        if settings["pushover-enabled"] == True:
            self.enabled = True
        else:
            self.enabled = False

    def send_pushover_message(self, message):
        if self.enabled:
            print("Sending push message")
            self.client.send_message(
                message, title="New apartment found by sscom-tracker")
        else:
            print("Push messages not enabled!")
