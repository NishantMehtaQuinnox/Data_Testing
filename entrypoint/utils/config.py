# type: ignore
import os
from dotenv import load_dotenv

load_dotenv()

for k, v in os.environ.items():
    print(f"{k}: {v}")


class ENV:
    ENV_NAME = os.environ.get("ENV_NAME")  # can be LOCAL, STG_PROD, CLIENT


class AWS_CONFIGS:
    AWS_ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")
    ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566/")
    QUEUE_ROLE_ARN = os.environ.get("ROLE_ARN")
    REGION = os.environ.get("REGION")


class EVENTS_SQS:
    ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566/")
    QUEUE_URL = os.environ.get("EVENT_QUEUE_URL")
    QUEUE_ROLE_SESSION = os.environ.get("EVENT_QUEUE_ROLE_SESSION",
                                        "trigger_event_handler_sqs_session")
    
class TEAMS_EVENTS_SQS:
    ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566/")
    QUEUE_URL = os.environ.get("TEAMS_EVENT_QUEUE_URL")
    QUEUE_ROLE_SESSION = os.environ.get("TEAMS_EVENT_QUEUE_ROLE_SESSION",
                                        "trigger_event_handler_sqs_session")

class INTERACTION_SQS:

    ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566/")
    QUEUE_URL = os.environ.get("INTERACTION_QUEUE_URL")
    QUEUE_ROLE_SESSION = os.environ.get(
        "INTERACTION_QUEUE_ROLE_SESSION",
        "trigger_interaction_handler_sqs_session")

class TEAMS_INTERACTION_SQS:

    ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566/")
    QUEUE_URL = os.environ.get("TEAMS_INTERACTION_QUEUE_URL")
    QUEUE_ROLE_SESSION = os.environ.get(
        "TEAMS_INTERACTION_QUEUE_ROLE_SESSION",
        "trigger_interaction_handler_sqs_session")

class SLACK_APP_CONFIGS:
    BOT_TOKEN = os.environ.get("BOT_TOKEN") if "Bearer " in os.environ.get(
        "BOT_TOKEN") else f"Bearer {os.environ.get('BOT_TOKEN')}"
    USER_TOKEN = os.environ.get("USER_TOKEN") if "Bearer " in os.environ.get(
        "USER_TOKEN") else f"Bearer {os.environ.get('USER_TOKEN')}"
    SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")  # verification token
    SLACK_SIGNING_SECRET = os.environ.get(
        "SLACK_SIGNING_SECRET")  # signing secret

class TEAMS_BOT_CONFIG:
    """ Bot Configuration """
    APP_ID = os.environ.get("MICROSOFT_APP_ID")
    APP_PASSWORD = os.environ.get("MICROSOFT_APP_PASSWORD")
