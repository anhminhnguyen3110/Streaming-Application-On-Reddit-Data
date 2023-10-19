from os import environ
from dotenv import load_dotenv

load_dotenv()


class CoreSetting:
    ACCESS_TOKEN = str(environ.get("TWITTER_ACCESS_TOKEN"))
    ACCESS_SECRET = str(environ.get("TWITTER_ACCESS_SECRET"))
    CONSUMER_KEY = str(environ.get("TWITTER_CONSUMER_KEY"))
    CONSUMER_SECRET = str(environ.get("TWITTER_CONSUMER_SECRET"))
    KAFKA_BOOTSTRAP_SERVER = str(environ.get("KAFKA_BOOSTRAP_SERVER"))
    # REDDIT
    CLIENT_ID = str(environ.get("REDDIT_CLIENT_ID"))
    CLIENT_SECRET = str(environ.get("REDDIT_CLIENT_SECRET"))
    USER_AGENT = str(environ.get("REDDIT_USER_AGENT"))
    USERNAME = str(environ.get("REDDIT_USERNAME"))
    PASSWORD = str(environ.get("REDDIT_PASSWORD"))
    PARTITION_NUMBER_PER_TOPIC = 10
    REPLICA_FACTOR = 2


Setting = CoreSetting()
