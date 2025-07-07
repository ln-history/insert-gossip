import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVER_IP_ADDRESS = os.getenv("KAFKA_SERVER_IP_ADDRESS", "localhost")
KAFKA_SERVER_PORT = int(os.getenv("KAFKA_SERVER_PORT", 9092))

KAFKA_TOPIC_TO_PRODUCE = os.getenv("KAFKA_TOPIC_TO_PRODUCE", "gossip")

VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", None)

COUCHDB_URL = os.getenv("COUCHDB_URL")
COUCHDB_USER = os.getenv("COUCHDB_USER")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD")

EXPLORER_RPC_URL = os.getenv("EXPLORER_RPC_URL")
PROXIES = {
    "http": os.getenv("HTTP_PROXY"),
    "https": os.getenv("HTTPS_PROXY"),
}

NODE_ID = os.getenv("NODE_ID")