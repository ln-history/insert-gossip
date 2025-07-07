import os

from dotenv import load_dotenv

load_dotenv()


VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", None)

EXPLORER_RPC_URL = os.getenv("EXPLORER_RPC_URL")
EXPLORER_RPC_PASSWORD = os.getenv("EXPLORER_RPC_PASSWORD")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")