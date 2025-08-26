import os

from dotenv import load_dotenv

load_dotenv()

EXPLORER_RPC_URL = os.getenv("EXPLORER_RPC_URL")
EXPLORER_RPC_PASSWORD = os.getenv("EXPLORER_RPC_PASSWORD")

USE_SQLITE_DB = os.getenv("USE_SQLITE_DB", True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

LN_HISTORY_DATABASE_CONNECTION_STRING=os.getenv("LN_HISTORY_DATABASE_CONNECTION_STRING")

FILE_PATH=os.getenv("FILE_PATH")