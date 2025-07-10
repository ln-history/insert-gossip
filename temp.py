from valkey import Valkey

from config import VALKEY_HOST, VALKEY_PASSWORD, VALKEY_PORT

valkey = Valkey(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD)

# Use SCAN to avoid blocking the server
cursor = 0
deleted = 0

while True:
    cursor, keys = valkey.scan(cursor=cursor, match="node:*", count=1000)
    if keys:
        valkey.delete(*keys)
        deleted += len(keys)
    if cursor == 0:
        break

print(f"Deleted {deleted} node:* keys from Valkey.")
