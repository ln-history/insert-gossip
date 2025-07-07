import hashlib
import json

from lnhistoryclient.model.types import GossipCache
from valkey import Valkey

from datetime import datetime
from typing import Optional

from config import VALKEY_HOST, VALKEY_PASSWORD, VALKEY_PORT


class ValkeyCache:
    def __init__(self) -> None:
        self.client = Valkey(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD, db=0)

    @staticmethod
    def hash_raw_hex(raw_hex: str) -> str:
        return hashlib.sha256(raw_hex.encode()).hexdigest()
    
    def has_gossip(self, gossip_id: bytes) -> bool:
        return self.client.exists(f"gossip:{gossip_id.hex()}") == 1

    def cache_gossip(self, gossip_id: bytes) -> None:
        self.client.set(f"gossip:{gossip_id.hex()}", "1")

    def has_node(self, node_id: bytes) -> bool:
        return self.client.exists(f"node:{node_id.hex()}") == 1

    def cache_node(self, node_id: bytes) -> None:
        self.client.set(f"node:{node_id.hex()}", "1")

    def invalidate_node(self, node_id: bytes) -> None:
        self.client.delete(f"node:{node_id.hex()}")

    def get_node_validity(self, node_id: bytes) -> Optional[tuple[datetime, datetime]]:
        raw = self.client.get(f"node:{node_id.hex()}")
        if not raw:
            return None
        try:
            data = json.loads(raw)
            v = data.get("validity")
            if v and isinstance(v, list) and len(v) == 2:
                return (
                    datetime.fromisoformat(v[0]),
                    datetime.fromisoformat(v[1])
                )
        except Exception:
            return None

    def set_node_validity(self, node_id: bytes, from_ts: datetime, to_ts: datetime) -> None:
        value = json.dumps({"validity": [from_ts.isoformat(), to_ts.isoformat()]})
        self.client.set(f"node:{node_id.hex()}", value)

    def has_gossip(self, gossip_id: bytes) -> bool:
        return self.client.exists(f"gossip:{gossip_id.hex()}") == 1

    def cache_gossip(self, gossip_id: bytes) -> None:
        self.client.set(f"gossip:{gossip_id.hex()}", "1")

