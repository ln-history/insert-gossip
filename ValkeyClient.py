import hashlib
import json

from lnhistoryclient.model.types import GossipCache
from valkey import Valkey

from datetime import datetime
from typing import Optional, List

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

    def cache_gossip_batch(self, gossip_ids: List[bytes], batch_size: int = 500) -> int:
        """
        Batch-insert gossip_ids into Valkey cache using pipelines.
        Returns the number of successfully queued keys.
        """
        total_queued = 0
        for i in range(0, len(gossip_ids), batch_size):
            batch = gossip_ids[i:i + batch_size]
            pipe = self.client.pipeline()
            for gossip_id in batch:
                key = f"gossip:{gossip_id.hex()}"
                pipe.set(key, "1")
            pipe.execute()
            total_queued += len(batch)
        return total_queued

    def has_node(self, node_id: bytes) -> bool:
        return self.client.exists(f"node:{node_id.hex()}") == 1

    def cache_node(self, node_id: bytes) -> None:
        self.client.set(f"node:{node_id.hex()}", "1")

    def invalidate_node(self, node_id: bytes) -> None:
        self.client.delete(f"node:{node_id.hex()}")

    def has_channel(self, scid: str) -> bool:
        return self.client.exists(f"scid:{scid}") == 1

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

    def delete_gossip_ids(self, gossip_ids: list[bytes], batch_size: int = 500) -> int:
        """
        Delete gossip_ids from the cache in batches.
        Returns the total number of keys successfully deleted.
        """
        total_deleted = 0
        keys = [f"gossip:{gossip_id.hex()}" for gossip_id in gossip_ids]

        for i in range(0, len(keys), batch_size):
            batch = keys[i:i + batch_size]
            deleted = self.client.delete(*batch)
            total_deleted += deleted

        return total_deleted