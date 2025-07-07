from datetime import datetime, timedelta, timezone
from psycopg2.extras import execute_values

from ValkeyClient import ValkeyCache

class NodeAnnouncementBatcher:
    def __init__(self, conn, cache: ValkeyCache, batch_size: int = 1000):
        self.conn = conn
        self.cache = cache
        self.batch_size = batch_size

        self.raw_gossip_batch = []
        self.nodes_raw_batch = []
        self.nodes_insert_batch = []
        self.nodes_update_batch = []

        self.gossip_ids_to_cache = []
        self.node_validity_cache_updates = {}

    def add(self, raw_bytes: bytes, node_id: bytes, timestamp_unix: int, gossip_id: bytes):
        timestamp = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

        self.raw_gossip_batch.append((gossip_id, 257, timestamp, raw_bytes))
        self.nodes_raw_batch.append((gossip_id, node_id))

        validity = self.cache.get_node_validity(node_id)

        if not validity:
            self.conn.rollback()  # In case of partial transactions
            with self.conn.cursor() as cur:
                cur.execute("SELECT lower(validity), upper(validity) FROM nodes WHERE node_id = %s", (node_id,))
                result = cur.fetchone()
            if result:
                validity = result

        if not validity:
            # New node
            valid_from = timestamp
            valid_to = timestamp + timedelta(seconds=1)
            self.nodes_insert_batch.append((node_id, valid_from, valid_to))
            self.node_validity_cache_updates[node_id] = (valid_from, valid_to)
        else:
            lower, upper = validity
            if timestamp < lower or timestamp > upper:
                new_lower = min(lower, timestamp)
                new_upper = max(upper, timestamp + timedelta(seconds=1))
                self.nodes_update_batch.append((new_lower, new_upper, node_id))
                self.node_validity_cache_updates[node_id] = (new_lower, new_upper)

        self.gossip_ids_to_cache.append((gossip_id, node_id))

        if len(self.raw_gossip_batch) >= self.batch_size:
            self.flush()

    def flush(self):
        with self.conn:
            with self.conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
                    VALUES %s ON CONFLICT DO NOTHING
                """, self.raw_gossip_batch)

                execute_values(cur, """
                    INSERT INTO nodes_raw_gossip (gossip_id, node_id)
                    VALUES %s ON CONFLICT DO NOTHING
                """, self.nodes_raw_batch)

                if self.nodes_insert_batch:
                    execute_values(
                    cur,
                    """
                    INSERT INTO nodes (node_id, validity)
                    VALUES %s ON CONFLICT DO NOTHING
                    """,
                    self.nodes_insert_batch,
                    template="(%s, tstzrange(%s, %s))"
                )

                for new_lower, new_upper, node_id in self.nodes_update_batch:
                    cur.execute("""
                        UPDATE nodes
                        SET validity = tstzrange(%s, %s)
                        WHERE node_id = %s
                    """, (new_lower, new_upper, node_id))

        # Cache after commit
        for gossip_id, node_id in self.gossip_ids_to_cache:
            self.cache.cache_gossip(gossip_id)

        for node_id, (vfrom, vto) in self.node_validity_cache_updates.items():
            self.cache.set_node_validity(node_id, vfrom, vto)

        # Reset batches
        self.raw_gossip_batch.clear()
        self.nodes_raw_batch.clear()
        self.nodes_insert_batch.clear()
        self.nodes_update_batch.clear()
        self.gossip_ids_to_cache.clear()
        self.node_validity_cache_updates.clear()

    def close(self):
        if self.raw_gossip_batch:
            self.flush()
