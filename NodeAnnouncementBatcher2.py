from psycopg2.extras import execute_values
from datetime import datetime
from typing import List, Tuple, Set
import threading

class NodeAnnouncementBatcher:
    def __init__(self, conn, cache, logger, batch_size=100):
        self.conn = conn
        self.cache = cache
        self.logger = logger
        self.batch_size = batch_size
        self.lock = threading.Lock()
        self.node_items: List[Tuple[bytes, datetime, bytes]] = []
        self.node_ids_seen: Set[bytes] = set()

    def add(self, node_id: bytes, from_timestamp: datetime, gossip_id: bytes):
        with self.lock:
            if self.cache.has_node(node_id) or node_id in self.node_ids_seen:
                return
            self.node_items.append((node_id, from_timestamp, gossip_id))
            self.node_ids_seen.add(node_id)

            if len(self.node_items) >= self.batch_size:
                self.flush()

    def flush(self):
        with self.lock:
            if not self.node_items:
                return

            try:
                with self.conn:
                    with self.conn.cursor() as cur:
                        execute_values(cur, """
                            INSERT INTO nodes_raw_gossip (gossip_id, node_id)
                            VALUES %s
                            ON CONFLICT DO NOTHING;
                        """, [(gossip_id, node_id) for node_id, _, gossip_id in self.node_items])

                        execute_values(cur, """
                            INSERT INTO nodes (node_id, validity)
                            VALUES (%s, tstzrange(%s, NULL))
                            ON CONFLICT DO NOTHING;
                        """, [
                            (node_id, from_timestamp)
                            for node_id, from_timestamp, _ in self.node_items
                        ])

                        for node_id, _, _ in self.node_items:
                            self.cache.cache_node(node_id)

                        self.logger.info(f"Inserted {len(self.node_items)} new node_ids")

            except Exception as e:
                self.logger.error(f"Failed node batch insert: {e}")
            finally:
                self.node_items.clear()
                self.node_ids_seen.clear()
