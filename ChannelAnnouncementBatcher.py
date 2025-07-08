from collections import namedtuple
from psycopg2.extras import execute_values
from datetime import datetime
from typing import List
from datetime import timezone
import logging
import hashlib
import threading

from lnhistoryclient.parser.parser import parse_channel_announcement
from lnhistoryclient.parser.common import strip_known_message_type

from BlockchainRequester import get_block_by_block_height, get_amount_sat_by_tx_idx_and_output_idx

INVALID_CHANNEL_ANNOUNCEMENTS_FILE = "invalid_channel_announcements-multi_threaded.gsp"

def write_invalid_gossip(raw_bytes: bytes, logger: logging.Logger) -> None:
    try:
        with open(INVALID_CHANNEL_ANNOUNCEMENTS_FILE, "ab") as f:
            f.write(raw_bytes + b"\n")
        logger.info("Invalid gossip written to file for later inspection.")
    except Exception as e:
        logger.error(f"Failed to write invalid gossip to file: {e}")

def split_scid(scid, logger: logging.Logger) -> tuple[int, int, int]:
    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid}. - Skipping further handling")
        return

ChannelRecord = namedtuple("ChannelRecord", [
    "gossip_id", "from_timestamp", "raw_bytes", "scid",
    "source_node_id", "target_node_id", "amount_sat"
])

class ChannelAnnouncementBatcher:
    def __init__(self, conn, cache, logger, node_batcher, batch_size=100):
        self.conn = conn
        self.cache = cache
        self.logger = logger
        self.node_batcher = node_batcher
        self.batch_size = batch_size
        self.channel_batch: List[ChannelRecord] = []
        self.lock = threading.Lock()

    def handle_message(self, raw_bytes: bytes):
        gossip_id = hashlib.sha256(raw_bytes).digest()
        if self.cache.has_gossip(gossip_id):
            return

        try:
            ca = parse_channel_announcement(strip_known_message_type(raw_bytes))
            scid = ca.scid_str
            block_height, tx_idx, out_idx = split_scid(scid, self.logger)
            source_node_id = ca.node_id_1
            target_node_id = ca.node_id_2

            if source_node_id >= target_node_id:
                self.logger.critical(
                    f"Node order violation: {source_node_id.hex()} >= {target_node_id.hex()} for scid {scid}"
                )
                write_invalid_gossip(raw_bytes, self.logger)
                return

            block = get_block_by_block_height(block_height, self.logger)
            if not block:
                write_invalid_gossip(raw_bytes, self.logger)
                return

            timestamp_unix = block.get("time")
            from_timestamp = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

            txs = block.get("tx", [])
            if tx_idx >= len(txs):
                write_invalid_gossip(raw_bytes, self.logger)
                return

            tx_id = txs[tx_idx]
            amount_sat = get_amount_sat_by_tx_idx_and_output_idx(tx_id, out_idx, self.logger)
            if amount_sat is None:
                write_invalid_gossip(raw_bytes, self.logger)
                return

            for node_id in [source_node_id, target_node_id]:
                if not self.cache.has_node(node_id):
                    self.node_batcher.add(node_id, from_timestamp, gossip_id)

            record = ChannelRecord(
                gossip_id, from_timestamp, raw_bytes, scid,
                source_node_id, target_node_id, amount_sat
            )

            with self.lock:
                self.channel_batch.append(record)
                if len(self.channel_batch) >= self.batch_size:
                    self.flush_locked()

        except Exception as e:
            self.logger.warning(f"Failed to handle message: {e}")
            write_invalid_gossip(raw_bytes, self.logger)

    def flush(self):
        with self.lock:
            self.flush_locked()

    def flush_locked(self):
        if not self.channel_batch:
            return
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
                        VALUES %s
                        ON CONFLICT DO NOTHING;
                    """, [(r.gossip_id, 256, r.from_timestamp, r.raw_bytes) for r in self.channel_batch])

                    execute_values(cur, """
                        INSERT INTO channels_raw_gossip (gossip_id, scid)
                        VALUES %s
                        ON CONFLICT DO NOTHING;
                    """, [(r.gossip_id, r.scid) for r in self.channel_batch])

                    execute_values(cur, """
                        INSERT INTO channels (scid, source_node_id, target_node_id, validity, amount_sat)
                        VALUES %s
                        ON CONFLICT DO NOTHING;
                    """, [
                        (
                            r.scid,
                            r.source_node_id,
                            r.target_node_id,
                            f"[{r.from_timestamp.isoformat()},)",
                            r.amount_sat
                        ) for r in self.channel_batch
                    ])

                    for r in self.channel_batch:
                        self.cache.cache_gossip(r.gossip_id)

                    self.logger.info(f"Inserted batch of {len(self.channel_batch)} channel announcements.")

        except Exception as e:
            self.logger.error(f"Batch insert failed: {e}")
        finally:
            self.channel_batch.clear()
