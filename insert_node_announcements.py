import logging
import os
import signal
import threading
import sys
import bz2
import struct
import psycopg2
import hashlib
from datetime import datetime, timezone

from logging.handlers import RotatingFileHandler
from typing import Optional
from types import FrameType

from ValkeyClient import ValkeyCache
from NodeAnnouncementBatcher import NodeAnnouncementBatcher
from config import POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type
from lnhistoryclient.parser.parser import parse_node_announcement
from lnhistoryclient.model.NodeAnnouncement import NodeAnnouncement

def handle_node_announcement(raw_bytes: bytes, cache: ValkeyCache) -> None:
    gossip_id = hashlib.sha256(raw_bytes).digest()
    if cache.has_gossip(gossip_id):
        return

    try:

        node_announcement: NodeAnnouncement = parse_node_announcement(strip_known_message_type(raw_bytes))
        node_id = node_announcement.node_id
        timestamp_unix = node_announcement.timestamp
        timestamp = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)
    except Exception as e:
        raise ValueError(f"Invalid node_announcement format: {e}")

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (gossip_id, 257, timestamp, raw_bytes))

            cur.execute("""
                INSERT INTO nodes_raw_gossip (gossip_id, node_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (gossip_id, node_id))

            validity = cache.get_node_validity(node_id)

            if not validity:
                # Node unknown in cache → try DB
                cur.execute("SELECT lower(validity), upper(validity) FROM nodes WHERE node_id = %s", (node_id,))
                result = cur.fetchone()
                if result:
                    validity = result
                else:
                    # First ever seen → insert and cache
                    valid_from = timestamp
                    valid_to = timestamp
                    cur.execute("""
                        INSERT INTO nodes (node_id, validity)
                        VALUES (%s, tstzrange(%s, %s))
                        ON CONFLICT DO NOTHING
                    """, (node_id, valid_from, valid_to))
                    cache.set_node_validity(node_id, valid_from, valid_to)
                    cache.cache_gossip(gossip_id)
                    return

            # If timestamp outside cached/known range → update
            lower, upper = validity
            if timestamp < lower or timestamp > upper:
                new_lower = min(lower, timestamp)
                new_upper = max(upper, timestamp)
                cur.execute("""
                    UPDATE nodes
                    SET validity = tstzrange(%s, %s)
                    WHERE node_id = %s
                """, (new_lower, new_upper, node_id))
                cache.set_node_validity(node_id, new_lower, new_upper)

    cache.cache_gossip(gossip_id)

def varint_decode(r):
    """
    Decode an integer from reader `r`
    """
    raw = r.read(1)
    if len(raw) != 1:
        return None

    i, = struct.unpack("!B", raw)
    if i < 0xFD:
        return i
    elif i == 0xFD:
        return struct.unpack("!H", r.read(2))[0]
    elif i == 0xFE:
        return struct.unpack("!L", r.read(4))[0]
    else:
        return struct.unpack("!Q", r.read(8))[0]

def read_dataset(filename: str, start: int = 0, logger: Optional[logging.Logger] = None):
    with bz2.open(filename, 'rb') as f:
        header = f.read(4)
        assert header[:3] == b'GSP' and header[3] == 1

        skipped = 0
        yielded = 0
        last_logged = 0

        while True:
            length = varint_decode(f)
            if length is None:
                break

            msg = f.read(length)
            if len(msg) != length:
                raise ValueError(f"Incomplete message read from {filename}")

            if skipped < start:
                skipped += 1
                continue

            yield msg
            yielded += 1

            if logger and yielded % 100_000 == 0 and yielded != last_logged:
                logger.info(f"Yielded {yielded} messages (starting from offset {start})")
                last_logged = yielded


def setup_logging(log_dir: str = "logs", log_file: str = "insert_gossip.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("insert_gossip")
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

def shutdown(logger: logging.Logger, signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """Clean up resources"""
    global running, stop_event
    logger.info("Shutting down gossip-post-processor...")
    running = False

    # Signal all worker threads to stop
    if stop_event:
        stop_event.set()

    logger.info("Shutdown complete.")
    sys.exit(0)

def main() -> None:
    file_path = "./testnode_announcements.gsp.bz2"

    logger: logging.Logger = setup_logging()
    
    # Register shutdown handlers
    signal.signal(signal.SIGINT, lambda s, f: shutdown(logger, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown(logger, s, f))

    logger.info("Starting insert-gossip... Press Ctrl+C to exit.")
    
    # Initialize stop event and thread tracking
    global stop_event, running
    stop_event = threading.Event()
    running = True

    # Initialize cache
    cache = ValkeyCache()

    # Initialize batcher
    node_announcement_batcher = NodeAnnouncementBatcher(conn, cache, batch_size=1000)
    
    # Track message types and counts
    count = 0
    type_counts = {}
    current_type = None
    
    # Process messages using the simpler read_dataset function
    try:
        for gossip_message in read_dataset(file_path, start=0, logger=logger):
            if not running:
                break

            try:
                msg_type = get_message_type_by_raw_hex(gossip_message)

                if msg_type == 257:  # node_announcement
                    gossip_id = hashlib.sha256(gossip_message).digest()
                    if cache.has_gossip(gossip_id):
                        logger.warning(f"Found duplicate gossip_message for gossip_id {gossip_id}")
                        continue

                    try:
                        node_announcement = parse_node_announcement(strip_known_message_type(gossip_message))
                        node_id = node_announcement.node_id
                        timestamp_unix = node_announcement.timestamp
                    except Exception as e:
                        logger.warning(f"Skipping invalid node_announcement: {e}")
                        continue

                    node_announcement_batcher.add(gossip_message, node_id, timestamp_unix, gossip_id)

                elif msg_type == 256:  # channel_announcement
                    pass  # To be implemented

                elif msg_type == 258:  # channel_update
                    pass  # To be implemented

                else:
                    logger.warning(f"Unknown message type: {msg_type}")

                # Update counts
                type_counts[msg_type] = type_counts.get(msg_type, 0) + 1
                count += 1

                # Log type changes
                if current_type != msg_type:
                    if current_type is not None:
                        logger.info(
                            f"Message type changed from {GOSSIP_TYPE_NAMES.get(current_type, f'unknown({current_type})')} "
                            f"to {GOSSIP_TYPE_NAMES.get(msg_type, f'unknown({msg_type})')} after {type_counts.get(current_type, 0)} messages")
                    current_type = msg_type

                # Periodic logging
                if count % 1000 == 0:
                    logger.info(f"Processed {count} messages so far. Current type: {GOSSIP_TYPE_NAMES.get(current_type, f'unknown({current_type})')}")

            except Exception as e:
                logger.error(f"Error processing message: {e}")

        # Final flush for remaining messages
        node_announcement_batcher.close()

        # Log final statistics
        logger.info(f"Completed processing {count} messages")
        logger.info("Message type distribution:")
        for msg_type, type_count in type_counts.items():
            type_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")
            percentage = (type_count / count) * 100 if count > 0 else 0
            logger.info(f"  {type_name}: {type_count} ({percentage:.2f}%)")

    except Exception as e:
        logger.error(f"Error reading dataset: {e}")
    finally:
        node_announcement_batcher.close()

# Global variables
running: bool = True
stop_event: Optional[threading.Event] = None

# Assume you have a global db connection
conn = psycopg2.connect(
    dbname=POSTGRES_DB_NAME, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT
)

if __name__ == "__main__":
    main()