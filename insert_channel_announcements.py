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
from psycopg2.errors import ForeignKeyViolation

from logging.handlers import RotatingFileHandler
from typing import Optional, List
from types import FrameType

from ValkeyClient import ValkeyCache
from NodeAnnouncementBatcher import NodeAnnouncementBatcher
from BlockchainRequester import get_block_by_block_height, get_amount_sat_by_tx_idx_and_output_idx
from config import POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type
from lnhistoryclient.parser.parser import parse_channel_announcement
from lnhistoryclient.model.ChannelAnnouncement import ChannelAnnouncement

INVALID_CHANNEL_ANNOUNCEMENTS_FILE = "invalid_channel_announcements.gsp"

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

def handle_channel_announcement(raw_bytes: bytes, cache: ValkeyCache, logger: logging.Logger) -> None:
    gossip_id = hashlib.sha256(raw_bytes).digest()
    if cache.has_gossip(gossip_id):
        return

    try:
        channel_announcement: ChannelAnnouncement = parse_channel_announcement(strip_known_message_type(raw_bytes))
        scid = channel_announcement.scid_str
        block_height, tx_idx, out_idx = split_scid(scid, logger)
        source_node_id = channel_announcement.node_id_1
        target_node_id = channel_announcement.node_id_2

       
        # Lexicographical check
        if source_node_id >= target_node_id:
            logger.critical(
                f"Node order violation: source_node_id ({source_node_id.hex()}) >= target_node_id ({target_node_id.hex()}) for scid {scid}"
            )
            write_invalid_gossip(raw_bytes, logger)
            return

        try:
            block = get_block_by_block_height(block_height, logger)
            if not block:
                logger.warning(f"Block at height {block_height} not found.")
                write_invalid_gossip(raw_bytes, logger)
                return
        except Exception as e:
            logger.warning(f"Failed to get bitcoin block at height {block_height}: {e}")
            write_invalid_gossip(raw_bytes, logger)
            return

        timestamp_unix: Optional[int] = block.get("time", None)
        if not timestamp_unix:
            logger.warning(f"Missing timestamp in block at height {block_height}")
            write_invalid_gossip(raw_bytes, logger)
            return

        from_timestamp = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

        block_tx: Optional[List[str]] = block.get("tx", None)
        if not block_tx or tx_idx >= len(block_tx):
            logger.warning(f"Invalid tx index {tx_idx} for block at height {block_height}")
            write_invalid_gossip(raw_bytes, logger)
            return

        tx_id = block_tx[tx_idx]
        if not tx_id:
            logger.warning(f"Transaction ID is missing for tx index {tx_idx} in block {block_height}")
            write_invalid_gossip(raw_bytes, logger)
            return

        try:
            amount_sat = get_amount_sat_by_tx_idx_and_output_idx(tx_id, out_idx, logger)
            if amount_sat is None:
                logger.warning(f"Could not determine amount_sat for tx {tx_id} output {out_idx}")
                write_invalid_gossip(raw_bytes, logger)
                return
        except Exception as e:
            logger.warning(f"Failed to get amount_sat for block_height {block_height} with tx_id {tx_id}, out {out_idx}: {e}")
            write_invalid_gossip(raw_bytes, logger)
            return

    except Exception as e:
        raise ValueError(f"Invalid channel_announcement with raw_gossip {raw_bytes.hex()} format: {e}")
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Insert the raw_gossip into raw_gossip table
                cur.execute("""
                    INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    gossip_id, 256, from_timestamp, raw_bytes
                ))
                logger.debug(f"Inserted gossip_id {gossip_id.hex()} into raw_gossip")

                # Insert SCID mapping into channels_raw_gossip
                cur.execute("""
                    INSERT INTO channels_raw_gossip (gossip_id, scid)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    gossip_id, scid
                ))
                logger.debug(f"Linked gossip_id {gossip_id.hex()} to scid {scid} in channels_raw_gossip")

                # Ensure both source and target node_ids exist before inserting the channel
                for node_id in [source_node_id, target_node_id]:
                    if not cache.has_node(node_id):
                        # Insert into nodes_raw_gossip
                        cur.execute("""
                            INSERT INTO nodes_raw_gossip (gossip_id, node_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            gossip_id, node_id
                        ))
                        logger.info(f"Linked node_id {node_id.hex()} to gossip_id in nodes_raw_gossip")

                        # Insert node into nodes table
                        cur.execute("""
                            INSERT INTO nodes (node_id, validity)
                            VALUES (%s, tstzrange(%s, NULL))
                            ON CONFLICT DO NOTHING;
                        """, (
                            node_id, from_timestamp
                        ))
                        logger.info(f"Inserted missing node_id {node_id.hex()} into nodes table")

                        # Add to cache
                        cache.cache_node(node_id)

                # Insert into channels
                cur.execute("""
                    INSERT INTO channels (scid, source_node_id, target_node_id, validity, amount_sat)
                    VALUES (%s, %s, %s, tstzrange(%s, NULL), %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    scid, source_node_id, target_node_id, from_timestamp, amount_sat
                ))
                logger.info(f"Inserted channel {scid} from {source_node_id.hex()} to {target_node_id.hex()}")

    except Exception as e:
        logger.error(f"Database error while processing channel_announcement for scid {scid}: {e}")
        write_invalid_gossip(raw_bytes, logger)
        return

    # Cache the gossip_id to prevent reprocessing
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


def setup_logging(log_dir: str = "logs", log_file: str = "insert_channel_announcements.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("insert_channel_announcements")
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
    file_path = "./testchannel_announcements.gsp.bz2"

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

                if msg_type == 256:  # channel_announcement
                    gossip_id = hashlib.sha256(gossip_message).digest()
                    if cache.has_gossip(gossip_id):
                        logger.warning(f"Found duplicate gossip_message raw_gossip {gossip_message.hex()} for gossip_id {gossip_id.hex()}")
                        continue

                    try:
                        handle_channel_announcement(gossip_message, cache, logger)

                    except Exception as e:
                        logger.warning(f"Skipping invalid channel_announcement: {e}")
                        continue


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

        # Log final statistics
        logger.info(f"Completed processing {count} messages")
        logger.info("Message type distribution:")
        for msg_type, type_count in type_counts.items():
            type_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")
            percentage = (type_count / count) * 100 if count > 0 else 0
            logger.info(f"  {type_name}: {type_count} ({percentage:.2f}%)")

    except Exception as e:
        logger.error(f"Error reading dataset: {e}")

# Global variables
running: bool = True
stop_event: Optional[threading.Event] = None

# Assume you have a global db connection
conn = psycopg2.connect(
    dbname=POSTGRES_DB_NAME, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT
)

if __name__ == "__main__":
    main()