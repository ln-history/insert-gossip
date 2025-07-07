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
from typing import Optional
from types import FrameType

from ValkeyClient import ValkeyCache
from NodeAnnouncementBatcher import NodeAnnouncementBatcher
from config import POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type
from lnhistoryclient.parser.parser import parse_channel_update
from BlockchainRequester import get_unix_timestamp_by_blockheight, get_amount_sat_by_tx_idx_and_output_idx

from lnhistoryclient.model.ChannelUpdate import ChannelUpdate

def split_scid(scid, logger: logging.Logger) -> tuple[int, int, int]:
    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid}. - Skipping further handling")
        return

def handle_channel_update(raw_bytes: bytes, cache: ValkeyCache, logger: logging.Logger) -> None:
    try:
        gossip_id = hashlib.sha256(raw_bytes).digest()

        channel_update: ChannelUpdate = parse_channel_update(strip_known_message_type(raw_bytes))
        scid = channel_update.scid_str
        direction = channel_update.direction
        timestamp_unix = channel_update.timestamp
        update_timestamp_dt = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

    except Exception as e:
        raise ValueError(f"Invalid channel_announcement with raw_gossip {raw_bytes} format: {e}")
    
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Insert raw_gossip
                cur.execute("""
                    INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    gossip_id, 258, update_timestamp_dt, raw_bytes
                ))
                logger.info(f"Inserted raw_gossip for channel_update {scid}, gossip_id={gossip_id.hex()}")

                # 2. Insert into channels_raw_gossip
                cur.execute("""
                    INSERT INTO channels_raw_gossip (gossip_id, scid)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    gossip_id, scid
                ))
                logger.info(f"Linked gossip_id to scid in channels_raw_gossip: {scid}")

                # 3. Insert into channel_updates
                try:
                    cur.execute("""
                        INSERT INTO channel_updates (scid, direction, validity)
                        VALUES (%s, %s, tstzrange(%s, NULL))
                        ON CONFLICT DO NOTHING;
                    """, (
                        scid, direction, update_timestamp_dt
                    ))
                    logger.info(f"Inserted channel_update for {scid}, direction={direction}")

                except ForeignKeyViolation as fk_error:
                    logger.warning(f"Missing channel {scid} for channel_update. FK violation: {fk_error}")
                    logger.info(f"Inserting placeholder channel and retrying...")

                    try:
                        # Insert fallback channel
                        cur.execute("""
                            INSERT INTO channels (scid, validity, source_node_id, target_node_id, amount_sat)
                            VALUES (%s, tstzrange(%s, NULL), NULL, NULL, NULL)
                            ON CONFLICT DO NOTHING;
                        """, (
                            scid, update_timestamp_dt
                        ))
                        logger.warning(f"Inserted placeholder channel for scid {scid}")

                        # Retry inserting channel_update
                        cur.execute("""
                            INSERT INTO channel_updates (scid, direction, validity)
                            VALUES (%s, %s, tstzrange(%s, NULL))
                            ON CONFLICT DO NOTHING;
                        """, (
                            scid, direction, update_timestamp_dt
                        ))
                        logger.info(f"Successfully retried channel_update insert for scid {scid}")

                    except Exception as retry_error:
                        logger.error(f"Failed retry after inserting placeholder channel: {retry_error}")
                        return

    except Exception as e:
        logger.error(f"Unhandled exception while processing channel_update {scid}: {e}")
        return
    
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
                        logger.warning(f"Found duplicate gossip_message raw_gossip {gossip_message} for gossip_id {gossip_id}")
                        continue

                    try:
                        handle_channel_update(gossip_message, cache, logger)

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