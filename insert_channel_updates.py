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
from datetime import datetime
from typing import Optional, List, Tuple
from types import FrameType

from ValkeyClient import ValkeyCache
from config import POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT
from BlockchainRequester import get_block_by_block_height, get_amount_sat_by_tx_idx_and_output_idx

from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type
from lnhistoryclient.parser.parser import parse_channel_update

from lnhistoryclient.model.ChannelUpdate import ChannelUpdate

def split_scid(scid, logger: logging.Logger) -> tuple[int, int, int]:
    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid}. - Skipping further handling")
        return

def get_channel_creation_data(block_height: int, tx_idx: int, out_idx: int, logger: logging.Logger) -> Tuple[Optional[datetime], Optional[int]]:
    result = (None, None)
    try:
        block = get_block_by_block_height(block_height, logger)
        if not block:
            logger.warning(f"Block at height {block_height} not found.")
            return result
    except Exception as e:
        logger.warning(f"Failed to get bitcoin block at height {block_height}: {e}")
        return result

    block_timestamp_unix: Optional[int] = block.get("time", None)
    if not block_timestamp_unix:
        logger.warning(f"Missing timestamp in block at height {block_height}")
        return result

    channel_creation_timestamp = datetime.fromtimestamp(block_timestamp_unix, tz=timezone.utc)
    result[0] = channel_creation_timestamp

    block_tx: Optional[List[str]] = block.get("tx", None)
    if not block_tx or tx_idx >= len(block_tx):
        logger.warning(f"Invalid tx index {tx_idx} for block at height {block_height}")
        return result
    tx_id = block_tx[tx_idx]

    if not tx_id:
        logger.warning(f"Transaction ID (tx_id) is missing for tx index {tx_idx} in block {block_height}")
        return result

    try:
        amount_sat: int = get_amount_sat_by_tx_idx_and_output_idx(tx_id, out_idx, logger)
        if amount_sat is None:
            logger.warning(f"Could not determine amount_sat for tx {tx_id} output {out_idx}")
            return None
        result[1] = amount_sat
        return result
    except Exception as e:
        logger.warning(f"Failed to get amount_sat for block_height {block_height} with tx_id {tx_id}, out {out_idx}: {e}")
        return result

def process_channel_updates_batch(
    raw_updates: List[bytes],
    cache: ValkeyCache,
    logger: logging.Logger,
    batch_size: int = 500
) -> None:
    raw_gossip_values = []
    channels_raw_gossip_values = []
    channel_updates_values = []
    gossip_ids_to_cache = []

    for raw_bytes in raw_updates:
        try:
            gossip_id = hashlib.sha256(raw_bytes).digest()
            channel_update: ChannelUpdate = parse_channel_update(strip_known_message_type(raw_bytes))
            scid = channel_update.scid_str
            direction = bool(channel_update.direction)
            timestamp_unix = channel_update.timestamp
            update_timestamp_dt = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

            raw_gossip_values.append((gossip_id, 258, update_timestamp_dt, raw_bytes))
            channels_raw_gossip_values.append((gossip_id, scid))

            if cache.has_channel(scid):
                channel_updates_values.append((scid, direction, update_timestamp_dt))
            else:
                logger.info(f"Skipping channel_update due to missing channel:{scid} in Valkey cache")
                # Optionally: collect for retry/fallback
                # fallback_channel_updates.append((gossip_id, scid, direction, update_timestamp_dt))
                continue

            gossip_ids_to_cache.append(gossip_id)

        except Exception as e:
            logger.warning(f"Skipping invalid channel_update: {e}")
            continue

    try:
        with conn:
            with conn.cursor() as cur:
                if raw_gossip_values:
                    cur.executemany("""
                        INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, raw_gossip_values)
                    logger.info(f"Inserted {len(raw_gossip_values)} entries into raw_gossip")

                if channels_raw_gossip_values:
                    cur.executemany("""
                        INSERT INTO channels_raw_gossip (gossip_id, scid)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, channels_raw_gossip_values)
                    logger.info(f"Linked {len(channels_raw_gossip_values)} entries in channels_raw_gossip")

                if channel_updates_values:
                    cur.executemany("""
                        INSERT INTO channel_updates (scid, direction, validity)
                        VALUES (%s, %s, tstzrange(%s, NULL))
                        ON CONFLICT DO NOTHING;
                    """, channel_updates_values)
                    logger.info(f"Inserted {len(channel_updates_values)} entries into channel_updates")

                # Cache inserted gossip_ids
                for gossip_id in gossip_ids_to_cache:
                    cache.cache_gossip(gossip_id)

    except ForeignKeyViolation as fk:
        logger.error(f"FK violation during batch insert: {fk}")
    except Exception as e:
        logger.error(f"Unhandled error in batch insert: {e}")

def handle_channel_update(raw_bytes: bytes, cache: ValkeyCache, logger: logging.Logger) -> None:
    try:
        gossip_id = hashlib.sha256(raw_bytes).digest()

        channel_update: ChannelUpdate = parse_channel_update(strip_known_message_type(raw_bytes))
        scid = channel_update.scid_str
        block_height, tx_idx, out_idx = split_scid(scid, logger)
        direction = channel_update.direction
        timestamp_unix = channel_update.timestamp
        update_timestamp_dt = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

    except Exception as e:
        raise ValueError(f"Invalid channel_update with raw_gossip {raw_bytes} format: {e}")
    
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
                logger.info(f"Inserted raw_gossip with gossip_id={gossip_id.hex()} for channel_update (scid, direction) ({scid, direction})")

                # 2. Insert into channels_raw_gossip
                cur.execute("""
                    INSERT INTO channels_raw_gossip (gossip_id, scid)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    gossip_id, scid
                ))
                logger.info(f"Linked gossip_id {gossip_id.hex()} to scid in channels_raw_gossip: {scid}")

                # 3. Insert into channel_updates
                try:
                    cur.execute("""
                        INSERT INTO channel_updates (scid, direction, validity)
                        VALUES (%s, %s, tstzrange(%s, NULL))
                        ON CONFLICT DO NOTHING;
                    """, (
                        scid, bool(direction), update_timestamp_dt
                    ))
                    logger.info(f"Inserted channel_update for scid {scid}, direction {direction}")

                except ForeignKeyViolation as fk_error:
                    logger.warning(f"Missing channel scid {scid} for channel_update with gossip_id {gossip_id}. FK violation: {fk_error}")
                    logger.info(f"Inserting placeholder channel and retrying...")

                    try:
                        channel_creation_data = get_channel_creation_data(block_height, tx_idx, out_idx, logger)
                        channel_creation_dt: Optional[datetime] = channel_creation_data[0]
                        amount_sat: Optional[int] = channel_creation_data[1]

                        # Insert fallback channel
                        cur.execute("""
                            INSERT INTO channels (scid, validity, source_node_id, target_node_id, amount_sat)
                            VALUES (%s, tstzrange(%s, NULL), NULL, NULL, %s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            scid, channel_creation_dt, amount_sat
                        ))
                        logger.warning(f"Inserted placeholder channel with scid {scid} at creation timestamp {channel_creation_dt} with amount_sat {amount_sat}")

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


def setup_logging(log_dir: str = "logs", log_file_base: str = "insert_channel_updates") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("insert_gossip")
    logger.setLevel(logging.INFO)

    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{log_file_base}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=100)
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
    file_path = "./testchannel_update.gsp.bz2"

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

    BATCH_SIZE = 1000

    batch = []
    
    # Process messages using the simpler read_dataset function
    try:
        for gossip_message in read_dataset(file_path, start=0, logger=logger):
            if not running:
                break

            try:
                msg_type = get_message_type_by_raw_hex(gossip_message)

                if msg_type == 258:  # channel_update
                    gossip_id = hashlib.sha256(gossip_message).digest()
                    # Ignore duplicates
                    if cache.has_gossip(gossip_id):
                        logger.warning(f"Found duplicate gossip_message for gossip_id {gossip_id.hex()}")
                        continue

                    batch.append(gossip_message)

                    # If batch is full, insert
                    if len(batch) >= BATCH_SIZE:
                        process_channel_updates_batch(batch, cache, logger, batch_size=BATCH_SIZE)
                        batch.clear()

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