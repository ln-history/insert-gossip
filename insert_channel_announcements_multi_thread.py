import signal
import threading
import hashlib
import logging
import bz2
import struct
import psycopg2
import os
import sys
from typing import Optional
from logging.handlers import RotatingFileHandler

from ValkeyClient import ValkeyCache
from NodeAnnouncementBatcher import NodeAnnouncementBatcher
from BlockchainRequester import get_block_by_block_height, get_amount_sat_by_tx_idx_and_output_idx
from config import POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT
from ChannelAnnouncementProcessor import ChannelAnnouncementProcessor

from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type
from lnhistoryclient.parser.parser import parse_channel_announcement
from lnhistoryclient.model.ChannelAnnouncement import ChannelAnnouncement

# Shutdown flag
stop_event = threading.Event()
running = True

def setup_logging(log_dir: str = "logs", log_file: str = "insert_channel_announcements.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("insert_channel_announcements")
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s -  %(threadName)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

def varint_decode(r):
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

def shutdown(logger: logging.Logger, signum=None, frame=None):
    """Clean up resources"""
    global running, stop_event
    logger.info("Shutting down insert-gossip: channel_announcements multi-threaded...")
    running = False

    # Signal all worker threads to stop
    if stop_event:
        stop_event.set()

    logger.info("Shutdown complete.")
    sys.exit(0)

def main():
    file_path = "./testchannel_announcements.gsp.bz2"
    logger = setup_logging()
    logger.info("Starting insert-gossip... Press Ctrl+C to exit.")

    # Setup signal handling
    signal.signal(signal.SIGINT, lambda s, f: shutdown(logger, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown(logger, s, f))

    # Setup cache and processor
    cache = ValkeyCache()
    processor = ChannelAnnouncementProcessor(
        conn=conn,
        cache=cache,
        logger=logger,
        max_workers=5,
        batch_size=100
    )

    count = 0
    type_counts = {}

    try:
        for gossip_message in read_dataset(file_path, start=2800, logger=logger):
            if not running:
                break

            msg_type = get_message_type_by_raw_hex(gossip_message)
            type_counts[msg_type] = type_counts.get(msg_type, 0) + 1

            if msg_type == 256:  # channel_announcement
                gossip_id = hashlib.sha256(gossip_message).digest()
                if cache.has_gossip(gossip_id):
                    logger.info(f"Duplicate gossip_id: {gossip_id.hex()}")
                    continue

                processor.submit(gossip_message)
                count += 1

        # Final flush + wait for threads
        processor.run([])

    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")

    logger.info(f"Finished. Processed {count} channel_announcements.")
    logger.info(f"Message type counts: {type_counts}")

conn = psycopg2.connect(
    dbname=f"{POSTGRES_DB_NAME}-backup", user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT
)

if __name__ == "__main__":
    main()
