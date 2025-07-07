import os
import sys
import json
import bz2
import logging
import signal
import threading
import struct
from types import FrameType
from typing import Optional, Dict, Any, List
from logging.handlers import RotatingFileHandler
from kafka.producer import KafkaProducer

from ValkeyClient import ValkeyCache
from BlockchainRequester import get_recorded_at_unix_timestamp_from_fallback
from config import KAFKA_SERVER_IP_ADDRESS, KAFKA_SERVER_PORT, KAFKA_TOPIC_TO_PRODUCE, NODE_ID

from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata
from lnhistoryclient.parser.common import get_message_type_by_raw_hex


def compactsize_encode(i, w):
    """Encode an integer `i` into the writer `w`
    """
    if i < 0xFD:
        w.write(struct.pack("!B", i))
    elif i <= 0xFFFF:
        w.write(struct.pack("!BH", 0xFD, i))
    elif i <= 0xFFFFFFFF:
        w.write(struct.pack("!BL", 0xFE, i))
    else:
        w.write(struct.pack("!BQ", 0xFF, i))


def compactsize_decode(r):
    """Decode an integer from reader `r`
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


def varint_encode(i, w):
    return compactsize_encode(i, w)


def varint_decode(r):
    return compactsize_decode(r)


def read_dataset(filename: str):
    with bz2.open(filename, 'rb') as f:
        header = f.read(4)
        assert(header[:3] == b'GSP' and header[3] == 1)
        while True:
            length = varint_decode(f)
            if length is None:  # End of file
                break
            msg = f.read(length)
            if len(msg) != length:
                raise ValueError(f"Incomplete message read from {filename}")

            yield msg


def setup_logging(log_dir: str = "logs", log_file: str = "gossip_syncer.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("gossip_syncer")
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def create_kafka_producer() -> KafkaProducer:
    bootstrap_servers = f"{KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}"
    print(f"Connecting to Kafka at: {bootstrap_servers}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            client_id="insert-gossip",
            api_version="3",
            security_protocol="SASL_SSL",
            ssl_cafile="./certs/kafka.truststore.pem",
            ssl_certfile="./certs/kafka.keystore.pem",
            ssl_keyfile="./certs/kafka.keystore.pem",
            ssl_password=os.getenv("SSL_PASSWORD"),
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
            sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
            ssl_check_hostname=False,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        return producer
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        raise


def shutdown(logger: logging.Logger, signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """Clean up resources"""
    global running, producer, stop_event
    logger.info("Shutting down gossip-post-processor...")
    running = False

    # Signal all worker threads to stop
    if stop_event:
        stop_event.set()

    if producer:
        producer.close()
        logger.info("Kafka producer closed.")
    logger.info("Shutdown complete.")
    sys.exit(0)


def construct_platform_event(gossip_message: bytes, cache: ValkeyCache) -> PlatformEvent:
    """Constructs the outgoing message format with computed id."""
    raw_hex = gossip_message.hex()
    msg_type = get_message_type_by_raw_hex(gossip_message)
    
    metadata: PlatformEventMetadata = {
        "id": cache.hash_raw_hex(raw_hex),
        "type": msg_type,
        "timestamp": None,  # Will be filled by process_and_forward_gossip_message
    }

    return {
        "metadata": metadata,
        "raw_hex": raw_hex,
    }


def process_and_forward_gossip_message(
    gossip_msg: bytes,
    cache: ValkeyCache,
    logger: logging.Logger,
    producer: KafkaProducer,
    topic: str
) -> int:
    msg_type = get_message_type_by_raw_hex(gossip_msg)
    msg_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")

    if msg_type not in ALL_TYPES:
        logger.error(f"Unknown gossip message type received: {msg_type}")
        return msg_type

    raw_hex = gossip_msg.hex()
    logger.debug(f"Handling {msg_name} message.")

    gossip_id = cache.hash_raw_hex(raw_hex)
    seen_by = cache.get_seen_from_node_id(msg_type, gossip_id)

    if not seen_by:
        logger.debug(f"New gossip message (hash={gossip_id[:8]}...) seen for the first time.")
    elif NODE_ID not in seen_by:
        logger.debug(f"New node {NODE_ID} for gossip hash={gossip_id[:8]}...")

    recorded_at = get_recorded_at_unix_timestamp_from_fallback(raw_hex, gossip_id, logger)
    cache.append_seen_by(msg_type, gossip_id, NODE_ID, recorded_at)

    platform_event = construct_platform_event(gossip_msg, cache)
    platform_event["metadata"]["timestamp"] = recorded_at
    
    producer.send(topic, value=platform_event)
    logger.debug(f"Forwarded {msg_name} message to Kafka topic {topic}")
    
    return msg_type


def main() -> None:
    file_path = "/Users/fabiankraus/Programming/topology/data/uniq2.gsp.bz2"

    logger: logging.Logger = setup_logging()
    
    # Register shutdown handlers
    signal.signal(signal.SIGINT, lambda s, f: shutdown(logger, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown(logger, s, f))

    logger.info("Starting insert-gossip... Press Ctrl+C to exit.")
    
    # Initialize stop event and thread tracking
    global stop_event, producer, running
    stop_event = threading.Event()
    running = True
    
    # Initialize Kafka producer
    try:
        producer = create_kafka_producer()
        logger.info(f"Connected to Kafka at {KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return

    # Initialize cache
    cache = ValkeyCache()
    
    # Track message types and counts
    count = 0
    type_counts = {}
    current_type = None
    
    # Process messages using the simpler read_dataset function
    try:
        for gossip_message in read_dataset(file_path):
            if not running:
                break
            
            try:
                msg_type = process_and_forward_gossip_message(
                    gossip_message,
                    cache,
                    logger,
                    producer,
                    KAFKA_TOPIC_TO_PRODUCE
                )
                
                # Update counts
                type_counts[msg_type] = type_counts.get(msg_type, 0) + 1
                count += 1
                
                # Log type changes
                if current_type != msg_type:
                    if current_type is not None:
                        logger.info(f"Message type changed from {GOSSIP_TYPE_NAMES.get(current_type, f'unknown({current_type})')} "
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
    finally:
        # Clean up resources
        if producer:
            producer.close()
            logger.info("Kafka producer closed")


# Global variables
running: bool = True
producer: Optional[KafkaProducer] = None
stop_event: Optional[threading.Event] = None

if __name__ == "__main__":
    main()