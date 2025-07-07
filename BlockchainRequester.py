from typing import Optional
import logging
import requests  # type: ignore[import-untyped]
from lnhistoryclient.constants import (
    MSG_TYPE_CHANNEL_ANNOUNCEMENT,
    MSG_TYPE_CHANNEL_UPDATE,
    MSG_TYPE_NODE_ANNOUNCEMENT,
)
from lnhistoryclient.parser import parser_factory
from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type

from config import COUCHDB_PASSWORD, COUCHDB_URL, COUCHDB_USER


def get_block_by_block_height(block_height: int, logger: logging.Logger) -> Optional[dict]:  # type: ignore[type-arg]
    try:
        block_url = f"{COUCHDB_URL}/blocks/{block_height}"
        logger.debug(f"Fetching block data from: {block_url}")
        resp = requests.get(block_url, auth=(COUCHDB_USER, COUCHDB_PASSWORD), timeout=15)

        if resp.status_code != 200:
            logger.warning(f"Block {block_height} not found (status {resp.status_code})")
            return None

        logger.debug(f"Sucessuffly fetched block at height {block_height}")
        return resp.json()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Error fetching block at block_height {block_height}: {e}")
        return None


def get_unix_timestamp_by_blockheight(block_height: int, logger: logging.Logger) -> Optional[int]:
    logger.debug(f"Fetching timestamp for block_height: {block_height}")
    try:
        block_data = get_block_by_block_height(block_height, logger)

        if block_data is None:
            return None

        timestamp = block_data.get("time")
        if timestamp is None:
            logger.warning(f"Missing 'time' field in block {block_height}")
            return None

        timestamp_int = int(timestamp)
        logger.debug(f"Parsed timestamp: {timestamp_int} for block {block_height}")
        return timestamp_int

    except Exception as e:
        logger.exception(f"Failed to get timestamp for block {block_height}: {e}")
        return None


def get_recorded_at_unix_timestamp_from_fallback(raw_hex: str, gossip_id: str, logger: logging.Logger) -> Optional[int]:
    logger.warning(f"Missing timestamp for gossip_id={gossip_id} — using fallback parsing")
    try:
        raw_bytes = bytes.fromhex(raw_hex)
        logger.debug(f"Converted raw_hex to bytes for gossip_id={gossip_id}")

        msg_type = get_message_type_by_raw_hex(raw_bytes)
        parser = parser_factory.get_parser_by_message_type(msg_type)
        stripped_raw_bytes = strip_known_message_type(raw_bytes) 
        parsed = parser(stripped_raw_bytes)
        logger.debug(f"Parsed message type: {msg_type} for gossip_id={gossip_id}")

        if msg_type in (MSG_TYPE_NODE_ANNOUNCEMENT, MSG_TYPE_CHANNEL_UPDATE):
            timestamp: int = parsed.get("timestamp")
            if timestamp is None:
                logger.warning(f"No timestamp found in parsed {msg_type} message for gossip_id={gossip_id}")
                return None
            logger.debug(f"Extracted timestamp={timestamp} for gossip_id={gossip_id}")
            return timestamp

        elif msg_type == MSG_TYPE_CHANNEL_ANNOUNCEMENT:
            scid = parsed.scid_str
            logger.debug(f"Parsed scid={scid} from channel_announcement for gossip_id={gossip_id}")
            if not scid or "x" not in scid:
                logger.warning(f"Invalid or missing scid in gossip_id={gossip_id}")
                return None

            block_height_str = scid.split("x")[0]
            try:
                block_height = int(block_height_str)
                logger.debug(f"Extracted block_height={block_height} from scid for gossip_id={gossip_id}")
                return get_unix_timestamp_by_blockheight(block_height, logger)
            except ValueError:
                logger.warning(f"Non-integer block height in scid={scid} for gossip_id={gossip_id}")
                return None

        logger.warning(f"No fallback logic for message type: {msg_type}")
        return None

    except Exception as e:
        logger.exception(f"Error parsing fallback timestamp for gossip_id={gossip_id}: {e}")
        return None
