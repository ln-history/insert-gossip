from typing import Optional
import logging
import requests  # type: ignore[import-untyped]
from lnhistoryclient.parser import parser_factory
from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type

from config import EXPLORER_RPC_URL, EXPLORER_RPC_PASSWORD


def get_block_by_block_height(block_height: int, logger: logging.Logger) -> Optional[dict]:  # type: ignore[type-arg]
    try:
        block_url = f"{EXPLORER_RPC_URL}/api/block/{block_height}"
        resp = requests.get(block_url, auth=("block-requester", EXPLORER_RPC_PASSWORD), timeout=15)

        if resp.status_code != 200:
            logger.warning(f"Block {block_height} not found (status {resp.status_code})")
            return None

        return resp.json()  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Error fetching block at block_height {block_height}: {e}")
        return None


def get_amount_sat_by_tx_idx_and_output_idx(tx_id: int, output_idx: int, logger: logging.Logger) -> Optional[int]:
    try:
        tx_url = f"{EXPLORER_RPC_URL}/api/tx/{tx_id}"
        tx_resp = requests.get(tx_url, auth=("blockchain-requester", EXPLORER_RPC_PASSWORD), timeout=30)

        if tx_resp.status_code != 200:
            logger.error(f"Failed to fetch tx with tx_id {tx_id}: (status {tx_resp.status_code})")
            return None

        for vout in tx_resp.json().get("vout", []):
            if vout.get("n") == output_idx:
                value_btc = vout.get("value")
                if value_btc is None:
                    logger.error(f"Missing value for output index {output_idx} in tx {tx_id}")
                    return None
                amount_sat = int(value_btc * 100_000_000)
                logger.debug(f"Found vout {output_idx}: {value_btc} BTC = {amount_sat} sats")
                return amount_sat

        logger.warning(f"Output index {output_idx} not found in tx {tx_id}")
        return None

    except Exception as e:
        logger.exception(f"Error resolving amount_sat for tx_id {tx_id} and output_idx {output_idx}: {e}")
        return None