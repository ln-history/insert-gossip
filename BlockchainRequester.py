from typing import Optional
import logging
import requests  # type: ignore[import-untyped]
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import EXPLORER_RPC_URL, EXPLORER_RPC_PASSWORD
# Limit concurrent requests (you can tune this)
HTTP_CONCURRENCY_LIMIT = 5
http_sema = threading.BoundedSemaphore(HTTP_CONCURRENCY_LIMIT)

# Set up a shared requests session with retry and connection pooling
session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=HTTP_CONCURRENCY_LIMIT,
    pool_maxsize=HTTP_CONCURRENCY_LIMIT,
    max_retries=Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
)
session.mount("http://", adapter)
session.mount("https://", adapter)


def safe_get(*args, **kwargs):
    """Thread-safe, throttled GET request."""
    with http_sema:
        return session.get(*args, **kwargs)


def get_block_by_block_height(block_height: int, logger: logging.Logger) -> Optional[dict]:
    try:
        block_url = f"{EXPLORER_RPC_URL}/api/block/{block_height}"
        resp = safe_get(block_url, auth=("block-requester", EXPLORER_RPC_PASSWORD), timeout=15)

        if resp.status_code != 200:
            logger.warning(f"Block {block_height} not found (status {resp.status_code})")
            return None

        return resp.json()
    except Exception as e:
        logger.error(f"Error fetching block at block_height {block_height}: {e}")
        return None


def get_amount_sat_by_tx_idx_and_output_idx(tx_id: int, output_idx: int, logger: logging.Logger) -> Optional[int]:
    try:
        tx_url = f"{EXPLORER_RPC_URL}/api/tx/{tx_id}"
        tx_resp = safe_get(tx_url, auth=("blockchain-requester", EXPLORER_RPC_PASSWORD), timeout=30)

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