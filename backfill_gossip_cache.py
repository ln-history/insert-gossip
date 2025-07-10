import logging
import os
from typing import List
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

from psycopg2 import pool, sql
from valkey import Valkey

from config import POSTGRES_DB_NAME, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER, VALKEY_HOST, VALKEY_PASSWORD, VALKEY_PORT

# --- Config ---
BATCH_SIZE = 1000


# --- Logger ---
def setup_logging(log_dir: str = "logs", log_file: str = "gossip_backfill.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("gossip-backfill")
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


# --- ValkeyCache class ---
class ValkeyCache:
    def __init__(self) -> None:
        self.client = Valkey(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD, db=0)

    def cache_gossip_batch(self, gossip_ids: List[bytes], batch_size: int = 500) -> int:
        total_queued = 0
        for i in range(0, len(gossip_ids), batch_size):
            batch = gossip_ids[i:i + batch_size]
            pipe = self.client.pipeline()
            for gossip_id in batch:
                key = f"gossip:{gossip_id.hex()}"
                pipe.set(key, "1")
            pipe.execute()
            total_queued += len(batch)
        return total_queued
    
    def cache_node_ids_batch(self, node_ids: List[bytes], batch_size: int = 500) -> int:
        total_queued = 0
        for i in range(0, len(node_ids), batch_size):
            batch = node_ids[i:i + batch_size]
            pipe = self.client.pipeline()
            for node_id in batch:
                key = f"node:{node_id.hex()}"
                pipe.set(key, "1")
            pipe.execute()
            total_queued += len(batch)
        return total_queued


# --- PostgreSQLDataStore class ---
class PostgreSQLDataStore:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        try:
            self.pool = pool.SimpleConnectionPool(
                1,
                10,
                dbname=POSTGRES_DB_NAME,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
            )
            self.logger.info(f"PostgreSQL connection pool initialized. Connected to database {POSTGRES_DB_NAME}")
        except Exception:
            self.logger.exception("Failed to initialize PostgreSQL connection pool.")
            raise

    def _get_conn(self):
        return self.pool.getconn()

    def _put_conn(self, conn):
        self.pool.putconn(conn)

    def close(self) -> None:
        self.pool.closeall()
        self.logger.info("PostgreSQL connection pool closed.")

    @contextmanager
    def transaction(self):
        conn = self._get_conn()
        try:
            cur = conn.cursor(name="gossip_cursor")  # server-side cursor for streaming
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.critical(f"Transaction failed and rolled back: {e}")
            raise
        finally:
            cur.close()
            self._put_conn(conn)


# --- Main backfill process ---
def backfill_gossip_ids(postgres: PostgreSQLDataStore, valkey: ValkeyCache,  logger: logging.Logger, batch_size: int = BATCH_SIZE) -> None:
    logger.info("Starting backfill of gossip_ids from PostgreSQL to Valkey cache...")

    total_inserted = 0
    with postgres.transaction() as cur:
        cur.itersize = batch_size
        cur.execute("SELECT gossip_id FROM raw_gossip")

        buffer: List[bytes] = []
        for row in cur:
            buffer.append(row[0])
            if len(buffer) >= batch_size:
                inserted = valkey.cache_gossip_batch(buffer)
                total_inserted += inserted
                logger.info(f"Inserted batch of {inserted} gossip_ids into Valkey (total: {total_inserted})")
                buffer.clear()

        # Final batch
        if buffer:
            inserted = valkey.cache_gossip_batch(buffer)
            total_inserted += inserted
            logger.info(f"Inserted final batch of {inserted} gossip_ids into Valkey (total: {total_inserted})")

    logger.info(f"✅ Gossip_id backfill complete. Total inserted: {total_inserted}")


def backfill_node_ids(postgres: PostgreSQLDataStore, valkey: ValkeyCache, logger: logging.Logger, batch_size: int = BATCH_SIZE) -> None:
    logger.info("Starting node_id backfill from PostgreSQL to Valkey...")

    total_inserted = 0
    with postgres.transaction() as cur:
        cur.itersize = batch_size
        cur.execute("SELECT node_id FROM nodes")

        buffer: List[bytes] = []
        for row in cur:
            buffer.append(row[0])
            if len(buffer) >= batch_size:
                inserted = valkey.cache_node_ids_batch(buffer)
                total_inserted += inserted
                logger.info(f"Inserted batch of {inserted} node_ids (total: {total_inserted})")
                buffer.clear()

        if buffer:
            inserted = valkey.cache_node_ids_batch(buffer)
            total_inserted += inserted
            logger.info(f"Inserted final batch of {inserted} node_ids (total: {total_inserted})")

    logger.info(f"✅ Node_id backfill complete. Total inserted: {total_inserted}")

# --- Entry point ---
if __name__ == "__main__":
    logger = setup_logging()
    try:
        postgres = PostgreSQLDataStore(logger)
        valkey = ValkeyCache()
        backfill_node_ids(postgres, valkey, logger)
    finally:
        postgres.close()
