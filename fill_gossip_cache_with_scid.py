import logging
import os
from typing import List
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

from psycopg2 import pool, sql
from valkey import Valkey

from datetime import datetime

from config import POSTGRES_DB_NAME, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER, VALKEY_HOST, VALKEY_PASSWORD, VALKEY_PORT

# --- Config ---
BATCH_SIZE = 1000


# --- Logger ---
def setup_logging(log_dir: str = "logs", log_file_base: str = "fill_gossip_cache_with_scid") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("gossip-backfill")
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


# --- ValkeyCache class ---
class ValkeyCache:
    def __init__(self) -> None:
        self.client = Valkey(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD, db=0)
    
    def cache_scid_batch(self, scids: List[str], batch_size: int = 500) -> int:
        total_queued = 0
        for i in range(0, len(scids), batch_size):
            batch = scids[i:i + batch_size]
            pipe = self.client.pipeline()
            for scid in batch:
                key = f"scid:{scid}"
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


# --- Main backfill process --

def fill_scid(postgres: PostgreSQLDataStore, valkey: ValkeyCache, logger: logging.Logger, batch_size: int = BATCH_SIZE) -> None:
    logger.info("Starting scid fill from PostgreSQL to Valkey...")

    total_inserted = 0
    with postgres.transaction() as cur:
        cur.itersize = batch_size
        cur.execute("SELECT scid FROM channels")

        buffer: List[bytes] = []
        for row in cur:
            buffer.append(row[0])
            if len(buffer) >= batch_size:
                inserted = valkey.cache_scid_batch(buffer)
                total_inserted += inserted
                logger.info(f"Inserted batch of {inserted} scid (total: {total_inserted})")
                buffer.clear()

        if buffer:
            inserted = valkey.cache_scid_batch(buffer)
            total_inserted += inserted
            logger.info(f"Inserted final batch of {inserted} scids (total: {total_inserted})")

    logger.info(f"✅ Scid fill complete. Total inserted: {total_inserted}")

# --- Entry point ---
if __name__ == "__main__":
    logger = setup_logging()
    try:
        postgres = PostgreSQLDataStore(logger)
        valkey = ValkeyCache()
        fill_scid(postgres, valkey, logger)
    finally:
        postgres.close()
