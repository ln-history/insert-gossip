import duckdb
from common import open_gossip_store, parse_gossip_store_header, setup_logging, write_message
import os
import logging
import hashlib
import datetime
import psycopg2
from datetime import datetime, timezone
import pandas as pd
from collections import defaultdict
import io
import sys

from typing import Optional, Iterator, BinaryIO, Tuple, Dict, Union
from logging import Logger
from pathlib import Path
import struct

from lnhistoryclient.constants import (
    HEADER_FORMAT,
    MSG_TYPE_GOSSIP_STORE_ENDED,
)
from lnhistoryclient.model.ChannelAnnouncement import ChannelAnnouncement
from lnhistoryclient.model.ChannelUpdate import ChannelUpdate
from lnhistoryclient.model.core_lightning_internal.ChannelDying import ChannelDying
from lnhistoryclient.model.NodeAnnouncement import NodeAnnouncement
from lnhistoryclient.parser.gossip_file import read_gossip_file
from lnhistoryclient.parser.parser import parse_node_announcement, parse_channel_announcement, parse_channel_update
from lnhistoryclient.parser.core_lightning_internal.parser import parse_channel_dying
from lnhistoryclient.parser.common import get_message_type_by_bytes, strip_known_message_type

from BlockchainRequester import get_amount_sat_by_tx_idx_and_output_idx, get_block_by_block_height

from lnhistoryclient.constants import MSG_TYPE_NODE_ANNOUNCEMENT, MSG_TYPE_CHANNEL_ANNOUNCEMENT, MSG_TYPE_CHANNEL_UPDATE, MSG_TYPE_CHANNEL_AMOUNT, MSG_TYPE_DELETE_CHANNEL, MSG_TYPE_GOSSIP_STORE_ENDED, MSG_TYPE_PRIVATE_CHANNEL_ANNOUNCEMENT, MSG_TYPE_PRIVATE_CHANNEL_UPDATE, MSG_TYPE_CHANNEL_DYING

from config import FILE_PATH, USE_SQLITE_DB, LOG_LEVEL, LN_HISTORY_DATABASE_CONNECTION_STRING, EXPLORER_RPC_URL, EXPLORER_RPC_PASSWORD

# Constants
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
VALID_MSG_TYPES = {
    MSG_TYPE_NODE_ANNOUNCEMENT, MSG_TYPE_CHANNEL_ANNOUNCEMENT, MSG_TYPE_CHANNEL_UPDATE, 
    MSG_TYPE_CHANNEL_DYING, MSG_TYPE_CHANNEL_AMOUNT, MSG_TYPE_DELETE_CHANNEL, 
    MSG_TYPE_GOSSIP_STORE_ENDED, MSG_TYPE_PRIVATE_CHANNEL_ANNOUNCEMENT, 
    MSG_TYPE_PRIVATE_CHANNEL_UPDATE
}
DB_PATH = "temp-db.duckdb"
BYTEA_COLUMNS="raw_gossip"

def split_scid(scid, logger: logging.Logger) -> tuple[int, int, int]:
    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid}. - Skipping further handling")
        return
    
def fetch_channel_timestamp_and_amount(scid: str, logger: logging.Logger) -> Optional[Tuple[int, int]]:
    """
    Returns timestamp of the block and the amount in satoshis of the channel announcement 
    """

    block_height, tx_idx, out_idx = split_scid(scid, logger)
    
    block = get_block_by_block_height(block_height, logger)
    if not block:
        logger.error(f"Could not get block at block_height: {block_height}")
        return

    timestamp_unix = block.get("time")
    block_timestamp = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

    txs = block.get("tx", [])
    if tx_idx >= len(txs):
        logger.error(f"Could not get transactions of block at height: {block_height}")
        return

    tx_id = txs[tx_idx]
    
    amount_sat = get_amount_sat_by_tx_idx_and_output_idx(tx_id, out_idx, logger)

    return (block_timestamp, amount_sat)

def split_gossip_file(
    input_file_name: str,
    output_paths: Dict[int, str],
    logger: logging.Logger,
    start_offset: int = 0
) -> Dict[str, int]:
    """
    Splits an input gossip_store file into multiple files grouped by msg_type.

    Args:
        input_file_name: Path to the gossip_store input file.
        output_paths: Dict mapping msg_type (int) to output file path (str).
        logger: Logger instance.
        start_offset: Number of messages to skip at the start.

    Returns:
        A dict mapping msg_type (or 'unknown') to counts of messages written.
    """
    
    # Get file size for logging
    try:
        file_size = os.path.getsize(input_file_name)
        logger.info(f"Starting phase 1 Iterating through the file: `{os.path.basename(input_file_name)}`.")
        logger.info("")
        logger.info(f"File has a size of `{file_size}` bytes")
        logger.info("")
    except OSError as e:
        logger.error(f"Could not get file size for {input_file_name}: {e}")
        file_size = "unknown"
        return

    message_info = {}
    msg_type_counts = {k: 0 for k in output_paths.keys()}
    msg_type_counts['unknown'] = 0

    # Log file creation
    for msg_type, path in output_paths.items():
        logger.info(f"Creating {path} ...")
    logger.info("")

    # Log splitting summary
    logger.info(f"Splitting the file `{os.path.basename(input_file_name)}` into the following files:")
    for msg_type, path in output_paths.items():
        logger.info(f"+ {os.path.basename(path)}")
    logger.info("")

    # Open all output files once (append mode)
    for msg_type, path in output_paths.items():
        f = open(path, "ab")  # Append binary mode
        message_info[msg_type] = {
            'file': f,
            'count': 0,
            'path': path,
        }

    count = 0
    logger.info(f"Starting to iterate through gossip {input_file_name} file. This might take a while.")
    for i, msg in enumerate(read_gossip_file(input_file_name, start=start_offset, logger=logger)):
        try:
            msg_type = get_message_type_by_bytes(msg)
            if msg_type in message_info:
                # This is one of our target message types - write to specific file
                write_message(message_info[msg_type]['file'], msg)
                message_info[msg_type]['count'] += 1
                msg_type_counts[msg_type] += 1

                if message_info[msg_type]['count'] % 1_000 == 0:
                    logger.debug(f"Written {message_info[msg_type]['count']} messages to {message_info[msg_type]['path']}")
            elif msg_type in VALID_MSG_TYPES:
                # Valid message type but not one we're splitting - just count it
                pass  # We don't increment any counter for these valid but uninteresting types
            else:
                # Unknown/invalid message type
                msg_type_counts['unknown'] += 1
                logger.warning(f"Unknown msg_type {msg_type} encountered, skipping message #{count}")
        except Exception as e:
            logger.warning(f"Could not classify message #{count}: {e}")
            msg_type_counts['unknown'] += 1
        count += 1
        

    logger.info(f"Finished iterating through `{os.path.basename(input_file_name)}` file")
    logger.info("")
    logger.info("Distribution of messages:")
    
    # Map message types to readable names for output
    msg_type_names = {
        MSG_TYPE_NODE_ANNOUNCEMENT: "node_announcements",
        MSG_TYPE_CHANNEL_ANNOUNCEMENT: "channel_announcement", 
        MSG_TYPE_CHANNEL_UPDATE: "channel_updates",
        MSG_TYPE_CHANNEL_DYING: "channel_dying"
    }
    
    for msg_type, count in msg_type_counts.items():
        if msg_type == 'unknown':
            logger.info(f"+ unable to parse: {count}")
        elif msg_type in msg_type_names:
            logger.info(f"+ {msg_type_names[msg_type]}: {count}")
        else:
            logger.info(f"+ {msg_type}: {count}")
    
    logger.info("")
    logger.info("Successfully finished phase 1")

    return msg_type_counts


def create_duckdb_database(db_path: str, logger: logging.Logger) -> Dict[str, str]:
    """
    Creates a DuckDB database with the required tables for gossip data.
    
    Args:
        db_path: Path where the DuckDB database should be created
        logger: Logger instance
        
    Returns:
        Dict containing database connection credentials/info
    """
    logger.info("Starting phase 2: Creating DuckDB database")
    logger.info("")
    
    try:
        # Connect to DuckDB (creates the database if it doesn't exist)
        conn = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB database: {db_path}")
        
        # Define the SQL statements for table creation
        table_definitions = [
            {
                'name': 'nodes',
                'sql': """
                    CREATE TABLE nodes (
                        node_id VARCHAR(66) PRIMARY KEY,
                        from_timestamp TIMESTAMPTZ NOT NULL,
                        last_seen TIMESTAMPTZ NOT NULL
                    );
                """
            },
            {
                'name': 'nodes_raw_gossip',
                'sql': """
                    CREATE TABLE nodes_raw_gossip (
                        gossip_id VARCHAR(64) PRIMARY KEY,
                        node_id VARCHAR(66) REFERENCES nodes(node_id),
                        timestamp TIMESTAMPTZ NOT NULL,
                        raw_gossip BYTEA NOT NULL
                    );
                """
            },
            {
                'name': 'channels',
                'sql': """
                    CREATE TABLE channels (
                        gossip_id VARCHAR(64),                  -- Can be NULL if channel_update of channel with missing announcement
                        scid VARCHAR(23) PRIMARY KEY,
                        source_node_id VARCHAR(66) REFERENCES nodes(node_id),
                        target_node_id VARCHAR(66) REFERENCES nodes(node_id),
                        from_timestamp TIMESTAMPTZ NOT NULL,
                        to_timestamp TIMESTAMPTZ,
                        amount_sat INT NOT NULL,
                        raw_gossip BYTEA                        -- Can be NULL if channel_update of channel with missing announcement
                    );
                """
            },
            {
                'name': 'channel_updates',
                'sql': """
                    CREATE TABLE channel_updates (
                    gossip_id VARCHAR(64) UNIQUE NOT NULL,          
                    scid VARCHAR(23) REFERENCES channels(scid) NOT NULL,
                    direction BIT NOT NULL,
                    from_timestamp TIMESTAMPTZ NOT NULL,
                    to_timestamp TIMESTAMPTZ,
                    raw_gossip BYTEA NOT NULL,
                    PRIMARY KEY (scid, direction, from_timestamp)
                );
                """
            }
        ]
        
        # Create each table
        for table_def in table_definitions:
            conn.execute(table_def['sql'])
            logger.info(f"Successfully created table: {table_def['name']}")
        
        # Close the connection
        conn.close()
        
        logger.info("")
        logger.info("Successfully finished phase 2: DuckDB database created")
        logger.info("")
        
    except Exception as e:
        logger.error(f"Failed to create DuckDB database: {e}")
        return {}


def process_node_announcements(file_path: str, total_count: int, logger: logging.Logger, batch_size: int = 1_000) -> None:
    """
    Processes node_announcement messages using optimized bulk operations.
    """

    logger.info(f"Starting phase 2a: node_announcements")

    raw_gossip_entries = []
    node_updates = {}

    # Process all data first (no intermediate DB calls)
    logger.info(f"Starting to iterate through gossip {file_path} file. This might take a while.")
    log_interval = max(1, total_count // 10)

    for i, msg in enumerate(read_gossip_file(file_path, start=0, logger=logger)):
        parsed: NodeAnnouncement = parse_node_announcement(strip_known_message_type(msg))
        node_id_hex: str = parsed.node_id.hex()
        gossip_id: str = hashlib.sha256(msg).hexdigest()
        timestamp_dt: datetime = datetime.fromtimestamp(parsed.timestamp, tz=timezone.utc)
        
        raw_gossip_entries.append((gossip_id, node_id_hex, timestamp_dt, msg))
        
        if node_id_hex not in node_updates:
            node_updates[node_id_hex] = {"min_ts": timestamp_dt, "max_ts": timestamp_dt}
        else:
            node_updates[node_id_hex]["min_ts"] = min(node_updates[node_id_hex]["min_ts"], timestamp_dt)
            node_updates[node_id_hex]["max_ts"] = max(node_updates[node_id_hex]["max_ts"], timestamp_dt)

        # Log progress every 10%
        if i % log_interval == 0 or i == total_count:
            percent = (i / total_count) * 100
            logger.info(f"Processed {i}/{total_count} node_announcement messages in parsing loop  ({percent:.0f}%).")
    
    # Bulk operation
    with duckdb.connect(DB_PATH) as conn:
        logger.info(f"Starting bulk insertion of {len(node_updates)} nodes and {len(raw_gossip_entries)} gossip entries")
        
        try:
            # Begin transaction for atomicity
            conn.begin()
            
            # 1. Insert nodes table with pre-calculated min/max timestamps
            nodes_data = [
                (node_id, timestamps["min_ts"], timestamps["max_ts"]) 
                for node_id, timestamps in node_updates.items()
            ]
            
            # Simple bulk insert - we already have the correct min/max values
            conn.executemany("""
                INSERT OR REPLACE INTO nodes (node_id, from_timestamp, last_seen) 
                VALUES (?, ?, ?)
            """, nodes_data)
            
            logger.debug(f"Processed {len(nodes_data)} node records")
            
            # 2. Insert raw gossip data in batches to handle large datasets
            total_gossip_entries = len(raw_gossip_entries)
            
            for i in range(0, total_gossip_entries, batch_size):
                batch = raw_gossip_entries[i:i + batch_size]
                
                # Use INSERT OR IGNORE for DuckDB
                conn.executemany("""
                    INSERT OR IGNORE INTO nodes_raw_gossip (gossip_id, node_id, timestamp, raw_gossip)
                    VALUES (?, ?, ?, ?)
                """, batch)
                
                # Log progress
                if (i + batch_size) % (batch_size * 10) == 0 or i + batch_size >= total_gossip_entries:
                    percent = (i / total_gossip_entries) * 100
                    logger.info(f"Processed nodes batch: {min(i + batch_size, total_gossip_entries)}/{total_gossip_entries} ({percent:.0f}%).")
            
            # Commit transaction
            conn.commit()
            
            # Check actual insertion counts
            nodes_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            gossip_count = conn.execute("SELECT COUNT(*) FROM nodes_raw_gossip").fetchone()[0]
            
            logger.info(f"Successfully processed all node_announcement data. Database now contains: {nodes_count} total nodes, {gossip_count} total gossip entries")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during bulk insertion: {e}")
            raise


def process_channel_announcements(file_path: str, total_count: int, logger: logging.Logger, batch_size: int = 1_000, use_sqlite_db = True) -> None:
    """
    Processes channel_announcement messages using optimized bulk operations.
    """

    logger.info(f"Starting phase 2b: channel_announcements")

    channel_entries = []
    node_updates = {}
    
    if use_sqlite_db:
        import sqlite3
        sqlite_path = "channels.sqlite"
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

    # Process all data first (no intermediate DB calls)
    logger.info(f"Starting to process channel_announcement messages. Please note that this takes some _extra_ time due to the Blockchain requests.")
    log_interval = max(1, total_count // 100)

    for i, msg in enumerate(read_gossip_file(file_path, start=0, logger=logger)):
        parsed: ChannelAnnouncement = parse_channel_announcement(strip_known_message_type(msg))
        
        # Extract basic information
        scid: str = parsed.scid_str
        gossip_id: str = hashlib.sha256(msg).hexdigest()
        source_node_id: str = parsed.node_id_1.hex()
        target_node_id: str = parsed.node_id_2.hex()
        raw_gossip: bytes = msg            

        if source_node_id >= target_node_id:
            logger.critical(
                f"Node order violation: {source_node_id.hex()} >= {target_node_id.hex()} for scid {scid}"
            )
            return

        if not use_sqlite_db:
            from_timestamp, amount_sat = fetch_channel_timestamp_and_amount(scid, logger)

            # Store channel entry
            channel_entries.append((
                gossip_id,
                scid, 
                source_node_id, 
                target_node_id, 
                from_timestamp, 
                None, 
                amount_sat, 
                raw_gossip
            ))
        
        else:    
            cursor.execute("SELECT * FROM channels WHERE scid = ?", (scid,))
            rows = cursor.fetchall()
            
            if len(rows) == 0:
                logger.info("Continueing, skipping further channel_announcements due to time")
                break

            from_timestamp = rows[0][4]
            amount_sat = rows[0][6]

            if not isinstance(amount_sat, int):
                logger.info(f"amount_sat is of type {type(amount_sat)} for scid {scid}. Not adding")
                logger.info(f"Whole channel_entry is {rows}")
            else:
                channel_entries.append((
                    gossip_id,
                    scid,
                    source_node_id,
                    target_node_id,
                    from_timestamp,
                    None,
                    amount_sat,
                    raw_gossip
                ))
        
        # Track node updates for both source and target nodes
        for node_id in [source_node_id, target_node_id]:
            if node_id not in node_updates:
                node_updates[node_id] = {"min_ts": from_timestamp, "max_ts": from_timestamp}
            else:
                node_updates[node_id]["min_ts"] = min(node_updates[node_id]["min_ts"], from_timestamp)
                node_updates[node_id]["max_ts"] = max(node_updates[node_id]["max_ts"], from_timestamp)

        if i == 0:
            logger.debug(f"Succesfully connected and retrieved data for first scid {scid}")

        # Log progress every 1%
        if i % log_interval == 0 or i == total_count:
            percent = (i / total_count) * 100
            logger.info(f"Processed {i}/{total_count} channel_update messages in parsing loop ({percent:.0f}%).")
    
    logger.info(f"Processed {len(channel_entries)} channel entries from file")
    
    
    with duckdb.connect(DB_PATH) as conn:
        logger.info(f"Starting bulk insertion of new {len(node_updates)} nodes and {len(channel_entries)} channel entries")
        
        try:
            # Begin transaction for atomicity
            conn.begin()
            
            # 1. Insert/update nodes table first (to satisfy foreign key constraints)
            if node_updates:
                nodes_data = [
                    (node_id, timestamps["min_ts"], timestamps["max_ts"]) 
                    for node_id, timestamps in node_updates.items()
                ]
                
                # Insert or update nodes with pre-calculated min/max timestamps
                conn.executemany("""
                    INSERT OR REPLACE INTO nodes (node_id, from_timestamp, last_seen) 
                    VALUES (?, ?, ?)
                """, nodes_data)
                
                logger.info(f"Processed {len(nodes_data)} node records")
            
            # 2. Insert channel data in batches to handle large datasets
            total_channel_entries = len(channel_entries)
            
            for i in range(0, total_channel_entries, batch_size):
                batch = channel_entries[i:i + batch_size]
                
                # Use INSERT OR REPLACE for channels (assuming we want to update if scid exists)
                conn.executemany("""
                    INSERT OR REPLACE INTO channels (gossip_id, scid, source_node_id, target_node_id, from_timestamp, to_timestamp, amount_sat, raw_gossip)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                
                # Log progress
                if (i + batch_size) % (batch_size * 10) == 0 or i + batch_size >= total_channel_entries:
                    percent = (i / total_channel_entries) * 100
                    logger.info(f"Processed channel batch: {min(i + batch_size, total_channel_entries)}/{total_channel_entries} ({percent:.0f}%)")
            
            # Commit transaction
            conn.commit()
            
            # Check actual insertion counts
            nodes_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            channels_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
            
            logger.info(f"Successfully processed all channel_announcement data. Database now contains: {nodes_count} total nodes, {channels_count} total channels")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during bulk insertion: {e}")
            raise


def process_channel_updates(file_path: str, total_count: int, logger: logging.Logger, batch_size: int = 1_000, use_sqlite_db = True) -> None:
    """
    Processes channel_update messages using optimized bulk operations.
    - Parse all data first
    - Ensure channels table contains all required SCIDs
    - Calculate the `to_timestamp` value per (scid, direction)
    - Bulk insert into channel_updates table
    """
    logger.info("Starting phase 2c: channel_updates")

    updates_by_key = defaultdict(list)  # group by (scid, direction)
    unique_scids = set()

    # Step 1: Parse all messages
    logger.info(f"Starting to iterate through gossip {file_path} file. This might take a while.")
    log_interval = max(1, total_count // 10)
    try:
        for i, msg in enumerate(read_gossip_file(file_path, start=0, logger=logger)):
            parsed: ChannelUpdate = parse_channel_update(strip_known_message_type(msg))
            
            gossip_id = hashlib.sha256(msg).hexdigest()
            scid = parsed.scid_str
            direction = bool(parsed.direction)
            from_timestamp = datetime.fromtimestamp(parsed.timestamp, tz=timezone.utc)
            
            updates_by_key[(scid, direction)].append(
                (gossip_id, scid, direction, from_timestamp, None, msg)
            )
            unique_scids.add(scid)

            # Log progress every 10%
            if i % log_interval == 0 or i == total_count:
                percent = (i / total_count) * 100
                logger.info(f"Processed {i}/{total_count} channel_update messages in parsing loop  ({percent:.0f}%).")
    except Exception as e:
        logger.error(f"Error during parsing of channel_update messages.")

    if use_sqlite_db:
        import sqlite3
        sqlite_path = "channels.sqlite"
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

    # Step 2: Ensure all required channels exist
    try:
        with duckdb.connect(DB_PATH) as conn:
            # Query which SCIDs already exist in channels
            placeholders = ",".join("?" for _ in unique_scids) or "NULL"
            existing_scids = set()
            if unique_scids:
                query = f"SELECT scid FROM channels WHERE scid IN ({placeholders})"
                rows = conn.execute(query, list(unique_scids)).fetchall()
                existing_scids = {row[0] for row in rows}

            missing_scids = unique_scids - existing_scids
            
            if missing_scids:
                logger.info(f"{len(missing_scids)} channels missing. Fetching and inserting...")
                log_interval = max(1, len(missing_scids) // 100)

                new_channels = []
                misses = 0
                for i, scid in enumerate(missing_scids):
                    if not use_sqlite_db:
                        from_timestamp, amount_sat = fetch_channel_timestamp_and_amount(scid, logger)
                    else:
                        cursor.execute("SELECT * FROM channels WHERE scid = ?", (scid,))
                        rows = cursor.fetchall()
                    
                        if len(rows) == 0 and misses < 2:
                            logger.warning(f"Found channel_update of channel with scid {scid} that has not announced earlier.")
                            from_timestamp, amount_sat = fetch_channel_timestamp_and_amount(scid, logger)
                            misses += 1
                        else:
                            from_timestamp = rows[0][4]
                            amount_sat = rows[0][6]

                        if not isinstance(amount_sat, int):
                            logger.info(f"amount_sat is of type {type(amount_sat)} for scid {scid}. Not adding")
                            logger.info(f"Whole channel_entry is {rows}")
                        else:
                            new_channels.append((
                                gossip_id,
                                scid,
                                None,
                                None,
                                from_timestamp,
                                None,
                                amount_sat,
                                None
                            ))
                    
                    if i % log_interval == 0 or i == len(missing_scids):
                        percent = (i / len(missing_scids)) * 100
                        logger.info(f"Requested {i}/{len(missing_scids)} missing scids from blockchain ({percent:.0f}%).")

                    new_channels.append((None, scid, None, None, from_timestamp, None, amount_sat, None))
                
                if new_channels:
                    conn.executemany("""
                        INSERT OR IGNORE INTO channels (gossip_id, scid, source_node_id, target_node_id, from_timestamp, to_timestamp, amount_sat, raw_gossip)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, new_channels)
                    logger.info(f"Inserted {len(new_channels)} new channels into channels table")
    except Exception as e:
        logger.error(f"Error during check that all channel_updates have an channel_announcement: {e}")


    # Step 3: Calculate to_timestamp within each (scid, direction) group
    logger.info(f"Calculating `to_timestamp` of channel_updates by grouping into (scid, direction). This might take a while. ")
    channel_updates = []
    for key, records in updates_by_key.items():
        records.sort(key=lambda r: r[3])  # sort by from_timestamp
        for i in range(len(records)):
            if i < len(records) - 1:
                to_ts = records[i + 1][3]
            else:
                to_ts = None
            gossip_id, scid, direction, from_ts, _, msg = records[i]
            channel_updates.append((gossip_id, scid, direction, from_ts, to_ts, msg))

    # TEMP: Remove all channel_updates from channel_updates list if scid does not exist in channel_announcements
    with duckdb.connect(DB_PATH) as conn:
        # Fetch all existing SCIDs from channels table
        existing_scids = set(row[0] for row in conn.execute("SELECT scid FROM channels").fetchall())

        # Keep only updates whose scid exists
        filtered_channel_updates = [cu for cu in channel_updates if cu[1] in existing_scids]

        logger.info(f"Filtered channel_updates: {len(channel_updates)} -> {len(filtered_channel_updates)} after removing updates with missing channels")
        channel_updates = filtered_channel_updates

    # Step 4: Insert all channel_updates in bulk
    with duckdb.connect(DB_PATH) as conn:
        logger.info(f"Starting bulk insertion of {len(channel_updates)} channel_update entries")

        try:
            conn.begin()
            for i in range(0, len(channel_updates), batch_size):
                batch = channel_updates[i:i + batch_size]
                conn.executemany("""
                    INSERT OR IGNORE INTO channel_updates 
                    (gossip_id, scid, direction, from_timestamp, to_timestamp, raw_gossip)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, batch)

                if (i + batch_size) % (batch_size * 10) == 0 or i + batch_size >= len(channel_updates):
                    percent = (i / len(channel_updates)) * 100
                    logger.info(f"Processed channel_update batch: {min(i + batch_size, len(channel_updates))}/{len(channel_updates)} ({percent:.0f}%).")

            conn.commit()

            updates_count = conn.execute("SELECT COUNT(*) FROM channel_updates").fetchone()[0]
            logger.info(f"Successfully processed channel_update data. Database now contains {updates_count} total channel_updates")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error during bulk insertion: {e}")
            raise

        

def process_channel_dying(file_path: str, total_count: int, logger: logging.Logger, batch_size: int = 1_000) -> None:
    # TODO: Likely incorrect writing of channel_dying messages -> cannot be read consistently    
    """
    Set `to_timestamp` of channels based on the block_height of closing transactions (channel_dying messages).
    Logs missing channels if SCID not in channels table.
    """

    if total_count == 0:
        logger.info("No channel_dying messages in gossip file found - Skipping this phase.")
        return

    logger.info("")
    logger.info("Starting phase 2d: channel_dying")
    logger.info(f"Iterating through gossip file {file_path}. This may take extra time due to blockchain requests.")

    log_interval = max(1, total_count // 10)

    try:
        with duckdb.connect(DB_PATH) as conn:
            # Fetch all existing SCIDs upfront to avoid repeated queries
            existing_scids = set(row[0] for row in conn.execute("SELECT scid FROM channels").fetchall())

            for i, msg in enumerate(read_gossip_file(file_path, start=0, logger=logger)):
                if i == 0:
                    msg = msg[4:]
                channel_dying: ChannelDying = parse_channel_dying(strip_known_message_type(msg))
                scid: str = channel_dying.scid_str
                
                if scid not in existing_scids:
                    logger.warning(f"Found channel_dying message for channel with scid {scid} that is not in channels table - Skipping.")
                    continue

                
                block_height_msg = channel_dying.blockheight
                block_height_scid, _, _ = split_scid(scid, logger)

                if block_height_msg != block_height_scid:
                    logger.error(f"Conflicting block_height for channel_dying message {channel_dying.to_dict()}")

                block = get_block_by_block_height(block_height_msg, logger)
                if not block:
                    logger.error(f"Could not get block at block_height: {block_height_msg} - Skipping message.")
                    continue

                timestamp_unix = block.get("time")
                if not timestamp_unix:
                    logger.error(f"Could not get timestamp of block at block_height {block_height_msg} - Skipping message.")
                    continue

                block_timestamp = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

                # Update to_timestamp
                conn.execute(
                    "UPDATE channels SET to_timestamp = ? WHERE scid = ?",
                    (block_timestamp, scid)
                )

                # Log progress
                if i % log_interval == 0 or i == total_count:
                    percent = (i / total_count) * 100
                    logger.info(f"Processed {i}/{total_count} channel_dying messages ({percent:.0f}%).")

    except Exception as e:
        logger.exception(f"Error processing channel_dying messages: {e}")

    logger.info(f"Successfully processed channely_dying data.")

def update_channel_to_timestamp(logger: logging.Logger):
    """
    Set the `to_timestamp` of every channel to the latest channel_update.from_timestamp + 14 days
    — but only if that calculated date is lower than the most recent timestamp in the database.
    """

    logger.info("")
    logger.info("Starting phase 2e: Updating `to_timestamp` of channels")
    
    with duckdb.connect(DB_PATH) as conn:
        # 1. Get most recent timestamps from each table (DuckDB returns datetime objects)
        max_ts_nodes_raw_gossip = conn.execute(
            "SELECT MAX(timestamp) FROM nodes_raw_gossip;"
        ).fetchone()[0]

        max_ts_channels = conn.execute(
            "SELECT MAX(from_timestamp) FROM channels;"
        ).fetchone()[0]

        max_ts_channel_updates = conn.execute(
            "SELECT MAX(from_timestamp) FROM channel_updates;"
        ).fetchone()[0]

        # Handle case where some tables may be empty (None)
        timestamps = [ts for ts in [max_ts_nodes_raw_gossip, max_ts_channels, max_ts_channel_updates] if ts is not None]
        if not timestamps:
            logger.warning("No timestamps found in database — skipping update.")
            return

        oldest_timestamp = min(timestamps)
        most_recent_timestamp = max(timestamps)
        logger.info(f"Oldest and most recent timestamps: {str(oldest_timestamp), str(most_recent_timestamp)}")

        # 2. Perform the update:
        # For each channel, find its latest channel_update.from_timestamp,
        # add 14 days, and update to_timestamp.
        # If that value exceeds most_recent_timestamp, cap it.
        conn.execute("""
            UPDATE channels
            SET to_timestamp = LEAST(
                (
                    SELECT from_timestamp + INTERVAL 14 DAYS
                    FROM channel_updates cu
                    WHERE cu.scid = channels.scid
                    ORDER BY from_timestamp DESC
                    LIMIT 1
                ),
                ?
            )
        """, [most_recent_timestamp])

        logger.info("Finished updating to_timestamp for channels.")



def export_data_to_postgres(logger: logging.Logger):
    logger.info("Starting export from DuckDB to Postgres...")
    
    # Connect to Postgres
    pg_conn = psycopg2.connect(LN_HISTORY_DATABASE_CONNECTION_STRING)
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    with duckdb.connect(DB_PATH) as dconn:
        # for table in ["nodes", "nodes_raw_gossip", "channels", "channel_updates"]:
        for table in ["channel_updates"]:
            logger.info(f"Fetching {table} from DuckDB...")
            df = dconn.execute(f"SELECT * FROM {table}").fetchdf()  # pandas dataframe
            
            # --- Get column order from Postgres ---
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table}'
                ORDER BY ordinal_position
            """)
            pg_columns = [row[0] for row in cur.fetchall()]
            
            # --- Reorder DataFrame columns to match Postgres ---
            df = df[pg_columns]

            # --- Serialize rows to COPY buffer ---
            output = io.StringIO()
            for row in df.itertuples(index=False, name=None):
                fields = []
                for colname, val in zip(df.columns, row):
                    if val is None or pd.isna(val):
                        fields.append(r'\N')
                    elif colname == "direction":
                        fields.append('1' if bool(val) else '0')
                    elif isinstance(val, bytearray):
                        # Force bytea-hex format
                        fields.append(r'\x' + val.hex())
                    elif colname in BYTEA_COLUMNS and isinstance(val, str):
                        # In case DuckDB gives you hex as str
                        fields.append(r'\x' + val.lower())
                    else:
                        fields.append(str(val))
                output.write('\t'.join(fields) + '\n')
            output.seek(0)

            # --- 1. Create staging table ---
            staging_table = f"{table}_staging"
            logger.info(f"Creating staging table {staging_table}...")
            cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
            cur.execute(f"""
                CREATE TEMP TABLE {staging_table} 
                (LIKE {table} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY)
            """)

            # Ensure no constraints (TEMP rarely inherit constraints but we enforce it)
            cur.execute(f"""
                DO $$
                DECLARE r RECORD;
                BEGIN
                    FOR r IN 
                        SELECT conname 
                        FROM pg_constraint 
                        WHERE conrelid = '{staging_table}'::regclass 
                    LOOP
                        EXECUTE 'ALTER TABLE {staging_table} DROP CONSTRAINT ' || r.conname;
                    END LOOP;
                END$$;
            """)

            # --- 2. COPY into staging table with explicit column list ---
            logger.info(f"Copying {table} into staging table ({len(df)} rows)...")
            cur.copy_from(output, staging_table, columns=pg_columns, null='\\N', sep='\t')

            # --- 3. Insert into real table with ON CONFLICT DO NOTHING ---
            col_list = ", ".join(pg_columns)
            insert_sql = f"""
                INSERT INTO {table} ({col_list})
                SELECT {col_list} FROM {staging_table}
                ON CONFLICT DO NOTHING
                RETURNING 1;
            """
            logger.info(f"Inserting from staging table into {table} with conflict handling...")
            cur.execute(insert_sql)
            inserted_count = cur.rowcount  # number of actually inserted rows

            # Count total rows in staging (to calculate skipped rows)
            cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
            total_rows = cur.fetchone()[0]
            skipped_count = total_rows - inserted_count

            logger.info(f"Table {table}: inserted {inserted_count}, skipped {skipped_count} duplicates")
    
    cur.close()
    pg_conn.close()
    logger.info("Export completed successfully.")


if __name__ == "__main__":
    logger = setup_logging(LOG_LEVEL)

    output_paths = {
        MSG_TYPE_CHANNEL_ANNOUNCEMENT: FILE_PATH + "-channel_announcements.gsp",
        MSG_TYPE_NODE_ANNOUNCEMENT: FILE_PATH + "-node_announcements.gsp",
        MSG_TYPE_CHANNEL_UPDATE: FILE_PATH + "-channel_update.gsp",
        MSG_TYPE_CHANNEL_DYING: FILE_PATH + "-channel_dying.gsp",
    }

    logger.info("The `insert-gossip` service started.")
    logger.info("")
    logger.info("Press CTRL+C or `docker compose down` to stop it.")
    logger.info("")
    logger.info("")
    logger.info("=="*50)
    logger.info("")
    logger.info("")

    # # Phase 1: Splitting
    msg_type_count = split_gossip_file(FILE_PATH, output_paths, logger)
    if not msg_type_count:
        logger.error("Terminated due to error when splitting the file")
        sys.exit()

    # Phase 2: Setup DuckDB
    logger.info("")
    logger.info("")
    logger.info("=="*50)
    logger.info("")
    logger.info("")
    logger.info(f"Starting phase 2 Setting up {DB_PATH} database.")
    logger.info("")

    create_duckdb_database(DB_PATH, logger)

    # Phase 2a: node_announcements
    process_node_announcements(output_paths[MSG_TYPE_NODE_ANNOUNCEMENT], msg_type_count[MSG_TYPE_NODE_ANNOUNCEMENT],logger)
    logger.info(f"")

    # Phase 2b: channel_announcements
    process_channel_announcements(output_paths[MSG_TYPE_CHANNEL_ANNOUNCEMENT], msg_type_count[MSG_TYPE_CHANNEL_ANNOUNCEMENT], logger, use_sqlite_db=USE_SQLITE_DB)
    logger.info("")

    # Phase 2c: channel_updates
    process_channel_updates(output_paths[MSG_TYPE_CHANNEL_UPDATE], msg_type_count[MSG_TYPE_CHANNEL_UPDATE], logger, use_sqlite_db=USE_SQLITE_DB)
    logger.info("")

    # Phase 2d: channel_dying
    # process_channel_dying(output_paths[MSG_TYPE_CHANNEL_DYING], msg_type_count[MSG_TYPE_CHANNEL_DYING], logger)
    logger.info(f"Currently not handling any channel_dying messages.")
    logger.info(f"")

    # Phase 2e
    update_channel_to_timestamp(logger)
    logger.info(f"")


    # Phase 3: Export data
    logger.info("")
    logger.info("")
    logger.info("=="*50)
    logger.info("")
    logger.info("")
    logger.info(f"Starting phase 3 Exporting data to postgres database.")
    logger.info("")
    export_data_to_postgres(logger)
    logger.info("")

    logger.info("Sucessfully finished `insert-gossip` service.")