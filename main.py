import os
import sys
import logging
import hashlib
import duckdb
import psycopg
import time
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge

# lnhistoryclient imports
from lnhistoryclient.parser.gossip_file import read_gossip_file
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.parser.parser import parse_channel_announcement, parse_node_announcement, parse_channel_update

# --- CONFIGURATION ---
COLLECTOR_ID = "020000000000000000000000000000000000000000000000000000000000000000"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
GOSSIP_FILE_PATH = os.getenv("GOSSIP_FILE_PATH")
POSTGRES_URI = os.getenv("POSTGRES_URI")
DUCK_DB_PATH = "import_staging.duckdb"
METRICS_PORT = 8002
BATCH_SIZE = 50000

# --- OBSERVABILITY ---
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)
logger = logging.getLogger("importer")

METRIC_STAGED = Counter('gossip_import_staged_total', 'Total messages staged', ['type'])
METRIC_SYNCED = Counter('gossip_import_synced_total', 'Total rows synced', ['table'])
METRIC_PHASE = Gauge('gossip_import_phase', 'Current phase: 0=Idle, 1=Staging, 2=Reporting, 3=Syncing')

# --- HELPERS ---
def get_scid_int(scid_str):
    if not scid_str: return None
    # If it's already an int (from parser), return it
    if isinstance(scid_str, int): return scid_str
    # If string '800x1x1'
    if isinstance(scid_str, str) and 'x' in scid_str:
        try:
            b, t, o = map(int, scid_str.split('x'))
            return (b << 40) | (t << 16) | o
        except:
            return None
    return None

def _strip_varint_len(data: bytes) -> bytes:
    if len(data) < 1: return data
    first = data[0]
    if first < 0xfd: return data[1:]
    elif first == 0xfd: return data[3:]
    elif first == 0xfe: return data[5:]
    elif first == 0xff: return data[9:]
    return data

def setup_duckdb():
    if os.path.exists(DUCK_DB_PATH):
        try: os.remove(DUCK_DB_PATH)
        except: pass
    
    con = duckdb.connect(DUCK_DB_PATH)
    
    # Tables matching your Postgres Schema EXACTLY in content requirements
    con.execute("""
        CREATE TABLE stage_inventory (gossip_id VARCHAR, type INT, first_seen_at TIMESTAMP);
        CREATE TABLE stage_observations (gossip_id VARCHAR, collector_node_id VARCHAR, seen_at TIMESTAMP);
        
        CREATE TABLE stage_nodes (node_id VARCHAR, first_seen TIMESTAMP, last_seen TIMESTAMP);
        
        -- Matches 'channels' table structure
        CREATE TABLE stage_channels (
            gossip_id VARCHAR, scid BIGINT, 
            source_node_id VARCHAR, target_node_id VARCHAR,
            node_sig_1 VARCHAR, node_sig_2 VARCHAR, 
            btc_sig_1 VARCHAR, btc_sig_2 VARCHAR,
            features BLOB, chain_hash VARCHAR, 
            btc_key_1 VARCHAR, btc_key_2 VARCHAR,
            raw_gossip BLOB
        );
        
        -- Matches 'node_announcements' table structure
        CREATE TABLE stage_node_announcements (
            gossip_id VARCHAR, node_id VARCHAR,
            valid_from TIMESTAMP,
            signature VARCHAR, features BLOB,
            rgb_color VARCHAR, alias VARCHAR,
            raw_gossip BLOB
        );
        
        -- Matches 'node_addresses' table structure
        CREATE TABLE stage_node_addresses (
            gossip_id VARCHAR, type_id INT, address VARCHAR, port INT
        );

        -- Matches 'channel_updates' table structure
        CREATE TABLE stage_channel_updates (
            gossip_id VARCHAR, scid BIGINT, direction INT,
            valid_from TIMESTAMP, 
            signature VARCHAR, chain_hash VARCHAR,
            msg_flags INT, chan_flags INT,
            cltv INT, htlc_min UBIGINT, htlc_max UBIGINT,
            fee_base BIGINT, fee_prop BIGINT,
            raw_gossip BLOB
        );
    """)
    return con

def parse_and_stage(con):
    METRIC_PHASE.set(1)
    
    buf_inv = []
    buf_obs = []
    buf_node = []
    buf_chan = []
    buf_nann = []
    buf_addr = []
    buf_upd = []

    logger.info(f"📖 Reading {GOSSIP_FILE_PATH}...")
    count = 0
    import_time = datetime.now(timezone.utc)

    for raw_with_varint in read_gossip_file(GOSSIP_FILE_PATH, logger=logger):
        count += 1
        if count % 50000 == 0:
            logger.info(f"Parsed {count} messages...")

        try:
            # 1. Hashing & Type Extraction
            gossip_id = hashlib.sha256(raw_with_varint).hexdigest()
            type_bytes = raw_with_varint[:2]
            msg_type = int.from_bytes(type_bytes, 'big')
            
            METRIC_STAGED.labels(type=str(msg_type)).inc()

            buf_inv.append((gossip_id, msg_type, import_time))
            buf_obs.append((gossip_id, COLLECTOR_ID, import_time))

            body_only = strip_known_message_type(raw_with_varint)
            
            if msg_type == 256: # Channel Ann
                # Use parser directly to avoid to_dict() overhead for speed
                p = parse_channel_announcement(body_only)
                
                # SCID might be int or string depending on version, handle both
                raw_scid = p.scid
                scid_int = get_scid_int(raw_scid)
                
                n1 = p.node_id_1.hex()
                n2 = p.node_id_2.hex()
                
                buf_node.append((n1, import_time, import_time))
                buf_node.append((n2, import_time, import_time))

                buf_chan.append((
                    gossip_id, scid_int,
                    n1, n2,
                    p.node_signature_1.hex(), p.node_signature_2.hex(),
                    p.bitcoin_signature_1.hex(), p.bitcoin_signature_2.hex(),
                    p.features, # BLOB
                    p.chain_hash.hex(),
                    p.bitcoin_key_1.hex(), p.bitcoin_key_2.hex(),
                    raw_with_varint
                ))

            elif msg_type == 257: # Node Ann
                p = parse_node_announcement(body_only)
                ts = datetime.fromtimestamp(p.timestamp, timezone.utc)
                nid = p.node_id.hex()
                
                buf_node.append((nid, ts, ts))

                buf_nann.append((
                    gossip_id, nid, ts,
                    p.signature.hex(), p.features, # BLOB
                    p.rgb_color.hex(), 
                    p.alias.decode('utf-8', errors='replace').strip('\x00'), 
                    raw_with_varint
                ))
                
                # Normalize Addresses
                # Using _parse_addresses() from your library to get objects
                for addr in p._parse_addresses():
                    # addr is Address object with typ (AddressType object)
                    t_id = addr.typ.id if addr.typ else None
                    if t_id:
                        buf_addr.append((gossip_id, t_id, addr.addr, addr.port))

            elif msg_type == 258: # Channel Update
                p = parse_channel_update(body_only)
                raw_scid = p.scid
                scid_int = get_scid_int(raw_scid)
                ts = datetime.fromtimestamp(p.timestamp, timezone.utc)
                
                # Flag handling (bytes to int)
                c_flags = p.channel_flags
                if isinstance(c_flags, bytes): c_flags = int.from_bytes(c_flags, 'big')
                
                m_flags = p.message_flags
                if isinstance(m_flags, bytes): m_flags = int.from_bytes(m_flags, 'big')

                direction = c_flags & 1
                
                buf_upd.append((
                    gossip_id, scid_int, direction, ts,
                    p.signature.hex(), p.chain_hash.hex(),
                    m_flags, c_flags,
                    p.cltv_expiry_delta, 
                    p.htlc_minimum_msat, p.htlc_maximum_msat,
                    p.fee_base_msat, p.fee_proportional_millionths,
                    raw_with_varint
                ))

            # Flush
            if len(buf_inv) >= BATCH_SIZE:
                con.executemany("INSERT INTO stage_inventory VALUES (?, ?, ?)", buf_inv); buf_inv = []
                con.executemany("INSERT INTO stage_observations VALUES (?, ?, ?)", buf_obs); buf_obs = []
                if buf_node: con.executemany("INSERT INTO stage_nodes VALUES (?, ?, ?)", buf_node); buf_node = []
                if buf_chan: con.executemany("INSERT INTO stage_channels VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", buf_chan); buf_chan = []
                if buf_nann: con.executemany("INSERT INTO stage_node_announcements VALUES (?, ?, ?, ?, ?, ?, ?, ?)", buf_nann); buf_nann = []
                if buf_addr: con.executemany("INSERT INTO stage_node_addresses VALUES (?, ?, ?, ?)", buf_addr); buf_addr = []
                if buf_upd:  con.executemany("INSERT INTO stage_channel_updates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", buf_upd); buf_upd = []
            
        except Exception as e:
            continue

    # Final Flush
    if buf_inv: con.executemany("INSERT INTO stage_inventory VALUES (?, ?, ?)", buf_inv)
    if buf_obs: con.executemany("INSERT INTO stage_observations VALUES (?, ?, ?)", buf_obs)
    if buf_node: con.executemany("INSERT INTO stage_nodes VALUES (?, ?, ?)", buf_node)
    if buf_chan: con.executemany("INSERT INTO stage_channels VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", buf_chan)
    if buf_nann: con.executemany("INSERT INTO stage_node_announcements VALUES (?, ?, ?, ?, ?, ?, ?, ?)", buf_nann)
    if buf_addr: con.executemany("INSERT INTO stage_node_addresses VALUES (?, ?, ?, ?)", buf_addr)
    if buf_upd:  con.executemany("INSERT INTO stage_channel_updates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", buf_upd)
    
    logger.info(f"✅ Parsing complete. {count} messages staged.")

def print_analysis_report(con):
    METRIC_PHASE.set(2)
    logger.info("\n" + "="*60)
    logger.info("📊 IMPORT ANALYSIS REPORT")
    logger.info("="*60)
    res = con.execute("SELECT type, COUNT(*) FROM stage_inventory GROUP BY type ORDER BY type").fetchall()
    for row in res:
        logger.info(f"Type {row[0]}: {row[1]:,}")
    logger.info("="*60 + "\n")

def register_import_collector(pg):
    with pg.cursor() as cur:
        cur.execute("""
            INSERT INTO collectors (node_id, alias, notes)
            VALUES (%s, 'Gossip File Import', 'Batch import from file')
            ON CONFLICT (node_id) DO NOTHING
        """, (COLLECTOR_ID,))
        pg.commit()

def safe_bulk_insert(pg_conn, table_name, columns, select_sql, pk_col="gossip_id"):
    logger.info(f"📦 Syncing table: {table_name}...")
    with pg_conn.cursor() as cur:
        staging_table = f"temp_stage_{table_name}"
        # Create temp table matching structure
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS)")
        
        with cur.copy(f"COPY {staging_table} ({', '.join(columns)}) FROM STDIN") as copy:
            local_con = duckdb.connect(DUCK_DB_PATH, read_only=True)
            res = local_con.execute(select_sql)
            while True:
                chunk = res.fetchmany(50000)
                if not chunk: break
                for row in chunk: copy.write_row(row)
                METRIC_SYNCED.labels(table=table_name).inc(len(chunk))
        
        conflict_action = "DO NOTHING"
        if table_name == "nodes":
            conflict_action = """
                DO UPDATE SET 
                    last_seen = GREATEST(nodes.last_seen, EXCLUDED.last_seen),
                    first_seen = LEAST(nodes.first_seen, EXCLUDED.first_seen)
            """
        
        cur.execute(f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {staging_table}
            ON CONFLICT ({pk_col}) {conflict_action}
        """)
        logger.info(f"   -> Merged rows into {table_name}")
        cur.execute(f"DROP TABLE {staging_table}")
        pg_conn.commit()

def sync_to_postgres():
    METRIC_PHASE.set(3)
    pg = psycopg.connect(POSTGRES_URI)
    register_import_collector(pg)
    
    # 1. Inventory
    safe_bulk_insert(pg, "gossip_inventory", 
        ["gossip_id", "type", "first_seen_at"],
        "SELECT DISTINCT gossip_id, type, first_seen_at FROM stage_inventory"
    )

    # 2. Nodes
    safe_bulk_insert(pg, "nodes",
        ["node_id", "first_seen", "last_seen"],
        "SELECT node_id, MIN(first_seen), MAX(last_seen) FROM stage_nodes GROUP BY node_id",
        pk_col="node_id"
    )

    # 3. Channels
    # MATCHES POSTGRES SCHEMA ORDER
    safe_bulk_insert(pg, "channels",
        ["gossip_id", "scid", "source_node_id", "target_node_id", 
         "node_signature_1", "node_signature_2", "bitcoin_signature_1", "bitcoin_signature_2", 
         "features", "chain_hash", "bitcoin_key_1", "bitcoin_key_2", "raw_gossip"],
        """
        SELECT 
            gossip_id, scid, source_node_id, target_node_id, 
            node_sig_1, node_sig_2, btc_sig_1, btc_sig_2, 
            features, chain_hash, btc_key_1, btc_key_2, raw_gossip
        FROM stage_channels
        """,
        pk_col="scid"
    )

    # 4. Node Announcements (With SCD2 valid_to calc)
    safe_bulk_insert(pg, "node_announcements",
        ["gossip_id", "node_id", "valid_from", "valid_to", "signature", "features", "rgb_color", "alias", "raw_gossip"],
        """
        SELECT 
            gossip_id, node_id, valid_from,
            LEAD(valid_from) OVER (PARTITION BY node_id ORDER BY valid_from ASC) as valid_to,
            signature, features, rgb_color, alias, raw_gossip
        FROM stage_node_announcements
        """
    )
    
    # 5. Node Addresses
    logger.info("📦 Syncing Node Addresses...")
    with pg.cursor() as cur:
        # Straight copy, conflicts handled by IDs naturally or minimal overlap risk in history
        with cur.copy("COPY node_addresses (gossip_id, type_id, address, port) FROM STDIN") as copy:
             local_con = duckdb.connect(DUCK_DB_PATH, read_only=True)
             res = local_con.execute("SELECT gossip_id, type_id, address, port FROM stage_node_addresses")
             while True:
                chunk = res.fetchmany(50000)
                if not chunk: break
                for row in chunk: copy.write_row(row)
        logger.info("   -> Node Addresses Synced.")
        pg.commit()

    # 6. Channel Updates (With SCD2 valid_to calc)
    safe_bulk_insert(pg, "channel_updates",
        ["gossip_id", "scid", "direction", "valid_from", "valid_to", 
         "signature", "chain_hash", "message_flags", "channel_flags",
         "cltv_expiry_delta", "htlc_minimum_msat", "fee_base_msat", 
         "fee_proportional_millionths", "htlc_maximum_msat", "raw_gossip"],
        """
        SELECT 
            gossip_id, scid, direction, valid_from,
            LEAD(valid_from) OVER (PARTITION BY scid, direction ORDER BY valid_from ASC) as valid_to,
            signature, chain_hash, msg_flags, chan_flags,
            cltv, htlc_min, fee_base, fee_prop, 
            htlc_max, raw_gossip
        FROM stage_channel_updates
        """
    )

    # 7. Observations
    logger.info("📦 Syncing Observations...")
    with pg.cursor() as cur:
        cur.execute("CREATE TEMP TABLE stage_obs (LIKE gossip_observations INCLUDING DEFAULTS)")
        with cur.copy("COPY stage_obs (gossip_id, collector_node_id, seen_at) FROM STDIN") as copy:
             local_con = duckdb.connect(DUCK_DB_PATH, read_only=True)
             res = local_con.execute("SELECT DISTINCT gossip_id, collector_node_id, seen_at FROM stage_observations")
             while True:
                chunk = res.fetchmany(50000)
                if not chunk: break
                for row in chunk: copy.write_row(row)
        
        cur.execute("""
            INSERT INTO gossip_observations (gossip_id, collector_node_id, seen_at)
            SELECT gossip_id, collector_node_id, seen_at FROM stage_obs
            ON CONFLICT (gossip_id, collector_node_id) DO NOTHING
        """)
        logger.info(f"   -> Observations Merged: {cur.rowcount}")
        pg.commit()
    pg.close()

def validate_environment():
    if not os.path.exists(GOSSIP_FILE_PATH):
        logger.error(f"❌ CRITICAL: Gossip file not found at {GOSSIP_FILE_PATH}")
        sys.exit(1)
    if not POSTGRES_URI:
        logger.error("❌ CRITICAL: POSTGRES_URI environment variable is missing.")
        sys.exit(1)

def main():
    start_http_server(METRICS_PORT)
    logger.info(f"📈 Metrics server started on port {METRICS_PORT}")
    validate_environment()
    
    start_time = time.time()
    
    resume = False
    if os.path.exists(DUCK_DB_PATH) and os.path.getsize(DUCK_DB_PATH) > 1024 * 1024:
        logger.info(f"💾 Found existing DuckDB cache. Resuming Phase 2?")
        resume = True 

    if not resume:
        logger.info("🚀 Starting Phase 1: Parsing")
        con = setup_duckdb()
        try:
            parse_and_stage(con)
            print_analysis_report(con)
        finally:
            con.close()
    else:
        logger.info("⏩ Skipping Phase 1 (Resuming from Cache)")
        con = duckdb.connect(DUCK_DB_PATH, read_only=True)
        print_analysis_report(con)
        con.close()
    
    sync_to_postgres()
    METRIC_PHASE.set(0)
    logger.info(f"🎉 IMPORT SUCCESSFUL in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: sys.exit(0)