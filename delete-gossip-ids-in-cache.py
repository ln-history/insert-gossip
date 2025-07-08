import psycopg2
from ValkeyClient import ValkeyCache
from typing import List
from config import POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def get_channel_announcement_gossip_ids(
    conn: psycopg2.extensions.connection
) -> List[bytes]:
    """
    Fetch all gossip_id values from raw_gossip table where gossip_message_type is 256 (channel_announcement).
    Returns a list of gossip_id as bytes.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT gossip_id
            FROM raw_gossip
            WHERE gossip_message_type = 256
        """)
        rows = cur.fetchall()
        return [row[0] for row in rows]
    
def delete_all_channel_announcements_from_cache():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=POSTGRES_DB_NAME, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT
    )

    try:
        gossip_ids = get_channel_announcement_gossip_ids(conn)
        print(f"Found {len(gossip_ids)} gossip_ids to add.")

        cache = ValkeyCache()
        deleted = cache.cache_gossip(gossip_ids)
        print(f"Deleted {deleted} gossip_ids from Valkey.")

    finally:
        conn.close()


if __name__ == "__main__":
    delete_all_channel_announcements_from_cache()