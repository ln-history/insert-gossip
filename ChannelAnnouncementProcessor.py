from concurrent.futures import ThreadPoolExecutor, as_completed

from ChannelAnnouncementBatcher import ChannelAnnouncementBatcher
from NodeAnnouncementBatcher2 import NodeAnnouncementBatcher

class ChannelAnnouncementProcessor:
    def __init__(self, conn, cache, logger, max_workers=8, batch_size=100):
        self.conn = conn
        self.cache = cache
        self.logger = logger
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.node_batcher = NodeAnnouncementBatcher(conn, cache, logger, batch_size=batch_size)
        self.channel_batcher = ChannelAnnouncementBatcher(conn, cache, logger, self.node_batcher, batch_size=batch_size)
        self.futures = []

    def submit(self, raw_bytes: bytes):
        future = self.executor.submit(self.channel_batcher.handle_message, raw_bytes)
        self.futures.append(future)

    def run(self, messages: list[bytes]):
        for raw_bytes in messages:
            self.submit(raw_bytes)

        for future in as_completed(self.futures):
            try:
                future.result()
            except Exception as e:
                self.logger.error(f"Exception in worker thread: {e}")

        self.channel_batcher.flush()
        self.node_batcher.flush()
        self.executor.shutdown(wait=True)
