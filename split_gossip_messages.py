import bz2
import struct
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

from lnhistoryclient.parser.common import get_message_type_by_raw_hex, strip_known_message_type

def varint_decode(r):
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
    

def read_dataset(filename: str, start: int = 0, logger: Optional[logging.Logger] = None):
    with bz2.open(filename, 'rb') as f:
        header = f.read(4)
        assert header[:3] == b'GSP' and header[3] == 1

        skipped = 0
        yielded = 0
        last_logged = 0

        while True:
            length = varint_decode(f)
            if length is None:
                break

            msg = f.read(length)
            if len(msg) != length:
                raise ValueError(f"Incomplete message read from {filename}")

            if skipped < start:
                skipped += 1
                continue

            yield msg
            yielded += 1

            if logger and yielded % 100_000 == 0 and yielded != last_logged:
                logger.info(f"Yielded {yielded} messages (starting from offset {start})")
                last_logged = yielded

def setup_logging(log_dir: str = "logs", log_file: str = "split_gossip_messages.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("split_gossip_messages")
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def write_message(outfile, msg: bytes):
    # Write varint-prefixed message
    length = len(msg)
    if length < 0xFD:
        outfile.write(struct.pack("!B", length))
    elif length <= 0xFFFF:
        outfile.write(b'\xFD' + struct.pack("!H", length))
    elif length <= 0xFFFFFFFF:
        outfile.write(b'\xFE' + struct.pack("!L", length))
    else:
        outfile.write(b'\xFF' + struct.pack("!Q", length))
    outfile.write(msg)


def split_gossip_file(input_file: str, output_prefix: str = ""):
    logger = setup_logging()

    # Output files
    output_paths = {
        256: output_prefix + "channel_announcements.gsp.bz2",
        257: output_prefix + "node_announcements.gsp.bz2",
        258: output_prefix + "channel_update.gsp.bz2"
    }

    # Open BZ2-compressed output streams and write header
    outputs = {}
    for key, path in output_paths.items():
        f = bz2.BZ2File(path, "wb")
        f.write(b'GSP\x01')  # Write the file header
        outputs[key] = f

    count = 0
    unknown = 0

    try:
        for msg in read_dataset(input_file, logger=logger):
            try:
                msg_type = get_message_type_by_raw_hex(msg)
                if msg_type in outputs:
                    write_message(outputs[msg_type], msg)
                else:
                    unknown += 1
            except Exception as e:
                logger.warning(f"Could not classify message #{count}: {e}")
                unknown += 1
            count += 1

    finally:
        for f in outputs.values():
            f.close()

    logger.info(f"Finished splitting {count} messages. Unknown messages: {unknown}")

# --- Entry point ---
if __name__ == "__main__":
    split_gossip_file("/Users/fabiankraus/Programming/topology/data/uniq2.gsp.bz2", "test")