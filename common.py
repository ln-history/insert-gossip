from typing import BinaryIO, Optional, Tuple, Literal
from pathlib import Path
import os
import struct
from logging import Logger, getLogger, INFO, DEBUG, Formatter, StreamHandler
from logging.handlers import RotatingFileHandler

def setup_logging(log_level: Literal[10, 20],  # 10 = DEBUG, 20 = INFO
                  log_dir: str = "logs", 
                  log_file: str = "insert_gossip.log") -> Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = getLogger("insert-gossip")
    logger.setLevel(log_level)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=100)
    file_handler.setFormatter(Formatter("%(asctime)s - %(levelname)s -  %(threadName)s: %(message)s"))

    stream_handler = StreamHandler()
    stream_handler.setFormatter(Formatter("%(asctime)s - %(levelname)s - %(threadName)s: %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

def write_message(outfile, msg: bytes):
    length = len(msg)
    if length < 0xFD:
        outfile.write(struct.pack("!B", length))          # prefix is a single byte anyway, so '!' or '<' doesn't matter
    elif length <= 0xFFFF:
        outfile.write(b'\xFD' + struct.pack("<H", length))  # little-endian 2 bytes
    elif length <= 0xFFFFFFFF:
        outfile.write(b'\xFE' + struct.pack("<L", length))  # little-endian 4 bytes
    else:
        outfile.write(b'\xFF' + struct.pack("<Q", length))  # little-endian 8 bytes
    outfile.write(msg)


def open_gossip_store(file_path: str, logger: Logger) -> Optional[BinaryIO]:
    """
    Open the gossip_store file, validate its version, and return an open file handle.
    The handle will be positioned at offset 1 (after the version byte).
    
    Returns:
        BinaryIO if successful, otherwise None.
    """
    try:
        path = Path(file_path)
        if not path.exists():
            logger.error(f"Gossip store file does not exist: {file_path}")
            return None

        file_handle = open(path, "rb")  # caller must close this!
        file_size = path.stat().st_size

        # Read and verify version
        version_byte = file_handle.read(1)
        if not version_byte:
            logger.warning("Empty gossip_store file")
            file_handle.close()
            return None

        version = version_byte[0]
        major_version = (version >> 5) & 0x07
        minor_version = version & 0x1F

        if major_version != 0:
            logger.error(f"Unsupported gossip_store major version: {major_version}")
            file_handle.close()
            return None

        logger.info(f"Opened gossip_store file ({file_size} bytes), version {major_version}.{minor_version}")
        logger.info("Starting from offset 1")

        # Position file after version byte
        file_handle.seek(1)
        return file_handle

    except Exception as e:
        logger.error(f"Error opening gossip_store: {e}")
        return None


def parse_gossip_store_header(file: BinaryIO, logger: Logger) -> Tuple[int, int]:
    """Read and log the gossip_store version header."""
    header = file.read(1)
    if not header:
        raise ValueError("Empty file, no header found")

    version_byte = header[0]
    major_version = (version_byte >> 5) & 0x07
    minor_version = version_byte & 0x1F

    if major_version == 0:
        logger.info(f"[read] Found gossip_store version: {major_version}.{minor_version}")
    else:
        logger.warning(f"[read] Unsupported gossip_store major version: {major_version}, continuing anyway")

    return major_version, minor_version