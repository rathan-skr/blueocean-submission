import zlib

from . import config


def partition_of(merchant_id: str, partitions: int = config.PARTITIONS) -> int:
    """Stable merchant -> partition map. All of a merchant's frames share one partition (= one
    JetStream subject), which is what preserves per-merchant order through the broker."""
    return zlib.crc32(merchant_id.encode()) % partitions


def subject(p: int) -> str:
    return f"{config.SUBJECT_PREFIX}.{p}"
