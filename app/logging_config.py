import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"


def setup_logging(level: str) -> None:
    # A no-op if the root logger is already configured (e.g. by pytest or an embedding app).
    logging.basicConfig(level=level, format=LOG_FORMAT, stream=sys.stdout)
    # aio-pika and aiormq log every reconnect attempt; their warnings are enough.
    for name in ("aio_pika", "aiormq"):
        logging.getLogger(name).setLevel(logging.WARNING)
