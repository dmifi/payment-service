import asyncio

from app.config import get_settings
from app.logging_config import setup_logging
from app.outbox.app import create_app


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    asyncio.run(create_app(settings).run())


if __name__ == "__main__":
    main()
