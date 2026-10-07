import asyncio

from alembic import command
from sqlalchemy import inspect, make_url, text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import alembic_config

# Synchronous tests on purpose: Alembic's env.py runs its own event loop.


def recreate_database(server_url: str, name: str) -> str:
    async def run() -> None:
        engine = create_async_engine(server_url, isolation_level="AUTOCOMMIT")
        async with engine.connect() as connection:
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
            await connection.execute(text(f'CREATE DATABASE "{name}"'))
        await engine.dispose()

    asyncio.run(run())
    return make_url(server_url).set(database=name).render_as_string(hide_password=False)


def table_names(database_url: str) -> set[str]:
    async def run() -> set[str]:
        engine = create_async_engine(database_url)
        async with engine.connect() as connection:
            names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        await engine.dispose()
        return set(names)

    return asyncio.run(run())


def test_migrations_match_models_and_downgrade_cleanly(database_url: str) -> None:
    url = recreate_database(database_url, "migrations_check")
    config = alembic_config(url)

    command.upgrade(config, "head")
    assert {"payments", "outbox"} <= table_names(url)
    command.check(config)  # fails if the models differ from the migrated schema

    command.downgrade(config, "base")
    assert table_names(url) == {"alembic_version"}

    command.upgrade(config, "head")
    assert {"payments", "outbox"} <= table_names(url)
