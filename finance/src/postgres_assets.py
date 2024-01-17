"""
Module to handle postgres dagster assets
"""

from os import getenv

import sqlalchemy
from sqlalchemy import MetaData, Table, create_engine

import config
from dagster import AssetIn, Definitions, asset, in_process_executor, mem_io_manager
from finance.src.utils import custom_logger

logger = custom_logger(__name__)


@asset
def neon_postgres_engine():
    """
    Create a postgres engine for neon
    """
    user = getenv("NEON_POSTGRES_USER")
    password = getenv("NEON_POSTGRES_PASSWORD")
    host = getenv("NEON_POSTGRES_HOST")
    port = getenv("NEON_POSTGRES_PORT")
    database = getenv("NEON_POSTGRES_DB")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    logger.info("Created postgres engine for neon")

    return engine


@asset
def postgres_schema():
    """
    Create a postgres schema
    """
    logger.info("Creating postgres schema")
    return config.POSTGRES_SCHEMA


@asset(
    ins={
        "engine": AssetIn("neon_postgres_engine"),
        "schema": AssetIn("postgres_schema"),
    }
)
def balance_sheet_table(engine: sqlalchemy.engine.base.Engine, schema: str):
    """
    Create a balance sheet table object
    """
    logger.info("Creating balance sheet table object")
    metadata = MetaData()
    return Table("balance_sheet", metadata, schema=schema, autoload_with=engine)


@asset(
    ins={
        "engine": AssetIn("neon_postgres_engine"),
        "schema": AssetIn("postgres_schema"),
    }
)
def cashflow_table(engine: sqlalchemy.engine.base.Engine, schema: str):
    """
    Create a cashflow table object
    """
    logger.info("Creating cashflow table object")
    metadata = MetaData()
    return Table("cashflow", metadata, schema=schema, autoload_with=engine)


@asset(
    ins={
        "engine": AssetIn("neon_postgres_engine"),
        "schema": AssetIn("postgres_schema"),
    }
)
def income_stmt_table(engine: sqlalchemy.engine.base.Engine, schema: str):
    """
    Create a income statement table object
    """
    logger.info("Creating income statement table object")
    metadata = MetaData()
    return Table("income_stmt", metadata, schema=schema, autoload_with=engine)


@asset(
    ins={
        "engine": AssetIn("neon_postgres_engine"),
        "schema": AssetIn("postgres_schema"),
    }
)
def financials_table(engine: sqlalchemy.engine.base.Engine, schema: str):
    """
    Create a financials table object
    """
    logger.info("Creating financials table object")
    metadata = MetaData()
    return Table("financials", metadata, schema=schema, autoload_with=engine)


@asset(
    ins={
        "engine": AssetIn("neon_postgres_engine"),
        "schema": AssetIn("postgres_schema"),
    }
)
def tickers_list_table(engine: sqlalchemy.engine.base.Engine, schema: str):
    """
    Create a tickers list table object
    """
    logger.info("Creating tickers list table object")
    metadata = MetaData()
    return Table("tickers_list", metadata, schema=schema, autoload_with=engine)


@asset(
    ins={
        "engine": AssetIn("neon_postgres_engine"),
        "schema": AssetIn("postgres_schema"),
    }
)
def valid_tickers_table(engine: sqlalchemy.engine.base.Engine, schema: str):
    """
    Create a valid tickers table object
    """
    logger.info("Creating valid tickers table object")
    metadata = MetaData()
    return Table("valid_tickers", metadata, schema=schema, autoload_with=engine)


defs = Definitions(
    assets=[
        neon_postgres_engine,
        postgres_schema,
        balance_sheet_table,
        cashflow_table,
        income_stmt_table,
        financials_table,
        tickers_list_table,
        valid_tickers_table,
    ],
    resources={
        "io_manager": mem_io_manager,
    },
    executor=in_process_executor,
)
