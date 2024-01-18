"""
Module to handle postgres dagster assets
"""

from os import getenv

import sqlalchemy
from sqlalchemy import MetaData, Table, create_engine

import config
from dagster import (
    AssetIn,
    AssetOut,
    Definitions,
    asset,
    in_process_executor,
    mem_io_manager,
    multi_asset,
)
from finance.src.utils import custom_logger

logger = custom_logger(__name__)


@asset
def neon_postgres_engine() -> sqlalchemy.engine.base.Engine:
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
def postgres_schema() -> str:
    """
    Create a postgres schema
    """
    logger.info("Creating postgres schema")
    return config.POSTGRES_SCHEMA


@multi_asset(
    ins={
        "engine": AssetIn("neon_postgres_engine"),
        "schema": AssetIn("postgres_schema"),
    },
    outs={
        "balance_sheet_table": AssetOut("balance_sheet_table"),
        "cashflow_table": AssetOut("cashflow_table"),
        "income_stmt_table": AssetOut("income_stmt_table"),
        "financials_table": AssetOut("financials_table"),
        "tickers_list_table": AssetOut("tickers_list_table"),
        "valid_tickers_table": AssetOut("valid_tickers_table"),
    },
)
def postgres_assets(engine: sqlalchemy.engine.base.Engine, schema: str):
    """
    Create a valid tickers table object
    """
    logger.info("Creating valid tickers table object")
    metadata = MetaData()
    balance_sheet_table = Table(
        "balance_sheet", metadata, schema=schema, autoload_with=engine
    )
    cashflow_table = Table("cashflow", metadata, schema=schema, autoload_with=engine)
    income_stmt_table = Table(
        "income_stmt", metadata, schema=schema, autoload_with=engine
    )
    financials_table = Table(
        "financials", metadata, schema=schema, autoload_with=engine
    )
    tickers_list_table = Table(
        "tickers_list", metadata, schema=schema, autoload_with=engine
    )
    valid_tickers_table = Table(
        "valid_tickers", metadata, schema=schema, autoload_with=engine
    )

    return (
        balance_sheet_table,
        cashflow_table,
        income_stmt_table,
        financials_table,
        tickers_list_table,
        valid_tickers_table,
    )


defs = Definitions(
    assets=[
        neon_postgres_engine,
        postgres_schema,
        postgres_assets,
    ],
    resources={
        "io_manager": mem_io_manager,
    },
    executor=in_process_executor,
)
