from sqlalchemy import asc, func, select

from dagster import AssetIn, Definitions, asset, in_process_executor, mem_io_manager
from finance.src.postgres_assets import (
    balance_sheet_table,
    cashflow_table,
    financials_table,
    income_stmt_table,
    neon_postgres_engine,
    postgres_schema,
    tickers_list_table,
    valid_tickers_table,
)
from finance.utils import custom_logger


@asset
def tickers_to_query_from_yahoo() -> list:
    """
    TODO: make this docs better
    TODO: make the query better

    Method to get a batch of tickers from valid_tickers table that have not
    been inserted into other main tables (financials, balance_sheet,
    cashflow, etc.).


    Parameters
    ----------
    table_name : str
        name of the table to get the tickers from
    engine : sqlalchemy.engine.Engine
        engine to connect to the database, defines if it is local or neon
    frequency : str
        frequency of the data (either annual or quarterly)

    The query is equivalent to (for cahsflow table):
        select valid_tickers.ticker, valid_tickers.cashflow_annual_available,
        subquery.max_insert_date
        from stocks.valid_tickers
        left join (
            select cashflow.ticker, max(cashflow.insert_date) as max_insert_date
            from stocks.cashflow
            group by cashflow.ticker
        ) as subquery
        on valid_tickers.ticker = subquery.ticker
        where valid_tickers.cashflow_annual_available
        order by subquery.max_insert_date asc

    Returns
    -------
    list
        list of tickers
    """

    valid_tickers_table = postgres_interface.create_table_object(
        "valid_tickers", engine
    )
    table = postgres_interface.create_table_object(table_name, engine)

    # this is the column that will be used to check if the ticker is available
    availablility_column = getattr(
        valid_tickers_table.c, f"{table_name}_{frequency}_available"
    )

    subquery = (
        select(
            table.c.ticker,
            func.max(table.c.insert_date).label("max_insert_date"),
        )
        .group_by(table.c.ticker)
        .alias()
    )

    query = (
        select(valid_tickers_table.c.ticker, availablility_column)
        .join(
            subquery,
            valid_tickers_table.c.ticker == subquery.c.ticker,
            isouter=True,
        )
        .order_by(asc(subquery.c.max_insert_date))
        .where(availablility_column)
    )

    with engine.connect() as conn:
        result = conn.execute(query).fetchmany(self.batch_size)

    return [result[0] for result in result]
