# fmt: off
from dagster import (
    AssetIn,
    Definitions,
    asset,
    in_process_executor,
    load_assets_from_modules,
    mem_io_manager,
)

from .src import postgres_assets, tickers_assets

_postgres_assets = load_assets_from_modules([postgres_assets])
_tickers_assets = load_assets_from_modules([tickers_assets])

defs = Definitions(
    assets=[*_postgres_assets, *_tickers_assets],
    resources={
        "io_manager": mem_io_manager,
    },
    executor=in_process_executor,
)
