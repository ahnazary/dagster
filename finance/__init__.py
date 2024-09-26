# fmt: off
from dagster import Definitions, load_assets_from_modules

from .src import postgres_assets, tickers_assets

_postgres_assets = load_assets_from_modules([postgres_assets])
_tickers_assets = load_assets_from_modules([tickers_assets])

defs = Definitions(
    assets=[*_postgres_assets, *_tickers_assets]
)
