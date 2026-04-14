from dagster import Definitions, load_assets_from_modules

from .assets import bronze, silver, gold

all_assets = load_assets_from_modules([bronze, silver, gold])

defs = Definitions(
    assets=all_assets,
)