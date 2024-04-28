import pandas as pd
import pandas_market_calendars as mcal
from cachetools import cached, TTLCache
from nautilus_trader.model.data import InstrumentStatus
from nautilus_trader.model.enums import MarketStatus, HaltReason
from nautilus_trader.model.identifiers import InstrumentId
from historical_data.import_data.polygon_equity_data import PolygonEquityData
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
import pytz
from pathlib import Path


path = Path.cwd() / "nautilus_catalog"  # "C:/Repos/polygon_nautilus/nautilus_catalog"
catalog_ = ParquetDataCatalog(path=path.as_posix())
# poly = PolygonEquityData(None, None, "nautilus_catalog")
catalog_instrument = catalog_.instruments(as_nautilus=True)
# catalog_instrument = catalog_.read_catalog_instruments(instrument_ids="QQQ")[0]
# events = create_instrument_status_events(market_calendar, catalog_instrument)

# 'events' now contains InstrumentStatus events for market open and close times
print("done!")