from datetime import datetime
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


# Helper function to convert datetime to UNIX timestamp in nanoseconds
def datetime_to_unix_ns(dt):
    return int(pd.Timestamp(dt).value)


# Define cache for calendar data to improve performance
CALENDAR_CACHE_SECONDS = 60 * 60 * 6.5  # Cache duration: 6.5 hours


@cached(cache=TTLCache(maxsize=128, ttl=CALENDAR_CACHE_SECONDS))
def get_market_calendar(market: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Fetches the market calendar, with caching to avoid redundant fetches."""
    calendar = mcal.get_calendar(market)

    # If no start date is provided, default to a lookback of two years
    if not start_date:
        start_date = (pd.Timestamp('now') - pd.Timedelta(days=365 * 2)).strftime('%Y-%m-%d')

    # If no end date is provided, default to today
    if not end_date:
        end_date = pd.Timestamp('now').strftime('%Y-%m-%d')

    schedule = calendar.schedule(start_date=start_date, end_date=end_date)
    return schedule


def create_instrument_status_events(schedule: pd.DataFrame, instrument) -> list:
    """Transforms market calendar schedule into InstrumentStatus events."""
    utc = pytz.UTC
    instrument_status_events = []
    for _, times in schedule.iterrows():
        open_time = times['market_open']
        close_time = times['market_close']

        # Convert times to UNIX timestamps in nanoseconds
        ts_event_open = datetime_to_unix_ns(open_time)
        ts_event_close = datetime_to_unix_ns(close_time)

        # Creating events for market open and close
        open_event = InstrumentStatus(
            instrument_id=instrument.id,
            status=MarketStatus.OPEN,
            ts_event=ts_event_open,
            ts_init=ts_event_open,  # Assuming the init timestamp is the same as the event timestamp
            trading_session="Regular",
            halt_reason=HaltReason.NOT_HALTED
        )
        close_event = InstrumentStatus(
            instrument_id=instrument.id,
            status=MarketStatus.CLOSED,
            ts_event=ts_event_close,
            ts_init=ts_event_close,  # Assuming the init timestamp is the same as the event timestamp
            trading_session="Regular",
            halt_reason=HaltReason.NOT_HALTED
        )

        instrument_status_events.extend([open_event, close_event])

    return instrument_status_events


# Example usage
market_calendar = get_market_calendar('NYSE')

path = Path.cwd() / "nautilus_catalog"  # "C:/Repos/polygon_nautilus/nautilus_catalog"
catalog_ = ParquetDataCatalog(path=path.as_posix())
# poly = PolygonEquityData(None, None, "nautilus_catalog")
catalog_instrument = catalog_.instruments(as_nautilus=True)
# catalog_instrument = catalog_.read_catalog_instruments(instrument_ids="QQQ")[0]
# events = create_instrument_status_events(market_calendar, catalog_instrument)

# 'events' now contains InstrumentStatus events for market open and close times
print("done!")
