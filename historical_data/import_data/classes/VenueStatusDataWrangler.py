import pandas_market_calendars as mcal
from nautilus_trader.model.data import InstrumentStatus
from nautilus_trader.model.enums import MarketStatus
from datetime import datetime, timedelta
import pytz


def fetch_market_hours(exchange_name, start_date, end_date):
    # Create a calendar for the specified exchange
    calendar = mcal.get_calendar(exchange_name)

    # Fetch the market open and close times for the date range
    schedule = calendar.schedule(start_date=start_date, end_date=end_date)

    # Initialize a list to hold InstrumentStatus events
    instrument_statuses = []

    # Iterate over each day in the schedule
    for date, row in schedule.iterrows():
        # Convert market open and close to UNIX nanoseconds
        market_open = calendar.open_at_time(schedule, date)
        market_close = calendar.close_at_time(schedule, date)

        # Assuming 'symbol' and 'venue' are known for your instrument
        symbol = 'ExampleSymbol'
        venue = 'ExampleVenue'

        # Create InstrumentStatus event for market open
        status_open = InstrumentStatus(
            ts_event=market_open,
            ts_init=market_open,
            symbol=symbol,
            venue=venue,
            status=MarketStatus.OPEN)

        # Create InstrumentStatus event for market close
        status_close = InstrumentStatus(
            ts_event=market_close,
            ts_init=market_close,
            symbol=symbol,
            venue=venue,
            status=MarketStatus.CLOSED)

        # Append the events to the list
        instrument_statuses.extend([status_open, status_close])

    return instrument_statuses


# Example usage
exchange_name = 'NYSE'
start_date = '2023-01-01'
end_date = '2023-01-07'
instrument_statuses = fetch_market_hours






from datetime import datetime

import pandas as pd
from pandas_market_calendars import get_calendar

from nautilus_trader.model.enums import MarketStatus
from nautilus_trader.model.events import Market VenueStatusEvent
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.data import InstrumentStatus

class VenueStatusDataWrangler:

    def __init__(self, venue: Venue):
        self.venue = venue
        self.calendar = get_calendar(venue.value)

    def process(self, start: datetime, end: datetime) -> list[VenueStatusEvent]:
        dates = self.calendar.schedule(start_date=start, end_date=end)

        events = []
        for date, status in dates.iterrows():
            if status == "open":
                event = VenueStatusEvent(
                    venue=self.venue,
                    timestamp=date.timestamp() * 1_000_000_000,  # Nanoseconds
                    status=VenueStatus.OPEN,
                )
            else:
                event = VenueStatusEvent(
                    venue=self.venue,
                    timestamp=date.timestamp() * 1_000_000_000,  # Nanoseconds
                    status=VenueStatus.CLOSED,
                )

            events.append(event)

        return events
