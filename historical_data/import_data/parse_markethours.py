"""
To parse market market hours run 
python historical_data/import_data/parse_markethours.py
"""
import pathlib
import logging
import pytz
from datetime import time
from datetime import datetime as dt
import pandas as pd
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import InstrumentStatus
from nautilus_trader.model.enums import MarketStatus
from historical_data.import_data.polygon_equity_data import PolygonEquityData


def parse_markethours(markethours_file, instrument):
    """parse market hours file and create instrumentstatus
    nautilus objects

    Args:
        markethours_file (str): csv file containing markethours
        instrument (nautilus_trader.model.instruments.Instrument): nautilus instrument

    Returns:
        list: list of InstrumentStatus nautilus objects
    """
    logging.info(f"reading file : {markethours_file}")
    df_hours = pd.read_csv(markethours_file)
    status = []
    for _, row in df_hours.iterrows():
        if "CLOSED" in row["Trading Hours"]:
            continue
        times = row["Trading Hours"].split("-")
        est = pytz.timezone("US/Eastern")
        start_regular = est.localize(dt.strptime(times[0], "%Y%m%d:%H%M"))
        end_regular = est.localize(dt.strptime(times[1], "%Y%m%d:%H%M"))
        times = row["Liquid Hours"].split("-")
        start_liquid = est.localize(dt.strptime(times[0], "%Y%m%d:%H%M"))
        end_liquid = est.localize(dt.strptime(times[1], "%Y%m%d:%H%M"))
        if (
            start_regular.time() == time(4, 0)
            and end_regular.time() == time(20, 0)
            and start_liquid.time() == time(9, 30)
            and end_liquid.time() == time(16, 0)
        ):
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.PRE_OPEN,
                    ts_event=dt_to_unix_nanos(start_regular.replace(hour=2, minute=30)),
                    ts_init=dt_to_unix_nanos(start_regular.replace(hour=2, minute=30)),
                    trading_session="NYSE Arca Early Trading",
                )
            )
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.OPEN,
                    ts_event=dt_to_unix_nanos(start_regular),
                    ts_init=dt_to_unix_nanos(start_regular),
                    trading_session="NYSE Arca Early Trading",
                )
            )
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.CLOSED,
                    ts_event=dt_to_unix_nanos(start_liquid) - 1,
                    ts_init=dt_to_unix_nanos(start_liquid) - 1,
                    trading_session="NYSE Arca Early Trading",
                )
            )

            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.OPEN,
                    ts_event=dt_to_unix_nanos(start_liquid),
                    ts_init=dt_to_unix_nanos(start_liquid),
                    trading_session="NYSE American Core Trading",
                )
            )
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.CLOSED,
                    ts_event=dt_to_unix_nanos(end_liquid) - 1,
                    ts_init=dt_to_unix_nanos(end_liquid) - 1,
                    trading_session="NYSE American Core Trading",
                )
            )

            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.OPEN,
                    ts_event=dt_to_unix_nanos(end_liquid),
                    ts_init=dt_to_unix_nanos(end_liquid),
                    trading_session="NYSE American Late Trading",
                )
            )
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.CLOSED,
                    ts_event=dt_to_unix_nanos(end_regular),
                    ts_init=dt_to_unix_nanos(end_regular),
                    trading_session="NYSE American Late Trading",
                )
            )
    return status


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    poly = PolygonEquityData(None, None, "data")
    instrument = poly.read_catalog_instruments(instrument_ids="QQQ")[0]
    filepath = pathlib.Path(__file__).parent.joinpath("MarketHours.csv")
    status = parse_markethours(filepath, instrument)
    print(status)
