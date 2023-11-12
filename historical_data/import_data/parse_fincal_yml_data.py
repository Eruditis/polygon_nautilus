"""
This script is to read sessions data from the pkl file and 
load it to a nautilus catalog.
The pickle file contains the output from fincal repository's
GetSessions function.

Usage:
python historical_data/import_data/parse_fincal_yml_data.py
"""
import pickle
import pytz
from  datetime import datetime
from nautilus_trader.model.data import InstrumentStatus
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.enums import MarketStatus

def main():
    with open('sessions_data/sessions.pkl', 'rb') as f:
        sessions = pickle.load(f)
    
    catalog = ParquetDataCatalog('nautilus_catalog')
    instrument = catalog.instruments(instrument_ids="QQQ")[0]
    nautilus_status = []
    for session in sessions:
        tzinfo = pytz.timezone(session["timezone"])
        session_start = tzinfo.localize(
            datetime.strptime(
                f"{session['date']} {session['gte']['time']}", "%Y-%m-%d %I:%M %p"
            )
        )
        session_end = tzinfo.localize(
            datetime.strptime(
                f"{session['date']} {session['lt']['time']}", "%Y-%m-%d %I:%M %p"
            )
        )
        nautilus_status.append(InstrumentStatus(
            instrument_id=instrument.id,
            status=[x for x in list(MarketStatus) if x.name==session['gte']['status']][0],
            ts_event=datetime.timestamp(session_start),
            ts_init=datetime.timestamp(session_start),
            trading_session=session['session']
        ))
        nautilus_status.append(InstrumentStatus(
            instrument_id=instrument.id,
            status=[x for x in list(MarketStatus) if x.name==session['lt']['status']][0],
            ts_event=datetime.timestamp(session_end),
            ts_init=datetime.timestamp(session_end),
            trading_session=session['session']
        ))
    catalog.write_data(nautilus_status)

if __name__=='__main__':
    main()