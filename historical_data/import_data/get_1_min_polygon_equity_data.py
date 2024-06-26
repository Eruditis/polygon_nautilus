import traceback
import pandas as pd
from datetime import datetime, date, timedelta
import time

import pytz

import config
from datetime import timezone
import asyncio
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import BarType, BarSpecification
from nautilus_trader.model.enums import AssetClass, AggregationSource, BarAggregation, PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.equity import Equity
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

# import uvloop  # Unix only
from polygon import StocksClient
from polygon import ReferenceClient

key = config.polygon_key

"""
This is what we have used to create 1 minute bar Nautilus catalogs
"""


def unix_convert(ts):
    """convert from unix time to python datetime

    Args:
        ts (int): unix time

    Raises:
        TypeError: in case unix timestamp is invalid

    Returns:
        date: return python date
    """
    if len(str(ts)) == 13:
        ts = int(ts / 1000)
    elif len(str(ts)) == 19:
        ts = int(ts / 1000000000)
    else:
        raise TypeError(f"timestamp length {len(str(ts))} was not 13 or 19")
    return datetime.fromtimestamp(ts, pytz.UTC)  # .strftime('%Y-%m-%d %H:%M:%S')


async def get_bar_data(stocks_client, ticker: str, start_date, end_date):
    resp = await stocks_client.get_aggregate_bars(symbol=ticker
                                                  , from_date=start_date, to_date=end_date, timespan='minute'
                                                  , full_range=True, run_parallel=True
                                                  , high_volatility=True, warnings=False, adjusted=True)

    for d in resp:
        d.setdefault('a')
        d.setdefault('op')
        d.setdefault('n')
        d.setdefault('vw')
        if 'a' in d:
            del d['a']
        if 'n' in d:
            del d['n']
        if 'op' in d:
            del d['op']
    resp = [{'v': d['v'], 'vw': d['vw'], 'o': d['o'], 'c': d['c'], 'h': d['h'], 'l': d['l'], 't': d['t']} for d in resp]
    for d in resp:
        d['bar_volume'] = d.pop('v')
        d['bar_vwap'] = d.pop('vw')
        d['bar_open'] = d.pop('o')
        d['bar_close'] = d.pop('c')
        d['bar_high'] = d.pop('h')
        d['bar_low'] = d.pop('l')
        d['bar_time_start'] = d.pop('t')
        d['symbol'] = ticker
        d['bar_volume'] = int(d['bar_volume'])
        d['timestamp'] = unix_convert(d['bar_time_start'] * 1000000 + 59999999999)  # add 1 minute -1 nanoseconds
        d['save_date'] = datetime.now(timezone.utc)

    resp = [{'symbol': d['symbol'], 'bar_time_start': d['bar_time_start'], 'open': d['bar_open'], 'high': d['bar_high'],
             'low': d['bar_low'], 'close': d['bar_close'], 'volume': d['bar_volume'], 'vwap': d['bar_vwap'],
             'timestamp': d['timestamp'], 'save_date': d['save_date']
             } for d in resp]

    return resp


def generic_equity(ticker_symbol: str, ticker_venue: str, currency, cik) -> Equity:
    return Equity(
        instrument_id=InstrumentId(Symbol(ticker_symbol), Venue(ticker_venue)),
        raw_symbol=Symbol(ticker_symbol),
        currency=USD,
        price_precision=2,
        price_increment=Price.from_str("0.01"),
        lot_size=Quantity.from_int(1),
        isin=cik,  # we should be converting this to isin...
        ts_event=0,
        ts_init=0,
    )


def get_polygon_key():
    return config.polygon_key


def ticker_factory():
    print("Retrieving tickers")
    ref_client = ReferenceClient(get_polygon_key(), use_async=False)

    try:
        tickers = ref_client.get_tickers(market='stocks', all_pages=True, merge_all_pages=True)
        # filter here if you are just testing, or need to skip symbols
        # tickers = [d for d in tickers if d['ticker'] in symbols]
        tickers = [d for d in tickers if d['ticker'] == 'QQQ']
        print("Tickers retrieved")
        return tickers
    finally:
        ref_client.close()


async def main():
    stocks_client = StocksClient(get_polygon_key(), use_async=True, read_timeout=120)

    try:
        catalog_path = "C:/Repos/polygon_nautilus/.data/polygon/1_min_bar"
        catalog = ParquetDataCatalog(catalog_path)
        start_date = date(2014, 4, 1)
        end_date = date(2024, 3, 26)

        ticker_list = ticker_factory()
        for ticker in ticker_list:
            t = time.time()
            print(f"Getting data for {ticker}")

            # never stop trying to get the data!!
            while True:
                try:
                    ticker_dict = await get_bar_data(stocks_client, ticker['ticker'], start_date, end_date)
                except Exception as e:
                    # loop forever, reconnecting and retrying
                    print(e)
                    traceback.print_exc()
                    print(f"ERROR:: Bugger! Gonna sleep for a tick, then retry ticker {ticker['ticker']}")

                    try:
                        await asyncio.sleep(3)
                        await stocks_client.close()
                    except Exception:
                        pass

                    stocks_client = StocksClient(get_polygon_key(), use_async=True, read_timeout=120)
                    continue
                break

            if ticker_dict:
                # print("Time to data:", time.time() - t)

                # t = time.time()
                df = pd.DataFrame(ticker_dict)
                df.set_index('timestamp', inplace=True)
                # print("Time to dataframe:", time.time() - t)

                # t = time.time()
                ticker.setdefault("cik", "")

                try:
                    instrument = generic_equity(ticker['ticker'], ticker['primary_exchange'], USD, str(ticker['cik']))
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    print(f"ERROR:: Bugger, couldn't create an instrument. Ah well, sally forth!")
                    continue

                try:
                    bar_type = BarType(
                        instrument.id,
                        BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                        AggregationSource.EXTERNAL,  # Bars are aggregated in the data already
                    )
                    wrangler = BarDataWrangler(bar_type, instrument)
                    ticks = wrangler.process(data=df[['open', 'high', 'low', 'close', 'volume']], ts_init_delta=1)
                    # print("Time to wrangle:", time.time() - t)

                    # t = time.time()
                    catalog.write_data([instrument])
                    catalog.write_data(ticks)
                    print("Time to persist:", time.time() - t)
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    print(
                        f"ERROR:: Bugger, couldn't create bar_type, wrangler, ticks, or commit. Ah well, sally forth!")
                    continue
            else:
                print(f"No data for {ticker['ticker']}, carrying on")

    except Exception as e:
        print(e)
        traceback.print_exc()

    finally:
        await stocks_client.close()

    return


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    print('program start')
    asyncio.run(main())
    print('program end')
