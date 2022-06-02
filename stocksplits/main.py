import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import pool
from datetime import datetime, timedelta
import time
import config
import polygon
from polygon import StocksClient
import io
import asyncio
# import uvloop  # Unix only


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate


def sql_fun(df):
    conn = postgreSQL_pool.getconn()
    conn.autocommit = True
    cur = conn.cursor()
    sio = io.StringIO()
    df.to_csv(sio, index=False, header=False)
    sio.seek(0)
    cur.copy_from(file=sio, table='stockdata_hist', sep=",", columns=stock_cols, null='')
    cur.close()
    postgreSQL_pool.putconn(conn)


async def get_ticker_data(ticker):
    # Make API Call
    today = datetime.today().date()
    resp = await stocks_client.get_aggregate_bars(ticker, '2005-01-01', today, full_range=True, timespan='minute',
                                                  high_volatility=True, warnings=False, adjusted=True)
    df = pd.DataFrame.from_dict(resp)
    # Formatting
    if 'a' in df.columns:
        df = df[['v', 'a', 'vw', 'o', 'c', 'h', 'l', 't', 'n', 'op']]
    else:
        df.insert(1, 'a', None)
        df.insert(9, 'op', None)
        df = df[['v', 'a', 'vw', 'o', 'c', 'h', 'l', 't', 'n', 'op']]
    df.drop('a', axis=1, inplace=True)
    df.drop('n', axis=1, inplace=True)
    df.columns = ['tick_volume', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'time_end', 'opening_price']
    df['symbol'] = ticker
    df['total_volume'] = None
    df['vwap'] = None
    df['avg_trade_size'] = None
    df['time_beg'] = None
    df['tick_volume'] = df['tick_volume'].astype('int')
    df['tdate'] = df['time_end'].map(lambda x: unix_convert(x))
    df['save_date'] = datetime.utcnow()
    df = df[['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close',
             'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']]
    return df


async def main(symbol_list):
    print(f"Getting data for {len(symbol_list)} symbols")
    for ticker in symbol_list:
        print(f"Getting data for {ticker}")
        try:
            df = await get_ticker_data(ticker)
            Save to database
            clear_data = text(f"""DELETE FROM stockdata_hist WHERE symbol = '{ticker}';""")
            with engine.connect() as conn:
                 conn.execute(clear_data)
            sql_fun(df)
            print(df)
        except Exception as e:
            print(e)
            pass


def stock_splits(symbol_list):
    splits_list = []
    execution_date = datetime.today().date()  # Check for splits since this date
    for ticker in symbol_list:
        print(f"Checking {ticker} for splits since {execution_date}")
        resp = reference_client.get_stock_splits(ticker, all_pages=True)
        if resp:
            if resp[0]['ticker']:
                for split in resp:
                    split_date = datetime.strptime(split['execution_date'], "%Y-%m-%d").date()
                    if datetime.today().date() >= split_date >= execution_date:
                        splits_list.append(resp[0]['ticker'])
                        print(splits_list)
    return splits_list


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    engine = create_engine(config.psql)
    postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 50, host=config.psql_host, database=config.psql_db,
                                                           user=config.psql_user, password=config.psql_pw)
    reference_client = polygon.ReferenceClient(config.polygon_key, False, read_timeout=60)
    symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
    symbols = symbols_df['ticker'].to_list()
    # symbols = ['FAMI', 'YINN', 'ERY', 'CWEB', 'BQ']  # Example List
    stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close',
                  'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
    
    splits_list = stock_splits(symbols)
    print(splits_list)
    reference_client.close()
    stocks_client = StocksClient(config.polygon_key, True)
    asyncio.run(main(splits_list))
    stocks_client.close()
