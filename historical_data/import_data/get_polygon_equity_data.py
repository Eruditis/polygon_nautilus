"""
Usage:
python historical_data/import_data/get_polygon_equity_data.py
"""
import config
from datetime import date
import logging
import pandas as pd
from historical_data.import_data.polygon_equity_data import PolygonEquityData
from nautilus_trader.model.data import Bar

logging.basicConfig(level=logging.INFO)
# test PolygonEquityData class
poly = PolygonEquityData(config, ["QQQ"], "C:/Repos/polygon_nautilus/.data")
poly.get_tickers()
poly.get_bar_data_for_tickers(date(2014, 4, 1), date(2024, 3, 26))

# read from catalog data
bars = poly.read_catalog_bars()
# print(bars)
df_bars = pd.DataFrame([Bar.to_dict(b) for b in bars])
# print(df_bars)
