import alpaca.data.historical as alpaca_historical
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import pandas as pd

from client.alpaca_pod import AlpacaPod

class StockHistoryBatch:
    """Class for retrieving historical stock data for multiple symbols in one batch."""
    
    def __init__(self,alpaca_pod: AlpacaPod):
        self._historical_client = alpaca_pod.historical

    def get_historical_bars(self, symbols: list, start: datetime, end: datetime, timeframe: dict) -> pd.DataFrame:
        """ Retrieve historical bars for multiple symbols in a single batch request.
        Returns a MultiIndex DataFrame (symbol, timestamp)."""

        tf_map = {
        '1Day': TimeFrame.Day,
        '1Min': TimeFrame.Minute,
        '1Hour': TimeFrame.Hour
        }
    
        actual_tf = tf_map.get(timeframe, TimeFrame.Day)
        
        request = StockBarsRequest(
            symbol_or_symbols=symbols,
            start=start,
            end=end,
            timeframe=actual_tf,
            adjustment='split'
        )
        
        # The .df property automatically converts the response to a clean Pandas DataFrame
        barset_df = self._historical_client.get_stock_bars(request).df
        
        return barset_df