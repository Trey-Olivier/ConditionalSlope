import alpaca.data.historical as alpaca_historical
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import pandas as pd

class StockHistoryBatch:
    """Class for retrieving historical stock data for multiple symbols in one batch."""
    
    def __init__(self, historical_client: alpaca_historical.StockHistoricalDataClient):
        self._historical_client = historical_client

    def get_historical_bars(self, symbols: list, start: datetime, end: datetime, timeframe=TimeFrame.Day) -> pd.DataFrame:
        """
        Retrieve historical bars for multiple symbols in a single batch request.
        Returns a MultiIndex DataFrame (symbol, timestamp).
        """
        request = StockBarsRequest(
            symbol_or_symbols=symbols,
            start=start,
            end=end,
            timeframe=timeframe
        )
        
        # The .df property automatically converts the response to a clean Pandas DataFrame
        barset_df = self._historical_client.get_stock_bars(request).df
        
        return barset_df