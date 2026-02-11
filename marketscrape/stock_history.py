import alpaca.data.historical as alpaca_historical
from alpaca.data.models.bars import BarSet, Bar
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import pandas as pd

class stock_history():
    """class for retrieving and storing historical stock data for a given symbol."""
    
    def __init__(self, symbol: str, historical_client: alpaca_historical.StockHistoricalDataClient):
        self.symbol = symbol
        self.historical_client = historical_client
        self.bars: pd.DataFrame = pd.DataFrame()
        

    def get_historical_bars(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Retrieve historical bars for the symbol between start and end datetimes."""
        
        request = StockBarsRequest(symbol_or_symbols=self.symbol, start=start, end=end, timeframe=TimeFrame.Minute)
        barset: BarSet = self.historical_client.get_stock_bars(request)
        bars_list = []

        for bar in barset[self.symbol]:
            bar: Bar
            bars_list.append({
                "timestamp": bar.timestamp,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume
            })

        self.bars = pd.DataFrame(bars_list)
        self.bars.set_index("timestamp", inplace=True)
        return self.bars
    
        
        
        
       
        

        

    
        
