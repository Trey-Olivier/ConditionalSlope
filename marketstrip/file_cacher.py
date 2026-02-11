import pandas as pd
from pathlib import Path
from marketstrip.stockgrabber import Stock

class CachedFile():
    """A class to handle caching of stock data in both CSV and Parquet formats."""

    def __init__(self, base_path: str | Path = None, 
                 serialized_path: str | Path = None,) -> None:
        
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.serialized_path = self.base_path / serialized_path if serialized_path else self.base_path

    def parquet_cache(self, ticker: str) -> None:
        market_data = Stock(ticker).scrapeMarket()
        df = market_data.copy()

        df.to_parquet(self.serialized_path / f"{ticker}.parquet")

    def load_cache(self, ticker: str) -> pd.DataFrame:
        return pd.read_parquet(self.serialized_path / f"{ticker}.parquet")
    
    def cache_exists(self, ticker: str, csv: bool) -> bool:
        if csv:
            return (self.serialized_path / f"{ticker}.csv").exists()
        else:
            return (self.serialized_path / f"{ticker}.parquet").exists()
    
    def csv_parquet_cache(self, ticker: str) -> None:
        market_data = Stock(ticker).scrapeMarket()
        df = market_data.copy()

        df.to_csv(self.serialized_path / f"{ticker}.csv")
        df.to_parquet(self.serialized_path / f"{ticker}.parquet")

    def __eq__(self, value):
        if isinstance(value, CachedFile):
            return self.base_path == value.base_path and self.serialized_path == value.serialized_path
        return False
    
        
