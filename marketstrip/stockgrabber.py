import alpaca as ap
import pandas as pd

class Stock():
    """A class to represent a stock and its associated data."""

    _ticker: str

    def __init__(self, ticker):

        self._ticker = ticker

    def scrapeMarket(self) -> pd.DataFrame:
        
        ticker = 
        data = ticker

        return data

    def getTicker(self) -> str:
        return self._ticker
    
    def getStockName(self) -> str:
        ticker = 
        return ticker
    
    def __eq__(self, value):
        if isinstance(value, Stock):
            return self._ticker == value._ticker
        return False
    


    



        