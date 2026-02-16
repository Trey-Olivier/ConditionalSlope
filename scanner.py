from indicators import Indicators

from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus, AssetClass
from client.alpaca_pod import AlpacaPod
import pandas as pd

class Scanner:



    def __init__(self):
        self.trading_client = AlpacaPod.trading

    def get_stock_universe(self) -> pd.DataFrame:
        """Get the stock universe from Alpaca API."""
        trading_client = self.trading_client
        try:
            assets_request = GetAssetsRequest(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY
            )
            assets = trading_client.get_all_assets(assets_request)
            df = pd.DataFrame([asset.model_dump() for asset in assets])
            return df
        
        except Exception as e:
            print(f"Error fetching stock universe: {e}")
            return pd.DataFrame()
        
       
        
        