from dataclasses import dataclass
from datetime import datetime
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream

@dataclass
class AlpacaPod:

    # --- clients ---
    trading: TradingClient
    historical: StockHistoricalDataClient
    stream: StockDataStream

    # --- market state ---
    is_market_open: bool = False
    last_clock_check: datetime | None = None

    # --- rate limiting ---
    rest_calls_this_minute: int = 0
    last_rest_reset: datetime | None = None
    

    