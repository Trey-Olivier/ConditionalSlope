from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from client.alpaca_config import AlpacaConfig

def create_alpaca_clients(cfg: AlpacaConfig):
    trading = TradingClient(
        cfg.api_key,
        cfg.secret_key,
        paper=cfg.paper
    )

    historical = StockHistoricalDataClient(
        cfg.api_key,
        cfg.secret_key
    )

    stream = StockDataStream(
        cfg.api_key,
        cfg.secret_key
    )

    return trading, historical, stream
