from client.alpaca_pod import AlpacaPod
from client.create_alpaca_clients import create_alpaca_clients
from client.alpaca_config import AlpacaConfig
from marketscrape.stock_history import stock_history
from marketscrape.live_stock import StockLive

def main():
    # 1. load config
    cfg = AlpacaConfig.load_alpaca_config()

    # 2. create clients
    trading, historical, stream = create_alpaca_clients(cfg)

    # 3. assemble pod
    alpaca = AlpacaPod(
        trading=trading,
        historical=historical,
        stream=stream
    )

    

    

    




if __name__ == "__main__":
    main()
