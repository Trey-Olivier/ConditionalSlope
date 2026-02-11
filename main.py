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

    print("Alpaca POD initialized")

    # 4. run tests on market scrape
    shistory = stock_history(symbol="AAPL", historical_client=alpaca.historical).get_historical_bars(start="2024-01-01", end="2024-01-31")
    print("Historical data test complete")
    print(shistory.head())

    # 5. test live stock subscription
    stock_live = StockLive(symbol="AAPL", stream=alpaca.stream)
    stock_live.run_in_background()
    print("Live stock subscription test started. Waiting for data...")

    

    




if __name__ == "__main__":
    main()
