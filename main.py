from client.alpaca_pod import AlpacaPod
from client.create_alpaca_clients import create_alpaca_clients
from client.alpaca_config import AlpacaConfig

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
    print(alpaca.trading.get_account().cash)




if __name__ == "__main__":
    main()
