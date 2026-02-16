import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus, AssetClass

from client.alpaca_pod import AlpacaPod
from client.alpaca_config import AlpacaConfig
from client.create_alpaca_clients import create_alpaca_clients
from marketscrape.stock_history import StockHistoryBatch

# --- SYSTEM CONFIG ---
VERSION = "1.1.0"
BASEPATH = Path(__file__).resolve().parent
DATADIR = BASEPATH / "data"
DATADIR.mkdir(exist_ok=True)



def main():
    try:
        cfg = AlpacaConfig.load_alpaca_config()
        trading, historical, stream = create_alpaca_clients(cfg)
        
    except Exception as e:
        print(f"FATAL: {e}")

if __name__ == "__main__":
    main()