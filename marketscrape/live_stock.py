import asyncio
import pandas as pd
from threading import Thread
from alpaca.data.live import StockDataStream
from alpaca.data.models import Bar

class StockLive:
    """Live stock object that subscribes to live stock data and stores it in memory."""

    def __init__(self, symbol: str, stream: StockDataStream):
        self.symbol = symbol
        self.stream = stream
        self._raw_data = [] # Store as list of dicts for speed
        self._running = False

    async def _run_stream(self):
        """Internal coroutine to manage the subscription."""
        self._running = True
        await self.stream.subscribe_bars(self.handle_bar, self.symbol)
        # _run() is the actual Alpaca method to start the listener
        await self.stream._run() 

    def run_in_background(self):
        """Starts the stream in a separate background thread."""
        def start_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._run_stream())

        self.thread = Thread(target=start_loop, daemon=True)
        self.thread.start()

    async def handle_bar(self, bar: Bar):
        """Handler called whenever a new bar arrives."""
        # Append as a simple dict to avoid expensive DataFrame overhead
        self._raw_data.append({
            "timestamp": bar.timestamp,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })
        print(f"Received bar for {self.symbol} at {bar.timestamp}")

    def get_bars(self) -> pd.DataFrame:
        """Return the DataFrame of stored bars."""
        if not self._raw_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(self._raw_data)
        df.set_index("timestamp", inplace=True)
        return df

    async def stop(self):
        """Stop the subscription."""
        self._running = False
        await self.stream.unsubscribe_bars(self.symbol)
        await self.stream.stop_ws()