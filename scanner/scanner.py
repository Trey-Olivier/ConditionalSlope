import pandas as pd
import time
from scanner.indicators import Indicators
from scanner.clean_data import CleanData
from marketscrape.stock_history import StockHistoryBatch
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus, AssetClass
from client.alpaca_pod import AlpacaPod


class Scanner:

    def __init__(self,alpaca_pod: AlpacaPod):

        self._pod = alpaca_pod
        self._trading_client = alpaca_pod.trading
        self._historical_client = alpaca_pod.historical
        self._stream_client = alpaca_pod.stream

    # =====================================================
    # Universe
    # =====================================================

    def get_stock_universe(self) -> pd.DataFrame:
        try:
            request = GetAssetsRequest(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY
            )

            assets = self._trading_client.get_all_assets(request)
            df = pd.DataFrame([a.model_dump() for a in assets])

            df = df[
                (df['tradable']) &
                (df['exchange'].isin(['NASDAQ', 'NYSE'])) &
                (df['shortable'])
            ]

            return df[['symbol']]

        except Exception as e:
            print(f"Universe fetch error: {e}")
            return pd.DataFrame()

    # =====================================================
    # Core Setup Logic
    # =====================================================

    @staticmethod
    def is_long_setup(df: pd.DataFrame) -> pd.Series:
        ema50 = Indicators.ema(df, 50)
        ema200 = Indicators.ema(df, 200)

        return (df['close'] > ema200) & (ema50 > ema200)

    @staticmethod
    def is_bullish_engulfing(df: pd.DataFrame) -> pd.Series:
        prev = df.groupby(level='symbol').shift(1)

        prev_red = prev['close'] < prev['open']
        curr_green = df['close'] > df['open']

        engulf = (
            (df['close'] >= prev['open']) &
            (df['open'] <= prev['close'])
        )

        return prev_red & curr_green & engulf

    @staticmethod
    def gap_score(df: pd.DataFrame) -> pd.Series:
        prev_close = df.groupby(level='symbol')['close'].shift(1)
        gap = (df['open'] - prev_close) / prev_close
        return gap.clip(0, 0.05) / 0.05

    # =====================================================
    # Scoring Model
    # =====================================================

    def calculate_scores(self, df: pd.DataFrame, weights: dict | None = None) -> pd.Series:

        if weights is None:
            weights = {
                'trend': 0.25,
                'engulf': 0.15,
                'rvol': 0.20,
                'gap': 0.15,
                'rs': 0.25,
            }

        # --- Trend Strength ---
        ema200 = Indicators.ema(df, 200)
        trend_strength = ((df['close'] - ema200) / ema200).clip(0, 0.2) / 0.2

        # --- Engulfing ---
        engulf = self.is_bullish_engulfing(df).astype(float)

        # --- Relative Volume ---
        rvol = Indicators.rvol(df)
        rvol_score = (rvol / 5.0).clip(0, 1)

        # --- Gap ---
        gap = self.gap_score(df)

        # --- Relative Strength ---
        if 'rs_spy' in df.columns:
            rs_sma = df['rs_spy'].groupby(level='symbol').transform(
                lambda x: x.rolling(50, min_periods=50).mean()
            )
            rs_score = ((df['rs_spy'] - rs_sma) / rs_sma).clip(0, 0.1) / 0.1
        else:
            rs_score = pd.Series(0, index=df.index)

        total: pd.DataFrame = (
            trend_strength * weights['trend'] +
            engulf * weights['engulf'] +
            rvol_score * weights['rvol'] +
            gap * weights['gap'] +
            rs_score * weights['rs']
        )

        return total.fillna(0)

    # =====================================================
    # Daily Ranking
    # =====================================================

    def rank_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['score'] = self.calculate_scores(df)

        last_ts = df.index.get_level_values('timestamp').max()
        today = df[df.index.get_level_values('timestamp') == last_ts]

        today: pd.DataFrame = today[today['score'] > 0.4]

        return today.sort_values('score', ascending=False)

    # =====================================================
    # Relative Strength vs SPY
    # =====================================================

    def add_relative_strength(day_data: pd.DataFrame) -> pd.DataFrame:
        # Fail-safe: Check if SPY actually exists in the index
        if 'SPY' not in day_data.index.get_level_values('symbol'):
            print("CRITICAL: SPY missing from data. Relative Strength set to 0.")
            day_data['rs_spy'] = 1.0
            return day_data

        spy_close = day_data.xs('SPY', level='symbol')['close']
        candidates = day_data.drop(index='SPY', level='symbol', errors='ignore')

        # Use reindex and ffill to ensure SPY dates align with individual stock dates
        candidates['rs_spy'] = (
        candidates.groupby(level='symbol')['close']
        .transform(lambda x: x / spy_close.reindex(x.index.get_level_values('timestamp'), method='ffill').values))

        return candidates

    # =====================================================
    # Intraday Confirmation Layer
    # =====================================================

    @staticmethod
    def intraday_filter(min_df: pd.DataFrame) -> pd.DataFrame:

        min_df = min_df.copy()

        min_df['vwap'] = Indicators.vwap(min_df)
        min_df['intraday_rvol'] = Indicators.intraday_rvol(min_df)

        last_ts = min_df.index.get_level_values('timestamp').max()
        latest = min_df[min_df.index.get_level_values('timestamp') == last_ts]

        confirmed = latest[
            (latest['close'] > latest['vwap']) &
            (latest['intraday_rvol'] > 1.5)
        ]

        return confirmed

    # =====================================================
    # Full Pipeline
    # =====================================================

    def fetch_daily_data(self, symbols: list[str], days: int = 250) -> pd.DataFrame:
        """Fetch daily historical bars in batches to avoid rate limits."""
        all_data = []
        # Alpaca handles max symbols per request (~200 is very stable)
        # Your batch size logic is good, but let's ensure it doesn't exceed Alpaca limits
        batch_size = min(200, self._pod.allowed_rest_calls_per_minute // 2)

        history_loader = StockHistoryBatch(self._pod)

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]

            try:
                df = history_loader.get_historical_bars(
                    symbols=batch,
                    timeframe='1Day',
                    start=pd.Timestamp.now() - pd.Timedelta(days=days),
                    end=pd.Timestamp.now())

                if not df.empty:
                    all_data.append(df)
            
                # 1. Update your pod counter
                self._pod.rest_calls_this_minute += 1
            
                # 2. PHYSICAL PAUSE: Crucial for respecting the 200/min limit
                # This ensures we don't burst all requests in the first 2 seconds
                time.sleep(0.5) 

            except Exception as e:
                print(f"Error fetching batch starting at {batch[0]}: {e}")

        return pd.concat(all_data).sort_index() if all_data else pd.DataFrame()

    
    
    def run_scanner(self):

        print("=== Starting Scan ===")

        universe = self.get_stock_universe()
        if universe.empty:
            print("No universe symbols.")
            return pd.DataFrame(), pd.DataFrame()

        symbols = universe['symbol'].tolist()
        history = StockHistoryBatch(self._pod)

        print(f"Fetching daily bars for {len(symbols)} symbols...")
        all_symbols = symbols + ['SPY']

        day_data = self.fetch_daily_data(all_symbols, days=220)
        day_data = CleanData.clean_stock_data(day_data, timeframe='1Day', allow_bfill=True, resample=False)

        if day_data.empty:
            print("No daily data.")
            return pd.DataFrame(), pd.DataFrame()

        day_data = day_data.sort_index()

        candidates = self.add_relative_strength(day_data)

        print("Scoring daily setups...")
        ranked = self.rank_daily(candidates)

        if ranked.empty:
            print("No candidates passed daily filter.")
            return pd.DataFrame(), pd.DataFrame()

        top_symbols = (
            ranked.head(10)
            .index.get_level_values('symbol')
            .unique()
            .tolist()
        )

        print(f"Top symbols: {top_symbols}")
        print("Fetching intraday data...")

        min_data: pd.DataFrame = history.get_historical_bars(
            symbols=top_symbols,
            timeframe='1Min',
            start=pd.Timestamp.now() - pd.Timedelta(days=15),
            end=pd.Timestamp.now()
        )

        min_data = CleanData.clean_stock_data(min_data, timeframe='1Min', allow_bfill=True, resample=False)

        if min_data.empty:
            return ranked, pd.DataFrame()

        min_data = min_data.sort_index()

        confirmed = self.intraday_filter(min_data)

        print("=== Scan Complete ===")

        return ranked, confirmed