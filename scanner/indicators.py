import pandas as pd
import numpy as np


class Indicators:
    """
    Group-safe, multi-symbol technical indicators.

    Assumptions:
    - DataFrame uses a MultiIndex with levels: ('symbol', 'timestamp')
    - OR has a 'symbol' column and datetime index
    - Data is sorted by time per symbol
    """

    # =====================================================
    # Internal Helpers
    # =====================================================

    @staticmethod
    def _symbol_index(df: pd.DataFrame) -> pd.Index:
        if 'symbol' in df.index.names:
            return df.index.get_level_values('symbol')
        elif 'symbol' in df.columns:
            return df['symbol']
        else:
            raise ValueError("DataFrame must contain 'symbol' column or index level")

    @staticmethod
    def _timestamp_index(df: pd.DataFrame) -> pd.DatetimeIndex:
        if 'timestamp' in df.index.names:
            return df.index.get_level_values('timestamp')
        elif isinstance(df.index, pd.DatetimeIndex):
            return df.index
        else:
            raise ValueError("DataFrame must have datetime index or 'timestamp' level")

    @staticmethod
    def _groupby_symbol(df: pd.DataFrame):
        return df.groupby(Indicators._symbol_index(df), group_keys=False)

    # =====================================================
    # Moving Averages
    # =====================================================

    @staticmethod
    def sma(df: pd.DataFrame, window: int, column: str = 'close') -> pd.Series:
        return Indicators._groupby_symbol(df)[column].transform(
            lambda x: x.rolling(window, min_periods=window).mean()
        )

    @staticmethod
    def ema(df: pd.DataFrame, span: int, column: str = 'close') -> pd.Series:
        return Indicators._groupby_symbol(df)[column].transform(
            lambda x: x.ewm(span=span, adjust=False).mean()
        )

    # =====================================================
    # RSI (Wilder)
    # =====================================================

    @staticmethod
    def rsi(df: pd.DataFrame, period: int = 14, column: str = 'close') -> pd.Series:
        def _rsi(x: pd.Series) -> pd.Series:
            delta = x.diff()

            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)

            avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
            avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

            rs = avg_gain / avg_loss.replace(0, np.nan)
            return 100 - (100 / (1 + rs))

        return Indicators._groupby_symbol(df)[column].transform(_rsi)

    # =====================================================
    # MACD
    # =====================================================

    @staticmethod
    def macd(
        df: pd.DataFrame,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        column: str = 'close'
    ) -> pd.DataFrame:

        ema_fast = Indicators.ema(df, fast, column)
        ema_slow = Indicators.ema(df, slow, column)

        macd_line = ema_fast - ema_slow

        signal_line = (
            macd_line.groupby(Indicators._symbol_index(df))
            .transform(lambda x: x.ewm(span=signal, adjust=False).mean())
        )

        hist = macd_line - signal_line

        return pd.DataFrame(
            {
                'macd': macd_line,
                'signal': signal_line,
                'hist': hist,
            },
            index=df.index,
        )

    # =====================================================
    # Bollinger Bands
    # =====================================================

    @staticmethod
    def bollinger_bands(
        df: pd.DataFrame,
        window: int = 20,
        stds: float = 2.0,
        column: str = 'close'
    ) -> pd.DataFrame:

        mid = Indicators.sma(df, window, column)

        std = Indicators._groupby_symbol(df)[column].transform(
            lambda x: x.rolling(window, min_periods=window).std()
        )

        upper = mid + stds * std
        lower = mid - stds * std

        return pd.DataFrame(
            {
                'bb_mid': mid,
                'bb_upper': upper,
                'bb_lower': lower,
            },
            index=df.index,
        )

    # =====================================================
    # Relative Volume (Daily)
    # =====================================================

    @staticmethod
    def rvol(df: pd.DataFrame, window: int = 20, column: str = 'volume') -> pd.Series:
        avg_vol = Indicators._groupby_symbol(df)[column].transform(
            lambda x: x.rolling(window, min_periods=window).mean()
        )

        return df[column] / avg_vol.replace(0, np.nan)

    # =====================================================
    # ATR (Average True Range)
    # =====================================================

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        if not {'high', 'low', 'close'}.issubset(df.columns):
            raise ValueError("ATR requires high, low, close columns")

        def _atr(group: pd.DataFrame) -> pd.Series:
            high = group['high']
            low = group['low']
            close = group['close']

            prev_close = close.shift(1)

            tr = pd.concat(
                [
                    high - low,
                    (high - prev_close).abs(),
                    (low - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)

            return tr.ewm(alpha=1 / period, adjust=False).mean()

        return Indicators._groupby_symbol(df).apply(_atr)

    # =====================================================
    # VWAP (Intraday, Session Reset)
    # =====================================================

    @staticmethod
    def vwap(df: pd.DataFrame) -> pd.Series:
        """
        Intraday VWAP.
        Requires minute-level data.
        Resets per symbol per session.
        """

        if not {'high', 'low', 'close', 'volume'}.issubset(df.columns):
            raise ValueError("VWAP requires high, low, close, volume columns")

        ts = Indicators._timestamp_index(df)
        sym = Indicators._symbol_index(df)

        session = ts.floor("D")

        tp = (df['high'] + df['low'] + df['close']) / 3

        grouped = df.groupby([sym, session], group_keys=False)

        cum_vol = grouped['volume'].cumsum()
        cum_vp = (tp * df['volume']).groupby([sym, session]).cumsum()

        return cum_vp / cum_vol.replace(0, np.nan)

    # =====================================================
    # Intraday Relative Volume (Cumulative)
    # =====================================================

    @staticmethod
    def intraday_rvol(df: pd.DataFrame, lookback_days: int = 20) -> pd.Series:
        """
        Intraday cumulative RVOL.
        Compares current session cumulative volume to historical
        average cumulative volume at same minute-of-day.
        """

        ts = Indicators._timestamp_index(df)
        sym = Indicators._symbol_index(df)

        session = ts.floor("D")
        minute = ts.time

        temp = pd.DataFrame(
            {
                'symbol': sym,
                'session': session,
                'minute': minute,
                'volume': df['volume'],
            },
            index=df.index,
        )

        temp['cum_vol'] = temp.groupby(
            ['symbol', 'session']
        )['volume'].cumsum()

        temp['avg_cum_vol'] = (
            temp.groupby(['symbol', 'minute'])['cum_vol']
            .transform(lambda x: x.shift(1).rolling(lookback_days).mean())
        )

        return temp['cum_vol'] / temp['avg_cum_vol'].replace(0, np.nan)