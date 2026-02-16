import pandas as pd
import numpy as np

class Indicators:
    """Class for calculating technical indicators on stock data."""
    
    @staticmethod
    def moving_average(df: pd.DataFrame, window: int, column: str = 'close') -> pd.Series:
        """Calculate the moving average for a given window size."""
        return df[column].rolling(window=window).mean()
    
    @staticmethod
    def exponential_moving_average(df: pd.DataFrame, span: int, column: str = 'close') -> pd.Series:
        """Calculate the exponential moving average for a given span."""
        return df[column].ewm(span=span, adjust=False).mean()
    
    @staticmethod
    def relative_strength_index(df: pd.DataFrame, period: int = 14, column: str = 'close') -> pd.Series:
        """Calculate the Relative Strength Index (RSI) for a given period."""
        delta = df[column].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def Macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9, column: str = 'close') -> pd.DataFrame:
        """Calculate the Moving Average Convergence Divergence (MACD) and signal line."""
        ema_fast = Indicators.exponential_moving_average(df, span=fast_period, column=column)
        ema_slow = Indicators.exponential_moving_average(df, span=slow_period, column=column)
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        return pd.DataFrame({'MACD': macd_line, 'Signal': signal_line})
    