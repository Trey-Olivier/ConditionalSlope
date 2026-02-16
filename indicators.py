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
        """RSI is a momentum oscillator that measures the speed and change of price 
        movements. It is important because it helps traders identify 
        overbought and oversold conditions in the market."""
        
        delta = df[column].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9, column: str = 'close') -> pd.DataFrame:
        """MACD is good for identifying trend direction and momentum. 
        It consists of the MACD line (difference between fast and slow EMA) 
        and the signal line (EMA of the MACD line)."""

        ema_fast = Indicators.exponential_moving_average(df, span=fast_period, column=column)
        ema_slow = Indicators.exponential_moving_average(df, span=slow_period, column=column)
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        return pd.DataFrame({'MACD': macd_line, 'Signal': signal_line})
    
    @staticmethod
    def vwap(df: pd.DataFrame, column_price: str = 'close', column_volume: str = 'volume') -> pd.Series:
        """Volume Weighted Average Price (VWAP) is a trading benchmark that gives the 
        average price a security has traded at throughout the day, based on both 
        volume and price. It is important because it provides traders with insight 
        into the true average price of a security, taking into account the 
        volume of trades at different price levels."""

        cumulative_volume = df[column_volume].cumsum()
        cumulative_vwap = (df[column_price] * df[column_volume]).cumsum() / cumulative_volume
        return cumulative_vwap
    
    @staticmethod
    def stochastic_oscillator(df: pd.DataFrame, k_period: int = 14, d_period: int = 3, column: str = 'close') -> pd.DataFrame:
        """stochastic oscillator is a momentum indicator that compares a particular 
        closing price of a security to a range of its prices over a certain period 
        of time. The sensitivity of the oscillator to market movements can be reduced 
        by adjusting the time period or by taking a moving average of the result. 
        It is important because it helps traders identify overbought and oversold 
        conditions in the market, which can signal potential reversals or continuations 
        in price trends."""

        low_min = df[column].rolling(window=k_period).min()
        high_max = df[column].rolling(window=k_period).max()
        percent_k = 100 * (df[column] - low_min) / (high_max - low_min)
        percent_d = percent_k.rolling(window=d_period).mean()
        return pd.DataFrame({'%K': percent_k, '%D': percent_d})