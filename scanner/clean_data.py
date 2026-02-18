import pandas as pd
import numpy as np

class CleanData:
    """Class for cleaning and preprocessing stock data."""

    @staticmethod
    def standardize_index(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure the index is datetime and sort it."""
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        return df.sort_index()

    @staticmethod
    def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate based on MultiIndex or (index, symbol) if symbol exists."""
        df = df.copy()
        if isinstance(df.index, pd.MultiIndex):
            return df[~df.index.duplicated(keep='first')]
        elif 'symbol' in df.columns:
            return df[~df.reset_index()[['index', 'symbol']].duplicated(keep='first').values]
        else:
            return df[~df.index.duplicated(keep='first')]

    @staticmethod
    def handle_missing_data(df: pd.DataFrame, allow_bfill: bool = False) -> pd.DataFrame:
        """Forward fill missing data, optionally backward fill."""
        df = df.ffill()
        return df.bfill() if allow_bfill else df

    @staticmethod
    def resample_data(df: pd.DataFrame, timeframe: str = '1min') -> pd.DataFrame:
        """Resample OHLCV data by symbol or for single-symbol DataFrame."""
        df = df.copy()

        def _resample_logic(group: pd.DataFrame):
            return group.resample(timeframe).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).ffill()

        if isinstance(df.index, pd.MultiIndex) and 'symbol' in df.index.names:
            return df.groupby(level='symbol', group_keys=False).apply(_resample_logic)
        elif 'symbol' in df.columns:
            return df.groupby('symbol', group_keys=False).apply(_resample_logic)
        else:
            return _resample_logic(df)

    @staticmethod
    def clean_stock_data(df: pd.DataFrame, timeframe: str = '1min', allow_bfill: bool = False, resample: bool = True) -> pd.DataFrame:
        """Full cleaning pipeline."""
        try:
            df = CleanData.standardize_index(df)
            df = CleanData.deduplicate(df)
            df = CleanData.handle_missing_data(df, allow_bfill=allow_bfill)
            if resample:
                df = CleanData.resample_data(df, timeframe=timeframe)
            return df
        except Exception as e:
            print(f"Error cleaning stock data: {e}")
            return pd.DataFrame()