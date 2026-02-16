import os

import pandas as pd

class Cache:
    """Pickle and in-memory cache for storing and retrieving dataframes."""
    
    def __init__(self):
        self._cache = {}
    
    def set(self, key: str, value: pd.DataFrame):
        """Store a dataframe in the cache."""
        self._cache[key] = value
    
    def get(self, key: str) -> pd.DataFrame:
        """Retrieve a dataframe from the cache. Returns None if not found."""
        return self._cache.get(key, None)
    
    def clear(self):
        """Clear the entire cache."""
        self._cache.clear()

    def remove(self, key: str):
        """Remove a specific key from the cache."""
        if key in self._cache:
            del self._cache[key]
            
            del_path = f'{key}.pkl'
            if os.path.exists(del_path):
                os.remove(del_path)

    def pickle_to_disk(self, key: str, filepath: str):
        """Pickle a dataframe from the cache to disk."""
        df = self.get(key)
        if df is not None:
            df.to_pickle(filepath/key + '.pkl')

    def load_from_disk(self, key: str, filepath: str) -> pd.DataFrame:
        """Load a pickled dataframe from disk into the cache."""
        try:
            df = pd.read_pickle(filepath/key + '.pkl')
            self.set(key, df)
            return df
        except FileNotFoundError:
            return None
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        return key in self._cache