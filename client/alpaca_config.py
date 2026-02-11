from __future__ import annotations
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class AlpacaConfig:
    api_key: str
    secret_key: str
    paper: bool

    def load_alpaca_config() -> AlpacaConfig:
        api_key = os.getenv("APCA_API_KEY_ID")
        secret = os.getenv("APCA_API_SECRET_KEY")

        if not api_key or not secret:
            raise RuntimeError("Missing Alpaca credentials")

        return AlpacaConfig(
            api_key=api_key,
            secret_key=secret,
            paper=True
        )

