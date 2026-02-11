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
VERSION = "1.4.1"
BASEPATH = Path(__file__).resolve().parent
PLOT_PATH = BASEPATH / "Backtest_Results"
PLOT_PATH.mkdir(exist_ok=True)
RESULTSPATH = PLOT_PATH / VERSION.replace(".", "_")
RESULTSPATH.mkdir(exist_ok=True)

# ==========================================
# 1. CORE STRATEGY & ANALYTICS ENGINE
# ==========================================

def calculate_fees(dollar_volume: float) -> float:
    """SEC + FINRA regulatory fee approximation."""
    return dollar_volume * 0.000174

def backtest_rsi_reversion(df: pd.DataFrame, initial_cash=10000) -> pd.DataFrame:
    df = df.copy()
    
    # --- Indicator Vectorization ---
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss)))
    df['sma_filter'] = df['close'].rolling(50).mean()

    # --- State Management ---
    df['entry_signal'] = False
    df['exit_signal'] = False
    df['stop_signal'] = False
    
    current_pos, cash, units, entry_price = 0, initial_cash, 0, 0.0
    equity_curve = []
    exposure_tracker = [] 

    for i in range(len(df)):
        price = df['close'].iloc[i]
        rsi = df['rsi'].iloc[i]
        sma = df['sma_filter'].iloc[i]
        
        if pd.isna(rsi) or pd.isna(sma):
            equity_curve.append(cash)
            exposure_tracker.append(0)
            continue

        # Logic: Position Management (Sell)
        if current_pos == 1:
            pnl = (price - entry_price) / entry_price
            if rsi >= 70 or pnl <= -0.07:
                sell_val = units * price
                cash = sell_val - calculate_fees(sell_val)
                df.at[df.index[i], 'stop_signal' if pnl <= -0.07 else 'exit_signal'] = True
                current_pos, units = 0, 0
            
        # Logic: Entry (Buy)
        elif rsi <= 40 and price > sma:
            units = cash / price
            entry_price = price
            current_pos, cash = 1, 0 
            df.at[df.index[i], 'entry_signal'] = True

        exposure_tracker.append(1 if current_pos == 1 else 0)
        equity_curve.append(cash + (units * price))

    df['equity'] = equity_curve
    df['in_market'] = exposure_tracker
    df['strategy_return'] = df['equity'].pct_change().fillna(0)
    return df

# ==========================================
# 2. VISUALIZATION ENGINE
# ==========================================

def plot_detailed_analysis(df, symbol, total_ret, path):
    fig = plt.figure(figsize=(14, 10))
    ax1 = plt.subplot2grid((4, 1), (0, 0), rowspan=3)
    ax2 = plt.subplot2grid((4, 1), (3, 0), rowspan=1, sharex=ax1)

    # Panel 1: Price, SMA, and Signals
    ax1.plot(df.index, df['close'], color='black', alpha=0.3, label='Price')
    ax1.plot(df.index, df['sma_filter'], color='orange', lw=1.5, label='50-SMA')
    
    # Boolean indexing for markers
    ax1.scatter(df[df['entry_signal']].index, df[df['entry_signal']]['close'], marker='^', color='green', s=120, label='BUY', zorder=5)
    ax1.scatter(df[df['exit_signal']].index, df[df['exit_signal']]['close'], marker='v', color='blue', s=120, label='WIN', zorder=5)
    ax1.scatter(df[df['stop_signal']].index, df[df['stop_signal']]['close'], marker='x', color='red', s=120, label='STOP', zorder=5)
    
    ax1.set_title(f"{symbol} Analysis | Return: {total_ret:.2f}%", fontsize=14)
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.2)

    # Panel 2: RSI
    ax2.plot(df.index, df['rsi'], color='purple', lw=1)
    ax2.axhline(40, color='green', ls='--', alpha=0.4) 
    ax2.axhline(45, color='blue', ls='--', alpha=0.4)
    ax2.fill_between(df.index, 40, 45, color='purple', alpha=0.1)
    ax2.set_ylim(0, 100)
    ax2.set_ylabel('RSI (14)')
    ax2.grid(True, alpha=0.2)

    plt.tight_layout()
    plt.savefig(RESULTSPATH / f"{symbol}_detailed.png")
    plt.close()

# ==========================================
# 3. MAIN STRESS TEST RUNNER
# ==========================================

def run_stress_test(alpaca: AlpacaPod, num_samples=9):
    print(f"⏳ Processing {num_samples} Assets...")
    
    assets = alpaca.trading.get_all_assets(GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY))
    tickers = [a.symbol for a in assets if a.tradable and a.fractionable]
    symbols = random.sample(tickers, min(len(tickers), num_samples))
    
    history = StockHistoryBatch(alpaca.historical)
    master_df = history.get_historical_bars(symbols, datetime.now() - timedelta(days=500), datetime.now())

    all_returns, summary = [], []

    for symbol in symbols:
        try:
            if symbol not in master_df.index.get_level_values(0): continue
            df = backtest_rsi_reversion(master_df.xs(symbol))
            
            # --- Analytics Math ---
            final_val = df['equity'].iloc[-1]
            total_ret = ((final_val - 10000) / 10000) * 100
            
            # Drawdown & Sharpe
            rolling_max = df['equity'].cummax()
            mdd = ((df['equity'] - rolling_max) / rolling_max).min() * 100
            std = df['strategy_return'].std()
            sharpe = (df['strategy_return'].mean() / std) * np.sqrt(252) if std != 0 else 0
            
            # Exposure %
            exposure_pct = (df['in_market'].sum() / len(df)) * 100
            
            summary.append({
                "Ticker": symbol, "Ret": total_ret, 
                "W/L": f"{df['exit_signal'].sum()}/{df['stop_signal'].sum()}",
                "MDD": mdd, "Sharpe": sharpe, "Exp": exposure_pct
            })
            
            all_returns.append(df['strategy_return'].rename(symbol))
            plot_detailed_analysis(df, symbol, total_ret, PLOT_PATH)

        except Exception as e:
            print(f" ⚠️ {symbol} Error: {e}")

    # --- FINAL CONSOLIDATED REPORTING ---
    print("\n" + "="*85)
    print(f"{'SYMBOL':<10} | {'RET %':<9} | {'W/L':<7} | {'MDD %':<10} | {'SHARPE':<7} | {'EXP %':<6}")
    print("-" * 85)
    for s in summary:
        print(f"{s['Ticker']:<10} | {s['Ret']:>8.2f}% | {s['W/L']:<7} | {s['MDD']:>9.2f}% | {s['Sharpe']:>7.2f} | {s['Exp']:>5.1f}%")
    
    if all_returns:
        port_df = pd.concat(all_returns, axis=1).fillna(0)
        port_df['equity'] = 10000 * (1 + port_df.mean(axis=1)).cumprod()
        
        plt.figure(figsize=(12,6))
        port_df['equity'].plot(title="Portfolio Aggregate Equity (Compounded)", color='teal', lw=2)
        plt.grid(True, alpha=0.3)
        plt.savefig(RESULTSPATH / "portfolio_summary.png")
        plt.show()

def main():
    try:
        cfg = AlpacaConfig.load_alpaca_config()
        trading, historical, stream = create_alpaca_clients(cfg)
        run_stress_test(AlpacaPod(trading, historical, stream))
    except Exception as e:
        print(f"FATAL: {e}")

if __name__ == "__main__":
    main()