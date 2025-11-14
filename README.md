# algo-trader

A small Backtrader-based research/backtest project with yfinance data ingestion, fractional sizing and a couple of example strategies.

This repository contains utilities to download OHLCV data via `yfinance`, run backtests with `backtrader`, and print analyzer-based statistics (including a CAGR calculation that can exclude a warm-up period).

## Quick start

1. Create a Python virtual environment and install dependencies (example):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you don't have `requirements.txt`, install the main dependencies directly (recommended versions may vary):

```bash
pip install backtrader yfinance pandas matplotlib
```

2. Run the backtest runner (example):

```bash
python backtest.py
```

The `backtest.py` script contains a `main()` that demonstrates how to instantiate strategies and run `run_backtest(...)`.

## Files of interest

- `backtest.py` — runner: sets up `Cerebro`, loads data (via `data_loader.py`), attaches analyzers, and calls `print_stats()`.
- `data_loader.py` — helper to download OHLCV data using `yfinance` and normalize DataFrames for Backtrader.
- `stats.py` — installers and `print_stats()` helper that prints portfolio value, remaining cash, analyzer summaries and computes CAGR. `print_stats()` accepts an optional `warm_up` parameter to exclude a warm-up period from the CAGR calculation. When `warm_up=None` the function will attempt to infer a reasonable warm-up from the active strategy (by inspecting commonly named params and indicator period settings).
- `sizers.py` — fractional sizer implementation for fractional-size orders.
- `indicator_strategy.py` — example indicator-based strategy (SMA/RSI/MACD) with bracket entries and improved order logging.
- `random_strategy.py` — a simple strategy that buys a fixed currency amount every N bars.
- `fixed_schedule_strategy.py` — scheduled portfolio investing strategy with weight validation and scaling to available cash.

## Warm-up / CAGR notes

- `print_stats(..., warm_up=...)` accepts either an integer (days), a `datetime.timedelta`, or a pandas Timedelta-like object to exclude initial warm-up days from CAGR.
- If you pass `warm_up=None` the function will try to auto-deduce warm-up from the strategy (looks for common `warmup` attributes/params and inspects attached indicators for the largest period). This heuristic assumes daily bars; if you use intraday bars or different compression, pass `warm_up` explicitly or convert periods to calendar days.

## Analyzer output

`print_stats()` prints:
- Final portfolio value and remaining cash
- Annualized return (CAGR), optionally excluding warm-up
- Per-year returns from Backtrader's `TimeReturn`
- Approximate annualized volatility (from daily returns)
- Sharpe ratio and max drawdown (when analyzers are attached)
- Any remaining open positions after the run

## Next steps and tips

- If you want exact equity at the warm-up boundary (instead of heuristics), add an equity tracker (record broker.getvalue() each bar) and pass the exact value/time into `print_stats()`.
- For headless plotting use `matplotlib.use('Agg')` or save plots to PNG to avoid Tk GUI dependencies.
- If you want me to add a `requirements.txt`, CLI flags for `backtest.py`, or a small unit test for the CAGR-warmup behavior, tell me which you prefer and I'll add it.

---
Small project — designed to be a working research skeleton. Open issues or suggest features and I can implement them directly.
