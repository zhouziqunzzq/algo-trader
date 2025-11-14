"""
Data loading utilities for Backtrader using yfinance.

Exposes:
 - download_ohlc_yf(tickers, fromdate=None, todate=None, auto_adjust=False, threads=True)
   Returns a dict mapping ticker -> pandas.DataFrame with columns:
     Open, High, Low, Close, Volume, (Adj Close if available)

Notes:
 - Uses a single yfinance download call for multiple tickers (efficient).
 - Normalizes timezone to tz-naive and title-cases column names.
 - Handles single- and multi-ticker shapes returned by yfinance.
"""
from __future__ import annotations

from typing import Dict, Iterable, Optional
import datetime as _dt


def _tz_naive_index(df):
    idx = getattr(df, "index", None)
    if idx is not None and getattr(idx, "tz", None) is not None:
        try:
            df.index = df.index.tz_convert(None)
        except Exception:
            try:
                df.index = df.index.tz_localize(None)
            except Exception:
                pass
    return df


def download_ohlc_yf(
    tickers: Iterable[str],
    fromdate: Optional[_dt.datetime] = None,
    todate: Optional[_dt.datetime] = None,
    *,
    auto_adjust: bool = False,
    threads: bool = True,
):
    """
    Download OHLCV for one or more tickers using yfinance in a single request.

    Args:
      tickers: iterable of symbols (e.g., ["SPY", "QQQ"]).
      fromdate: inclusive start datetime (timezone-naive). If None, yfinance default.
      todate: inclusive end datetime (timezone-naive). If not None, one day is added
              to satisfy yfinance's exclusive 'end' semantics.
      auto_adjust: whether to auto-adjust OHLC (dividends/splits) in yfinance.
      threads: whether to enable threaded downloads inside yfinance (harmless for single call).

    Returns:
      dict: {ticker: pandas.DataFrame}
    """
    try:
        import yfinance as yf
        import pandas as pd
    except Exception as e:
        raise RuntimeError(
            "yfinance (and pandas) are required. Install with: pip install yfinance pandas"
        ) from e

    # yfinance expects 'end' to be exclusive; add one day to make inclusive
    start = fromdate if fromdate is not None else None
    end = (todate + _dt.timedelta(days=1)) if todate is not None else None

    tickers = list(tickers)
    if not tickers:
        return {}

    df_all = yf.download(
        tickers,
        start=start,
        end=end,
        progress=False,
        threads=threads,
        group_by="ticker",
        auto_adjust=auto_adjust,
    )

    # Unwrap (df, info) or similar
    if isinstance(df_all, tuple) and len(df_all) > 0:
        df_all = df_all[0]

    out: Dict[str, pd.DataFrame] = {}

    if df_all is None:
        return out

    is_multi = isinstance(getattr(df_all, "columns", None), pd.MultiIndex)

    for t in tickers:
        if is_multi:
            if t in df_all.columns.get_level_values(0):
                df = df_all[t]
            else:
                # ticker not present in returned data
                continue
        else:
            # Single-ticker result returned as flat columns
            df = df_all.copy()

        if df is None or df.empty:
            continue

        # Cleanup
        df = df.dropna(how="all")
        df = _tz_naive_index(df)

        # Normalize column names to Title case expected by Backtrader PandasData
        # Common yfinance columns: 'Open','High','Low','Close','Adj Close','Volume'
        colmap = {c: str(c).title() for c in df.columns}
        df = df.rename(columns=colmap)

        out[t] = df

    return out
