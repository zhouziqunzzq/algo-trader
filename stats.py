import math
import statistics
import datetime
import pandas as pd
import backtrader as bt

def install_daily_stats_analyzers(cerebro):
    """
    Install common statistics analyzers into the given Cerebro instance.
    """
    cerebro.addanalyzer(
        bt.analyzers.TimeReturn, _name='timereturn',
        timeframe=bt.TimeFrame.Years)
    cerebro.addanalyzer(
        bt.analyzers.TimeReturn, _name='daily_return',
        timeframe=bt.TimeFrame.Days, compression=1)
    cerebro.addanalyzer(
        bt.analyzers.SharpeRatio, _name='sharpe',
        riskfreerate=0.02)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

def print_stats(
        cerebro, strat, df_map, cash,
        fromdate, todate, warm_up=None, freq: str = "daily",
    ):
    """
    Print post-run statistics extracted from the Cerebro/broker and analyzers.

    Parameters
    ----------
    cerebro : backtrader.Cerebro
        The Cerebro instance used for the backtest.
    strat : backtrader.Strategy
        The strategy instance (used to read analyzers).
    df_map : dict
        Mapping ticker -> pandas.DataFrame used to infer actual data range.
    cash : float
        Starting cash used for CAGR start value.
    fromdate, todate : datetime.date or datetime-like
        Fallback date range if df_map is not available.
    warm_up : None, int, or datetime.timedelta, optional
        If provided, exclude this warm-up period from the CAGR calculation.
        Accepted forms:
          - int : number of days to exclude from the start
          - datetime.timedelta / pandas.Timedelta-like : amount to exclude
          - dict with key 'days' may also be accepted (best-effort)
    freq : {"daily", "weekly"}, optional
        Frequency of the returns used by the 'daily_return' analyzer.
        - "daily": analyzer holds daily returns  -> annualize with 252 periods
        - "weekly": analyzer holds weekly returns -> annualize with 52 periods
    """
    print(f"Final Portfolio Value: {cerebro.broker.getvalue():.2f}")

    # Remaining cash after strategy execution
    remaining_cash = cerebro.broker.get_cash()
    print(f"Remaining Cash: {remaining_cash:.2f}")

    # Compute simple annualized return (CAGR) using actual data timestamps (first/last available dates)
    try:
        end_value = float(cerebro.broker.getvalue())
        start_value = float(cash)

        # Determine warm_up timedelta (default zero)
        try:
            if warm_up is None:
                warm_up_td = datetime.timedelta(0)
            elif isinstance(warm_up, (int, float)):
                warm_up_td = datetime.timedelta(days=int(warm_up))
            elif isinstance(warm_up, datetime.timedelta):
                warm_up_td = warm_up
            else:
                # try pandas-friendly parsing
                warm_up_td = pd.to_timedelta(warm_up).to_pytimedelta()
        except Exception:
            warm_up_td = datetime.timedelta(0)

        # Prefer actual data range from loaded data frames (df_map) if available
        try:
            if df_map:
                starts = [df.index.min() for df in df_map.values() if hasattr(df, 'index') and len(df.index) > 0]
                ends = [df.index.max() for df in df_map.values() if hasattr(df, 'index') and len(df.index) > 0]
                if starts and ends:
                    actual_start = min(starts)
                    actual_end = max(ends)
                    # pandas Timestamp subtraction returns Timedelta
                    total_days = (actual_end - actual_start).days
        except Exception:
            total_days = None

        # Fallback to provided fromdate/todate if no data range available
        if (total_days is None or total_days <= 0) and fromdate is not None and todate is not None:
            try:
                actual_start = fromdate
                actual_end = todate
                total_days = (todate - fromdate).days
            except Exception:
                total_days = None

        if total_days and total_days > 0 and start_value > 0:
            # Prefer analyzer-based daily returns to precisely exclude warm-up
            cagr_printed = False
            try:
                if hasattr(strat.analyzers, 'daily_return'):
                    dr = strat.analyzers.daily_return.get_analysis() or {}
                    # convert to list of (timestamp, return)
                    daily_pairs = []
                    for k, v in dr.items():
                        try:
                            ts = pd.to_datetime(k)
                            daily_pairs.append((ts, float(v)))
                        except Exception:
                            continue
                    daily_pairs.sort()
                    if daily_pairs:
                        actual_start_ts = pd.to_datetime(actual_start)
                        warmup_end_ts = actual_start_ts + pd.to_timedelta(warm_up_td)
                        # filter returns on or after warmup_end_ts
                        filtered = [r for ts, r in daily_pairs if ts >= warmup_end_ts]
                        if filtered:
                            cumulative = 1.0
                            for r in filtered:
                                cumulative *= (1.0 + r)
                            cumulative_return = cumulative - 1.0
                            days_period = (pd.to_datetime(actual_end) - max(warmup_end_ts, actual_start_ts)).days
                            if days_period > 0:
                                years = days_period / 365.25
                                cagr = (1.0 + cumulative_return) ** (1.0 / years) - 1.0
                                print(f"Annualized Return (CAGR) excluding warm-up: {cagr * 100.0:.2f}% over {years:.2f} years (warm_up={warm_up_td})")
                                cagr_printed = True
            except Exception:
                cagr_printed = False

            if not cagr_printed:
                # fallback: subtract warm_up days from the span and compute using start/end values
                try:
                    warm_up_days = int(pd.to_timedelta(warm_up_td).days)
                except Exception:
                    warm_up_days = 0
                adj_days = max(0, total_days - warm_up_days)
                if adj_days > 0:
                    years = adj_days / 365.25
                    cagr = (end_value / start_value) ** (1.0 / years) - 1.0
                    print(f"Annualized Return (CAGR) excluding warm-up (~{warm_up_days} days): {cagr * 100.0:.2f}% over {years:.2f} years")
    except Exception:
        pass

    # Analyzer-based summary: per-year returns, annualized vol, Sharpe, max drawdown
    try:
        # Yearly returns
        if hasattr(strat.analyzers, 'timereturn'):
            tr = strat.analyzers.timereturn.get_analysis()
            if tr:
                print("Per-year returns (from TimeReturn):")
                for y, r in sorted(tr.items()):
                    # keys may be datetime.date or string
                    yr = getattr(y, 'year', y)
                    print(f"  {yr}: {r * 100.0:.2f}%")

        # Returns -> annualized volatility
        vol_ann = None
        if hasattr(strat.analyzers, 'daily_return'):
            dr = strat.analyzers.daily_return.get_analysis()
            daily_vals = [float(v) for v in (dr.values() if isinstance(dr, dict) else [])]
            if daily_vals:
                sd = statistics.pstdev(daily_vals)

                # Pick annualization factor based on freq
                if freq == "weekly":
                    periods_per_year = 52.0
                else:
                    # default: daily
                    periods_per_year = 252.0

                vol_ann = sd * math.sqrt(periods_per_year)
                print(f"Annualized Volatility (approx): {vol_ann * 100.0:.2f}%")

        # Sharpe
        if hasattr(strat.analyzers, 'sharpe'):
            try:
                sh_raw = strat.analyzers.sharpe.get_analysis()
                try:
                    sharpe = float(sh_raw)
                except Exception:
                    # try dict-like
                    if isinstance(sh_raw, dict):
                        # pick first numeric value
                        sharpe = None
                        for v in sh_raw.values():
                            try:
                                sharpe = float(v)
                                break
                            except Exception:
                                continue
                    else:
                        sharpe = None
                if sharpe is not None:
                    print(f"Sharpe Ratio: {sharpe:.2f}")
            except Exception:
                pass

        # Drawdown
        if hasattr(strat.analyzers, 'drawdown'):
            dd = strat.analyzers.drawdown.get_analysis()
            maxdd = None
            if isinstance(dd, dict):
                if 'max' in dd and isinstance(dd['max'], dict) and 'drawdown' in dd['max']:
                    maxdd = dd['max']['drawdown']
                elif 'maxdrawdown' in dd:
                    maxdd = dd['maxdrawdown']
            if maxdd is not None:
                print(f"Max Drawdown: {maxdd:.2f}%")
    except Exception:
        # analyzer summary best-effort; ignore errors
        pass

    # Print any open positions that remain
    open_positions = []
    for data in cerebro.datas:
        pos = cerebro.broker.getposition(data)
        if pos.size:
            last_price = data.close[0] if len(data) else float('nan')
            market_value = pos.size * last_price if last_price == last_price else float('nan')
            open_positions.append((data._name, pos.size, last_price, market_value))

    if open_positions:
        print("Open positions (post-backtest):")
        for name, size, price, value in open_positions:
            print(f"  {name}: size={size:.4f} last_price={price:.2f} value={value:.2f}")
    else:
        print("No open positions remaining.")
