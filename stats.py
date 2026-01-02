import math
import statistics
import datetime
import pandas as pd
import backtrader as bt


def _compute_max_drawdown_period(values_by_date: dict):
    """Return max drawdown period info from an equity series.

    Parameters
    ----------
    values_by_date : dict
        Mapping datetime/date/parseable -> equity value.

    Returns
    -------
    dict | None
        {
          'maxdd_pct': float,
          'peak_date': datetime.date,
          'trough_date': datetime.date,
          'recovery_date': datetime.date | None,
          'peak_value': float,
          'trough_value': float,
        }
    """
    if not values_by_date:
        return None

    s = pd.Series(values_by_date)
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    if s.empty:
        return None

    peak_value = float(s.iloc[0])
    peak_ts = s.index[0]

    maxdd = 0.0
    max_peak_value = peak_value
    max_peak_ts = peak_ts
    max_trough_value = peak_value
    max_trough_ts = peak_ts

    for ts, v in s.items():
        v = float(v)
        if v >= peak_value:
            peak_value = v
            peak_ts = ts
            continue

        dd = (v / peak_value) - 1.0
        if dd < maxdd:
            maxdd = dd
            max_peak_value = peak_value
            max_peak_ts = peak_ts
            max_trough_value = v
            max_trough_ts = ts

    # Find recovery: first date after trough where value >= prior peak.
    recovery_ts = None
    if maxdd < 0:
        after = s[s.index >= max_trough_ts]
        for ts, v in after.items():
            if float(v) >= float(max_peak_value):
                recovery_ts = ts
                break

    return {
        "maxdd_pct": float(-maxdd * 100.0),
        "peak_date": max_peak_ts.date(),
        "trough_date": max_trough_ts.date(),
        "recovery_date": recovery_ts.date() if recovery_ts is not None else None,
        "peak_value": float(max_peak_value),
        "trough_value": float(max_trough_value),
    }


class CashFlowAdjustedReturns(bt.Analyzer):
    """Compute returns adjusted for external cashflows (e.g., deposits).

    This analyzer expects the strategy to expose external cashflows via either:
      - strategy.get_cashflow_by_date() -> {datetime.date: amount}
      - strategy._cashflow_by_date      -> {datetime.date: amount}

    Sign convention: broker-side. Deposits are positive (+X).
    The adjusted daily return is computed as:
        r_t = (V_t - flow_t) / V_{t-1} - 1
    """

    def __init__(self):
        self._prev_value = None
        self._prev_date = None
        self._returns = {}
        self._values = {}
        self._flows = {}

    def _cashflow_for_date(self, dt_date):
        try:
            by_date = self.strategy.get_cashflow_by_date()
        except Exception:
            by_date = getattr(self.strategy, "_cashflow_by_date", None)
        if not by_date:
            return 0.0
        try:
            return float(by_date.get(dt_date, 0.0))
        except Exception:
            return 0.0

    def next(self):
        dt_date = self.datas[0].datetime.date(0)
        value = float(self.strategy.broker.getvalue())
        self._values[dt_date] = value

        if self._prev_value is None:
            self._prev_value = value
            self._prev_date = dt_date
            return

        # Analyzer/strategy execution ordering can vary; in practice, deposits
        # often become visible in broker value between the previous and current
        # analyzer ticks. Apply the previous date's recorded cashflow to the
        # return for the period ending at dt_date.
        flow = float(self._cashflow_for_date(self._prev_date))
        if abs(flow) > 0:
            self._flows[self._prev_date] = flow

        if self._prev_value == 0:
            ret = 0.0
        else:
            ret = (value - flow) / self._prev_value - 1.0

        self._returns[dt_date] = float(ret)
        self._prev_value = value
        self._prev_date = dt_date

    def get_analysis(self):
        return {
            "returns": dict(self._returns),
            "values": dict(self._values),
            "flows": dict(self._flows),
        }


def _xnpv(rate: float, cashflows):
    """Cashflows: list of (datetime.date, amount)."""
    if not cashflows:
        return 0.0
    if rate <= -1.0:
        return float("inf")
    t0 = cashflows[0][0]
    total = 0.0
    for t, cf in cashflows:
        years = (t - t0).days / 365.25
        total += float(cf) / ((1.0 + rate) ** years)
    return total


def xirr(cashflows, max_iter: int = 200):
    """Compute annualized IRR for dated cashflows using bisection.

    Convention: negative = investor contribution, positive = investor inflow.
    """
    if not cashflows or len(cashflows) < 2:
        return None

    cashflows = sorted(cashflows, key=lambda x: x[0])
    has_pos = any(cf > 0 for _, cf in cashflows)
    has_neg = any(cf < 0 for _, cf in cashflows)
    if not (has_pos and has_neg):
        return None

    lo = -0.9999
    hi_candidates = [0.0, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]

    f_lo = _xnpv(lo, cashflows)
    hi = None
    f_hi = None
    for cand in hi_candidates:
        f_cand = _xnpv(cand, cashflows)
        if f_lo == 0:
            return lo
        if f_cand == 0:
            return cand
        if (f_lo > 0 and f_cand < 0) or (f_lo < 0 and f_cand > 0):
            hi = cand
            f_hi = f_cand
            break

    if hi is None:
        return None

    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        f_mid = _xnpv(mid, cashflows)
        if abs(f_mid) < 1e-10:
            return mid
        # maintain bracket
        if (f_lo > 0 and f_mid < 0) or (f_lo < 0 and f_mid > 0):
            hi = mid
            f_hi = f_mid
        else:
            lo = mid
            f_lo = f_mid

    return (lo + hi) / 2.0


def install_daily_stats_analyzers(
    cerebro,
    rf_rate: float = 0.04,
):
    """
    Install common statistics analyzers into the given Cerebro instance.
    """
    cerebro.addanalyzer(
        bt.analyzers.TimeReturn, _name="timereturn", timeframe=bt.TimeFrame.Years
    )
    cerebro.addanalyzer(
        bt.analyzers.TimeReturn,
        _name="daily_return",
        timeframe=bt.TimeFrame.Days,
        compression=1,
    )
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", riskfreerate=rf_rate)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")

    # Cashflow-adjusted returns/value series for strategies that simulate deposits.
    cerebro.addanalyzer(CashFlowAdjustedReturns, _name="flowadj")


def print_stats(
    cerebro,
    strat,
    df_map,
    cash,
    fromdate,
    todate,
    warm_up=None,
    freq: str = "daily",
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

        # Best-effort: detect external cashflows (deposits) recorded by strategy.
        try:
            cashflow_by_date = strat.get_cashflow_by_date()
        except Exception:
            cashflow_by_date = getattr(strat, "_cashflow_by_date", {}) or {}
        has_cashflows = any(abs(float(v)) > 0 for v in cashflow_by_date.values())

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
                starts = [
                    df.index.min()
                    for df in df_map.values()
                    if hasattr(df, "index") and len(df.index) > 0
                ]
                ends = [
                    df.index.max()
                    for df in df_map.values()
                    if hasattr(df, "index") and len(df.index) > 0
                ]
                if starts and ends:
                    actual_start = min(starts)
                    actual_end = max(ends)
                    # pandas Timestamp subtraction returns Timedelta
                    total_days = (actual_end - actual_start).days
        except Exception:
            total_days = None

        # Fallback to provided fromdate/todate if no data range available
        if (
            (total_days is None or total_days <= 0)
            and fromdate is not None
            and todate is not None
        ):
            try:
                actual_start = fromdate
                actual_end = todate
                total_days = (todate - fromdate).days
            except Exception:
                total_days = None

        if total_days and total_days > 0 and start_value > 0:
            # If there are regular deposits, prefer cashflow-aware metrics.
            if has_cashflows:
                try:
                    actual_start_dt = pd.to_datetime(actual_start).date()
                    actual_end_dt = pd.to_datetime(actual_end).date()

                    total_deposits = float(
                        sum(float(v) for v in cashflow_by_date.values())
                    )
                    total_contributed = float(start_value + total_deposits)
                    print(
                        f"Total Contributed (start + deposits): {total_contributed:.2f} "
                        f"(start={start_value:.2f}, deposits={total_deposits:.2f})"
                    )

                    irr_cashflows = [(actual_start_dt, -start_value)]
                    for d, amt in sorted(cashflow_by_date.items(), key=lambda x: x[0]):
                        irr_cashflows.append((d, -float(amt)))
                    irr_cashflows.append((actual_end_dt, end_value))
                    irr = xirr(irr_cashflows)
                    if irr is not None:
                        years_total = max(
                            1e-9, (actual_end_dt - actual_start_dt).days / 365.25
                        )
                        print(
                            f"Money-weighted Return (XIRR): {irr * 100.0:.2f}% over {years_total:.2f} years"
                        )
                except Exception:
                    pass

            # Prefer analyzer-based daily returns to precisely exclude warm-up
            cagr_printed = False
            try:
                if has_cashflows and hasattr(strat.analyzers, "flowadj"):
                    fa = strat.analyzers.flowadj.get_analysis() or {}
                    dr = fa.get("returns", {}) if isinstance(fa, dict) else {}
                    cagr_label = (
                        "Annualized Return (CAGR, cashflow-adjusted) excluding warm-up"
                    )
                elif hasattr(strat.analyzers, "daily_return"):
                    dr = strat.analyzers.daily_return.get_analysis() or {}
                    cagr_label = "Annualized Return (CAGR) excluding warm-up"

                # convert to list of (timestamp, return)
                daily_pairs = []
                for k, v in dr.items() if isinstance(dr, dict) else []:
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
                            cumulative *= 1.0 + r
                        cumulative_return = cumulative - 1.0
                        days_period = (
                            pd.to_datetime(actual_end)
                            - max(warmup_end_ts, actual_start_ts)
                        ).days
                        if days_period > 0:
                            years = days_period / 365.25
                            cagr = (1.0 + cumulative_return) ** (1.0 / years) - 1.0
                            print(
                                f"{cagr_label}: {cagr * 100.0:.2f}% over {years:.2f} years (warm_up={warm_up_td})"
                            )
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
                    print(
                        f"Annualized Return (CAGR) excluding warm-up (~{warm_up_days} days): {cagr * 100.0:.2f}% over {years:.2f} years"
                    )
    except Exception:
        pass

    # Analyzer-based summary: per-year returns, annualized vol, Sharpe, max drawdown
    try:
        # Yearly returns
        try:
            cashflow_by_date = strat.get_cashflow_by_date()
        except Exception:
            cashflow_by_date = getattr(strat, "_cashflow_by_date", {}) or {}
        has_cashflows = any(abs(float(v)) > 0 for v in cashflow_by_date.values())

        if has_cashflows and hasattr(strat.analyzers, "flowadj"):
            fa = strat.analyzers.flowadj.get_analysis() or {}
            rets = fa.get("returns", {}) if isinstance(fa, dict) else {}
            if rets:
                by_year = {}
                for d, r in rets.items():
                    try:
                        yr = pd.to_datetime(d).year
                    except Exception:
                        continue
                    by_year.setdefault(yr, 1.0)
                    by_year[yr] *= 1.0 + float(r)

                print("Per-year returns (cashflow-adjusted):")
                for yr in sorted(by_year.keys()):
                    print(f"  {yr}: {(by_year[yr] - 1.0) * 100.0:.2f}%")
        else:
            if hasattr(strat.analyzers, "timereturn"):
                tr = strat.analyzers.timereturn.get_analysis()
                if tr:
                    print("Per-year returns (from TimeReturn):")
                    for y, r in sorted(tr.items()):
                        # keys may be datetime.date or string
                        yr = getattr(y, "year", y)
                        print(f"  {yr}: {r * 100.0:.2f}%")

        # Returns -> annualized volatility
        vol_ann = None
        daily_vals = None

        if has_cashflows and hasattr(strat.analyzers, "flowadj"):
            fa = strat.analyzers.flowadj.get_analysis() or {}
            rets = fa.get("returns", {}) if isinstance(fa, dict) else {}
            daily_vals = [
                float(v) for v in (rets.values() if isinstance(rets, dict) else [])
            ]
        elif hasattr(strat.analyzers, "daily_return"):
            dr = strat.analyzers.daily_return.get_analysis()
            daily_vals = [
                float(v) for v in (dr.values() if isinstance(dr, dict) else [])
            ]

        if daily_vals:
            sd = statistics.pstdev(daily_vals)

            # Pick annualization factor based on freq
            if freq == "weekly":
                periods_per_year = 52.0
            else:
                # default: daily
                periods_per_year = 252.0

            vol_ann = sd * math.sqrt(periods_per_year)
            label = (
                "Annualized Volatility (cashflow-adjusted, approx)"
                if has_cashflows
                else "Annualized Volatility (approx)"
            )
            print(f"{label}: {vol_ann * 100.0:.2f}%")

        # Sharpe
        if has_cashflows and daily_vals:
            try:
                # Approx daily Sharpe from cashflow-adjusted returns.
                rf_annual = (
                    strat.analyzers.sharpe.params.riskfreerate
                    if hasattr(strat.analyzers.sharpe, "params")
                    else 0.04
                )
                if freq == "weekly":
                    periods_per_year = 52.0
                else:
                    periods_per_year = 252.0
                rf_period = (1.0 + rf_annual) ** (1.0 / periods_per_year) - 1.0
                excess = [r - rf_period for r in daily_vals]
                sd = statistics.pstdev(excess)
                if sd > 0:
                    sharpe = (statistics.mean(excess) / sd) * math.sqrt(
                        periods_per_year
                    )
                    print(f"Sharpe Ratio (cashflow-adjusted, approx): {sharpe:.2f}")
            except Exception:
                pass
        elif hasattr(strat.analyzers, "sharpe"):
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
        if hasattr(strat.analyzers, "drawdown"):
            dd = strat.analyzers.drawdown.get_analysis()
            maxdd = None
            maxdd_len = None
            if isinstance(dd, dict):
                if (
                    "max" in dd
                    and isinstance(dd["max"], dict)
                    and "drawdown" in dd["max"]
                ):
                    maxdd = dd["max"]["drawdown"]
                    if "len" in dd["max"]:
                        maxdd_len = dd["max"]["len"]
                elif "maxdrawdown" in dd:
                    maxdd = dd["maxdrawdown"]
                if maxdd_len is None and "maxlen" in dd:
                    maxdd_len = dd["maxlen"]
            if maxdd is not None:
                if maxdd_len is not None:
                    try:
                        maxdd_len_i = int(maxdd_len)
                        print(f"Max Drawdown: {maxdd:.2f}% ({maxdd_len_i} days)")
                    except Exception:
                        print(f"Max Drawdown: {maxdd:.2f}%")
                else:
                    print(f"Max Drawdown: {maxdd:.2f}%")

                # Best-effort: print drawdown period dates from the equity curve.
                try:
                    fa = (
                        strat.analyzers.flowadj.get_analysis()
                        if hasattr(strat.analyzers, "flowadj")
                        else {}
                    )
                    values = fa.get("values") if isinstance(fa, dict) else None
                    info = _compute_max_drawdown_period(values or {})
                    if info and info.get("peak_date") and info.get("trough_date"):
                        peak_d = info["peak_date"]
                        trough_d = info["trough_date"]
                        rec_d = info.get("recovery_date")
                        if rec_d is None:
                            print(
                                f"Max Drawdown Period: {peak_d} -> (not recovered) (trough={trough_d})"
                            )
                        else:
                            print(
                                f"Max Drawdown Period: {peak_d} -> {rec_d} (trough={trough_d})"
                            )
                except Exception:
                    pass
    except Exception:
        # analyzer summary best-effort; ignore errors
        pass

    # Print any open positions that remain
    open_positions = []
    for data in cerebro.datas:
        pos = cerebro.broker.getposition(data)
        if pos.size:
            last_price = data.close[0] if len(data) else float("nan")
            market_value = (
                pos.size * last_price if last_price == last_price else float("nan")
            )
            open_positions.append((data._name, pos.size, last_price, market_value))

    if open_positions:
        print("Open positions (post-backtest):")
        for name, size, price, value in open_positions:
            print(f"  {name}: size={size:.4f} last_price={price:.2f} value={value:.2f}")
    else:
        print("No open positions remaining.")
