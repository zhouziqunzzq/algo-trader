import math
import backtrader as bt


class MomentumDCA(bt.Strategy):
    """
    MomentumDCA: invest a baseline cash amount on a fixed schedule, but
    modulate each asset's spend by its momentum vs slow trend.

    Momentum signal:
        mom = (SMA_fast - SMA_slow) / SMA_slow
        z = mom / rolling_std(mom)

    Positive z = strong uptrend -> buy more.
    Negative z = weak/downtrend -> buy less.

    Params:
      amount: float        Baseline cash to deploy per investment date (before scaling)
      interval: int        Bars between investment dates (1 = every bar)
      portfolio: dict      {ticker -> weight}, must sum to 1.0
      fast_period: int     Fast SMA length (momentum component)
      slow_period: int     Slow SMA length (trend component)
      vol_window: int      Window for std dev of momentum (for z-score)
      k: float             Sensitivity: m = clip(1 + k*z, m_min, m_max)
      m_min, m_max: float  Multiplier floor/ceiling for per-asset cash
      reserve_multiplier:  Cash buffer divider to avoid margin issues
      _sum_tol: float      Tolerance for weight sum check
    """

    params = dict(
        amount=1000.0,
        interval=20,
        portfolio=None,
        fast_period=50,
        slow_period=200,
        vol_window=60,
        k=0.5,
        m_min=0.5,
        m_max=1.5,
        reserve_multiplier=1.01,
        _sum_tol=1e-9,
    )

    def __init__(self):
        if self.p.portfolio is None:
            raise ValueError("MomentumDCA requires 'portfolio' dict mapping tickers to weights")

        total = sum(self.p.portfolio.values())
        if not math.isclose(total, 1.0, rel_tol=self.p._sum_tol, abs_tol=self.p._sum_tol):
            raise ValueError(f"Portfolio weights must sum to 1.0 (got {total})")

        # Map data feeds by name so portfolio keys can refer to tickers
        self._data_by_name = {d._name: d for d in self.datas}
        missing = [t for t in self.p.portfolio if t not in self._data_by_name]
        if missing:
            raise ValueError(f"Portfolio contains tickers not present in data feeds: {missing}")

        # Indicators per data
        self.fast = {}
        self.slow = {}
        self.mom = {}      # (fast - slow) / slow
        self.mom_std = {}  # rolling std of mom

        for name, d in self._data_by_name.items():
            fast = bt.ind.SMA(d.close, period=self.p.fast_period)
            slow = bt.ind.SMA(d.close, period=self.p.slow_period)
            self.fast[name] = fast
            self.slow[name] = slow

            # Avoid division by zero: Backtrader handles NaNs in early periods
            mom = (fast - slow) / slow
            self.mom[name] = mom
            self.mom_std[name] = bt.ind.StdDev(mom, period=self.p.vol_window)

        self._last_invest_bar = -1

    def log(self, txt):
        dt = self.datas[0].datetime.date(0)
        print(f"{dt} {txt}")

    def _zscore_mom(self, name):
        """z = momentum / stddev; if stddev invalid/small, return 0 (baseline)."""
        raw = float(self.mom[name][0]) if not math.isnan(self.mom[name][0]) else float('nan')
        sd = float(self.mom_std[name][0]) if not math.isnan(self.mom_std[name][0]) else float('nan')

        if (sd is None) or (sd <= 1e-12) or math.isnan(sd) or math.isnan(raw):
            return 0.0
        return raw / sd

    def next(self):
        # Only invest on schedule
        if self._last_invest_bar >= 0 and (len(self) - self._last_invest_bar) < self.p.interval:
            return

        cash_available = self.broker.get_cash()
        max_deployable = cash_available / self.p.reserve_multiplier

        if max_deployable <= 0:
            self.log("Skipping investment: no available cash")
            self._last_invest_bar = len(self)
            return

        baseline = float(self.p.amount)

        desired = {}
        total_desired = 0.0

        # First pass: compute desired spend per asset based on momentum
        for name, weight in self.p.portfolio.items():
            d = self._data_by_name[name]
            price = float(d.close[0])

            if price <= 0 or math.isnan(price):
                continue

            z = self._zscore_mom(name)

            # MomentumDCA: positive z (strong uptrend) -> m > 1 (buy more)
            #               negative z (weak/downtrend) -> m < 1 (buy less)
            m = 1.0 + self.p.k * z
            m = max(self.p.m_min, min(self.p.m_max, m))

            spend = baseline * weight * m
            if spend > 0:
                desired[name] = (spend, z)
                total_desired += spend

        if total_desired <= 0:
            self.log("Nothing to allocate (all spends <= 0 or invalid)")
            self._last_invest_bar = len(self)
            return

        # If over budget, rescale proportionally
        scale = 1.0
        if total_desired > max_deployable:
            scale = max_deployable / total_desired
            self.log(
                f"Scaling spends by {scale:.3f} "
                f"(desired={total_desired:.2f}, cap={max_deployable:.2f})"
            )

        # Place market buys
        for name, (spend, z) in desired.items():
            d = self._data_by_name[name]
            price = float(d.close[0])
            alloc = spend * scale
            size = alloc / price
            if size <= 0:
                continue

            self.buy(data=d, size=size)
            self.log(
                f"Momentum buy {name}: z={z:+.2f} "
                f"alloc={alloc:.2f} price={price:.2f} size={size:.6f}"
            )

        self._last_invest_bar = len(self)

    def notify_order(self, order):
        if order.status in (order.Submitted, order.Accepted):
            return

        if order.status == order.Completed:
            action = 'BUY' if order.isbuy() else 'SELL'
            size = order.executed.size
            price = order.executed.price
            comm = order.executed.comm
            cost = size * price
            self.log(
                f"{action} EXECUTED {order.data._name} "
                f"size={size:.6f} price={price:.2f} cost={cost:.2f} comm={comm:.2f}"
            )
        elif order.status == order.Canceled:
            self.log(f"ORDER CANCELED {order.data._name}")
        elif order.status == order.Margin:
            self.log(f"ORDER MARGIN {order.data._name}")
        elif order.status == order.Rejected:
            self.log(f"ORDER REJECTED {order.data._name}")

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(
            f"TRADE CLOSED {trade.data._name} "
            f"gross={trade.pnl:.2f} net={trade.pnlcomm:.2f}"
        )


class MomentumDCAv2(bt.Strategy):
    """
    MomentumDCA v2: DCA with momentum tilt + trend guard.

    Core ideas:
      - Baseline: invest a fixed cash amount on a fixed schedule (DCA).
      - Momentum tilt: use fast-vs-slow SMA to compute a momentum z-score.
          mom = (SMA_fast - SMA_slow) / SMA_slow
          z   = mom / StdDev(mom)
        Positive z  -> strong uptrend -> buy more.
        Negative z  -> weak/downtrend -> buy less.
      - Trend guard:
          Only allow multipliers > 1 when price > slow SMA AND slow SMA is rising.
      - Momentum floor:
          If z < z_floor (e.g. -1.0), clamp multiplier to m_min
          (i.e. sharply de-emphasize that asset).

    Params:
      amount: float        Baseline cash to deploy per investment date (before scaling)
      interval: int        Bars between investment dates (1 = every bar)
      portfolio: dict      {ticker -> weight}, must sum to 1.0

      fast_period: int     Fast SMA length (momentum component)
      slow_period: int     Slow SMA length (trend component)
      vol_window: int      Window for std dev of momentum (for z-score)

      k: float             Sensitivity:
                             m_raw = 1 + k * z
      m_min, m_max: float  Multiplier floor/ceiling for per-asset cash

      z_floor: float       If z < z_floor, force m = m_min (momentum floor)

      use_trend_guard: bool  Enable trend guard for boosted buys
      slope_lookback: int    Bars to measure SMA slope for trend guard

      reserve_multiplier:  Cash buffer divider to avoid margin issues
      _sum_tol: float      Tolerance for weight sum check
    """

    params = dict(
        amount=1000.0,
        interval=20,
        portfolio=None,

        fast_period=50,
        slow_period=200,
        vol_window=60,

        k=0.5,
        m_min=0.5,
        m_max=1.5,

        z_floor=-1.0,
        use_trend_guard=True,
        slope_lookback=5,

        reserve_multiplier=1.01,
        _sum_tol=1e-9,
    )

    def __init__(self):
        if self.p.portfolio is None:
            raise ValueError("MomentumDCAv2 requires 'portfolio' dict mapping tickers to weights")

        total = sum(self.p.portfolio.values())
        if not math.isclose(total, 1.0, rel_tol=self.p._sum_tol, abs_tol=self.p._sum_tol):
            raise ValueError(f"Portfolio weights must sum to 1.0 (got {total})")

        # Map data feeds by name so portfolio keys can refer to tickers
        self._data_by_name = {d._name: d for d in self.datas}
        missing = [t for t in self.p.portfolio if t not in self._data_by_name]
        if missing:
            raise ValueError(f"Portfolio contains tickers not present in data feeds: {missing}")

        # Indicators per data
        self.fast = {}
        self.slow = {}
        self.mom = {}      # (fast - slow) / slow
        self.mom_std = {}  # rolling std of mom

        for name, d in self._data_by_name.items():
            fast = bt.ind.SMA(d.close, period=self.p.fast_period)
            slow = bt.ind.SMA(d.close, period=self.p.slow_period)
            self.fast[name] = fast
            self.slow[name] = slow

            mom = (fast - slow) / slow
            self.mom[name] = mom
            self.mom_std[name] = bt.ind.StdDev(mom, period=self.p.vol_window)

        self._last_invest_bar = -1

    def log(self, txt):
        dt = self.datas[0].datetime.date(0)
        print(f"{dt} {txt}")

    def _zscore_mom(self, name):
        """z = momentum / stddev; if stddev invalid/small, return 0 (baseline)."""
        mom_val = float(self.mom[name][0]) if not math.isnan(self.mom[name][0]) else float('nan')
        sd = float(self.mom_std[name][0]) if not math.isnan(self.mom_std[name][0]) else float('nan')

        if (sd is None) or (sd <= 1e-12) or math.isnan(sd) or math.isnan(mom_val):
            return 0.0
        return mom_val / sd

    def _trend_ok(self, name):
        """
        Trend guard:
          - price > slow SMA
          - slow SMA slope over lookback > 0
        """
        if not self.p.use_trend_guard:
            return True

        slow = self.slow[name]
        d = self._data_by_name[name]

        lb = min(self.p.slope_lookback, len(slow) - 1)
        if lb <= 0:
            return False

        try:
            price_above = (d.close[0] > slow[0])
            slope_up = (slow[0] - slow[-lb]) > 0
            return price_above and slope_up
        except IndexError:
            return False

    def next(self):
        # Only invest on schedule
        if self._last_invest_bar >= 0 and (len(self) - self._last_invest_bar) < self.p.interval:
            return

        cash_available = self.broker.get_cash()
        max_deployable = cash_available / self.p.reserve_multiplier

        if max_deployable <= 0:
            self.log("Skipping investment: no available cash")
            self._last_invest_bar = len(self)
            return

        baseline = float(self.p.amount)
        desired = {}
        total_desired = 0.0

        # First pass: compute desired spend per asset based on momentum & guards
        for name, weight in self.p.portfolio.items():
            d = self._data_by_name[name]
            price = float(d.close[0])

            if price <= 0 or math.isnan(price):
                continue

            z = self._zscore_mom(name)

            # Raw momentum-based multiplier
            m = 1.0 + self.p.k * z

            # Momentum floor: if z very negative, clamp to minimum
            if z < self.p.z_floor:
                m = self.p.m_min

            # Clip multiplier within [m_min, m_max]
            m = max(self.p.m_min, min(self.p.m_max, m))

            # Trend guard: only allow boosts when trend is OK
            if self.p.use_trend_guard and (m > 1.0) and (not self._trend_ok(name)):
                # Don't boost in bad trends; cap at neutral (1.0)
                m = min(m, 1.0)

            spend = baseline * weight * m
            if spend > 0:
                desired[name] = (spend, z, m)
                total_desired += spend

        if total_desired <= 0:
            self.log("Nothing to allocate (all spends <= 0 or invalid)")
            self._last_invest_bar = len(self)
            return

        # If over budget, rescale proportionally
        scale = 1.0
        if total_desired > max_deployable:
            scale = max_deployable / total_desired
            self.log(
                f"Scaling spends by {scale:.3f} "
                f"(desired={total_desired:.2f}, cap={max_deployable:.2f})"
            )

        # Place market buys
        for name, (spend, z, m) in desired.items():
            d = self._data_by_name[name]
            price = float(d.close[0])
            alloc = spend * scale
            size = alloc / price
            if size <= 0:
                continue

            self.buy(data=d, size=size)
            self.log(
                f"Momentum v2 buy {name}: z={z:+.2f} m={m:.2f} "
                f"alloc={alloc:.2f} price={price:.2f} size={size:.6f}"
            )

        self._last_invest_bar = len(self)

    def notify_order(self, order):
        if order.status in (order.Submitted, order.Accepted):
            return

        if order.status == order.Completed:
            action = 'BUY' if order.isbuy() else 'SELL'
            size = order.executed.size
            price = order.executed.price
            comm = order.executed.comm
            cost = size * price
            self.log(
                f"{action} EXECUTED {order.data._name} "
                f"size={size:.6f} price={price:.2f} cost={cost:.2f} comm={comm:.2f}"
            )
        elif order.status == order.Canceled:
            self.log(f"ORDER CANCELED {order.data._name}")
        elif order.status == order.Margin:
            self.log(f"ORDER MARGIN {order.data._name}")
        elif order.status == order.Rejected:
            self.log(f"ORDER REJECTED {order.data._name}")

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(
            f"TRADE CLOSED {trade.data._name} "
            f"gross={trade.pnl:.2f} net={trade.pnlcomm:.2f}"
        )


class MomentumDCAv3(bt.Strategy):
    """
    MomentumDCA v3: weekly-friendly DCA with momentum tilt, trend guard,
    momentum confirmation, and an overextension/valuation guard.

    Core ideas:
      - Baseline: invest a fixed cash amount on a fixed schedule (DCA).
      - Momentum signal:
            mom = (SMA_fast - SMA_slow) / SMA_slow
            z_mom = mom / StdDev(mom)
        Positive z_mom  -> uptrend strength.
        Negative z_mom  -> weak/downtrend.
      - Adaptive multiplier m(z_mom):
          * z_mom <= z_floor   -> m = m_min (strongly de-emphasize).
          * z_floor < z_mom < 0 -> smoothly ramp from m_min up to ~1.0.
          * 0 <= z_mom < z_entry -> stay around 1.0 (no real boost).
          * z_entry <= z_mom <= z_full -> smoothly ramp from 1.0 to m_max.
          * z_mom > z_full     -> m = m_max (cap the enthusiasm).

      - Trend guard:
          Only allow m > 1 when trend is supportive:
            price > slow SMA AND slow SMA slope over lookback > 0.

      - Valuation/overextension guard:
          If price is extremely extended above slow SMA, measured by:
             dev = close/slow - 1
             z_val = dev / StdDev(dev)
          then clamp m to at most 1.0 (i.e., no boost at very frothy levels).

    Params:
      amount: float        Baseline cash to deploy per investment date (before scaling)
      interval: int        Bars between investment dates (1 = every bar)
      portfolio: dict      {ticker -> weight}, must sum to 1.0

      fast_period: int     Fast SMA length (momentum)
      slow_period: int     Slow SMA length (trend anchor)
      vol_window: int      Window for std dev of mom/dev (for z-scores)

      m_min, m_max: float  Multiplier floor/ceiling for per-asset cash

      z_floor: float       Below this z_mom -> hard min multiplier
      z_entry: float       Momentum confirmation threshold to start boosting
      z_full: float        z_mom where we reach full boost m_max

      val_cap: float       If z_val >= val_cap, clamp m <= 1.0 (no boost)
      use_trend_guard: bool
      slope_lookback: int  Bars to measure slow SMA slope

      reserve_multiplier:  Cash buffer divider to avoid margin issues
      _sum_tol: float      Tolerance for weight sum check
    """

    params = dict(
        amount=1000.0,
        interval=4,           # assuming weekly bars -> every 4 weeks (~monthly)
        portfolio=None,

        fast_period=10,       # on weekly bars: ~10 weeks
        slow_period=40,       # on weekly bars: ~200 trading days
        vol_window=26,        # ~6 months of weekly history

        m_min=0.5,
        m_max=1.5,

        z_floor=-1.0,         # strong negative momentum
        z_entry=0.5,          # start boosting around 0.5Ï momentum
        z_full=2.0,           # full boost by 2Ï momentum

        val_cap=2.0,          # 2Ï overvaluation cap
        use_trend_guard=True,
        slope_lookback=4,

        reserve_multiplier=1.01,
        _sum_tol=1e-9,
    )

    def __init__(self):
        if self.p.portfolio is None:
            raise ValueError("MomentumDCAv3 requires 'portfolio' dict mapping tickers to weights")

        total = sum(self.p.portfolio.values())
        if not math.isclose(total, 1.0, rel_tol=self.p._sum_tol, abs_tol=self.p._sum_tol):
            raise ValueError(f"Portfolio weights must sum to 1.0 (got {total})")

        # Map data feeds by name so portfolio keys can refer to tickers
        self._data_by_name = {d._name: d for d in self.datas}
        missing = [t for t in self.p.portfolio if t not in self._data_by_name]
        if missing:
            raise ValueError(f"Portfolio contains tickers not present in data feeds: {missing}")

        # Indicators per data
        self.fast = {}
        self.slow = {}
        self.mom = {}
        self.mom_std = {}

        # Valuation (overextension) indicators
        self.dev = {}
        self.dev_std = {}

        for name, d in self._data_by_name.items():
            fast = bt.ind.SMA(d.close, period=self.p.fast_period)
            slow = bt.ind.SMA(d.close, period=self.p.slow_period)
            self.fast[name] = fast
            self.slow[name] = slow

            mom = (fast - slow) / slow
            self.mom[name] = mom
            self.mom_std[name] = bt.ind.StdDev(mom, period=self.p.vol_window)

            dev = (d.close / slow) - 1.0
            self.dev[name] = dev
            self.dev_std[name] = bt.ind.StdDev(dev, period=self.p.vol_window)

        self._last_invest_bar = -1

    def log(self, txt):
        dt = self.datas[0].datetime.date(0)
        print(f"{dt} {txt}")

    # ---------- helpers ----------

    def _zscore(self, series, series_std):
        """Generic z-score helper."""
        val = float(series[0]) if not math.isnan(series[0]) else float('nan')
        sd = float(series_std[0]) if not math.isnan(series_std[0]) else float('nan')
        if (sd is None) or (sd <= 1e-12) or math.isnan(sd) or math.isnan(val):
            return 0.0
        return val / sd

    def _z_mom(self, name):
        return self._zscore(self.mom[name], self.mom_std[name])

    def _z_val(self, name):
        return self._zscore(self.dev[name], self.dev_std[name])

    def _trend_ok(self, name):
        """
        Trend guard:
          - price > slow SMA
          - slow SMA slope over lookback > 0
        """
        if not self.p.use_trend_guard:
            return True

        slow = self.slow[name]
        d = self._data_by_name[name]

        lb = min(self.p.slope_lookback, len(slow) - 1)
        if lb <= 0:
            return False

        try:
            price_above = (d.close[0] > slow[0])
            slope_up = (slow[0] - slow[-lb]) > 0
            return price_above and slope_up
        except IndexError:
            return False

    def _multiplier_from_z(self, z_mom):
        """
        Piecewise adaptive multiplier based on momentum z-score.

        Region logic:

            z <= z_floor    -> m_min
            z_floor < z < 0 -> ramp from m_min up to ~1
            0 <= z < z_entry -> ~1 (no real boost)
            z_entry <= z <= z_full -> ramp from 1 up to m_max
            z > z_full      -> m_max
        """
        m_min = self.p.m_min
        m_max = self.p.m_max
        z_floor = self.p.z_floor
        z_entry = self.p.z_entry
        z_full = self.p.z_full

        # Strong negative momentum region
        if z_mom <= z_floor:
            return m_min

        # Negative but above floor: interpolate [z_floor, 0] -> [m_min, 1.0]
        if z_floor < z_mom < 0.0:
            span = 0.0 - z_floor
            if span <= 0:
                return 1.0
            frac = (z_mom - z_floor) / span
            return m_min + frac * (1.0 - m_min)

        # Mild momentum: 0 <= z_mom < z_entry -> around 1.0 (no real boost)
        if 0.0 <= z_mom < z_entry:
            return 1.0

        # Boost region: z_entry <= z_mom <= z_full -> interpolate [z_entry, z_full] -> [1.0, m_max]
        if z_entry <= z_mom <= z_full:
            span = z_full - z_entry
            if span <= 0:
                return m_max
            frac = (z_mom - z_entry) / span
            return 1.0 + frac * (m_max - 1.0)

        # Very strong momentum beyond z_full
        if z_mom > z_full:
            return m_max

        # Fallback
        return 1.0

    # ---------- main logic ----------

    def next(self):
        # Only invest on schedule
        if self._last_invest_bar >= 0 and (len(self) - self._last_invest_bar) < self.p.interval:
            return

        cash_available = self.broker.get_cash()
        max_deployable = cash_available / self.p.reserve_multiplier

        if max_deployable <= 0:
            self.log("Skipping investment: no available cash")
            self._last_invest_bar = len(self)
            return

        baseline = float(self.p.amount)
        desired = {}
        total_desired = 0.0

        # First pass: compute desired spend per asset
        for name, weight in self.p.portfolio.items():
            d = self._data_by_name[name]
            price = float(d.close[0])

            if price <= 0 or math.isnan(price):
                continue

            z_mom = self._z_mom(name)
            z_val = self._z_val(name)

            # Adaptive multiplier from momentum
            m = self._multiplier_from_z(z_mom)

            # Valuation/overextension guard: if heavily overvalued, disallow boosts
            if z_val >= self.p.val_cap and m > 1.0:
                m = 1.0

            # Trend guard: only allow m > 1 if trend is OK
            if self.p.use_trend_guard and (m > 1.0) and (not self._trend_ok(name)):
                m = min(m, 1.0)

            # Final safety clip
            m = max(self.p.m_min, min(self.p.m_max, m))

            spend = baseline * weight * m
            if spend > 0:
                desired[name] = (spend, z_mom, z_val, m)
                total_desired += spend

        if total_desired <= 0:
            self.log("Nothing to allocate (all spends <= 0 or invalid)")
            self._last_invest_bar = len(self)
            return

        # If over budget, rescale proportionally
        scale = 1.0
        if total_desired > max_deployable:
            scale = max_deployable / total_desired
            self.log(
                f"Scaling spends by {scale:.3f} "
                f"(desired={total_desired:.2f}, cap={max_deployable:.2f})"
            )

        # Place market buys
        for name, (spend, z_mom, z_val, m) in desired.items():
            d = self._data_by_name[name]
            price = float(d.close[0])
            alloc = spend * scale
            size = alloc / price
            if size <= 0:
                continue

            self.buy(data=d, size=size)
            self.log(
                f"Momentum v3 buy {name}: "
                f"z_mom={z_mom:+.2f} z_val={z_val:+.2f} m={m:.2f} "
                f"alloc={alloc:.2f} price={price:.2f} size={size:.6f}"
            )

        self._last_invest_bar = len(self)

    def notify_order(self, order):
        if order.status in (order.Submitted, order.Accepted):
            return

        if order.status == order.Completed:
            action = 'BUY' if order.isbuy() else 'SELL'
            size = order.executed.size
            price = order.executed.price
            comm = order.executed.comm
            cost = size * price
            self.log(
                f"{action} EXECUTED {order.data._name} "
                f"size={size:.6f} price={price:.2f} cost={cost:.2f} comm={comm:.2f}"
            )
        elif order.status == order.Canceled:
            self.log(f"ORDER CANCELED {order.data._name}")
        elif order.status == order.Margin:
            self.log(f"ORDER MARGIN {order.data._name}")
        elif order.status == order.Rejected:
            self.log(f"ORDER REJECTED {order.data._name}")

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(
            f"TRADE CLOSED {trade.data._name} "
            f"gross={trade.pnl:.2f} net={trade.pnlcomm:.2f}"
        )
