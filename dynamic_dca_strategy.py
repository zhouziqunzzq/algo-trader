import math
import backtrader as bt


class DynamicDCA(bt.Strategy):
    """
    Dynamic DCA: invest a baseline cash amount on a fixed schedule, but
    modulate each asset's spend by its SMA-based z-score (cheap -> buy more).

    Params:
      amount: float        Baseline cash to deploy per investment date (pre-scaling)
      interval: int        Bars between investment dates (1 = every bar)
      portfolio: dict      {ticker -> weight}, must sum to 1.0
      sma_period: int      Slow SMA length (on close)
      vol_window: int      Window for std dev of (pct deviation from SMA)
      k: float             Linear sensitivity: m = clip(1 - k*z, m_min, m_max)
      m_min, m_max: float  Multiplier floor/ceiling for per-asset cash
      trend_guard: bool    If True, only allow boosts when trend is up
      slope_lookback: int  Bars to measure SMA slope (positive -> uptrend)
      reserve_multiplier:  Cash buffer divider to avoid margin issues
      _sum_tol: float      Tolerance for weight sum check
    """

    params = dict(
        amount=1000.0,
        interval=30,
        portfolio=None,
        sma_period=200,
        vol_window=60,
        k=0.5,
        m_min=0.3,
        m_max=1.8,
        trend_guard=True,
        slope_lookback=5,
        reserve_multiplier=1.01,
        _sum_tol=1e-9,
    )

    def __init__(self):
        if self.p.portfolio is None:
            raise ValueError("DynamicDCA requires 'portfolio' dict mapping tickers to weights")

        total = sum(self.p.portfolio.values())
        if not math.isclose(total, 1.0, rel_tol=self.p._sum_tol, abs_tol=self.p._sum_tol):
            raise ValueError(f"Portfolio weights must sum to 1.0 (got {total})")

        # Map feeds by name to portfolio tickers
        self._data_by_name = {d._name: d for d in self.datas}
        missing = [t for t in self.p.portfolio if t not in self._data_by_name]
        if missing:
            raise ValueError(f"Portfolio contains tickers not present in data feeds: {missing}")

        # Indicators per data
        self.sma = {}
        self.dev = {}      # pct deviation: P/SMA - 1
        self.dev_std = {}  # rolling std of deviation
        for name, d in self._data_by_name.items():
            sma = bt.ind.SMA(d.close, period=self.p.sma_period)
            self.sma[name] = sma
            # pct deviation; safe-guard against early NaNs by using (price / sma) - 1
            self.dev[name] = (d.close / sma) - 1.0
            self.dev_std[name] = bt.ind.StdDev(self.dev[name], period=self.p.vol_window)

        self._last_invest_bar = -1

    def log(self, txt):
        dt = self.datas[0].datetime.date(0)
        print(f"{dt} {txt}")

    def _zscore(self, name):
        """z = deviation / stddev; if stddev invalid/small, return 0 (baseline)."""
        dev = float(self.dev[name][0]) if not math.isnan(self.dev[name][0]) else float('nan')
        sd = float(self.dev_std[name][0]) if not math.isnan(self.dev_std[name][0]) else float('nan')
        if (sd is None) or (sd <= 1e-12) or math.isnan(sd) or math.isnan(dev):
            return 0.0
        return dev / sd

    def _trend_ok(self, name):
        """Basic trend guard: price > SMA and SMA slope over lookback > 0."""
        if not self.p.trend_guard:
            return True
        sma = self.sma[name]
        lb = min(self.p.slope_lookback, len(sma) - 1)
        if lb <= 0:
            return False
        try:
            price_above = (self._data_by_name[name].close[0] > sma[0])
            slope_up = (sma[0] - sma[-lb]) > 0
            return price_above and slope_up
        except IndexError:
            return False

    def next(self):
        # invest only on schedule
        if self._last_invest_bar >= 0 and (len(self) - self._last_invest_bar) < self.p.interval:
            return

        cash_available = self.broker.get_cash()
        max_deployable = cash_available / self.p.reserve_multiplier
        if max_deployable <= 0:
            self.log("Skipping investment: no available cash")
            self._last_invest_bar = len(self)
            return

        baseline = float(self.p.amount)
        # First pass: compute desired spends per asset
        desired = {}
        total_desired = 0.0

        for name, weight in self.p.portfolio.items():
            d = self._data_by_name[name]
            price = float(d.close[0])
            if price <= 0 or math.isnan(price):
                continue

            z = self._zscore(name)
            m = 1.0 - self.p.k * z
            # clip multiplier
            m = max(self.p.m_min, min(self.p.m_max, m))

            # trend guard: allow cuts always, but only allow boosts (>1) if trend_ok
            if self.p.trend_guard and (m > 1.0) and (not self._trend_ok(name)):
                m = min(m, 1.0)

            spend = baseline * weight * m
            if spend > 0:
                desired[name] = spend
                total_desired += spend

        if total_desired <= 0:
            self.log("Nothing to allocate (all spends <= 0 or invalid)")
            self._last_invest_bar = len(self)
            return

        # If over budget, rescale proportionally
        scale = 1.0
        if total_desired > max_deployable:
            scale = max_deployable / total_desired
            self.log(f"Scaling portfolio spends by {scale:.3f} (desired={total_desired:.2f}, cap={max_deployable:.2f})")

        # Place market buys with fractional sizes
        for name, spend in desired.items():
            d = self._data_by_name[name]
            price = float(d.close[0])
            alloc = spend * scale
            size = alloc / price
            if size <= 0:
                continue
            self.buy(data=d, size=size)
            self.log(f"Dynamic buy {name}: z={self._zscore(name):+.2f} alloc={alloc:.2f} price={price:.2f} size={size:.6f}")

        self._last_invest_bar = len(self)

    def notify_order(self, order):
        if order.status in (order.Submitted, order.Accepted):
            return
        if order.status == order.Completed:
            action = 'BUY' if order.isbuy() else 'SELL'
            size = order.executed.size
            price = order.executed.price
            comm = order.executed.comm
            self.log(f"{action} EXECUTED {order.data._name} size={size:.6f} price={price:.2f} cost={size*price:.2f} comm={comm:.2f}")
        elif order.status == order.Canceled:
            self.log(f"ORDER CANCELED {order.data._name}")
        elif order.status == order.Margin:
            self.log(f"ORDER MARGIN {order.data._name}")
        elif order.status == order.Rejected:
            self.log(f"ORDER REJECTED {order.data._name}")

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f"TRADE CLOSED {trade.data._name} gross={trade.pnl:.2f} net={trade.pnlcomm:.2f}")
