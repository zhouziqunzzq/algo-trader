import math
import backtrader as bt


class FixedDCA(bt.Strategy):
    """
    Invest a fixed cash amount into a target portfolio on a fixed schedule.

    Parameters (via Cerebro.addstrategy / class params):
        amount: float  - cash amount to deploy each interval
        deposit_amount: float - cash amount to deposit into the broker right before investing
        interval: int  - number of bars between investments (1 => every bar)
        portfolio: dict - mapping ticker string -> ratio (must sum to 1.0 within tolerance)
        reserve_multiplier: float - factor to reserve cash for commissions/rounding (default 1.01)
        _sum_tol: float - tolerance for portfolio weight sum checking (default 1e-9)
    """

    params = dict(
        amount=1000.0,
        deposit_amount=0.0,
        interval=20,
        portfolio=None,
        reserve_multiplier=1.01,
        _sum_tol=1e-9,
    )

    def __init__(self):
        if self.p.portfolio is None:
            raise ValueError(
                "FixedDCA requires a 'portfolio' dict mapping tickers to ratios"
            )

        # Validate weights sum to 1. Allow for floating point tiny errors.
        total = sum(self.p.portfolio.values())
        if not math.isclose(
            total, 1.0, rel_tol=self.p._sum_tol, abs_tol=self.p._sum_tol
        ):
            raise ValueError(f"Portfolio weights must sum to 1.0 (got {total})")

        # Map data feeds by their name so portfolio keys can refer to tickers
        self._data_by_name = {d._name: d for d in self.datas}

        missing = [t for t in self.p.portfolio.keys() if t not in self._data_by_name]
        if missing:
            raise ValueError(
                f"Portfolio contains tickers not present in data feeds: {missing}"
            )

        self._last_invest_bar = -1

        # External cashflows (e.g., deposits) for cashflow-aware stats.
        # Broker-side sign convention: deposit of $X is recorded as +X.
        self._cashflow_by_date = {}

    def get_cashflow_by_date(self):
        """Return dict mapping datetime.date -> net external cashflow (broker-side sign)."""
        return dict(self._cashflow_by_date)

    def get_cashflows(self):
        """Return sorted list of (date, amount) external cashflows (broker-side sign)."""
        return sorted(self._cashflow_by_date.items(), key=lambda x: x[0])

    def _deposit_cash(self, amount: float) -> bool:
        """Deposit cash into broker if supported by the broker implementation.

        Returns True if the broker accepted the deposit, False otherwise.
        """
        amount = float(amount)
        if amount <= 0:
            return False

        broker = self.broker
        # Backtrader brokers commonly expose add_cash; fall back to set_cash/setcash if present.
        if hasattr(broker, "add_cash"):
            broker.add_cash(amount)
            return True

        if hasattr(broker, "get_cash") and hasattr(broker, "set_cash"):
            broker.set_cash(float(broker.get_cash()) + amount)
            return True

        if hasattr(broker, "get_cash") and hasattr(broker, "setcash"):
            broker.setcash(float(broker.get_cash()) + amount)
            return True

        # If the broker doesn't support deposits, keep the strategy running.
        self.log(
            f"Broker does not support cash deposits; skipping deposit of {amount:.2f}"
        )

        return False

    def log(self, txt):
        dt = self.datas[0].datetime.date(0)
        print(f"{dt} {txt}")

    def next(self):
        # Only run when interval has elapsed (or first time)
        if (
            self._last_invest_bar >= 0
            and (len(self) - self._last_invest_bar) < self.p.interval
        ):
            return

        if float(self.p.deposit_amount) > 0:
            dep = float(self.p.deposit_amount)
            ok = self._deposit_cash(dep)
            if ok:
                dt_dep = self.datas[0].datetime.date(0)
                self._cashflow_by_date[dt_dep] = float(self._cashflow_by_date.get(dt_dep, 0.0)) + dep
                self.log(f"Deposited cash before investment: {dep:.2f}")

        # Print day of the tick for debugging
        dt = self.datas[0].datetime.date(0)
        self.log(f"Scheduled investment on {dt}")
        # Determine how much cash is actually available. Multiple simultaneous
        # buys can exceed available cash because orders execute together; to
        # avoid margin rejections, cap the deployable amount to what we have
        # (reserve a small buffer for commission/rounding).
        cash_available = self.broker.get_cash()
        # Reserve ~1% to cover commissions/rounding (safe default)
        max_deployable = cash_available / self.p.reserve_multiplier
        deploy_amount = min(float(self.p.amount), max_deployable)

        if deploy_amount <= 0:
            self.log("Skipping scheduled investment: no available cash")
            self._last_invest_bar = len(self)
            return

        if deploy_amount < float(self.p.amount):
            self.log(
                f"Insufficient cash: scaling deploy from {self.p.amount:.2f} to {deploy_amount:.2f}"
            )

        # Allocate per configured portfolio ratios based on deploy_amount
        for ticker, ratio in self.p.portfolio.items():
            data = self._data_by_name[ticker]
            price = data.close[0]
            if price is None or price <= 0:
                # skip bad prices
                continue

            alloc = float(deploy_amount) * float(ratio)
            size = alloc / price
            if size <= 0:
                continue

            # Place a buy order; fractional sizes are allowed
            self.buy(data=data, size=size)
            self.log(
                f"Scheduled buy {ticker}: alloc={alloc:.2f} size={size:.6f} price={price:.2f}"
            )

        self._last_invest_bar = len(self)

    def notify_order(self, order):
        # minimal informative logging
        if order.status in (order.Submitted, order.Accepted):
            return

        if order.status == order.Completed:
            action = "BUY" if order.isbuy() else "SELL"
            size = order.executed.size
            price = order.executed.price
            comm = order.executed.comm
            cost = size * price
            self.log(
                f"{action} EXECUTED {order.data._name} size={size:.6f} price={price:.2f} cost={cost:.2f} comm={comm:.2f}"
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
            f"TRADE CLOSED {trade.data._name} gross={trade.pnl:.2f} net={trade.pnlcomm:.2f}"
        )
