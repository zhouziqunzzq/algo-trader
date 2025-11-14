import random
import backtrader as bt


class RandomBuyStrategy(bt.Strategy):
    params = dict(
        buy_amount=1000.0,   # currency units to deploy each scheduled buy
        buy_every=5,         # buy interval in bars (assumes daily data -> every 5 trading days)
        random_seed=None,    # set for reproducibility
    )

    def __init__(self):
        if self.p.random_seed is not None:
            random.seed(self.p.random_seed)
        self.last_buy_bar = -1

    def next(self):
        # Skip if no data loaded or insufficient bars
        if len(self.datas[0]) == 0:
            return

        # Only act every buy_every bars
        if self.last_buy_bar >= 0 and (len(self) - self.last_buy_bar) < self.p.buy_every:
            return

        # Choose a random data feed
        data = random.choice(self.datas)
        price = data.close[0]
        if price <= 0:
            return

        # Determine size from fixed buy_amount (fractional allowed with fund mode)
        size = self.p.buy_amount / price
        if size <= 0:
            return

        self.buy(data=data, size=size)
        self.last_buy_bar = len(self)

        dt = data.datetime.date(0)
        self.log(f"Random buy {data._name}: size={size:.4f} price={price:.2f} cash={self.broker.get_cash():.2f}")

    def log(self, txt):
        dt = self.datas[0].datetime.date(0)
        print(f"{dt} {txt}")

    def notify_order(self, order):
        # Skip submitted/accepted states
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status == order.Completed:
            action = 'BUY' if order.isbuy() else 'SELL'
            size = order.executed.size
            price = order.executed.price
            cost = size * price
            comm = order.executed.comm
            self.log(
                f"{action} EXECUTED {order.data._name} size={size:.4f} price={price:.2f} cost={cost:.2f} comm={comm:.2f}"
            )
        elif order.status == order.Canceled:
            self.log(f"ORDER CANCELED {order.data._name}")
        elif order.status == order.Margin:
            self.log(f"ORDER MARGIN {order.data._name}")
        elif order.status == order.Rejected:
            self.log(f"ORDER REJECTED {order.data._name}")
