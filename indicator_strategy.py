import math
import backtrader as bt

class IndicatorStrategy(bt.Strategy):
    params = dict(
        print_indicators=False,
        sma_fast=20,
        sma_slow=50,
        rsi_period=14,
        atr_period=14,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        vol_period=63,          # ~3 months
        invest_perc=10.0,       # % of available cash per entry
        invest_amount=None,     # fixed $ per entry (overrides invest_perc)
        atr_stop_mult=3.0,      # initial stop = entry - k*ATR
        min_bars_between=5,     # throttle re-entries to cut churn
        band=0.003,         # 0.3% buffer around slow MA
        confirm_bars=0,     # set >0 to require consecutive confirmations
        min_hold=5,         # ignore opposite signals for N bars after entry
    )

    def __init__(self):
        self.inds = {}
        self.last_entry_bar = {}

        for d in self.datas:
            i = self.inds[d] = {}

            # MAs
            i['sma_fast'] = bt.indicators.SMA(d.close, period=self.p.sma_fast, subplot=False)
            i['sma_slow'] = bt.indicators.SMA(d.close, period=self.p.sma_slow, subplot=False)
            # Upper/lower bands around slow MA
            i['slow_up'] = i['sma_slow'] * (1.0 + self.p.band)
            i['slow_dn'] = i['sma_slow'] * (1.0 - self.p.band)
            # Two separate crossovers: enter vs exit
            i['x_up'] = bt.ind.CrossOver(i['sma_fast'], i['slow_up'])  # +1 on bullish cross through upper band
            i['x_dn'] = bt.ind.CrossOver(i['sma_fast'], i['slow_dn'])  # -1 on bearish cross through lower band
            # Optional consecutive-bar confirmation
            i['fast_gt_up'] = bt.ind.Highest(i['sma_fast'] > i['slow_up'], period=max(1, self.p.confirm_bars))
            i['fast_lt_dn'] = bt.ind.Highest(i['sma_fast'] < i['slow_dn'], period=max(1, self.p.confirm_bars))
            self.last_entry_bar[d] = -10**9

            # RSI / ATR
            i['rsi'] = bt.indicators.RSI(d.close, period=self.p.rsi_period)
            i['atr'] = bt.indicators.ATR(d, period=self.p.atr_period)

            # MACD (+ robust hist)
            macd = bt.indicators.MACD(d.close,
                                      period_me1=self.p.macd_fast,
                                      period_me2=self.p.macd_slow,
                                      period_signal=self.p.macd_signal)
            i['macd'] = macd.macd
            i['macd_signal'] = macd.signal
            i['macd_hist'] = getattr(macd, 'histo', getattr(macd, 'hist', getattr(macd, 'histogram', macd.macd - macd.signal)))

            # Rolling volatility: std dev of (log) returns, with fallbacks for BT versions
            try:
                ret = bt.indicators.LogReturn(d.close, period=1)  # newer BT
            except AttributeError:
                try:
                    ret = bt.indicators.PercentChange(d.close, period=1)  # some builds
                except AttributeError:
                    ret = bt.indicators.PctChange(d.close, period=1)      # other builds

            self.inds[d]['vol'] = bt.indicators.StandardDeviation(ret, period=self.p.vol_period)


        # longest warmup needed
        self._warmup = max(self.p.sma_slow, self.p.rsi_period, self.p.atr_period,
                           self.p.vol_period, self.p.macd_slow)

    def next(self):
        # skip until indicators warmed up
        if len(self.data) < self._warmup:
            return

        dt = self.datas[0].datetime.date(0)
        if self.p.print_indicators:
            for d in self.datas:
                i = self.inds[d]
                print(f"{dt} {d._name} Close={d.close[0]:.2f} "
                      f"SMA{self.p.sma_fast}={i['sma_fast'][0]:.2f} "
                      f"SMA{self.p.sma_slow}={i['sma_slow'][0]:.2f} "
                      f"X={i['crossover'][0]:+d} RSI={i['rsi'][0]:.2f} "
                      f"ATR={i['atr'][0]:.2f} MACD={i['macd'][0]:.4f} "
                      f"Sig={i['macd_signal'][0]:.4f} Hist={i['macd_hist'][0]:.4f} "
                      f"Vol={i['vol'][0]:.4f} Pos={self.getposition(d).size:.4f}")

        for d in self.datas:
            i = self.inds[d]
            # cross = i['crossover'][0]
            pos   = self.getposition(d)
            price = float(d.close[0])
            # atr   = float(i['atr'][0])
            now = len(d)

            can_exit = (now - self.last_entry_bar[d]) >= self.p.min_hold

            # Entry: cross above upper band (optionally confirmed)
            enter = (i['x_up'][0] > 0) and (i['fast_gt_up'][0] if self.p.confirm_bars else True)
            # Exit: cross below lower band (optionally confirmed) + min hold
            exit_  = (i['x_dn'][0] < 0) and (i['fast_lt_dn'][0] if self.p.confirm_bars else True) and can_exit

            # ENTRY: bullish cross, flat, ATR positive, and price above slow MA (extra filter)
            # if cross > 0 and pos.size <= 0 and atr > 0 and price > i['sma_slow'][0]:
            #     if self.p.invest_amount is not None:
            #         cash_alloc = min(self.broker.get_cash(), float(self.p.invest_amount))
            #     else:
            #         cash_alloc = self.broker.get_cash() * (self.p.invest_perc / 100.0)

            #     size = cash_alloc / price
            #     if size > 0:
            #         # bracket with ATR stop for downside control
            #         stop_price = max(0.01, price - self.p.atr_stop_mult * atr)
            #         self.buy_bracket(
            #             data=d,
            #             size=size,
            #             limitprice=None,            # no profit target for now
            #             stopprice=stop_price,       # initial protective stop
            #             exectype=bt.Order.Market,
            #         )
            #         self.last_trade_bar[d] = len(d)
            if not pos and enter:
                if self.p.invest_amount is not None:
                    cash_alloc = min(self.broker.get_cash(), float(self.p.invest_amount))
                else:
                    cash_alloc = self.broker.get_cash() * (self.p.invest_perc / 100.0)
                size = cash_alloc / price
                o = self.buy(data=d, size=size)  # or your bracket
                self.last_entry_bar[d] = now

            # EXIT: bearish cross while long -> close
            # elif cross < 0 and pos.size > 0:
            #     self.close(data=d)
            #     self.last_trade_bar[d] = len(d)
            elif pos and exit_:
                self.close(data=d)

    # --- notifications ---
    def log(self, txt):
        dt = self.datas[0].datetime.date(0) if len(self.datas[0]) else 'NA'
        print(f"{dt} {txt}")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        dname = order.data._name
        if order.status == order.Completed:
            side = 'BUY' if order.isbuy() else 'SELL'
            self.log(f"EXECUTED {side} {dname} size={order.executed.size:.4f} px={order.executed.price:.2f} "
                     f"val={order.executed.value:.2f} comm={order.executed.comm:.2f}")
        elif order.status == order.Canceled:
            self.log(f"ORDER CANCELED {dname}")
        elif order.status == order.Margin:
            self.log(f"ORDER MARGIN {dname}")
        elif order.status == order.Rejected:
            self.log(f"ORDER REJECTED {dname}")
