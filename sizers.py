import backtrader as bt


class FractionalSizer(bt.Sizer):
    """
    Fractional position sizer.

    - For buys: invests a percentage of current available cash (perc) and
      returns a fractional size = (cash * perc/100) / price.
    - For sells: defaults to closing the entire current position on that data.

    Parameters
    -----------
    perc: float
        Percentage of available cash to allocate per buy (default 10.0).
    """

    params = dict(perc=10.0)

    def _getsizing(self, comminfo, cash, data, isbuy):
        price = data.close[0]

        if isbuy:
            if cash <= 0 or price <= 0:
                return 0.0
            alloc_value = cash * (self.p.perc / 100.0)
            size = alloc_value / price
            return float(size)

        # For sells: close existing position entirely (fractional-friendly)
        position = self.broker.getposition(data)
        size = position.size
        return float(size) if size > 0 else 0.0
