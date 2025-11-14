import datetime
import backtrader as bt

from data_loader import download_ohlc_yf
from stats import print_stats
from momentum_dca_strategy import MomentumDCA, MomentumDCAv2, MomentumDCAv3
from fixed_dca_strategy import FixedDCA
from dynamic_dca_strategy import DynamicDCA
from indicator_strategy import IndicatorStrategy
from random_strategy import RandomBuyStrategy


def run_backtest_weekly(
    cerebro: bt.Cerebro,
    tickers,
    fromdate=None,
    todate=None,
    show_plot=False,
    warm_up=None,
):
    """
    Run a backtest on WEEKLY bars using an already-configured ``cerebro``.

    - Loads DAILY OHLC data via `download_ohlc_yf`
    - Adds a daily feed per ticker (named '{ticker}_daily' for optional plotting)
    - Resamples each daily feed to WEEKLY bars (named '{ticker}')
      so portfolio dicts like {"QQQ": 0.25, ...} still match data feed names.

    The caller should:
      1) Instantiate `cerebro`
      2) Configure broker, commission, etc.
      3) Add the desired strategy (which will operate on weekly bars)
      4) Then call this helper.
    """
    # Load daily data for all tickers
    df_map = download_ohlc_yf(
        tickers,
        fromdate=fromdate,
        todate=todate,
        auto_adjust=False,
        threads=True,
    )
    if not df_map:
        print("Warning: no data returned for requested tickers")

    # Add daily data + weekly resampled data
    for ticker, df in df_map.items():
        data_daily = bt.feeds.PandasData(dataname=df)
        # IMPORTANT: do NOT `adddata` the daily feed.
        # Only resample it to weekly so the strategy sees weekly bars only.

        # Resampled weekly feed; name must match portfolio keys
        cerebro.resampledata(
            data_daily,
            timeframe=bt.TimeFrame.Weeks,
            compression=1,
            name=ticker,
        )

    # Attach analyzers:
    # - 'timereturn': yearly returns (same as before)
    # - 'daily_return': here it's actually WEEKLY returns, but we keep the name
    #   for compatibility with `print_stats`.
    cerebro.addanalyzer(
        bt.analyzers.TimeReturn,
        _name='timereturn',
        timeframe=bt.TimeFrame.Years,
    )
    cerebro.addanalyzer(
        bt.analyzers.TimeReturn,
        _name='daily_return',  # now weekly granularity
        timeframe=bt.TimeFrame.Weeks,
        compression=1,
    )
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    # Run backtest and print stats
    start_cash = cerebro.broker.getvalue()
    print(f"Starting Portfolio Value: {start_cash:.2f}")
    results = cerebro.run()
    strat = results[0]

    # warm_up is interpreted as "bars to ignore" by your existing print_stats;
    # with weekly data, think of it as "warm-up WEEKS".
    print_stats(
        cerebro,
        strat,
        df_map,
        start_cash,
        fromdate,
        todate,
        warm_up=warm_up,
        freq="weekly",
    )

    if show_plot:
        cerebro.plot()


def main():
    start_cash = 500_000.0
    tickers = [
        "QQQ", "NVDA", "VGT", "VOO",
        "VTI", "META", "BND",
    ]
    fromdate = datetime.datetime(2015, 1, 1)
    todate = datetime.datetime(2025, 10, 31)
    show_plot = True

    # With weekly bars, warm_up is now in *weeks*.
    warm_up = datetime.timedelta(weeks=40)

    cerebro = bt.Cerebro()

    # Set the starting cash
    cerebro.broker.setcash(start_cash)

    # Classic brokerage accounting (no fund mode)
    cerebro.broker.set_fundmode(False)

    # Commission model
    comminfo = bt.CommInfoBase(
        commission=0.001,
        commtype=bt.CommInfoBase.COMM_PERC,
        stocklike=True,
    )
    cerebro.broker.addcommissioninfo(comminfo)

    # ---- Portfolio definitions ----
    reg_portfolio = {
        "QQQ": 0.25,
        "NVDA": 0.20,
        "VGT": 0.15,
        "VOO": 0.10,
        "VTI": 0.10,
        "META": 0.10,
        "BND": 0.10,
    }
    # vol_portfolio = {
    #     "IWM": 0.25,   # Small-cap stocks (high beta)
    #     "EEM": 0.20,   # Emerging markets equity
    #     "XBI": 0.15,   # Biotech ETF (high vol)
    #     "GDX": 0.10,   # Gold miners (extremely volatile)
    #     "USO": 0.10,   # Crude oil ETF (very high vol)
    #     "TLT": 0.10,   # Long-term bonds (rate sensitive)
    #     "GLD": 0.10,   # Gold spot ETF (hedge + volatility)
    # }
    # tickers.extend(vol_portfolio.keys())
    vol_portfolio_max = {
        "XBI": 0.20,
        "GDX": 0.20,
        "USO": 0.20,
        "IWM": 0.20,
        "EEM": 0.10,
        "TLT": 0.10,
    }
    tickers.extend(vol_portfolio_max.keys())
    all_qqq_portfolio = {"QQQ": 1.0}
    all_in_portfolio = {"IWM": 1.0}

    # ---- Choose ONE strategy below ----

    # FixedDCA on weekly bars (example):
    # cerebro.addstrategy(
    #     FixedDCA,
    #     amount=800.0,
    #     interval=2,  # every 2 weeks, biweekly
    #     portfolio=reg_portfolio,
    # )
    # warm_up = None # no warm-up for FixedDCA

    # DynamicDCA on weekly bars (example):
    # cerebro.addstrategy(
    #     DynamicDCA,
    #     amount=800.0,
    #     interval=2,      # every 2 weeks
    #     portfolio=vol_portfolio,
    #     sma_period=40,   # 40 weeks ~= 200 trading days
    #     vol_window=26,   # ~6 months of weekly data
    #     k=0.5,
    #     m_min=0.6,
    #     m_max=2.0,
    #     trend_guard=True,
    #     slope_lookback=4,
    # )

    # MomentumDCA on weekly bars (example):
    # cerebro.addstrategy(
    #     MomentumDCA,
    #     amount=800.0,
    #     interval=2,       # every 2 weeks
    #     portfolio=reg_portfolio,
    #     fast_period=10,   # 10 weeks
    #     slow_period=40,   # 40 weeks â 200 trading days
    #     vol_window=26,    # ~6 months of weekly data
    #     k=0.5,
    #     m_min=0.5,
    #     m_max=1.5,
    # )

    # MomentumDCAv2 on weekly bars (default for this script):
    cerebro.addstrategy(
        MomentumDCAv2,
        amount=800.0,
        interval=2,        # invest every 2 weeks (~biweekly)
        portfolio=vol_portfolio_max,

        fast_period=10,    # 10-week fast SMA
        slow_period=40,    # 40-week slow SMA (~200 trading days)
        vol_window=26,     # ~6 months of weekly momentum history

        k=0.5,
        m_min=0.5,
        m_max=1.5,

        z_floor=-1.0,
        use_trend_guard=True,
        slope_lookback=4,
    )

    # MomentumDCAv3 on weekly bars:
    # cerebro.addstrategy(
    #     MomentumDCAv3,
    #     amount=800.0,
    #     interval=2,          # every 2 weekly bars (~biweekly)
    #     portfolio=vol_portfolio_max,

    #     fast_period=10,      # weekly: 10 weeks
    #     slow_period=40,      # weekly: 40 weeks ~ 200 trade days
    #     vol_window=26,       # weekly: ~6 months

    #     m_min=0.5,
    #     m_max=1.5,

    #     z_floor=-1.0,
    #     z_entry=0.2,
    #     z_full=2.0,

    #     val_cap=3.0,
    #     use_trend_guard=True,
    #     slope_lookback=2,
    # )

    # Example: indicator / random strategies would also operate on weekly bars
    # cerebro.addstrategy(IndicatorStrategy, invest_amount=1000.0)
    # cerebro.addstrategy(RandomBuyStrategy, buy_amount=100.0, buy_every=1, random_seed=None)

    run_backtest_weekly(
        cerebro,
        tickers,
        fromdate,
        todate,
        show_plot=show_plot,
        warm_up=warm_up,
    )


if __name__ == "__main__":
    main()