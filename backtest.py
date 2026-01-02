import backtrader as bt
import datetime

from data_loader import download_ohlc_yf
from stats import install_daily_stats_analyzers, print_stats
from indicator_strategy import IndicatorStrategy
from random_strategy import RandomBuyStrategy
from fixed_dca_strategy import FixedDCA
from dynamic_dca_strategy import DynamicDCA
from momentum_dca_strategy import MomentumDCA, MomentumDCAv2


def run_backtest(
    cerebro: bt.Cerebro,
    tickers,
    fromdate=None,
    todate=None,
    show_plot=False,
    warm_up=None,
):
    """
    Run the backtest using an already-configured ``cerebro`` instance.

    The caller (typically ``main``) should instantiate and add the desired
    strategy to `cerebro` before calling this helper.
    """
    # Load data for all tickers
    df_map = download_ohlc_yf(
        tickers, fromdate=fromdate, todate=todate, auto_adjust=False, threads=True
    )
    if not df_map:
        print("Warning: no data returned for requested tickers")
    for ticker, df in df_map.items():
        cerebro.adddata(bt.feeds.PandasData(dataname=df), name=ticker)

    # Run backtest and print stats
    start_cash = cerebro.broker.getvalue()
    print(f"Starting Portfolio Value: {start_cash:.2f}")
    results = cerebro.run()
    strat = results[0]
    print_stats(
        cerebro,
        strat,
        df_map,
        start_cash,
        fromdate,
        todate,
        warm_up=warm_up,
        freq="daily",
    )
    if show_plot:
        cerebro.plot()


def main():
    start_cash = 520_000.0
    tickers = []  # to be added later according to portfolio
    fromdate = datetime.datetime(2005, 11, 30)
    todate = datetime.datetime(2025, 11, 30)
    warm_up_days = 0
    show_plot = True

    cerebro = bt.Cerebro()

    # Set the starting cash
    cerebro.broker.setcash(start_cash)
    # Classic brokerage accounting (no fund mode)
    cerebro.broker.set_fundmode(False)
    # Explicit commission info with stocklike=True for correct cost (size*price) handling
    comminfo = bt.CommInfoBase(
        commission=0.001,
        commtype=bt.CommInfoBase.COMM_PERC,
        stocklike=True,
    )
    cerebro.broker.addcommissioninfo(comminfo)

    # Instantiate the strategy here (choose one)
    # RandomBuyStrategy example:
    # cerebro.addstrategy(
    #     RandomBuyStrategy,
    #     buy_amount=100.0, buy_every=15, random_seed=None,
    # )
    # DCAs:
    reg_portfolio = {
        "QQQ": 0.25,
        "NVDA": 0.20,
        "VGT": 0.15,
        "VOO": 0.10,
        "VTI": 0.10,
        "META": 0.10,
        "BND": 0.10,
    }
    vol_portfolio = {
        "ARKK": 0.25,
        "IWM": 0.20,
        "EEM": 0.15,
        "XBI": 0.10,
        "PLTR": 0.10,
        "TLT": 0.10,
        "GLD": 0.10,
    }
    # tickers.extend(vol_portfolio.keys())
    joint_portfolio = {
        "QQQ": 0.25,
        "VGT": 0.10,
        "VOO": 0.25,
        "BRK-B": 0.15,
        "GLD": 0.10,
        "VTI": 0.05,
        "NVDA": 0.05,
        "MSFT": 0.05,
    }
    all_in_portfolio = {
        "SPMO": 1.0,
    }

    portfolio = all_in_portfolio
    cerebro.addstrategy(
        FixedDCA,
        amount=2000.0,
        interval=10,  # biweekly on trading days
        portfolio=portfolio,
    )
    tickers.extend(portfolio.keys())

    # DynamicDCA:
    # cerebro.addstrategy(
    #     DynamicDCA,
    #     amount=800.0,
    #     interval=10, # biweekly on trading days
    #     portfolio=vol_portfolio,
    #     sma_period=200,
    #     vol_window=60,
    #     k=0.5, m_min=0.6, m_max=2.0,
    #     trend_guard=True, slope_lookback=5
    # )

    # MomentumDCA:
    # cerebro.addstrategy(
    #     MomentumDCA,
    #     amount=800.0,
    #     interval=10, # biweekly on trading days
    #     portfolio=reg_portfolio,
    #     fast_period=50,
    #     slow_period=200,
    #     vol_window=60,
    #     k=0.5,
    #     m_min=0.5,
    #     m_max=1.5,
    # )
    # warm_up_days = 200

    # MomentumDCAv2:
    # portfolio=joint_portfolio
    # cerebro.addstrategy(
    #     MomentumDCAv2,
    #     amount=1666.67,
    #     interval=10,          # ~biweekly on trading days
    #     portfolio=portfolio,

    #     fast_period=50,
    #     slow_period=200,
    #     vol_window=60,

    #     k=0.5,
    #     m_min=0.5,
    #     m_max=1.5,

    #     z_floor=-1.0,
    #     use_trend_guard=True,
    #     slope_lookback=5,
    # )
    # warm_up_days = 200
    # tickers.extend(portfolio.keys())

    # IndicatorStrategy example:
    # cerebro.addstrategy(
    #     IndicatorStrategy,
    #     invest_amount=1000.0,
    # )

    # Attach useful analyzers: yearly and daily returns, Sharpe, DrawDown
    install_daily_stats_analyzers(cerebro)

    run_backtest(
        cerebro,
        tickers,
        fromdate,
        todate,
        show_plot=show_plot,
        warm_up=warm_up_days,
    )


if __name__ == "__main__":
    main()
