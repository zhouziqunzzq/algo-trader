import matplotlib.pyplot as plt
import pandas as pd


def plot_drawdown_curve(
    strat, *, title: str = "Drawdown", save_path: str | None = "drawdown.png"
):
    """Plot a standalone drawdown curve (percent from peak over time)."""
    values = None
    try:
        fa = strat.analyzers.flowadj.get_analysis() or {}
        if isinstance(fa, dict):
            values = fa.get("values")
    except Exception:
        values = None

    if not values:
        print("Warning: no equity curve series available to plot drawdown")
        return

    s = pd.Series(values)
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()

    running_max = s.cummax()
    dd = (s / running_max) - 1.0

    fig, ax = plt.subplots(figsize=(10, 3))
    ax.fill_between(dd.index, dd.values * 100.0, 0.0, alpha=0.35)
    ax.plot(dd.index, dd.values * 100.0, linewidth=1.0)
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Drawdown (%)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)

    plt.show()


def plot_equity_curve(
    strat, *, title: str = "Equity Curve", save_path: str | None = "equity_curve.png"
):
    """Plot a standalone equity curve (portfolio value over time).

    Uses the `flowadj` analyzer value series when available.
    """
    values = None
    try:
        fa = strat.analyzers.flowadj.get_analysis() or {}
        if isinstance(fa, dict):
            values = fa.get("values")
    except Exception:
        values = None

    if not values:
        print("Warning: no equity curve series available to plot")
        return

    s = pd.Series(values)
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(s.index, s.values, linewidth=1.5)
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)

    plt.show()
