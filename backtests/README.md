# VolGuard — Backtests

Two standalone scripts that validate the VolGuard trading edge.

---

## 1. `vrp_backtest_10year.py` — VRP Existence Proof

**Purpose:** Proves that Volatility Risk Premium (VRP) structurally exists in NIFTY over 10 years.

**What it answers:** Does IV consistently exceed RV? Is the edge real and statistically significant?

**Data:** Real NSE + India VIX via yfinance · 2015–2025 · 2,213 trading days

**Key results:**
- VRP positive **84.8%** of all trading days
- Mean weighted VRP: **+2.14%**
- t-statistic: **~28.7** (far beyond any significance threshold)
- Formula validated: GARCH×70% + Parkinson×15% + Standard RV×15%

**Run:**
```bash
pip install yfinance arch pandas numpy matplotlib seaborn
python vrp_backtest_10year.py
```

---

## 2. `volguard_v5_backtest.py` — Full System Backtest

**Purpose:** End-to-end simulation of the VolGuard V5 strategy with realistic execution.

**What it tests:** Regime scoring → strategy selection → position sizing → GTT stops → exits → costs

**Data:** Real NSE + India VIX via yfinance · 2021–2026 · 4.8 years · ₹15L starting capital

**Key results:**
- CAGR: **+26.5%**
- Total Return: **+211%**
- Sharpe Ratio: **1.91**
- Sortino Ratio: **2.09**
- Max Drawdown: **−20.6%**
- Win Rate: **75.0%**
- Profit Factor: **2.14×**
- Total Trades: **607**

**Run:**
```bash
pip install yfinance scipy matplotlib pandas numpy
python volguard_v5_backtest.py
```

---

## Honest Caveats

Both scripts use **real NSE price data** but **estimated option premiums** via Black-Scholes (India VIX as IV proxy). Real option chain data requires a paid feed.

The following are applied to make results realistic:
- Slippage: 1.0–1.5% of premium per leg + min ₹1.5/leg
- STT + exchange + brokerage fees
- No look-ahead bias in IVP or VoV z-score
- 1-DTE mandatory exit enforced
- 2× GTT stop using intraday high/low path
- 3% daily circuit breaker

**These are not paper results. They are honest estimates with conservative assumptions.**

---

## Why Two Files?

`vrp_backtest_10year.py` — Pure science. Does the edge exist? 10 years of proof.

`volguard_v5_backtest.py` — Full system. Does the strategy capture it profitably with real costs?

Both questions need separate answers.
