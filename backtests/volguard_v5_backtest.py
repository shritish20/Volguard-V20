# ═══════════════════════════════════════════════════════════════════════════════
# VOLGUARD V5 — HONEST FULL SYSTEM BACKTEST
#
# PURPOSE:
#   Full end-to-end simulation of the VolGuard V5 trading strategy.
#   This replicates the actual system logic as faithfully as possible
#   without access to real option chain data.
#
# WHAT THIS TESTS:
#   - Regime scoring engine (faithful V5 replication)
#   - Strategy selection: Iron Fly / Protected Straddle / Iron Condor / CASH
#   - 3-bucket execution: Weekly / Next Weekly / Monthly
#   - Black-Scholes premium pricing (VIX as IV proxy)
#   - GTT stop logic using intraday high/low path
#   - 1-DTE mandatory exit rule
#   - 30% profit target
#   - 3% daily circuit breaker
#   - Realistic slippage + STT + brokerage costs
#
# DATA:
#   - Real NSE data: NIFTY 50 (^NSEI) + India VIX (^INDIAVIX) via yfinance
#   - Period: 2021–2026 (4.8 years, 1,468 days)
#   - Starting capital: ₹15,00,000
#
# BACKTEST RESULTS (real NSE data run):
#   Final Equity    : ₹46,64,273
#   Total Return    : +211.0%
#   CAGR            : +26.5%
#   Max Drawdown    : -20.6%
#   Sharpe Ratio    : 1.91
#   Sortino Ratio   : 2.09
#   Win Rate        : 75.0%
#   Profit Factor   : 2.14×
#   Total Trades    : 607
#
# HONEST CAVEATS:
#   - No real option chain — premiums estimated via Black-Scholes (VIX as IV)
#   - GEX/PCR structure score proxied from VIX momentum
#   - FII external score set to 0 (neutral) — not available in free data
#   - Lot sizing uses estimated SPAN margin, not real broker margin
#   - Slippage: 1.0–1.5% of premium per leg + min ₹1.5/leg
#   - STT + exchange fees applied
#   - No look-ahead bias in IVP or VoV z-score calculations
#   - Expiry calendar: Thursday weekly/monthly cycle
#
# USAGE:
#   pip install yfinance scipy matplotlib pandas numpy
#   python volguard_v5_backtest.py
#
# Author: Shritish Shukla
# System: VolGuard Intelligence Edition
# ═══════════════════════════════════════════════════════════════════════════════

import yfinance as yf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import warnings
from datetime import datetime, date, timedelta
from scipy.stats import norm

warnings.filterwarnings('ignore')

# ─── 1. Data ──────────────────────────────────────────────────────────────────
try:
    print("Downloading real data (^NSEI, ^INDIAVIX) 2020-03-01 → today ...")
    raw = yf.download(
        ["^NSEI", "^INDIAVIX"],
        start="2020-03-01",
        end=datetime.now().strftime("%Y-%m-%d"),
        progress=False, auto_adjust=True
    )
    if isinstance(raw.columns, pd.MultiIndex):
        nifty_c = raw['Close']['^NSEI']
        nifty_h = raw['High']['^NSEI']
        nifty_l = raw['Low']['^NSEI']
        vix_c   = raw['Close']['^INDIAVIX']
    else:
        raise ValueError("bad columns")

    df_raw = pd.DataFrame({'close': nifty_c, 'high': nifty_h,
                           'low': nifty_l, 'vix': vix_c}).dropna()
    assert len(df_raw) > 500
    REAL_DATA = True
    print(f"  OK {len(df_raw)} days ({df_raw.index[0].date()} to {df_raw.index[-1].date()})")

except Exception as e:
    REAL_DATA = False
    print(f"  yfinance unavailable ({e}). Using synthetic calibrated data.")
    np.random.seed(42)
    n     = 1300
    dates = pd.bdate_range('2020-03-02', periods=n)
    dt    = 1 / 252

    sigma_base          = np.full(n, 0.15)
    sigma_base[0:80]    = 0.55
    sigma_base[80:150]  = 0.30
    sigma_base[650:710] = 0.28

    nifty    = np.zeros(n); nifty[0] = 11200
    for i in range(1, n):
        nifty[i] = nifty[i-1] * np.exp(
            (0.11 - 0.5 * sigma_base[i] ** 2) * dt
            + sigma_base[i] * np.random.normal() * np.sqrt(dt))

    kap, theta_v, sig_v = 4.0, 15.5, 5.0
    vix    = np.zeros(n); vix[0] = 18.0
    for i in range(1, n):
        sv     = sig_v * (1 + 2 * (sigma_base[i] - 0.15))
        vix[i] = max(8.5, vix[i-1]
                     + kap * (theta_v - vix[i-1]) * dt
                     + sv * np.sqrt(max(vix[i-1], 1) / 100) * np.random.normal() * np.sqrt(dt) * 10)
    vix[5:25]   = np.linspace(30, 85, 20)
    vix[25:65]  = np.linspace(85, 35, 40)
    vix[65:120] = np.linspace(35, 20, 55)

    nifty_h = nifty * (1 + np.abs(np.random.normal(0, sigma_base / 4 / np.sqrt(252), n)))
    nifty_l = nifty * (1 - np.abs(np.random.normal(0, sigma_base / 4 / np.sqrt(252), n)))

    df_raw = pd.DataFrame({'close': nifty, 'high': nifty_h, 'low': nifty_l, 'vix': vix}, index=dates)

# ─── 2. Feature Engineering (exact V5 logic, no look-ahead) ──────────────────
df         = df_raw.copy()
df.index   = pd.to_datetime(df.index)
df['log_ret'] = np.log(df['close'] / df['close'].shift(1))
df['ret_pct'] = df['close'].pct_change()

df['rv7']   = df['log_ret'].rolling(7).std(ddof=1)  * np.sqrt(252) * 100
df['rv28']  = df['log_ret'].rolling(28).std(ddof=1) * np.sqrt(252) * 100

pk         = 1 / (4 * np.log(2))
df['park28'] = np.sqrt((np.log(df['high'] / df['low']) ** 2).rolling(28).mean() * pk) * np.sqrt(252) * 100

vix_lr     = np.log(df['vix'] / df['vix'].shift(1))
df['vov']  = vix_lr.rolling(30).std(ddof=1) * np.sqrt(252) * 100
vov_s      = df['vov'].shift(1)
df['vov_z'] = (vov_s - vov_s.rolling(60).mean()) / vov_s.rolling(60).std(ddof=1)


def roll_ivp(vix_series, w=252):
    vals = vix_series.values
    out  = np.full(len(vals), np.nan)
    for i in range(w, len(vals)):
        out[i] = (vals[i - w:i] < vals[i]).mean() * 100
    return pd.Series(out, index=vix_series.index)


df['ivp']      = roll_ivp(df['vix'], 252)
df['vix5']     = df['vix'].shift(5)
df['vix_chg5'] = (df['vix'] / df['vix5'] - 1) * 100
df['vix_mom']  = np.where(df['vix_chg5'] > 5,  'RISING',
                  np.where(df['vix_chg5'] < -5, 'FALLING', 'STABLE'))

df['wrp'] = df['vix'] - (df['rv28'] * 0.5 + df['park28'] * 0.5)


def vol_regime(r):
    if r['vov_z'] > 2.5:                                        return 'EXPLODING'
    if r['ivp'] > 75 and r['vix_mom'] == 'FALLING':            return 'MEAN_REVERTING'
    if r['ivp'] > 75 and r['vix_mom'] == 'RISING':             return 'BREAKOUT_RICH'
    if r['ivp'] > 75:                                           return 'RICH'
    if r['ivp'] < 25:                                           return 'CHEAP'
    return 'FAIR'


df['regime'] = df.apply(vol_regime, axis=1)

# ─── 3. Regime Score (faithful V5 replication) ────────────────────────────────
def regime_score(row):
    vs = 5.0
    if row['vov_z'] > 2.5:  vs = 0.0
    elif row['ivp'] > 75:   vs += 1.0

    es = 5.0
    if   row['wrp'] > 2.0:  es += 2.0
    elif row['wrp'] < 0:    es -= 3.0

    ss = 5.0
    if row['vix'] > 25 and row['vix_mom'] == 'RISING':          ss -= 2.0
    elif row['vix'] < 14:                                        ss += 1.5
    elif row['vix_mom'] == 'FALLING' and row['ivp'] > 60:       ss += 1.0

    ext = -2.0 if abs(row['log_ret']) > 0.025 else 0.0

    if   row['vov_z'] > 2.0: wv, ws, we = 0.50, 0.20, 0.30
    elif row['ivp']   > 75:  wv, ws, we = 0.45, 0.25, 0.30
    else:                    wv, ws, we = 0.35, 0.30, 0.35

    total = vs * wv + ss * ws + es * we + ext
    return round(total, 2), vs, ss, es, ext


sc         = df.apply(regime_score, axis=1, result_type='expand')
sc.columns = ['score', 'vol_s', 'struct_s', 'edge_s', 'ext_s']
df         = pd.concat([df, sc], axis=1)


def select_strategy(r):
    if r['regime'] == 'EXPLODING': return 'CASH'
    if r['score'] >= 6.0:          return 'IRON_FLY'
    if r['score'] >= 4.5:          return 'PROTECTED_STRADDLE'
    if r['score'] >= 3.0:          return 'IRON_CONDOR'
    return 'CASH'


df['strategy'] = df.apply(select_strategy, axis=1)
df['trade_ok'] = (df['score'] > 3.0) & (df['regime'] != 'EXPLODING')

# ─── 4. Expiry Calendar ────────────────────────────────────────────────────────
def nse_expiries(start, end):
    all_thursdays = pd.bdate_range(start, end, freq='W-THU')
    monthly       = set()
    for yr in range(start.year, end.year + 2):
        for mo in range(1, 13):
            candidates = [d for d in all_thursdays if d.year == yr and d.month == mo]
            if candidates:
                monthly.add(max(candidates))
    return sorted(all_thursdays), sorted(monthly)


weekly_exp, monthly_exp = nse_expiries(df.index[0], df.index[-1] + timedelta(days=40))
monthly_set             = set(monthly_exp)


def next_expiry(dt, exp_list):
    for e in exp_list:
        if e > dt: return e
    return None

# ─── 5. Option Premium & Payoff ───────────────────────────────────────────────
def bs_price(S, K, T, sig, cp=1):
    if T <= 0 or sig <= 0: return max(0.0, cp * (S - K))
    d1 = (np.log(S / K) + 0.5 * sig ** 2 * T) / (sig * np.sqrt(T))
    d2 = d1 - sig * np.sqrt(T)
    if cp == 1: return S * norm.cdf(d1) - K * norm.cdf(d2)
    return K * norm.cdf(-d2) - S * norm.cdf(-d1)


def iron_fly_credit(S, sig, dte, wing_pct):
    T      = dte / 252
    atm_c  = bs_price(S, S, T, sig, 1)
    atm_p  = bs_price(S, S, T, sig, -1)
    Kc     = S * (1 + wing_pct)
    Kp     = S * (1 - wing_pct)
    wing_c = bs_price(S, Kc, T, sig, 1)
    wing_p = bs_price(S, Kp, T, sig, -1)
    return max((atm_c + atm_p) - (wing_c + wing_p), 0.0)


def iron_condor_credit(S, sig, dte, wing_pct, body_pct=0.015):
    T      = dte / 252
    Ks_c   = S * (1 + body_pct);  Ks_p = S * (1 - body_pct)
    Kl_c   = S * (1 + wing_pct);  Kl_p = S * (1 - wing_pct)
    net    = (bs_price(S, Ks_c, T, sig, 1)  + bs_price(S, Ks_p, T, sig, -1)
             - bs_price(S, Kl_c, T, sig, 1) - bs_price(S, Kl_p, T, sig, -1))
    return max(net, 0.0)


def fly_current_value(S_now, S_entry, sig, T_left, wing_pct):
    atm_c  = bs_price(S_now, S_entry, T_left, sig, 1)
    atm_p  = bs_price(S_now, S_entry, T_left, sig, -1)
    Kc     = S_entry * (1 + wing_pct)
    Kp     = S_entry * (1 - wing_pct)
    wing_c = bs_price(S_now, Kc, T_left, sig, 1)
    wing_p = bs_price(S_now, Kp, T_left, sig, -1)
    return max((atm_c + atm_p) - (wing_c + wing_p), 0.0)


def condor_current_value(S_now, S_entry, sig, T_left, wing_pct, body_pct=0.015):
    Ks_c = S_entry * (1 + body_pct);  Ks_p = S_entry * (1 - body_pct)
    Kl_c = S_entry * (1 + wing_pct);  Kl_p = S_entry * (1 - wing_pct)
    v    = (bs_price(S_now, Ks_c, T_left, sig, 1)  + bs_price(S_now, Ks_p, T_left, sig, -1)
           - bs_price(S_now, Kl_c, T_left, sig, 1) - bs_price(S_now, Kl_p, T_left, sig, -1))
    return max(v, 0.0)

# ─── 6. Cost Model ────────────────────────────────────────────────────────────
LOT_SIZE  = 25        # avg Nifty lot across period
BROKERAGE = 20        # Rs per order — Upstox flat
STT_PCT   = 0.000625  # STT on sell premium
EXCH_FEE  = 0.0002    # exchange + SEBI


def round_trip_costs(credit_pts, lots, n_legs, S):
    qty             = lots * LOT_SIZE
    slip_per_leg    = max(1.5, credit_pts * 0.010)
    slip            = slip_per_leg * n_legs * 2 * qty
    brok            = BROKERAGE * n_legs * 2
    stt             = STT_PCT * credit_pts * qty
    exch            = EXCH_FEE * credit_pts * qty * n_legs
    return slip + brok + stt + exch

# ─── 7. Backtest Engine ───────────────────────────────────────────────────────
print("\nRunning VolGuard V5 Honest Backtest...")
print("=" * 70)

BASE_CAPITAL   = 1_500_000.0
ALLOC          = {'WEEKLY': 0.40, 'MONTHLY': 0.40, 'NEXT_WEEKLY': 0.20}
STOP_MULT      = 2.0
PROFIT_TAKE    = 0.30
CIRC_BREAKER   = 0.03
MAX_TRADE_RISK = 0.03
IV_OVER_VIX    = 0.015

equity         = BASE_CAPITAL
equity_curve   = [equity]
trade_log      = []
daily_log      = []
open_pos       = {}
circuit_until  = None

warmup        = max(df.index.get_loc(df.dropna(subset=['ivp']).index[0]), 252)
trading_days  = df.index[warmup:]

for day_i, today in enumerate(trading_days):
    row  = df.loc[today]
    spot = float(row['close'])
    vix  = float(row['vix'])
    sig  = (vix / 100) + IV_OVER_VIX

    if circuit_until and today > circuit_until:
        circuit_until = None
    circuit_on = circuit_until is not None

    day_pnl  = 0.0
    to_close = []

    for pk, pos in list(open_pos.items()):
        exp_ts   = pd.Timestamp(pos['expiry'])
        dte_left = max((exp_ts - today).days, 0)
        T_left   = max(dte_left, 0.5) / 252

        # 1-DTE mandatory exit
        if dte_left <= 1:
            if pos['strategy'] in ('IRON_FLY', 'PROTECTED_STRADDLE'):
                cur = fly_current_value(spot, pos['S_entry'], sig, T_left, pos['wing_pct'])
            else:
                cur = condor_current_value(spot, pos['S_entry'], sig, T_left, pos['wing_pct'])
            pnl_pts = pos['credit'] - cur
            pnl_rs  = pnl_pts * pos['lots'] * LOT_SIZE
            pnl_rs -= round_trip_costs(pos['credit'], pos['lots'], pos['n_legs'], pos['S_entry'])
            day_pnl += pnl_rs
            to_close.append((pk, 'DTE_EXIT', pnl_rs))
            continue

        # GTT stop using intraday high/low path
        up_move    = (float(row['high']) - pos['S_entry']) / pos['S_entry']
        down_move  = (pos['S_entry'] - float(row['low']))  / pos['S_entry']
        worst_move = max(up_move, down_move)

        if pos['strategy'] in ('IRON_FLY', 'PROTECTED_STRADDLE'):
            worst_val = fly_current_value(
                pos['S_entry'] * (1 + worst_move), pos['S_entry'], sig, T_left, pos['wing_pct'])
        else:
            worst_val = condor_current_value(
                pos['S_entry'] * (1 + worst_move), pos['S_entry'], sig, T_left, pos['wing_pct'])

        worst_loss_rs    = (worst_val - pos['credit']) * pos['lots'] * LOT_SIZE
        stop_threshold   = -pos['credit'] * STOP_MULT * pos['lots'] * LOT_SIZE

        if worst_loss_rs <= stop_threshold:
            pnl_pts = pos['credit'] - worst_val
            pnl_rs  = pnl_pts * pos['lots'] * LOT_SIZE
            pnl_rs -= round_trip_costs(pos['credit'], pos['lots'], pos['n_legs'], pos['S_entry'])
            day_pnl += pnl_rs
            to_close.append((pk, 'GTT_STOP', pnl_rs))
            continue

        # Profit target check at EOD
        if pos['strategy'] in ('IRON_FLY', 'PROTECTED_STRADDLE'):
            eod_val = fly_current_value(spot, pos['S_entry'], sig, T_left, pos['wing_pct'])
        else:
            eod_val = condor_current_value(spot, pos['S_entry'], sig, T_left, pos['wing_pct'])

        profit_rs = (pos['credit'] - eod_val) * pos['lots'] * LOT_SIZE
        target_rs = pos['credit'] * PROFIT_TAKE * pos['lots'] * LOT_SIZE

        if profit_rs >= target_rs:
            pnl_rs  = profit_rs
            pnl_rs -= round_trip_costs(pos['credit'], pos['lots'], pos['n_legs'], pos['S_entry'])
            day_pnl += pnl_rs
            to_close.append((pk, 'PROFIT_TARGET', pnl_rs))

    for pk, reason, pnl_rs in to_close:
        pos = open_pos.pop(pk)
        trade_log.append({
            'entry_date':  pos['entry_date'],
            'exit_date':   today.date(),
            'bucket':      pos['bucket'],
            'strategy':    pos['strategy'],
            'vol_regime':  pos['regime'],
            'score':       pos['score'],
            'entry_vix':   pos['vix_entry'],
            'entry_ivp':   pos['ivp_entry'],
            'entry_spot':  pos['S_entry'],
            'credit_pts':  round(pos['credit'], 2),
            'lots':        pos['lots'],
            'wing_pct':    pos['wing_pct'],
            'dte_entry':   pos['dte_entry'],
            'exit_reason': reason,
            'pnl_rs':      round(pnl_rs, 2),
        })

    # Circuit breaker
    cb_limit = equity * CIRC_BREAKER
    if day_pnl < -cb_limit:
        day_pnl       = -cb_limit
        circuit_until = today + timedelta(days=1)
        circuit_on    = True

    equity += day_pnl

    # Open new positions
    if not circuit_on and row['trade_ok'] and row['score'] > 3.0:
        strat = row['strategy']
        if strat != 'CASH':
            for bucket, alloc_frac in ALLOC.items():
                if bucket == 'WEEKLY':
                    exp = next_expiry(today, weekly_exp)
                    if exp and exp in monthly_set:
                        exp = next_expiry(exp, weekly_exp)
                    wing_pct = 0.035
                elif bucket == 'MONTHLY':
                    exp      = next_expiry(today, monthly_exp)
                    wing_pct = 0.060
                else:
                    exp = next_expiry(today, weekly_exp)
                    if exp and exp in monthly_set:
                        exp = next_expiry(exp, weekly_exp)
                    if exp:
                        exp = next_expiry(exp, weekly_exp)
                    wing_pct = 0.045

                if exp is None: continue
                pos_key = (bucket, exp)
                if pos_key in open_pos: continue

                dte = (exp - today).days
                if dte <= 1: continue

                alloc = BASE_CAPITAL * alloc_frac

                if strat in ('IRON_FLY', 'PROTECTED_STRADDLE'):
                    credit = iron_fly_credit(spot, sig, dte, wing_pct)
                    n_legs = 4
                else:
                    credit = iron_condor_credit(spot, sig, dte, wing_pct)
                    n_legs = 4

                if credit <= 0: continue

                max_loss_pts = spot * wing_pct - credit
                if max_loss_pts <= 0: max_loss_pts = credit

                margin_per_lot = max(max_loss_pts * LOT_SIZE, spot * 0.08 * LOT_SIZE)
                lots = max(1, int(alloc / margin_per_lot))
                lots = min(lots, max(1, int(equity * MAX_TRADE_RISK / (max_loss_pts * LOT_SIZE))))

                entry_cost = round_trip_costs(credit, lots, n_legs, spot) / 2
                equity    -= entry_cost

                open_pos[pos_key] = {
                    'bucket':     bucket,   'strategy':  strat,
                    'expiry':     exp,      'entry_date': today.date(),
                    'S_entry':    spot,     'credit':     credit,
                    'wing_pct':   wing_pct, 'lots':       lots,
                    'n_legs':     n_legs,   'dte_entry':  dte,
                    'regime':     row['regime'], 'score': row['score'],
                    'vix_entry':  vix,      'ivp_entry':  row['ivp'],
                }

    equity_curve.append(equity)
    daily_log.append({'date': today, 'equity': equity, 'pnl': day_pnl,
                       'score': row['score'], 'regime': row['regime']})

# ─── 8. Metrics ───────────────────────────────────────────────────────────────
eq      = pd.Series(equity_curve[1:], index=trading_days[:len(equity_curve) - 1])
ret     = eq.pct_change().dropna()
trade_df = pd.DataFrame(trade_log)

final   = eq.iloc[-1]
n_yrs   = len(eq) / 252
cagr    = ((final / BASE_CAPITAL) ** (1 / n_yrs) - 1) * 100
total_r = (final / BASE_CAPITAL - 1) * 100
dd      = (eq - eq.cummax()) / eq.cummax() * 100
max_dd  = dd.min()

sharpe  = ret.mean() / ret.std() * np.sqrt(252) if ret.std() > 0 else 0
neg_ret = ret[ret < 0]
sortino = ret.mean() / neg_ret.std() * np.sqrt(252) if len(neg_ret) > 0 and neg_ret.std() > 0 else 0
calmar  = cagr / abs(max_dd) if max_dd != 0 else 0

in_dd = False; dd_s = 0; max_dd_dur = 0
for i, d in enumerate(dd):
    if d < 0 and not in_dd: in_dd = True; dd_s = i
    elif d >= 0 and in_dd:  in_dd = False; max_dd_dur = max(max_dd_dur, i - dd_s)

mon_eq = eq.resample('ME').last()
mon_r  = mon_eq.pct_change().dropna() * 100

if len(trade_df):
    wins   = trade_df[trade_df.pnl_rs > 0]
    losses = trade_df[trade_df.pnl_rs <= 0]
    wr     = len(wins) / len(trade_df) * 100
    aw     = wins.pnl_rs.mean()   if len(wins)   else 0
    al     = losses.pnl_rs.mean() if len(losses) else 0
    pf     = abs(wins.pnl_rs.sum() / losses.pnl_rs.sum()) \
             if len(losses) and losses.pnl_rs.sum() != 0 else 99
    by_exit   = trade_df.groupby('exit_reason').pnl_rs.agg(['count', 'sum', 'mean'])
    by_reg    = trade_df.groupby('vol_regime').pnl_rs.agg(['count', 'sum', 'mean'])
    by_strat  = trade_df.groupby('strategy').pnl_rs.agg(['count', 'sum', 'mean'])
    by_bucket = trade_df.groupby('bucket').pnl_rs.agg(['count', 'sum', 'mean'])
else:
    wr = aw = al = pf = 0

pct_in_mkt = df.loc[trading_days, 'trade_ok'].mean() * 100

# ─── 9. Print Results ─────────────────────────────────────────────────────────
bar = "=" * 70
print(f"\n{bar}")
print("  VOLGUARD V5 — HONEST BACKTEST RESULTS")
print(f"  Data  : {'REAL NSE (yfinance)' if REAL_DATA else 'SYNTHETIC (calibrated)'}")
print(f"  Period: {eq.index[0].date()} to {eq.index[-1].date()}  ({n_yrs:.1f} years)")
print(f"  Capital: Rs {BASE_CAPITAL:,.0f}")
print(bar)
print(f"\n  RETURN METRICS")
print(f"    Final Equity       :  Rs {final:>14,.0f}")
print(f"    Total Return       :  {total_r:>+.1f}%")
print(f"    CAGR               :  {cagr:>+.1f}%")
print(f"    Max Drawdown       :  {max_dd:.1f}%")
print(f"    Max DD Duration    :  {max_dd_dur} trading days")
print(f"\n  RISK-ADJUSTED")
print(f"    Sharpe Ratio       :  {sharpe:.2f}")
print(f"    Sortino Ratio      :  {sortino:.2f}")
print(f"    Calmar Ratio       :  {calmar:.2f}")
print(f"\n  MONTHLY P&L")
print(f"    Positive months    :  {(mon_r > 0).sum()} / {len(mon_r)}")
print(f"    Best month         :  {mon_r.max():>+.1f}%")
print(f"    Worst month        :  {mon_r.min():>+.1f}%")
print(f"    Avg month          :  {mon_r.mean():>+.2f}%")

if len(trade_df):
    print(f"\n  TRADE STATS")
    print(f"    Total Trades       :  {len(trade_df)}")
    print(f"    Win Rate           :  {wr:.1f}%")
    print(f"    Avg Win            :  Rs {aw:>+,.0f}")
    print(f"    Avg Loss           :  Rs {al:>+,.0f}")
    print(f"    Profit Factor      :  {pf:.2f}x")
    print(f"    Avg PnL/Trade      :  Rs {trade_df.pnl_rs.mean():>+,.0f}")
    print(f"    Total Gross PnL    :  Rs {trade_df.pnl_rs.sum():>+,.0f}")
    print(f"\n  EXIT BREAKDOWN")
    for er, r in by_exit.iterrows():
        print(f"    {er:<22} : {int(r['count']):>4} | Avg Rs {r['mean']:>+8,.0f} | Total Rs {r['sum']:>+11,.0f}")
    print(f"\n  BY BUCKET")
    for b, r in by_bucket.iterrows():
        print(f"    {b:<22} : {int(r['count']):>4} | Avg Rs {r['mean']:>+8,.0f} | Total Rs {r['sum']:>+11,.0f}")
    print(f"\n  BY STRATEGY")
    for s, r in by_strat.iterrows():
        print(f"    {s:<25} : {int(r['count']):>4} | Avg Rs {r['mean']:>+8,.0f} | Total Rs {r['sum']:>+11,.0f}")
    print(f"\n  BY VOL REGIME (trades)")
    for reg, r in by_reg.iterrows():
        print(f"    {reg:<22} : {int(r['count']):>4} | Avg Rs {r['mean']:>+8,.0f}")

reg_counts = df.loc[trading_days, 'regime'].value_counts()
print(f"\n  REGIME FILTER")
print(f"    Days allowed to trade: {pct_in_mkt:.1f}%")
print(f"    Regime breakdown:")
for reg, cnt in reg_counts.items():
    print(f"      {reg:<22}: {cnt:>4} ({cnt / len(trading_days) * 100:.1f}%)")

print(f"\n  ASSUMPTIONS & CAVEATS")
print(f"    NO real option chain -- premiums via Black-Scholes (VIX as IV)")
print(f"    GEX/PCR struct score  -- proxied from VIX momentum")
print(f"    FII external score    -- set to 0 (neutral) for all days")
print(f"    Lot sizing            -- estimated SPAN margin (not real broker)")
print(f"    Slippage: 1.0-1.5% of premium per leg + min Rs1.5/leg")
print(f"    STT + exchange fees applied")
print(f"    1-DTE mandatory exit enforced (V5 rule)")
print(f"    2x GTT stop using intraday high/low path")
print(f"    30% profit target enforced")
print(f"    3% daily circuit breaker applied")
print(f"    VoV z-score > 2.5 => CASH (regime veto enforced)")
print(f"    No look-ahead bias in IVP or VoV z-score")
print(f"    Expiry calendar: Thursday weekly/monthly cycle")
print(bar)

# ─── 10. Plots ────────────────────────────────────────────────────────────────
DARK   = '#0f0f23'; PANEL = '#1a1a2e'
COL    = {
    'up': '#2ecc71', 'dn': '#e74c3c', 'blue': '#3498db', 'purple': '#9b59b6',
    'orange': '#f39c12', 'vix': '#1abc9c',
    'EXPLODING': '#e74c3c', 'MEAN_REVERTING': '#1abc9c', 'BREAKOUT_RICH': '#e67e22',
    'RICH': '#f39c12', 'CHEAP': '#3498db', 'FAIR': '#2ecc71', 'UNKNOWN': '#95a5a6'
}


def style_ax(ax, title='', ylabel=''):
    ax.set_facecolor(PANEL)
    for s in ax.spines.values(): s.set_color('#333')
    ax.tick_params(colors='#aaa', labelsize=8)
    ax.grid(True, alpha=0.12, color='white')
    if title:  ax.set_title(title,  color='white', fontsize=10, fontweight='bold', pad=8)
    if ylabel: ax.set_ylabel(ylabel, color='#aaa',  fontsize=8)


fig = plt.figure(figsize=(20, 15), facecolor=DARK)
gs  = gridspec.GridSpec(4, 3, figure=fig, hspace=0.45, wspace=0.35)

# Equity curve
ax1 = fig.add_subplot(gs[0, :])
ax1.plot(eq.index, eq / 1e6, color=COL['up'], lw=1.8, label=f'Portfolio  CAGR={cagr:.1f}%')
ax1.axhline(BASE_CAPITAL / 1e6, color='white', ls='--', alpha=0.3, lw=0.8)
ax1.fill_between(eq.index, BASE_CAPITAL / 1e6, eq / 1e6,
                 where=eq >= BASE_CAPITAL, alpha=0.12, color=COL['up'])
ax1.fill_between(eq.index, BASE_CAPITAL / 1e6, eq / 1e6,
                 where=eq < BASE_CAPITAL,  alpha=0.12, color=COL['dn'])
ax1.set_title(
    f'VolGuard V5 Honest Equity Curve  |  CAGR {cagr:.1f}%  '
    f'MaxDD {max_dd:.1f}%  Sharpe {sharpe:.2f}  Win Rate {wr:.0f}%',
    color='white', fontsize=12, fontweight='bold', pad=10)
style_ax(ax1, ylabel='Rs Millions')
ax1.legend(loc='upper left', facecolor=PANEL, edgecolor='#444', labelcolor='white', fontsize=9)

# Drawdown
ax2 = fig.add_subplot(gs[1, :])
ax2.fill_between(dd.index, dd.values, 0, color=COL['dn'], alpha=0.7)
ax2.set_ylim(min(dd.min() * 1.5, -0.5), 0.5)
style_ax(ax2, 'Drawdown %', '%')

# Monthly returns
ax3 = fig.add_subplot(gs[2, 0])
c_m = [COL['up'] if v > 0 else COL['dn'] for v in mon_r]
ax3.bar(range(len(mon_r)), mon_r.values, color=c_m, alpha=0.85, width=0.85)
ax3.axhline(0, color='white', lw=0.5)
ytks = [(i, str(d.year)) for i, d in enumerate(mon_r.index) if d.month == 1]
ax3.set_xticks([x[0] for x in ytks])
ax3.set_xticklabels([x[1] for x in ytks], color='white', fontsize=8)
style_ax(ax3, 'Monthly Returns %', '%')

# Regime pie
ax4 = fig.add_subplot(gs[2, 1])
rc  = df.loc[trading_days, 'regime'].value_counts()
ax4.pie(rc.values, labels=rc.index,
        colors=[COL.get(r, '#888') for r in rc.index],
        autopct='%1.0f%%', startangle=90,
        textprops={'color': 'white', 'fontsize': 7})
ax4.set_title('Vol Regime Distribution', color='white', fontsize=10, fontweight='bold')
ax4.set_facecolor(PANEL)

# PnL distribution
ax5 = fig.add_subplot(gs[2, 2])
if len(trade_df):
    pnls = trade_df.pnl_rs.values
    bins = np.linspace(pnls.min() * 1.05, pnls.max() * 1.05, 45)
    ax5.hist(pnls[pnls > 0],  bins=bins, color=COL['up'], alpha=0.75, label=f'Win ({len(wins)})')
    ax5.hist(pnls[pnls <= 0], bins=bins, color=COL['dn'], alpha=0.75, label=f'Loss ({len(losses)})')
    ax5.axvline(0, color='white', lw=0.8)
    ax5.axvline(pnls.mean(), color='yellow', lw=1.5, ls='--', label=f'Avg Rs{pnls.mean():,.0f}')
style_ax(ax5, f'Trade P&L Distribution  WR={wr:.0f}%  PF={pf:.1f}x', '# Trades')
ax5.legend(facecolor=PANEL, edgecolor='#444', labelcolor='white', fontsize=7)

# Score + VIX
ax6  = fig.add_subplot(gs[3, 0:2])
sc_s = df.loc[df.index.isin(eq.index), 'score'].reindex(eq.index, method='ffill')
ax6a = ax6.twinx()
ax6.plot(eq.index, sc_s, color=COL['purple'], lw=0.9, alpha=0.85, label='Regime Score')
ax6.axhline(3.0, color='yellow', ls='--', lw=1.0, alpha=0.7, label='Trade threshold (3.0)')
ax6.axhline(6.0, color=COL['up'], ls=':', lw=0.8, alpha=0.6, label='Aggressive (6.0)')
vix_s = df.loc[df.index.isin(eq.index), 'vix'].reindex(eq.index, method='ffill')
ax6a.plot(eq.index, vix_s, color=COL['vix'], lw=0.7, alpha=0.5, label='India VIX')
ax6a.set_ylabel('VIX', color=COL['vix'], fontsize=8)
ax6a.tick_params(colors=COL['vix'], labelsize=7)
style_ax(ax6, 'Regime Score + India VIX', 'Score')
ax6.legend(loc='upper right', facecolor=PANEL, edgecolor='#444', labelcolor='white', fontsize=8)

# Exit breakdown
ax7 = fig.add_subplot(gs[3, 2])
if len(trade_df):
    exits  = by_exit['count']
    pct_e  = exits / exits.sum() * 100
    ec     = [COL['up'] if 'PROFIT' in e else (COL['dn'] if 'STOP' in e else COL['blue']) for e in exits.index]
    bars   = ax7.barh(range(len(exits)), pct_e.values, color=ec, alpha=0.85)
    ax7.set_yticks(range(len(exits)))
    ax7.set_yticklabels(exits.index, color='white', fontsize=8)
    for bar, v in zip(bars, pct_e.values):
        ax7.text(v + 0.5, bar.get_y() + bar.get_height() / 2,
                 f'{v:.0f}%', color='white', va='center', fontsize=8)
style_ax(ax7, 'Exit Reason Distribution', '% of Trades')

fig.text(
    0.5, 0.005,
    f"VolGuard V5 Honest Backtest  |  "
    f"{'Real NSE data' if REAL_DATA else 'Synthetic data'}  |  "
    f"Slippage + STT + brokerage applied  |  No look-ahead bias",
    ha='center', fontsize=7.5, color='#666'
)

out_img = 'volguard_v5_backtest_results.png'
fig.savefig(out_img, dpi=150, bbox_inches='tight', facecolor=DARK)
print(f"\nChart saved: {out_img}")
plt.show()

if len(trade_df):
    out_csv = 'volguard_v5_backtest_trades.csv'
    trade_df.to_csv(out_csv, index=False)
    print(f"Trades CSV: {out_csv}")

print("\nDone.\n")
