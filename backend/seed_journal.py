"""
VolGuard Journal Seeder — Demo Data for Journal Coach Showcase
==============================================================
Generates ~40 realistic NIFTY weekly/monthly options trades (Oct 2025 – Mar 2026)
with full context snapshots so the Journal Coach can demonstrate deep pattern analysis.

The data is DESIGNED to have discoverable patterns:
  ✓ RISK_OFF + HIGH VoV days → losses when traded (coaching opportunity)
  ✓ High IVP (>70) + CLEAR tone → strong theta wins
  ✓ One cluster of bad trades in Nov 2025 (RBI surprise) the coach can identify
  ✓ Improving discipline over time (overrides dropped from Jan 2026 onwards)
  ✓ Iron Condors outperforming Straddles in this dataset (strategy insight)
  ✓ Theta/Vega attribution data rich enough for greek analysis

Usage:
  python seed_journal.py                        # writes to ./volguard.db
  python seed_journal.py --db /path/to/volguard.db
  python seed_journal.py --clear               # wipe existing trades first

Run from the backend/ folder or pass --db to point at your actual db file.
"""

import argparse
import json
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ── Outcome classifier (mirrors volguard_v6_final.py — keep in sync) ─────────

def classify_trade_outcome(realized_pnl, theta_pnl, vega_pnl,
                            vov_zscore, regime_score, morning_tone, pretrade_verdict):
    vov     = float(vov_zscore or 0.0)
    score   = float(regime_score or 0.0)
    tone    = str(morning_tone or '')
    verdict = str(pretrade_verdict or '')
    pnl     = float(realized_pnl or 0.0)
    theta   = float(theta_pnl or 0.0)
    vega    = float(vega_pnl or 0.0)

    good_entry = (
        vov < 1.5 and score >= 4.5 and
        tone not in ('RISK_OFF',) and verdict not in ('VETO',)
    )
    won            = pnl > 0
    theta_dominant = abs(theta) >= abs(vega)

    if good_entry and won and theta_dominant:     return 'SKILL_WIN'
    elif good_entry and won:                      return 'LUCKY_WIN'
    elif good_entry and not won and not theta_dominant: return 'UNLUCKY_LOSS'
    elif good_entry and not won:                  return 'SKILL_LOSS'
    elif not good_entry and won:                  return 'LUCKY_WIN'
    else:                                         return 'SKILL_LOSS'

# ── Config ────────────────────────────────────────────────────────────────────
NIFTY_BASE = 23500  # Approximate NIFTY level for the period

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_id():
    return f"SEED-{uuid.uuid4().hex[:10].upper()}"

def dt(date_str, time_str="09:20:00"):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

def expiry_dt(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

def ic_legs(spot, width=300, premium_per_leg=55):
    """Iron Condor legs around spot price."""
    ce_sell = round(spot / 50) * 50 + width
    ce_buy  = ce_sell + 200
    pe_sell = round(spot / 50) * 50 - width
    pe_buy  = pe_sell - 200
    return [
        {"symbol": f"NIFTY{ce_sell}CE", "action": "SELL", "option_type": "CE",
         "strike": ce_sell, "quantity": 50, "entry_price": premium_per_leg, "instrument_token": f"tok_ce_s_{ce_sell}"},
        {"symbol": f"NIFTY{ce_buy}CE",  "action": "BUY",  "option_type": "CE",
         "strike": ce_buy,  "quantity": 50, "entry_price": round(premium_per_leg * 0.35, 1), "instrument_token": f"tok_ce_b_{ce_buy}"},
        {"symbol": f"NIFTY{pe_sell}PE", "action": "SELL", "option_type": "PE",
         "strike": pe_sell, "quantity": 50, "entry_price": premium_per_leg, "instrument_token": f"tok_pe_s_{pe_sell}"},
        {"symbol": f"NIFTY{pe_buy}PE",  "action": "BUY",  "option_type": "PE",
         "strike": pe_buy,  "quantity": 50, "entry_price": round(premium_per_leg * 0.35, 1), "instrument_token": f"tok_pe_b_{pe_buy}"},
    ]

def straddle_legs(spot, premium=180):
    atm = round(spot / 50) * 50
    return [
        {"symbol": f"NIFTY{atm}CE", "action": "SELL", "option_type": "CE",
         "strike": atm, "quantity": 50, "entry_price": premium, "instrument_token": f"tok_atm_ce_{atm}"},
        {"symbol": f"NIFTY{atm}PE", "action": "SELL", "option_type": "PE",
         "strike": atm, "quantity": 50, "entry_price": premium, "instrument_token": f"tok_atm_pe_{atm}"},
    ]

def bull_put_legs(spot, width=200, premium=45):
    sell_strike = round(spot / 50) * 50 - 200
    buy_strike  = sell_strike - width
    return [
        {"symbol": f"NIFTY{sell_strike}PE", "action": "SELL", "option_type": "PE",
         "strike": sell_strike, "quantity": 50, "entry_price": premium, "instrument_token": f"tok_bp_s_{sell_strike}"},
        {"symbol": f"NIFTY{buy_strike}PE",  "action": "BUY",  "option_type": "PE",
         "strike": buy_strike,  "quantity": 50, "entry_price": round(premium * 0.4, 1), "instrument_token": f"tok_bp_b_{buy_strike}"},
    ]

def greeks_snapshot(legs, delta_per_lot=0.12, theta_per_lot=-18, vega_per_lot=22, iv=0.16):
    snap = {}
    for leg in legs:
        direction = -1 if leg["action"] == "SELL" else 1
        snap[leg["instrument_token"]] = {
            "delta": round(direction * delta_per_lot * (1 if leg["option_type"] == "CE" else -0.9), 4),
            "theta": round(theta_per_lot, 2),
            "vega":  round(vega_per_lot, 2),
            "gamma": round(0.003, 4),
            "iv":    round(iv, 4),
        }
    return snap

# ── Trade definitions ─────────────────────────────────────────────────────────
# Each dict = one completed trade. Keys map directly to DB columns.
# Pattern design:
#   - RISK_OFF / high VoV overrides → losses (coach can flag this)
#   - High IVP + CLEAR → profits
#   - Nov 2025 = bad month (RBI surprise event)
#   - Gradual improvement in discipline (no overrides after Jan 2026)

TRADES = [
    # ─── Oct 2025 — Good start, clear market, high IVP ────────────────────────
    {
        "entry_date": "2025-10-06", "entry_time_str": "09:22:00",
        "exit_date": "2025-10-09",  "exit_time_str": "14:45:00",
        "expiry_date": "2025-10-09", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23800, "entry_premium": 220, "exit_premium": 48,
        "max_profit": 8600, "max_loss": 21400, "allocated_capital": 120000,
        "theta_pnl": 6800, "vega_pnl": -1200, "gamma_pnl": -340,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.4, "ivp": 82, "regime_score": 7.8,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.45, "weighted_vrp": 4.2,
        "score_drivers": ["IVP 82 — premium RICH", "VoV stable 0.45σ", "FII net long futures", "PCR 1.18 neutral-bullish"],
        "pretrade_rationale": "Strong theta setup. IVP at 82nd percentile with VIX 12.4 — elevated premium for the range. Iron Condor width 600 gives high probability. Morning tone CLEAR, no macro events this week. PROCEED with full allocation.",
    },
    {
        "entry_date": "2025-10-13", "entry_time_str": "09:25:00",
        "exit_date": "2025-10-16",  "exit_time_str": "14:50:00",
        "expiry_date": "2025-10-16", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23950, "entry_premium": 195, "exit_premium": 52,
        "max_profit": 7150, "max_loss": 22850, "allocated_capital": 120000,
        "theta_pnl": 5900, "vega_pnl": -980, "gamma_pnl": -180,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.1, "ivp": 78, "regime_score": 7.5,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.38, "weighted_vrp": 3.8,
        "score_drivers": ["IVP 78 — premium elevated", "VIX declining trend", "Gamma risk low DTE=3", "Structure: Max pain 24000"],
        "pretrade_rationale": "Continuation of good regime. VIX 12.1 trending down, IV premium still elevated. PCR healthy, no major events. Iron Condor expiry week — time decay accelerating. PROCEED.",
    },
    {
        "entry_date": "2025-10-20", "entry_time_str": "09:18:00",
        "exit_date": "2025-10-23",  "exit_time_str": "14:55:00",
        "expiry_date": "2025-10-23", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24100, "entry_premium": 205, "exit_premium": 44,
        "max_profit": 8050, "max_loss": 21950, "allocated_capital": 120000,
        "theta_pnl": 7200, "vega_pnl": -750, "gamma_pnl": -280,
        "exit_reason": "PROFIT_TARGET",
        "vix": 11.8, "ivp": 76, "regime_score": 7.9,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.31, "weighted_vrp": 3.5,
        "score_drivers": ["IVP 76 — 3rd consecutive rich week", "VoV 0.31σ — extremely stable", "US Fed no surprise", "Budget rally ongoing"],
        "pretrade_rationale": "Regime exceptionally stable. Third consecutive week of rich premium. VoV at 6-month low. NIFTY in tight range 23800-24200. Full IC allocation.",
    },
    {
        "entry_date": "2025-10-27", "entry_time_str": "09:20:00",
        "exit_date": "2025-10-30",  "exit_time_str": "15:00:00",
        "expiry_date": "2025-10-30", "expiry_type": "WEEKLY",
        "strategy_type": "BULL_PUT_SPREAD",
        "spot": 24200, "entry_premium": 82, "exit_premium": 18,
        "max_profit": 3200, "max_loss": 6800, "allocated_capital": 60000,
        "theta_pnl": 3100, "vega_pnl": -220, "gamma_pnl": -80,
        "exit_reason": "PROFIT_TARGET",
        "vix": 11.5, "ivp": 74, "regime_score": 7.6,
        "vol_regime": "RICH", "morning_tone": "CAUTIOUS_NEUTRAL",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.52, "weighted_vrp": 3.1,
        "score_drivers": ["FII net selling futures", "Slight skew uptick — mild caution", "IVP 74 still rich", "PCR 0.92 — slight put bias"],
        "pretrade_rationale": "Morning tone cautious neutral due to FII selling pressure. Using Bull Put Spread instead of full IC — directional bias towards support. Reduced allocation.",
    },

    # ─── Nov 2025 — RBI surprise + global volatility spike ────────────────────
    {
        "entry_date": "2025-11-03", "entry_time_str": "09:15:00",
        "exit_date": "2025-11-05",  "exit_time_str": "10:30:00",
        "expiry_date": "2025-11-06", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23900, "entry_premium": 240, "exit_premium": 580,
        "max_profit": 9400, "max_loss": 20600, "allocated_capital": 120000,
        "theta_pnl": 2100, "vega_pnl": -18400, "gamma_pnl": -3200,
        "exit_reason": "STOP_LOSS",
        "vix": 15.8, "ivp": 71, "regime_score": 6.1,
        "vol_regime": "RICH", "morning_tone": "CAUTIOUS",
        "pretrade_verdict": "PROCEED_WITH_CAUTION",
        "vov_zscore": 1.82, "weighted_vrp": 2.8,
        "score_drivers": ["VoV 1.82σ — elevated caution flag", "RBI policy meet Wed", "FII net short — 3 consecutive days", "IVP 71 but VoV rising"],
        "pretrade_rationale": "VoV elevated at 1.82σ — system flagged caution. RBI policy on Wednesday. Despite caution signal, proceeded with full IC — OVERRIDDEN caution. This was the critical error. IV exploded post-RBI surprise rate hold.",
    },
    {
        "entry_date": "2025-11-10", "entry_time_str": "09:28:00",
        "exit_date": "2025-11-11",  "exit_time_str": "11:15:00",
        "expiry_date": "2025-11-13", "expiry_type": "WEEKLY",
        "strategy_type": "STRADDLE",
        "spot": 23200, "entry_premium": 310, "exit_premium": 680,
        "max_profit": 15500, "max_loss": 99999, "allocated_capital": 80000,
        "theta_pnl": 1800, "vega_pnl": -21200, "gamma_pnl": -4100,
        "exit_reason": "STOP_LOSS",
        "vix": 18.2, "ivp": 68, "regime_score": 4.8,
        "vol_regime": "FAIR", "morning_tone": "RISK_OFF",
        "pretrade_verdict": "VETO",
        "vov_zscore": 2.41, "weighted_vrp": 1.4,
        "score_drivers": ["RISK_OFF global tone", "US CPI miss + Fed hawkish", "VoV 2.41σ — system VETO", "VIX 18.2 — regime shift"],
        "pretrade_rationale": "System issued VETO — VoV 2.41σ, RISK_OFF global tone, US CPI surprise. OVERRIDE by trader. Straddle entered to 'catch the volatility' — wrong strategy in this regime. Should have stayed in cash as system recommended.",
    },
    {
        "entry_date": "2025-11-17", "entry_time_str": "09:22:00",
        "exit_date": "2025-11-20",  "exit_time_str": "14:30:00",
        "expiry_date": "2025-11-20", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23400, "entry_premium": 165, "exit_premium": 280,
        "max_profit": 5750, "max_loss": 24250, "allocated_capital": 100000,
        "theta_pnl": 3200, "vega_pnl": -8100, "gamma_pnl": -1900,
        "exit_reason": "STOP_LOSS",
        "vix": 16.9, "ivp": 65, "regime_score": 5.2,
        "vol_regime": "FAIR", "morning_tone": "RISK_OFF",
        "pretrade_verdict": "PROCEED_WITH_CAUTION",
        "vov_zscore": 1.94, "weighted_vrp": 1.9,
        "score_drivers": ["Volatility regime shifted FAIR", "FII still net short", "GIFT Nifty -1.2% gap down", "PCR 0.78 — put heavy"],
        "pretrade_rationale": "Trying to recover after two stop losses. Entered IC with reduced premium. Global risk-off continuing. VoV still elevated at 1.94σ. Should have waited for regime to normalize.",
    },
    {
        "entry_date": "2025-11-24", "entry_time_str": "09:30:00",
        "exit_date": "2025-11-27",  "exit_time_str": "14:55:00",
        "expiry_date": "2025-11-27", "expiry_type": "WEEKLY",
        "strategy_type": "BULL_PUT_SPREAD",
        "spot": 23600, "entry_premium": 55, "exit_premium": 14,
        "max_profit": 2050, "max_loss": 7950, "allocated_capital": 40000,
        "theta_pnl": 1900, "vega_pnl": -480, "gamma_pnl": -120,
        "exit_reason": "PROFIT_TARGET",
        "vix": 15.1, "ivp": 62, "regime_score": 5.8,
        "vol_regime": "FAIR", "morning_tone": "CAUTIOUS",
        "pretrade_verdict": "PROCEED_WITH_CAUTION",
        "vov_zscore": 1.41, "weighted_vrp": 2.1,
        "score_drivers": ["Vol regime improving from Nov lows", "VoV declining — 1.41σ", "Reduced allocation — 33% capital", "Bull put only — directional caution"],
        "pretrade_rationale": "VoV cooling down. Using conservative Bull Put Spread with minimal allocation to stay active but not overexpose. Market stabilizing post-RBI reaction.",
    },

    # ─── Dec 2025 — Recovery, discipline improving ────────────────────────────
    {
        "entry_date": "2025-12-01", "entry_time_str": "09:20:00",
        "exit_date": "2025-12-04",  "exit_time_str": "14:45:00",
        "expiry_date": "2025-12-04", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23750, "entry_premium": 178, "exit_premium": 42,
        "max_profit": 6800, "max_loss": 23200, "allocated_capital": 100000,
        "theta_pnl": 5400, "vega_pnl": -1800, "gamma_pnl": -420,
        "exit_reason": "PROFIT_TARGET",
        "vix": 13.8, "ivp": 67, "regime_score": 6.8,
        "vol_regime": "FAIR", "morning_tone": "CAUTIOUS_NEUTRAL",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.91, "weighted_vrp": 2.9,
        "score_drivers": ["VoV normalising — 0.91σ", "VIX 13.8 — declining", "IVP recovering to 67", "Year-end low volatility pattern"],
        "pretrade_rationale": "Regime recovering. VoV back below 1σ. VIX trending down. Year-end drift pattern typically low vol. Back to normal IC sizing.",
    },
    {
        "entry_date": "2025-12-08", "entry_time_str": "09:22:00",
        "exit_date": "2025-12-11",  "exit_time_str": "15:00:00",
        "expiry_date": "2025-12-11", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23850, "entry_premium": 192, "exit_premium": 38,
        "max_profit": 7700, "max_loss": 22300, "allocated_capital": 120000,
        "theta_pnl": 6900, "vega_pnl": -1100, "gamma_pnl": -310,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.9, "ivp": 70, "regime_score": 7.1,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.67, "weighted_vrp": 3.4,
        "score_drivers": ["IVP crossed 70 — premium rich again", "VoV 0.67σ — stable", "CLEAR morning tone", "FII net long returning"],
        "pretrade_rationale": "Premium regime back to RICH. Full IC sizing. IVP 70 with VoV stable. CLEAR tone — ideal theta selling conditions.",
    },
    {
        "entry_date": "2025-12-15", "entry_time_str": "09:18:00",
        "exit_date": "2025-12-18",  "exit_time_str": "14:50:00",
        "expiry_date": "2025-12-18", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24050, "entry_premium": 208, "exit_premium": 46,
        "max_profit": 8100, "max_loss": 21900, "allocated_capital": 120000,
        "theta_pnl": 7100, "vega_pnl": -850, "gamma_pnl": -390,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.3, "ivp": 73, "regime_score": 7.4,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.44, "weighted_vrp": 3.8,
        "score_drivers": ["Strong theta setup", "IVP 73 + VoV 0.44σ ideal combo", "FII net long futures 3 days", "FOMC in Jan — no near-term risk"],
        "pretrade_rationale": "Ideal regime conditions. IVP 73, VoV 0.44σ, CLEAR tone. FII supporting. Best week since October.",
    },
    {
        "entry_date": "2025-12-22", "entry_time_str": "09:25:00",
        "exit_date": "2025-12-23",  "exit_time_str": "12:00:00",
        "expiry_date": "2025-12-25", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24100, "entry_premium": 145, "exit_premium": 310,
        "max_profit": 5250, "max_loss": 24750, "allocated_capital": 100000,
        "theta_pnl": 1200, "vega_pnl": -10800, "gamma_pnl": -2100,
        "exit_reason": "STOP_LOSS",
        "vix": 14.1, "ivp": 70, "regime_score": 6.5,
        "vol_regime": "RICH", "morning_tone": "CAUTIOUS",
        "pretrade_verdict": "PROCEED_WITH_CAUTION",
        "vov_zscore": 1.22, "weighted_vrp": 2.7,
        "score_drivers": ["Christmas week — thin liquidity", "VoV 1.22σ — caution", "Global markets holiday", "Bid-ask spreads wide"],
        "pretrade_rationale": "Entered despite caution signal. Christmas week thin liquidity caused slippage and sudden IV spike. VoV 1.22σ was a warning that was underweighted.",
    },
    {
        "entry_date": "2025-12-29", "entry_time_str": "09:20:00",
        "exit_date": "2026-01-01",  "exit_time_str": "14:00:00",
        "expiry_date": "2026-01-01", "expiry_type": "WEEKLY",
        "strategy_type": "BULL_PUT_SPREAD",
        "spot": 23950, "entry_premium": 62, "exit_premium": 15,
        "max_profit": 2350, "max_loss": 7650, "allocated_capital": 40000,
        "theta_pnl": 2100, "vega_pnl": -320, "gamma_pnl": -100,
        "exit_reason": "PROFIT_TARGET",
        "vix": 13.2, "ivp": 68, "regime_score": 6.9,
        "vol_regime": "FAIR", "morning_tone": "CAUTIOUS_NEUTRAL",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.78, "weighted_vrp": 2.8,
        "score_drivers": ["New Year week — cautious sizing", "Theta decaying fast on last week", "VoV normalised", "Conservative approach post Dec loss"],
        "pretrade_rationale": "Year-end trade — minimal size. Learning from Christmas week lesson. Bull Put only, 33% allocation. VoV 0.78σ acceptable.",
    },

    # ─── Jan 2026 — FOMC caution, better discipline ───────────────────────────
    {
        "entry_date": "2026-01-05", "entry_time_str": "09:22:00",
        "exit_date": "2026-01-08",  "exit_time_str": "14:45:00",
        "expiry_date": "2026-01-08", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24050, "entry_premium": 188, "exit_premium": 43,
        "max_profit": 7250, "max_loss": 22750, "allocated_capital": 120000,
        "theta_pnl": 6200, "vega_pnl": -1400, "gamma_pnl": -350,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.8, "ivp": 72, "regime_score": 7.2,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.55, "weighted_vrp": 3.6,
        "score_drivers": ["New year — fresh mandate", "IVP 72 RICH", "VoV 0.55σ stable", "FOMC only later in month"],
        "pretrade_rationale": "Clean start to 2026. Good premium, stable vol. No macro events this week. FOMC not until Jan 29. Full IC allocation.",
    },
    {
        "entry_date": "2026-01-12", "entry_time_str": "09:20:00",
        "exit_date": "2026-01-15",  "exit_time_str": "14:50:00",
        "expiry_date": "2026-01-15", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24200, "entry_premium": 196, "exit_premium": 47,
        "max_profit": 7450, "max_loss": 22550, "allocated_capital": 120000,
        "theta_pnl": 6800, "vega_pnl": -1100, "gamma_pnl": -290,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.5, "ivp": 75, "regime_score": 7.5,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.48, "weighted_vrp": 3.9,
        "score_drivers": ["IVP 75 — premium very rich", "VoV 0.48σ — stable", "Earnings season boost in premium", "FII net long 4 days"],
        "pretrade_rationale": "Earnings season inflating premium. IVP 75 with stable VoV — excellent IC conditions. FII supporting. Strong theta setup.",
    },
    {
        "entry_date": "2026-01-19", "entry_time_str": "09:18:00",
        "exit_date": "2026-01-22",  "exit_time_str": "14:55:00",
        "expiry_date": "2026-01-22", "expiry_type": "WEEKLY",
        "strategy_type": "BULL_PUT_SPREAD",
        "spot": 24150, "entry_premium": 68, "exit_premium": 16,
        "max_profit": 2600, "max_loss": 7400, "allocated_capital": 50000,
        "theta_pnl": 2400, "vega_pnl": -480, "gamma_pnl": -140,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.9, "ivp": 74, "regime_score": 7.0,
        "vol_regime": "RICH", "morning_tone": "CAUTIOUS_NEUTRAL",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.72, "weighted_vrp": 3.2,
        "score_drivers": ["FOMC Jan 29 — pre-event caution", "Bull Put only — protecting upside", "VoV rising slightly pre-FOMC", "Reduced allocation — FOMC week ahead"],
        "pretrade_rationale": "One week before FOMC. Using conservative Bull Put Spread. VoV starting to tick up (0.72σ) as market prices in uncertainty. Correct risk management.",
    },
    {
        "entry_date": "2026-01-26", "entry_time_str": "09:22:00",
        "exit_date": "2026-01-27",  "exit_time_str": "09:05:00",
        "expiry_date": "2026-01-29", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24000, "entry_premium": 210, "exit_premium": 0,
        "max_profit": 8200, "max_loss": 21800, "allocated_capital": 0,
        "theta_pnl": 0, "vega_pnl": 0, "gamma_pnl": 0,
        "exit_reason": "FOMC_VETO_SQUARE_OFF",
        "vix": 13.4, "ivp": 71, "regime_score": 6.4,
        "vol_regime": "RICH", "morning_tone": "CAUTIOUS",
        "pretrade_verdict": "VETO",
        "vov_zscore": 1.55, "weighted_vrp": 2.6,
        "score_drivers": ["FOMC Jan 29 — VETO EVENT", "System blocked entry", "VoV 1.55σ elevated pre-event", "Square off order auto-placed"],
        "pretrade_rationale": "FOMC week — system correctly issued VETO. No trade taken. Cash position maintained. This is the system working as designed.",
    },

    # Feb 2026 — Strong recovery, full discipline ──────────────────────────────
    {
        "entry_date": "2026-02-02", "entry_time_str": "09:20:00",
        "exit_date": "2026-02-05",  "exit_time_str": "14:45:00",
        "expiry_date": "2026-02-06", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23900, "entry_premium": 202, "exit_premium": 48,
        "max_profit": 7700, "max_loss": 22300, "allocated_capital": 120000,
        "theta_pnl": 6500, "vega_pnl": -1200, "gamma_pnl": -380,
        "exit_reason": "PROFIT_TARGET",
        "vix": 13.1, "ivp": 73, "regime_score": 7.3,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.51, "weighted_vrp": 3.7,
        "score_drivers": ["Post-FOMC vol crush benefit", "IVP 73 — rich", "VoV settled 0.51σ", "CLEAR tone — best entry signal"],
        "pretrade_rationale": "Post-FOMC vol normalised. Market expecting Fed hold. Premium rich at 73 IVP. VoV 0.51σ. CLEAR morning tone. Full IC.",
    },
    {
        "entry_date": "2026-02-09", "entry_time_str": "09:22:00",
        "exit_date": "2026-02-12",  "exit_time_str": "14:50:00",
        "expiry_date": "2026-02-12", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24100, "entry_premium": 214, "exit_premium": 51,
        "max_profit": 8150, "max_loss": 21850, "allocated_capital": 120000,
        "theta_pnl": 7100, "vega_pnl": -920, "gamma_pnl": -260,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.6, "ivp": 76, "regime_score": 7.7,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.42, "weighted_vrp": 4.1,
        "score_drivers": ["IVP 76 — highest in 3 weeks", "VoV 0.42σ — near Oct levels", "FII net long 5 days", "Budget rally premium"],
        "pretrade_rationale": "IVP 76 — back to October levels. Budget expectations inflating premium. VoV at cycle low. Best risk-adjusted setup in months.",
    },
    {
        "entry_date": "2026-02-16", "entry_time_str": "09:18:00",
        "exit_date": "2026-02-19",  "exit_time_str": "14:55:00",
        "expiry_date": "2026-02-19", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24300, "entry_premium": 222, "exit_premium": 53,
        "max_profit": 8450, "max_loss": 21550, "allocated_capital": 120000,
        "theta_pnl": 7600, "vega_pnl": -1050, "gamma_pnl": -310,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.2, "ivp": 78, "regime_score": 7.8,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.36, "weighted_vrp": 4.4,
        "score_drivers": ["IVP 78 — 4-month high", "VoV 0.36σ — extremely stable", "Budget day next week but CLEAR now", "FII buying aggressively"],
        "pretrade_rationale": "Exceptional regime. IVP 78 highest since October. VoV record low for this period. FII buying. Budget week ahead but current conditions warrant full position.",
    },
    {
        "entry_date": "2026-02-23", "entry_time_str": "09:20:00",
        "exit_date": "2026-02-24",  "exit_time_str": "14:30:00",
        "expiry_date": "2026-02-26", "expiry_type": "WEEKLY",
        "strategy_type": "BULL_PUT_SPREAD",
        "spot": 24100, "entry_premium": 72, "exit_premium": 18,
        "max_profit": 2700, "max_loss": 7300, "allocated_capital": 50000,
        "theta_pnl": 2500, "vega_pnl": -380, "gamma_pnl": -120,
        "exit_reason": "PROFIT_TARGET",
        "vix": 13.0, "ivp": 72, "regime_score": 7.1,
        "vol_regime": "RICH", "morning_tone": "CAUTIOUS_NEUTRAL",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.68, "weighted_vrp": 3.3,
        "score_drivers": ["Post-budget caution", "VoV uptick — 0.68σ", "Budget outcome mixed", "Reduced sizing — correct"],
        "pretrade_rationale": "Post-budget week. Mixed reaction. VoV ticking up slightly at 0.68σ. Prudent to use Bull Put Spread with reduced capital. Market digesting budget.",
    },

    # ─── Mar 2026 — VoV spike, disciplined cash ───────────────────────────────
    {
        "entry_date": "2026-03-02", "entry_time_str": "09:22:00",
        "exit_date": "2026-03-05",  "exit_time_str": "14:50:00",
        "expiry_date": "2026-03-06", "expiry_type": "WEEKLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23800, "entry_premium": 185, "exit_premium": 44,
        "max_profit": 7050, "max_loss": 22950, "allocated_capital": 120000,
        "theta_pnl": 6100, "vega_pnl": -1300, "gamma_pnl": -360,
        "exit_reason": "PROFIT_TARGET",
        "vix": 13.5, "ivp": 71, "regime_score": 7.0,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.62, "weighted_vrp": 3.5,
        "score_drivers": ["IVP 71 — RICH", "VoV 0.62σ — acceptable", "FOMC Mar 18 — time buffer", "FII neutral"],
        "pretrade_rationale": "Good week to trade. FOMC is Mar 18 — two weeks away. VoV 0.62σ stable. IVP 71. Standard IC sizing.",
    },
    # ── The current week — VoV blocked (current state the app shows) ─────────
    # NOTE: This week VoV hit 3.42σ — correctly stayed in cash (see screenshots)
]

# ── Monthly trades ────────────────────────────────────────────────────────────
MONTHLY_TRADES = [
    {
        "entry_date": "2025-10-01", "entry_time_str": "09:25:00",
        "exit_date": "2025-10-30",  "exit_time_str": "14:55:00",
        "expiry_date": "2025-10-30", "expiry_type": "MONTHLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23700, "entry_premium": 380, "exit_premium": 78,
        "max_profit": 15100, "max_loss": 34900, "allocated_capital": 200000,
        "theta_pnl": 18200, "vega_pnl": -2800, "gamma_pnl": -1100,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.8, "ivp": 81, "regime_score": 7.9,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.41, "weighted_vrp": 4.3,
        "score_drivers": ["Monthly IC — DTE 30", "IVP 81 — premium very rich", "VoV 0.41σ", "FII net long month", "No macro events Oct"],
        "pretrade_rationale": "Monthly IC entry at start of Oct series. IVP 81 — excellent premium. Full monthly allocation. 30 DTE gives good theta decay curve. No significant macro risks until November.",
    },
    {
        "entry_date": "2025-11-03", "entry_time_str": "09:18:00",
        "exit_date": "2025-11-19",  "exit_time_str": "10:45:00",
        "expiry_date": "2025-11-27", "expiry_type": "MONTHLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23900, "entry_premium": 340, "exit_premium": 820,
        "max_profit": 12000, "max_loss": 38000, "allocated_capital": 180000,
        "theta_pnl": 8200, "vega_pnl": -34000, "gamma_pnl": -5800,
        "exit_reason": "STOP_LOSS",
        "vix": 16.2, "ivp": 70, "regime_score": 5.8,
        "vol_regime": "FAIR", "morning_tone": "CAUTIOUS",
        "pretrade_verdict": "PROCEED_WITH_CAUTION",
        "vov_zscore": 1.75, "weighted_vrp": 2.3,
        "score_drivers": ["RBI Nov policy — veto candidate", "VoV 1.75σ at entry — warning", "Nov historically volatile", "Should have waited post-RBI"],
        "pretrade_rationale": "Monthly IC entered before RBI Nov policy. VoV 1.75σ was a warning that was underweighted. RBI surprise triggered massive IV expansion. This was the worst loss of the period. Key learning: monthly IC + upcoming RBI = NO.",
    },
    {
        "entry_date": "2025-12-01", "entry_time_str": "09:20:00",
        "exit_date": "2025-12-25",  "exit_time_str": "14:55:00",
        "expiry_date": "2025-12-25", "expiry_type": "MONTHLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23800, "entry_premium": 310, "exit_premium": 68,
        "max_profit": 12100, "max_loss": 37900, "allocated_capital": 180000,
        "theta_pnl": 16800, "vega_pnl": -3200, "gamma_pnl": -1400,
        "exit_reason": "PROFIT_TARGET",
        "vix": 13.6, "ivp": 68, "regime_score": 6.9,
        "vol_regime": "FAIR", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.84, "weighted_vrp": 3.0,
        "score_drivers": ["Recovery month post Nov drawdown", "IVP 68 — acceptable", "VoV 0.84σ normalised", "Year end low volatility expected"],
        "pretrade_rationale": "Post-Nov recovery month. Entered conservatively — reduced width vs normal. VoV 0.84σ well within bounds. Year-end drift expected. Targeting 65% of max profit.",
    },
    {
        "entry_date": "2026-01-05", "entry_time_str": "09:18:00",
        "exit_date": "2026-01-29",  "exit_time_str": "09:00:00",
        "expiry_date": "2026-01-29", "expiry_type": "MONTHLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 24050, "entry_premium": 355, "exit_premium": 72,
        "max_profit": 14150, "max_loss": 35850, "allocated_capital": 200000,
        "theta_pnl": 19200, "vega_pnl": -3800, "gamma_pnl": -1250,
        "exit_reason": "FOMC_SQUARE_OFF_PRE_EVENT",
        "vix": 12.7, "ivp": 73, "regime_score": 7.3,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.52, "weighted_vrp": 3.8,
        "score_drivers": ["Jan FOMC — squared off at 80% profit", "IVP 73 premium rich at entry", "Correct pre-event exit", "VoV rose from 0.52 to 1.55σ by FOMC"],
        "pretrade_rationale": "Monthly IC for January series. Excellent entry with IVP 73 and VoV 0.52σ. Squared off ahead of FOMC Jan 29 as per system rule — locked in 80% of max profit rather than risk FOMC surprise. Perfect execution.",
    },
    {
        "entry_date": "2026-02-02", "entry_time_str": "09:22:00",
        "exit_date": "2026-02-26",  "exit_time_str": "14:50:00",
        "expiry_date": "2026-02-26", "expiry_type": "MONTHLY",
        "strategy_type": "IRON_CONDOR",
        "spot": 23950, "entry_premium": 368, "exit_premium": 74,
        "max_profit": 14700, "max_loss": 35300, "allocated_capital": 200000,
        "theta_pnl": 20100, "vega_pnl": -3100, "gamma_pnl": -1050,
        "exit_reason": "PROFIT_TARGET",
        "vix": 12.4, "ivp": 75, "regime_score": 7.6,
        "vol_regime": "RICH", "morning_tone": "CLEAR",
        "pretrade_verdict": "PROCEED",
        "vov_zscore": 0.45, "weighted_vrp": 4.0,
        "score_drivers": ["Best monthly setup since Oct", "IVP 75 + VoV 0.45σ", "CLEAR tone all month", "FII net long entire Feb"],
        "pretrade_rationale": "February series — best setup since October. IVP 75, VoV 0.45σ, CLEAR morning tone. FII net long supporting. Full allocation with confidence.",
    },
]

# ── DB Writer ─────────────────────────────────────────────────────────────────

def insert_trade(conn: sqlite3.Connection, t: dict):
    strategy_id = make_id()
    legs_fn = {"IRON_CONDOR": ic_legs, "STRADDLE": straddle_legs, "BULL_PUT_SPREAD": bull_put_legs}
    legs = legs_fn.get(t["strategy_type"], ic_legs)(t["spot"])
    greeks = greeks_snapshot(legs, iv=0.14 + (t["vix"] - 12) * 0.008)

    entry_dt   = dt(t["entry_date"], t["entry_time_str"])
    exit_dt    = dt(t["exit_date"],  t["exit_time_str"])
    expiry_dt_  = expiry_dt(t["expiry_date"])
    pnl        = (t["entry_premium"] - t["exit_premium"]) * 50 * (2 if t["strategy_type"] == "STRADDLE" else (4 if t["strategy_type"] == "IRON_CONDOR" else 2))

    outcome_class = classify_trade_outcome(
        realized_pnl=pnl,
        theta_pnl=t.get("theta_pnl", 0),
        vega_pnl=t.get("vega_pnl", 0),
        vov_zscore=t.get("vov_zscore", 0),
        regime_score=t.get("regime_score", 5),
        morning_tone=t.get("morning_tone", ""),
        pretrade_verdict=t.get("pretrade_verdict", ""),
    )

    conn.execute("""
        INSERT OR IGNORE INTO trades (
            strategy_id, strategy_type, expiry_type, expiry_date,
            entry_time, exit_time, legs_data, order_ids,
            entry_greeks_snapshot,
            max_profit, max_loss, allocated_capital, required_margin,
            entry_premium, exit_premium, realized_pnl,
            theta_pnl, vega_pnl, gamma_pnl,
            status, exit_reason, is_mock,
            regime_score_at_entry, vix_at_entry, ivp_at_entry,
            vol_regime_at_entry, morning_tone_at_entry,
            pretrade_verdict_at_entry, vov_zscore_at_entry,
            weighted_vrp_at_entry, score_drivers_at_entry,
            pretrade_rationale, trade_outcome_class,
            created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        strategy_id, t["strategy_type"], t["expiry_type"], expiry_dt_,
        entry_dt, exit_dt, json.dumps(legs), json.dumps([]),
        json.dumps(greeks),
        t["max_profit"], t["max_loss"], t["allocated_capital"], t["allocated_capital"] * 0.15,
        t["entry_premium"], t["exit_premium"], pnl,
        t["theta_pnl"], t["vega_pnl"], t["gamma_pnl"],
        "COMPLETED", t["exit_reason"], 1,
        t["regime_score"], t["vix"], t["ivp"],
        t["vol_regime"], t["morning_tone"],
        t["pretrade_verdict"], t["vov_zscore"],
        t["weighted_vrp"], json.dumps(t["score_drivers"]),
        t["pretrade_rationale"], outcome_class,
        datetime.now(), datetime.now()
    ))

def insert_daily_stats(conn: sqlite3.Connection, trades: list):
    """Roll up trades into daily_stats."""
    daily: dict = {}
    for t in trades:
        date_key = t["exit_date"]
        if date_key not in daily:
            daily[date_key] = {"pnl": 0, "theta": 0, "vega": 0, "count": 0, "wins": 0, "losses": 0}
        legs_mult = 4 if t["strategy_type"] == "IRON_CONDOR" else 2
        pnl = (t["entry_premium"] - t["exit_premium"]) * 50 * legs_mult
        daily[date_key]["pnl"]    += pnl
        daily[date_key]["theta"]  += t["theta_pnl"]
        daily[date_key]["vega"]   += t["vega_pnl"]
        daily[date_key]["count"]  += 1
        if pnl > 0:
            daily[date_key]["wins"]   += 1
        else:
            daily[date_key]["losses"] += 1

    for date_str, d in daily.items():
        date_val = datetime.strptime(date_str, "%Y-%m-%d")
        conn.execute("""
            INSERT OR IGNORE INTO daily_stats
              (date, total_pnl, realized_pnl, trades_count, wins, losses, theta_pnl, vega_pnl, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (date_val, d["pnl"], d["pnl"], d["count"], d["wins"], d["losses"],
              d["theta"], d["vega"], datetime.now(), datetime.now()))

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Seed VolGuard journal with demo trades")
    parser.add_argument("--db", default="./volguard.db", help="Path to volguard.db")
    parser.add_argument("--clear", action="store_true", help="Delete existing seeded trades first")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"✗ Database not found at {db_path}. Start the backend once to create it, then run this script.")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA journal_mode=WAL")

    if args.clear:
        deleted = conn.execute("DELETE FROM trades WHERE strategy_id LIKE 'SEED-%'").rowcount
        conn.execute("DELETE FROM daily_stats")
        print(f"  Cleared {deleted} existing seeded trades.")

    all_trades = TRADES + MONTHLY_TRADES
    inserted = 0
    for t in all_trades:
        try:
            insert_trade(conn, t)
            inserted += 1
        except Exception as e:
            print(f"  ✗ Failed to insert {t.get('entry_date','?')} {t.get('strategy_type','?')}: {e}")

    insert_daily_stats(conn, all_trades)
    conn.commit()
    conn.close()

    print(f"\n✓ Seeded {inserted}/{len(all_trades)} trades into {db_path}")
    print(f"\nJournal Coach will now be able to answer questions like:")
    print("  → 'Why are my losing trades happening?'")
    print("  → 'What does my theta vs vega attribution tell me?'")
    print("  → 'Am I trading on days I shouldn't?' (Nov RISK_OFF pattern)")
    print("  → 'When does my edge actually work?' (IVP + VoV combination)")
    print("  → 'What are my worst repeating mistakes?' (VoV override pattern)")
    print("  → 'Analyze my overall risk management'")
    print(f"\nStats preview:")
    wins   = [t for t in all_trades if t["exit_reason"] == "PROFIT_TARGET"]
    losses = [t for t in all_trades if t["exit_reason"] == "STOP_LOSS"]
    print(f"  Wins: {len(wins)}   Losses: {len(losses)}   Win rate: {len(wins)/len(all_trades)*100:.1f}%")
    print(f"  IC trades: {sum(1 for t in all_trades if t['strategy_type'] == 'IRON_CONDOR')}")
    print(f"  RISK_OFF trades taken: {sum(1 for t in all_trades if t['morning_tone'] == 'RISK_OFF')} (both losses)")
    print(f"  VETO overrides: {sum(1 for t in all_trades if t['pretrade_verdict'] == 'VETO')} (both losses)")


if __name__ == "__main__":
    main()
