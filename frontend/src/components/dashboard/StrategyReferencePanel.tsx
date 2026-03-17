import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'

// ── Score tiers with strategy and strike logic ────────────────────────────────
const TIERS = [
  {
    range: '7.0 – 10.0',
    regime: 'AGGRESSIVE SHORT',
    regimeColor: 'text-neon-green',
    regimeBg: 'bg-neon-green/10 border-neon-green/20',
    strategy: 'PROTECTED STRADDLE',
    bias: 'Any',
    strikeLogic: 'Sell ATM Call + ATM Put  |  Buy wings at 0.02 delta each side',
    why: 'Highest score = richest vol = sell ATM for maximum premium. Wings at 0.02Δ are cheap tail protection.',
  },
  {
    range: '6.0 – 6.9',
    regime: 'AGGRESSIVE SHORT',
    regimeColor: 'text-neon-green',
    regimeBg: 'bg-neon-green/10 border-neon-green/20',
    strategy: 'IRON FLY',
    bias: 'Any',
    strikeLogic: 'Sell ATM Call + ATM Put  |  Buy wings at ± straddle premium distance',
    why: 'Strong but not peak conditions. Wings placed at exact breakeven — self-calibrating to vol regime.',
  },
  {
    range: '4.0 – 5.9',
    regime: 'DEFENSIVE',
    regimeColor: 'text-yellow-400',
    regimeBg: 'bg-yellow-400/10 border-yellow-400/20',
    strategy: 'IRON CONDOR',
    bias: 'Neutral',
    strikeLogic: 'Sell 0.20Δ Call + 0.20Δ Put  |  Buy 0.05Δ wings each side',
    why: 'Moderate conditions. Wider structure, lower premium, margin-efficient wings.',
  },
  {
    range: '4.0 – 5.9',
    regime: 'DEFENSIVE',
    regimeColor: 'text-yellow-400',
    regimeBg: 'bg-yellow-400/10 border-yellow-400/20',
    strategy: 'BULL PUT SPREAD',
    bias: 'Bullish *',
    strikeLogic: 'Sell 0.30Δ Put  |  Buy 0.10Δ Put as hedge',
    why: 'Lean bullish when 4 signals agree: PCR ATM > 1.3, FII bullish, Spot > MA20, VIX falling.',
  },
  {
    range: '4.0 – 5.9',
    regime: 'DEFENSIVE',
    regimeColor: 'text-yellow-400',
    regimeBg: 'bg-yellow-400/10 border-yellow-400/20',
    strategy: 'BEAR CALL SPREAD',
    bias: 'Bearish *',
    strikeLogic: 'Sell 0.30Δ Call  |  Buy 0.10Δ Call as hedge',
    why: 'Lean bearish when 4 signals agree: PCR ATM < 0.7, FII bearish, Spot < MA20, VIX falling.',
  },
  {
    range: '0 – 3.9',
    regime: 'CASH',
    regimeColor: 'text-signal-red',
    regimeBg: 'bg-signal-red/10 border-signal-red/20',
    strategy: 'NO TRADE',
    bias: 'Any',
    strikeLogic: '—',
    why: 'VoV too high or vol regime unfavourable. Capital stays in G-Sec earning bond yield.',
  },
]

// ── Bias confluence note ──────────────────────────────────────────────────────
const BIAS_NOTE = `* Directional spreads only fire when ALL 4 signals agree simultaneously:
  1. PCR ATM confirms the direction (>1.3 bullish / <0.7 bearish)
  2. FII institutional flow agrees
  3. Spot is above MA20 (bullish) or below MA20 (bearish)
  4. VIX momentum is FALLING — fear reducing, market stabilising
  If any signal is missing or VIX is rising → defaults to IRON CONDOR`

export function StrategyReferencePanel() {
  const [open, setOpen] = useState(false)

  return (
    <section>
      {/* Toggle header */}
      <button
        onClick={() => setOpen(v => !v)}
        className="w-full flex items-center justify-between glass-card px-4 py-3 hover:bg-white/5 transition-colors"
      >
        <div className="flex items-center gap-2">
          {open ? <ChevronDown size={14} className="text-electric-blue" /> : <ChevronRight size={14} className="text-electric-blue" />}
          <span className="text-header text-xs">STRATEGY SELECTION LOGIC — SCORE × BIAS × STRIKES</span>
        </div>
        <span className="text-[10px] text-muted-foreground">{open ? 'collapse' : 'expand for presentation'}</span>
      </button>

      {/* Expandable content */}
      {open && (
        <div className="glass-card p-5 mt-1 space-y-5 border-t-0 rounded-t-none">

          {/* Score → Strategy table */}
          <div>
            <p className="text-[10px] text-header mb-3">SCORE BAND → STRATEGY → STRIKE SELECTION</p>
            <div className="overflow-x-auto">
              <table className="w-full text-[10px] font-mono-data border-collapse">
                <thead>
                  <tr className="border-b border-white/10">
                    <th className="text-left text-header py-2 pr-4 min-w-[90px]">SCORE</th>
                    <th className="text-left text-header py-2 px-3 min-w-[110px]">REGIME</th>
                    <th className="text-left text-header py-2 px-3 min-w-[160px]">STRATEGY</th>
                    <th className="text-left text-header py-2 px-3 min-w-[80px]">BIAS</th>
                    <th className="text-left text-header py-2 pl-3">STRIKE LOGIC</th>
                  </tr>
                </thead>
                <tbody>
                  {TIERS.map((t, i) => (
                    <tr key={i} className="border-b border-white/5 hover:bg-white/5 transition-colors">
                      <td className="py-2.5 pr-4">
                        <span className="font-bold text-foreground">{t.range}</span>
                      </td>
                      <td className="py-2.5 px-3">
                        <span className={`px-2 py-0.5 rounded border text-[9px] font-bold ${t.regimeBg} ${t.regimeColor}`}>
                          {t.regime}
                        </span>
                      </td>
                      <td className="py-2.5 px-3">
                        <span className={`font-bold ${t.strategy === 'NO TRADE' ? 'text-signal-red' : 'text-electric-blue'}`}>
                          {t.strategy}
                        </span>
                      </td>
                      <td className="py-2.5 px-3 text-muted-foreground">{t.bias}</td>
                      <td className="py-2.5 pl-3 text-muted-foreground">{t.strikeLogic}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Why each strategy section */}
          <div>
            <p className="text-[10px] text-header mb-3">WHY EACH STRATEGY AT THAT SCORE</p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
              {TIERS.filter(t => t.strategy !== 'NO TRADE').map((t, i) => (
                <div key={i} className={`rounded border p-3 ${t.regimeBg}`}>
                  <p className={`text-[10px] font-bold mb-1 ${t.regimeColor}`}>{t.strategy}</p>
                  <p className="text-[9px] text-muted-foreground leading-relaxed">{t.why}</p>
                </div>
              ))}
              <div className="rounded border bg-signal-red/10 border-signal-red/20 p-3">
                <p className="text-[10px] font-bold mb-1 text-signal-red">NO TRADE (CASH)</p>
                <p className="text-[9px] text-muted-foreground leading-relaxed">
                  VoV too high or vol regime unfavourable. Capital stays in G-Sec earning bond yield.
                </p>
              </div>
            </div>
          </div>

          {/* Bias confluence note */}
          <div className="rounded border border-yellow-400/20 bg-yellow-400/5 p-3">
            <p className="text-[10px] text-header mb-2">DIRECTIONAL BIAS — 4-SIGNAL CONFLUENCE REQUIRED</p>
            <pre className="text-[9px] text-muted-foreground whitespace-pre-wrap leading-relaxed font-mono-data">
              {BIAS_NOTE}
            </pre>
          </div>

          {/* GTT note */}
          <div className="rounded border border-white/10 bg-white/5 p-3">
            <p className="text-[10px] text-header mb-1">RISK MANAGEMENT</p>
            <p className="text-[9px] text-muted-foreground leading-relaxed">
              GTT stop-loss placed immediately at entry at <span className="text-foreground font-bold">2× premium received</span> per leg.
              Wings provide margin benefit and define max loss at exchange level.
              System squares off all positions <span className="text-foreground font-bold">1 day before expiry at 14:00 IST</span>.
            </p>
          </div>

        </div>
      )}
    </section>
  )
}
