import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import type { ProfessionalDashboard, RegimeScoreDetail, RegimeComponent, MandateDetail } from '@/lib/types'
import { RadialBarChart, RadialBar, PolarAngleAxis } from 'recharts'

// ── Score gauge radial chart ──────────────────────────────────────────────────
function ScoreGauge({ score }: { score: number }) {
  const s = score ?? 0
  const color = s >= 7 ? 'hsl(142,71%,45%)' : s >= 4 ? 'hsl(45,93%,47%)' : 'hsl(0,84%,60%)'
  return (
    <div className="relative w-16 h-16">
      <RadialBarChart width={64} height={64} cx={32} cy={32} innerRadius={20} outerRadius={30} barSize={7}
        data={[{ value: s * 10, fill: color }]} startAngle={90} endAngle={-270}>
        <PolarAngleAxis type="number" domain={[0, 100]} angleAxisId={0} tick={false} />
        <RadialBar background dataKey="value" angleAxisId={0} cornerRadius={3} />
      </RadialBarChart>
      <div className="absolute inset-0 flex items-center justify-center">
        <span className="font-mono-data text-xs font-black text-foreground">{s.toFixed(1)}</span>
      </div>
    </div>
  )
}

// ── Component score bar ───────────────────────────────────────────────────────
function Bar({ label, score, weight }: { label: string; score: number; weight?: string }) {
  const s = score ?? 0
  const color = s >= 7 ? 'bg-neon-green' : s >= 4 ? 'bg-yellow-500' : 'bg-signal-red'
  return (
    <div className="space-y-0.5">
      <div className="flex justify-between text-[10px]">
        <span className="text-muted-foreground">
          {label}{weight ? <span className="text-[9px] opacity-50 ml-1">({weight})</span> : ''}
        </span>
        <span className="font-mono-data">{s.toFixed(1)}</span>
      </div>
      <div className="h-1 bg-secondary rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full`} style={{ width: `${s * 10}%` }} />
      </div>
    </div>
  )
}

// ── Parse score driver string into color class ────────────────────────────────
// Driver formats from backend:
//   "Edge: VRP 7.9% (Excellent) +3.0"
//   "Vol: VOV Crash (3.4σ) → ZERO"
//   "Vol: VOV Danger (2.8σ) -3.5"
//   "Vol: VOV Elevated (2.6σ) -2.0"
//   "Vol: VOV Warning (2.3σ) -1.0"
//   "Struct: Slippery GEX -1.0"
function driverColor(d: string): string {
  if (d.includes('→ ZERO'))       return 'text-red-400'
  if (d.includes('VOV Danger'))   return 'text-red-400'
  if (d.includes('VOV Elevated')) return 'text-orange-400'
  if (d.includes('VOV Warning'))  return 'text-yellow-400'
  const match = d.match(/([+-]\d+\.?\d*)$/)
  if (match) {
    return parseFloat(match[1]) > 0 ? 'text-neon-green' : 'text-signal-red'
  }
  return 'text-muted-foreground'
}

function driverPrefix(d: string): string {
  if (d.startsWith('Edge:'))   return '◆'
  if (d.startsWith('Vol:'))    return '◆'
  if (d.startsWith('Struct:')) return '◆'
  return '•'
}

// ── Derive trade status from mandate + warnings ──────────────────────────────
// Three states:
//   ALLOWED  — normal green
//   WARNED   — orange  (trade not blocked but VoV in warning/elevated/danger band)
//   BLOCKED  — red     (trade blocked by veto/vol regime)
type TradeStatus = 'ALLOWED' | 'WARNED' | 'BLOCKED'

function getTradeStatus(mandate: MandateDetail): TradeStatus {
  if (mandate.trade_status === 'ALLOWED') {
    // Check if any warning has elevated/danger severity (VoV warning bands)
    const hasVovWarning = (mandate.warnings ?? []).some(
      w => w.type === 'VOL_OF_VOL' && ['MEDIUM', 'HIGH', 'DANGER'].includes(w.severity)
    )
    return hasVovWarning ? 'WARNED' : 'ALLOWED'
  }
  return 'BLOCKED'
}

const STATUS_STYLE: Record<TradeStatus, { card: string; pill: string; text: string; label: string; icon: string }> = {
  ALLOWED: {
    card:  'clear-glow',
    pill:  'bg-neon-green/10 border-neon-green/30',
    text:  'text-neon-green',
    label: 'TRADE ALLOWED',
    icon:  '✅',
  },
  WARNED: {
    card:  'border border-orange-500/40 rounded-lg',
    pill:  'bg-orange-500/10 border-orange-500/40',
    text:  'text-orange-400',
    label: 'HIGH RISK — Elevated VoV',
    icon:  '⚠️',
  },
  BLOCKED: {
    card:  'veto-glow',
    pill:  'bg-signal-red/10 border-signal-red/30',
    text:  'text-signal-red',
    label: 'TRADE BLOCKED',
    icon:  '🚫',
  },
}

// ── Per-expiry card ───────────────────────────────────────────────────────────
function ExpiryCard({ label, regime, mandate }: {
  label: string
  regime: RegimeScoreDetail
  mandate: MandateDetail
}) {
  const [driversExpanded, setDriversExpanded] = useState(false)

  if (!regime || !mandate) return null

  const tradeStatus   = getTradeStatus(mandate)
  const statusStyle   = STATUS_STYLE[tradeStatus]
  const score         = regime.composite?.score ?? 0
  const allDrivers    = regime.score_drivers ?? []
  const previewDrivers = allDrivers.slice(0, 3)
  const hasMore       = allDrivers.length > 3

  // Deduplicate warnings by message
  const uniqueWarnings = (mandate.warnings ?? []).filter(
    (w, i, arr) => arr.findIndex(x => x.message === w.message) === i
  )

  // VOV warning severity label mapping for inline warning display
  const sevLabel: Record<string, string> = {
    CRITICAL: 'text-red-400',
    DANGER:   'text-orange-400',
    HIGH:     'text-amber-400',
    MEDIUM:   'text-yellow-400',
  }

  return (
    <div className={`glass-card p-5 space-y-4 ${statusStyle.card}`}>

      {/* Header: expiry label + strategy + score gauge */}
      <div className="flex items-center justify-between">
        <div>
          <p className="text-[10px] text-header">{label}</p>
          <p className="text-sm font-bold text-foreground mt-0.5">{mandate.strategy}</p>
          {mandate.directional_bias && (
            <p className="text-[9px] text-muted-foreground mt-0.5">Bias: {mandate.directional_bias}</p>
          )}
        </div>
        <div className="flex flex-col items-center gap-1">
          <ScoreGauge score={score} />
          <p className="text-[9px] text-muted-foreground">{regime.composite?.confidence ?? '—'}</p>
        </div>
      </div>

      {/* Component score bars */}
      <div className="space-y-1.5">
        <Bar label="Volatility" score={regime.components?.volatility?.score ?? 0} weight={(regime.components?.volatility as RegimeComponent)?.weight} />
        <Bar label="Structure"  score={regime.components?.structure?.score ?? 0}  weight={(regime.components?.structure as RegimeComponent)?.weight} />
        <Bar label="Edge"       score={regime.components?.edge?.score ?? 0}       weight={(regime.components?.edge as RegimeComponent)?.weight} />
      </div>

      {/* Trade status pill — ALLOWED / WARNED / BLOCKED */}
      <div className={`rounded px-3 py-2 border ${statusStyle.pill}`}>
        <p className={`text-xs font-black uppercase tracking-wider ${statusStyle.text}`}>
          {statusStyle.icon} {statusStyle.label}
        </p>
        <p className="text-[10px] text-muted-foreground mt-0.5">{mandate.capital.deployment_formatted}</p>
      </div>

      {/* Square-off instruction (if present) */}
      {mandate.square_off_instruction && (
        <p className="text-[10px] text-yellow-400 font-semibold">⏰ {mandate.square_off_instruction}</p>
      )}

      {/* Inline warnings — colour-coded per severity */}
      {uniqueWarnings.length > 0 && (
        <div className="space-y-1">
          {uniqueWarnings.map((w, i) => {
            const textColor = sevLabel[w.severity] ?? 'text-yellow-400'
            const icon = w.severity === 'CRITICAL' ? '🚨'
                       : w.severity === 'DANGER'   ? '🔴'
                       : w.severity === 'HIGH'      ? '🟠'
                       : '⚠️'
            return (
              <p key={i} className={`text-[10px] ${textColor}`}>{icon} {w.message}</p>
            )
          })}
        </div>
      )}

      {/* Score drivers — first 3 always visible, rest expandable */}
      {allDrivers.length > 0 && (
        <div className="space-y-1 border-t border-white/5 pt-3">
          <p className="text-[9px] text-header uppercase tracking-wider mb-1">Score Drivers</p>
          {previewDrivers.map((d, i) => (
            <p key={i} className={`text-[10px] ${driverColor(d)}`}>
              {driverPrefix(d)} {d}
            </p>
          ))}

          {hasMore && (
            <>
              {driversExpanded && allDrivers.slice(3).map((d, i) => (
                <p key={i + 3} className={`text-[10px] ${driverColor(d)}`}>
                  {driverPrefix(d)} {d}
                </p>
              ))}
              <button
                onClick={() => setDriversExpanded(v => !v)}
                className="flex items-center gap-1 text-[9px] text-muted-foreground hover:text-foreground transition-colors mt-1"
              >
                {driversExpanded
                  ? <><ChevronDown size={10} /> Show less</>
                  : <><ChevronRight size={10} /> +{allDrivers.length - 3} more drivers</>
                }
              </button>
            </>
          )}
        </div>
      )}

      {/* Weight rationale */}
      {regime.weight_rationale && (
        <p className="text-[9px] text-muted-foreground/60 italic border-t border-white/5 pt-2">
          {regime.weight_rationale}
        </p>
      )}
    </div>
  )
}

// ── Main export ───────────────────────────────────────────────────────────────
export function StrategyEngine({
  regimes,
  mandates,
}: {
  regimes: ProfessionalDashboard['regime_scores']
  mandates: ProfessionalDashboard['mandates']
}) {
  return (
    <section>
      <h2 className="text-header text-xs mb-3">STRATEGY ENGINE — REGIME × MANDATE</h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <ExpiryCard label="WEEKLY"      regime={regimes?.weekly}      mandate={mandates?.weekly} />
        <ExpiryCard label="NEXT WEEKLY" regime={regimes?.next_weekly} mandate={mandates?.next_weekly} />
        <ExpiryCard label="MONTHLY"     regime={regimes?.monthly}     mandate={mandates?.monthly} />
      </div>
    </section>
  )
}
