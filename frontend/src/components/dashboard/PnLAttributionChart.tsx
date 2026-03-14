import { useState, useEffect } from 'react'
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts'
import { fetchPnLAttribution } from '@/lib/api'
import type { PnLAttribution } from '@/lib/types'

// ─────────────────────────────────────────────────────────────────────────────
// Natural-language verdict — reads real attribution numbers, generates one
// honest sentence about what is driving P&L right now.
// Refreshes every 10 s with the attribution poll.
// ─────────────────────────────────────────────────────────────────────────────
function buildVerdict(d: PnLAttribution & { mock?: boolean }): string {
  const { theta_pnl: th, vega_pnl: va, delta_pnl: de, total_pnl: tot } = d
  const fmt = (n: number) => `₹${Math.abs(Math.round(n)).toLocaleString('en-IN')}`
  const absT = Math.abs(th), absV = Math.abs(va), absD = Math.abs(de)
  const dominant = absT >= absV && absT >= absD ? 'theta' : absV >= absD ? 'vega' : 'delta'

  if (th > 0 && va < 0 && tot > 0 && dominant === 'theta')
    return `Theta is carrying this trade — time decay earned ${fmt(th)} but IV expansion cost ${fmt(va)}, net positive. Your edge is working.`

  if (va < 0 && Math.abs(va) > th && tot < 0 && dominant === 'vega')
    return `Vega is hurting you — IV expanded and cost ${fmt(va)}, wiping out ${fmt(th)} of theta earned. This is a vol event, not a theta day — monitor your short strikes.`

  if (th > 0 && va > 0 && tot > 0)
    return `Both theta and vega are working for you — IV compressed while time decayed. ${fmt(tot)} total gain. Clean day for an IC seller.`

  if (dominant === 'delta' && absD > 50)
    return de < 0
      ? `Spot movement is the biggest drag today (${fmt(de)} loss). Your IC has drifted directionally — check whether delta is outside your tolerance.`
      : `Spot movement added ${fmt(de)} today. The IC has a directional tilt right now — reassess if spot continues moving.`

  if (th < 0 && va < 0 && tot < 0)
    return `Theta earned ${fmt(th)} but vega and delta together cost ${fmt(absV + absD)} — IV spike and spot move both hit you. Evaluate whether to hold or exit.`

  if (th > 0 && tot > 0 && absT >= absV)
    return `Theta is the primary earner today — ${fmt(th)} in time decay with minimal interference from vol or spot. Textbook theta-seller day.`

  return `Early in the session — theta has earned ${fmt(th)} so far. Watching IV for any expansion pressure on the short legs.`
}

export function PnLAttributionChart() {
  const [data, setData] = useState<(PnLAttribution & { mock?: boolean }) | null>(null)

  useEffect(() => {
    const load = async () => {
      try { setData(await fetchPnLAttribution() as PnLAttribution & { mock?: boolean }) } catch {}
    }
    load()
    const iv = setInterval(load, 10000)
    return () => clearInterval(iv)
  }, [])

  if (!data) return null

  const items = [
    { name: 'Theta (Time)',  raw: data.theta_pnl, color: '#22c55e' },
    { name: 'Vega (Vol)',    raw: data.vega_pnl,  color: '#3b82f6' },
    { name: 'Delta (Price)', raw: data.delta_pnl, color: '#eab308' },
    { name: 'Other',         raw: data.other_pnl, color: '#64748b' },
  ].filter(x => Math.abs(x.raw) > 0)

  const chartData  = items.map(x => ({ ...x, value: Math.abs(x.raw) }))
  const verdict    = buildVerdict(data)
  const isMock     = !!data.mock
  const verdictBg  = data.total_pnl >= 0 ? 'bg-neon-green/5 border-neon-green/20' : 'bg-signal-red/5 border-signal-red/20'
  const verdictTxt = data.total_pnl >= 0 ? 'text-neon-green/90' : 'text-signal-red/90'

  return (
    <div className="glass-card p-4 flex flex-col gap-3">

      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <p className="text-[10px] text-header">P&L ATTRIBUTION</p>
          {isMock && (
            <span className="text-[9px] text-yellow-400 bg-yellow-400/10 border border-yellow-400/20 px-1.5 py-0.5 rounded">MOCK</span>
          )}
        </div>
        <span className="text-[10px] text-muted-foreground font-mono-data">
          IV Δ: <span className={data.iv_change >= 0 ? 'text-signal-red' : 'text-neon-green'}>
            {data.iv_change >= 0 ? '+' : ''}{data.iv_change.toFixed(2)}%
          </span>
        </span>
      </div>

      {/* Chart + legend */}
      <div className="flex flex-col md:flex-row items-center gap-4">
        <div className="w-full md:w-44 h-44">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie data={chartData} cx="50%" cy="50%" innerRadius={45} outerRadius={65} paddingAngle={2} dataKey="value" stroke="none">
                {chartData.map((e, i) => <Cell key={i} fill={e.color} />)}
              </Pie>
              <Tooltip
                contentStyle={{ backgroundColor: '#000', borderColor: '#333', fontSize: '11px' }}
                formatter={(v: number, n: string, p: { payload: { raw: number } }) => [`₹${p.payload.raw.toFixed(2)}`, n]}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>

        <div className="flex-1 space-y-2.5">
          {items.map((item, i) => (
            <div key={i} className="flex items-center justify-between text-xs">
              <div className="flex items-center gap-2">
                <span className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: item.color }} />
                <span className="text-muted-foreground">{item.name}</span>
              </div>
              <span className={`font-mono-data font-bold ${item.raw >= 0 ? 'text-neon-green' : 'text-signal-red'}`}>
                {item.raw >= 0 ? '+' : ''}₹{item.raw.toFixed(2)}
              </span>
            </div>
          ))}
          <div className="pt-2 border-t border-white/10 flex items-center justify-between text-xs font-bold">
            <span>Total P&L</span>
            <span className={`font-mono-data ${data.total_pnl >= 0 ? 'text-neon-green' : 'text-signal-red'}`}>
              {data.total_pnl >= 0 ? '+' : ''}₹{data.total_pnl.toFixed(2)}
            </span>
          </div>
        </div>
      </div>

      {/* Natural language verdict */}
      <div className={`rounded-md border px-3 py-2 ${verdictBg}`}>
        <p className={`text-[11px] leading-relaxed ${verdictTxt}`}>{verdict}</p>
      </div>

      {/* Journal Coach nudge */}
      <p className="text-[9px] text-muted-foreground/60 text-center pt-0.5">
        For deeper pattern analysis across all your trades, ask the{' '}
        <span className="text-electric-blue/70">Journal Coach</span>.
      </p>

    </div>
  )
}