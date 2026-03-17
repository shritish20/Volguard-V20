import { useState } from 'react'
import { InfoPopover } from './InfoPopover'
import type { ProfessionalDashboard, EdgeMetrics } from '@/lib/types'

const M = {
  atm_iv: {
    title: 'ATM IV',
    what: "At-the-Money Implied Volatility — the market's consensus forecast of future vol for this expiry, extracted from the live option chain.",
    how: 'Average of CE IV and PE IV of the strike closest to current NIFTY spot in the option chain for this expiry.',
    window: 'Live — refreshed every analytics cycle from the Upstox option chain.',
  },
  vrp: {
    title: 'Volatility Risk Premium (VRP)',
    what: 'The edge you earn as a premium seller. VRP = ATM IV minus Realised Vol. Positive = options are expensive relative to actual vol → edge exists. Negative = options are cheap.',
    how: 'Three VRP measures: vs RV (simple historical), vs GARCH (model forecast), vs Parkinson (high-low). Weighted VRP = 70% GARCH + 15% Parkinson + 15% RV.',
    window: 'Weekly: IV vs RV7 / GARCH7 / Park7 · Monthly: IV vs RV28 / GARCH28 / Park28.',
    context: 'RICH = Weighted VRP > 0, options expensive → premium selling edge. CHEAP = VRP ≤ 0 → edge is thin or negative.',
  },
  term_structure: {
    title: 'Term Structure',
    what: 'Whether near-term or longer-term options are pricing more uncertainty per day. True backwardation = market fears near-term risk more than long-term.',
    how: 'DTE-adjusted: each expiry converted to daily implied variance = (IV² × DTE) / 365, then compared. Raw IV comparison is misleading near expiry — a weekly at 6 DTE almost always shows lower raw IV purely due to mean reversion mechanics, not regime. Display value is raw IV difference (iv_monthly − iv_weekly) for readability.',
    window: 'Weekly vs Monthly expiry, using live ATM IV and current DTE for each.',
    context: 'BACKWARDATION = near-term daily var > long-term · CONTANGO = normal upward slope · FLAT = near-equal.',
  },
}

const EXPIRY_TABS = [
  { key: 'weekly',      label: 'WEEKLY' },
  { key: 'next_weekly', label: 'NEXT WEEKLY' },
  { key: 'monthly',     label: 'MONTHLY' },
] as const

function EdgePanel({ data }: { data: EdgeMetrics }) {
  if (!data) return null
  const vrpNum   = parseFloat(data.weighted_vrp)
  const vrpColor = vrpNum > 0 ? 'text-neon-green' : 'text-signal-red'
  const tagColor: Record<string, string> = {
    RICH: 'text-neon-green', CHEAP: 'text-signal-red', FAIR: 'text-yellow-400',
  }

  const rows: { label: string; value: string; color?: string; info?: keyof typeof M }[] = [
    { label: 'ATM IV',           value: `${data.atm_iv}%`,        info: 'atm_iv' },
    { label: 'VRP vs RV',        value: data.vrp_vs_rv,           color: parseFloat(data.vrp_vs_rv) > 0 ? 'text-neon-green' : 'text-signal-red', info: 'vrp' },
    { label: 'VRP vs GARCH',     value: data.vrp_vs_garch,        color: parseFloat(data.vrp_vs_garch) > 0 ? 'text-neon-green' : 'text-signal-red' },
    { label: 'VRP vs Parkinson', value: data.vrp_vs_parkinson,    color: parseFloat(data.vrp_vs_parkinson) > 0 ? 'text-neon-green' : 'text-signal-red' },
    { label: 'Weighted VRP',     value: data.weighted_vrp,        color: vrpColor },
    { label: 'Tag',              value: data.weighted_vrp_tag,    color: tagColor[data.weighted_vrp_tag] ?? 'text-foreground' },
  ]

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 gap-3 pt-4">
      {rows.map(r => (
        <div key={r.label} className="glass-card p-3">
          <p className="text-[10px] text-header mb-1 flex items-center gap-0.5">
            {r.label}
            {r.info && <InfoPopover entry={M[r.info]} />}
          </p>
          <p className={`font-mono-data text-lg font-bold ${r.color ?? 'text-foreground'}`}>{r.value ?? '—'}</p>
        </div>
      ))}
    </div>
  )
}

export function OptionEdges({
  data,
  isUsingFallback = false,
}: {
  data: ProfessionalDashboard['option_edges']
  isUsingFallback?: boolean
}) {
  const [active, setActive] = useState<'weekly' | 'next_weekly' | 'monthly'>('weekly')

  return (
    <section>
      <h2 className="text-header text-xs mb-3">OPTION EDGES — VRP ANALYSIS</h2>
      <div className="glass-card p-4">
        <div className="flex items-center justify-between border-b border-white/10 pb-3">
          <div className="flex gap-1">
            {EXPIRY_TABS.map(t => (
              <button
                key={t.key}
                onClick={() => setActive(t.key)}
                className={`text-[10px] px-3 py-1.5 rounded font-semibold uppercase tracking-wider transition-all ${
                  active === t.key
                    ? 'bg-electric-blue/20 text-electric-blue border border-electric-blue/40'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {t.label}
              </button>
            ))}
          </div>

          {/* Term spread — uses raw IV diff for display, label from DTE-adjusted regime */}
          <div className="text-[10px] text-muted-foreground font-mono-data flex items-center gap-1">
            Term Spread:
            <span className="text-electric-blue font-bold">{data?.term_spread_pct ?? '—'}</span>
            <InfoPopover entry={M.term_structure} />
            <span className="ml-3">
              Regime: <span className={`font-bold ${
                data?.primary_edge === 'BACKWARDATION' ? 'text-signal-red' :
                data?.primary_edge === 'CONTANGO'      ? 'text-neon-green' : 'text-yellow-400'
              }`}>{data?.primary_edge ?? '—'}</span>
            </span>
          </div>
        </div>

        <EdgePanel data={data?.[active]} />
      </div>
    </section>
  )
}
