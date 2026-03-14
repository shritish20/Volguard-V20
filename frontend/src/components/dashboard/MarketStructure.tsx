import { useState } from 'react'
import type { ProfessionalDashboard, StructureMetrics } from '@/lib/types'

const EXPIRY_TABS = [
  { key: 'weekly', label: 'WEEKLY' },
  { key: 'next_weekly', label: 'NEXT WEEKLY' },
  { key: 'monthly', label: 'MONTHLY' },
] as const

function MetricItem({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div className="flex justify-between items-center py-2 border-b border-white/5 last:border-0">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className={`font-mono-data text-sm font-bold ${color ?? 'text-foreground'}`}>{value}</span>
    </div>
  )
}

function StructurePanel({ data }: { data: StructureMetrics }) {
  if (!data) return <p className="text-muted-foreground text-sm text-center py-8">No data available</p>
  const gexColor = (data.net_gex_formatted ?? '').startsWith('-') ? 'text-signal-red' : 'text-neon-green'
  const regimeColor: Record<string, string> = { SLIPPERY: 'text-signal-red', STICKY: 'text-neon-green', VERY_STICKY: 'text-electric-blue', NEUTRAL: 'text-muted-foreground' }
  const skewColor: Record<string, string> = { CRASH_FEAR: 'text-signal-red', MELT_UP: 'text-neon-green', ELEVATED: 'text-orange-400', NORMAL: 'text-foreground' }
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 pt-4">
      <div className="glass-card p-4">
        <p className="text-[10px] text-header mb-2">GEX ANALYSIS</p>
        <MetricItem label="Net GEX" value={data.net_gex_formatted ?? 'N/A'} color={gexColor} />
        <MetricItem label="Weighted GEX" value={data.weighted_gex_formatted ?? 'N/A'} color={gexColor} />
        <MetricItem label="GEX Ratio" value={data.gex_ratio_pct ?? 'N/A'} />
        <MetricItem label="GEX Regime" value={data.gex_regime ?? 'N/A'} color={regimeColor[data.gex_regime] ?? 'text-foreground'} />
      </div>
      <div className="glass-card p-4">
        <p className="text-[10px] text-header mb-2">FLOW & POSITIONING</p>
        <MetricItem label="PCR (All)" value={data.pcr_all?.toFixed(2) ?? 'N/A'} color={data.pcr_all > 1.2 ? 'text-neon-green' : data.pcr_all < 0.8 ? 'text-signal-red' : 'text-foreground'} />
        <MetricItem label="PCR (ATM)" value={data.pcr_atm?.toFixed(2) ?? 'N/A'} />
        <MetricItem label="Max Pain" value={data.max_pain?.toLocaleString('en-IN') ?? 'N/A'} color="text-electric-blue" />
      </div>
      <div className="glass-card p-4">
        <p className="text-[10px] text-header mb-2">SKEW</p>
        <MetricItem label="Skew 25D" value={data.skew_25d ?? 'N/A'} />
        <MetricItem label="Skew Regime" value={data.skew_regime ?? 'N/A'} color={skewColor[data.skew_regime] ?? 'text-foreground'} />
      </div>
    </div>
  )
}

export function MarketStructure({ data }: { data: ProfessionalDashboard['structure_analysis'] }) {
  const [active, setActive] = useState<'weekly' | 'next_weekly' | 'monthly'>('weekly')
  return (
    <section>
      <h2 className="text-header text-xs mb-3">MARKET STRUCTURE</h2>
      <div className="glass-card p-4">
        <div className="flex gap-1 border-b border-white/10 pb-3">
          {EXPIRY_TABS.map(t => (
            <button key={t.key}
              onClick={() => setActive(t.key)}
              className={`text-[10px] px-3 py-1.5 rounded font-semibold uppercase tracking-wider transition-all ${active === t.key ? 'bg-electric-blue/20 text-electric-blue border border-electric-blue/40' : 'text-muted-foreground hover:text-foreground'}`}
            >{t.label}</button>
          ))}
        </div>
        <StructurePanel data={data[active]} />
      </div>
    </section>
  )
}