import { useEffect, useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import api from '@/lib/api'

interface CapitalStructure {
  base_capital: number
  haircut_pct: number
  available_margin: number
  gsec_yield_pct: number
  annual_yield_inr: number
  monthly_yield_inr: number
  daily_yield_inr: number
  hard_reserve_pct: number
  hard_reserve: number
  deployed_capital: number
  active_positions: number
  available_to_deploy: number
  deployment_utilization_pct: number
  idle_capital: number
  idle_yield_monthly: number
  is_mock: boolean
  note: string
}

const fmt = (n: number) =>
  n >= 100000 ? `₹${(n / 100000).toFixed(2)}L` : `₹${n.toLocaleString('en-IN')}`

const fmtPct = (n: number) => `${n.toFixed(1)}%`

export function CapitalStructurePanel() {
  const [data, setData] = useState<CapitalStructure | null>(null)
  const [loading, setLoading] = useState(true)
  const [open, setOpen] = useState(false)

  useEffect(() => {
    const load = async () => {
      try {
        const res = await api.get('/api/capital/structure')
        setData(res.data)
      } catch {
        // silently fail
      } finally {
        setLoading(false)
      }
    }
    load()
    const interval = setInterval(load, 30000)
    return () => clearInterval(interval)
  }, [])

  if (loading || !data) return null

  const deployedPct = data.deployment_utilization_pct
  const idlePct = Math.max(0, 100 - deployedPct - data.hard_reserve_pct)

  return (
    <section>
      <button
        onClick={() => setOpen(v => !v)}
        className="w-full flex items-center justify-between glass-card px-4 py-3 hover:bg-white/5 transition-colors"
      >
        <div className="flex items-center gap-2">
          {open ? <ChevronDown size={14} className="text-electric-blue" /> : <ChevronRight size={14} className="text-electric-blue" />}
          <span className="text-header text-xs">CAPITAL STRUCTURE</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[9px] text-neon-green font-mono-data">{fmtPct(data.gsec_yield_pct)} G-SEC YIELD</span>
          {data.is_mock && (
            <span className="text-[8px] text-amber-400/60 font-mono-data bg-amber-400/10 border border-amber-400/20 px-1.5 py-0.5 rounded">MOCK</span>
          )}
        </div>
      </button>

      {open && (
        <div className="glass-card p-4 mt-1 border-t-0 rounded-t-none space-y-4">

          <div className="space-y-1.5">
            <div className="flex h-3 rounded overflow-hidden gap-px">
              <div className="bg-electric-blue/80 transition-all duration-500" style={{ width: `${Math.min(deployedPct, 80)}%` }} />
              <div className="bg-neon-green/30 transition-all duration-500" style={{ width: `${Math.max(idlePct, 0)}%` }} />
              <div className="bg-signal-red/40" style={{ width: `${data.hard_reserve_pct}%` }} />
              <div className="bg-white/10" style={{ width: `${data.haircut_pct}%` }} />
            </div>
            <div className="flex items-center gap-3 text-[9px] text-muted-foreground">
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-electric-blue/80 inline-block" />Deployed {fmtPct(deployedPct)}</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-neon-green/30 inline-block" />Available {fmtPct(idlePct)}</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-signal-red/40 inline-block" />Reserve {fmtPct(data.hard_reserve_pct)}</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-white/10 inline-block" />Haircut {fmtPct(data.haircut_pct)}</span>
            </div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="space-y-0.5">
              <p className="text-[9px] text-header">TOTAL CAPITAL</p>
              <p className="font-mono-data text-sm text-foreground font-bold">{fmt(data.base_capital)}</p>
              <p className="text-[9px] text-muted-foreground">pledged as G-Sec</p>
            </div>
            <div className="space-y-0.5">
              <p className="text-[9px] text-header">AVAILABLE MARGIN</p>
              <p className="font-mono-data text-sm text-neon-green font-bold">{fmt(data.available_margin)}</p>
              <p className="text-[9px] text-muted-foreground">after {fmtPct(data.haircut_pct)} haircut</p>
            </div>
            <div className="space-y-0.5">
              <p className="text-[9px] text-header">DEPLOYED</p>
              <p className="font-mono-data text-sm text-electric-blue font-bold">{fmt(data.deployed_capital)}</p>
              <p className="text-[9px] text-muted-foreground">{data.active_positions} active position{data.active_positions !== 1 ? 's' : ''}</p>
            </div>
            <div className="space-y-0.5">
              <p className="text-[9px] text-header">HARD RESERVE</p>
              <p className="font-mono-data text-sm text-signal-red font-bold">{fmt(data.hard_reserve)}</p>
              <p className="text-[9px] text-muted-foreground">untouchable buffer</p>
            </div>
          </div>

          <div className="border border-neon-green/20 rounded-lg p-3 bg-neon-green/5 space-y-2">
            <p className="text-[9px] text-neon-green font-semibold tracking-wider">BOND YIELD — CAPITAL NEVER IDLE</p>
            <div className="grid grid-cols-3 gap-3">
              <div><p className="text-[9px] text-header">DAILY</p><p className="font-mono-data text-xs text-neon-green font-bold">+{fmt(data.daily_yield_inr)}</p></div>
              <div><p className="text-[9px] text-header">MONTHLY</p><p className="font-mono-data text-xs text-neon-green font-bold">+{fmt(data.monthly_yield_inr)}</p></div>
              <div><p className="text-[9px] text-header">ANNUAL</p><p className="font-mono-data text-xs text-neon-green font-bold">+{fmt(data.annual_yield_inr)}</p></div>
            </div>
            <p className="text-[9px] text-muted-foreground">
              Even on CASH days — G-Sec earns {fmtPct(data.gsec_yield_pct)} p.a. on full {fmt(data.base_capital)}. Idle margin earning +{fmt(data.idle_yield_monthly)}/month passively.
            </p>
          </div>

          <div className="border border-white/10 rounded-lg p-3 space-y-2">
            <p className="text-[9px] text-header">RETURN ARCHITECTURE</p>
            <div className="space-y-1.5">
              {[
                { label: 'G-Sec Yield (base layer)', value: `${fmtPct(data.gsec_yield_pct)} p.a.`, color: 'text-neon-green', bar: data.gsec_yield_pct / 20 * 100 },
                { label: 'Options Premium (target)', value: '10–12% p.a.', color: 'text-electric-blue', bar: 55 },
                { label: 'Combined Target', value: '16–18% p.a.', color: 'text-foreground', bar: 85 },
              ].map(row => (
                <div key={row.label} className="flex items-center gap-3">
                  <div className="w-32 flex-shrink-0"><p className="text-[9px] text-muted-foreground truncate">{row.label}</p></div>
                  <div className="flex-1 h-1.5 bg-white/10 rounded overflow-hidden">
                    <div className="h-full rounded transition-all duration-700" style={{ width: `${row.bar}%`, background: row.color === 'text-neon-green' ? 'rgba(34,197,94,0.7)' : row.color === 'text-electric-blue' ? 'rgba(59,130,246,0.7)' : 'rgba(255,255,255,0.5)' }} />
                  </div>
                  <p className={`font-mono-data text-xs font-bold w-20 text-right ${row.color}`}>{row.value}</p>
                </div>
              ))}
            </div>
            <p className="text-[9px] text-muted-foreground pt-1">
              System says CASH → bond yield continues. System deploys → premium adds on top. Max drawdown target: ≤15% | 5-year CAGR target: 16–20%
            </p>
          </div>

        </div>
      )}
    </section>
  )
}
