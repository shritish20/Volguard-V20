import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import type { ParticipantPositions } from '@/lib/types'

interface Props {
  data: ParticipantPositions
  isUsingFallback?: boolean
}

const CONVICTION_COLOR: Record<string, string> = {
  VERY_HIGH: 'text-signal-red font-bold',
  HIGH: 'text-orange-400 font-bold',
  MODERATE: 'text-yellow-400',
  LOW: 'text-muted-foreground',
  NEUTRAL: 'text-muted-foreground',
}

const DIR_COLOR: Record<string, string> = {
  BULLISH: 'text-neon-green',
  BEARISH: 'text-signal-red',
  NEUTRAL: 'text-muted-foreground',
}

const PARTICIPANTS = ['FII', 'DII', 'Pro', 'Client'] as const

function fmt(val: number | undefined): string {
  if (val === undefined || val === null) return '—'
  const abs = Math.abs(val)
  const sign = val >= 0 ? '+' : '-'
  if (abs >= 100000) return `${sign}${(abs / 100000).toFixed(1)}L`
  if (abs >= 1000) return `${sign}${(abs / 1000).toFixed(1)}K`
  return `${sign}${abs.toFixed(0)}`
}

function numColor(val: number | undefined): string {
  if (!val) return 'text-muted-foreground'
  return val > 0 ? 'text-neon-green' : 'text-signal-red'
}

export function FIITable({ data, isUsingFallback = false }: Props) {
  const [expanded, setExpanded] = useState(false)

  const fii = data?.fii
  if (!fii) return null

  const netColor = (fii.net_change ?? 0) >= 0 ? 'text-neon-green' : 'text-signal-red'
  const hasParticipants = data?.participants && Object.keys(data.participants).length > 0

  return (
    <section>
      <h2 className="text-header text-xs mb-3">INSTITUTIONAL FLOW — FII/DII</h2>
      <div className="glass-card p-4">

        {/* Always visible summary row */}
        <div className="flex flex-wrap items-center gap-6">
          <div>
            <p className="text-[10px] text-header mb-1">DIRECTION</p>
            <p className={`font-mono-data text-xl font-bold ${DIR_COLOR[fii.direction] ?? 'text-foreground'}`}>
              {fii.direction ?? '—'}
            </p>
          </div>
          <div>
            <p className="text-[10px] text-header mb-1">CONVICTION</p>
            <p className={`font-mono-data text-xl ${CONVICTION_COLOR[fii.conviction] ?? 'text-foreground'}`}>
              {fii.conviction ?? '—'}
            </p>
          </div>
          <div>
            <p className="text-[10px] text-header mb-1">FLOW REGIME</p>
            <p className="font-mono-data text-base text-electric-blue">{fii.flow_regime ?? '—'}</p>
          </div>
          <div>
            <p className="text-[10px] text-header mb-1">FII NET Δ</p>
            <p className={`font-mono-data text-xl font-black ${netColor}`}>
              {fii.net_change_formatted ?? '—'}
            </p>
          </div>
          <div className="ml-auto text-right">
            <p className="text-[10px] text-muted-foreground mb-1">{fii.data_date ?? '—'}</p>
            <p className="text-[9px] text-muted-foreground italic">Context only — no position impact</p>
          </div>
        </div>

        {/* Expand toggle */}
        {hasParticipants && (
          <button
            onClick={() => setExpanded(v => !v)}
            className="mt-3 flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground transition-colors"
          >
            {expanded
              ? <><ChevronDown size={12} /> Hide participant breakdown</>
              : <><ChevronRight size={12} /> Show FII / DII / Pro / Client table</>
            }
          </button>
        )}

        {/* Expandable participant table */}
        {expanded && hasParticipants && (
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-[10px] font-mono-data border-collapse">
              <thead>
                <tr className="border-b border-white/10">
                  <th className="text-left text-header py-2 pr-4">TYPE</th>
                  <th className="text-right text-header py-2 px-3">FUT LONG</th>
                  <th className="text-right text-header py-2 px-3">FUT SHORT</th>
                  <th className="text-right text-header py-2 px-3">FUT NET</th>
                  <th className="text-right text-header py-2 px-3">CALL NET</th>
                  <th className="text-right text-header py-2 px-3">PUT NET</th>
                  <th className="text-right text-header py-2 pl-3">STK NET</th>
                </tr>
              </thead>
              <tbody>
                {PARTICIPANTS.map(key => {
                  const p = data.participants?.[key]
                  if (!p) return null
                  return (
                    <tr key={key} className="border-b border-white/5 hover:bg-white/5 transition-colors">
                      <td className="py-2 pr-4 text-foreground font-bold">{key}</td>
                      <td className="py-2 px-3 text-right text-muted-foreground">{fmt(p.fut_long)}</td>
                      <td className="py-2 px-3 text-right text-muted-foreground">{fmt(p.fut_short)}</td>
                      <td className={`py-2 px-3 text-right font-bold ${numColor(p.fut_net)}`}>{fmt(p.fut_net)}</td>
                      <td className={`py-2 px-3 text-right ${numColor(p.call_net)}`}>{fmt(p.call_net)}</td>
                      <td className={`py-2 px-3 text-right ${numColor(p.put_net)}`}>{fmt(p.put_net)}</td>
                      <td className={`py-2 pl-3 text-right ${numColor(p.stock_net)}`}>{fmt(p.stock_net)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}

        {isUsingFallback && (
          <p className="text-[9px] text-yellow-500 mt-3">⚠️ Fallback data — last known FII snapshot</p>
        )}
      </div>
    </section>
  )
}
