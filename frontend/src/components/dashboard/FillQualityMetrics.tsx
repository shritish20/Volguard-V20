import { useState, useEffect } from 'react'
import { fetchFillQuality } from '@/lib/api'
import type { FillQualityData } from '@/lib/types'

export function FillQualityMetrics() {
  const [data, setData] = useState<FillQualityData | null>(null)

  useEffect(() => {
    const load = async () => { try { setData(await fetchFillQuality()) } catch {} }
    load()
  }, [])

  if (!data) return null

  const highSlippage = (data.avg_slippage_pct ?? 0) > 1.0

  return (
    <div className="glass-card p-5 space-y-4">
      <div className="flex items-center justify-between border-b border-white/10 pb-3">
        <p className="text-[10px] text-header">EXECUTION & FILL QUALITY</p>
        <span className="text-[10px] text-muted-foreground">Total Fills: <span className="font-bold text-foreground">{data.total_fills}</span></span>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div>
          <p className="text-[9px] text-header mb-1">AVG SLIPPAGE</p>
          <p className={`font-mono-data text-xl font-bold ${highSlippage ? 'text-signal-red' : 'text-neon-green'}`}>{(data.avg_slippage_pct ?? 0).toFixed(3)}%</p>
        </div>
        <div>
          <p className="text-[9px] text-header mb-1">MAX SLIPPAGE</p>
          <p className="font-mono-data text-xl font-bold text-signal-red">{(data.max_slippage_pct ?? 0).toFixed(3)}%</p>
        </div>
        <div>
          <p className="text-[9px] text-header mb-1">AVG FILL TIME</p>
          <p className="font-mono-data text-xl font-bold text-electric-blue">{(data.avg_time_to_fill ?? 0).toFixed(1)}s</p>
        </div>
        <div>
          <p className="text-[9px] text-header mb-1">PARTIAL FILLS</p>
          <p className={`font-mono-data text-xl font-bold ${(data.partial_fills ?? 0) > 0 ? 'text-yellow-400' : 'text-neon-green'}`}>{data.partial_fills ?? 0}</p>
        </div>
      </div>
    </div>
  )
}