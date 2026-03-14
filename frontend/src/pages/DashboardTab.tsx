import { useState, useEffect, lazy, Suspense } from 'react'
import { fetchDashboard } from '@/lib/api'
import { ErrorBoundary } from '@/components/ErrorBoundary'
import { TimeContext } from '@/components/dashboard/TimeContext'
import { MacroRisks } from '@/components/dashboard/MacroRisks'
import { VolatilityMatrix } from '@/components/dashboard/VolatilityMatrix'
import { FIITable } from '@/components/dashboard/FIITable'
import { MarketStructure } from '@/components/dashboard/MarketStructure'
import { OptionEdges } from '@/components/dashboard/OptionEdges'
import { StrategyEngine } from '@/components/dashboard/StrategyEngine'
import { FinalRecommendation } from '@/components/dashboard/FinalRecommendation'
import { WarningsBanner } from '@/components/dashboard/WarningsBanner'
import { CapitalStructurePanel } from '@/components/dashboard/CapitalStructurePanel'
import type { ProfessionalDashboard } from '@/lib/types'

const REFRESH_MS = 60 * 1000 // 60s — regime scores must stay fresh during trading hours

function Spinner() {
  return (
    <div className="flex flex-col items-center justify-center py-16 gap-4">
      <div className="w-10 h-10 border-3 border-electric-blue border-t-transparent rounded-full animate-spin" />
      <p className="text-muted-foreground text-sm">Loading market analytics…</p>
    </div>
  )
}

function extractWarnings(mandates: ProfessionalDashboard['mandates']) {
  const out: Array<{ type: string; message: string; severity: string }> = []
  const expiries = [
    { key: 'weekly', label: 'WEEKLY' },
    { key: 'next_weekly', label: 'NEXT WEEKLY' },
    { key: 'monthly', label: 'MONTHLY' },
  ] as const
  expiries.forEach(({ key, label }) => {
    const ws = mandates?.[key]?.warnings ?? []
    ws.forEach(w => out.push({ type: label, message: typeof w === 'string' ? w : w.message, severity: typeof w === 'string' ? 'MEDIUM' : (w.severity ?? 'LOW') }))
  })
  return out
}

export function DashboardTab() {
  const [data, setData] = useState<ProfessionalDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [fallback, setFallback] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  const load = async (force = false) => {
    force ? setRefreshing(true) : setLoading(true)
    try {
      const d = await fetchDashboard(force)
      setData(d)
      setFallback(!!(d as unknown as { _fallback?: boolean })._fallback)
      setLastUpdated(new Date())
    } catch (e) {
      console.error('Dashboard load failed', e)
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }

  useEffect(() => {
    load()
    const iv = setInterval(() => load(true), REFRESH_MS)
    return () => clearInterval(iv)
  }, [])

  if (loading && !data) return <Spinner />

  if (!data) return (
    <div className="glass-card p-8 text-center space-y-4">
      <p className="text-signal-red font-semibold">Failed to load market data</p>
      <button onClick={() => load(true)} className="px-4 py-2 bg-electric-blue text-white rounded-md text-sm">Retry</button>
    </div>
  )

  const warnings = extractWarnings(data.mandates)

  return (
    <ErrorBoundary>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <h1 className="text-sm font-bold text-foreground">Market Overview</h1>
          <div className="flex items-center gap-3">
            {fallback && <span className="text-[9px] text-yellow-400 bg-yellow-400/10 border border-yellow-400/30 px-2 py-1 rounded">FALLBACK DATA</span>}
            {lastUpdated && !fallback && <span className="text-[9px] text-muted-foreground">Updated {lastUpdated.toLocaleTimeString()}</span>}
            {refreshing && <div className="w-3 h-3 border-2 border-electric-blue border-t-transparent rounded-full animate-spin" />}
            <button onClick={() => load(true)} disabled={refreshing} className="text-[10px] bg-secondary hover:bg-secondary/80 px-3 py-1 rounded-md transition-colors disabled:opacity-50">Refresh</button>
          </div>
        </div>

        {warnings.length > 0 && (
          <ErrorBoundary><WarningsBanner warnings={warnings} isUsingFallback={fallback} /></ErrorBoundary>
        )}

        <ErrorBoundary><TimeContext data={{ ...data.time_context, timestamp: data.timestamp }} /></ErrorBoundary>
        <ErrorBoundary><MacroRisks data={data.economic_calendar} /></ErrorBoundary>
        <ErrorBoundary><VolatilityMatrix data={data.volatility_analysis} /></ErrorBoundary>
        <ErrorBoundary><FIITable data={data.participant_positions} isUsingFallback={fallback} /></ErrorBoundary>
        <ErrorBoundary><MarketStructure data={data.structure_analysis} /></ErrorBoundary>
        <ErrorBoundary><OptionEdges data={data.option_edges} isUsingFallback={fallback} /></ErrorBoundary>
        <ErrorBoundary><StrategyEngine regimes={data.regime_scores} mandates={data.mandates} /></ErrorBoundary>
        <ErrorBoundary><FinalRecommendation data={data.professional_recommendation} /></ErrorBoundary>
        <ErrorBoundary><CapitalStructurePanel /></ErrorBoundary>
      </div>
    </ErrorBoundary>
  )
}