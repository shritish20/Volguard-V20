import { useState, useEffect } from 'react'
import { fetchHealth, fetchReconcile, triggerReconcile } from '@/lib/api'
import type { HealthData, ReconcileData } from '@/lib/types'

export function SystemHealthPanel() {
  const [health, setHealth] = useState<HealthData | null>(null)
  const [recon, setRecon] = useState<ReconcileData | null>(null)
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(false)

  const load = async () => {
    try {
      const [h, r] = await Promise.allSettled([fetchHealth(true), fetchReconcile()])
      if (h.status === 'fulfilled') setHealth(h.value)
      if (r.status === 'fulfilled') setRecon(r.value)
    } catch {}
    setLoading(false)
  }

  useEffect(() => { load(); const iv = setInterval(load, 15000); return () => clearInterval(iv) }, [])

  const sync = async () => {
    setSyncing(true)
    try { await triggerReconcile(); await load() } catch {}
    setSyncing(false)
  }

  if (loading) return (
    <div className="glass-card p-4 animate-pulse">
      <div className="h-4 bg-white/10 w-40 rounded mb-3" />
      <div className="h-16 bg-white/5 rounded" />
    </div>
  )

  const cbActive = health?.circuit_breaker === 'ACTIVE'
  const mismatch = recon && !recon.reconciled

  return (
    <div className="glass-card p-5 space-y-4">
      <div className="flex items-center justify-between border-b border-white/10 pb-3">
        <p className="text-[10px] text-header">SYSTEM HEALTH</p>
        <span className="flex items-center gap-1.5 text-[9px] text-muted-foreground">
          <span className="w-1.5 h-1.5 rounded-full bg-electric-blue pulse-blue" />
          Auto-sync active
        </span>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {/* Circuit breaker */}
        <div className={`p-3 rounded border ${cbActive ? 'bg-signal-red/10 border-signal-red/40' : 'bg-neon-green/5 border-neon-green/20'}`}>
          <p className="text-[9px] text-header mb-1">CIRCUIT BREAKER</p>
          <p className={`font-mono-data font-bold text-sm ${cbActive ? 'text-signal-red' : 'text-neon-green'}`}>
            {cbActive ? 'TRIPPED' : 'NORMAL'}
          </p>
        </div>

        {/* DB */}
        <div className={`p-3 rounded border ${health?.database ? 'bg-neon-green/5 border-neon-green/20' : 'bg-signal-red/10 border-signal-red/40'}`}>
          <p className="text-[9px] text-header mb-1">DATABASE</p>
          <p className={`font-mono-data font-bold text-sm ${health?.database ? 'text-neon-green' : 'text-signal-red'}`}>
            {health?.database ? 'ONLINE' : 'OFFLINE'}
          </p>
        </div>

        {/* Cache */}
        <div className={`p-3 rounded border ${health?.daily_cache === 'VALID' ? 'bg-neon-green/5 border-neon-green/20' : 'bg-yellow-500/10 border-yellow-500/30'}`}>
          <p className="text-[9px] text-header mb-1">DAILY CACHE</p>
          <p className={`font-mono-data font-bold text-sm ${health?.daily_cache === 'VALID' ? 'text-neon-green' : 'text-yellow-400'}`}>
            {health?.daily_cache ?? '—'}
          </p>
        </div>

        {/* Reconcile */}
        <div className={`p-3 rounded border ${mismatch ? 'bg-yellow-500/10 border-yellow-500/40' : 'bg-neon-green/5 border-neon-green/20'}`}>
          <div className="flex items-center justify-between mb-1">
            <p className="text-[9px] text-header">POSITION SYNC</p>
            <button onClick={sync} disabled={syncing} className="text-[8px] text-electric-blue hover:text-electric-blue/70 disabled:opacity-50">
              {syncing ? 'Syncing…' : 'Force Sync'}
            </button>
          </div>
          <p className={`font-mono-data font-bold text-xs ${mismatch ? 'text-yellow-400' : 'text-neon-green'}`}>
            {recon ? (mismatch ? `MISMATCH (${recon.discrepancies.length})` : 'RECONCILED') : 'CHECKING'}
          </p>
        </div>
      </div>

      {health && (
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3 text-[10px]">
          <div className="glass-card p-3">
            <p className="text-header mb-1">WS MARKET</p>
            <p className={`font-mono-data font-bold ${health.websocket.market_streamer === 'CONNECTED' ? 'text-neon-green' : 'text-signal-red'}`}>{health.websocket.market_streamer}</p>
          </div>
          <div className="glass-card p-3">
            <p className="text-header mb-1">WS PORTFOLIO</p>
            <p className={`font-mono-data font-bold ${health.websocket.portfolio_streamer === 'CONNECTED' ? 'text-neon-green' : 'text-signal-red'}`}>{health.websocket.portfolio_streamer}</p>
          </div>
          <div className="glass-card p-3">
            <p className="text-header mb-1">CACHE AGE</p>
            <p className="font-mono-data font-bold text-muted-foreground">{health.analytics_cache_age === 'N/A' ? 'N/A' : `${health.analytics_cache_age}m`}</p>
          </div>
        </div>
      )}
    </div>
  )
}