import { useState, useEffect } from 'react'
import { useWebSocketContext } from '@/context/WebSocketContext'
import { fetchLivePositions } from '@/lib/api'
import { EmergencyExit } from '@/components/EmergencyExit'
import { PnLAttributionChart } from '@/components/dashboard/PnLAttributionChart'
import { GTTManager } from '@/components/dashboard/GTTManager'
import type { LiveData, ActiveStrategy } from '@/lib/types'

export function LiveDeskTab() {
  const [restData, setRestData] = useState<LiveData | null>(null)
  const { positions, portfolioMtm, portfolioGreeks, portfolioStatus, isConnected, reconnect } = useWebSocketContext()

  useEffect(() => {
    const poll = async () => {
      try { setRestData(await fetchLivePositions(true)) } catch {}
    }
    poll()
    const interval = isConnected ? 10000 : 5000
    const iv = setInterval(poll, interval)
    return () => clearInterval(iv)
  }, [isConnected])

  const liveData = restData
  const isMock = !!(liveData?.mock)

  const activePnl = isConnected && !isMock ? portfolioMtm : (restData?.mtm_pnl ?? 0)
  const activeGreeks = isConnected && !isMock
    ? (portfolioGreeks ?? { delta: 0, theta: 0, vega: 0, gamma: 0 })
    : (restData?.greeks ?? { delta: 0, theta: 0, vega: 0, gamma: 0 })

  const activeStrategies: ActiveStrategy[] = liveData?.active_strategies ?? []
  const hasPositions = activeStrategies.length > 0 || (liveData?.positions?.length ?? 0) > 0

  const pnlColor = activePnl >= 0 ? 'text-neon-green' : 'text-signal-red'
  const connecting = portfolioStatus === 'connecting'

  return (
    <div className="space-y-6">

      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-bold text-foreground tracking-tight">Live Positions</h2>
          {isMock && (
            <span className="flex items-center gap-1.5 text-[10px] font-bold text-yellow-400 bg-yellow-400/10 border border-yellow-400/30 px-2.5 py-1 rounded-full animate-pulse">
              ⚠ DEMO MODE — Mock Data
            </span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {!isMock && (
            <div className={`flex items-center gap-1.5 text-[10px] font-mono-data ${isConnected ? 'text-neon-green' : 'text-yellow-400'}`}>
              <span className={`w-1.5 h-1.5 rounded-full ${isConnected ? 'bg-neon-green pulse-green' : 'bg-yellow-400 pulse-blue'}`} />
              {isConnected ? 'LIVE FEED' : connecting ? 'CONNECTING' : 'REST FALLBACK'}
            </div>
          )}
          <EmergencyExit hasPositions={hasPositions} />
        </div>
      </div>

      {/* Demo mode explanation banner */}
      {isMock && (
        <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
          <p className="text-yellow-300 text-xs font-semibold">
            🎭 Demo Mode Active — showing a simulated NIFTY Iron Condor with realistic drifting P&L.
          </p>
          <p className="text-yellow-400/70 text-[10px] mt-1">
            Set <span className="font-mono-data bg-yellow-400/10 px-1 rounded">DEMO_MODE=false</span> in your <span className="font-mono-data">.env</span> to switch to live trading data.
          </p>
        </div>
      )}

      {!isMock && !isConnected && !connecting && (
        <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3 flex items-center justify-between">
          <p className="text-yellow-400 text-xs font-semibold">⚠ WebSocket disconnected — using REST polling (5s delay)</p>
          <button onClick={reconnect} className="text-xs bg-yellow-500/20 hover:bg-yellow-500/30 px-3 py-1 rounded transition-colors">Reconnect</button>
        </div>
      )}

      {!isMock && connecting && (
        <div className="flex items-center gap-3 p-4">
          <div className="w-5 h-5 border-2 border-electric-blue border-t-transparent rounded-full animate-spin" />
          <p className="text-muted-foreground text-sm">Connecting to live feed…</p>
        </div>
      )}

      {/* MTM + Attribution */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className={`glass-card p-6 relative flex flex-col justify-center items-center gap-2 ${isMock ? 'border border-yellow-400/20' : ''}`}>
          <div className={`absolute top-3 right-3 text-[9px] px-2 py-0.5 rounded-full border font-mono-data ${
            isMock
              ? 'bg-yellow-400/20 text-yellow-400 border-yellow-400/30'
              : isConnected
                ? 'bg-neon-green/20 text-neon-green border-neon-green/30'
                : 'bg-yellow-400/20 text-yellow-400 border-yellow-400/30'
          }`}>
            {isMock ? '⚠ MOCK' : isConnected ? '● REAL-TIME' : 'REST API'}
          </div>
          <p className="text-[10px] text-header">TOTAL MTM P&L</p>
          <p className={`font-mono-data font-black ${pnlColor}`} style={{ fontSize: 'clamp(2.5rem, 5vw, 4rem)' }}>
            {activePnl >= 0 ? '+' : ''}₹{Math.abs(activePnl).toLocaleString('en-IN')}
          </p>
          {isMock && <p className="text-[9px] text-yellow-400/60 font-mono-data">simulated · not real capital</p>}
        </div>
        <PnLAttributionChart />
      </div>

      {/* Greeks */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {(['delta', 'theta', 'vega', 'gamma'] as const).map(g => {
          const v = activeGreeks[g]
          const formatted = g === 'delta' || g === 'gamma' ? v.toFixed(3) : Math.round(v).toLocaleString('en-IN')
          return (
            <div key={g} className={`glass-card p-4 text-center ${isMock ? 'border border-yellow-400/10' : ''}`}>
              <p className="text-[9px] text-header mb-2">{g.toUpperCase()}</p>
              <p className="font-mono-data text-xl font-bold text-foreground">{formatted}</p>
            </div>
          )
        })}
      </div>

      {/* GTT */}
      <GTTManager />

      {/* Active Strategies */}
      {activeStrategies.length > 0 ? (
        <div className="space-y-3">
          <p className="text-[10px] text-header">
            ACTIVE STRATEGIES ({activeStrategies.length})
            {isMock && <span className="ml-2 text-yellow-400/70">— DEMO</span>}
          </p>
          {activeStrategies.map((strat) => {
            const pnlPositive = strat.pnl >= 0
            const profitPct = strat.max_profit > 0 ? (strat.pnl / strat.max_profit) * 100 : 0
            return (
              <div key={strat.strategy_id} className={`glass-card p-4 border ${
                strat.mock
                  ? 'border-yellow-400/25'
                  : pnlPositive ? 'border-neon-green/20' : 'border-signal-red/20'
              }`}>
                {/* Strategy header */}
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-3">
                    <span className="text-xs font-bold text-foreground">{strat.strategy_type}</span>
                    <span className="text-[9px] font-mono-data text-electric-blue bg-electric-blue/10 px-2 py-0.5 rounded">{strat.expiry_type}</span>
                    <span className="text-[9px] text-muted-foreground font-mono-data">EXP: {strat.expiry_date}</span>
                    {strat.mock && (
                      <span className="text-[9px] font-bold text-yellow-400 bg-yellow-400/10 border border-yellow-400/20 px-2 py-0.5 rounded">
                        ⚠ DEMO POSITION
                      </span>
                    )}
                  </div>
                  <div className="text-right">
                    <p className={`font-mono-data font-black text-lg ${pnlPositive ? 'text-neon-green' : 'text-signal-red'}`}>
                      {pnlPositive ? '+' : ''}₹{Math.abs(strat.pnl).toLocaleString('en-IN')}
                    </p>
                    <p className="text-[9px] text-muted-foreground font-mono-data">
                      {profitPct.toFixed(1)}% of max ₹{strat.max_profit.toLocaleString('en-IN')}
                    </p>
                  </div>
                </div>

                {/* Progress bar */}
                <div className="h-1 bg-white/5 rounded-full mb-3 overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${strat.mock ? 'bg-yellow-400' : pnlPositive ? 'bg-neon-green' : 'bg-signal-red'}`}
                    style={{ width: `${Math.min(Math.abs(profitPct), 100)}%` }}
                  />
                </div>

                {/* Legs table */}
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-white/10">
                      {['Symbol', 'Side', 'Qty', 'Entry', 'LTP', 'P&L'].map(h => (
                        <th key={h} className="py-1.5 text-header font-normal text-left text-[9px]">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {strat.legs.map((leg, i) => (
                      <tr key={i} className="border-b border-white/5">
                        <td className="py-1.5 font-mono-data text-foreground text-[10px]">{leg.symbol}</td>
                        <td className={`py-1.5 font-mono-data font-bold text-[10px] ${leg.action === 'SELL' ? 'text-signal-red' : 'text-neon-green'}`}>
                          {leg.action}
                        </td>
                        <td className="py-1.5 font-mono-data text-muted-foreground text-[10px]">{leg.qty}</td>
                        <td className="py-1.5 font-mono-data text-muted-foreground text-[10px]">₹{leg.entry_price.toFixed(2)}</td>
                        <td className="py-1.5 font-mono-data text-foreground text-[10px]">₹{leg.ltp.toFixed(2)}</td>
                        <td className={`py-1.5 font-mono-data font-bold text-[10px] ${leg.pnl >= 0 ? 'text-neon-green' : 'text-signal-red'}`}>
                          {leg.pnl >= 0 ? '+' : ''}₹{Math.abs(leg.pnl).toLocaleString('en-IN')}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                {/* Max loss / allocated */}
                <div className="flex items-center gap-4 mt-2 text-[9px] text-muted-foreground font-mono-data">
                  <span>MAX LOSS: <span className="text-signal-red">₹{strat.max_loss.toLocaleString('en-IN')}</span></span>
                  <span>ALLOCATED: <span className="text-foreground">₹{(strat.allocated_capital || 0).toLocaleString('en-IN')}</span></span>
                </div>
              </div>
            )
          })}
        </div>
      ) : (
        <div className="glass-card p-8 text-center">
          <p className="text-muted-foreground text-sm">No active positions</p>
        </div>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between text-[9px] text-muted-foreground">
        <span>Data: {isMock
          ? <span className="text-yellow-400">Demo Mode (simulated)</span>
          : isConnected
            ? <span className="text-neon-green">WebSocket (real-time)</span>
            : <span className="text-yellow-400">REST API (5s polling)</span>
        }</span>
        <span>Last update: {new Date().toLocaleTimeString()}</span>
      </div>
    </div>
  )
}
