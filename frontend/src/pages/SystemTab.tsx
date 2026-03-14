import { useState, useEffect, useRef } from 'react'
import { fetchCurrentConfig, saveConfig, fetchLogs, fetchLLMUsage } from '@/lib/api'
import api from '@/lib/api'
import { SystemHealthPanel } from '@/components/dashboard/SystemHealthPanel'
import { FillQualityMetrics } from '@/components/dashboard/FillQualityMetrics'
import { Switch } from '@/components/ui/switch'
import { Slider } from '@/components/ui/slider'
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent,
  AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import type { V5LLMUsage } from '@/lib/types'
import { toast } from 'sonner'

export function SystemTab() {
  const [maxLoss, setMaxLoss] = useState(3)
  const [profitTarget, setProfitTarget] = useState(70)
  const [autoTrading, setAutoTrading] = useState(false)
  const [pendingAutoTrading, setPendingAutoTrading] = useState<boolean | null>(null)
  const [gttSL, setGttSL] = useState(2.0)
  const [gttTP, setGttTP] = useState(0.3)
  const [gttGap, setGttGap] = useState(0.1)
  const [configLoaded, setConfigLoaded] = useState(false)
  const [saving, setSaving] = useState(false)
  const [newToken, setNewToken] = useState('')
  const [tokenSaving, setTokenSaving] = useState(false)
  const [logs, setLogs] = useState<string[]>([])
  const [logsLoading, setLogsLoading] = useState(true)
  const [llm, setLlm] = useState<V5LLMUsage | null>(null)
  const logRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const load = async () => {
      try {
        const cfg = await fetchCurrentConfig()
        setMaxLoss(cfg.MAX_LOSS_PCT ?? 3)
        setProfitTarget(cfg.PROFIT_TARGET ?? 70)
        setAutoTrading(cfg.AUTO_TRADING ?? false)
        setGttSL(cfg.GTT_STOP_LOSS_MULTIPLIER ?? 2.0)
        setGttTP(cfg.GTT_PROFIT_TARGET_MULTIPLIER ?? 0.3)
        setGttGap(cfg.GTT_TRAILING_GAP ?? 0.1)
        setConfigLoaded(true)
      } catch { setConfigLoaded(true) }
    }
    load()
  }, [])

  useEffect(() => {
    const loadLogs = async () => {
      try {
        const raw = await fetchLogs(60)
        setLogs(raw.map(l => `[${l.timestamp.slice(11, 19)}] [${l.level}] ${l.message}`))
      } catch {}
      setLogsLoading(false)
    }
    loadLogs()
    const iv = setInterval(loadLogs, 10000)
    return () => clearInterval(iv)
  }, [])

  useEffect(() => {
    const loadLLM = async () => {
      try { setLlm(await fetchLLMUsage()) } catch {}
    }
    loadLLM()
    const iv = setInterval(loadLLM, 30000)
    return () => clearInterval(iv)
  }, [])

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [logs])

  const handleSave = async () => {
    setSaving(true)
    try {
      await saveConfig({
        max_loss: maxLoss,
        profit_target: profitTarget,
        auto_trading: autoTrading,
        gtt_stop_loss_multiplier: gttSL,
        gtt_profit_target_multiplier: gttTP,
        gtt_trailing_gap: gttGap,
      })
      toast.success('Configuration saved')
    } catch { toast.error('Failed to save configuration') }
    setSaving(false)
  }

  const handleReset = () => {
    setMaxLoss(3); setProfitTarget(70); setAutoTrading(false)
    setGttSL(2.0); setGttTP(0.3); setGttGap(0.1)
  }

  const handleTokenUpdate = async () => {
    if (!newToken.trim()) { toast.error('Token cannot be empty'); return }
    setTokenSaving(true)
    try {
      await api.post('/api/system/token/update', { new_token: newToken.trim() })
      localStorage.setItem('upstox_token', newToken.trim())
      toast.success('Token updated — session and backend refreshed')
      setNewToken('')
    } catch { toast.error('Failed to update token — check backend logs') }
    setTokenSaving(false)
  }

  // Auto trading toggle: intercept the toggle, show confirmation dialog
  const handleAutoTradingToggle = (next: boolean) => {
    if (next === true) {
      // Turning ON requires confirmation
      setPendingAutoTrading(true)
    } else {
      // Turning OFF is immediate, no friction
      setAutoTrading(false)
    }
  }

  const confirmAutoTrading = () => {
    setAutoTrading(true)
    setPendingAutoTrading(null)
  }

  return (
    <div className="space-y-6">
      <SystemHealthPanel />
      <FillQualityMetrics />

      {/* Core risk */}
      <div>
        <h2 className="text-header text-xs mb-3">RISK CONTROLS</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="glass-card p-5 space-y-4">
            <p className="text-[10px] text-header">MAX DAILY LOSS</p>
            <Slider value={[maxLoss]} onValueChange={([v]) => setMaxLoss(v)} min={0.5} max={10} step={0.5} className="[&_[role=slider]]:bg-signal-red [&_[role=slider]]:border-signal-red" />
            <p className="font-mono-data text-3xl font-black text-signal-red">{maxLoss.toFixed(1)}%</p>
          </div>
          <div className="glass-card p-5 space-y-4">
            <p className="text-[10px] text-header">PROFIT TARGET</p>
            <Slider value={[profitTarget]} onValueChange={([v]) => setProfitTarget(v)} min={10} max={95} step={0.5} />
            <p className="font-mono-data text-3xl font-black text-neon-green">{profitTarget.toFixed(1)}%</p>
          </div>
        </div>
      </div>

      {/* GTT */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-header text-xs">GTT EXECUTION PARAMETERS</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {[
            { label: 'SL MULTIPLIER', sublabel: 'Exit when option rises to Nx entry', val: gttSL, set: setGttSL, min: 1, max: 5, step: 0.1, fmt: (v: number) => `${v.toFixed(1)}x`, color: 'text-signal-red' },
            { label: 'TARGET MULTIPLIER', sublabel: `Exit at ${(gttTP * 100).toFixed(0)}% of entry → captures ${((1 - gttTP) * 100).toFixed(0)}% profit`, val: gttTP, set: setGttTP, min: 0.1, max: 1, step: 0.05, fmt: (v: number) => `${v.toFixed(2)}x`, color: 'text-neon-green' },
            { label: 'TRAILING GAP', sublabel: 'Gap for trailing stop activation', val: gttGap, set: setGttGap, min: 0.05, max: 0.5, step: 0.01, fmt: (v: number) => v.toFixed(2), color: 'text-electric-blue' },
          ].map(s => (
            <div key={s.label} className="glass-card p-5 space-y-3">
              <div>
                <p className="text-[10px] text-header">{s.label}</p>
                <p className="text-[9px] text-muted-foreground mt-0.5">{s.sublabel}</p>
              </div>
              <Slider value={[s.val]} onValueChange={([v]) => s.set(v)} min={s.min} max={s.max} step={s.step} />
              <p className={`font-mono-data text-2xl font-bold ${s.color}`}>{s.fmt(s.val)}</p>
            </div>
          ))}
        </div>
        {/* GTT warning — existing orders not affected */}
        <div className="mt-3 bg-yellow-500/8 border border-yellow-500/25 rounded-lg px-4 py-2.5 flex items-start gap-2">
          <span className="text-yellow-400 text-sm mt-0.5 shrink-0">⚠</span>
          <p className="text-[10px] text-yellow-400 leading-relaxed">
            <span className="font-bold">Changes apply to new trades only.</span> GTT orders already placed at the exchange
            for existing positions are <span className="font-bold">not updated</span> when you change these values here.
            To adjust stop-loss or target on an open position, cancel the existing GTTs manually from the GTT Manager and re-enter.
          </p>
        </div>
      </div>

      {/* Auto trading toggle with confirmation */}
      <div className={`glass-card p-5 flex items-center justify-between ${autoTrading ? 'veto-glow' : ''}`}>
        <div>
          <p className="text-[10px] text-header">AUTO TRADING</p>
          <p className="text-xs text-muted-foreground mt-1">
            {autoTrading ? '🔴 ACTIVE — System is placing live orders at the exchange' : 'INACTIVE — Manual mode only, no real orders placed'}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {autoTrading && <span className="w-3 h-3 rounded-full bg-signal-red pulse-red" />}
          <Switch checked={autoTrading} onCheckedChange={handleAutoTradingToggle} />
        </div>
      </div>

      {/* Auto trading confirmation dialog */}
      <AlertDialog open={pendingAutoTrading === true} onOpenChange={(open) => { if (!open) setPendingAutoTrading(null) }}>
        <AlertDialogContent className="bg-card border-signal-red/50">
          <AlertDialogHeader>
            <AlertDialogTitle className="text-signal-red text-lg">⚠️ Enable Auto Trading?</AlertDialogTitle>
            <AlertDialogDescription className="text-muted-foreground space-y-2">
              <span className="block">This will enable <span className="text-foreground font-semibold">live order placement</span> at the exchange. The system will automatically enter and exit positions without further confirmation.</span>
              <span className="block text-yellow-400 font-semibold text-xs">Ensure your Upstox token is valid and position sizes are correct before proceeding.</span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel className="bg-secondary text-foreground hover:bg-secondary/80">
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmAutoTrading}
              className="bg-signal-red hover:bg-signal-red/80 text-white font-bold"
            >
              Enable Auto Trading
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Save */}
      <div className="flex gap-3">
        <button onClick={handleSave} disabled={saving} className="flex-1 bg-electric-blue hover:bg-electric-blue/80 text-white font-bold uppercase tracking-wider py-2.5 rounded-md transition-all disabled:opacity-50 text-sm">
          {saving ? 'Saving…' : 'Save Configuration'}
        </button>
        <button onClick={handleReset} className="px-6 border border-white/10 hover:bg-white/5 rounded-md text-sm transition-colors">Reset</button>
      </div>

      {/* LLM Usage */}
      {llm && !llm.error && (
        <div className="glass-card p-5 space-y-4">
          <div className="flex items-center justify-between border-b border-white/10 pb-3">
            <p className="text-[10px] text-header">V5 AI USAGE</p>
            <span className="text-[10px] text-electric-blue font-mono-data">{llm.provider?.toUpperCase() ?? 'UNKNOWN'} — {llm.model ?? '—'}</span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[
              { label: 'TOTAL CALLS', value: llm.total_calls ?? 0, color: 'text-foreground' },
              { label: 'INPUT TOKENS', value: (llm.total_input_tokens ?? 0).toLocaleString(), color: 'text-electric-blue' },
              { label: 'OUTPUT TOKENS', value: (llm.total_output_tokens ?? 0).toLocaleString(), color: 'text-electric-blue' },
              { label: 'EST. COST', value: `$${(llm.estimated_cost_usd ?? 0).toFixed(4)}`, color: 'text-neon-green' },
            ].map(s => (
              <div key={s.label}>
                <p className="text-[9px] text-header mb-1">{s.label}</p>
                <p className={`font-mono-data text-lg font-bold ${s.color}`}>{s.value}</p>
              </div>
            ))}
          </div>
          {llm.calls_by_agent && (
            <div className="pt-2 border-t border-white/10">
              <p className="text-[9px] text-header mb-2">CALLS BY AGENT</p>
              <div className="flex flex-wrap gap-3">
                {Object.entries(llm.calls_by_agent).map(([agent, count]) => (
                  <div key={agent} className="flex items-center gap-1.5 text-[10px]">
                    <span className="text-muted-foreground">{agent}:</span>
                    <span className="font-mono-data font-bold text-electric-blue">{count}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Token Update */}
      <div className="glass-card p-5 space-y-3 border border-yellow-500/20">
        <div className="flex items-center justify-between border-b border-white/10 pb-3">
          <p className="text-[10px] text-header">DAILY TOKEN UPDATE</p>
          <span className="text-[9px] text-yellow-400 font-mono-data">Upstox token expires 03:30 AM IST daily</span>
        </div>
        <p className="text-[10px] text-muted-foreground">After approving the morning token request on your Upstox app, paste the new token here to refresh both your session and the backend without restarting the container.</p>
        <div className="flex gap-2">
          <input
            type="password"
            value={newToken}
            onChange={e => setNewToken(e.target.value)}
            placeholder="Paste new Upstox token..."
            className="flex-1 bg-secondary border border-white/10 rounded px-3 py-2 font-mono-data text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-yellow-500"
          />
          <button
            onClick={handleTokenUpdate}
            disabled={tokenSaving || !newToken.trim()}
            className="bg-yellow-500/20 hover:bg-yellow-500/30 text-yellow-400 border border-yellow-500/40 font-bold uppercase tracking-wider px-4 py-2 rounded transition-all disabled:opacity-50 text-xs"
          >
            {tokenSaving ? 'Updating…' : 'Update Token'}
          </button>
        </div>
      </div>

      {/* Terminal */}
      <div className="glass-card p-4">
        <p className="text-[10px] text-header mb-3">SYSTEM TERMINAL</p>
        <div ref={logRef} className="bg-black rounded border border-white/5 p-3 h-64 overflow-y-auto font-mono-data">
          {logsLoading ? (
            <p className="text-neon-green text-[11px]">Loading logs…</p>
          ) : logs.length === 0 ? (
            <p className="text-muted-foreground text-[11px]">No logs available</p>
          ) : logs.map((l, i) => (
            <p key={i} className={`text-[11px] leading-relaxed ${l.includes('ERROR') || l.includes('❌') ? 'text-signal-red' : l.includes('WARNING') || l.includes('⚠') ? 'text-yellow-400' : 'text-neon-green'}`}>{l}</p>
          ))}
        </div>
      </div>
    </div>
  )
}
