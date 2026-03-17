import { useState, useEffect, useRef } from 'react'
import {
  fetchMorningBrief, generateBrief, fetchNews, fetchVetoLog,
  fetchAlerts, fetchMacroSnapshot, overrideVeto, triggerMonitorScan,
} from '@/lib/api'
import type {
  V5BriefResponse, V5NewsResponse, V5VetoLogResponse,
  V5AlertsResponse, V5MacroSnapshot, V5VetoRecord,
} from '@/lib/types'
import { toast } from 'sonner'

// ─── Tone helpers ────────────────────────────────────────────────────────────
const TONE_EMOJI: Record<string, string> = {
  CLEAR: '🟢', CAUTIOUS_NEUTRAL: '🟡', CAUTIOUS: '🟠',
  RISK_OFF: '🔴', MIXED: '⚪', UNKNOWN: '❓',
}
const TONE_COLOR: Record<string, string> = {
  CLEAR: 'text-neon-green', CAUTIOUS_NEUTRAL: 'text-yellow-400',
  CAUTIOUS: 'text-orange-400', RISK_OFF: 'text-signal-red',
  MIXED: 'text-muted-foreground', UNKNOWN: 'text-muted-foreground',
}
const TONE_BG: Record<string, string> = {
  CLEAR: 'bg-neon-green/10 border-neon-green/30',
  CAUTIOUS_NEUTRAL: 'bg-yellow-400/10 border-yellow-400/30',
  CAUTIOUS: 'bg-orange-400/10 border-orange-400/30',
  RISK_OFF: 'bg-signal-red/15 border-signal-red/40',
  MIXED: 'bg-secondary border-white/10',
  UNKNOWN: 'bg-secondary border-white/10',
}

function Spinner({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="glass-card p-6 flex items-center gap-3">
      <div className="w-4 h-4 border-2 border-electric-blue border-t-transparent rounded-full animate-spin shrink-0" />
      <span className="text-muted-foreground text-sm">{label}</span>
    </div>
  )
}

// ─── Section: Morning Brief ─────────────────────────────────────────────────
function MorningBrief() {
  const [brief, setBrief] = useState<V5BriefResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [generating, setGenerating] = useState(false)
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current) }, [])

  const load = async (force = false) => {
    try { setBrief(await fetchMorningBrief(force)) } catch (e) { console.error('Brief load error', e) }
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const generate = async () => {
    setGenerating(true)
    try {
      const r = await generateBrief()
      toast.info(r.message ?? 'Brief generation started — check back in ~20s')
      let attempts = 0
      pollRef.current = setInterval(async () => {
        attempts++
        try {
          const b = await fetchMorningBrief(true)
          if (b.available) { setBrief(b); clearInterval(pollRef.current!); pollRef.current = null; setGenerating(false) }
        } catch {}
        if (attempts > 15) { clearInterval(pollRef.current!); pollRef.current = null; setGenerating(false) }
      }, 5000)
    } catch {
      toast.error('Failed to start brief generation')
      setGenerating(false)
    }
  }

  const toggle = (key: string) => setExpanded(p => ({ ...p, [key]: !p[key] }))

  if (loading) return <Spinner label="Loading morning brief…" />

  const tone = brief?.data?.global_tone ?? 'UNKNOWN'

  const sections = [
    { key: 'us_session_summary', label: '🇺🇸 US Session', value: brief?.data?.us_session_summary },
    { key: 'asian_session', label: '🌏 Asia Session', value: brief?.data?.asian_session },
    { key: 'gift_nifty_signal', label: '🇮🇳 Gift Nifty Signal', value: brief?.data?.gift_nifty_signal },
    { key: 'macro_gauges', label: '📊 Macro Gauges', value: brief?.data?.macro_gauges },
    { key: 'crypto_signal', label: '₿ Crypto Signal', value: brief?.data?.crypto_signal },
    { key: 'cross_asset_coherence', label: '🔗 Cross-Asset Coherence', value: brief?.data?.cross_asset_coherence },
    { key: 'key_risks_today', label: '⚠️ Key Risks Today', value: brief?.data?.key_risks_today },
    { key: 'volguard_implication', label: '🤖 VolGuard Implication', value: brief?.data?.volguard_implication },
  ].filter(s => !!s.value)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-header text-xs">MORNING INTELLIGENCE BRIEF</h2>
          {brief?.generated_at && (
            <p className="text-[10px] text-muted-foreground mt-0.5 font-mono-data">
              Generated: {new Date(brief.generated_at).toLocaleString('en-IN', { hour12: false })}
            </p>
          )}
        </div>
        <button
          onClick={generate}
          disabled={generating}
          className="flex items-center gap-2 text-[10px] bg-electric-blue/20 hover:bg-electric-blue/30 text-electric-blue border border-electric-blue/40 px-3 py-1.5 rounded transition-all disabled:opacity-50 font-semibold uppercase tracking-wide"
        >
          {generating ? (
            <><span className="w-3 h-3 border border-electric-blue border-t-transparent rounded-full animate-spin" />Generating…</>
          ) : '↻ Force Generate'}
        </button>
      </div>

      {!brief?.available ? (
        <div className="glass-card p-8 text-center space-y-3">
          <p className="text-4xl">📋</p>
          <p className="text-muted-foreground text-sm">
            {generating
              ? 'AI is analyzing global markets… this takes ~20 seconds'
              : 'Morning brief not yet generated today. Auto-generates at 08:30 IST.'}
          </p>
          {!generating && (
            <button onClick={generate} className="text-sm bg-electric-blue hover:bg-electric-blue/80 text-white px-4 py-2 rounded-md transition-all font-semibold">
              Generate Now
            </button>
          )}
        </div>
      ) : (
        <div className="space-y-3">
          {/* Tone Hero */}
          <div className={`glass-card p-5 border ${TONE_BG[tone] ?? 'bg-secondary border-white/10'} intel-glow`}>
            <div className="flex items-center gap-4">
              <span className="text-4xl">{TONE_EMOJI[tone] ?? '❓'}</span>
              <div>
                <p className="text-[10px] text-header mb-0.5">GLOBAL TONE</p>
                <p className={`font-mono-data text-3xl font-black ${TONE_COLOR[tone] ?? 'text-foreground'}`}>{tone}</p>
              </div>
            </div>
          </div>

          {/* Implication — always shown prominently */}
          {brief.data?.volguard_implication && (
            <div className="glass-card p-5 border border-electric-blue/20 bg-electric-blue/5">
              <p className="text-[10px] text-header mb-2">🤖 VOLGUARD IMPLICATION</p>
              <p className="text-sm text-foreground leading-relaxed">{brief.data.volguard_implication}</p>
            </div>
          )}

          {/* Key Risks */}
          {brief.data?.key_risks_today && (
            <div className="glass-card p-5 border border-signal-red/20 bg-signal-red/5">
              <p className="text-[10px] text-header mb-2">⚠️ KEY RISKS TODAY</p>
              <p className="text-sm text-foreground leading-relaxed">{brief.data.key_risks_today}</p>
            </div>
          )}

          {/* Expandable sections grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {sections
              .filter(s => !['volguard_implication', 'key_risks_today'].includes(s.key))
              .map(s => (
                <div key={s.key} className="glass-card overflow-hidden">
                  <button
                    onClick={() => toggle(s.key)}
                    className="w-full flex items-center justify-between p-4 hover:bg-white/3 transition-colors"
                  >
                    <p className="text-[10px] text-header text-left">{s.label}</p>
                    <span className="text-muted-foreground text-xs ml-2 shrink-0">{expanded[s.key] ? '▲' : '▼'}</span>
                  </button>
                  {expanded[s.key] ? (
                    <div className="px-4 pb-4 border-t border-white/5">
                      <p className="text-xs text-foreground leading-relaxed pt-3">{s.value}</p>
                    </div>
                  ) : (
                    <p className="px-4 pb-3 text-xs text-muted-foreground line-clamp-2">{s.value}</p>
                  )}
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Section: Macro Snapshot ─────────────────────────────────────────────────
function MacroSnapshot() {
  const [snap, setSnap] = useState<V5MacroSnapshot | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const load = async () => {
      try { setSnap(await fetchMacroSnapshot(true)) } catch {}
      setLoading(false)
    }
    load()
    const iv = setInterval(load, 60000)
    return () => clearInterval(iv)
  }, [])

  if (loading) return (
    <div className="glass-card p-5 animate-pulse">
      <div className="flex gap-4">
        {[1,2,3,4,5,6,7].map(i => <div key={i} className="flex-1 h-12 bg-white/5 rounded" />)}
      </div>
    </div>
  )
  if (!snap) return null

  const toneColor = TONE_COLOR[snap.global_tone] ?? 'text-muted-foreground'

  const metrics = [
    {
      label: 'DXY',
      value: snap.dxy_level != null ? snap.dxy_level.toFixed(2) : '—',
      sub: snap.dxy_direction ?? '',
      color: snap.dxy_direction === 'UP' ? 'text-signal-red' : snap.dxy_direction === 'DOWN' ? 'text-neon-green' : 'text-foreground',
    },
    {
      label: 'US 10Y',
      value: snap.us_10y_yield != null ? `${snap.us_10y_yield.toFixed(2)}%` : '—',
      sub: snap.us_10y_elevated ? 'ELEVATED' : 'NORMAL',
      color: snap.us_10y_elevated ? 'text-signal-red' : 'text-neon-green',
    },
    {
      label: 'VIX FUT',
      value: snap.vix_futures != null ? snap.vix_futures.toFixed(2) : '—',
      sub: snap.vix_sentiment ?? '',
      color: snap.vix_futures != null && snap.vix_futures > 20 ? 'text-signal-red' : 'text-neon-green',
    },
    {
      label: 'CRUDE',
      value: snap.crude_price != null ? `$${snap.crude_price.toFixed(1)}` : '—',
      sub: snap.crude_direction ?? '',
      color: 'text-foreground',
    },
    {
      label: 'GOLD',
      value: snap.gold_price != null ? `$${snap.gold_price.toFixed(0)}` : '—',
      sub: snap.gold_sentiment ?? '',
      color: 'text-yellow-400',
    },
    {
      label: 'BTC',
      value: snap.btc_price != null ? `$${(snap.btc_price / 1000).toFixed(1)}K` : '—',
      sub: snap.btc_direction ?? '',
      color: 'text-orange-400',
    },
    {
      label: 'PREV NIFTY',
      value: snap.sgx_nifty != null ? snap.sgx_nifty.toLocaleString('en-IN') : '—',
      sub: snap.sgx_signal ?? '',
      color: snap.sgx_signal === 'BULLISH' ? 'text-neon-green' : snap.sgx_signal === 'BEARISH' ? 'text-signal-red' : 'text-foreground',
    },
  ]

  return (
    <div className="glass-card p-5 space-y-4">
      <div className="flex items-center justify-between border-b border-white/10 pb-3">
        <p className="text-[10px] text-header">LIVE MACRO SNAPSHOT</p>
        <div className="flex items-center gap-3 text-[10px] text-muted-foreground font-mono-data">
          <span>Risk-Off: <span className="text-signal-red font-bold">{snap.risk_off_signals}</span></span>
          <span>Risk-On: <span className="text-neon-green font-bold">{snap.risk_on_signals}</span></span>
          <span>Tone: <span className={`font-bold ${toneColor}`}>{snap.global_tone}</span></span>
        </div>
      </div>
      <div className="grid grid-cols-3 md:grid-cols-4 lg:grid-cols-7 gap-3">
        {metrics.map(m => (
          <div key={m.label} className="text-center">
            <p className="text-[9px] text-header mb-1">{m.label}</p>
            <p className={`font-mono-data text-sm font-bold ${m.color}`}>{m.value}</p>
            {m.sub && <p className="text-[9px] text-muted-foreground mt-0.5">{m.sub}</p>}
          </div>
        ))}
      </div>
    </div>
  )
}

// ─── Section: News VETO Scanner ─────────────────────────────────────────────
function NewsScanner() {
  const [news, setNews] = useState<V5NewsResponse | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const load = async () => {
      try { setNews(await fetchNews(true)) } catch {}
      setLoading(false)
    }
    load()
    const iv = setInterval(load, 30000)
    return () => clearInterval(iv)
  }, [])

  if (loading) return <Spinner label="Scanning news feeds…" />
  if (!news) return null

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-header text-xs">NEWS VETO SCANNER</h2>
        <span className="text-[9px] text-muted-foreground font-mono-data">
          {news.total_scanned} scanned · {news.fetch_errors} errors
        </span>
      </div>

      {news.has_veto && (
        <div className="glass-card veto-glow p-4 border-signal-red/50 space-y-3">
          <p className="text-signal-red font-bold text-sm">🚫 VETO-LEVEL NEWS — TRADING HALTED BY NEWS GATE</p>
          {news.veto_items.map((item, i) => (
            <div key={i} className="border-t border-signal-red/20 pt-2">
              <p className="text-xs text-signal-red font-semibold">{item.title}</p>
              <div className="flex items-center gap-2 mt-1 flex-wrap">
                <span className="text-[9px] text-muted-foreground">{item.source}</span>
                {(item.keywords ?? []).map(k => (
                  <span key={k} className="text-[8px] bg-signal-red/20 text-signal-red px-1.5 py-0.5 rounded">{k}</span>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}

      {!news.has_veto && news.has_high_impact && (
        <div className="glass-card p-4 border border-yellow-400/30 bg-yellow-400/5 space-y-2">
          <p className="text-yellow-400 font-bold text-sm">⚠️ High Impact Events Detected</p>
          {news.high_impact_items.map((item, i) => (
            <div key={i} className="border-t border-yellow-400/10 pt-2">
              <p className="text-xs text-foreground">{item.title}</p>
              <span className="text-[9px] text-muted-foreground">{item.source}</span>
            </div>
          ))}
        </div>
      )}

      {!news.has_veto && !news.has_high_impact && (
        <div className="glass-card p-4 border border-neon-green/20 bg-neon-green/5">
          <p className="text-neon-green text-sm font-semibold">✅ No veto-level news — news gate is clear</p>
        </div>
      )}

      {(news.watch_items?.length ?? 0) > 0 && (
        <div className="glass-card p-4">
          <p className="text-[10px] text-header mb-2">WATCH LIST ({news.watch_items.length})</p>
          <div className="space-y-1.5">
            {news.watch_items.map((item, i) => (
              <div key={i} className="flex items-start gap-2 text-xs">
                <span className="text-muted-foreground mt-0.5 shrink-0">•</span>
                <div className="min-w-0">
                  <span className="text-foreground">{item.title}</span>
                  <span className="text-muted-foreground text-[9px] ml-2">{item.source}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Section: Alerts ─────────────────────────────────────────────────────────
function AlertsPanel() {
  const [data, setData] = useState<V5AlertsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [scanning, setScanning] = useState(false)

  const load = async () => {
    try { setData(await fetchAlerts(10)) } catch {}
    setLoading(false)
  }

  useEffect(() => {
    load()
    const iv = setInterval(load, 30000)
    return () => clearInterval(iv)
  }, [])

  const scan = async () => {
    setScanning(true)
    try {
      await triggerMonitorScan()
      toast.info('Monitor scan triggered — refreshing in 3s')
      setTimeout(load, 3000)
    } catch { toast.error('Scan failed') }
    setScanning(false)
  }

  if (loading) return <Spinner label="Loading alerts…" />

  const alerts = data?.alerts ?? []

  const LEVEL_COLOR: Record<string, string> = {
    CRITICAL: 'border-signal-red/40 bg-signal-red/10',
    REVIEW_POSITIONS: 'border-orange-400/40 bg-orange-400/10',
    CONSIDER_EXIT: 'border-signal-red/40 bg-signal-red/15',
    MONITOR: 'border-yellow-400/30 bg-yellow-400/5',
    CLEAR: 'border-neon-green/30 bg-neon-green/5',
  }
  const LEVEL_TEXT: Record<string, string> = {
    CRITICAL: 'text-signal-red',
    REVIEW_POSITIONS: 'text-orange-400',
    CONSIDER_EXIT: 'text-signal-red',
    MONITOR: 'text-yellow-400',
    CLEAR: 'text-neon-green',
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-header text-xs">AI MONITOR ALERTS</h2>
        <button
          onClick={scan}
          disabled={scanning}
          className="text-[10px] bg-secondary hover:bg-secondary/80 text-muted-foreground hover:text-foreground px-3 py-1 rounded transition-all disabled:opacity-50"
        >{scanning ? 'Scanning…' : '↻ Force Scan'}</button>
      </div>

      {alerts.length === 0 ? (
        <div className="glass-card p-4 border border-neon-green/20 bg-neon-green/5">
          <p className="text-neon-green text-sm">✅ No active alerts — market conditions normal</p>
        </div>
      ) : (
        <div className="space-y-2">
          {alerts.map((a, i) => {
            const cls = LEVEL_COLOR[a.alert_level] ?? LEVEL_COLOR.MONITOR
            const textCls = LEVEL_TEXT[a.alert_level] ?? LEVEL_TEXT.MONITOR
            return (
              <div key={i} className={`glass-card p-4 border rounded-lg ${cls}`}>
                <div className="flex items-center justify-between mb-1.5">
                  <span className={`text-[10px] font-black uppercase tracking-wider ${textCls}`}>{a.alert_level}</span>
                  <span className="text-[9px] text-muted-foreground font-mono-data">
                    {new Date(a.timestamp).toLocaleTimeString('en-IN', { hour12: false })}
                  </span>
                </div>
                {a.what_changed && (
                  <p className="text-sm font-semibold text-foreground mb-1">{a.what_changed}</p>
                )}
                {a.why_it_matters && (
                  <p className="text-xs text-muted-foreground">{a.why_it_matters}</p>
                )}
                {a.suggested_action && (
                  <p className="text-xs text-electric-blue mt-1.5 font-semibold">→ {a.suggested_action}</p>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ─── Section: VETO Log ───────────────────────────────────────────────────────
function VetoLog() {
  const [data, setData] = useState<V5VetoLogResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [overrideTarget, setOverrideTarget] = useState<V5VetoRecord | null>(null)
  const [overrideReason, setOverrideReason] = useState('')
  const [overriding, setOverriding] = useState(false)

  const load = async () => {
    try { setData(await fetchVetoLog(20)) } catch {}
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const doOverride = async () => {
    if (overrideReason.trim().length < 10) {
      toast.error('Reason must be at least 10 characters')
      return
    }
    setOverriding(true)
    try {
      const r = await overrideVeto(overrideReason.trim())
      if (r.success) {
        toast.success('VETO overridden — trade will proceed')
        setOverrideTarget(null)
        setOverrideReason('')
        load()
      } else {
        toast.error(r.message ?? 'Override failed')
      }
    } catch { toast.error('Override request failed') }
    setOverriding(false)
  }

  const REC_STYLE: Record<string, string> = {
    VETO: 'text-signal-red',
    PROCEED_WITH_CAUTION: 'text-yellow-400',
    PROCEED: 'text-neon-green',
  }
  const REC_ICON: Record<string, string> = {
    VETO: '🚫',
    PROCEED_WITH_CAUTION: '⚠️',
    PROCEED: '✅',
  }

  if (loading) return <Spinner label="Loading pre-trade gate log…" />

  const log = data?.veto_log ?? []

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-header text-xs">PRE-TRADE GATE LOG</h2>
        <span className="text-[9px] text-muted-foreground font-mono-data">{data?.total_in_memory ?? 0} evaluations in memory</span>
      </div>

      {log.length === 0 ? (
        <div className="glass-card p-4">
          <p className="text-muted-foreground text-sm">No pre-trade evaluations yet — gate fires when execute_strategy is called</p>
        </div>
      ) : (
        <div className="space-y-2">
          {log.map((entry, i) => (
            <div key={i} className={`glass-card p-4 border ${
              entry.recommendation === 'VETO' ? 'border-signal-red/30 bg-signal-red/5' :
              entry.recommendation === 'PROCEED_WITH_CAUTION' ? 'border-yellow-400/20' :
              'border-neon-green/20'
            }`}>
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap mb-1.5">
                    <span className={`font-mono-data font-black text-sm ${REC_STYLE[entry.recommendation] ?? 'text-foreground'}`}>
                      {REC_ICON[entry.recommendation] ?? '?'} {entry.recommendation}
                    </span>
                    <span className="text-[9px] text-electric-blue font-mono-data bg-electric-blue/10 px-1.5 py-0.5 rounded">{entry.strategy}</span>
                    <span className="text-[9px] text-muted-foreground">{entry.expiry_type}</span>
                    <span className="text-[9px] text-muted-foreground font-mono-data">
                      Score: {entry.regime_score != null ? entry.regime_score.toFixed(1) : '—'}
                    </span>
                  </div>
                  {entry.rationale && (
                    <p className="text-xs text-muted-foreground leading-relaxed">{entry.rationale}</p>
                  )}
                  {entry.veto_reason && (
                    <p className="text-xs text-signal-red mt-1 font-semibold">🚫 {entry.veto_reason}</p>
                  )}
                  {(entry.adjustments?.length ?? 0) > 0 && (
                    <div className="mt-1.5 space-y-0.5">
                      {entry.adjustments.map((a, j) => (
                        <p key={j} className="text-[10px] text-yellow-400">→ {a}</p>
                      ))}
                    </div>
                  )}
                  {entry.overridden && (
                    <p className="text-[10px] text-electric-blue mt-1.5">
                      ✓ Overridden: {entry.override_reason}
                    </p>
                  )}
                </div>
                <div className="flex flex-col items-end gap-1.5 shrink-0">
                  <span className="text-[9px] text-muted-foreground font-mono-data">
                    {new Date(entry.timestamp).toLocaleTimeString('en-IN', { hour12: false })}
                  </span>
                  {entry.recommendation === 'VETO' && !entry.overridden && (
                    <button
                      onClick={() => { setOverrideTarget(entry); setOverrideReason('') }}
                      className="text-[9px] bg-signal-red/20 hover:bg-signal-red/30 text-signal-red border border-signal-red/40 px-2 py-0.5 rounded transition-all font-semibold"
                    >Override</button>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Override Modal */}
      {overrideTarget && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="glass-card p-6 w-full max-w-md space-y-4 border border-signal-red/50">
            <p className="text-signal-red font-bold text-base">⚠️ Override VETO Decision</p>
            <div className="bg-signal-red/10 rounded p-3 space-y-1">
              <p className="text-xs text-muted-foreground">
                Strategy: <span className="text-foreground font-semibold">{overrideTarget.strategy}</span>
                <span className="mx-2">·</span>
                Expiry: <span className="text-foreground">{overrideTarget.expiry_type}</span>
              </p>
              {overrideTarget.veto_reason && (
                <p className="text-xs text-signal-red">{overrideTarget.veto_reason}</p>
              )}
            </div>
            <div>
              <label className="text-[10px] text-header block mb-2">
                JUSTIFICATION (min 10 chars · logged permanently in DB)
              </label>
              <textarea
                value={overrideReason}
                onChange={e => setOverrideReason(e.target.value)}
                rows={3}
                placeholder="Why are you overriding this VETO?"
                className="w-full bg-secondary border border-white/10 rounded px-3 py-2 text-xs font-mono-data text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-signal-red resize-none"
              />
              <p className={`text-[9px] mt-1 ${overrideReason.length >= 10 ? 'text-neon-green' : 'text-muted-foreground'}`}>
                {overrideReason.length} / 10 minimum
              </p>
            </div>
            <div className="flex gap-3">
              <button
                onClick={() => setOverrideTarget(null)}
                className="flex-1 py-2 border border-white/10 hover:bg-white/5 rounded text-sm transition-colors"
              >Cancel</button>
              <button
                onClick={doOverride}
                disabled={overriding || overrideReason.trim().length < 10}
                className="flex-1 py-2 bg-signal-red hover:bg-signal-red/80 text-white font-bold rounded text-sm transition-all disabled:opacity-50"
              >{overriding ? 'Overriding…' : 'Confirm Override'}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Main Tab ─────────────────────────────────────────────────────────────────
export function IntelligenceTab() {
  return (
    <div className="space-y-8">
      {/* Tab header */}
      <div className="flex items-center gap-3">
        <div className="w-1.5 h-6 bg-electric-blue rounded-full" />
        <h1 className="text-sm font-bold text-foreground">V5 Intelligence Layer</h1>
        <span className="text-[9px] text-electric-blue font-mono-data border border-electric-blue/40 bg-electric-blue/10 px-2 py-0.5 rounded-full">
          AI-POWERED · 3 AGENTS
        </span>
      </div>

      {/* Macro always visible at top — global context at a glance */}
      <MacroSnapshot />

      {/* Two-column: Brief + News/Alerts */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
        <MorningBrief />
        <div className="space-y-6">
          <NewsScanner />
          <AlertsPanel />
        </div>
      </div>

      {/* Full-width Veto Log */}
      <VetoLog />
    </div>
  )
}
