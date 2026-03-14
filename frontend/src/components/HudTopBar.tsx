import { useState, useEffect, useRef } from 'react'
import { fetchBulkPrice, fetchV5Status, fetchGlobalTone } from '@/lib/api'
import type { V5Status, V5GlobalTone } from '@/lib/types'

interface Props { onDisconnect: () => void }

const TONE_COLOR: Record<string, string> = {
  CLEAR: 'text-neon-green',
  CAUTIOUS_NEUTRAL: 'text-yellow-400',
  CAUTIOUS: 'text-orange-400',
  RISK_OFF: 'text-signal-red',
  MIXED: 'text-muted-foreground',
  UNKNOWN: 'text-muted-foreground',
}

const TONE_DOT: Record<string, string> = {
  CLEAR: 'bg-neon-green',
  CAUTIOUS_NEUTRAL: 'bg-yellow-400',
  CAUTIOUS: 'bg-orange-400',
  RISK_OFF: 'bg-signal-red',
  MIXED: 'bg-muted-foreground',
  UNKNOWN: 'bg-muted-foreground',
}

export function HudTopBar({ onDisconnect }: Props) {
  const [spot, setSpot] = useState<number | null>(null)
  const [vix, setVix] = useState<number | null>(null)
  const [prevSpot, setPrevSpot] = useState<number | null>(null)
  const [v5Status, setV5Status] = useState<V5Status | null>(null)
  const [globalTone, setGlobalTone] = useState<V5GlobalTone | null>(null)
  const [loading, setLoading] = useState(true)

  // Fetch market data every 10s
  useEffect(() => {
    const fetch = async () => {
      try {
        const prices = await fetchBulkPrice(['NSE_INDEX|Nifty 50', 'NSE_INDEX|India VIX'])
        const newSpot = prices['NSE_INDEX|Nifty 50']
        setPrevSpot(spot)
        setSpot(prev => { setPrevSpot(prev); return newSpot ?? null })
        setVix(prices['NSE_INDEX|India VIX'] ?? null)
      } catch {}
      setLoading(false)
    }
    fetch()
    const iv = setInterval(fetch, 10000)
    return () => clearInterval(iv)
  }, [])

  // Fetch intelligence status every 60s
  useEffect(() => {
    const fetch = async () => {
      try {
        const [status, tone] = await Promise.all([fetchV5Status(), fetchGlobalTone()])
        setV5Status(status)
        setGlobalTone(tone)
      } catch {}
    }
    fetch()
    const iv = setInterval(fetch, 60000)
    return () => clearInterval(iv)
  }, [])

  const spotDir = prevSpot && spot ? (spot > prevSpot ? 'up' : spot < prevSpot ? 'down' : null) : null
  const tone = globalTone?.global_tone ?? v5Status?.morning_brief?.global_tone ?? 'UNKNOWN'
  const briefReady = v5Status?.morning_brief?.status === 'AVAILABLE'
  const aiOnline = v5Status?.intelligence_layer === 'ONLINE'

  // Provider name from intelligence_layer string
  const provider = aiOnline
    ? (v5Status?.intelligence_layer?.includes('OFFLINE') ? 'OFFLINE' : 'ONLINE')
    : 'OFFLINE'

  return (
    <header className="sticky top-0 z-50 border-b border-white/10 backdrop-blur-md bg-black/80">
      <div className="max-w-[1800px] mx-auto px-4 h-13 flex items-center justify-between gap-4">

        {/* LEFT: Logo */}
        <div className="flex items-center gap-3 min-w-fit">
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 rounded bg-electric-blue/20 border border-electric-blue/40 flex items-center justify-center">
              <span className="text-electric-blue text-[9px] font-black">VG</span>
            </div>
            <span className="text-sm font-black tracking-widest uppercase text-foreground">
              Vol<span className="text-electric-blue">Guard</span>
            </span>
            <span className="text-[9px] text-muted-foreground font-mono-data hidden sm:block">V5</span>
          </div>
        </div>

        {/* CENTER: Market ticker + intelligence status */}
        <div className="flex-1 flex items-center justify-center gap-6 text-xs font-mono-data overflow-hidden">
          {/* SPOT */}
          <div className="flex items-center gap-1.5">
            <span className="text-muted-foreground text-[10px]">SPOT</span>
            <span className={`font-bold text-sm tabular-nums ${spotDir === 'up' ? 'tick-up text-neon-green' : spotDir === 'down' ? 'tick-down text-signal-red' : 'text-foreground'}`}>
              {loading ? '…' : spot?.toLocaleString('en-IN', { maximumFractionDigits: 2 }) ?? '—'}
            </span>
            {spotDir === 'up' && <span className="text-neon-green text-[8px]">▲</span>}
            {spotDir === 'down' && <span className="text-signal-red text-[8px]">▼</span>}
          </div>

          <div className="w-px h-4 bg-white/10" />

          {/* VIX */}
          <div className="flex items-center gap-1.5">
            <span className="text-muted-foreground text-[10px]">VIX</span>
            <span className={`font-bold text-sm tabular-nums ${vix && vix > 20 ? 'text-signal-red' : 'text-neon-green'}`}>
              {loading ? '…' : vix?.toFixed(2) ?? '—'}
            </span>
          </div>

          <div className="w-px h-4 bg-white/10 hidden md:block" />

          {/* TONE */}
          <div className="hidden md:flex items-center gap-1.5">
            <span className="text-muted-foreground text-[10px]">TONE</span>
            <div className={`w-1.5 h-1.5 rounded-full ${TONE_DOT[tone] ?? 'bg-muted-foreground'}`} />
            <span className={`font-bold text-[11px] ${TONE_COLOR[tone] ?? 'text-muted-foreground'}`}>{tone}</span>
          </div>

          <div className="w-px h-4 bg-white/10 hidden lg:block" />

          {/* BRIEF STATUS */}
          <div className="hidden lg:flex items-center gap-1.5">
            <span className="text-muted-foreground text-[10px]">BRIEF</span>
            {briefReady ? (
              <span className="flex items-center gap-1 text-neon-green text-[10px] font-bold">
                <span className="w-1.5 h-1.5 rounded-full bg-neon-green pulse-green" />
                READY
              </span>
            ) : (
              <span className="flex items-center gap-1 text-yellow-400 text-[10px] font-bold">
                <span className="w-1.5 h-1.5 rounded-full bg-yellow-400 pulse-blue" />
                PENDING
              </span>
            )}
          </div>

          <div className="w-px h-4 bg-white/10 hidden lg:block" />

          {/* AI STATUS */}
          <div className="hidden lg:flex items-center gap-1.5">
            <span className="text-muted-foreground text-[10px]">AI</span>
            {aiOnline ? (
              <span className="flex items-center gap-1 text-electric-blue text-[10px] font-bold">
                <span className="w-1.5 h-1.5 rounded-full bg-electric-blue pulse-blue" />
                ONLINE
              </span>
            ) : (
              <span className="text-muted-foreground text-[10px] font-bold">OFFLINE</span>
            )}
          </div>
        </div>

        {/* RIGHT: Disconnect */}
        <div className="min-w-fit">
          <button
            onClick={onDisconnect}
            className="text-[10px] uppercase tracking-wider text-muted-foreground hover:text-signal-red transition-colors font-semibold border border-white/10 px-3 py-1.5 rounded hover:border-signal-red/50"
          >
            Disconnect
          </button>
        </div>

      </div>
    </header>
  )
}