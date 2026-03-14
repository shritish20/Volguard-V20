import { useState, useEffect, useRef } from 'react'
import { fetchJournal } from '@/lib/api'
import api from '@/lib/api'
import type { TradeEntry } from '@/lib/types'

interface CoachMessage {
  role: 'user' | 'coach'
  content: string
  timestamp: string
  stats?: Record<string, unknown>
  hasMockData?: boolean
  llmProvider?: string
}

const SUGGESTED_QUESTIONS = [
  "Why are my losing trades happening?",
  "What does my theta vs vega attribution tell me?",
  "Am I trading on days I shouldn't?",
  "What are my worst repeating mistakes?",
  "Analyze my overall risk management",
  "When does my edge actually work?",
]

export function JournalTab() {
  const [trades, setTrades] = useState<TradeEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [fallback, setFallback] = useState(false)
  const [filter, setFilter] = useState('')
  const [activeSection, setActiveSection] = useState<'journal' | 'coach'>('journal')

  const [coachMessages, setCoachMessages] = useState<CoachMessage[]>([])
  const [coachInput, setCoachInput] = useState('')
  const [coachLoading, setCoachLoading] = useState(false)
  const [coachError, setCoachError] = useState('')
  const messagesEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const load = async () => {
      try {
        const data = await fetchJournal(50)
        setTrades(Array.isArray(data) ? data : [])
        setFallback(false)
      } catch {
        setFallback(true)
      } finally { setLoading(false) }
    }
    load()
  }, [])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [coachMessages])

  const mockTrades = trades.filter(t => t.is_mock)
  const realTrades = trades.filter(t => !t.is_mock)
  const hasMockData = mockTrades.length > 0
  const allMock = hasMockData && realTrades.length === 0

  const filtered = trades.filter(t =>
    !filter ||
    (t.strategy ?? '').toLowerCase().includes(filter.toLowerCase()) ||
    (t.expiry_type ?? '').toLowerCase().includes(filter.toLowerCase()) ||
    (t.result ?? '').toLowerCase().includes(filter.toLowerCase())
  )

  const dailyPnl = trades.reduce<Record<string, number>>((acc, t) => {
    acc[t.date] = (acc[t.date] ?? 0) + t.pnl
    return acc
  }, {})

  const totalTrades = trades.length
  const wins = trades.filter(t => t.pnl > 0)
  const losses = trades.filter(t => t.pnl <= 0)
  const totalPnl = trades.reduce((a, t) => a + t.pnl, 0)
  const winRate = totalTrades > 0 ? ((wins.length / totalTrades) * 100).toFixed(1) : '0.0'
  const avgWin = wins.length > 0 ? wins.reduce((a, t) => a + t.pnl, 0) / wins.length : 0
  const avgLoss = losses.length > 0 ? losses.reduce((a, t) => a + t.pnl, 0) / losses.length : 0
  const grossWins = wins.reduce((a, t) => a + t.pnl, 0)
  const grossLosses = Math.abs(losses.reduce((a, t) => a + t.pnl, 0))
  const profitFactor = grossLosses > 0 ? grossWins / grossLosses : grossWins > 0 ? Infinity : 0
  const dailyPnlValues = Object.values(dailyPnl)
  const maxDailyLoss = dailyPnlValues.length > 0 ? Math.min(...dailyPnlValues) : 0

  // Outcome classification breakdown
  const skillWins     = trades.filter(t => t.trade_outcome_class === 'SKILL_WIN').length
  const luckyWins     = trades.filter(t => t.trade_outcome_class === 'LUCKY_WIN').length
  const unluckyLosses = trades.filter(t => t.trade_outcome_class === 'UNLUCKY_LOSS').length
  const skillLosses   = trades.filter(t => t.trade_outcome_class === 'SKILL_LOSS').length
  const trueSkillWinRate = totalTrades > 0 ? ((skillWins / totalTrades) * 100).toFixed(1) : '0.0'
  const hasOutcomeData = trades.some(t => t.trade_outcome_class && t.trade_outcome_class !== 'UNCLASSIFIED')

  const askCoach = async (question: string) => {
    if (!question.trim() || coachLoading) return
    const userMsg: CoachMessage = {
      role: 'user',
      content: question.trim(),
      timestamp: new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }),
    }
    setCoachMessages(prev => [...prev, userMsg])
    setCoachInput('')
    setCoachLoading(true)
    setCoachError('')
    try {
      const resp = await api.post('/api/intelligence/coach', { question: question.trim() })
      const data = resp.data
      const coachMsg: CoachMessage = {
        role: 'coach',
        content: data.response,
        timestamp: new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }),
        stats: data.stats,
        hasMockData: data.has_mock_data,
        llmProvider: data.llm_provider,
      }
      setCoachMessages(prev => [...prev, coachMsg])
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Coach unavailable'
      setCoachError(msg)
    } finally {
      setCoachLoading(false)
    }
  }

  if (loading) return (
    <div className="flex flex-col items-center justify-center py-16 gap-4">
      <div className="w-10 h-10 border-3 border-electric-blue border-t-transparent rounded-full animate-spin" />
      <p className="text-muted-foreground text-sm">Loading trade history…</p>
    </div>
  )

  return (
    <div className="space-y-6">
      {fallback && (
        <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
          <p className="text-yellow-400 text-xs font-semibold">⚠ Backend unavailable — no trade history to display</p>
        </div>
      )}

      {hasMockData && (
        <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg p-3 flex items-start gap-3">
          <span className="text-amber-400 text-sm mt-0.5">🎭</span>
          <div className="flex-1">
            <p className="text-amber-300 text-xs font-semibold">
              {allMock
                ? 'Demo Data — Simulated NIFTY options trades (Oct 2025 – Mar 2026)'
                : `Mixed Data — ${realTrades.length} real trade${realTrades.length !== 1 ? 's' : ''} + ${mockTrades.length} demo trade${mockTrades.length !== 1 ? 's' : ''}`
              }
            </p>
            <p className="text-amber-400/70 text-[10px] mt-1">
              {allMock
                ? 'Journal Coach will analyze this with full rigor — patterns are realistic. Once real trades exist, run the seed script with --clear to remove demo data.'
                : 'Demo trades marked ⚠. Coach analyzes all trades together.'
              }
            </p>
          </div>
        </div>
      )}

      <div className="flex gap-2">
        {(['journal', 'coach'] as const).map(s => (
          <button
            key={s}
            onClick={() => setActiveSection(s)}
            className={`px-4 py-2 rounded text-xs font-semibold uppercase tracking-wider transition-colors ${
              activeSection === s
                ? 'bg-electric-blue text-black'
                : 'bg-secondary text-muted-foreground hover:text-foreground'
            }`}
          >
            {s === 'journal' ? '📊 Journal' : '🧠 Coach'}
          </button>
        ))}
      </div>

      {activeSection === 'journal' && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {[
              { label: 'TOTAL TRADES', value: totalTrades, sub: hasMockData ? `${realTrades.length} real · ${mockTrades.length} demo` : undefined, color: 'text-foreground' },
              { label: 'WIN RATE', value: `${winRate}%`, color: parseFloat(winRate) >= 50 ? 'text-neon-green' : 'text-signal-red' },
              { label: 'TOTAL P&L', value: `${totalPnl >= 0 ? '+' : ''}₹${Math.abs(totalPnl).toLocaleString('en-IN')}`, color: totalPnl >= 0 ? 'text-neon-green' : 'text-signal-red' },
              { label: 'TRADING DAYS', value: Object.keys(dailyPnl).length, color: 'text-electric-blue' },
            ].map(s => (
              <div key={s.label} className="glass-card p-4">
                <p className="text-[9px] text-header mb-1">{s.label}</p>
                <p className={`font-mono-data text-xl font-bold ${s.color}`}>{s.value}</p>
                {s.sub && <p className="text-[9px] text-amber-400/70 mt-1 font-mono-data">{s.sub}</p>}
              </div>
            ))}
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="glass-card p-4">
              <p className="text-[9px] text-header mb-1">PROFIT FACTOR</p>
              <p className={`font-mono-data text-xl font-bold ${profitFactor >= 1.5 ? 'text-neon-green' : profitFactor >= 1 ? 'text-yellow-400' : 'text-signal-red'}`}>
                {profitFactor === Infinity ? '∞' : profitFactor.toFixed(2)}x
              </p>
            </div>
            <div className="glass-card p-4">
              <p className="text-[9px] text-header mb-1">AVG WIN</p>
              <p className="font-mono-data text-xl font-bold text-neon-green">
                {avgWin > 0 ? `+₹${Math.round(avgWin).toLocaleString('en-IN')}` : '—'}
              </p>
              <p className="text-[9px] text-muted-foreground mt-1">{wins.length} winning trades</p>
            </div>
            <div className="glass-card p-4">
              <p className="text-[9px] text-header mb-1">AVG LOSS</p>
              <p className="font-mono-data text-xl font-bold text-signal-red">
                {avgLoss < 0 ? `₹${Math.round(Math.abs(avgLoss)).toLocaleString('en-IN')}` : '—'}
              </p>
              <p className="text-[9px] text-muted-foreground mt-1">{losses.length} losing trades</p>
            </div>
            <div className="glass-card p-4">
              <p className="text-[9px] text-header mb-1">WORST DAY</p>
              <p className={`font-mono-data text-xl font-bold ${maxDailyLoss < 0 ? 'text-signal-red' : 'text-muted-foreground'}`}>
                {maxDailyLoss < 0 ? `₹${Math.abs(Math.round(maxDailyLoss)).toLocaleString('en-IN')}` : '—'}
              </p>
            </div>
          </div>

          {avgWin > 0 && avgLoss < 0 && (
            <div className={`glass-card p-3 flex items-center gap-3 border ${Math.abs(avgWin / avgLoss) >= 1 ? 'border-neon-green/20 bg-neon-green/5' : 'border-yellow-400/20 bg-yellow-400/5'}`}>
              <span className="text-sm">{Math.abs(avgWin / avgLoss) >= 1 ? '✅' : '⚠️'}</span>
              <p className="text-xs text-foreground">
                R-ratio: <span className={`font-mono-data font-bold ${Math.abs(avgWin / avgLoss) >= 1 ? 'text-neon-green' : 'text-yellow-400'}`}>{(Math.abs(avgWin / avgLoss)).toFixed(2)}</span>
                <span className="text-muted-foreground ml-2">— avg win is {(Math.abs(avgWin / avgLoss)).toFixed(2)}x avg loss.</span>
              </p>
            </div>
          )}

          {/* Skill vs Luck breakdown */}
          {hasOutcomeData && (
            <div className="glass-card p-4 space-y-3">
              <div className="flex items-center justify-between">
                <p className="text-[10px] text-header">SKILL vs LUCK BREAKDOWN</p>
                <div className="flex items-center gap-1.5">
                  <span className="text-[9px] text-muted-foreground">True skill win rate:</span>
                  <span className={`font-mono-data text-xs font-bold ${parseFloat(trueSkillWinRate) >= 50 ? 'text-neon-green' : 'text-signal-red'}`}>
                    {trueSkillWinRate}%
                  </span>
                  <span className="text-[8px] text-muted-foreground">(reported: {winRate}%)</span>
                </div>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                {[
                  {
                    label: 'SKILL WIN',
                    count: skillWins,
                    desc: 'Edge worked. Theta drove it. Repeatable.',
                    color: 'text-neon-green',
                    border: 'border-neon-green/30',
                    bg: 'bg-neon-green/5',
                    icon: '✦',
                  },
                  {
                    label: 'LUCKY WIN',
                    count: luckyWins,
                    desc: 'Won but not through theta or good conditions.',
                    color: 'text-yellow-400',
                    border: 'border-yellow-400/30',
                    bg: 'bg-yellow-400/5',
                    icon: '◈',
                  },
                  {
                    label: 'UNLUCKY LOSS',
                    count: unluckyLosses,
                    desc: 'Right conditions. Vol shock hit. Accept and move on.',
                    color: 'text-electric-blue',
                    border: 'border-electric-blue/30',
                    bg: 'bg-electric-blue/5',
                    icon: '⊘',
                  },
                  {
                    label: 'SKILL LOSS',
                    count: skillLosses,
                    desc: 'Bad conditions entered. Lost as predicted. Avoidable.',
                    color: 'text-signal-red',
                    border: 'border-signal-red/30',
                    bg: 'bg-signal-red/5',
                    icon: '✕',
                  },
                ].map(row => (
                  <div key={row.label} className={`rounded-lg p-3 border ${row.border} ${row.bg}`}>
                    <div className="flex items-center justify-between mb-1">
                      <span className={`text-[9px] font-semibold tracking-wider ${row.color}`}>{row.label}</span>
                      <span className={`font-mono-data text-lg font-bold ${row.color}`}>{row.count}</span>
                    </div>
                    <p className="text-[8px] text-muted-foreground leading-relaxed">{row.desc}</p>
                  </div>
                ))}
              </div>
              <p className="text-[9px] text-muted-foreground">
                Lucky wins inflate your reported win rate. Skill losses are the only ones you can fix.
                Unlucky losses are the cost of doing business — not a system failure.
              </p>
            </div>
          )}

          <div className="glass-card p-4">
            <div className="flex items-center justify-between mb-3">
              <p className="text-[10px] text-header">TRADE HISTORY</p>
              <input
                value={filter}
                onChange={e => setFilter(e.target.value)}
                placeholder="Filter…"
                className="bg-secondary border border-white/10 rounded px-2 py-1 text-xs font-mono-data text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-electric-blue w-32"
              />
            </div>
            {filtered.length === 0 ? (
              <p className="text-muted-foreground text-sm text-center py-8">{trades.length === 0 ? 'No trades recorded yet' : 'No trades match filter'}</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-white/10">
                      {['Date', 'Strategy', 'Expiry', 'Entry', 'Exit', 'P&L', 'Result', 'Outcome'].map(h => (
                        <th key={h} className="py-2 text-header font-normal text-left whitespace-nowrap">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((t, i) => {
                      const outcomeColor: Record<string, string> = {
                        'SKILL_WIN':    'text-neon-green',
                        'LUCKY_WIN':    'text-yellow-400',
                        'UNLUCKY_LOSS': 'text-electric-blue',
                        'SKILL_LOSS':   'text-signal-red',
                      }
                      const outcomeIcon: Record<string, string> = {
                        'SKILL_WIN':    '✦',
                        'LUCKY_WIN':    '◈',
                        'UNLUCKY_LOSS': '⊘',
                        'SKILL_LOSS':   '✕',
                      }
                      const oc = t.trade_outcome_class ?? ''
                      return (
                        <tr key={t.id ?? i} className={`border-b border-white/5 hover:bg-white/2 transition-colors ${t.is_mock ? 'opacity-80' : ''}`}>
                          <td className="py-2.5 font-mono-data text-muted-foreground">
                            {t.date}
                            {t.is_mock && <span className="ml-1 text-[8px] text-amber-400/70">⚠</span>}
                          </td>
                          <td className="py-2.5 text-foreground font-medium">{t.strategy}</td>
                          <td className="py-2.5 text-muted-foreground">{t.expiry_type ?? '—'}</td>
                          <td className="py-2.5 font-mono-data text-muted-foreground">{t.entry ? `₹${t.entry}` : '—'}</td>
                          <td className="py-2.5 font-mono-data text-muted-foreground">{t.exit ? `₹${t.exit}` : '—'}</td>
                          <td className={`py-2.5 font-mono-data font-bold ${t.pnl >= 0 ? 'text-neon-green' : 'text-signal-red'}`}>
                            {t.pnl >= 0 ? '+' : ''}₹{Math.abs(t.pnl).toLocaleString('en-IN')}
                          </td>
                          <td className={`py-2.5 font-mono-data font-semibold text-[10px] ${t.result === 'WIN' ? 'text-neon-green' : t.result === 'LOSS' ? 'text-signal-red' : 'text-muted-foreground'}`}>
                            {t.result}
                          </td>
                          <td className={`py-2.5 font-mono-data text-[10px] font-semibold whitespace-nowrap ${outcomeColor[oc] ?? 'text-muted-foreground'}`}>
                            {oc ? `${outcomeIcon[oc] ?? '—'} ${oc.replace('_', ' ')}` : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {Object.keys(dailyPnl).length > 0 && (
            <div className="glass-card p-4">
              <p className="text-[10px] text-header mb-3">DAILY P&L CALENDAR</p>
              <div className="grid grid-cols-5 md:grid-cols-7 gap-2">
                {Object.entries(dailyPnl).sort(([a], [b]) => a.localeCompare(b)).map(([date, pnl]) => {
                  const intensity = Math.min(Math.abs(pnl) / 8000, 1)
                  const bg = pnl >= 0
                    ? `rgba(34,197,94,${0.15 + intensity * 0.5})`
                    : `rgba(239,68,68,${0.15 + intensity * 0.5})`
                  return (
                    <div key={date} className="glass-card p-2 text-center hover:border-electric-blue/40 transition-colors cursor-default" style={{ background: bg }}>
                      <p className="text-[9px] text-muted-foreground">{date.slice(5)}</p>
                      <p className={`font-mono-data text-xs font-bold ${pnl >= 0 ? 'text-neon-green' : 'text-signal-red'}`}>
                        {pnl >= 0 ? '+' : ''}₹{(Math.abs(pnl) / 1000).toFixed(1)}K
                      </p>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </>
      )}

      {activeSection === 'coach' && (
        <div className="space-y-4">
          <div className="glass-card p-4 border border-electric-blue/20">
            <div className="flex items-center justify-between mb-1">
              <p className="text-[10px] text-header">🧠 JOURNAL COACH</p>
              {hasMockData && (
                <span className="text-[9px] text-amber-400 bg-amber-400/10 border border-amber-400/30 px-2 py-0.5 rounded font-mono-data">
                  {allMock ? '🎭 DEMO DATA' : '🎭 MIXED DATA'}
                </span>
              )}
            </div>
            <p className="text-xs text-muted-foreground">
              Reads your full trade history — strategy, context at entry, greek attribution, exit reason — and answers with data-driven precision.
              {hasMockData && <span className="text-amber-400/70"> Demo data is realistic; coaching is fully applicable.</span>}
            </p>
          </div>

          {coachMessages.length === 0 && (
            <div className="glass-card p-4">
              <p className="text-[9px] text-header mb-3">SUGGESTED QUESTIONS</p>
              <div className="flex flex-wrap gap-2">
                {SUGGESTED_QUESTIONS.map(q => (
                  <button
                    key={q}
                    onClick={() => askCoach(q)}
                    disabled={coachLoading}
                    className="text-xs bg-secondary border border-white/10 hover:border-electric-blue/40 rounded px-3 py-1.5 text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}

          {coachMessages.length > 0 && (
            <div className="glass-card p-4 space-y-4 max-h-[60vh] overflow-y-auto">
              {coachMessages.map((msg, i) => (
                <div key={i} className={`flex flex-col gap-1 ${msg.role === 'user' ? 'items-end' : 'items-start'}`}>
                  <div className={`flex items-center gap-2 text-[9px] text-muted-foreground ${msg.role === 'user' ? 'flex-row-reverse' : ''}`}>
                    <span>{msg.role === 'user' ? 'YOU' : '🧠 COACH'}</span>
                    <span>·</span>
                    <span>{msg.timestamp}</span>
                    {msg.role === 'coach' && msg.llmProvider && (
                      <span className={`px-1.5 py-0.5 rounded text-[8px] font-mono-data border ${
                        msg.llmProvider === 'claude'
                          ? 'bg-electric-blue/10 text-electric-blue border-electric-blue/30'
                          : 'bg-purple-500/10 text-purple-400 border-purple-500/30'
                      }`}>
                        {msg.llmProvider === 'claude' ? '✦ Claude' : '◈ Groq'}
                      </span>
                    )}
                    {msg.role === 'coach' && msg.hasMockData && (
                      <span className="text-[8px] text-amber-400/60 font-mono-data">🎭 demo</span>
                    )}
                  </div>
                  <div className={`max-w-[85%] rounded-lg px-4 py-3 text-xs leading-relaxed whitespace-pre-wrap ${
                    msg.role === 'user'
                      ? 'bg-electric-blue/20 border border-electric-blue/30 text-foreground'
                      : 'bg-secondary border border-white/10 text-foreground'
                  }`}>
                    {msg.content}
                  </div>
                </div>
              ))}
              {coachLoading && (
                <div className="flex items-start gap-2">
                  <div className="text-[9px] text-muted-foreground">🧠 COACH</div>
                  <div className="bg-secondary border border-white/10 rounded-lg px-4 py-3">
                    <div className="flex gap-1">
                      {[0, 1, 2].map(i => (
                        <div key={i} className="w-1.5 h-1.5 bg-electric-blue rounded-full animate-bounce" style={{ animationDelay: `${i * 0.15}s` }} />
                      ))}
                    </div>
                  </div>
                </div>
              )}
              <div ref={messagesEndRef} />
            </div>
          )}

          {coachError && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
              <p className="text-red-400 text-xs">⚠ {coachError}</p>
            </div>
          )}

          <div className="glass-card p-3 flex gap-2">
            <input
              value={coachInput}
              onChange={e => setCoachInput(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); askCoach(coachInput) } }}
              placeholder="Ask about your trading… (Enter to send)"
              disabled={coachLoading}
              className="flex-1 bg-transparent border-none outline-none text-xs text-foreground placeholder:text-muted-foreground disabled:opacity-50"
            />
            <button
              onClick={() => askCoach(coachInput)}
              disabled={coachLoading || !coachInput.trim()}
              className="px-4 py-1.5 bg-electric-blue text-black text-xs font-semibold rounded hover:bg-electric-blue/90 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {coachLoading ? '…' : 'Ask'}
            </button>
          </div>

          {coachMessages.length > 0 && (
            <button
              onClick={() => { setCoachMessages([]); setCoachError('') }}
              className="text-[10px] text-muted-foreground hover:text-foreground transition-colors"
            >
              Clear conversation
            </button>
          )}
        </div>
      )}
    </div>
  )
}
