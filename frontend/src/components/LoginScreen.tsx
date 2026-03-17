import { useState } from 'react'

interface Props { onLogin: (token: string) => void }

export function LoginScreen({ onLogin }: Props) {
  const [token, setToken] = useState('')
  const [show, setShow] = useState(false)
  const [err, setErr] = useState('')

  const submit = () => {
    if (token.trim().length > 0 && token.trim().length < 20) {
      setErr('Token too short — check and try again.'); return
    }
    setErr('')
    onLogin(token.trim())
  }

  return (
    <div className="min-h-screen bg-black flex items-center justify-center p-4">
      <div className="w-full max-w-md space-y-8">
        {/* Logo */}
        <div className="text-center space-y-2">
          <div className="flex items-center justify-center gap-2 mb-4">
            <div className="w-8 h-8 rounded-full bg-electric-blue/20 border border-electric-blue/40 flex items-center justify-center">
              <span className="text-electric-blue text-xs font-black">VG</span>
            </div>
          </div>
          <h1 className="text-3xl font-black tracking-widest uppercase text-white">
            Vol<span className="text-electric-blue">Guard</span>
          </h1>
          <p className="text-xs text-muted-foreground font-mono-data tracking-widest">V5 INTELLIGENCE TERMINAL</p>
          <div className="flex items-center justify-center gap-2 mt-1">
            <span className="w-1.5 h-1.5 rounded-full bg-neon-green pulse-green" />
            <span className="text-[10px] text-neon-green font-mono-data">SYSTEM ONLINE</span>
          </div>
        </div>

        {/* Card */}
        <div className="glass-card p-8 space-y-6">
          {err && (
            <div className="bg-signal-red/10 border border-signal-red/30 rounded p-3">
              <p className="text-signal-red text-xs">{err}</p>
            </div>
          )}

          <div className="space-y-2">
            <label className="text-[10px] text-header block">UPSTOX API TOKEN</label>
            <div className="relative">
              <input
                type={show ? 'text' : 'password'}
                value={token}
                onChange={e => { setToken(e.target.value); setErr('') }}
                onKeyDown={e => e.key === 'Enter' && submit()}
                placeholder="Paste your daily token here..."
                className="w-full bg-secondary border border-white/10 rounded px-3 py-2.5 font-mono-data text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-electric-blue"
              />
              <button
                type="button"
                onClick={() => setShow(s => !s)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground text-xs"
              >{show ? 'Hide' : 'Show'}</button>
            </div>
            <p className="text-[10px] text-muted-foreground">Expires daily at 03:30 AM IST</p>
          </div>

          <button
            onClick={submit}
            className="w-full bg-electric-blue hover:bg-electric-blue/80 text-white font-bold uppercase tracking-widest py-2.5 rounded-md transition-all text-sm"
          >
            Connect Terminal
          </button>

          <p className="text-center text-[10px] text-muted-foreground">
            Leave blank to enter demo mode with mock data
          </p>
        </div>

        {/* Status strip */}
        <div className="grid grid-cols-3 gap-2 text-center">
          {[
            { label: 'AI ENGINE', value: 'V5 GROQ', color: 'text-neon-green' },
            { label: 'QUANT ENGINE', value: 'V4 ACTIVE', color: 'text-electric-blue' },
            { label: 'AGENTS', value: '3 ONLINE', color: 'text-neon-green' },
          ].map(s => (
            <div key={s.label} className="glass-card p-2">
              <p className="text-[8px] text-header">{s.label}</p>
              <p className={`font-mono-data text-[10px] font-bold ${s.color}`}>{s.value}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}