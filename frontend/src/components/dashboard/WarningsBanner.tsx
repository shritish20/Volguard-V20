import { useState, useEffect } from 'react'
import { X, AlertTriangle, AlertOctagon, ShieldAlert, Info } from 'lucide-react'

interface Warning { type: string; message: string; severity: string }
interface Props { warnings?: Warning[]; isUsingFallback?: boolean }

function wKey(w: Warning) { return `${w.type}||${w.message}` }

// ── Severity config ────────────────────────────────────────────────────────
// Four VoV bands aligned with backend thresholds:
//   CRITICAL  VoV ≥ 3.0σ  → BLOCKED              (red, pulsing, no dismiss)
//   DANGER    VoV ≥ 2.75σ → Reduce size to 40%    (orange)
//   HIGH      VoV ≥ 2.50σ → Reduce size to 60%    (amber)
//   MEDIUM    VoV ≥ 2.25σ → Reduce size to 80%    (yellow, auto-dismiss 30s)
//   INFO      everything else                       (blue, auto-dismiss 30s)
type SevKey = 'CRITICAL' | 'DANGER' | 'HIGH' | 'MEDIUM' | 'INFO'
const SEV: Record<SevKey, { bg: string; border: string; text: string; label: string; pulse: boolean; autoDismissMs: number | null }> = {
  CRITICAL: { bg: 'bg-red-600/20',    border: 'border-red-500/60',    text: 'text-red-400',    label: '🚨 CRITICAL', pulse: true,  autoDismissMs: null  },
  DANGER:   { bg: 'bg-orange-500/15', border: 'border-orange-500/50', text: 'text-orange-400', label: '🔴 DANGER',   pulse: false, autoDismissMs: null  },
  HIGH:     { bg: 'bg-amber-500/15',  border: 'border-amber-500/40',  text: 'text-amber-400',  label: '🟠 ELEVATED', pulse: false, autoDismissMs: null  },
  MEDIUM:   { bg: 'bg-yellow-500/15', border: 'border-yellow-500/40', text: 'text-yellow-400', label: '⚠️ WARNING',  pulse: false, autoDismissMs: 30000 },
  INFO:     { bg: 'bg-blue-500/10',   border: 'border-blue-500/30',   text: 'text-blue-400',   label: 'ℹ️ INFO',     pulse: false, autoDismissMs: 30000 },
}
const SEV_ORDER: Record<string, number> = { CRITICAL: 0, DANGER: 1, HIGH: 2, MEDIUM: 3, INFO: 4 }

function cfg(severity: string) { return SEV[(severity as SevKey)] ?? SEV['INFO'] }

export function WarningsBanner({ warnings = [], isUsingFallback = false }: Props) {
  const [dismissed, setDismissed] = useState<Set<string>>(new Set())

  const unique = warnings.filter(
    (w, i, arr) => arr.findIndex(x => x.message === w.message) === i
  )

  useEffect(() => {
    const timers = unique
      .map(w => {
        const ms = cfg(w.severity).autoDismissMs
        if (!ms) return null
        return setTimeout(() => setDismissed(prev => new Set([...prev, wKey(w)])), ms)
      })
      .filter(Boolean) as ReturnType<typeof setTimeout>[]
    return () => timers.forEach(clearTimeout)
  }, [warnings]) // eslint-disable-line react-hooks/exhaustive-deps

  const active = unique
    .filter(w => !dismissed.has(wKey(w)))
    .sort((a, b) => (SEV_ORDER[a.severity] ?? 99) - (SEV_ORDER[b.severity] ?? 99))

  if (active.length === 0 && !isUsingFallback) return null

  return (
    <div className="space-y-2">
      {isUsingFallback && (
        <div className="flex items-center gap-3 rounded-lg p-3 border bg-yellow-500/10 border-yellow-500/30">
          <AlertTriangle className="h-3.5 w-3.5 text-yellow-400 flex-shrink-0" />
          <span className="text-xs text-yellow-400 font-semibold">
            FALLBACK DATA — live market data unavailable, showing last cached snapshot
          </span>
        </div>
      )}

      {active.map((w) => {
        const c = cfg(w.severity)
        const Icon = w.severity === 'CRITICAL' ? AlertOctagon
                   : w.severity === 'DANGER'   ? ShieldAlert
                   : AlertTriangle
        return (
          <div
            key={wKey(w)}
            className={`flex items-start gap-3 rounded-lg p-3 border ${c.bg} ${c.border} ${c.pulse ? 'animate-pulse' : ''}`}
          >
            <Icon className={`h-3.5 w-3.5 mt-0.5 flex-shrink-0 ${c.text}`} />
            <div className="flex-1 min-w-0">
              <span className={`text-[10px] font-black uppercase tracking-wider ${c.text}`}>{c.label}: </span>
              <span className={`text-xs ${c.text}`}>{w.message}</span>
            </div>
            {w.severity !== 'CRITICAL' && (
              <button
                onClick={() => setDismissed(prev => new Set([...prev, wKey(w)]))}
                className="opacity-50 hover:opacity-100 flex-shrink-0 transition-opacity"
              >
                <X className="h-3 w-3" />
              </button>
            )}
          </div>
        )
      })}
    </div>
  )
}
