/**
 * InfoPopover — shared ⓘ tooltip for methodology transparency.
 *
 * Uses position:fixed + useRef to measure the button's screen position and
 * flip the popover left/right/up based on available viewport space.
 * This avoids the two bugs seen with position:absolute:
 *   1. Clipped by parent overflow:hidden (glass-card)
 *   2. Goes off the right/bottom edge of the screen
 */
import { useState, useRef, useEffect } from 'react'
import { Info, X } from 'lucide-react'
import { createPortal } from 'react-dom'

export interface MethodologyEntry {
  title: string
  what: string
  how: string
  window: string
  context?: string
}

interface InfoPopoverProps {
  entry: MethodologyEntry
}

const POPOVER_WIDTH  = 288   // w-72 = 18rem = 288px
const POPOVER_OFFSET = 8     // gap between button and popover

export function InfoPopover({ entry }: InfoPopoverProps) {
  const [open, setOpen]   = useState(false)
  const [pos,  setPos]    = useState<{ top: number; left: number }>({ top: 0, left: 0 })
  const btnRef            = useRef<HTMLButtonElement>(null)

  // Recalculate position whenever opened
  useEffect(() => {
    if (!open || !btnRef.current) return

    const rect   = btnRef.current.getBoundingClientRect()
    const vw     = window.innerWidth
    const vh     = window.innerHeight

    // Horizontal: prefer right of button; flip left if not enough space
    let left = rect.right + POPOVER_OFFSET
    if (left + POPOVER_WIDTH > vw - 8) {
      left = rect.left - POPOVER_WIDTH - POPOVER_OFFSET
    }
    // If still off-screen left, clamp to viewport edge
    if (left < 8) left = 8

    // Vertical: open downward from button; flip up if near bottom
    let top = rect.top
    // Rough popover height estimate — clamp if too close to bottom
    const estimatedHeight = 280
    if (top + estimatedHeight > vh - 8) {
      top = Math.max(8, vh - estimatedHeight - 8)
    }

    setPos({ top, left })
  }, [open])

  const popover = open ? (
    <>
      {/* Full-screen invisible backdrop */}
      <div
        className="fixed inset-0 z-[9998]"
        onClick={() => setOpen(false)}
      />
      {/* Popover — rendered in document.body via portal, fully outside card DOM */}
      <div
        style={{ position: 'fixed', top: pos.top, left: pos.left, width: POPOVER_WIDTH, zIndex: 9999 }}
        className="rounded-lg border border-electric-blue/30 bg-[#0d1117] shadow-2xl p-4 space-y-2.5"
      >
        {/* Header */}
        <div className="flex items-start justify-between gap-2">
          <span className="text-xs font-bold text-electric-blue uppercase tracking-wider leading-tight">
            {entry.title}
          </span>
          <button
            onClick={() => setOpen(false)}
            className="opacity-50 hover:opacity-100 flex-shrink-0 mt-0.5"
          >
            <X className="h-3 w-3 text-muted-foreground" />
          </button>
        </div>

        {/* What */}
        <p className="text-[11px] text-foreground leading-relaxed">{entry.what}</p>

        {/* How */}
        <div>
          <p className="text-[9px] text-header uppercase tracking-wider mb-0.5">How it's calculated</p>
          <p className="text-[11px] text-muted-foreground leading-relaxed">{entry.how}</p>
        </div>

        {/* Window */}
        <div>
          <p className="text-[9px] text-header uppercase tracking-wider mb-0.5">Data window</p>
          <p className="text-[11px] text-electric-blue font-mono-data">{entry.window}</p>
        </div>

        {/* Thresholds / context */}
        {entry.context && (
          <div className="border-t border-white/5 pt-2">
            <p className="text-[9px] text-header uppercase tracking-wider mb-0.5">Thresholds</p>
            <p className="text-[11px] text-yellow-400 leading-relaxed">{entry.context}</p>
          </div>
        )}
      </div>
    </>
  ) : null

  return (
    <span className="inline-flex items-center">
      <button
        ref={btnRef}
        onClick={e => { e.stopPropagation(); setOpen(v => !v) }}
        className="ml-1 opacity-40 hover:opacity-90 transition-opacity align-middle"
        title="Show methodology"
      >
        <Info className="h-3 w-3 text-electric-blue" />
      </button>
      {/* Portal: renders outside any parent overflow:hidden */}
      {popover && createPortal(popover, document.body)}
    </span>
  )
}
