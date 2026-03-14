import type { ProfessionalDashboard } from '@/lib/types'

export function MacroRisks({ data }: { data: ProfessionalDashboard['economic_calendar'] }) {
  return (
    <section>
      <h2 className="text-header text-xs mb-3">MACRO RISKS — ECONOMIC CALENDAR</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="space-y-2">
          <h3 className="text-[10px] text-signal-red uppercase tracking-widest font-semibold">Veto Events</h3>
          {data.veto_events.length === 0 ? (
            <div className="glass-card p-4 text-xs text-neon-green flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-neon-green" />
              No veto events — clear to trade
            </div>
          ) : (
            data.veto_events.map((e, i) => (
              <div key={i} className="glass-card veto-glow p-4 border-signal-red/50 space-y-1">
                <p className="text-sm font-bold text-signal-red">⚠️ {e.event_name}</p>
                <p className="text-xs text-muted-foreground font-mono-data">{e.time}</p>
                {e.square_off_by && <p className="text-xs text-signal-red font-mono-data">Square off by: {e.square_off_by}</p>}
                <p className="text-xs font-semibold text-signal-red uppercase tracking-wide">ACTION: {e.action_required}</p>
              </div>
            ))
          )}
        </div>
        <div className="space-y-2">
          <h3 className="text-[10px] text-electric-blue uppercase tracking-widest font-semibold">Awareness Events</h3>
          <div className="glass-card p-4 space-y-2">
            {data.other_events.length === 0 ? (
              <p className="text-xs text-muted-foreground">No upcoming events</p>
            ) : data.other_events.map((e, i) => (
              <div key={i} className="flex items-center justify-between text-xs border-b border-white/5 pb-1.5 last:border-0 last:pb-0">
                <span className="text-foreground">{e.event_name}</span>
                <div className="flex items-center gap-2">
                  {e.days_until !== undefined && <span className="text-muted-foreground font-mono-data text-[10px]">{e.days_until}d</span>}
                  <span className={`font-mono-data font-semibold ${e.impact === 'HIGH' ? 'text-signal-red' : e.impact === 'MEDIUM' ? 'text-yellow-400' : 'text-muted-foreground'}`}>{e.impact}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}