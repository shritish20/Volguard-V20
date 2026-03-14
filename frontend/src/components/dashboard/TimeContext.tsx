import type { ProfessionalDashboard } from '@/lib/types'

export function TimeContext({ data }: { data: ProfessionalDashboard['time_context'] & { timestamp: string } }) {
  const statusColor = data.status === 'MARKET_OPEN' ? 'text-neon-green' : data.status === 'PRE_MARKET' ? 'text-yellow-400' : 'text-muted-foreground'
  const cards = [
    { label: 'MARKET STATUS', value: data.status.replace('_', ' '), sub: new Date(data.timestamp).toLocaleTimeString('en-IN', { hour12: false }), color: statusColor },
    { label: 'WEEKLY EXPIRY', value: data.weekly_expiry.date, sub: `DTE: ${data.weekly_expiry.dte}`, color: data.weekly_expiry.dte <= 1 ? 'text-signal-red' : data.weekly_expiry.dte <= 3 ? 'text-yellow-400' : 'text-electric-blue' },
    { label: 'NEXT WEEKLY', value: data.next_weekly_expiry.date, sub: `DTE: ${data.next_weekly_expiry.dte}`, color: 'text-electric-blue' },
    { label: 'MONTHLY EXPIRY', value: data.monthly_expiry.date, sub: `DTE: ${data.monthly_expiry.dte}`, color: 'text-muted-foreground' },
  ]
  return (
    <section className="grid grid-cols-2 lg:grid-cols-4 gap-3">
      {cards.map(c => (
        <div key={c.label} className="glass-card p-4 space-y-1.5">
          <p className="text-[10px] text-header">{c.label}</p>
          <p className={`font-mono-data text-base font-bold ${c.color}`}>{c.value}</p>
          <p className={`text-xs font-mono-data ${c.color} opacity-70`}>{c.sub}</p>
        </div>
      ))}
    </section>
  )
}