import type { ProfessionalDashboard } from '@/lib/types'

export function FinalRecommendation({ data }: { data: ProfessionalDashboard['professional_recommendation'] }) {
  if (!data?.primary) return null
  return (
    <section className="glass-card glass-border-glow p-5">
      <p className="text-[10px] text-header mb-3">FINAL RECOMMENDATION</p>
      <div className="flex flex-wrap items-center gap-6">
        <div>
          <p className="text-[9px] text-muted-foreground mb-0.5">PRIMARY EXPIRY</p>
          <p className="font-mono-data text-2xl font-black text-electric-blue">{data.primary.expiry_type}</p>
        </div>
        <div>
          <p className="text-[9px] text-muted-foreground mb-0.5">STRATEGY</p>
          <p className="font-mono-data text-2xl font-black text-neon-green">{data.primary.strategy}</p>
        </div>
        <div>
          <p className="text-[9px] text-muted-foreground mb-0.5">CAPITAL DEPLOY</p>
          <p className="font-mono-data text-2xl font-black text-foreground">{data.primary.capital_deploy_formatted}</p>
        </div>
      </div>
    </section>
  )
}