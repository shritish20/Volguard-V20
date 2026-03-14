import { InfoPopover } from './InfoPopover'
import type { ProfessionalDashboard } from '@/lib/types'

const M = {
  ivp: {
    title: 'IV Percentile (IVP)',
    what: "Where today's India VIX sits relative to its own history. 96% = VIX is higher than 96% of all days in the window.",
    how: "Today's VIX is ranked against the last N days of VIX closes (excluding today). IVP = % of historical days where VIX was lower.",
    window: '30D = last 30 trading days · 90D = last 90 · 1Yr = last 252 trading days.',
    context: '> 75% = RICH (good for premium sellers). < 25% = CHEAP (thin edge). Falls back to 50% (neutral) if insufficient history.',
  },
  vov: {
    title: 'VoV (Raw)',
    what: 'Raw Volatility-of-Volatility: the annualised standard deviation of daily VIX changes over the last 30 days.',
    how: 'Log-returns of India VIX (daily closes) → 30-day rolling std × √252 × 100.',
    window: '30 trading days of India VIX history.',
  },
  vov_zscore: {
    title: 'VoV Z-Score',
    what: "Measures how erratically the vol surface itself is moving. The primary regime stability gate in this system.",
    how: "30-day rolling VoV is z-scored against a 60-day rolling mean and std. India VIX is used (not ATM IV) because ATM IV spikes mechanically on expiry day (can hit 70%+), producing false alarms every Thursday.",
    window: '30-day rolling VoV · 60-day z-score baseline · ~280 trading days of VIX available.',
    context: '≥ 3.0σ = ALL TRADES BLOCKED · 2.75–2.99σ = Danger (40% size) · 2.50–2.74σ = Elevated (60%) · 2.25–2.49σ = Warning (80%)',
  },
  rv: {
    title: 'Realised Volatility (RV)',
    what: 'Historical (backward-looking) volatility of NIFTY price returns.',
    how: 'Log-returns of NIFTY daily closes. Rolling std (ddof=1) over N days, annualised with √252 × 100.',
    window: 'RV 7D = last 7 trading days · RV 28D = last 28 · RV 90D = last 90.',
  },
  garch: {
    title: 'GARCH 7D / 28D',
    what: 'Forward-looking vol forecast using GARCH(1,1) with Student-t distribution.',
    how: 'Fitted on ~280 days of NIFTY daily returns. Student-t handles fat tails (budget, RBI, elections) — normal dist underestimates by 15–25%. Uses terminal-day variance at horizon h. Fit once per trading day, cached intraday for consistency.',
    window: '~280 trading days of NIFTY returns · min 100 observations required to fit.',
    context: 'Falls back to RV7 / RV28 if GARCH fails. Gets 70% weight in Weighted VRP.',
  },
  parkinson: {
    title: 'Parkinson Volatility',
    what: 'High-Low range based vol estimator. More efficient than close-to-close RV — captures intraday moves.',
    how: '[ (1 / 4·ln2) × mean(ln(High/Low)²) ]^0.5 × √252 × 100.',
    window: 'Park 7D = last 7 daily candles · Park 28D = last 28.',
    context: 'Gets 15% weight in Weighted VRP alongside GARCH (70%) and RV (15%).',
  },
}

function Row({ label, value, color, info }: {
  label: string; value: string | number; color?: string; info?: keyof typeof M
}) {
  return (
    <div className="flex items-center justify-between py-1.5 border-b border-white/5 last:border-0">
      <span className="text-xs text-muted-foreground flex items-center gap-0.5">
        {label}
        {info && <InfoPopover entry={M[info]} />}
      </span>
      <span className={`font-mono-data text-sm font-bold ${color ?? 'text-foreground'}`}>{value}</span>
    </div>
  )
}

export function VolatilityMatrix({ data }: { data: ProfessionalDashboard['volatility_analysis'] }) {
  const spotDiff = data.spot - data.spot_ma20
  const ivpColor = (v: number) => v > 75 ? 'text-signal-red' : v > 50 ? 'text-yellow-400' : 'text-neon-green'

  return (
    <section>
      <h2 className="text-header text-xs mb-3">VOLATILITY MATRIX</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">

        <div className="glass-card p-4">
          <p className="text-[10px] text-header mb-2">PRICE & VIX</p>
          <Row label="Spot"      value={data.spot.toLocaleString('en-IN')} color={spotDiff >= 0 ? 'text-neon-green' : 'text-signal-red'} />
          <Row label="vs MA20"   value={`${spotDiff >= 0 ? '+' : ''}${spotDiff.toFixed(1)}`} color={spotDiff >= 0 ? 'text-neon-green' : 'text-signal-red'} />
          <Row label="VIX"       value={data.vix.toFixed(2)} color={data.vix > 20 ? 'text-signal-red' : 'text-neon-green'} />
          <Row label="VIX Trend" value={data.vix_trend} color={data.vix_trend === 'FALLING' ? 'text-neon-green' : data.vix_trend === 'STABLE' ? 'text-yellow-400' : 'text-signal-red'} />
          <Row label="Trend Str" value={`${(data.trend_strength * 100).toFixed(0)}%`} />
        </div>

        <div className="glass-card p-4">
          <p className="text-[10px] text-header mb-2">IV PERCENTILES</p>
          <Row label="30D IVP"    value={`${data.ivp_30d.toFixed(1)}%`}  color={ivpColor(data.ivp_30d)}  info="ivp" />
          <Row label="90D IVP"    value={`${data.ivp_90d.toFixed(1)}%`}  color={ivpColor(data.ivp_90d)} />
          <Row label="1Yr IVP"    value={`${data.ivp_1y.toFixed(1)}%`}   color={ivpColor(data.ivp_1y)} />
          <Row label="VoV"        value={data.vov.toFixed(3)}             info="vov" />
          <Row
            label="VoV Z-Score"
            value={data.vov_zscore.toFixed(2)}
            color={data.vov_zscore >= 3.0 ? 'text-red-400' : data.vov_zscore >= 2.75 ? 'text-orange-400' : data.vov_zscore >= 2.25 ? 'text-yellow-400' : 'text-foreground'}
            info="vov_zscore"
          />
        </div>

        <div className="glass-card p-4">
          <p className="text-[10px] text-header mb-2">REALIZED VOL</p>
          <Row label="RV 7D"     value={`${data.rv_7d.toFixed(2)}%`}    info="rv" />
          <Row label="RV 28D"    value={`${data.rv_28d.toFixed(2)}%`} />
          <Row label="RV 90D"    value={`${data.rv_90d.toFixed(2)}%`} />
          <Row label="GARCH 7D"  value={`${data.garch_7d.toFixed(2)}%`}  color="text-electric-blue" info="garch" />
          <Row label="GARCH 28D" value={`${data.garch_28d.toFixed(2)}%`} color="text-electric-blue" />
        </div>

        <div className="glass-card p-4">
          <p className="text-[10px] text-header mb-2">PARKINSON</p>
          <Row label="Park 7D"  value={`${data.parkinson_7d.toFixed(2)}%`}  info="parkinson" />
          <Row label="Park 28D" value={`${data.parkinson_28d.toFixed(2)}%`} />
        </div>

      </div>
    </section>
  )
}
