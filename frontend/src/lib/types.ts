// ─── Dashboard Types ────────────────────────────────────────────────────────

export interface StructureMetrics {
  net_gex_formatted: string
  weighted_gex_formatted: string
  gex_regime: string
  gex_ratio_pct: string
  pcr_all: number
  pcr_atm: number
  max_pain: number
  skew_25d: string
  skew_regime: string
}

export interface EdgeMetrics {
  atm_iv: string
  vrp_vs_rv: string
  vrp_vs_garch: string
  vrp_vs_parkinson: string
  weighted_vrp: string
  weighted_vrp_tag: string
}

export interface RegimeComponent {
  score: number
  weight?: string   // e.g. "45%"
  signal?: string   // e.g. "NEUTRAL", "HIGH_VOL"
}

export interface RegimeScoreDetail {
  composite: { score: number; confidence: string; stability: number }
  weights: { volatility: number; structure: number; edge: number; rationale: string }
  weight_rationale?: string
  score_stability?: string
  components: {
    volatility: RegimeComponent
    structure: RegimeComponent
    edge: RegimeComponent
  }
  score_drivers: string[]
}

export interface MandateDetail {
  trade_status: 'ALLOWED' | 'BLOCKED'
  strategy: string
  directional_bias: string
  square_off_instruction?: string
  capital: { deployment_formatted: string; allocation_pct: number }
  rationale: string[]
  warnings: Array<{ type: string; message: string; severity: string }>
}

export interface FiiDetail {
  direction: string
  conviction: string
  flow_regime: string
  net_change: number
  net_change_formatted: string
  data_date: string
}

export interface ParticipantPositions {
  fii_summary: FiiDetail
  participants?: Record<string, Record<string, number>>
}

export interface ProfessionalDashboard {
  timestamp: string
  time_context: {
    status: string
    weekly_expiry: { date: string; dte: number }
    monthly_expiry: { date: string; dte: number }
    next_weekly_expiry: { date: string; dte: number }
  }
  economic_calendar: {
    veto_events: Array<{ event_name: string; time: string; square_off_by?: string; action_required: string }>
    other_events: Array<{ event_name: string; impact: string; days_until?: number }>
  }
  volatility_analysis: {
    spot: number; spot_ma20: number
    vix: number; vix_trend: string
    ivp_30d: number; ivp_90d: number; ivp_1y: number
    rv_7d: number; rv_28d: number; rv_90d: number
    garch_7d: number; garch_28d: number
    parkinson_7d: number; parkinson_28d: number
    vov: number; vov_zscore: number
    trend_strength: number
  }
  participant_positions: ParticipantPositions
  structure_analysis: {
    weekly: StructureMetrics
    next_weekly: StructureMetrics
    monthly: StructureMetrics
  }
  option_edges: {
    weekly: EdgeMetrics
    next_weekly: EdgeMetrics
    monthly: EdgeMetrics
    term_spread_pct: string
    primary_edge: string
  }
  regime_scores: {
    weekly: RegimeScoreDetail
    next_weekly: RegimeScoreDetail
    monthly: RegimeScoreDetail
  }
  mandates: {
    weekly: MandateDetail
    next_weekly: MandateDetail
    monthly: MandateDetail
  }
  professional_recommendation: {
    primary: { expiry_type: string; strategy: string; capital_deploy_formatted: string }
  }
  _fallback?: boolean
  _message?: string
}

// ─── Live / Position Types ──────────────────────────────────────────────────

export interface LivePosition {
  symbol: string
  qty: number
  ltp: number
  pnl: number
  avg_price: number
}

export interface ActiveStrategyLeg {
  symbol: string
  action: 'BUY' | 'SELL'
  option_type: string
  strike: number
  qty: number
  entry_price: number
  ltp: number
  pnl: number
}

export interface ActiveStrategy {
  strategy_id: string
  mock?: boolean
  strategy_type: string
  expiry_type: string
  expiry_date: string
  entry_time: string | null
  max_profit: number
  max_loss: number
  allocated_capital: number
  pnl: number
  legs: ActiveStrategyLeg[]
}

export interface LiveData {
  mtm_pnl: number
  mock?: boolean
  greeks: { delta: number; theta: number; vega: number; gamma: number }
  positions: LivePosition[]
  active_strategies?: ActiveStrategy[]
}

// ─── Journal Types ──────────────────────────────────────────────────────────

export interface TradeEntry {
  id?: string
  date: string
  strategy: string
  entry?: string
  exit?: string
  expiry_type?: string
  result: string
  pnl: number
  exit_reason: string
  is_mock?: boolean
  trade_outcome_class?: string
}

// ─── System Types ────────────────────────────────────────────────────────────

export interface SystemConfig {
  MAX_LOSS_PCT: number
  PROFIT_TARGET: number
  AUTO_TRADING: boolean
  GTT_STOP_LOSS_MULTIPLIER: number
  GTT_PROFIT_TARGET_MULTIPLIER: number
  GTT_TRAILING_GAP: number
  BASE_CAPITAL: number
  [key: string]: unknown
}

export interface HealthData {
  status: string
  database: boolean
  daily_cache: string
  circuit_breaker: string
  auto_trading: boolean
  mock_trading: boolean
  websocket: {
    market_streamer: string
    portfolio_streamer: string
    subscribed_instruments: number
  }
  gtt_config: {
    stop_loss_multiplier: number
    profit_target_multiplier: number
    trailing_gap: number
  }
  analytics_cache_age: number | string
  timestamp: string
}

export interface ReconcileData {
  reconciled: boolean
  db_positions: number
  broker_positions: number
  discrepancies: unknown[]
}

export interface FillQualityData {
  total_fills: number
  avg_slippage_pct: number
  max_slippage_pct: number
  avg_time_to_fill: number
  partial_fills: number
}

export interface GTTOrder {
  gtt_id: string
  instrument_token: string
  trading_symbol: string
  type: string
  status: string
  rules: unknown[]
}

export interface PnLAttribution {
  total_pnl: number
  theta_pnl: number
  vega_pnl: number
  delta_pnl: number
  other_pnl: number
  iv_change: number
}

// ─── Intelligence Types ───────────────────────────────────────────────────────

export interface V5BriefData {
  global_tone: string
  us_session_summary: string
  asian_session: string
  gift_nifty_signal: string
  macro_gauges: string
  crypto_signal: string
  cross_asset_coherence: string
  key_risks_today: string
  volguard_implication: string
  ok: boolean
}

export interface V5BriefResponse {
  available: boolean
  generated_at: string | null
  data?: V5BriefData
  message?: string
}

export interface V5NewsItem {
  title: string
  source: string
  keywords: string[]
}

export interface V5NewsResponse {
  timestamp: string
  has_veto: boolean
  has_high_impact: boolean
  veto_items: V5NewsItem[]
  high_impact_items: V5NewsItem[]
  watch_items: Array<{ title: string; source: string }>
  total_scanned: number
  fetch_errors: number
}

export interface V5VetoRecord {
  timestamp: string
  strategy: string
  expiry_type: string
  expiry_date: string
  regime_score: number
  recommendation: 'VETO' | 'PROCEED' | 'PROCEED_WITH_CAUTION'
  veto_reason: string
  rationale: string
  adjustments: string[]
  overridden: boolean
  override_reason?: string
  override_time?: string
}

export interface V5VetoLogResponse {
  veto_log: V5VetoRecord[]
  total_in_memory: number
}

// V5Alert matches backend _alert_rec structure (fixed in backend to use alert_level/suggested_action)
export interface V5Alert {
  timestamp: string
  alert_level: string
  what_changed: string
  why_it_matters: string
  suggested_action: string
  triggers?: unknown[]
}

export interface V5AlertsResponse {
  alerts: V5Alert[]
  timestamp: string
}

// V5MacroSnapshot matches the flattened response from fixed backend endpoint
export interface V5MacroSnapshot {
  timestamp: string
  global_tone: string
  risk_off_signals: number
  risk_on_signals: number
  us_10y_elevated: boolean
  // Flat fields (provided by fixed endpoint)
  dxy_level?: number | null
  dxy_direction?: string | null
  dxy_change_pct?: number | null
  us_10y_yield?: number | null
  vix_futures?: number | null
  vix_sentiment?: string | null
  crude_price?: number | null
  crude_direction?: string | null
  gold_price?: number | null
  gold_sentiment?: string | null
  btc_price?: number | null
  btc_direction?: string | null
  sgx_nifty?: number | null
  sgx_signal?: string | null
  // Nested full data
  assets?: Record<string, unknown>
}

export interface V5GlobalTone {
  global_tone: string
  source: string
  volguard_implication?: string
  generated_at?: string | null
  risk_off_signals?: number
  risk_on_signals?: number
}

export interface V5Status {
  system: string
  version: string
  intelligence_layer: string
  // Matches backend v5_status() dependencies dict exactly.
  // yfinance was removed from the project and replaced with Twelve Data + FRED.
  dependencies: {
    anthropic: boolean
    groq: boolean
    twelve_data: boolean
    fred: boolean
    feedparser: boolean
    coingecko: boolean
  }
  morning_brief: {
    status: string
    global_tone: string
    generated_at: string | null
  }
  agents: {
    morning_brief: string
    pretrade_context: string
    monitor: string
  }
}

export interface V5LLMUsage {
  provider?: string
  model?: string
  total_calls: number
  total_input_tokens: number
  total_output_tokens: number
  estimated_cost_usd: number
  calls_by_agent?: Record<string, number>
  error?: string
}
