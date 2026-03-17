import axios, { AxiosInstance, AxiosRequestConfig, AxiosError } from 'axios'
import type {
  ProfessionalDashboard, LiveData, TradeEntry, SystemConfig,
  HealthData, ReconcileData, FillQualityData, GTTOrder, PnLAttribution,
  V5BriefResponse, V5NewsResponse, V5VetoLogResponse, V5AlertsResponse,
  V5MacroSnapshot, V5GlobalTone, V5Status, V5LLMUsage,
} from './types'

// VITE_API_BASE routing:
//   .env (Docker/cloud) = empty string  -- relative /api/... -- works on localhost AND any EC2 IP/domain
//   .env.development     = http://localhost:8000 -- direct to backend for npm run dev
// Empty baseURL = axios sends relative requests to whatever host served the HTML (always nginx).
const API_BASE = (import.meta.env.VITE_API_BASE as string || '').trim()
interface CacheEntry<T> { data: T; timestamp: number; ttl: number }

class ApiCache {
  private cache = new Map<string, CacheEntry<unknown>>()
  private pending = new Map<string, Promise<unknown>>()
  get<T>(key: string): T | null {
    const e = this.cache.get(key)
    if (!e) return null
    if (Date.now() - e.timestamp > e.ttl) { this.cache.delete(key); return null }
    return e.data as T
  }
  set<T>(key: string, data: T, ttl: number) { this.cache.set(key, { data, timestamp: Date.now(), ttl }) }
  getPending<T>(key: string): Promise<T> | undefined { return this.pending.get(key) as Promise<T> }
  setPending<T>(key: string, p: Promise<T>) { this.pending.set(key, p); p.finally(() => this.pending.delete(key)) }
  clear() { this.cache.clear() }
  clearKey(key: string) { this.cache.delete(key) }
}

const cache = new ApiCache()
const TTL = { DASHBOARD: 60000, LIVE: 2000, INTEL: 60000, NEWS: 30000, HEALTH: 15000 }

let isLoggingOut = false

const api: AxiosInstance = axios.create({
  baseURL: API_BASE,
  timeout: 15000,
  headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
})

api.interceptors.request.use((config) => {
  if (isLoggingOut) { const ctrl = new AbortController(); config.signal = ctrl.signal; ctrl.abort(); return config }
  const token = localStorage.getItem('upstox_token')
  if (token) config.headers['X-Upstox-Token'] = token
  return config
})

api.interceptors.response.use(
  (r) => r,
  async (err: AxiosError) => {
    if (axios.isCancel(err)) return Promise.reject(err)
    const cfg = err.config as AxiosRequestConfig & { _retry?: number }
    if (err.response?.status === 401 && !isLoggingOut) {
      isLoggingOut = true
      localStorage.removeItem('upstox_token')
      window.dispatchEvent(new CustomEvent('auth:logout'))
      setTimeout(() => { isLoggingOut = false }, 5000)
      return Promise.reject(err)
    }
    const retryable = [408, 429, 500, 502, 503, 504]
    if (cfg && retryable.includes(err.response?.status ?? 0)) {
      cfg._retry = cfg._retry ?? 0
      if (cfg._retry < 3) {
        cfg._retry++
        await new Promise(r => setTimeout(r, 1000 * Math.pow(2, cfg._retry! - 1)))
        return api(cfg)
      }
    }
    return Promise.reject(err)
  }
)

async function cached<T>(key: string, fn: () => Promise<T>, ttl: number, force = false): Promise<T> {
  if (!force) { const hit = cache.get<T>(key); if (hit) return hit }
  const pending = cache.getPending<T>(key); if (pending) return pending
  const promise = fn(); cache.setPending(key, promise)
  const data = await promise; cache.set(key, data, ttl); return data
}

export async function fetchDashboard(force = false): Promise<ProfessionalDashboard> {
  return cached('dashboard', async () => { const r = await api.get('/api/dashboard/professional'); return r.data }, TTL.DASHBOARD, force)
}
export async function fetchLivePositions(force = false): Promise<LiveData> {
  return cached('live', async () => { const r = await api.get('/api/live/positions'); return r.data }, TTL.LIVE, force)
}
export async function fetchBulkPrice(instruments: string[]): Promise<Record<string, number>> {
  const r = await api.get(`/api/market/bulk-last-price?instruments=${encodeURIComponent(instruments.join(','))}`)
  return r.data?.prices ?? {}
}
export async function fetchJournal(limit = 50): Promise<TradeEntry[]> {
  const r = await api.get(`/api/journal/history?limit=${limit}`); return r.data
}
export async function fetchCurrentConfig(): Promise<SystemConfig> {
  const r = await api.get('/api/system/config/current'); return r.data
}
export async function saveConfig(payload: Record<string, unknown>): Promise<unknown> {
  const r = await api.post('/api/system/config', payload); cache.clearKey('dashboard'); return r.data
}
export async function fetchLogs(lines = 50): Promise<Array<{ timestamp: string; level: string; message: string }>> {
  const r = await api.get(`/api/system/logs?lines=${lines}`); return r.data?.logs ?? []
}
export async function fetchHealth(force = false): Promise<HealthData> {
  return cached('health', async () => { const r = await api.get('/api/health'); return r.data }, TTL.HEALTH, force)
}
export async function fetchReconcile(): Promise<ReconcileData> {
  const r = await api.get('/api/positions/reconcile'); return r.data
}
export async function triggerReconcile(): Promise<{ success: boolean; message: string }> {
  const r = await api.post('/api/positions/reconcile/trigger'); return r.data
}
export async function fetchFillQuality(): Promise<FillQualityData | null> {
  try { const r = await api.get('/api/orders/fill-quality'); if (!r.data?.total_fills) return null; return r.data } catch { return null }
}
export async function fetchGTTList(): Promise<GTTOrder[]> {
  const r = await api.get('/api/gtt/list'); return r.data?.gtt_orders ?? []
}
export async function cancelGTT(gttId: string): Promise<unknown> {
  const r = await api.delete(`/api/gtt/${gttId}`); return r.data
}
export async function fetchPnLAttribution(): Promise<PnLAttribution | null> {
  try { const r = await api.get('/api/pnl/attribution'); if (!r.data || Object.keys(r.data).length === 0) return null; return r.data } catch { return null }
}
export async function emergencyExitAll(): Promise<{ success: boolean; orders_placed: number; message: string }> {
  const r = await api.post('/api/emergency/exit-all'); return r.data
}
// Intelligence Layer
export async function fetchV5Status(force = false): Promise<V5Status> {
  return cached('v5status', async () => { const r = await api.get('/api/v5/status'); return r.data }, TTL.INTEL, force)
}
export async function fetchMorningBrief(force = false): Promise<V5BriefResponse> {
  return cached('brief', async () => { const r = await api.get('/api/intelligence/brief'); return r.data }, TTL.INTEL, force)
}
export async function generateBrief(): Promise<{ status: string; message: string }> {
  cache.clearKey('brief'); const r = await api.post('/api/intelligence/brief/generate'); return r.data
}
export async function fetchGlobalTone(force = false): Promise<V5GlobalTone> {
  return cached('globaltone', async () => { const r = await api.get('/api/intelligence/global-tone'); return r.data }, TTL.INTEL, force)
}
export async function fetchMacroSnapshot(force = false): Promise<V5MacroSnapshot> {
  return cached('macro', async () => { const r = await api.get('/api/intelligence/macro-snapshot'); return r.data }, TTL.INTEL, force)
}
export async function fetchNews(force = false): Promise<V5NewsResponse> {
  return cached('news', async () => { const r = await api.get('/api/intelligence/news'); return r.data }, TTL.NEWS, force)
}
export async function fetchVetoLog(limit = 20): Promise<V5VetoLogResponse> {
  const r = await api.get(`/api/intelligence/veto-log?limit=${limit}`); return r.data
}
export async function overrideVeto(reason: string): Promise<{ success: boolean; message: string }> {
  const r = await api.post('/api/intelligence/override', { reason }); return r.data
}
export async function fetchAlerts(limit = 10): Promise<V5AlertsResponse> {
  const r = await api.get(`/api/intelligence/alerts?limit=${limit}`); return r.data
}
export async function triggerMonitorScan(): Promise<unknown> {
  const r = await api.post('/api/intelligence/monitor/scan'); return r.data
}
export async function fetchLLMUsage(): Promise<V5LLMUsage> {
  const r = await api.get('/api/intelligence/claude-usage'); return r.data
}
export function clearAllCache() { cache.clear() }
export default api
