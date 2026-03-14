import { useState, useEffect } from 'react'
import { fetchGTTList, cancelGTT } from '@/lib/api'
import { toast } from 'sonner'
import type { GTTOrder } from '@/lib/types'

export function GTTManager() {
  const [orders, setOrders] = useState<GTTOrder[]>([])
  const [loading, setLoading] = useState(true)

  const load = async () => {
    try { setOrders(await fetchGTTList()) } catch {}
    setLoading(false)
  }

  useEffect(() => {
    load()
    const iv = setInterval(load, 10000)
    return () => clearInterval(iv)
  }, [])

  const cancel = async (id: string) => {
    try {
      await cancelGTT(id)
      toast.success('GTT cancelled', { description: `Order ${id} removed` })
      load()
    } catch { toast.error('Failed to cancel GTT') }
  }

  if (loading || orders.length === 0) return null

  return (
    <div className="glass-card p-4">
      <div className="flex items-center justify-between mb-3">
        <p className="text-[10px] text-header">ACTIVE GTT TRIGGERS</p>
        <span className="text-[9px] bg-electric-blue/20 text-electric-blue px-2 py-0.5 rounded-full border border-electric-blue/30">TRAILING STOPS ACTIVE</span>
      </div>
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-white/10">
            {['Symbol', 'Type', 'Status', ''].map(h => <th key={h} className="py-1.5 text-header font-normal text-left">{h}</th>)}
          </tr>
        </thead>
        <tbody>
          {orders.map(o => (
            <tr key={o.gtt_id} className="border-b border-white/5">
              <td className="py-2 font-mono-data text-foreground">{o.trading_symbol || o.instrument_token}</td>
              <td className="py-2 font-mono-data text-muted-foreground">{o.type}</td>
              <td className="py-2 font-mono-data text-neon-green">{o.status}</td>
              <td className="py-2 text-right">
                <button onClick={() => cancel(o.gtt_id)} className="text-[10px] text-signal-red hover:text-signal-red/80 transition-colors">CANCEL</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}