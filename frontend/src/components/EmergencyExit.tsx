import { useState } from 'react'
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog'
import { emergencyExitAll } from '@/lib/api'
import { toast } from 'sonner'

interface Props {
  hasPositions?: boolean
}

export function EmergencyExit({ hasPositions = false }: Props) {
  const [loading, setLoading] = useState(false)

  const handle = async () => {
    setLoading(true)
    try {
      const r = await emergencyExitAll()
      if (r.success) toast.success(`Emergency exit executed — ${r.orders_placed} orders placed`)
      else toast.error(`Exit failed: ${r.message}`)
    } catch { toast.error('Emergency exit failed — check broker terminal') }
    finally { setLoading(false) }
  }

  return (
    <AlertDialog>
      <AlertDialogTrigger asChild>
        <button
          disabled={loading}
          className={`text-white font-bold uppercase tracking-wider px-4 py-2 rounded-md text-sm transition-all disabled:opacity-50 border
            ${hasPositions
              ? 'bg-signal-red hover:bg-signal-red/80 border-signal-red/50 animate-pulse'
              : 'bg-signal-red/60 hover:bg-signal-red/80 border-signal-red/30'
            }`}
        >
          {loading ? 'EXECUTING…' : '🚨 EMERGENCY EXIT'}
        </button>
      </AlertDialogTrigger>
      <AlertDialogContent className="bg-card border-signal-red/50">
        <AlertDialogHeader>
          <AlertDialogTitle className="text-signal-red text-lg">⚠️ CONFIRM EMERGENCY EXIT</AlertDialogTitle>
          <AlertDialogDescription className="text-muted-foreground">
            This will immediately place MARKET orders to close ALL positions. This action cannot be undone and may result in significant slippage.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel className="bg-secondary text-foreground hover:bg-secondary/80">Cancel</AlertDialogCancel>
          <AlertDialogAction onClick={handle} className="bg-signal-red hover:bg-signal-red/80 text-white font-bold">
            CONFIRM EXIT ALL
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}