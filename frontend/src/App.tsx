import { Toaster } from '@/components/ui/sonner'
import { WebSocketProvider } from '@/context/WebSocketContext'
import { Index } from '@/pages/Index'

export default function App() {
  return (
    <WebSocketProvider>
      <Index />
      <Toaster position="bottom-right" theme="dark" richColors />
    </WebSocketProvider>
  )
}
