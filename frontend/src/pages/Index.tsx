import { useAuth } from '@/hooks/useAuth'
import { LoginScreen } from '@/components/LoginScreen'
import { HudTopBar } from '@/components/HudTopBar'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { DashboardTab } from './DashboardTab'
import { LiveDeskTab } from './LiveDeskTab'
import { JournalTab } from './JournalTab'
import { IntelligenceTab } from './IntelligenceTab'
import { SystemTab } from './SystemTab'

const TABS = [
  { value: 'intel', label: 'V5 INTEL' },
  { value: 'market', label: 'MARKET' },
  { value: 'position', label: 'POSITIONS' },
  { value: 'log', label: 'LOG' },
  { value: 'system', label: 'SYSTEM' },
]

export function Index() {
  const { isAuthenticated, login, logout } = useAuth()
  if (!isAuthenticated) return <LoginScreen onLogin={login} />
  return (
    <div className="min-h-screen bg-background bg-radial-subtle">
      <HudTopBar onDisconnect={logout} />
      <main className="max-w-[1800px] mx-auto px-4 py-4">
        <Tabs defaultValue="intel">
          <TabsList className="w-full justify-start bg-card border border-white/10 mb-6 h-10">
            {TABS.map(t => (
              <TabsTrigger key={t.value} value={t.value}
                className="text-[10px] uppercase tracking-widest text-muted-foreground data-[state=active]:text-electric-blue data-[state=active]:bg-secondary font-semibold h-8 px-4"
              >{t.label}</TabsTrigger>
            ))}
          </TabsList>
          <TabsContent value="intel"><IntelligenceTab /></TabsContent>
          <TabsContent value="market"><DashboardTab /></TabsContent>
          <TabsContent value="position"><LiveDeskTab /></TabsContent>
          <TabsContent value="log"><JournalTab /></TabsContent>
          <TabsContent value="system"><SystemTab /></TabsContent>
        </Tabs>
      </main>
    </div>
  )
}