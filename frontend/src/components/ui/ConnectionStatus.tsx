import { useState, useEffect } from 'react';
import { Wifi, WifiOff, RefreshCw } from 'lucide-react';
import { useWebSocketContext } from '@/context/WebSocketContext';
import { WebSocketManager } from '@/services/WebSocketManager';

interface ConnectionStatusProps {
  showDetails?: boolean;
  onReconnect?: () => void;
}

export function ConnectionStatus({ showDetails = false, onReconnect }: ConnectionStatusProps) {
  const { isConnected, reconnect, marketStatus, portfolioStatus } = useWebSocketContext();
  const [stats, setStats] = useState<Record<string, any>>({});
  const [showStats, setShowStats] = useState(false);

  // Refresh stats periodically
  useEffect(() => {
    if (!showDetails) return;

    const interval = setInterval(() => {
      setStats(WebSocketManager.getStats());
    }, 2000);

    return () => clearInterval(interval);
  }, [showDetails]);

  const handleReconnect = () => {
    reconnect();
    onReconnect?.();
  };

  // Simple version (just an icon)
  if (!showDetails) {
    return (
      <button
        onClick={handleReconnect}
        className="relative group"
        title={isConnected ? 'Connected' : 'Disconnected'}
      >
        {isConnected ? (
          <Wifi className="h-4 w-4 text-neon-green" />
        ) : (
          <WifiOff className="h-4 w-4 text-signal-red" />
        )}
        <span className="absolute -top-8 left-1/2 -translate-x-1/2 bg-black text-[8px] px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap">
          {isConnected ? 'Live Connection' : 'Disconnected - Click to reconnect'}
        </span>
      </button>
    );
  }

  // Detailed version
  return (
    <div className="glass-card p-3 text-[10px]">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className="text-header">CONNECTION STATUS</span>
          {isConnected ? (
            <span className="flex items-center gap-1 text-neon-green">
              <span className="w-1.5 h-1.5 rounded-full bg-neon-green pulse-green"></span>
              LIVE
            </span>
          ) : (
            <span className="flex items-center gap-1 text-signal-red">
              <span className="w-1.5 h-1.5 rounded-full bg-signal-red pulse-red"></span>
              OFFLINE
            </span>
          )}
        </div>
        <button
          onClick={handleReconnect}
          className="p-1 hover:bg-secondary rounded transition-colors"
          title="Reconnect"
        >
          <RefreshCw className={`h-3 w-3 ${!isConnected ? 'animate-spin' : ''}`} />
        </button>
      </div>

      <div className="space-y-1">
        <div className="flex justify-between">
          <span className="text-muted-foreground">Market Data:</span>
          <span className={`font-mono-data ${marketStatus === 'connected' ? 'text-neon-green' : 'text-signal-red'}`}>
            {marketStatus}
          </span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Portfolio:</span>
          <span className={`font-mono-data ${portfolioStatus === 'connected' ? 'text-neon-green' : 'text-signal-red'}`}>
            {portfolioStatus}
          </span>
        </div>
      </div>

      {showStats && Object.keys(stats).length > 0 && (
        <div className="mt-2 pt-2 border-t border-white/10">
          <p className="text-header mb-1">ACTIVE CONNECTIONS</p>
          {Object.entries(stats).map(([id, data]: [string, any]) => (
            <div key={id} className="flex justify-between text-[8px]">
              <span className="text-muted-foreground">{id.split('/').pop()}:</span>
              <span className={data.status === 'connected' ? 'text-neon-green' : 'text-signal-red'}>
                {data.status} ({data.listenerCount})
              </span>
            </div>
          ))}
        </div>
      )}

      <button
        onClick={() => setShowStats(!showStats)}
        className="w-full mt-2 text-[8px] text-muted-foreground hover:text-foreground transition-colors"
      >
        {showStats ? 'Hide Details' : 'Show Details'}
      </button>
    </div>
  );
}
