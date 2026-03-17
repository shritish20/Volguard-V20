import { useWebSocket } from './useWebSocket';
import { useCallback, useEffect, useRef, useState } from 'react';

// In Docker production, VITE_API_BASE is intentionally empty so the React app
// makes same-origin requests through the nginx proxy. But the native WebSocket()
// constructor requires an absolute ws:// URL — it cannot accept a relative path.
// Derive the correct absolute WS origin from window.location at runtime.
function getWsBase(): string {
  const configured = import.meta.env.VITE_API_BASE;
  if (configured) {
    // e.g. "http://localhost:8000" → "ws://localhost:8000"
    return configured.replace(/^http/, 'ws');
  }
  // Same-origin in production: derive from page origin and swap protocol
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${window.location.host}`;
}

export interface MarketUpdate {
  instrument_key: string;
  ltp: number;
  ltt?: number;
  ltq?: number;
  cp?: number;
  volume?: number;
  oi?: number;
  bid_price?: number;
  bid_qty?: number;
  ask_price?: number;
  ask_qty?: number;
  timestamp: string;
}

interface UseMarketWebSocketOptions {
  onPriceUpdate?: (instrumentKey: string, price: number) => void;
  onFullUpdate?: (update: MarketUpdate) => void;
  autoConnect?: boolean;
}

export function useMarketWebSocket(
  instrumentKeys: string[],
  options: UseMarketWebSocketOptions = {}
) {
  const [prices, setPrices] = useState<Record<string, number>>({});
  const [updates, setUpdates] = useState<Record<string, MarketUpdate>>({});
  const [isSubscribed, setIsSubscribed] = useState(false);

  // Connect to the subscribe endpoint, not /api/ws/market
  const socketUrl = `${getWsBase()}/api/ws/subscribe`;
  
  const { sendMessage, lastMessage, status, connect, disconnect } = useWebSocket(
    socketUrl,
    {
      onMessage: (data) => {
        if (data.type === 'market_update') {
          const update = data.data as MarketUpdate;
          
          setPrices(prev => ({ ...prev, [update.instrument_key]: update.ltp }));
          setUpdates(prev => ({ ...prev, [update.instrument_key]: update }));
          
          options.onPriceUpdate?.(update.instrument_key, update.ltp);
          options.onFullUpdate?.(update);
        } else if (data.type === 'subscription_result') {
          console.log('Subscription result:', data);
        }
      },
      autoConnect: options.autoConnect,
    }
  );

  // Keep a ref to the current status so the cleanup function always reads the
  // live value instead of the stale value captured at mount time.
  // Without this, status in the cleanup closure is always 'disconnected' and
  // unsubscribe() is never called, causing subscriptions to accumulate.
  const statusRef = useRef(status);
  useEffect(() => { statusRef.current = status; }, [status]);

  const subscribe = useCallback((keys: string[]) => {
    if (status === 'connected') {
      sendMessage({
        action: 'subscribe',
        instruments: keys,
        mode: 'ltpc'
      });
      setIsSubscribed(true);
    } else {
      console.warn('Cannot subscribe: WebSocket not connected');
    }
  }, [status, sendMessage]);

  const unsubscribe = useCallback((keys: string[]) => {
    if (status === 'connected') {
      sendMessage({
        action: 'unsubscribe',
        instruments: keys
      });
    }
  }, [status, sendMessage]);

  const changeMode = useCallback((keys: string[], mode: 'ltpc' | 'full' | 'option_greeks') => {
    if (status === 'connected') {
      sendMessage({
        action: 'change_mode',
        instruments: keys,
        mode
      });
    }
  }, [status, sendMessage]);

  // Auto-subscribe when connected
  useEffect(() => {
    if (status === 'connected' && instrumentKeys.length > 0 && !isSubscribed) {
      subscribe(instrumentKeys);
    }
  }, [status, instrumentKeys, subscribe, isSubscribed]);

  // Cleanup on unmount — use statusRef so we always read the current status,
  // not the stale value that was captured when the effect first ran.
  useEffect(() => {
    return () => {
      if (instrumentKeys.length > 0 && statusRef.current === 'connected') {
        sendMessage({
          action: 'unsubscribe',
          instruments: instrumentKeys
        });
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return {
    prices,
    updates,
    status,
    subscribe,
    unsubscribe,
    changeMode,
    isConnected: status === 'connected',
    connect,
    disconnect,
  };
}
