import { useWebSocket } from './useWebSocket';
import { useState } from 'react';
import type { LivePosition, LiveData } from '@/lib/types';

// In Docker production, VITE_API_BASE is intentionally empty (same-origin via nginx).
// The native WebSocket() constructor requires an absolute ws:// URL — derive it at runtime.
function getWsBase(): string {
  const configured = import.meta.env.VITE_API_BASE;
  if (configured) {
    return configured.replace(/^http/, 'ws');
  }
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${window.location.host}`;
}

export interface OrderUpdate {
  order_id: string;
  status: string;
  filled_quantity: number;
  average_price: number;
  instrument_token: string;
  transaction_type: string;
  quantity: number;
  timestamp: string;
}

export interface PositionUpdate {
  instrument_token: string;
  quantity: number;
  buy_price: number;
  current_price: number;
  pnl: number;
  product: string;
  timestamp: string;
}

interface UsePortfolioWebSocketOptions {
  onOrderUpdate?: (update: OrderUpdate) => void;
  onPositionUpdate?: (update: PositionUpdate) => void;
  onPortfolioSnapshot?: (data: LiveData) => void;
  autoConnect?: boolean;
}

export function usePortfolioWebSocket(options: UsePortfolioWebSocketOptions = {}) {
  const [positions, setPositions] = useState<LivePosition[]>([]);
  const [mtmPnl, setMtmPnl] = useState<number>(0);
  const [greeks, setGreeks] = useState({ delta: 0, theta: 0, vega: 0, gamma: 0 });
  const [lastOrderUpdate, setLastOrderUpdate] = useState<OrderUpdate | null>(null);

  // Safely derive absolute ws:// URL — same-origin in prod, explicit in dev
  const socketUrl = `${getWsBase()}/api/ws/portfolio`;

  const { sendMessage, lastMessage, status, connect, disconnect } = useWebSocket(
    socketUrl,
    {
      onMessage: (data) => {
        if (data.type === 'portfolio_update') {
          const portfolioData = data.data;
          
          if (portfolioData.positions) {
            setPositions(portfolioData.positions);
          }
          
          if (portfolioData.mtm_pnl !== undefined) {
            setMtmPnl(portfolioData.mtm_pnl);
          }
          
          if (portfolioData.greeks) {
            setGreeks(portfolioData.greeks);
          }
          
          options.onPortfolioSnapshot?.(portfolioData);
        }
        
        if (data.type === 'order_update') {
          const orderUpdate = data.data as OrderUpdate;
          setLastOrderUpdate(orderUpdate);
          options.onOrderUpdate?.(orderUpdate);
        }
        
        if (data.type === 'position_update') {
          const positionUpdate = data.data as PositionUpdate;
          options.onPositionUpdate?.(positionUpdate);
        }
      },
      autoConnect: options.autoConnect,
    }
  );

  return {
    positions,
    mtmPnl,
    greeks,
    lastOrderUpdate,
    status,
    isConnected: status === 'connected',
    connect,
    disconnect,
  };
}
