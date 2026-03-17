import React, { createContext, useContext, ReactNode } from 'react';
import { useMarketWebSocket } from '@/hooks/useMarketWebSocket';
import { usePortfolioWebSocket } from '@/hooks/usePortfolioWebSocket';

interface WebSocketContextType {
  // Market data
  marketPrices: Record<string, number>;
  marketStatus: 'connecting' | 'connected' | 'disconnected' | 'error';
  subscribeToInstruments: (keys: string[]) => void;
  unsubscribeFromInstruments: (keys: string[]) => void;
  
  // Portfolio data
  positions: any[];
  portfolioMtm: number;
  portfolioGreeks: { delta: number; theta: number; vega: number; gamma: number };
  portfolioStatus: 'connecting' | 'connected' | 'disconnected' | 'error';
  
  // Connection management
  reconnect: () => void;
  disconnectAll: () => void;
  isConnected: boolean;
}

const WebSocketContext = createContext<WebSocketContextType | undefined>(undefined);

interface WebSocketProviderProps {
  children: ReactNode;
  defaultInstruments?: string[];
}

export function WebSocketProvider({ 
  children, 
  defaultInstruments = ['NSE_INDEX|Nifty 50', 'NSE_INDEX|India VIX'] 
}: WebSocketProviderProps) {
  
  // Market WebSocket
  const {
    prices: marketPrices,
    status: marketStatus,
    subscribe,
    unsubscribe,
    isConnected: marketConnected,
    connect: connectMarket,
    disconnect: disconnectMarket,
  } = useMarketWebSocket(defaultInstruments, {
    autoConnect: true,
  });

  // Portfolio WebSocket
  const {
    positions,
    mtmPnl,
    greeks,
    status: portfolioStatus,
    isConnected: portfolioConnected,
    connect: connectPortfolio,
    disconnect: disconnectPortfolio,
  } = usePortfolioWebSocket({
    autoConnect: true,
  });

  const subscribeToInstruments = (keys: string[]) => {
    subscribe(keys);
  };

  const unsubscribeFromInstruments = (keys: string[]) => {
    unsubscribe(keys);
  };

  const reconnect = () => {
    disconnectMarket();
    disconnectPortfolio();
    setTimeout(() => {
      connectMarket();
      connectPortfolio();
    }, 1000);
  };

  const disconnectAll = () => {
    disconnectMarket();
    disconnectPortfolio();
  };

  const isConnected = marketConnected && portfolioConnected;

  const value: WebSocketContextType = {
    marketPrices,
    marketStatus,
    subscribeToInstruments,
    unsubscribeFromInstruments,
    positions,
    portfolioMtm: mtmPnl,
    portfolioGreeks: greeks,
    portfolioStatus,
    reconnect,
    disconnectAll,
    isConnected,
  };

  return (
    <WebSocketContext.Provider value={value}>
      {children}
    </WebSocketContext.Provider>
  );
}

export function useWebSocketContext() {
  const context = useContext(WebSocketContext);
  if (context === undefined) {
    throw new Error('useWebSocketContext must be used within a WebSocketProvider');
  }
  return context;
}
