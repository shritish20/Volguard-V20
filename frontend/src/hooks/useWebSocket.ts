import { useState, useEffect, useRef, useCallback } from 'react';
import { WebSocketManager } from '@/services/WebSocketManager';

// Define connection status types
export type WebSocketStatus = 'connecting' | 'connected' | 'disconnected' | 'error';

interface WebSocketOptions {
  onMessage?: (data: any) => void;
  onOpen?: () => void;
  onClose?: () => void;
  onError?: (error: Event | string) => void;
  autoConnect?: boolean;
  reconnectInterval?: number;
  maxRetries?: number;
}

export function useWebSocket(url: string, options: WebSocketOptions = {}) {
  const [status, setStatus] = useState<WebSocketStatus>('disconnected');
  const [lastMessage, setLastMessage] = useState<any>(null);
  
  // Use the URL as the unique connection ID for the manager
  const id = url;

  // Stabilizer for React re-renders
  const optionsRef = useRef(options);
  useEffect(() => {
    optionsRef.current = options;
  });

  const connect = useCallback(() => {
    const token = localStorage.getItem("upstox_token");
    
    // Construct URL with auth query param if token exists
    const fullUrl = token && !url.includes('token=') 
      ? `${url}${url.includes('?') ? '&' : '?'}token=${token}`
      : url;

    // Tell the Singleton Manager to establish the connection
    WebSocketManager.connect(id, fullUrl, {
      reconnectAttempts: optionsRef.current.maxRetries || 5,
      reconnectInterval: optionsRef.current.reconnectInterval || 3000,
    });
  }, [url, id]);

  const disconnect = useCallback(() => {
    WebSocketManager.disconnect(id);
    setStatus('disconnected');
  }, [id]);

  const sendMessage = useCallback((data: any) => {
    WebSocketManager.send(id, data);
  }, [id]);

  useEffect(() => {
    if (options.autoConnect !== false) {
      connect();
    }

    // Subscribe to messages from the Singleton Manager
    const unsubscribe = WebSocketManager.subscribe(id, (data) => {
      // The Manager sends internal connection status updates
      if (data && data.type === 'connection') {
        setStatus(data.status);
        if (data.status === 'connected' && optionsRef.current.onOpen) optionsRef.current.onOpen();
        if (data.status === 'disconnected' && optionsRef.current.onClose) optionsRef.current.onClose();
      } 
      // The Manager sends internal error updates
      else if (data && data.type === 'error') {
        setStatus('error');
        if (optionsRef.current.onError) optionsRef.current.onError(data.error);
      } 
      // Actual WebSocket payload data
      else {
        setLastMessage(data);
        if (optionsRef.current.onMessage) optionsRef.current.onMessage(data);
      }
    });

    // Set initial status
    setStatus(WebSocketManager.getStatus(id) as WebSocketStatus);

    return () => {
      // When the component unmounts, unsubscribe. 
      // The Manager will automatically close the socket if listeners reach 0!
      unsubscribe();
    };
  }, [id, connect, options.autoConnect]);

  return {
    status,
    lastMessage,
    connect,
    disconnect,
    sendMessage,
    isConnected: status === 'connected'
  };
}
