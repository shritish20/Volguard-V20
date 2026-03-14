/**
 * WebSocket Manager Singleton
 * Manages all WebSocket connections with automatic reconnection, 
 * connection pooling, and event handling.
 */

type WebSocketStatus = 'connecting' | 'connected' | 'disconnected' | 'error';
type WebSocketListener = (data: any) => void;

interface ConnectionConfig {
  url: string;
  reconnectAttempts?: number;
  reconnectInterval?: number;
  heartbeatInterval?: number;
}

interface Connection {
  ws: WebSocket | null;
  status: WebSocketStatus;
  listeners: Set<WebSocketListener>;
  reconnectCount: number;
  reconnectTimeout?: NodeJS.Timeout;
  heartbeatInterval?: NodeJS.Timeout;
  config: ConnectionConfig;
}

class WebSocketManagerClass {
  private connections: Map<string, Connection> = new Map();
  private defaultConfig: Partial<ConnectionConfig> = {
    reconnectAttempts: 5,
    reconnectInterval: 3000,
    heartbeatInterval: 30000, // 30 seconds
  };

  /**
   * Create or get a WebSocket connection
   */
  connect(id: string, url: string, config?: Partial<ConnectionConfig>): void {
    if (this.connections.has(id)) {
      const conn = this.connections.get(id)!;
      if (conn.status === 'connected' || conn.status === 'connecting') {
        return;
      }
      this.disconnect(id);
    }

    const fullConfig = { ...this.defaultConfig, ...config, url };
    const connection: Connection = {
      ws: null,
      status: 'connecting',
      listeners: new Set(),
      reconnectCount: 0,
      config: fullConfig,
    };

    this.connections.set(id, connection);
    this.establishConnection(id);
  }

  /**
   * Establish WebSocket connection
   */
  private establishConnection(id: string): void {
    const conn = this.connections.get(id);
    if (!conn) return;

    try {
      conn.status = 'connecting';
      conn.ws = new WebSocket(conn.config.url);

      conn.ws.onopen = () => {
        conn.status = 'connected';
        conn.reconnectCount = 0;
        this.setupHeartbeat(id);
        this.notifyListeners(id, { type: 'connection', status: 'connected' });
      };

      conn.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          this.notifyListeners(id, data);
        } catch (e) {
          console.error(`[WebSocketManager:${id}] Failed to parse message:`, e);
        }
      };

      conn.ws.onclose = () => {
        conn.status = 'disconnected';
        this.clearHeartbeat(id);
        this.handleReconnect(id);
        this.notifyListeners(id, { type: 'connection', status: 'disconnected' });
      };

      conn.ws.onerror = (error) => {
        conn.status = 'error';
        console.error(`[WebSocketManager:${id}] Error:`, error);
        this.notifyListeners(id, { type: 'error', error });
      };
    } catch (error) {
      conn.status = 'error';
      console.error(`[WebSocketManager:${id}] Failed to establish connection:`, error);
      this.handleReconnect(id);
    }
  }

  /**
   * Setup heartbeat to keep connection alive
   */
  private setupHeartbeat(id: string): void {
    const conn = this.connections.get(id);
    if (!conn || !conn.config.heartbeatInterval) return;

    conn.heartbeatInterval = setInterval(() => {
      if (conn.ws?.readyState === WebSocket.OPEN) {
        conn.ws.send(JSON.stringify({ type: 'ping' }));
      }
    }, conn.config.heartbeatInterval);
  }

  /**
   * Clear heartbeat interval
   */
  private clearHeartbeat(id: string): void {
    const conn = this.connections.get(id);
    if (conn?.heartbeatInterval) {
      clearInterval(conn.heartbeatInterval);
      delete conn.heartbeatInterval;
    }
  }

  /**
   * Handle reconnection logic
   */
  private handleReconnect(id: string): void {
    const conn = this.connections.get(id);
    if (!conn || !conn.config.reconnectAttempts) return;

    if (conn.reconnectCount < conn.config.reconnectAttempts) {
      conn.reconnectCount++;
      
      const delay = conn.config.reconnectInterval || 3000;
      conn.reconnectTimeout = setTimeout(() => {
        this.establishConnection(id);
      }, delay * conn.reconnectCount); // Exponential backoff
    } else {
      console.error(`[WebSocketManager:${id}] Max reconnection attempts reached`);
      this.notifyListeners(id, { type: 'error', error: 'Max reconnection attempts reached' });
    }
  }

  /**
   * Subscribe to WebSocket messages
   */
  subscribe(id: string, listener: WebSocketListener): () => void {
    let conn = this.connections.get(id);
    
    if (!conn) {
      // Create a placeholder connection if it doesn't exist
      conn = {
        ws: null,
        status: 'disconnected',
        listeners: new Set(),
        reconnectCount: 0,
        config: { url: '' },
      };
      this.connections.set(id, conn);
    }

    conn.listeners.add(listener);

    // Return unsubscribe function
    return () => {
      const currentConn = this.connections.get(id);
      if (currentConn) {
        currentConn.listeners.delete(listener);
        if (currentConn.listeners.size === 0) {
          // Auto-disconnect when no listeners
          this.disconnect(id);
        }
      }
    };
  }

  /**
   * Notify all listeners of a connection
   */
  private notifyListeners(id: string, data: any): void {
    const conn = this.connections.get(id);
    if (conn) {
      conn.listeners.forEach(listener => {
        try {
          listener(data);
        } catch (e) {
          console.error(`[WebSocketManager:${id}] Listener error:`, e);
        }
      });
    }
  }

  /**
   * Send message through WebSocket
   */
  send(id: string, data: any): boolean {
    const conn = this.connections.get(id);
    if (conn?.ws?.readyState === WebSocket.OPEN) {
      conn.ws.send(JSON.stringify(data));
      return true;
    }
    return false;
  }

  /**
   * Get connection status
   */
  getStatus(id: string): WebSocketStatus {
    return this.connections.get(id)?.status || 'disconnected';
  }

  /**
   * Check if connection is active
   */
  isConnected(id: string): boolean {
    return this.connections.get(id)?.status === 'connected';
  }

  /**
   * Disconnect specific connection
   */
  disconnect(id: string): void {
    const conn = this.connections.get(id);
    if (conn) {
      this.clearHeartbeat(id);
      
      if (conn.reconnectTimeout) {
        clearTimeout(conn.reconnectTimeout);
        delete conn.reconnectTimeout;
      }

      if (conn.ws) {
        conn.ws.close();
        conn.ws = null;
      }

      conn.status = 'disconnected';
      this.connections.delete(id);
    }
  }

  /**
   * Disconnect all connections
   */
  disconnectAll(): void {
    Array.from(this.connections.keys()).forEach(id => this.disconnect(id));
  }

  /**
   * Get connection stats
   */
  getStats(): Record<string, { status: WebSocketStatus; listenerCount: number }> {
    const stats: Record<string, { status: WebSocketStatus; listenerCount: number }> = {};
    
    this.connections.forEach((conn, id) => {
      stats[id] = {
        status: conn.status,
        listenerCount: conn.listeners.size,
      };
    });

    return stats;
  }
}

// Export singleton instance
export const WebSocketManager = new WebSocketManagerClass();
