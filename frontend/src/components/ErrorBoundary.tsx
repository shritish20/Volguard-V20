import { Component, ReactNode } from 'react'

export class ErrorBoundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  constructor(props: { children: ReactNode }) {
    super(props)
    this.state = { error: null }
  }
  static getDerivedStateFromError(error: Error) { return { error } }
  render() {
    if (this.state.error) {
      return (
        <div className="glass-card p-4 border-signal-red/30">
          <p className="text-signal-red text-xs font-semibold">Component error</p>
          <p className="text-muted-foreground text-[10px] mt-1 font-mono-data">{this.state.error.message}</p>
          <button onClick={() => this.setState({ error: null })} className="mt-2 text-[10px] text-electric-blue hover:underline">
            Retry
          </button>
        </div>
      )
    }
    return this.props.children
  }
}