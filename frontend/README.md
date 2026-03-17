# VolGuard V5

**Institutional-grade options trading intelligence system for Nifty derivatives.**

V4 Quant Engine + V5 AI Reasoning Layer — three intelligent agents: morning brief, pre-trade gate, continuous monitor.

---

## Quick Start

```bash
npm install
npm run dev
```

## Production Build

```bash
npm run build
npm run preview
```

## Environment

Copy `.env` and set your backend URL:

```
VITE_API_BASE=http://localhost:8000
```

## Tabs

| Tab | Purpose |
|---|---|
| **V5 INTEL** | Morning brief · News VETO scanner · AI alerts · Macro snapshot · Pre-trade gate log |
| **MARKET** | Volatility matrix · Market structure · VRP analysis · Strategy engine |
| **POSITIONS** | Live MTM P&L · Greeks · P&L attribution · GTT manager · Emergency exit |
| **LOG** | Trade history · Daily P&L calendar · Win rate · Stats |
| **SYSTEM** | Risk controls · System health · Fill quality · LLM usage · Terminal logs |
