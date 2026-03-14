"""
VolGuard — Intelligence Edition
Quant Engine + AI Reasoning Layer
Version: 6.0.0
"""

import os
import sys
import math
import io
import re
import asyncio
import aiohttp
import logging
import threading
import time
import uuid
import socket
import urllib3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, time as dt_time, timedelta
from typing import List, Dict, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import json
from contextlib import asynccontextmanager, contextmanager
import urllib.parse
import copy

# FastAPI WebSocket
from fastapi import WebSocket, WebSocketDisconnect

# Third-party imports
import pandas as pd
import numpy as np
import pytz

# FastAPI
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Header, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

# Database - SQLAlchemy 2.0 compatible
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, JSON, desc, event, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Upstox SDK (required)
try:
    import upstox_client
    from upstox_client.rest import ApiException
    UPSTOX_AVAILABLE = True
except ImportError:
    UPSTOX_AVAILABLE = False
    logging.error("upstox_client NOT INSTALLED! Please install: pip install upstox-python-sdk")

# For FII data fetching
import requests

# ============================================================================
# INTELLIGENCE LAYER — IMPORTS
# Zero additional cost except anthropic (~$3-6/month)
# ============================================================================
import hashlib
import email.utils
import threading as _v5_threading

# yfinance removed — blocked on AWS datacenter IPs.
# Replaced with: Twelve Data API (global equities/FX/commodities) + FRED (US 10Y yield).
# Both work on any server, free, no IP restrictions.
V5_YFINANCE = False  # kept as compatibility flag

_TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
V5_TWELVEDATA = bool(_TWELVE_DATA_KEY)
if not V5_TWELVEDATA:
    logging.warning("TWELVE_DATA_API_KEY not set — macro data unavailable. Free key at twelvedata.com")

_FRED_KEY = os.getenv("FRED_API_KEY", "")
V5_FRED = bool(_FRED_KEY)
if not V5_FRED:
    logging.warning("FRED_API_KEY not set — US 10Y yield unavailable. Free key at fred.stlouisfed.org")

try:
    import feedparser
    V5_FEEDPARSER = True
except ImportError:
    V5_FEEDPARSER = False
    logging.warning("feedparser not installed — news scanner unavailable. pip install feedparser")

try:
    import anthropic as _anthropic_lib
    V5_ANTHROPIC = True
except ImportError:
    V5_ANTHROPIC = False
    logging.warning("anthropic not installed — pip install anthropic")

try:
    from groq import Groq as _GroqClient
    V5_GROQ = True
except ImportError:
    V5_GROQ = False
    # groq not installed — silent, it's optional

# Which LLM backend is active?
# Priority: ANTHROPIC_API_KEY (Claude — primary) → GROQ_API_KEY (Groq — free fallback)
# Right now running on Groq (free tier).  Drop ANTHROPIC_API_KEY in .env to silently
# upgrade to Claude — no code change needed anywhere.
_V5_LLM_PROVIDER = (
    "claude"    if (V5_ANTHROPIC  and os.getenv("ANTHROPIC_API_KEY")) else
    "groq"      if (V5_GROQ      and os.getenv("GROQ_API_KEY"))      else
    "none"
)
V5_LLM_READY = _V5_LLM_PROVIDER != "none"  

# ============================================================================
# LOGGING SETUP
# ============================================================================

class LogBufferHandler(logging.Handler):
    def __init__(self, capacity=1000):
        super().__init__()
        self.capacity = capacity
        self.buffer = []
        self._lock = threading.Lock()
        
    def emit(self, record):
        with self._lock:
            msg = self.format(record)
            self.buffer.append({
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": msg
            })
            if len(self.buffer) > self.capacity:
                self.buffer.pop(0)
    
    def get_logs(self, lines=50, level=None):
        with self._lock:
            logs = self.buffer[-lines:] if lines < len(self.buffer) else self.buffer
            if level:
                logs = [l for l in logs if l["level"] == level.upper()]
            return logs

log_buffer = LogBufferHandler(capacity=1000)
log_file_path = os.getenv("LOG_FILE", "volguard.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(),
        log_buffer
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# DYNAMIC CONFIGURATION
# ============================================================================

class DynamicConfig:
    DEFAULTS = {
        "BASE_CAPITAL": 1500000.0,
        "MAX_LOSS_PCT": 3.0,
        "PROFIT_TARGET": 70.0,
        "MAX_DAILY_LOSS_PCT": 3.0,
        "MAX_CONSECUTIVE_LOSSES": 3,
        "CIRCUIT_BREAKER_PCT": 3.0,
        "AUTO_TRADING": False,
        "ENABLE_MOCK_TRADING": True,
        "MIN_OI": 50000,
        "MAX_BID_ASK_SPREAD_PCT": 2.0,
        "MAX_POSITION_RISK_PCT": 2.0,
        "HIGH_VOL_IVP": 75.0,
        "LOW_VOL_IVP": 25.0,
        "VOV_CRASH_ZSCORE": 3.0,       # z-score >= 3.0  → TRADE BLOCKED (score → ZERO)
        "VOV_HEAVY_ZSCORE": 2.75,      # z-score >= 2.75 → DANGER  (-3.5 vol score penalty)
        "VOV_MEDIUM_ZSCORE": 2.50,     # z-score >= 2.50 → ELEVATED (-2.0 vol score penalty)
        "VOV_WARNING_ZSCORE": 2.25,    # z-score >= 2.25 → WARNING  (-1.0 vol score penalty)
        "VIX_MOMENTUM_BREAKOUT": 5.0,
        "GEX_STICKY_RATIO": 0.03,
        "SKEW_CRASH_FEAR": 5.0,
        "SKEW_MELT_UP": -2.0,
        "FII_VERY_HIGH_CONVICTION": 150000,
        "FII_HIGH_CONVICTION": 80000,
        "FII_MODERATE_CONVICTION": 40000,
        "WEEKLY_ALLOCATION_PCT": 40.0,
        "MONTHLY_ALLOCATION_PCT": 40.0,
        "NEXT_WEEKLY_ALLOCATION_PCT": 20.0,
        "STOP_LOSS_MULTIPLIER": 2.0,
        "PROFIT_TARGET_MULTIPLIER": 0.30,
        "ANALYTICS_INTERVAL_MINUTES": 15,
        "ANALYTICS_OFFHOURS_INTERVAL_MINUTES": 60,
        "POSITION_RECONCILE_INTERVAL_MINUTES": 10,
        "SPOT_CHANGE_TRIGGER_PCT": 0.3,
        "VIX_CHANGE_TRIGGER_PCT": 2.0,
        "PNL_DISCREPANCY_THRESHOLD": 100.0,
        "MAX_CONCURRENT_SAME_STRATEGY": 2,
        "GTT_STOP_LOSS_MULTIPLIER": 2.0,
        "GTT_PROFIT_TARGET_MULTIPLIER": 0.30,
        "GTT_TRAILING_GAP": 0.1,
        "ORDER_FILL_TIMEOUT_SECONDS": 15,
        "ORDER_FILL_CHECK_INTERVAL": 0.5,
        "PROTECTED_STRADDLE_WING_DELTA": 0.02,
        "PROTECTED_STRANGLE_WING_DELTA": 0.05,
        "IRON_CONDOR_WING_DELTA": 0.15,
        "IRON_FLY_WING_MULTIPLIER": 1.10,
        "MONITOR_INTERVAL_SECONDS": 5,
    }
    _values = {}
    _db_session_factory = None
    _initialized = False
    _lock = threading.RLock()
    
    @classmethod
    def initialize(cls, db_session_factory):
        with cls._lock:
            cls._db_session_factory = db_session_factory
            db = db_session_factory()
            try:
                db.execute(text("""
                    CREATE TABLE IF NOT EXISTS dynamic_config (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                db.commit()
                
                for key, default_val in cls.DEFAULTS.items():
                    result = db.execute(
                        text("SELECT value FROM dynamic_config WHERE key = :key"),
                        {"key": key}
                    ).fetchone()
                    
                    if result:
                        stored = result[0]
                        try:
                            if isinstance(default_val, bool):
                                cls._values[key] = stored.lower() == 'true'
                            elif isinstance(default_val, int):
                                cls._values[key] = int(stored)
                            elif isinstance(default_val, float):
                                cls._values[key] = float(stored)
                            else:
                                cls._values[key] = stored
                        except Exception:
                            cls._values[key] = default_val
                    else:
                        cls._values[key] = default_val
                        cls._persist(key, default_val, db)
                
                cls._initialized = True
                logger.info(f"✅ DynamicConfig initialized with {len(cls._values)} settings")
            finally:
                db.close()
    
    @classmethod
    def _persist(cls, key, value, db=None):
        close_db = False
        if db is None:
            db = cls._db_session_factory()
            close_db = True
        
        try:
            str_val = str(value)
            db.execute(
                text("""
                    INSERT INTO dynamic_config (key, value, updated_at) 
                    VALUES (:key, :value, :updated_at)
                    ON CONFLICT(key) DO UPDATE SET 
                    value = excluded.value, 
                    updated_at = excluded.updated_at
                """),
                {"key": key, "value": str_val, "updated_at": datetime.now()}
            )
            db.commit()
        finally:
            if close_db:
                db.close()
    
    @classmethod
    def get(cls, key, default=None):
        with cls._lock:
            return cls._values.get(key, default)
    
    @classmethod
    def update(cls, updates: dict):
        with cls._lock:
            if not cls._initialized:
                raise RuntimeError("DynamicConfig not initialized")
            
            changed = {}
            db = cls._db_session_factory()
            try:
                for key, new_val in updates.items():
                    if key in cls.DEFAULTS:
                        default_type = type(cls.DEFAULTS[key])
                        try:
                            if default_type == bool:
                                if isinstance(new_val, str):
                                    new_val = new_val.lower() == 'true'
                                else:
                                    new_val = bool(new_val)
                            elif default_type == int:
                                new_val = int(new_val)
                            elif default_type == float:
                                new_val = float(new_val)
                            
                            cls._values[key] = new_val
                            cls._persist(key, new_val, db)
                            changed[key] = new_val
                            logger.info(f"Config updated: {key} = {new_val}")
                        except Exception as e:
                            logger.error(f"Failed to update {key}: {e}")
                
                return changed
            finally:
                db.close()
    
    @classmethod
    def to_dict(cls):
        with cls._lock:
            return cls._values.copy()


# ============================================================================
# SYSTEM CONFIGURATION
# ============================================================================

class SystemConfig:
    UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    
    NIFTY_KEY = "NSE_INDEX|Nifty 50"
    VIX_KEY = "NSE_INDEX|India VIX"
    
    VETO_KEYWORDS = [
        "RBI Monetary Policy", "RBI Policy", "Repo Rate Decision", "MPC Meeting",
        "FOMC", "Federal Funds Rate Decision",
        "Union Budget", "Budget Speech"
    ]
    HIGH_IMPACT_KEYWORDS = [
        "GDP", "Gross Domestic Product", "NFP", "Non-Farm Payroll",
        "CPI", "Consumer Price Index"
    ]
    EVENT_RISK_DAYS_AHEAD = 14
    
    PRE_EXPIRY_SQUARE_OFF_DAYS = 1
    PRE_EXPIRY_SQUARE_OFF_TIME = dt_time(14, 0)
    PRE_EVENT_SQUARE_OFF_DAYS = 1
    PRE_EVENT_SQUARE_OFF_TIME = dt_time(14, 0)
    
    DAILY_FETCH_TIME_IST = dt_time(21, 0)
    PRE_MARKET_WARM_TIME_IST = dt_time(8, 55)
    MARKET_OPEN_IST = dt_time(9, 15)
    MARKET_CLOSE_IST = dt_time(15, 30)
    PNL_RECONCILE_TIME_IST = dt_time(16, 0)
    
    HOST = "0.0.0.0"
    PORT = int(os.getenv("PORT", "8000"))
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./volguard.db")
    
    @classmethod
    def should_square_off_position(cls, trade) -> Tuple[bool, str]:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.date()
        
        expiry_date = trade.expiry_date.date()
        days_to_expiry = (expiry_date - today).days
        
        if days_to_expiry == cls.PRE_EXPIRY_SQUARE_OFF_DAYS:
            square_off_time = ist.localize(datetime.combine(
                today, 
                cls.PRE_EXPIRY_SQUARE_OFF_TIME
            ))
            if now >= square_off_time:
                return True, f"PRE_EXPIRY_SQUARE_OFF - {days_to_expiry} day before expiry"
        
        if hasattr(trade, 'associated_event_date') and trade.associated_event_date:
            event_date = trade.associated_event_date.date()
            days_to_event = (event_date - today).days
            
            if days_to_event == cls.PRE_EVENT_SQUARE_OFF_DAYS:
                square_off_time = ist.localize(datetime.combine(
                    today,
                    cls.PRE_EVENT_SQUARE_OFF_TIME
                ))
                if now >= square_off_time:
                    return True, f"PRE_EVENT_SQUARE_OFF - {trade.associated_event_name}"
        
        return False, ""
    
    @classmethod
    def is_expiry_day(cls, date_to_check: date, all_expiries: List[date]) -> bool:
        return date_to_check in all_expiries


# ============================================================================
# AUTHENTICATION
# ============================================================================

security = HTTPBearer(auto_error=False)

async def verify_token(x_upstox_token: Optional[str] = Header(None, alias="X-Upstox-Token")):
    if not x_upstox_token:
        if SystemConfig.UPSTOX_ACCESS_TOKEN:
            logger.warning("⚠️ Using UPSTOX_ACCESS_TOKEN from environment - only for development!")
            return SystemConfig.UPSTOX_ACCESS_TOKEN
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Upstox-Token header"
        )
    return x_upstox_token


# ============================================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================================

class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = threading.RLock()
        self.logger = logging.getLogger("WebSocketManager")
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        with self._lock:
            self.active_connections.append(websocket)
        self.logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        self.logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_message(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_json(message)
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: dict):
        disconnected = []
        with self._lock:
            connections = self.active_connections.copy()
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        for connection in disconnected:
            self.disconnect(connection)


# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class ConfigUpdateRequest(BaseModel):
    max_loss: Optional[float] = Field(None, ge=0.1, le=10.0)
    profit_target: Optional[float] = Field(None, ge=10.0, le=95.0)
    base_capital: Optional[float] = Field(None, ge=100000, le=100000000)
    auto_trading: Optional[bool] = Field(None)
    min_oi: Optional[int] = Field(None, ge=10000, le=500000)
    max_spread_pct: Optional[float] = Field(None, ge=0.1, le=10.0)
    max_position_risk_pct: Optional[float] = Field(None, ge=0.5, le=5.0)
    max_concurrent_same_strategy: Optional[int] = Field(None, ge=1, le=5)
    gtt_stop_loss_multiplier: Optional[float] = Field(None, ge=1.0, le=5.0)
    gtt_profit_target_multiplier: Optional[float] = Field(None, ge=0.1, le=1.0)
    gtt_trailing_gap: Optional[float] = Field(None, ge=0.05, le=1.0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "max_loss": 1.5,
                "profit_target": 75,
                "base_capital": 2000000,
                "auto_trading": True,
                "min_oi": 50000,
                "max_spread_pct": 2.0,
                "max_position_risk_pct": 2.0,
                "max_concurrent_same_strategy": 2,
                "gtt_stop_loss_multiplier": 2.0,
                "gtt_profit_target_multiplier": 0.30,
                "gtt_trailing_gap": 0.1
            }
        }

class DashboardAnalyticsResponse(BaseModel):
    market_status: Dict
    mandate: Dict
    scores: Dict
    events: List[Dict]

class LivePositionsResponse(BaseModel):
    mtm_pnl: float
    pnl_color: str
    greeks: Dict  # delta, theta, vega, gamma, theta_vega_ratio
    positions: List[Dict]
    market_status: Optional[Dict] = None
    straddle_info: Optional[Dict] = None

class TradeJournalEntry(BaseModel):
    id: Optional[str] = None
    date: str
    strategy: str
    entry: Optional[str] = None
    exit: Optional[str] = None
    expiry_type: Optional[str] = None
    result: str
    pnl: float
    exit_reason: str
    is_mock: bool = False
    trade_outcome_class: Optional[str] = None

class SystemLogsResponse(BaseModel):
    logs: List[Dict]
    total_lines: int


# ============================================================================
# ENUMS & DATA MODELS
# ============================================================================

class AlertPriority(Enum):
    CRITICAL = "🔴 CRITICAL"
    HIGH = "🟠 HIGH"
    MEDIUM = "🟡 MEDIUM"
    LOW = "🔵 INFO"
    SUCCESS = "🟢 SUCCESS"

class StrategyType(str, Enum):
    IRON_FLY = "IRON_FLY"
    IRON_CONDOR = "IRON_CONDOR"
    PROTECTED_STRADDLE = "PROTECTED_STRADDLE"
    PROTECTED_STRANGLE = "PROTECTED_STRANGLE"
    BULL_PUT_SPREAD = "BULL_PUT_SPREAD"
    BEAR_CALL_SPREAD = "BEAR_CALL_SPREAD"
    CASH = "CASH"

class ExpiryType(str, Enum):
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    NEXT_WEEKLY = "NEXT_WEEKLY"

class OrderStatus(str, Enum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    PARTIAL = "PARTIAL"

class TradeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED_PROFIT_TARGET = "CLOSED_PROFIT_TARGET"
    CLOSED_STOP_LOSS = "CLOSED_STOP_LOSS"
    CLOSED_EXPIRY_EXIT = "CLOSED_EXPIRY_EXIT"
    CLOSED_VETO_EVENT = "CLOSED_VETO_EVENT"
    CLOSED_CIRCUIT_BREAKER = "CLOSED_CIRCUIT_BREAKER"
    CLOSED_GTT = "CLOSED_GTT"          # GTT stop-loss or target fired at exchange
    PENDING_EXIT = "PENDING_EXIT"


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class TimeMetrics:
    current_date: date
    current_time_ist: datetime
    weekly_exp: date
    monthly_exp: date
    next_weekly_exp: date
    dte_weekly: int
    dte_monthly: int
    dte_next_weekly: int
    is_expiry_day_weekly: bool
    is_expiry_day_monthly: bool
    is_expiry_day_next_weekly: bool
    is_past_square_off_time: bool

@dataclass
class VolMetrics:
    spot: float
    vix: float
    rv7: float
    rv28: float
    rv90: float
    garch7: float
    garch28: float
    park7: float
    park28: float
    vov: float
    vov_zscore: float
    ivp_30d: float
    ivp_90d: float
    ivp_1yr: float
    ma20: float
    atr14: float
    trend_strength: float
    vol_regime: str
    is_fallback: bool
    vix_change_5d: float
    vix_momentum: str

@dataclass
class StructMetrics:
    net_gex: float
    gex_ratio: float
    total_oi_value: float
    gex_regime: str
    pcr: float
    max_pain: float
    skew_25d: float
    oi_regime: str
    lot_size: int
    pcr_atm: float
    skew_regime: str
    gex_weighted: float

@dataclass
class EdgeMetrics:
    iv_weekly: float
    vrp_rv_weekly: float
    vrp_garch_weekly: float
    vrp_park_weekly: float
    iv_monthly: float
    vrp_rv_monthly: float
    vrp_garch_monthly: float
    vrp_park_monthly: float
    iv_next_weekly: float
    vrp_rv_next_weekly: float
    vrp_garch_next_weekly: float
    vrp_park_next_weekly: float
    expiry_risk_discount_weekly: float
    expiry_risk_discount_monthly: float
    expiry_risk_discount_next_weekly: float
    term_structure_slope: float        # DTE-adjusted — used only for regime label
    term_spread_display: float         # Raw iv_monthly - iv_weekly — shown in UI
    term_structure_regime: str
    weighted_vrp_weekly: float
    weighted_vrp_monthly: float
    weighted_vrp_next_weekly: float

@dataclass
class ParticipantData:
    fut_long: float
    fut_short: float
    fut_net: float
    call_long: float
    call_short: float
    call_net: float
    put_long: float
    put_short: float
    put_net: float
    stock_net: float
    total_net: float

@dataclass
class EconomicEvent:
    title: str
    country: str
    event_date: datetime
    impact_level: str
    event_type: str
    forecast: str
    previous: str
    days_until: int
    hours_until: float
    is_veto_event: bool
    suggested_square_off_time: Optional[datetime]

@dataclass
class ExternalMetrics:
    fii_data: Optional[Dict[str, ParticipantData]]
    fii_secondary: Optional[Dict[str, ParticipantData]]
    fii_net_change: float
    fii_conviction: str
    fii_sentiment: str
    fii_data_date: str
    fii_is_fallback: bool
    flow_regime: str
    economic_events: List[EconomicEvent]
    veto_event_near: bool
    high_impact_event_near: bool
    suggested_square_off_time: Optional[datetime]
    risk_score: float

@dataclass
class DynamicWeights:
    vol_weight: float
    struct_weight: float
    edge_weight: float
    rationale: str

@dataclass
class RegimeScore:
    total_score: float
    vol_score: float
    struct_score: float
    edge_score: float
    external_score: float
    vol_signal: str
    struct_signal: str
    edge_signal: str
    external_signal: str
    overall_signal: str
    confidence: str
    weights_used: Optional['DynamicWeights'] = None
    weight_rationale: str = ""
    score_stability: float = 0.0
    score_drivers: List[str] = field(default_factory=list)

@dataclass
class TradingMandate:
    expiry_type: str
    expiry_date: date
    is_trade_allowed: bool
    suggested_structure: str
    deployment_amount: float
    risk_notes: List[str]
    veto_reasons: List[str]
    regime_summary: str
    confidence_level: str
    directional_bias: str = "NEUTRAL"
    regime_name: str = "UNKNOWN"
    wing_protection: str = "N/A"
    square_off_instruction: Optional[str] = None

@dataclass
class OptionLeg:
    instrument_token: str
    strike: float
    option_type: str
    action: str
    quantity: int
    delta: float
    gamma: float
    vega: float
    theta: float
    iv: float
    ltp: float
    bid: float
    ask: float
    oi: float
    lot_size: int
    entry_price: float = 0.0
    entry_bid: float = 0.0
    entry_ask: float = 0.0
    product: str = "D"
    pop: float = 0.0
    filled_quantity: int = 0
    order_id: Optional[str] = None
    correlation_id: Optional[str] = None
    fill_price: Optional[float] = None
    fill_time: Optional[datetime] = None

@dataclass
class ConstructedStrategy:
    strategy_id: str
    strategy_type: StrategyType
    expiry_type: ExpiryType
    expiry_date: date
    legs: List[OptionLeg]
    max_profit: float
    max_loss: float
    pop: float
    net_theta: float
    net_vega: float
    net_delta: float
    net_gamma: float
    allocated_capital: float
    required_margin: float
    max_risk_amount: float
    validation_passed: bool
    validation_errors: List[str] = field(default_factory=list)
    construction_time: datetime = field(default_factory=datetime.now)


# ============================================================================
# SMART DATA FETCHER WITH FALLBACK
# ============================================================================

class SmartDataFetcher:
    def __init__(self, fetcher):
        self.fetcher = fetcher
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ist_tz = pytz.timezone('Asia/Kolkata')
    
    def is_market_open(self) -> bool:
        status = self.fetcher.get_market_status_detailed()
        return status.lower() == "normal_open"
    
    def get_market_status(self) -> Dict:
        now = datetime.now(self.ist_tz)
        is_open = self.is_market_open()
        detailed_status = self.fetcher.get_market_status_detailed()
        
        next_open = None
        if not is_open:
            next_day = now
            while True:
                next_day = next_day + timedelta(days=1)
                if next_day.weekday() < 5:
                    next_open = next_day.replace(hour=9, minute=15, second=0, microsecond=0)
                    break
        
        return {
            "is_open": is_open,
            "current_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "Asia/Kolkata",
            "next_open": next_open.strftime("%Y-%m-%d %H:%M:%S") if next_open else None,
            "message": f"Market is {detailed_status}" if is_open else "Market is closed",
            "detailed_status": detailed_status
        }
    
    def get_ltp(self, instrument_key: str) -> Optional[float]:
        market_open = self.is_market_open()
        
        if market_open and self.fetcher.market_streamer.is_connected:
            ws_price = self.fetcher.market_streamer.get_ltp(instrument_key)
            if ws_price and ws_price > 0:
                self.logger.debug(f"WebSocket LTP for {instrument_key}: {ws_price}")
                return ws_price
        
        try:
            self.fetcher._check_rate_limit()
            response = self.fetcher.quote_api_v3.get_ltp(
                instrument_key=instrument_key
            )
            
            if response.status == 'success' and response.data:
                response_key = instrument_key.replace('|', ':')
                if response_key in response.data:
                    item = response.data[response_key]
                    if hasattr(item, 'last_price'):
                        price = float(item.last_price)
                        return price
        except Exception as e:
            self.logger.error(f"REST API LTP fallback failed for {instrument_key}: {e}")
        
        return None
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        result = {}
        market_open = self.is_market_open()
        
        if market_open and self.fetcher.market_streamer.is_connected:
            ws_prices = self.fetcher.market_streamer.get_bulk_ltp(instrument_keys)
            for key in instrument_keys:
                if key in ws_prices and ws_prices[key] and ws_prices[key] > 0:
                    result[key] = ws_prices[key]
        
        missing = [k for k in instrument_keys if k not in result]
        if missing:
            try:
                self.fetcher._check_rate_limit()
                chunks = [missing[i:i+50] for i in range(0, len(missing), 50)]
                
                for chunk in chunks:
                    response = self.fetcher.quote_api_v3.get_ltp(
                        instrument_key=",".join(chunk)
                    )
                    
                    if response.status == 'success' and response.data:
                        for key in chunk:
                            response_key = key.replace('|', ':')
                            if response_key in response.data:
                                item = response.data[response_key]
                                if hasattr(item, 'last_price'):
                                    result[key] = float(item.last_price)
                    
                    if len(chunks) > 1:
                        time.sleep(0.1)
                        
            except Exception as e:
                self.logger.error(f"Bulk REST LTP failed: {e}")
        
        return result
    
    def get_ohlc(self, instrument_key: str, interval: str = "1d") -> Optional[Dict]:
        try:
            self.fetcher._check_rate_limit()
            response = self.fetcher.quote_api_v3.get_market_quote_ohlc(
                interval=interval,
                instrument_key=instrument_key
            )
            
            if response.status == 'success' and response.data:
                response_key = instrument_key.replace('|', ':')
                if response_key in response.data:
                    item = response.data[response_key]
                    # SDK MarketQuoteV3Api OHLC response has live_ohlc and prev_ohlc (not ohlc)
                    ohlc = getattr(item, 'live_ohlc', None) or getattr(item, 'prev_ohlc', None)
                    return {
                        'open': float(ohlc.open) if ohlc and hasattr(ohlc, 'open') else None,
                        'high': float(ohlc.high) if ohlc and hasattr(ohlc, 'high') else None,
                        'low': float(ohlc.low) if ohlc and hasattr(ohlc, 'low') else None,
                        'close': float(ohlc.close) if ohlc and hasattr(ohlc, 'close') else None,
                        'volume': int(ohlc.volume) if ohlc and hasattr(ohlc, 'volume') else 0,
                        'timestamp': getattr(ohlc, 'ts', None) if ohlc else None
                    }
        except Exception as e:
            self.logger.error(f"OHLC fetch failed for {instrument_key}: {e}")
        
        return None
    
    def get_full_quote(self, instrument_key: str) -> Optional[Dict]:
        try:
            self.fetcher._check_rate_limit()
            response = self.fetcher.quote_api.get_full_market_quote(
                symbol=instrument_key,
                api_version="2.0"
            )
            
            if response.status == 'success' and response.data:
                for api_key, item in response.data.items():
                    actual_token = getattr(item, 'instrument_token', api_key)
                    if actual_token == instrument_key:
                        return {
                            'last_price': float(item.last_price) if hasattr(item, 'last_price') else None,
                            'volume': int(item.volume) if hasattr(item, 'volume') else 0,
                            'open': float(item.ohlc.open) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'open') else None,
                            'high': float(item.ohlc.high) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'high') else None,
                            'low': float(item.ohlc.low) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'low') else None,
                            'close': float(item.ohlc.close) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'close') else None,
                            'timestamp': getattr(item, 'timestamp', None)
                        }
        except Exception as e:
            self.logger.error(f"Full quote fetch failed for {instrument_key}: {e}")
        
        return None


# ============================================================================
# CORRELATION MANAGER
# ============================================================================

@dataclass
class CorrelationRule:
    primary_strategy: StrategyType
    blocked_strategies: List[StrategyType]
    same_expiry_only: bool = True
    reason: str = ""

@dataclass
class CorrelationViolation:
    existing_trade_id: str
    existing_strategy: str
    existing_expiry: str
    proposed_strategy: str
    proposed_expiry: str
    rule: str
    severity: str

class CorrelationManager:
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.rules = [
            CorrelationRule(
                primary_strategy=StrategyType.IRON_FLY,
                blocked_strategies=[StrategyType.IRON_FLY],
                same_expiry_only=False,
                reason="Cannot hold IRON_FLY in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.IRON_CONDOR,
                blocked_strategies=[StrategyType.IRON_CONDOR],
                same_expiry_only=False,
                reason="Cannot hold IRON_CONDOR in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.PROTECTED_STRADDLE,
                blocked_strategies=[StrategyType.PROTECTED_STRADDLE],
                same_expiry_only=False,
                reason="Cannot hold PROTECTED_STRADDLE in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.PROTECTED_STRANGLE,
                blocked_strategies=[StrategyType.PROTECTED_STRANGLE],
                same_expiry_only=False,
                reason="Cannot hold PROTECTED_STRANGLE in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.PROTECTED_STRADDLE,
                blocked_strategies=[StrategyType.PROTECTED_STRANGLE],
                same_expiry_only=True,
                reason="PROTECTED_STRADDLE and PROTECTED_STRANGLE are similar - choose one per expiry"
            ),
        ]
    
    def can_take_position(self, proposed_strategy: ConstructedStrategy) -> Tuple[bool, List[CorrelationViolation]]:
        with self.db_session_factory() as db:
            active_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            violations = []
            
            for rule in self.rules:
                if (proposed_strategy.strategy_type != rule.primary_strategy and
                    proposed_strategy.strategy_type not in rule.blocked_strategies):
                    continue
                
                for trade in active_trades:
                    try:
                        trade_strategy = StrategyType(trade.strategy_type)
                        
                        is_primary_violation = (
                            proposed_strategy.strategy_type == rule.primary_strategy and
                            trade_strategy in rule.blocked_strategies
                        )
                        
                        is_blocked_violation = (
                            proposed_strategy.strategy_type in rule.blocked_strategies and
                            trade_strategy == rule.primary_strategy
                        )
                        
                        if is_primary_violation or is_blocked_violation:
                            if rule.same_expiry_only:
                                if trade.expiry_date.date() != proposed_strategy.expiry_date:
                                    continue
                            
                            violations.append(CorrelationViolation(
                                existing_trade_id=trade.strategy_id,
                                existing_strategy=trade.strategy_type,
                                existing_expiry=trade.expiry_type,
                                proposed_strategy=proposed_strategy.strategy_type.value,
                                proposed_expiry=proposed_strategy.expiry_type.value,
                                rule=rule.reason,
                                severity="BLOCK"
                            ))
                            break
                            
                    except Exception as e:
                        self.logger.error(f"Error checking trade {trade.strategy_id}: {e}")
                        continue
            
            strategy_counts = {}
            for trade in active_trades:
                strategy_counts[trade.strategy_type] = strategy_counts.get(trade.strategy_type, 0) + 1
            
            proposed_name = proposed_strategy.strategy_type.value
            strategy_counts[proposed_name] = strategy_counts.get(proposed_name, 0) + 1
            
            max_concurrent = DynamicConfig.get("MAX_CONCURRENT_SAME_STRATEGY")
            if strategy_counts.get(proposed_name, 0) > max_concurrent:
                violations.append(CorrelationViolation(
                    existing_trade_id="AGGREGATE",
                    existing_strategy="MULTIPLE",
                    existing_expiry="VARIOUS",
                    proposed_strategy=proposed_name,
                    proposed_expiry=proposed_strategy.expiry_type.value,
                    rule=f"Cannot have more than {max_concurrent} concurrent {proposed_name} positions",
                    severity="BLOCK"
                ))
            
            return len(violations) == 0, violations
    
    def get_correlation_report(self) -> Dict:
        with self.db_session_factory() as db:
            active_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            report = {
                "by_strategy": {},
                "warnings": []
            }
            
            for trade in active_trades:
                strategy = trade.strategy_type
                if strategy not in report["by_strategy"]:
                    report["by_strategy"][strategy] = {
                        "count": 0,
                        "trades": [],
                        "expiries": set()
                    }
                
                report["by_strategy"][strategy]["count"] += 1
                report["by_strategy"][strategy]["trades"].append(trade.strategy_id)
                report["by_strategy"][strategy]["expiries"].add(trade.expiry_type)
            
            max_concurrent = DynamicConfig.get("MAX_CONCURRENT_SAME_STRATEGY")
            for strategy, data in report["by_strategy"].items():
                if data["count"] > max_concurrent:
                    report["warnings"].append(
                        f"⚠️ {data['count']} concurrent {strategy} positions - maximum is {max_concurrent}"
                    )
                
                if strategy in ["IRON_FLY", "IRON_CONDOR", "PROTECTED_STRADDLE", "PROTECTED_STRANGLE"]:
                    if len(data["expiries"]) > 1:
                        report["warnings"].append(
                            f"⚠️ {strategy} held across multiple expiries: {', '.join(data['expiries'])}"
                        )
            
            for strategy in report["by_strategy"]:
                report["by_strategy"][strategy]["expiries"] = list(
                    report["by_strategy"][strategy]["expiries"]
                )
            
            return report


# ============================================================================
# INTELLIGENCE LAYER
# Macro Collector, News Scanner, Cross-Asset Engine,
# Claude Client, Prompt Templates, Response Parser,
# Morning Brief Agent, Pre-Trade Agent, Monitor Agent
# ============================================================================

IST_TZ = pytz.timezone("Asia/Kolkata")

# ── Asset & Macro Snapshot ──────────────────────────────────────────────────

@dataclass
class AssetSnapshot:
    symbol: str
    name: str
    price: Optional[float]
    change_pct: Optional[float]
    direction: str = "UNKNOWN"
    note: str = ""
    fetched_at: str = ""

    def __post_init__(self):
        if not self.fetched_at:
            self.fetched_at = datetime.utcnow().isoformat()
        if self.change_pct is None:
            self.direction = "UNKNOWN"
        elif self.change_pct > 0.15:
            self.direction = "UP"
        elif self.change_pct < -0.15:
            self.direction = "DOWN"
        else:
            self.direction = "FLAT"

    def to_dict(self):
        return {
            "symbol": self.symbol, "name": self.name,
            "price": self.price, "change_pct": self.change_pct,
            "direction": self.direction, "note": self.note,
        }


@dataclass
class MacroSnapshot:
    timestamp: str = ""
    sp500: Optional[AssetSnapshot] = None
    nasdaq: Optional[AssetSnapshot] = None
    dow: Optional[AssetSnapshot] = None
    us_vix: Optional[AssetSnapshot] = None
    nikkei: Optional[AssetSnapshot] = None
    hang_seng: Optional[AssetSnapshot] = None
    nifty_prev: Optional[AssetSnapshot] = None
    us_10y_yield: Optional[AssetSnapshot] = None
    dxy: Optional[AssetSnapshot] = None
    usd_inr: Optional[AssetSnapshot] = None
    crude_wti: Optional[AssetSnapshot] = None
    crude_brent: Optional[AssetSnapshot] = None
    gold: Optional[AssetSnapshot] = None
    bitcoin: Optional[AssetSnapshot] = None
    ethereum: Optional[AssetSnapshot] = None
    global_tone: str = "UNKNOWN"
    risk_off_signals: int = 0
    risk_on_signals: int = 0
    crypto_is_macro_driven: bool = False
    us_10y_elevated: bool = False
    fetch_errors: list = field(default_factory=list)

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self):
        def _a(a): return a.to_dict() if a else None
        return {
            "timestamp": self.timestamp,
            "global_tone": self.global_tone,
            "risk_off_signals": self.risk_off_signals,
            "risk_on_signals": self.risk_on_signals,
            "crypto_is_macro_driven": self.crypto_is_macro_driven,
            "us_10y_elevated": self.us_10y_elevated,
            "assets": {
                "sp500": _a(self.sp500), "nasdaq": _a(self.nasdaq),
                "dow": _a(self.dow), "us_vix": _a(self.us_vix),
                "nikkei": _a(self.nikkei), "hang_seng": _a(self.hang_seng),
                "nifty_prev": _a(self.nifty_prev),
                "us_10y_yield": _a(self.us_10y_yield),
                "dxy": _a(self.dxy), "usd_inr": _a(self.usd_inr),
                "crude_wti": _a(self.crude_wti), "crude_brent": _a(self.crude_brent),
                "gold": _a(self.gold),
                "bitcoin": _a(self.bitcoin), "ethereum": _a(self.ethereum),
            },
            "fetch_errors": self.fetch_errors,
        }


# ── Macro Collector ──────────────────────────────────────────────────────────

# Twelve Data symbols — maps snapshot field name → (td_symbol, display_name)
# Free tier: 800 API calls/day. These 11 calls = ~11 calls per refresh (15 min cache = ~50/day max).
_V5_TD_TICKERS = {
    # ETF proxies for indices — universally available on Twelve Data free tier
    "sp500":       {"td": "SPY",     "name": "S&P 500 (SPY ETF)"},
    "nasdaq":      {"td": "QQQ",     "name": "Nasdaq (QQQ ETF)"},
    "dow":         {"td": "DIA",     "name": "Dow Jones (DIA ETF)"},
    "us_vix":      {"td": "VXX",     "name": "VIX (VXX ETN)"},
    "nikkei":      {"td": "EWJ",     "name": "Nikkei (EWJ ETF)"},
    "hang_seng":   {"td": "EWH",     "name": "Hang Seng (EWH ETF)"},
    "dxy":         {"td": "UUP",     "name": "USD Index (UUP ETF)"},
    # Forex pairs — native Twelve Data free tier support
    "usd_inr":     {"td": "USD/INR", "name": "USD/INR"},
    "gold":        {"td": "XAU/USD", "name": "Gold (XAU/USD)"},
    # Commodity ETFs — avoids raw commodity symbol errors on free tier
    "crude_wti":   {"td": "USO",     "name": "Crude WTI (USO ETF)"},
    "crude_brent": {"td": "BNO",     "name": "Crude Brent (BNO ETF)"},
}
# US 10Y yield fetched from FRED (more reliable than Twelve Data for bonds)
# Crypto fetched from CoinGecko (already implemented, no change needed)
# NIFTY prev close fetched from Upstox SDK (already connected)

class V5MacroCollector:
    _instance = None
    _lock = _v5_threading.RLock()

    def __init__(self, cache_ttl: int = 900):
        self._cache: Optional[MacroSnapshot] = None
        self._cache_time: Optional[float] = None
        self._ttl = cache_ttl
        self._ilock = _v5_threading.RLock()
        self._bg_refresh_running = False   # prevents duplicate background refreshes
        self.logger = logging.getLogger("V5MacroCollector")

    @classmethod
    def get(cls, cache_ttl: int = 900) -> "V5MacroCollector":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(cache_ttl)
            return cls._instance

    def get_snapshot(self, force: bool = False) -> MacroSnapshot:
        """
        Always returns immediately.
        - If cache is fresh: returns cached data (zero latency).
        - If cache is stale and force=False: returns stale cache immediately
          AND triggers a background thread to refresh it — no blocking caller.
        - If no cache exists yet (cold start) or force=True: blocks once to
          fetch fresh data. Cold start is handled by the prewarm task.
        """
        with self._ilock:
            now = time.time()
            is_fresh = (
                self._cache is not None
                and self._cache_time is not None
                and (now - self._cache_time) < self._ttl
            )
            if is_fresh and not force:
                return self._cache
            # Stale but have data — return stale, refresh in background
            if self._cache is not None and not force and not self._bg_refresh_running:
                self._bg_refresh_running = True
                t = _v5_threading.Thread(target=self._bg_refresh, daemon=True, name="macro-refresh")
                t.start()
                return self._cache  # return stale data immediately, never block API

        # No cache at all (cold start) or force=True — must block once
        snap = self._fetch()
        with self._ilock:
            self._cache = snap
            self._cache_time = time.time()
        return snap

    def _bg_refresh(self):
        """Background thread: fetch fresh data and update cache without blocking callers."""
        try:
            snap = self._fetch()
            with self._ilock:
                self._cache = snap
                self._cache_time = time.time()
        except Exception as e:
            self.logger.warning(f"Background macro refresh failed: {e}")
        finally:
            with self._ilock:
                self._bg_refresh_running = False

    def _fetch(self) -> MacroSnapshot:
        """
        Fetch global macro data using:
        - Twelve Data API for equities, FX, commodities (free, AWS-friendly)
          Uses BATCHED /quote calls — 2 requests instead of 11 individual ones,
          keeping well under the 8 credits/minute free tier limit.
        - FRED API for US 10Y yield (Federal Reserve, always reliable)
        - CoinGecko for BTC/ETH (no key needed)
        - Upstox SDK for NIFTY prev close (already connected)
        yfinance removed — blocked on AWS datacenter IPs.
        """
        snap = MacroSnapshot()
        errors = []

        # ── Twelve Data: batched requests ──────────────────────────────────────
        # Free tier = 8 API credits/minute. Each symbol = 1 credit.
        # Split 11 symbols into batches of 7 max. Wait 62s between batches
        # so each batch lands in a fresh rate-limit window.
        # IMPORTANT: _fetch() is only called from:
        #   (a) prewarm — runs in run_in_executor (thread pool), blocking is fine
        #   (b) _bg_refresh() — background daemon thread, blocking is fine
        #   (c) cold-start (no cache yet) — acceptable one-time delay at boot
        # API endpoints always get cached data via get_snapshot() — never blocked.
        if V5_TWELVEDATA:
            _items = list(_V5_TD_TICKERS.items())
            batch_size = 7  # 7 credits per batch, safely under 8/min limit
            batches = [_items[i:i+batch_size] for i in range(0, len(_items), batch_size)]
            for batch_idx, batch in enumerate(batches):
                if batch_idx > 0:
                    # Wait for rate limit window to reset before next batch
                    time.sleep(62)
                results = self._td_fetch_batch(batch)
                for k, asset in results.items():
                    if asset is not None:
                        setattr(snap, k, asset)
                    else:
                        errors.append(f"td_{k}: no data returned")
        else:
            errors.append("TWELVE_DATA_API_KEY not set — global macro data unavailable. Free key at twelvedata.com")

        # ── FRED: US 10Y Treasury Yield ────────────────────────────────────────
        if V5_FRED:
            try:
                snap.us_10y_yield = self._fred_fetch_10y()
            except Exception as e:
                errors.append(f"fred_10y: {e}")
        else:
            errors.append("FRED_API_KEY not set — US 10Y yield unavailable. Free key at fred.stlouisfed.org")

        # ── CoinGecko: BTC + ETH (no key, always free) ──────────────────────────
        try:
            snap.bitcoin, snap.ethereum = self._cg_fetch()
        except Exception as e:
            errors.append(f"coingecko: {e}")

        # ── Upstox: NIFTY prev close (already connected, zero extra cost) ─────────────
        try:
            snap.nifty_prev = self._upstox_nifty_prev()
        except Exception as e:
            errors.append(f"upstox_nifty: {e}")

        snap.fetch_errors = errors
        self._derive_signals(snap)
        self.logger.info(
            f"MacroSnapshot: tone={snap.global_tone} off={snap.risk_off_signals} "
            f"on={snap.risk_on_signals} errors={len(errors)}"
        )
        return snap

    def _td_fetch(self, key: str, cfg: dict) -> Optional[AssetSnapshot]:
        """Fetch a single asset from Twelve Data REST API. Works on any server including AWS."""
        try:
            url = "https://api.twelvedata.com/quote"
            params = {
                "symbol": cfg["td"],
                "apikey": _TWELVE_DATA_KEY,
                "dp": 4,
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") == "error" or "close" not in data:
                self.logger.warning(f"Twelve Data {key}: {data.get('message','no data')}")
                return None
            price = float(data["close"])
            prev_close = float(data.get("previous_close") or data["close"])
            change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close else None
            return AssetSnapshot(
                symbol=cfg["td"],
                name=cfg["name"],
                price=round(price, 4),
                change_pct=change_pct,
            )
        except Exception as e:
            self.logger.warning(f"_td_fetch {key}: {e}")
            return None

    def _td_fetch_batch(self, batch: list) -> dict:
        """
        Fetch multiple assets from Twelve Data in a single API call.
        Uses the /quote endpoint with comma-separated symbols.
        One API request = len(batch) credits, but only 1 HTTP call.
        Returns dict keyed by VolGuard asset key (e.g. 'sp500', 'dxy').
        """
        results = {k: None for k, _ in batch}
        if not batch:
            return results
        try:
            # Build symbol→key reverse map for parsing response
            symbol_to_key = {cfg["td"]: k for k, cfg in batch}
            key_to_cfg    = {k: cfg for k, cfg in batch}
            symbols_str   = ",".join(cfg["td"] for _, cfg in batch)

            url = "https://api.twelvedata.com/quote"
            params = {
                "symbol": symbols_str,
                "apikey": _TWELVE_DATA_KEY,
                "dp": 4,
            }
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()

            # Single symbol → response is the quote object directly
            # Multiple symbols → response is {"SYMBOL": {quote}, ...}
            if len(batch) == 1:
                symbol = list(symbol_to_key.keys())[0]
                key    = symbol_to_key[symbol]
                results[key] = self._parse_td_quote(data, key_to_cfg[key])
            else:
                for symbol, key in symbol_to_key.items():
                    quote = data.get(symbol, {})
                    if not quote:
                        self.logger.warning(f"Twelve Data batch: no data for {symbol}")
                        continue
                    if quote.get("status") == "error":
                        self.logger.warning(f"Twelve Data {symbol}: {quote.get('message','error')}")
                        continue
                    results[key] = self._parse_td_quote(quote, key_to_cfg[key])

        except Exception as e:
            self.logger.warning(f"_td_fetch_batch error: {e}")

        return results

    def _parse_td_quote(self, data: dict, cfg: dict) -> Optional[AssetSnapshot]:
        """Parse a single Twelve Data quote dict into an AssetSnapshot."""
        try:
            if not data or "close" not in data:
                return None
            price      = float(data["close"])
            prev_close = float(data.get("previous_close") or data["close"])
            change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close else None
            return AssetSnapshot(
                symbol=cfg["td"],
                name=cfg["name"],
                price=round(price, 4),
                change_pct=change_pct,
            )
        except Exception as e:
            self.logger.warning(f"_parse_td_quote: {e}")
            return None

    def _fred_fetch_10y(self) -> Optional[AssetSnapshot]:
        """
        Fetch US 10Y Treasury yield from FRED (Federal Reserve Economic Data).
        Series: DGS10. Completely free, never blocks datacenter IPs.
        Returns today’s or most recent available value.
        """
        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id": "DGS10",
                "api_key": _FRED_KEY,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 5,
                "observation_start": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            obs = [o for o in data.get("observations", []) if o.get("value") != "."]
            if not obs:
                return None
            latest = obs[0]
            price = float(latest["value"])
            # Compute change vs previous observation
            change_pct = None
            if len(obs) >= 2:
                prev = float(obs[1]["value"])
                if prev > 0:
                    # Yield change in basis points → express as % change of the yield level
                    change_pct = round((price - prev) / prev * 100, 2)
            return AssetSnapshot(
                symbol="DGS10",
                name="US 10Y Treasury Yield",
                price=round(price, 3),
                change_pct=change_pct,
                note=f"FRED as of {latest['date']}",
            )
        except Exception as e:
            self.logger.warning(f"_fred_fetch_10y: {e}")
            return None

    def _upstox_nifty_prev(self) -> Optional[AssetSnapshot]:
        """Get NIFTY prev close from Upstox — already connected, zero extra API calls."""
        try:
            global volguard_system
            if not volguard_system:
                return None
            price = volguard_system.fetcher.get_ltp_with_fallback("NSE_INDEX|Nifty 50")
            if not price or price <= 0:
                return None
            return AssetSnapshot(
                symbol="NIFTY",
                name="Nifty 50 (live/prev ref via Upstox)",
                price=round(price, 2),
                change_pct=None,
            )
        except Exception as e:
            self.logger.warning(f"_upstox_nifty_prev: {e}")
            return None

    def _cg_fetch(self):
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin,ethereum", "vs_currencies": "usd",
                    "include_24hr_change": "true"},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        def _make(key, name):
            p = data.get(key, {}).get("usd")
            c = data.get(key, {}).get("usd_24h_change")
            if p is None:
                return None
            return AssetSnapshot(symbol=key.upper()[:3], name=name,
                                 price=round(p, 2),
                                 change_pct=round(c, 2) if c else None)
        return _make("bitcoin", "Bitcoin"), _make("ethereum", "Ethereum")

    def _derive_signals(self, snap: MacroSnapshot):
        ro, ron = 0, 0
        if snap.sp500 and snap.sp500.change_pct is not None:
            if snap.sp500.change_pct < -0.5: ro += 1
            elif snap.sp500.change_pct > 0.5: ron += 1
        if snap.us_vix and snap.us_vix.price is not None:
            if snap.us_vix.price > 25: ro += 2; snap.us_vix.note = "ELEVATED risk-off"
            elif snap.us_vix.price > 20: ro += 1; snap.us_vix.note = "ABOVE NORMAL"
        if snap.us_10y_yield and snap.us_10y_yield.price is not None:
            if snap.us_10y_yield.price > 4.5:
                ro += 1; snap.us_10y_elevated = True
                snap.us_10y_yield.note = f"ELEVATED at {snap.us_10y_yield.price:.2f}%"
            if snap.us_10y_yield.change_pct and snap.us_10y_yield.change_pct > 1.5:
                ro += 1; snap.us_10y_yield.note += " | Rising sharply"
        if snap.gold and snap.gold.change_pct is not None:
            if snap.gold.change_pct > 1.0: ro += 1; snap.gold.note = "Rising — risk-off hedging"
            elif snap.gold.change_pct < -1.0: ron += 1
        if snap.crude_wti and snap.crude_wti.change_pct is not None:
            if snap.crude_wti.change_pct > 2.0:
                ro += 1; snap.crude_wti.note = "Sharp rise — INR pressure risk"
        asian_down = sum(1 for a in [snap.nikkei, snap.hang_seng]
                         if a and a.change_pct is not None and a.change_pct < -0.5)
        asian_up = sum(1 for a in [snap.nikkei, snap.hang_seng]
                       if a and a.change_pct is not None and a.change_pct > 0.5)
        if asian_down >= 2: ro += 1
        elif asian_up >= 2: ron += 1
        if snap.bitcoin and snap.bitcoin.change_pct is not None:
            btc = snap.bitcoin.change_pct
            sp = snap.sp500.change_pct if snap.sp500 else 0
            if btc < -5 and sp is not None and sp < -0.3:
                snap.crypto_is_macro_driven = True
                snap.bitcoin.note = "Falling with equities — macro driven"
                ro += 1
            elif btc < -5:
                snap.bitcoin.note = "Falling but equities stable — crypto-native"
        snap.risk_off_signals = ro
        snap.risk_on_signals = ron
        if ro == 0 and ron >= 1: snap.global_tone = "CLEAR"
        elif ro <= 1 and not snap.us_10y_elevated: snap.global_tone = "CAUTIOUS_NEUTRAL"
        elif ro == 2 or (ro == 1 and snap.us_10y_elevated): snap.global_tone = "CAUTIOUS"
        elif ro >= 3: snap.global_tone = "RISK_OFF"
        else: snap.global_tone = "MIXED"

    def build_context_string(self, snap: MacroSnapshot) -> str:
        def _f(a):
            if a is None: return "N/A"
            parts = []
            if a.price is not None: parts.append(f"{a.price:,.2f}")
            if a.change_pct is not None:
                parts.append(f"{'+'if a.change_pct>=0 else ''}{a.change_pct:.2f}% ({a.direction})")
            if a.note: parts.append(f"| {a.note}")
            return " ".join(parts)
        lines = [
            f"=== GLOBAL MARKET SNAPSHOT === {snap.timestamp}",
            f"Global Tone: {snap.global_tone} | Risk-Off: {snap.risk_off_signals} | Risk-On: {snap.risk_on_signals}",
            "",
            "--- US EQUITIES ---",
            f"S&P 500:   {_f(snap.sp500)}",
            f"Nasdaq:    {_f(snap.nasdaq)}",
            f"Dow:       {_f(snap.dow)}",
            f"VIX:       {_f(snap.us_vix)}",
            "",
            "--- ASIA ---",
            f"Nikkei:    {_f(snap.nikkei)}",
            f"HangSeng:  {_f(snap.hang_seng)}",
            f"Nifty Prev: {_f(snap.nifty_prev)}  [prev-close ref; real Gift Nifty on IFSC terminal]",
            "",
            "--- BONDS & FX ---",
            f"US 10Y:    {_f(snap.us_10y_yield)}",
            f"DXY:       {_f(snap.dxy)}",
            f"USD/INR:   {_f(snap.usd_inr)}",
            f"10Y Elevated: {'YES' if snap.us_10y_elevated else 'No'}",
            "",
            "--- COMMODITIES ---",
            f"Crude WTI:  {_f(snap.crude_wti)}",
            f"Crude Brent:{_f(snap.crude_brent)}",
            f"Gold:       {_f(snap.gold)}",
            "",
            "--- CRYPTO ---",
            f"Bitcoin:   {_f(snap.bitcoin)}",
            f"Ethereum:  {_f(snap.ethereum)}",
            f"Crypto macro-driven: {'YES — treat as equity risk signal' if snap.crypto_is_macro_driven else 'No — crypto-native'}",
        ]
        return "\n".join(lines)

    def gift_nifty_signal(self, snap: MacroSnapshot) -> str:
        """
        Real Gift Nifty (NSE IFSC) is not available on yfinance.
        We use cross-asset synthesis instead: S&P direction + USD/INR + US VIX
        to estimate implied Nifty gap direction. Honest and more informative
        than showing stale ^NSEI and calling it Gift Nifty.
        """
        signals = []
        gap_points = 0.0

        # S&P 500 overnight move → ~0.5x correlation to Nifty gap
        if snap.sp500 and snap.sp500.change_pct is not None:
            sp_contrib = snap.sp500.change_pct * 0.5
            gap_points += sp_contrib
            signals.append(f"S&P {snap.sp500.change_pct:+.1f}% → Nifty ~{sp_contrib:+.1f}%")

        # USD/INR: rupee weakens → negative for Nifty (FII outflow pressure)
        if snap.usd_inr and snap.usd_inr.change_pct is not None:
            inr_contrib = -snap.usd_inr.change_pct * 0.3
            gap_points += inr_contrib
            if abs(snap.usd_inr.change_pct) > 0.3:
                signals.append(f"INR {snap.usd_inr.change_pct:+.2f}% → pressure {inr_contrib:+.1f}%")

        # US VIX spike → amplify negative signal
        if snap.us_vix and snap.us_vix.change_pct is not None and snap.us_vix.change_pct > 10:
            gap_points -= 0.3
            signals.append(f"VIX spike +{snap.us_vix.change_pct:.0f}% → additional drag")

        if not signals:
            return "Cross-asset gap signal: insufficient overnight data"

        direction = "GAP UP" if gap_points > 0.3 else "GAP DOWN" if gap_points < -0.3 else "FLAT OPEN"
        basis = " | ".join(signals)
        note = "[Note: Real Gift Nifty futures unavailable on yfinance — this is cross-asset synthesis]"
        return f"{direction} implied (~{gap_points:+.1f}%) via: {basis}. {note}"


# ── News Scanner ─────────────────────────────────────────────────────────────

_V5_RSS_FEEDS = {
    # India official & reliable feeds (always work, government servers)
    "rbi_official":      "https://www.rbi.org.in/rss/RBIRss.aspx",
    "nse_official":      "https://www.nseindia.com/feed/rssdata/news.xml",
    "pib_official":      "https://pib.gov.in/RssMain.aspx",
    # ET feeds (AWS-friendly, no IP restrictions)
    "et_markets":        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "et_economy":        "https://economictimes.indiatimes.com/economy/rssfeeds/1373380680.cms",
    # Google News RSS — aggregates AP, Bloomberg, WSJ, Reuters in real-time.
    # These catch Trump tweets, war declarations, Fed emergency statements within 2-3 min.
    # Completely free, no API key, works on any server including AWS.
    "google_us_markets":  "https://news.google.com/rss/search?q=wall+street+stock+market+today&hl=en&gl=US&ceid=US:en",
    "google_trump_macro": "https://news.google.com/rss/search?q=trump+tariff+trade+economy&hl=en&gl=US&ceid=US:en",
    "google_fed":         "https://news.google.com/rss/search?q=federal+reserve+rate+decision+powell&hl=en&gl=US&ceid=US:en",
    "google_india_macro": "https://news.google.com/rss/search?q=india+RBI+SEBI+nifty+rupee&hl=en&gl=IN&ceid=IN:en",
    "google_war_geo":     "https://news.google.com/rss/search?q=war+conflict+geopolitical+sanctions&hl=en&gl=US&ceid=US:en",
    "google_oil_opec":    "https://news.google.com/rss/search?q=crude+oil+opec+supply+shock&hl=en&gl=US&ceid=US:en",
    # AP Business — no IP restrictions, gold-standard breaking news
    "ap_business":        "https://feeds.apnews.com/apnews/business",
}

# ── Keyword classification for news triage ──────────────────────────────────
# VETO: only events that ACTUALLY halt/threaten markets imminently
# NOT analyst predictions — only decisions/announcements/emergencies
_V5_VETO_KW = [
    # RBI policy decisions (not speculation/analysis)
    "rbi monetary policy decision","mpc decision","mpc resolution","mpc announcement",
    "repo rate changed","repo rate hiked","repo rate cut","rbi cuts rate","rbi hikes rate",
    "rbi emergency","rbi circular ban","rbi moratorium",
    # FOMC decisions (not previews)
    "fomc decision","fed raises rates","fed cuts rates","federal reserve raises",
    "federal reserve cuts","emergency fed meeting",
    # India macro events — the decisions themselves
    "union budget","budget speech","vote on account",
    # Market halt events
    "circuit breaker","lower circuit","upper circuit","market halt","trading suspended",
    "nse halted","bse halted","trading ban","sebi ban","sebi trading ban",
    # Geopolitical/systemic
    "war declared","nuclear","martial law","pandemic declared","lockdown announced",
    "bank failure","bank collapse","default declared","sanctions imposed",
]

# HIGH IMPACT: data releases and significant but not immediate market-halting events
_V5_HIGH_KW = [
    # Indian macro data
    "india cpi","india inflation","india gdp","india iip","india wpi",
    "india trade deficit","india current account",
    # US macro data (market-moving releases)
    "non-farm payroll","nfp report","us cpi","us inflation data","us gdp",
    "us jobs report","federal reserve minutes","fomc minutes","fed chair speech",
    # Tariff / trade war
    "tariff announcement","tariff hike","trade war escalation","trade deal",
    # India-specific alerts
    "nifty crash","nifty circuit","rupee crisis","rupee hits record",
    "crude oil spike","oil shock","fii sell-off","fii outflow",
    "sebi order","sebi notice","rbi notice","rbi action",
    # Banking/corporate
    "bank failure","bank crisis","debt ceiling","us debt ceiling",
    "rbi governor","sebi chief","nse ceo",
    # Geopolitical with market impact
    "india china border","india pakistan","missile","terror attack india",
]

# WATCH: background awareness items
_V5_WATCH_KW = [
    "nifty","sensex","india vix","nse","bse",
    "fii","dii","institutional","foreign investor",
    "sebi","rbi","finance minister",
    "federal reserve","fed","us markets","wall street",
    "crude oil","opec","brent","wti",
    "rupee","usd inr","dollar",
    "inflation","interest rate","yield",
    "china markets","hong kong",
    "bitcoin","crypto","ethereum",
    "quarterly results","earnings","q1 results","q2 results","q3 results","q4 results",
    "capex","gdp forecast","imf","world bank",
]


@dataclass
class V5NewsItem:
    title: str
    source: str
    link: str
    published: str
    summary: str
    level: str
    matched_keywords: List[str] = field(default_factory=list)
    item_id: str = ""

    def __post_init__(self):
        if not self.item_id:
            self.item_id = hashlib.md5(f"{self.title}{self.source}".encode()).hexdigest()


@dataclass
class V5NewsScanResult:
    timestamp: str
    veto_items: List[V5NewsItem] = field(default_factory=list)
    high_impact_items: List[V5NewsItem] = field(default_factory=list)
    watch_items: List[V5NewsItem] = field(default_factory=list)
    total_scanned: int = 0
    fetch_errors: List[str] = field(default_factory=list)

    @property
    def has_veto(self): return len(self.veto_items) > 0

    @property
    def has_high_impact(self): return len(self.high_impact_items) > 0

    def format_for_prompt(self) -> str:
        lines = []
        if self.veto_items:
            lines.append("⚠️ VETO-LEVEL NEWS:")
            for i in self.veto_items[:3]: lines.append(f"  • [{i.source}] {i.title}")
        if self.high_impact_items:
            lines.append("📊 HIGH IMPACT:")
            for i in self.high_impact_items[:4]: lines.append(f"  • [{i.source}] {i.title}")
        if self.watch_items:
            lines.append("👁 WATCH:")
            for i in self.watch_items[:5]: lines.append(f"  • [{i.source}] {i.title}")
        return "\n".join(lines) if lines else "No significant market-moving news in scan window."


class V5NewsScanner:
    _instance = None
    _lock = _v5_threading.RLock()

    def __init__(self, lookback_hours: int = 4, cache_ttl: int = 300):
        self._seen_ids: set = set()
        self._cache: Optional[V5NewsScanResult] = None
        self._cache_time: Optional[float] = None
        self._ttl = cache_ttl
        self._lookback_hours = lookback_hours
        self._ilock = _v5_threading.RLock()
        self.logger = logging.getLogger("V5NewsScanner")

    @classmethod
    def get(cls) -> "V5NewsScanner":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def scan(self, force: bool = False, lookback_hours: int = None) -> V5NewsScanResult:
        with self._ilock:
            now = time.time()
            if (not force and self._cache is not None
                    and self._cache_time is not None
                    and (now - self._cache_time) < self._ttl
                    and lookback_hours is None):
                return self._cache
        result = self._do_scan(lookback_hours or self._lookback_hours)
        with self._ilock:
            self._cache = result
            self._cache_time = time.time()
        return result

    @staticmethod
    def _fetch_feed_with_timeout(url: str, timeout: int = 8):
        """Fetch RSS with hard socket timeout. feedparser has no built-in timeout."""
        import socket as _sock
        old_to = _sock.getdefaulttimeout()
        try:
            _sock.setdefaulttimeout(timeout)
            return feedparser.parse(url)
        finally:
            _sock.setdefaulttimeout(old_to)

    def _do_scan(self, lookback_hours: int) -> V5NewsScanResult:
        if not V5_FEEDPARSER:
            return V5NewsScanResult(
                timestamp=datetime.now(IST_TZ).isoformat(),
                fetch_errors=["feedparser not installed — pip install feedparser"]
            )
        from datetime import timezone as _tz
        from concurrent.futures import ThreadPoolExecutor, as_completed
        cutoff = datetime.now(_tz.utc) - timedelta(hours=lookback_hours)
        result = V5NewsScanResult(timestamp=datetime.now(IST_TZ).isoformat())

        # ── Parallel RSS fetch with per-feed 8s timeout ──
        def _fetch_one(src_url):
            src, url = src_url
            return src, self._fetch_feed_with_timeout(url, timeout=8)

        feeds_fetched = {}
        with ThreadPoolExecutor(max_workers=6, thread_name_prefix="v5_rss") as _ex:
            _futs = {_ex.submit(_fetch_one, (s, u)): s for s, u in _V5_RSS_FEEDS.items()}
            for _fut in as_completed(_futs, timeout=10):
                src_key = _futs[_fut]
                try:
                    src, feed = _fut.result(timeout=1)
                    feeds_fetched[src] = feed
                except Exception as e:
                    result.fetch_errors.append(f"{src_key}: {str(e)[:80]}")

        for src, url in _V5_RSS_FEEDS.items():
            feed = feeds_fetched.get(src)
            if feed is None:
                continue
            try:
                for entry in feed.entries:
                    result.total_scanned += 1
                    pub = getattr(entry, "published", None)
                    if pub:
                        try:
                            pt = email.utils.parsedate_to_datetime(pub)
                            from datetime import timezone as _tz2
                            if pt.tzinfo is None: pt = pt.replace(tzinfo=_tz2.utc)
                            if pt < cutoff: continue
                        except Exception:
                            pass
                    title = getattr(entry, "title", "")
                    summary = getattr(entry, "summary", "")[:300]
                    link = getattr(entry, "link", "")
                    pub_str = getattr(entry, "published", "")
                    item = self._classify(title, summary, src, link, pub_str)
                    if item is None: continue
                    if item.item_id in self._seen_ids: continue
                    self._seen_ids.add(item.item_id)
                    if item.level == "VETO": result.veto_items.append(item)
                    elif item.level == "HIGH_IMPACT": result.high_impact_items.append(item)
                    elif item.level == "WATCH": result.watch_items.append(item)
            except Exception as e:
                result.fetch_errors.append(f"{src}: {str(e)[:80]}")
        if len(self._seen_ids) > 5000:
            self._seen_ids = set(sorted(self._seen_ids)[-2000:])
        self.logger.info(f"NewsScanner: VETO={len(result.veto_items)} HIGH={len(result.high_impact_items)} WATCH={len(result.watch_items)}")
        return result

    def _classify(self, title, summary, source, link, published) -> Optional[V5NewsItem]:
        text = (title + " " + summary).lower()
        mv = [kw for kw in _V5_VETO_KW if kw in text]
        if mv: return V5NewsItem(title=title, source=source, link=link, published=published, summary=summary, level="VETO", matched_keywords=mv)
        mh = [kw for kw in _V5_HIGH_KW if kw in text]
        if mh: return V5NewsItem(title=title, source=source, link=link, published=published, summary=summary, level="HIGH_IMPACT", matched_keywords=mh)
        mw = [kw for kw in _V5_WATCH_KW if kw in text]
        if mw: return V5NewsItem(title=title, source=source, link=link, published=published, summary=summary, level="WATCH", matched_keywords=mw[:3])
        return None


# ── Claude Client ────────────────────────────────────────────────────────────

class V5ClaudeClient:
    """
    Unified LLM client.
    Auto-selects backend from environment:
      ANTHROPIC_API_KEY → Claude Sonnet (primary — best quality)
      GROQ_API_KEY      → Groq llama-3.3-70b (free fallback — active now)
    To upgrade: just add ANTHROPIC_API_KEY to .env. No code change needed.
    """
    _instance = None
    _lock = _v5_threading.RLock()

    # ── Model config per provider ──
    _CLAUDE_MODEL  = "claude-sonnet-4-5"
    _GROQ_MODEL    = "llama-3.3-70b-versatile"   # best free model on Groq
    _GROQ_FALLBACK = "llama3-70b-8192"            # fallback if versatile quota hit

    _CLAUDE_IN_PRICE  = 3.0    # $ per million input tokens
    _CLAUDE_OUT_PRICE = 15.0   # $ per million output tokens
    # Groq free tier: $0 — track calls only, not cost

    def __init__(self):
        if not V5_LLM_READY:
            raise RuntimeError(
                "No LLM backend available. Set GROQ_API_KEY (free) or ANTHROPIC_API_KEY."
            )
        self._provider = _V5_LLM_PROVIDER
        self._client   = None
        self._model    = None

        if self._provider == "groq":
            if not V5_GROQ:
                raise RuntimeError("groq package not installed — pip install groq")
            key = os.getenv("GROQ_API_KEY", "")
            if not key:
                raise RuntimeError("GROQ_API_KEY not set")
            self._client = _GroqClient(api_key=key)
            self._model  = self._GROQ_MODEL
            logging.getLogger("V5ClaudeClient").info(
                f"✅ LLM: Groq [{self._model}] — free tier active"
            )
        elif self._provider == "claude":
            if not V5_ANTHROPIC:
                raise RuntimeError("anthropic package not installed — pip install anthropic")
            key = os.getenv("ANTHROPIC_API_KEY", "")
            if not key:
                raise RuntimeError("ANTHROPIC_API_KEY not set")
            self._client = _anthropic_lib.Anthropic(api_key=key)
            self._model  = self._CLAUDE_MODEL
            logging.getLogger("V5ClaudeClient").info(
                f"✅ LLM: Claude [{self._model}]"
            )

        self._calls     = 0
        self._input_tok = 0
        self._output_tok= 0
        self._cost_usd  = 0.0
        self._ilock     = _v5_threading.RLock()
        self.logger     = logging.getLogger("V5ClaudeClient")

    @classmethod
    def get(cls) -> Optional["V5ClaudeClient"]:
        with cls._lock:
            if cls._instance is None:
                try:
                    cls._instance = cls()
                except Exception as e:
                    logging.getLogger("V5ClaudeClient").error(
                        f"LLM client init failed: {e}"
                    )
                    return None
            return cls._instance

    def call(self, system: str, user: str, max_tokens: int = 1500, ctx: str = "v5") -> Optional[str]:
        if not V5_LLM_READY: return None
        for attempt in range(1, 4):
            try:
                self.logger.info(f"[{self._provider.upper()}|{ctx}] attempt {attempt}")
                t0 = time.time()

                if self._provider == "groq":
                    text = self._call_groq(system, user, max_tokens)
                else:
                    text = self._call_claude(system, user, max_tokens)

                elapsed = time.time() - t0
                with self._ilock:
                    self._calls += 1
                self.logger.info(
                    f"[{self._provider.upper()}|{ctx}] {elapsed:.1f}s ✓"
                )
                return text

            except Exception as e:
                self.logger.warning(
                    f"[{self._provider.upper()}|{ctx}] attempt {attempt} failed: {e}"
                )
                # On Groq rate-limit (429), fall back to smaller model
                if self._provider == "groq" and "429" in str(e):
                    self.logger.warning(
                        f"Groq rate-limit hit — falling back to {self._GROQ_FALLBACK}"
                    )
                    self._model = self._GROQ_FALLBACK
                if attempt < 3:
                    time.sleep(2 * attempt)
        return None

    def _call_groq(self, system: str, user: str, max_tokens: int) -> str:
        """Groq uses OpenAI-compatible chat completions. System is a message, not a param."""
        resp = self._client.chat.completions.create(
            model=self._model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            temperature=0.3,   # lower = more consistent for structured financial analysis
        )
        text = resp.choices[0].message.content or ""
        with self._ilock:
            # Groq reports usage same as OpenAI
            if hasattr(resp, "usage") and resp.usage:
                self._input_tok  += resp.usage.prompt_tokens
                self._output_tok += resp.usage.completion_tokens
        return text

    def _call_claude(self, system: str, user: str, max_tokens: int) -> str:
        """Claude uses Anthropic SDK with separate system param."""
        resp = self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}]
        )
        text = resp.content[0].text if resp.content else ""
        with self._ilock:
            it = resp.usage.input_tokens
            ot = resp.usage.output_tokens
            self._input_tok  += it
            self._output_tok += ot
            self._cost_usd = (
                self._input_tok  / 1e6 * self._CLAUDE_IN_PRICE +
                self._output_tok / 1e6 * self._CLAUDE_OUT_PRICE
            )
        return text

    def call_with_websearch(self, system: str, user: str, max_tokens: int = 2000, ctx: str = "v5_ws") -> Optional[str]:
        """
        Call Claude with web_search tool enabled.
        Claude will autonomously search for current market news and use it in the brief.
        This gives the morning brief real causality — WHY markets moved — not just numbers.
        Only available when ANTHROPIC_API_KEY is set. Falls back to standard call otherwise.
        """
        if self._provider != "claude":
            # Groq doesn’t support tool use — fall back to plain call
            return self.call(system, user, max_tokens, ctx)
        if not V5_ANTHROPIC:
            return self.call(system, user, max_tokens, ctx)
        for attempt in range(1, 4):
            try:
                self.logger.info(f"[CLAUDE|{ctx}|websearch] attempt {attempt}")
                t0 = time.time()
                resp = self._client.messages.create(
                    model=self._model,
                    max_tokens=max_tokens,
                    system=system,
                    messages=[{"role": "user", "content": user}],
                    tools=[{"type": "web_search_20250305", "name": "web_search"}],
                )
                # Extract all text blocks from response (model may use search then respond)
                text = ""
                for block in resp.content:
                    if hasattr(block, "text"):
                        text += block.text
                elapsed = time.time() - t0
                with self._ilock:
                    self._calls += 1
                    if hasattr(resp, "usage") and resp.usage:
                        it = resp.usage.input_tokens
                        ot = resp.usage.output_tokens
                        self._input_tok += it
                        self._output_tok += ot
                        self._cost_usd = (
                            self._input_tok / 1e6 * self._CLAUDE_IN_PRICE +
                            self._output_tok / 1e6 * self._CLAUDE_OUT_PRICE
                        )
                self.logger.info(f"[CLAUDE|{ctx}|websearch] {elapsed:.1f}s ✓")
                return text if text.strip() else None
            except Exception as e:
                self.logger.warning(f"[CLAUDE|{ctx}|websearch] attempt {attempt} failed: {e}")
                if attempt < 3:
                    time.sleep(2 * attempt)
        return None

    def usage(self) -> dict:
        with self._ilock:
            base = {
                "provider":      self._provider,
                "model":         self._model,
                "calls_total":   self._calls,
                "input_tokens":  self._input_tok,
                "output_tokens": self._output_tok,
            }
            if self._provider == "claude":
                base["estimated_cost_usd"] = round(self._cost_usd, 5)
            else:
                base["cost"] = "Free (Groq free tier)"
                base["note"] = "Add ANTHROPIC_API_KEY to .env to upgrade to Claude (primary provider)"
            return base


# ── Prompt Templates ─────────────────────────────────────────────────────────

_V5_BRIEF_SYSTEM = """You are VolGuard's Market Intelligence Agent — macro analyst and overnight risk advisor for an Indian index derivatives desk.

PORTFOLIO YOU ARE ADVISING:
- Overnight short premium on Nifty 50 index options: Iron Fly, Iron Condor, Protected Straddle/Strangle, directional spreads
- Product type: D (delivery/carryforward) — positions held overnight, NOT intraday
- Base capital ~₹15 lakh; ₹3–6L deployed per expiry leg across weekly / monthly / next-weekly books
- GTT stop-losses always placed at 2x premium received; circuit breaker at 3% daily loss
- Square-off mandatory 1 day before expiry at 14:00 IST
- EXISTENTIAL RISK for this book: overnight gap-opens driven by macro shocks, VIX explosions, binary events

YOUR MANDATE:
Synthesize the overnight global data into a Morning Intelligence Brief that answers one question: what did the world do while India slept, and what does it mean for a short-premium Nifty book opening in the next 45 minutes?

RULES — NON-NEGOTIABLE:
1. You interpret ONLY the data provided. Never invent, assume, or extrapolate a number not given to you.
2. Write like a prop desk macro analyst — dense, direct, zero filler. No "in conclusion", no "it is worth noting".
3. CAUSALITY over description: not "VIX rose 8%" but "VIX spiked 8% as NFP came in hot, repricing Fed cut expectations — this directly inflates India VIX at open and widens ATM straddle premium by ~15–20%".
4. Cross-asset contradictions are the most important signal you can find. Two assets moving in opposite directions from what the narrative predicts = something is being mispriced. Name it.
5. Always translate global moves to India-specific mechanics: how does US 10Y rising affect FII flows into India? How does DXY+crude rising simultaneously hit INR? How does Hang Seng down 2% affect SGX Nifty at open?
6. Conservative default. This portfolio loses money on gap events, not on slow trends. Bias toward flagging tail risks.
7. GLOBAL TONE must be EXACTLY one word-pair from: CLEAR / CAUTIOUS_NEUTRAL / CAUTIOUS / RISK_OFF / MIXED
   CLEAR = aligned risk-on globally, no binary events nearby, short premium contextually supported
   CAUTIOUS_NEUTRAL = mixed signals, proceed but size at 75-80% of normal deployment  
   CAUTIOUS = meaningful risk-off or contradictions present, reduce deployment, prefer wider structures
   RISK_OFF = multiple aligned risk-off signals, skip new entries or hedge existing book
   MIXED = genuinely contradictory signals, no clean read — treat as CAUTIOUS until clarity emerges

8. CAUSALITY CHAIN IS MANDATORY — NON-NEGOTIABLE:
   Every market move you mention must be traced through a complete transmission chain ending at India.
   NEVER ACCEPTABLE: "S&P fell 1.2%" or "VIX rose 8%"
   ALWAYS REQUIRED: "S&P fell 1.2% because [specific catalyst] → reprices [Fed/earnings/macro] →
   flows into India via [FII selling / INR pressure / sentiment] → Nifty opens [direction] and
   ATM straddle premium widens/narrows by approximately [Y]%"
   If you cannot explain WHY a market moved, say that explicitly. Never describe without cause.

9. CONTRADICTION DETECTION IS THE HEADLINE:
   US up but Asia down. VIX rising while S&P flat. Gold up while equities also up.
   These contradictions are MORE IMPORTANT than any individual market direction.
   Lead with the contradiction. Name what is being mispriced. This is where the real overnight risk hides."""

_V5_BRIEF_USER = """
════════════════════════════════════════
OVERNIGHT GLOBAL MARKET DATA
════════════════════════════════════════
{macro_context}

════════════════════════════════════════
RECENT NEWS — RSS SCAN (last 4 hours)
════════════════════════════════════════
{news_context}

════════════════════════════════════════
VOLGUARD PORTFOLIO STATUS (08:30 IST)
════════════════════════════════════════
India VIX (current)   : {india_vix}
IVP 1-Year            : {ivp}   [<25=cheap vol, >75=rich vol]
FII Net Flow Yesterday: {fii_net}
Gift Nifty Signal     : {gift_nifty_signal}

Upcoming VETO Events (will block new trades):
{veto_events}

Upcoming High-Impact Events (require caution):
{high_impact_events}

════════════════════════════════════════
TASK: MORNING INTELLIGENCE BRIEF
════════════════════════════════════════
Produce the brief using EXACTLY this structure.
Each section must be prose — no bullet points within sections.
Be specific. Vague statements like "markets were volatile" are useless.

GLOBAL TONE: [CLEAR / CAUTIOUS_NEUTRAL / CAUTIOUS / RISK_OFF / MIXED]

US SESSION SUMMARY:
[3-4 sentences. What drove the US session — economic data, Fed commentary, earnings, geopolitics? What does the S&P/Nasdaq close and VIX level tell us? What does this mean for Indian markets opening today?]

ASIAN SESSION:
[2-3 sentences. Current state of Nikkei, Hang Seng. Any divergences from US? Is Asia confirming or contradicting the US overnight move?]

GIFT NIFTY SIGNAL:
[2 sentences. State the implied gap direction and magnitude. What should we expect at 09:15 IST open — and is the Gift Nifty move consistent with the broader overnight tone or is it diverging?]

MACRO GAUGES:
[3-4 sentences. Assess US 10Y yield, DXY, crude oil (WTI/Brent), and USD/INR as a system. Are they telling a coherent story? If crude is up AND DXY is up AND yields are rising simultaneously — that is a triple negative for India. Name the mechanism.]

CRYPTO SIGNAL:
[2 sentences. Is BTC/ETH down because of macro (falling alongside equities = risk-off confirmation) or crypto-native reasons (falling while equities stable = isolated)? What is the correct interpretation for equity risk today?]

CROSS-ASSET COHERENCE:
[3-4 sentences. This is the most important section. Look across ALL assets — are they telling the same story or are there contradictions? Examples of contradictions that matter: VIX rising while S&P flat (options market sensing something equities don't), gold up while equities also up (liquidity-driven but fragile), DXY up while crude also up (supply shock not demand), yields rising fast while equities ignoring it (rate sensitivity tail risk building). Name what you see specifically.]

KEY RISKS TODAY:
[Name exactly 3 concrete risks specific to TODAY — not generic market risks. Each risk should be 1-2 sentences. Include probability language where appropriate ("if X then Y"). These should be the 3 things that could cause this short premium portfolio to get hurt today.]

VOLGUARD IMPLICATION:
[3-4 sentences. Speak directly to the portfolio. Should the system deploy new positions today? If yes, which expiry (weekly/monthly) is better-suited given the overnight setup? Are there IV levels to watch? Any specific strike positioning implications from the global tone? End with a one-sentence directional verdict: the global context is [supportive / neutral / a headwind] for short premium today.]
"""

_V5_PRETRADE_SYSTEM = """You are VolGuard's Pre-Trade Intelligence Agent — senior derivatives risk manager.

WHAT YOU ARE EVALUATING:
VolGuard V4 has run a full quantitative regime analysis and generated a trade mandate. The math is valid. Your job: does the REAL WORLD context right now — macro, news, global tone — make this specific overnight position safe to enter?

PORTFOLIO CONTEXT:
- Overnight short premium on Nifty 50 options: Iron Fly / Iron Condor / Protected Straddle / Strangle / Spreads
- Product D (carryforward) — position held overnight, NOT intraday
- GTT stop-losses placed at 2x premium at entry and cannot be adjusted post-entry
- Once entered, position is committed until stop triggers or mandatory square-off (1 day before expiry at 14:00 IST)
- PRIMARY RISK: entering hours before a macro shock produces an overnight gap that blows through the stop

THREE VERDICTS — APPLY PRECISELY:
PROCEED = quant signal is strong AND real-world context is clean or neutral. Execute at full mandate size.
PROCEED_WITH_CAUTION = signal valid but context introduces a specific nameable risk. Give CONCRETE adjustments: exact size % reduction, specific strike change, expiry preference. Vague caution is worthless.
VETO = DO NOT enter today. Reserve ONLY for: binary event within 24 hours, actively breaking high-impact news, VIX actively spiking (not just elevated), confirmed multi-asset risk-off in progress.

VETO CALIBRATION — critical:
High IVP + stable VIX = rich premium = GOOD context for entry. This is NOT a veto signal.
Rising IVP with rising VIX = vol expanding = CAUTION territory, not automatic veto.
VETO only when overnight risk is genuinely unquantifiable (event within 24h, crisis in progress).
A missed trade costs one premium cycle. A wrong entry costs 2x premium. Calibrate accordingly.

RULES:
1. Only use data explicitly provided. Zero invented numbers or assumed context.
2. Be direct. State your verdict and the specific reason. No hedging prose.
3. CAUTION adjustments must be specific: 'reduce to 75% allocation', 'widen iron condor call wing by 100 points', 'prefer monthly over weekly given 2 DTE on weekly'
4. VETO must name the exact trigger in one sentence and the exact removal condition in one sentence."""

_V5_PRETRADE_USER = """
════════════════════════════════════════
V4 QUANTITATIVE MANDATE
════════════════════════════════════════
Strategy         : {strategy}
Expiry           : {expiry}  (DTE: {dte})
Overall Signal   : {signal}  |  Regime Score: {regime_score}/10  |  Confidence: {confidence}
Regime Name      : {regime_name}
Deployment       : ₹{deployment:,.0f}

--- COMPONENT SCORES (0-10 each) ---
Vol Score   : {vol_score}/10  |  {vol_signal}
Struct Score: {struct_score}/10  |  {struct_signal}
Edge Score  : {edge_score}/10  |  {edge_signal}

--- VOLATILITY PICTURE ---
India VIX           : {india_vix}  |  Momentum: {vix_direction}  |  5d Change: {vix_change_5d}
IVP (1yr / 30d)     : {ivp_1yr} / {ivp_30d}  [<25=cheap, 25-75=normal, >75=rich]
Vol Regime          : {vol_regime}  |  VoV Z-Score: {vov_zscore}
GARCH 7d forecast   : {garch7}%  |  GARCH 28d: {garch28}%
Weighted VRP        : {weighted_vrp}%  (GARCH 70% + Parkinson 15% + RV 15%)
   Raw components: VRP-GARCH {vrp_garch}%  |  VRP-Parkinson {vrp_park}%  |  VRP-RV {vrp_rv}%

--- STRUCTURE & POSITIONING ---
GEX Regime          : {gex_regime}  |  Net GEX: {net_gex}M
Skew 25Δ (P-C IV)  : {skew_25d}%  |  Skew Regime: {skew_regime}
PCR (OI)            : {pcr}  |  PCR ATM: {pcr_atm}
Max Pain            : {max_pain}
OI Regime           : {oi_regime}

--- TERM STRUCTURE ---
Term Structure Slope: {term_slope}%  |  Regime: {term_regime}
   [Positive slope=contango=normal | Negative=backwardation=fear premium in front]

--- KEY SCORE DRIVERS ---
{score_drivers}

--- WEIGHT RATIONALE (dynamic weighting reason) ---
{weight_rationale}

--- MANDATE NOTES ---
Directional Bias : {directional_bias}
V4 Risk Notes    : {risk_notes}

════════════════════════════════════════
REAL-WORLD CONTEXT
════════════════════════════════════════
Global Tone (from 08:30 brief): {global_tone}

Live Macro Snapshot:
{macro_context}

News Scan (last 2 hours):
{news_context}

Nifty Spot : {nifty_spot}  |  Current Time: {current_time} IST
FII Net Today: {fii_today}

Upcoming Events:
{upcoming_events}

════════════════════════════════════════
TASK: PRE-TRADE CONTEXT MEMO
════════════════════════════════════════
Use EXACTLY this structure:

RECOMMENDATION: [PROCEED / PROCEED_WITH_CAUTION / VETO]

QUANT SIGNAL ASSESSMENT:
[2-3 sentences. What is V4 seeing? Interpret the regime name, IVP, VRP, vol regime, and score drivers together. What edge is the model detecting? Is the weighted VRP convincing or marginal? Is VoV z-score indicating vol-of-vol risk?]

MACRO OVERLAY:
[3-4 sentences. Translate the global tone and macro snapshot to this specific trade. Does the cross-asset picture support or threaten a short premium overnight position? Consider: is DXY stable (supports FII inflows), is crude contained (no INR pressure), is US VIX calm (reduces India VIX sympathy spike risk), is term structure in contango (normal) or backwardation (fear)?]

KEY RISK FACTORS:
[Exactly 3 risks. Each 1-2 sentences. These must be SPECIFIC to this trade at this moment — not generic market risks. Reference the actual numbers: the VIX level, the DTE, the specific event, the GEX regime.]

RECOMMENDATION RATIONALE:
[4-5 sentences. Synthesize the quant picture and macro context into your verdict. For PROCEED: state what specifically makes this a clean entry. For CAUTION: state the exact tension between the good quant signal and the specific macro risk, and why CAUTION not VETO. For VETO: state the specific unacceptable risk and why it cannot be managed with parameter changes.]

[ONLY include if PROCEED_WITH_CAUTION:]
SUGGESTED ADJUSTMENTS:
- [Specific change 1 with exact numbers, e.g. 'Reduce allocation to 75% (₹X instead of ₹Y)']
- [Specific change 2, e.g. 'Prefer monthly expiry over weekly given {dte} DTE on weekly']
- [Optional change 3 if warranted]

[ONLY include if VETO:]
VETO REASON: [One precise sentence naming the specific unacceptable risk]
REVISIT WHEN: [One precise sentence naming the exact condition that removes the veto]
"""

_V5_MONITOR_SYSTEM = """You are VolGuard's Continuous Awareness Monitor — an intraday watchdog for an overnight short premium Nifty derivatives portfolio.

PORTFOLIO YOU ARE WATCHING:
Short premium positions on Nifty 50 options (Iron Fly / Iron Condor / Protected Straddle / Spreads).
These are overnight carryforward positions. GTT stop-losses are placed and cannot be changed.
The portfolio manager needs to know if something material has changed that makes the EXISTING POSITIONS more vulnerable.

ALERT LEVELS — use precisely:
MONITOR = something notable changed, but existing positions are not at immediate risk. Awareness only.
REVIEW_POSITIONS = the change is material enough that the manager should assess each open position against the new context. Not urgent enough to exit, but important enough to evaluate.
CONSIDER_EXIT = the change creates genuine unquantified risk to open positions. Voluntary early exit or hedge may be warranted. Not a command — a flag.

RULES:
1. Fire ONLY when material. A 0.3% Nifty move is not an alert. A 2% move is.
2. Every alert must be specific to the OPEN POSITIONS provided — a Nifty drop matters differently for a 19500 ATM straddle vs an iron condor centered at 19200.
3. No analysis theatre. Total output under 120 words. If you cannot state why it matters specifically, do not alert.
4. Do not alert on noise. Do not repeat an alert for the same trigger within one session."""

_V5_MONITOR_USER = """
TRIGGER:
{trigger_description}

MARKET NOW: Nifty {nifty_spot} | India VIX {india_vix} | {current_time} IST

OPEN POSITIONS:
{open_positions}

NEWS (last 30 min):
{news_context}

---
ALERT LEVEL: [MONITOR / REVIEW_POSITIONS / CONSIDER_EXIT]

WHAT CHANGED:
[One sentence. Specific numbers. Not 'markets moved' but 'India VIX jumped from 14.2 to 16.8 (+18%) in 25 minutes'.]

WHY IT MATTERS FOR OPEN POSITIONS:
[1-2 sentences. Connect the trigger to the specific open positions. If we have a 19500 straddle and Nifty is at 19200, that is relevant context.]

ACTION:
[One concrete sentence: what should the manager do in the next 15 minutes, if anything.]
"""


# ── Response Parser ───────────────────────────────────────────────────────────

import re as _re

def _v5_extract(text: str, label: str) -> str:
    p = rf"^{_re.escape(label)}:\s*\n?(.*?)(?=\n[A-Z][A-Z\s_]+:|$)"
    m = _re.search(p, text, _re.MULTILINE | _re.DOTALL | _re.IGNORECASE)
    if m: return m.group(1).strip()
    p2 = rf"{_re.escape(label)}:\s*(.+)"
    m2 = _re.search(p2, text, _re.IGNORECASE)
    if m2: return m2.group(1).strip()
    return ""

_V5_VALID_TONES = {"CLEAR","CAUTIOUS_NEUTRAL","CAUTIOUS","RISK_OFF","MIXED","UNKNOWN"}
_V5_VALID_RECS = {"PROCEED","PROCEED_WITH_CAUTION","VETO"}
_V5_VALID_LEVELS = {"MONITOR","REVIEW_POSITIONS","CONSIDER_EXIT"}


@dataclass
class V5BriefResult:
    global_tone: str = "UNKNOWN"
    us_session_summary: str = ""
    asian_session: str = ""
    gift_nifty_signal: str = ""
    macro_gauges: str = ""
    crypto_signal: str = ""
    cross_asset_coherence: str = ""
    key_risks_today: str = ""
    volguard_implication: str = ""
    raw: str = ""
    ok: bool = False

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if k != "raw"}

    def to_telegram(self) -> str:
        EMOJI = {"CLEAR":"🟢","CAUTIOUS_NEUTRAL":"🟡","CAUTIOUS":"🟠","RISK_OFF":"🔴","MIXED":"⚪","UNKNOWN":"❓"}
        e = EMOJI.get(self.global_tone, "❓")
        parts = [
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "🌏 VOLGUARD MORNING BRIEF",
            f"📅 {datetime.now(IST_TZ).strftime('%d %b %Y')} | 08:30 IST",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "",
            f"{e} GLOBAL TONE: {self.global_tone}",
            "",
        ]
        for em, lbl, attr in [
            ("🇺🇸","US SESSION", "us_session_summary"),
            ("🌏","ASIA", "asian_session"),
            ("🇮🇳","GIFT NIFTY", "gift_nifty_signal"),
            ("📊","MACRO GAUGES", "macro_gauges"),
            ("₿","CRYPTO", "crypto_signal"),
            ("🔗","CROSS-ASSET", "cross_asset_coherence"),
            ("⚠️","KEY RISKS TODAY", "key_risks_today"),
            ("🤖","VOLGUARD IMPLICATION", "volguard_implication"),
        ]:
            val = getattr(self, attr)
            if val: parts += [f"{em} {lbl}:", val, ""]
        parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(parts)


@dataclass
class V5PreTradeResult:
    recommendation: str = "PROCEED_WITH_CAUTION"
    quant_signal_assessment: str = ""
    macro_overlay: str = ""
    key_risk_factors: str = ""
    recommendation_rationale: str = ""
    suggested_adjustments: List[str] = field(default_factory=list)
    veto_reason: str = ""
    revisit_when: str = ""
    raw: str = ""
    ok: bool = False

    @property
    def is_veto(self): return self.recommendation == "VETO"
    @property
    def is_caution(self): return self.recommendation == "PROCEED_WITH_CAUTION"
    @property
    def is_proceed(self): return self.recommendation == "PROCEED"

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if k != "raw"}

    def to_telegram(self, strategy: str, expiry: str, score: float) -> str:
        EM = {"PROCEED":"✅","PROCEED_WITH_CAUTION":"⚠️","VETO":"🚫"}
        e = EM.get(self.recommendation, "❓")
        parts = [
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "⚙️ PRE-TRADE CONTEXT MEMO",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "",
            f"📊 {strategy} | {expiry} | Score:{score:.1f}",
            "",
            f"{e} RECOMMENDATION: {self.recommendation}",
            "",
        ]
        for lbl, attr in [
            ("📈 QUANT SIGNAL","quant_signal_assessment"),
            ("🌏 MACRO OVERLAY","macro_overlay"),
            ("⚠️ KEY RISKS","key_risk_factors"),
            ("🧠 RATIONALE","recommendation_rationale"),
        ]:
            val = getattr(self, attr)
            if val: parts += [lbl+":", val, ""]
        if self.suggested_adjustments:
            parts.append("📝 ADJUSTMENTS:")
            for a in self.suggested_adjustments: parts.append(f"  • {a}")
            parts.append("")
        if self.veto_reason: parts += [f"🚫 VETO REASON: {self.veto_reason}", ""]
        if self.revisit_when: parts += [f"🔄 REVISIT WHEN: {self.revisit_when}", ""]
        parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(parts)


@dataclass
class V5AlertResult:
    alert_level: str = "MONITOR"
    what_changed: str = ""
    why_it_matters: str = ""
    action: str = ""
    raw: str = ""

    def to_telegram(self) -> str:
        EM = {"MONITOR":"👁","REVIEW_POSITIONS":"⚠️","CONSIDER_EXIT":"🚨"}
        e = EM.get(self.alert_level, "📢")
        return f"{e} CONTEXT ALERT: {self.alert_level}\n\nWHAT: {self.what_changed}\nWHY: {self.why_it_matters}\nACTION: {self.action}"


def _v5_rule_based_pretrade(vol_metrics, regime_score, mandate, news) -> "V5PreTradeResult":
    """
    Rule-based pre-trade gate — fires when LLM is unavailable (Groq/Claude down/no key).
    Has real teeth: hard VETOs on dangerous market conditions, not a rubber stamp.
    Based purely on quant signals already computed by the regime engine.
    """
    vov_z    = getattr(vol_metrics, 'vov_zscore', 0.0) or 0.0
    ivp_1yr  = getattr(vol_metrics, 'ivp_1yr', 50.0) or 50.0
    vix      = getattr(vol_metrics, 'vix', 15.0) or 15.0
    vol_reg  = getattr(vol_metrics, 'vol_regime', 'FAIR')
    score    = getattr(regime_score, 'total_score', 5.0) if regime_score else 5.0
    has_veto_news = getattr(news, 'has_veto', False)

    # ── Hard VETOs ────────────────────────────────────────────────────────────
    if has_veto_news:
        headlines = "; ".join(i.title for i in (news.veto_items or [])[:2])
        return V5PreTradeResult(
            recommendation="VETO",
            recommendation_rationale=f"[RULE-BASED] VETO-level news: {headlines}",
            veto_reason=f"Breaking news: {headlines}",
            revisit_when="After news resolves and VIX stabilises below 18",
            ok=False,
        )
    if vov_z >= 3.0:
        return V5PreTradeResult(
            recommendation="VETO",
            recommendation_rationale=f"[RULE-BASED] Vol-of-Vol crash signal: VoV z-score={vov_z:.1f}σ ≥ 3.0. "
                                     "Market is in a regime-change — premium selling is dangerous.",
            veto_reason=f"VoV z-score {vov_z:.1f}σ (CRASH threshold ≥ 3.0)",
            revisit_when="When VoV z-score drops below 1.5 for two consecutive sessions",
            ok=False,
        )
    if vol_reg == "EXPLODING":
        return V5PreTradeResult(
            recommendation="VETO",
            recommendation_rationale="[RULE-BASED] Volatility regime is EXPLODING. "
                                     "Short premium in a vol explosion is uncapped loss risk.",
            veto_reason="Vol regime EXPLODING",
            revisit_when="When vol regime returns to RICH or FAIR",
            ok=False,
        )
    if vix > 28:
        return V5PreTradeResult(
            recommendation="VETO",
            recommendation_rationale=f"[RULE-BASED] India VIX={vix:.1f} > 28. "
                                     "Extreme fear — short premium has extremely poor risk/reward.",
            veto_reason=f"VIX {vix:.1f} above hard ceiling of 28",
            revisit_when="When VIX drops and holds below 22 for two sessions",
            ok=False,
        )
    if score < 3.0:
        return V5PreTradeResult(
            recommendation="VETO",
            recommendation_rationale=f"[RULE-BASED] Regime score {score:.1f} < 3.0 threshold. "
                                     "Quant engine says conditions are unfavourable.",
            veto_reason=f"Regime score {score:.1f} below minimum threshold of 3.0",
            revisit_when="When regime score exceeds 4.0",
            ok=False,
        )

    # ── Cautions ──────────────────────────────────────────────────────────────
    adjustments = []
    rationale_parts = ["[RULE-BASED — LLM unavailable] Quant signals reviewed."]

    if ivp_1yr < 25:
        # Vol is cheap — premium selling has thin edge
        adjustments.append("Reduce allocation to 50% — IVP very low, edge is thin")
        rationale_parts.append(f"IVP={ivp_1yr:.0f}% is low; premium is cheap.")

    if vov_z > 1.5:
        # Graduated allocation reduction based on VoV severity band
        if vov_z >= 2.75:
            adjustments.append(f"Reduce allocation to 40% — VoV z={vov_z:.1f}σ in DANGER zone (≥2.75σ)")
            rationale_parts.append(f"VoV z={vov_z:.1f}σ is in DANGER band (2.75–2.99σ). Heavy risk reduction required.")
        elif vov_z >= 2.50:
            adjustments.append(f"Reduce allocation to 60% — VoV z={vov_z:.1f}σ in ELEVATED zone (≥2.50σ)")
            rationale_parts.append(f"VoV z={vov_z:.1f}σ is in ELEVATED band (2.50–2.74σ). Significant risk reduction.")
        elif vov_z >= 2.25:
            adjustments.append(f"Reduce allocation to 80% — VoV z={vov_z:.1f}σ in WARNING zone (≥2.25σ)")
            rationale_parts.append(f"VoV z={vov_z:.1f}σ is in WARNING band (2.25–2.49σ). Mild reduction advised.")
        else:
            adjustments.append(f"Reduce allocation to 60% — elevated VoV z-score signals instability")
            rationale_parts.append(f"VoV z={vov_z:.1f}σ is elevated above 1.5σ.")

    if vix > 20:
        adjustments.append("Prefer wider wings — VIX elevated, tails are fatter")
        rationale_parts.append(f"VIX={vix:.1f} > 20.")

    if adjustments:
        return V5PreTradeResult(
            recommendation="PROCEED_WITH_CAUTION",
            recommendation_rationale=" ".join(rationale_parts),
            suggested_adjustments=adjustments,
            ok=False,
        )

    # ── Clean PROCEED ─────────────────────────────────────────────────────────
    return V5PreTradeResult(
        recommendation="PROCEED",
        recommendation_rationale=(
            f"[RULE-BASED — LLM unavailable] All quant guards passed: "
            f"Score={score:.1f}, VoV z={vov_z:.1f}σ, IVP={ivp_1yr:.0f}%, VIX={vix:.1f}. "
            f"No veto-level news. Proceeding at full size."
        ),
        ok=False,  # mark ok=False so caller knows LLM was bypassed
    )


def _v5_parse_brief(raw: str) -> V5BriefResult:
    r = V5BriefResult(raw=raw)
    if not raw: return r
    try:
        m = _re.search(r"GLOBAL TONE:\s*([A-Z_]+)", raw, _re.IGNORECASE)
        if m:
            t = m.group(1).strip().upper()
            r.global_tone = t if t in _V5_VALID_TONES else "UNKNOWN"
        r.us_session_summary = _v5_extract(raw, "US SESSION SUMMARY")
        r.asian_session = _v5_extract(raw, "ASIAN SESSION")
        r.gift_nifty_signal = _v5_extract(raw, "GIFT NIFTY SIGNAL")
        r.macro_gauges = _v5_extract(raw, "MACRO GAUGES")
        r.crypto_signal = _v5_extract(raw, "CRYPTO SIGNAL")
        r.cross_asset_coherence = _v5_extract(raw, "CROSS-ASSET COHERENCE")
        r.key_risks_today = _v5_extract(raw, "KEY RISKS TODAY")
        r.volguard_implication = _v5_extract(raw, "VOLGUARD IMPLICATION")
        r.ok = bool(r.global_tone != "UNKNOWN" and r.volguard_implication)
    except Exception as e:
        logger.error(f"parse_brief error: {e}")
    return r


def _v5_parse_pretrade(raw: str) -> V5PreTradeResult:
    r = V5PreTradeResult(raw=raw)
    if not raw: return r
    try:
        m = _re.search(r"RECOMMENDATION:\s*([A-Z_]+)", raw, _re.IGNORECASE)
        if m:
            rec = m.group(1).strip().upper()
            r.recommendation = rec if rec in _V5_VALID_RECS else "PROCEED_WITH_CAUTION"
        r.quant_signal_assessment = _v5_extract(raw, "QUANT SIGNAL ASSESSMENT")
        r.macro_overlay = _v5_extract(raw, "MACRO OVERLAY")
        r.key_risk_factors = _v5_extract(raw, "KEY RISK FACTORS")
        r.recommendation_rationale = _v5_extract(raw, "RECOMMENDATION RATIONALE")
        r.veto_reason = _v5_extract(raw, "VETO REASON")
        r.revisit_when = _v5_extract(raw, "REVISIT WHEN")
        adj_section = _v5_extract(raw, "SUGGESTED ADJUSTMENTS")
        if adj_section:
            r.suggested_adjustments = [a.strip() for a in _re.findall(r"[-•]\s*(.+)", adj_section) if a.strip()]
        r.ok = bool(r.recommendation_rationale)
    except Exception as e:
        logger.error(f"parse_pretrade error: {e}")
    return r


def _v5_parse_alert(raw: str) -> V5AlertResult:
    r = V5AlertResult(raw=raw)
    if not raw: return r
    try:
        m = _re.search(r"ALERT LEVEL:\s*([A-Z_]+)", raw, _re.IGNORECASE)
        if m:
            lvl = m.group(1).strip().upper()
            r.alert_level = lvl if lvl in _V5_VALID_LEVELS else "MONITOR"
        r.what_changed = _v5_extract(raw, "WHAT CHANGED")
        r.why_it_matters = _v5_extract(raw, "WHY IT MATTERS")
        r.action = _v5_extract(raw, "ACTION")
    except Exception as e:
        logger.error(f"parse_alert error: {e}")
    return r


# ── Morning Brief Agent ───────────────────────────────────────────────────────

class V5MorningBriefAgent:
    _instance = None
    _lock = _v5_threading.RLock()

    def __init__(self):
        self._latest: Optional[V5BriefResult] = None
        self._latest_time: Optional[datetime] = None
        self._ilock = _v5_threading.RLock()
        self.logger = logging.getLogger("V5MorningBriefAgent")
        self._restore_from_db()

    def _restore_from_db(self):
        """On startup: reload today's brief from DB if it was already generated.
        Prevents morning_tone = UNKNOWN after a server restart post 08:30."""
        try:
            db = SessionLocal()
            try:
                today = datetime.now(IST_TZ).date().isoformat()
                row = db.execute(
                    text("SELECT brief_json, created_at FROM intelligence_briefs WHERE date = :d"),
                    {"d": today}
                ).fetchone()
                if row:
                    brief_dict = json.loads(row[0])
                    result = V5BriefResult(**{k: v for k, v in brief_dict.items()
                                               if k in V5BriefResult.__dataclass_fields__})
                    result.ok = True
                    self._latest = result
                    try:
                        self._latest_time = datetime.fromisoformat(row[1]).astimezone(IST_TZ)
                    except Exception:
                        self._latest_time = datetime.now(IST_TZ)
                    self.logger.info(f"✅ Morning brief restored from DB — Tone: {result.global_tone}")
            finally:
                db.close()
        except Exception as e:
            self.logger.debug(f"Brief restore (non-critical): {e}")

    @classmethod
    def get(cls) -> "V5MorningBriefAgent":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def run(self, india_vix=None, ivp=None, fii_net=None, upcoming_events=None, force=False) -> V5BriefResult:
        self.logger.info("🌅 Morning Brief Agent starting...")
        collector = V5MacroCollector.get()
        snap = collector.get_snapshot(force=force)
        scanner = V5NewsScanner.get()
        news = scanner.scan(force=force)
        macro_ctx = collector.build_context_string(snap)
        news_ctx = news.format_for_prompt()
        gift_sig = collector.gift_nifty_signal(snap)

        def _fmt_events(events, is_veto):
            if not events: return "None in next 7 days"
            filtered = [e for e in events if getattr(e, "is_veto_event", False) == is_veto]
            if not filtered: return "None"
            lines = []
            for e in filtered[:4]:
                lines.append(f"  • {e.title} — {getattr(e, 'days_until', '?')} day(s) away")
            return "\n".join(lines)

        user_prompt = _V5_BRIEF_USER.format(
            macro_context=macro_ctx,
            news_context=news_ctx,
            gift_nifty_signal=gift_sig,
            india_vix=f"{india_vix:.1f}" if india_vix else "N/A",
            ivp=f"{ivp:.0f}%" if ivp else "N/A",
            fii_net=f"₹{fii_net:,.0f} Cr" if fii_net else "N/A",
            veto_events=_fmt_events(upcoming_events or [], True),
            high_impact_events=_fmt_events(upcoming_events or [], False),
        )

        claude = V5ClaudeClient.get()

        # ── Use Claude with web_search when Anthropic key is set ────────────────────────────
        # Web search lets Claude fetch the WHY behind market moves directly —
        # "why did US markets move today", "Asian markets reason today" etc.
        # This solves the generic brief problem: Claude no longer just describes numbers,
        # it searches for and explains the actual causality chain.
        # Falls back to standard call (Groq / plain Claude) if web search unavailable.
        raw = None
        if claude and claude._provider == "claude":
            raw = claude.call_with_websearch(_V5_BRIEF_SYSTEM, user_prompt, max_tokens=2000, ctx="morning_brief")
        elif claude:
            raw = claude.call(_V5_BRIEF_SYSTEM, user_prompt, max_tokens=1500, ctx="morning_brief")

        if not raw:
            result = V5BriefResult(
                global_tone=snap.global_tone,
                us_session_summary="Intelligence agent unavailable — using raw data only.",
                volguard_implication=f"Global tone from data: {snap.global_tone}. Review manually.",
                ok=False
            )
        else:
            result = _v5_parse_brief(raw)
            if result.global_tone == "UNKNOWN":
                result.global_tone = snap.global_tone

        with self._ilock:
            self._latest = result
            self._latest_time = datetime.now(IST_TZ)

        self._save_to_db(result)
        self.logger.info(f"✅ Morning brief complete — Tone: {result.global_tone}")
        return result

    def get_latest(self) -> Optional[V5BriefResult]:
        with self._ilock:
            return self._latest

    def get_latest_dict(self) -> dict:
        with self._ilock:
            if self._latest is None:
                return {"available": False, "message": "Not yet generated today.", "global_tone": "UNKNOWN"}
            return {
                "available": True,
                "generated_at": self._latest_time.isoformat() if self._latest_time else None,
                "data": self._latest.to_dict(),
            }

    def _save_to_db(self, result: V5BriefResult):
        try:
            db = SessionLocal()
            try:
                db.execute(text("""
                    INSERT INTO intelligence_briefs (date, global_tone, brief_json, created_at)
                    VALUES (:date, :tone, :json, :ts)
                    ON CONFLICT(date) DO UPDATE SET
                        global_tone = excluded.global_tone,
                        brief_json = excluded.brief_json,
                        created_at = excluded.created_at
                """), {
                    "date": datetime.now(IST_TZ).date().isoformat(),
                    "tone": result.global_tone,
                    "json": json.dumps(result.to_dict()),
                    "ts": datetime.now(IST_TZ).isoformat(),
                })
                db.commit()
            finally:
                db.close()
        except Exception as e:
            self.logger.error(f"DB save brief failed: {e}")


# ── Pre-Trade Agent ───────────────────────────────────────────────────────────

class V5PreTradeAgent:
    _instance = None
    _lock = _v5_threading.RLock()

    def __init__(self):
        self._veto_log: List[dict] = []
        self._ilock = _v5_threading.RLock()
        self.logger = logging.getLogger("V5PreTradeAgent")
        # Cache last evaluation per expiry_type: avoids 3 identical Claude calls
        # when evaluate_all_mandates fires weekly/monthly/next_weekly in rapid succession
        self._eval_cache: dict = {}   # expiry_type → (V5PreTradeResult, timestamp)
        self._CACHE_TTL = 90          # seconds — reuse result if same expiry called within 90s

    @classmethod
    def get(cls) -> "V5PreTradeAgent":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def evaluate(
        self,
        mandate: "TradingMandate",
        regime_score: "RegimeScore",
        vol_metrics: "VolMetrics",
        nifty_spot: float,
        india_vix: float,
        fii_today=None,
        upcoming_events=None,
        open_positions=None,
        morning_tone: str = "UNKNOWN",
        struct_metrics=None,
        edge_metrics=None,
    ) -> V5PreTradeResult:
        # ── Cooldown: reuse result if same expiry was evaluated within TTL ──
        _cache_key = mandate.expiry_type
        with self._ilock:
            _cached = self._eval_cache.get(_cache_key)
            if _cached:
                _prev_result, _prev_ts = _cached
                if (time.time() - _prev_ts) < self._CACHE_TTL:
                    self.logger.info(
                        f"Pre-trade [{_cache_key}] cache hit "
                        f"({int(time.time()-_prev_ts)}s ago) → {_prev_result.recommendation}"
                    )
                    return _prev_result

        self.logger.info(f"Pre-trade eval: {mandate.suggested_structure} | {mandate.expiry_type} | score={regime_score.total_score:.1f}")

        collector = V5MacroCollector.get()
        snap = collector.get_snapshot()
        scanner = V5NewsScanner.get()
        news = scanner.scan(lookback_hours=2)
        macro_ctx = collector.build_context_string(snap)
        news_ctx = news.format_for_prompt()

        def _fmt_events(evs):
            if not evs: return "None in next 7 days."
            lines = []
            for e in (evs or [])[:5]:
                lines.append(f"  • {e.title} — {getattr(e,'days_until','?')} day(s) | Veto:{getattr(e,'is_veto_event',False)}")
            return "\n".join(lines) if lines else "None"

        def _fmt_positions(pos):
            if not pos: return "No open positions."
            return "\n".join(
                f"  • {p.get('instrument_token','?')} Qty:{p.get('quantity',0)} P&L:₹{p.get('pnl',0):,.0f}"
                for p in (pos or [])[:4]
            )

        dte = (mandate.expiry_date - datetime.now(IST_TZ).date()).days if mandate.expiry_date else "?"
        risk_notes = ", ".join(mandate.risk_notes) if mandate.risk_notes else "None"

        # Extract all available market data for intelligence prompt
        def _gv(obj, attr, default="N/A", fmt=None):
            v = getattr(obj, attr, None) if obj else None
            if v is None: return default
            return fmt.format(v) if fmt else str(v)

        vm = vol_metrics  # shorthand
        rs = regime_score

        # Volatility fields
        vix_dir     = _gv(vm, "vix_momentum", "STABLE")
        vix_chg_5d  = _gv(vm, "vix_change_5d", "N/A", "{:.2f}%")
        vol_regime  = _gv(vm, "vol_regime", "UNKNOWN")
        vov_z       = _gv(vm, "vov_zscore", "N/A", "{:.2f}")
        ivp_1yr     = _gv(vm, "ivp_1yr", "N/A", "{:.0f}%")
        ivp_30d     = _gv(vm, "ivp_30d", "N/A", "{:.0f}%")
        garch7      = _gv(vm, "garch7", "N/A", "{:.1f}")
        garch28     = _gv(vm, "garch28", "N/A", "{:.1f}")

        # Edge (VRP) fields — use EdgeMetrics object if passed, else fallback to score
        em = edge_metrics
        _exp = mandate.expiry_type.lower()
        if em:
            _wrp_map = {"weekly": "weighted_vrp_weekly", "monthly": "weighted_vrp_monthly",
                        "next_weekly": "weighted_vrp_next_weekly"}
            _grp_map = {"weekly": "vrp_garch_weekly", "monthly": "vrp_garch_monthly",
                        "next_weekly": "vrp_garch_next_weekly"}
            _prp_map = {"weekly": "vrp_park_weekly", "monthly": "vrp_park_monthly",
                        "next_weekly": "vrp_park_next_weekly"}
            _rrp_map = {"weekly": "vrp_rv_weekly", "monthly": "vrp_rv_monthly",
                        "next_weekly": "vrp_rv_next_weekly"}
            weighted_vrp = _gv(em, _wrp_map.get(_exp, "weighted_vrp_weekly"), "N/A", "{:.2f}%")
            vrp_garch    = _gv(em, _grp_map.get(_exp, "vrp_garch_weekly"), "N/A", "{:.2f}%")
            vrp_park     = _gv(em, _prp_map.get(_exp, "vrp_park_weekly"), "N/A", "{:.2f}%")
            vrp_rv       = _gv(em, _rrp_map.get(_exp, "vrp_rv_weekly"), "N/A", "{:.2f}%")
            term_slope   = _gv(em, "term_structure_slope", "N/A", "{:.2f}%")
            term_regime  = _gv(em, "term_structure_regime", "N/A")
        else:
            weighted_vrp = f"{_gv(rs, 'edge_score', 'N/A')}/10 (score only)"
            vrp_garch = vrp_park = vrp_rv = "N/A — EdgeMetrics not passed"
            term_slope = term_regime = "N/A"

        # Struct fields — use StructMetrics if passed
        sm = struct_metrics
        if sm:
            gex_regime  = _gv(sm, "gex_regime", "N/A")
            net_gex     = _gv(sm, "gex_weighted", "N/A", "{:.1f}M")
            skew_25d    = _gv(sm, "skew_25d", "N/A", "{:.2f}%")
            skew_regime = _gv(sm, "skew_regime", "N/A")
            pcr         = _gv(sm, "pcr", "N/A", "{:.2f}")
            pcr_atm     = _gv(sm, "pcr_atm", "N/A", "{:.2f}")
            max_pain    = _gv(sm, "max_pain", "N/A", "{:,.0f}")
            oi_regime   = _gv(sm, "oi_regime", "N/A")
        else:
            gex_regime = net_gex = skew_25d = skew_regime = "N/A"
            pcr = pcr_atm = max_pain = oi_regime = "N/A"

        # Parse score_drivers for struct/edge data
        drivers_list = rs.score_drivers if rs and rs.score_drivers else []
        drivers_str  = "\n".join(f"  {i+1}. {d}" for i, d in enumerate(drivers_list[:8])) or "  N/A"
        weight_rationale = getattr(rs, "weight_rationale", "N/A") or "N/A"
        confidence   = getattr(rs, "confidence", "N/A") or "N/A"
        regime_name  = getattr(mandate, "regime_name", "UNKNOWN") or "UNKNOWN"

        user_prompt = _V5_PRETRADE_USER.format(
            strategy=mandate.suggested_structure,
            expiry=f"{mandate.expiry_type} ({mandate.expiry_date})",
            dte=dte,
            regime_score=round(rs.total_score, 1) if rs else "N/A",
            signal=getattr(rs, "overall_signal", "N/A"),
            confidence=confidence,
            regime_name=regime_name,
            deployment=mandate.deployment_amount,
            vol_score=round(getattr(rs, "vol_score", 0), 1),
            vol_signal=getattr(rs, "vol_signal", "N/A"),
            struct_score=round(getattr(rs, "struct_score", 0), 1),
            struct_signal=getattr(rs, "struct_signal", "N/A"),
            edge_score=round(getattr(rs, "edge_score", 0), 1),
            edge_signal=getattr(rs, "edge_signal", "N/A"),
            india_vix=f"{india_vix:.1f}",
            vix_direction=vix_dir,
            vix_change_5d=vix_chg_5d,
            ivp_1yr=ivp_1yr,
            ivp_30d=ivp_30d,
            vol_regime=vol_regime,
            vov_zscore=vov_z,
            garch7=garch7,
            garch28=garch28,
            weighted_vrp=weighted_vrp,
            vrp_garch=vrp_garch,
            vrp_park=vrp_park,
            vrp_rv=vrp_rv,
            gex_regime=gex_regime,
            net_gex=net_gex,
            skew_25d=skew_25d,
            skew_regime=skew_regime,
            pcr=pcr,
            pcr_atm=pcr_atm,
            max_pain=max_pain,
            oi_regime=oi_regime,
            term_slope=term_slope,
            term_regime=term_regime,
            score_drivers=drivers_str,
            weight_rationale=weight_rationale,
            directional_bias=mandate.directional_bias,
            risk_notes=risk_notes,
            global_tone=morning_tone,
            macro_context=macro_ctx,
            news_context=news_ctx,
            nifty_spot=f"{nifty_spot:,.0f}",
            fii_today=f"₹{fii_today:,.0f} Cr" if fii_today else "N/A",
            current_time=datetime.now(IST_TZ).strftime("%H:%M"),
            upcoming_events=_fmt_events(upcoming_events),
        )

        claude = V5ClaudeClient.get()
        raw = claude.call(_V5_PRETRADE_SYSTEM, user_prompt, max_tokens=1500, ctx="pretrade") if claude else None

        if not raw:
            # ── Rule-based fallback when LLM is unavailable ──────────────────
            # The gate must have real teeth even when Groq/Anthropic is down.
            # These rules mirror the quant signals directly so the system never
            # becomes a rubber stamp during an outage.
            result = _v5_rule_based_pretrade(vol_metrics, regime_score, mandate, news)
        else:
            result = _v5_parse_pretrade(raw)

        # Safety override: if VETO-level news exists and agent said PROCEED, escalate
        if news.has_veto and result.is_proceed:
            headlines = "; ".join(i.title for i in news.veto_items[:2])
            result.recommendation = "VETO"
            result.veto_reason = f"VETO-level news detected: {headlines}"
            result.revisit_when = "After news event resolves and VIX stabilizes"
            result.recommendation_rationale = (
                f"Agent recommended PROCEED but RSS scanner detected critical news: {result.veto_reason}. "
                f"Safety override applied."
            )

        self._log(mandate, regime_score, result)
        # ── Cache result for cooldown ──
        with self._ilock:
            self._eval_cache[mandate.expiry_type] = (result, time.time())
        return result

    def _log(self, mandate, regime_score, result: V5PreTradeResult):
        rec = {
            "timestamp": datetime.now(IST_TZ).isoformat(),
            "strategy": mandate.suggested_structure,
            "expiry_type": mandate.expiry_type,
            "expiry_date": str(mandate.expiry_date),
            "regime_score": regime_score.total_score,
            "recommendation": result.recommendation,
            "veto_reason": result.veto_reason,
            "rationale": result.recommendation_rationale[:300],
            "adjustments": result.suggested_adjustments,
            "overridden": False,
        }
        with self._ilock:
            self._veto_log.append(rec)
            if len(self._veto_log) > 100:
                self._veto_log = self._veto_log[-100:]
        if result.is_veto:
            try:
                db = SessionLocal()
                try:
                    db.execute(text("""
                        INSERT INTO intelligence_vetoes
                            (timestamp, strategy, expiry_type, expiry_date, regime_score,
                             recommendation, veto_reason, rationale, adjustments_json, overridden)
                        VALUES (:ts,:strat,:et,:ed,:score,:rec,:vr,:rat,:adj,:ov)
                    """), {
                        "ts": rec["timestamp"], "strat": rec["strategy"],
                        "et": rec["expiry_type"], "ed": rec["expiry_date"],
                        "score": rec["regime_score"], "rec": rec["recommendation"],
                        "vr": rec["veto_reason"], "rat": rec["rationale"],
                        "adj": json.dumps(rec["adjustments"]), "ov": False,
                    })
                    db.commit()
                finally:
                    db.close()
            except Exception as e:
                self.logger.error(f"DB veto save: {e}")

    def override_veto(self, reason: str) -> dict:
        with self._ilock:
            vetoes = [r for r in self._veto_log if r["recommendation"] == "VETO" and not r["overridden"]]
            if not vetoes:
                return {"success": False, "message": "No active VETO to override"}
            latest = vetoes[-1]
            latest["overridden"] = True
            latest["override_reason"] = reason
            latest["override_time"] = datetime.now(IST_TZ).isoformat()
        try:
            db = SessionLocal()
            try:
                db.execute(text("""
                    UPDATE intelligence_vetoes SET overridden=true, override_reason=:r
                    WHERE timestamp=:ts
                """), {"r": reason, "ts": latest["timestamp"]})
                db.commit()
            finally:
                db.close()
        except Exception as e:
            self.logger.error(f"DB override update: {e}")
        self.logger.warning(f"VETO OVERRIDDEN — Reason: {reason}")
        return {"success": True, "message": "VETO overridden. Trade will proceed.", "record": latest}

    def get_log(self, limit: int = 20) -> List[dict]:
        with self._ilock:
            return list(reversed(self._veto_log[-limit:]))


# ── Monitor Agent ─────────────────────────────────────────────────────────────

class V5MonitorAgent:
    _instance = None
    _lock = _v5_threading.RLock()

    def __init__(self):
        self._running = False
        self._thread: Optional[_v5_threading.Thread] = None
        self._seen_news_ids: set = set()
        self._last_vix: Optional[float] = None
        self._last_nifty: Optional[float] = None
        self._alerts: List[dict] = []
        self._ilock = _v5_threading.RLock()
        self.logger = logging.getLogger("V5MonitorAgent")

    @classmethod
    def get(cls) -> "V5MonitorAgent":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def start(self):
        if self._running: return
        self._running = True
        self._thread = _v5_threading.Thread(target=self._loop, daemon=True, name="V5Monitor")
        self._thread.start()
        self.logger.info("✅ Monitor Agent started")

    def stop(self):
        self._running = False

    def _is_market_hours(self) -> bool:
        now = datetime.now(IST_TZ)
        if now.weekday() >= 5: return False
        h, m = now.hour, now.minute
        if not ((h, m) >= (9, 15) and (h, m) <= (15, 30)):
            return False
        # Check NSE holidays via fetcher if available
        try:
            global volguard_system
            if volguard_system:
                holidays = volguard_system.fetcher.get_market_holidays(days_ahead=1)
                if now.date() in holidays:
                    return False
        except Exception:
            pass  # If holiday check fails, assume market is open (conservative)
        return True

    def _loop(self):
        while self._running:
            try:
                if self._is_market_hours():
                    self._scan()
            except Exception as e:
                self.logger.error(f"Monitor loop: {e}")
            time.sleep(60)  # poll every 60s — was 1800s (30min) which missed live events

    def _scan(self):
        triggers = []
        scanner = V5NewsScanner.get()
        news = scanner.scan(force=True)
        new_veto = [i for i in news.veto_items if i.item_id not in self._seen_news_ids]
        if new_veto:
            headlines = "; ".join(i.title for i in new_veto[:2])
            triggers.append(f"NEW VETO-LEVEL NEWS: {headlines}")
        with self._ilock:
            for i in news.veto_items + news.high_impact_items:
                self._seen_news_ids.add(i.item_id)
            if len(self._seen_news_ids) > 3000:
                self._seen_news_ids = set(sorted(self._seen_news_ids)[-1500:])

        # Try to get live VIX/Nifty from market system
        current_vix, current_nifty = None, None
        try:
            global volguard_system
            if volguard_system:
                current_vix = volguard_system.fetcher.get_ltp_with_fallback("NSE_INDEX|India VIX")
                current_nifty = volguard_system.fetcher.get_ltp_with_fallback("NSE_INDEX|Nifty 50")
        except Exception:
            pass

        if current_vix and self._last_vix:
            chg = abs(current_vix - self._last_vix) / self._last_vix * 100
            if chg >= 5.0:
                d = "spiked" if current_vix > self._last_vix else "dropped"
                triggers.append(f"India VIX {d}: {self._last_vix:.1f} → {current_vix:.1f} ({chg:+.1f}%)")
        if current_vix: self._last_vix = current_vix

        if current_nifty and self._last_nifty:
            chg = (current_nifty - self._last_nifty) / self._last_nifty * 100
            if abs(chg) >= 0.8:
                d = "rallied" if chg > 0 else "sold off"
                triggers.append(f"Nifty {d}: {self._last_nifty:,.0f} → {current_nifty:,.0f} ({chg:+.2f}%)")
        if current_nifty: self._last_nifty = current_nifty

        if triggers:
            self.logger.info(f"Monitor: {len(triggers)} trigger(s) found")
            self._fire_alert(triggers, news, current_vix, current_nifty)

    def _fire_alert(self, triggers, news, vix, nifty):
        trigger_desc = "\n".join(f"• {t}" for t in triggers)
        news_ctx = news.format_for_prompt()

        # Get open positions
        open_pos_str = "No open positions."
        try:
            db = SessionLocal()
            try:
                rows = db.execute(text(
                    "SELECT strategy_type, expiry_type FROM trades WHERE status='ACTIVE'"
                )).fetchall()
                if rows:
                    open_pos_str = "\n".join(f"  • {r[0]} | {r[1]}" for r in rows[:4])
            finally:
                db.close()
        except Exception:
            pass

        user_prompt = _V5_MONITOR_USER.format(
            trigger_description=trigger_desc,
            nifty_spot=f"{nifty:,.0f}" if nifty else "N/A",
            india_vix=f"{vix:.1f}" if vix else "N/A",
            current_time=datetime.now(IST_TZ).strftime("%H:%M"),
            open_positions=open_pos_str,
            news_context=news_ctx,
        )

        claude = V5ClaudeClient.get()
        raw = claude.call(_V5_MONITOR_SYSTEM, user_prompt, max_tokens=300, ctx="monitor") if claude else None

        if raw:
            result = _v5_parse_alert(raw)
            alert_text = result.to_telegram()
            _alert_rec = {
                "timestamp": datetime.now(IST_TZ).isoformat(),
                "alert_level": result.alert_level,
                "what_changed": result.what_changed,
                "why_it_matters": result.why_it_matters,
                "suggested_action": result.action,
                "triggers": triggers,
            }
            with self._ilock:
                self._alerts.append(_alert_rec)
                if len(self._alerts) > 50:
                    self._alerts = self._alerts[-50:]
            # Persist to DB
            try:
                db = SessionLocal()
                try:
                    db.execute(text("""
                        INSERT INTO intelligence_alerts
                            (timestamp, alert_level, what_changed, why_it_matters, action, triggers_json)
                        VALUES (:ts, :lvl, :wc, :wim, :act, :trg)
                    """), {
                        "ts": _alert_rec["timestamp"],
                        "lvl": result.alert_level,
                        "wc": result.what_changed[:500],
                        "wim": result.why_it_matters[:500],
                        "act": result.action[:300],
                        "trg": json.dumps(triggers),
                    })
                    db.commit()
                finally:
                    db.close()
            except Exception as _db_err:
                self.logger.debug(f"Alert DB persist: {_db_err}")
        else:
            alert_text = f"⚠️ CONTEXT ALERT\n{trigger_desc}"

        # Send via alert service
        try:
            global volguard_system
            if volguard_system and volguard_system.alert_service:
                volguard_system.alert_service.send_raw(alert_text)
        except Exception as e:
            self.logger.error(f"Alert send failed: {e}")

    def get_alerts(self, limit: int = 10) -> List[dict]:
        with self._ilock:
            return list(reversed(self._alerts[-limit:]))

    def force_scan(self) -> dict:
        self._scan()
        return {"scanned_at": datetime.now(IST_TZ).isoformat(), "alerts": self.get_alerts(5)}


# ── Intelligence DB Tables ─────────────────────────────────────────────────────────

def _v5_init_tables():
    db = SessionLocal()
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS intelligence_briefs (
                date TEXT PRIMARY KEY,
                global_tone TEXT NOT NULL,
                brief_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """))
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS intelligence_vetoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                strategy TEXT NOT NULL,
                expiry_type TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                regime_score REAL NOT NULL,
                recommendation TEXT NOT NULL,
                veto_reason TEXT,
                rationale TEXT,
                adjustments_json TEXT,
                overridden BOOLEAN DEFAULT FALSE,
                override_reason TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS intelligence_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                alert_level TEXT NOT NULL,
                what_changed TEXT NOT NULL,
                why_it_matters TEXT,
                action TEXT,
                triggers_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.commit()
        logger.info("✅ Intelligence tables initialized")
    except Exception as e:
        logger.error(f"Intelligence table init failed: {e}")
        db.rollback()
    finally:
        db.close()


# ============================================================================
# DATABASE MODELS
# ============================================================================

Base = declarative_base()

class TradeJournal(Base):
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True)
    strategy_id = Column(String, unique=True, index=True)
    strategy_type = Column(String)
    expiry_type = Column(String)
    expiry_date = Column(DateTime)
    entry_time = Column(DateTime)
    exit_time = Column(DateTime, nullable=True)
    legs_data = Column(JSON)
    order_ids = Column(JSON)
    filled_quantities = Column(JSON, nullable=True)
    fill_prices = Column(JSON, nullable=True)
    gtt_order_ids = Column(JSON, nullable=True)
    entry_greeks_snapshot = Column(JSON, nullable=True)
    max_profit = Column(Float)
    max_loss = Column(Float)
    allocated_capital = Column(Float)
    required_margin = Column(Float, default=0.0)
    entry_premium = Column(Float)
    exit_premium = Column(Float, nullable=True)
    realized_pnl = Column(Float, nullable=True)
    pnl_approximate = Column(Boolean, default=False)
    theta_pnl = Column(Float, nullable=True)
    vega_pnl = Column(Float, nullable=True)
    gamma_pnl = Column(Float, nullable=True)
    status = Column(String)
    exit_reason = Column(String, nullable=True)
    is_mock = Column(Boolean, default=False)
    associated_event_date = Column(DateTime, nullable=True)
    associated_event_name = Column(String, nullable=True)
    # ── Context snapshot at entry — the WHY behind each trade ─────────────────────────
    # These fields capture the full market context at the moment of entry.
    # Without this, you can only see what happened (PnL, greeks).
    # With this, the coaching layer can explain WHY it happened.
    regime_score_at_entry     = Column(Float,   nullable=True)  # total regime score 0-10
    vix_at_entry              = Column(Float,   nullable=True)  # India VIX at entry
    ivp_at_entry              = Column(Float,   nullable=True)  # IVP 1yr at entry
    vol_regime_at_entry       = Column(String,  nullable=True)  # RICH / FAIR / CHEAP / EXPLODING
    morning_tone_at_entry     = Column(String,  nullable=True)  # CLEAR / CAUTIOUS / RISK_OFF / MIXED
    pretrade_verdict_at_entry = Column(String,  nullable=True)  # PROCEED / PROCEED_WITH_CAUTION / VETO (overridden)
    vov_zscore_at_entry       = Column(Float,   nullable=True)  # vol-of-vol z-score at entry
    weighted_vrp_at_entry     = Column(Float,   nullable=True)  # weighted VRP % at entry
    score_drivers_at_entry    = Column(JSON,    nullable=True)  # list of score driver strings
    pretrade_rationale        = Column(String,  nullable=True)  # LLM rationale summary (first 500 chars)
    trade_outcome_class       = Column(String,  nullable=True)  # SKILL_WIN / LUCKY_WIN / UNLUCKY_LOSS / SKILL_LOSS
    # ─────────────────────────────────────────────────────────────────────────
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class DailyStats(Base):
    __tablename__ = "daily_stats"
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, unique=True, index=True)
    total_pnl = Column(Float, default=0.0)
    realized_pnl = Column(Float, default=0.0)
    unrealized_pnl = Column(Float, default=0.0)
    trades_count = Column(Integer, default=0)
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    theta_pnl = Column(Float, default=0.0)
    vega_pnl = Column(Float, default=0.0)
    broker_pnl = Column(Float, nullable=True)
    pnl_discrepancy = Column(Float, nullable=True)
    circuit_breaker_triggered = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ============================================================================
# DATABASE SETUP
# ============================================================================

engine = create_engine(
    SystemConfig.DATABASE_URL, 
    connect_args={"check_same_thread": False} if "sqlite" in SystemConfig.DATABASE_URL else {},
    pool_pre_ping=True
)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
Base.metadata.create_all(engine)

# ============================================================================
# TRADE OUTCOME CLASSIFIER
# ============================================================================

def classify_trade_outcome(
    realized_pnl: float,
    theta_pnl: float,
    vega_pnl: float,
    vov_zscore: float,
    regime_score: float,
    morning_tone: str,
    pretrade_verdict: str,
) -> str:
    """
    Classifies a completed trade into one of four outcome categories.
    Uses data already stored at entry — no new inputs required.

    SKILL_WIN    — Good conditions + theta drove the profit.
                   This is the edge working as designed. Fully repeatable.

    LUCKY_WIN    — Either bad/marginal conditions but won anyway,
                   OR good conditions but vega/gamma (not theta) drove the win.
                   Outcome was favourable but not because the system's edge played out.
                   Do not extrapolate from these.

    UNLUCKY_LOSS — Good entry conditions + external shock (vega spike) drove the loss.
                   System read the conditions correctly. Market was random.
                   Accept, learn nothing structural, move on.

    SKILL_LOSS   — Bad conditions entered anyway (override, VETO ignored, RISK_OFF morning)
                   and lost as the system predicted. Completely avoidable.
                   OR: low regime score / high VoV trade that lost. System warned you.

    Thresholds are calibrated to Nifty options writing context:
    - VoV > 1.5σ = elevated regime instability = bad entry territory
    - Regime score < 4.5 = system has low conviction = marginal at best
    - RISK_OFF morning or VETO verdict = system explicitly flagged the risk
    - theta_dominant: |theta_pnl| >= |vega_pnl| = time decay was primary P&L driver
    """
    vov    = float(vov_zscore or 0.0)
    score  = float(regime_score or 0.0)
    tone   = str(morning_tone or '')
    verdict = str(pretrade_verdict or '')
    pnl    = float(realized_pnl or 0.0)
    theta  = float(theta_pnl or 0.0)
    vega   = float(vega_pnl or 0.0)

    # ── Entry quality assessment ───────────────────────────────────────────
    # CAUTIOUS is acceptable — system still allowed entry, just at reduced size.
    # Only RISK_OFF and explicit VETO are "bad entry" conditions.
    good_entry = (
        vov < 1.5 and
        score >= 4.5 and
        tone not in ('RISK_OFF',) and
        verdict not in ('VETO',)
    )

    won = pnl > 0
    # Theta dominance: time decay was the primary P&L source (the actual edge)
    theta_dominant = abs(theta) >= abs(vega)

    # ── Classification matrix ──────────────────────────────────────────────
    if good_entry and won and theta_dominant:
        return 'SKILL_WIN'       # Edge worked exactly as designed. Repeatable.
    elif good_entry and won and not theta_dominant:
        return 'LUCKY_WIN'       # Won but vega/gamma did the work, not theta.
    elif good_entry and not won and not theta_dominant:
        return 'UNLUCKY_LOSS'    # Right conditions, external vol shock hit.
    elif good_entry and not won and theta_dominant:
        return 'SKILL_LOSS'      # Conditions seemed ok but regime score misread.
    elif not good_entry and won:
        return 'LUCKY_WIN'       # Bad conditions, won anyway. Do not repeat.
    else:
        return 'SKILL_LOSS'      # Entered against system's signal. Lost as expected.


def classify_trade_from_obj(trade) -> str:
    """Convenience wrapper — takes a TradeJournal ORM object directly."""
    return classify_trade_outcome(
        realized_pnl=getattr(trade, 'realized_pnl', None) or 0.0,
        theta_pnl=getattr(trade, 'theta_pnl', None) or 0.0,
        vega_pnl=getattr(trade, 'vega_pnl', None) or 0.0,
        vov_zscore=getattr(trade, 'vov_zscore_at_entry', None) or 0.0,
        regime_score=getattr(trade, 'regime_score_at_entry', None) or 0.0,
        morning_tone=getattr(trade, 'morning_tone_at_entry', None) or '',
        pretrade_verdict=getattr(trade, 'pretrade_verdict_at_entry', None) or '',
    )


# ── Session management policy ─────────────────────────────────────────────────
# All database access must use ONE of two patterns:
#
#   1. FastAPI endpoints → use Depends(get_db)
#      The dependency manager opens a session before the request and closes it after.
#
#   2. Background threads / standalone callsites → use get_db_context()
#      Example:
#          with get_db_context() as db:
#              db.query(...)
#
# NEVER create bare SessionLocal() calls without a matching close() in a finally block.
# SessionLocal() without close() leaks connections from the pool.
# ─────────────────────────────────────────────────────────────────────────────

@contextmanager
def get_db_context():
    """Context manager for database sessions outside of FastAPI dependency injection.
    Use this for background threads, startup code, and any non-request callsite."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def get_db():
    """FastAPI dependency — yields a session and always closes it after the request."""
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ============================================================================
# TELEGRAM ALERT SERVICE - THREAD SAFE
# ============================================================================

@dataclass
class AlertMessage:
    title: str
    message: str
    priority: AlertPriority
    timestamp: datetime

class TelegramAlertService:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self._queue = asyncio.Queue(maxsize=100)
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger("TelegramBot")
        self._last_alert_time = {}
        self._loop = None

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._process_queue())
        self.logger.info("✅ Telegram Service Started")

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self._session:
            await self._session.close()

    def send(self, title: str, message: str, priority: AlertPriority = AlertPriority.MEDIUM, throttle_key: str = None):
        if throttle_key:
            last = self._last_alert_time.get(throttle_key)
            if last and (datetime.now() - last).total_seconds() < 300:
                return
            self._last_alert_time[throttle_key] = datetime.now()

        alert = AlertMessage(title, message, priority, datetime.now())
        
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._queue.put(alert), self._loop)
        else:
            try:
                self._queue.put_nowait(alert)
            except asyncio.QueueFull:
                self.logger.error("⚠️ Alert queue full, dropping message")

    async def _process_queue(self):
        while True:
            try:
                alert = await self._queue.get()
                await self._post_to_api(alert)
                self._queue.task_done()
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Telegram Dispatch Error: {e}")

    async def _post_to_api(self, alert: AlertMessage):
        if not self._session:
            return
        text = f"{alert.priority.value} <b>{alert.title}</b>\n\n{alert.message}\n\n<i>{alert.timestamp.strftime('%H:%M:%S')}</i>"
        try:
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            async with self._session.post(f"{self.base_url}/sendMessage", json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    self.logger.error(f"Telegram Failed: {resp.status}")
        except Exception as e:
            self.logger.error(f"Telegram Network Error: {e}")

    def send_raw(self, raw_text: str):
        """Send pre-formatted text directly to Telegram."""
        self.send("Intelligence Alert", raw_text, AlertPriority.MEDIUM)


# ============================================================================
# AUTO TRADING ENGINE
# ============================================================================

class AutoTradingEngine:
    def __init__(self, volguard_system, db_session_factory):
        self.system = volguard_system
        self.db_session_factory = db_session_factory
        self.logger = logging.getLogger(self.__class__.__name__)
        self._lock = threading.RLock()
        self._last_trade_time = {}
        
    def should_trade_expiry(self, expiry_type: str) -> bool:
        with self.db_session_factory() as db:
            active = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value,
                TradeJournal.expiry_type == expiry_type
            ).first()
            return active is None
    
    def execute_mandate(self, analysis_data: Dict, mandate: TradingMandate) -> bool:
        auto_trading = DynamicConfig.get("AUTO_TRADING")
        mock_trading = DynamicConfig.get("ENABLE_MOCK_TRADING")
        
        if not (auto_trading or mock_trading):
            self.logger.info("Auto trading disabled - skipping execution")
            return False
        
        if not mandate.is_trade_allowed:
            self.logger.info(f"Trade not allowed by mandate: {mandate.veto_reasons}")
            return False
        
        with self._lock:
            if not self.should_trade_expiry(mandate.expiry_type):
                self.logger.info(f"Already have active {mandate.expiry_type} trade - skipping")
                return False

            # Block entries before 9:30 AM IST
            ist_now = datetime.now(pytz.timezone('Asia/Kolkata'))
            if ist_now.hour < 9 or (ist_now.hour == 9 and ist_now.minute < 30):
                self.logger.info(
                    f"Entry blocked: {ist_now.strftime('%H:%M')} IST is before 9:30 AM entry window"
                )
                return False

            now = datetime.now()
            last = self._last_trade_time.get(mandate.expiry_type)
            if last and (now - last).total_seconds() < 300:
                self.logger.info(f"Cooldown active for {mandate.expiry_type} - skipping")
                return False
            
            strategy = self.system.construct_strategy_from_mandate(mandate, analysis_data)
            
            if not strategy:
                self.logger.error(f"Failed to construct strategy for {mandate.expiry_type}")
                return False
            
            if not strategy.validation_passed:
                self.logger.error(f"Strategy validation failed: {strategy.validation_errors}")
                return False
            
            with self.db_session_factory() as db:
                # Derive per-expiry regime score and vol metrics
                _expiry_key = mandate.expiry_type.lower()
                _regime_score = analysis_data.get(f"{_expiry_key}_score") or analysis_data.get("weekly_score")
                _vol_metrics = analysis_data.get("vol_metrics")
                # Pass expiry-specific struct and edge metrics
                _struct_key = f"struct_{_expiry_key}"
                _struct_metrics = analysis_data.get(_struct_key) or analysis_data.get("struct_weekly")
                _edge_metrics = analysis_data.get("edge_metrics")
                result = self.system.execute_strategy(
                    strategy, db,
                    external_metrics=analysis_data.get('external_metrics'),
                    mandate=mandate,
                    regime_score=_regime_score,
                    vol_metrics=_vol_metrics,
                    struct_metrics=_struct_metrics,
                    edge_metrics=_edge_metrics,
                )
                
                if result.get("success"):
                    self._last_trade_time[mandate.expiry_type] = now
                    self.logger.info(f"✅ Auto-executed {mandate.expiry_type} {strategy.strategy_type.value}")
                    return True
                else:
                    self.logger.error(f"❌ Execution failed: {result.get('message')}")
                    return False
    
    def evaluate_all_mandates(self, analysis_data: Dict) -> Dict[str, bool]:
        """
        Evaluate and execute all three mandates in sequence.
        Each mandate is independent — a failure in one does not abort the others.
        Allocations: weekly 40%, monthly 40%, next_weekly 20% of BASE_CAPITAL.
        """
        results = {}

        mandates = [
            ("weekly_mandate",      analysis_data.get('weekly_mandate')),
            ("monthly_mandate",     analysis_data.get('monthly_mandate')),
            ("next_weekly_mandate", analysis_data.get('next_weekly_mandate'))
        ]

        for mandate_key, mandate in mandates:
            if mandate and mandate.is_trade_allowed:
                try:
                    success = self.execute_mandate(analysis_data, mandate)
                    results[mandate_key] = success
                    self.logger.info(
                        f"Mandate [{mandate_key}]: {'EXECUTED ✅' if success else 'SKIPPED (conditions not met)'}"
                    )
                except Exception as e:
                    self.logger.error(f"Mandate [{mandate_key}] raised exception: {e}")
                    results[mandate_key] = False
                    # Continue to next mandate — one failure must not block the others
            else:
                results[mandate_key] = False
                reason = "trade not allowed" if mandate else "mandate missing from analysis_data"
                self.logger.info(f"Mandate [{mandate_key}]: SKIPPED ({reason})")

        return results


# ============================================================================
# P&L ATTRIBUTION ENGINE
# ============================================================================

@dataclass
class AttributionResult:
    total_pnl: float
    theta_pnl: float
    vega_pnl: float
    delta_pnl: float
    other_pnl: float
    iv_change: float
    
    def to_dict(self):
        return {k: round(v, 2) for k, v in self.__dict__.items()}

class PnLAttributionEngine:
    def __init__(self, fetcher):
        self.fetcher = fetcher

    def calculate(self, trade_obj, live_prices: Dict, live_greeks: Dict) -> Optional[AttributionResult]:
        if not trade_obj.entry_greeks_snapshot:
            return None

        entry_greeks = json.loads(trade_obj.entry_greeks_snapshot)
        legs_data = json.loads(trade_obj.legs_data)
        
        total_pnl = 0.0
        theta_pnl = 0.0
        vega_pnl = 0.0
        delta_pnl = 0.0
        avg_iv_change = 0.0
        
        for leg in legs_data:
            key = leg['instrument_token']
            qty = leg.get('filled_quantity', leg['quantity'])
            direction = -1 if leg['action'] == 'SELL' else 1
            
            start = entry_greeks.get(key)
            now = live_greeks.get(key)
            current_price = live_prices.get(key)
            
            if not start or not now or not current_price:
                continue

            leg_pnl = (current_price - leg['entry_price']) * qty * direction
            total_pnl += leg_pnl

            avg_theta = (start.get('theta', 0) + now.get('theta', 0)) / 2
            days_held = (datetime.now() - trade_obj.entry_time).total_seconds() / 86400
            theta_pnl += (avg_theta * days_held * qty * direction * -1)

            avg_vega = (start.get('vega', 0) + now.get('vega', 0)) / 2
            iv_diff = now.get('iv', 0) - start.get('iv', 0)
            vega_pnl += (avg_vega * iv_diff * qty * direction)
            
            avg_delta = (start.get('delta', 0) + now.get('delta', 0)) / 2
            spot_diff = now.get('spot_price', 0) - start.get('spot_price', 0)
            delta_pnl += (avg_delta * spot_diff * qty * direction)
            
            avg_iv_change += iv_diff

        other_pnl = total_pnl - (theta_pnl + vega_pnl + delta_pnl)

        return AttributionResult(
            total_pnl=total_pnl,
            theta_pnl=theta_pnl,
            vega_pnl=vega_pnl,
            delta_pnl=delta_pnl,
            other_pnl=other_pnl,
            iv_change=avg_iv_change / len(legs_data) if legs_data else 0
        )


# ============================================================================
# FILL QUALITY TRACKER
# ============================================================================

@dataclass
class FillQualityMetrics:
    order_id: str
    instrument_token: str
    limit_price: float
    fill_price: float
    slippage: float
    slippage_pct: float
    time_to_fill_seconds: float
    partial_fill: bool
    filled_quantity: int
    requested_quantity: int
    timestamp: datetime

class FillQualityTracker:
    def __init__(self):
        self.fills: List[FillQualityMetrics] = []
      
    def record_fill(self, order_id: str, instrument: str, limit_price: float, 
                    fill_price: float, order_time: datetime, fill_time: datetime,
                    filled_qty: int, requested_qty: int, partial: bool = False):
        slippage = fill_price - limit_price
        slippage_pct = (slippage / limit_price * 100) if limit_price > 0 else 0
          
        metric = FillQualityMetrics(
            order_id=order_id,
            instrument_token=instrument,
            limit_price=limit_price,
            fill_price=fill_price,
            slippage=slippage,
            slippage_pct=slippage_pct,
            time_to_fill_seconds=(fill_time - order_time).total_seconds(),
            partial_fill=partial,
            filled_quantity=filled_qty,
            requested_quantity=requested_qty,
            timestamp=fill_time
        )
          
        self.fills.append(metric)
          
        if abs(slippage_pct) > 0.5:
            logger.warning(f"High slippage: {slippage_pct:.2f}% on {instrument}")
      
    def get_stats(self) -> Dict:
        if not self.fills:
            return {"count": 0}
          
        return {
            "total_fills": len(self.fills),
            "avg_slippage_pct": sum(f.slippage_pct for f in self.fills) / len(self.fills),
            "max_slippage_pct": max(f.slippage_pct for f in self.fills),
            "avg_time_to_fill": sum(f.time_to_fill_seconds for f in self.fills) / len(self.fills),
            "partial_fills": sum(1 for f in self.fills if f.partial_fill)
        }


# ============================================================================
# JSON CACHE MANAGER
# ============================================================================

class JSONCacheManager:
    FILE_PATH = "daily_context.json"
    
    def __init__(self, ist_tz=None):
        self.ist_tz = ist_tz or pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_fetch_attempt: Optional[datetime] = None
        self._lock = threading.Lock()
        self._data = self._load()
        from concurrent.futures import ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cache")
        self._calendar_engine = EconomicCalendarEngine()
        
    def _load(self) -> Dict:
        if not os.path.exists(self.FILE_PATH):
            return {}
        try:
            with open(self.FILE_PATH, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    
    def _save(self) -> bool:
        try:
            temp = self.FILE_PATH + ".tmp"
            with open(temp, 'w') as f:
                json.dump(self._data, f, indent=4, default=str)
            os.replace(temp, self.FILE_PATH)
            return True
        except Exception as e:
            self.logger.error(f"Save failed: {e}")
            return False
    
    def set_alert_service(self, alert_service):
        self._calendar_engine.set_alert_service(alert_service)
    
    def get_today_cache(self) -> Optional[Dict]:
        with self._lock:
            if not self._data.get("is_valid"):
                return None
            if self._data.get("cache_date") != str(date.today()):
                return None
            return self._data.copy()
    
    def is_valid_for_today(self) -> bool:
        cache = self.get_today_cache()
        return cache is not None and cache.get("is_valid", False)
    
    def get_context(self) -> Dict:
        return self._data.copy()
    
    def fetch_and_cache(self, force: bool = False) -> bool:
        now = datetime.now(self.ist_tz)
        today = now.date()

        if not force:
            if self.is_valid_for_today():
                self.logger.info("Daily cache already exists")
                return True

        self.logger.info(f"Starting daily fetch at {now}")
        with self._lock:
            self._last_fetch_attempt = now

        try:
            fii_primary, fii_secondary, fii_net_change, fii_date_str, is_fallback = \
                ParticipantDataFetcher.fetch_smart_participant_data()

            events = self._calendar_engine.fetch_calendar(SystemConfig.EVENT_RISK_DAYS_AHEAD)

            new_data = {
                "cache_date": str(today),
                "fetch_timestamp": now.isoformat(),
                "fii_data": {k: asdict(v) if v else None for k, v in fii_primary.items()} if fii_primary else None,
                "fii_secondary": {k: asdict(v) if v else None for k, v in fii_secondary.items()} if fii_secondary else None,
                "fii_net_change": fii_net_change or 0.0,
                "fii_data_date_str": fii_date_str or "NO DATA",
                "fii_is_fallback": is_fallback,
                "economic_events": [asdict(e) for e in events],
                "is_valid": True
            }

            with self._lock:
                self._data = new_data

            success = self._save()
            if success:
                self.logger.info("Daily cache saved")
            return success

        except Exception as e:
            self.logger.error(f"Daily fetch failed: {e}")
            with self._lock:
                self._data = {
                    "cache_date": str(today),
                    "fetch_timestamp": now.isoformat(),
                    "is_valid": False,
                    "error": str(e)
                }
            self._save()
            return False
    
    def get_external_metrics(self) -> ExternalMetrics:
        cache = self.get_today_cache()
        
        if not cache or not cache.get("is_valid"):
            return ExternalMetrics(
                fii_data=None, fii_secondary=None, fii_net_change=0.0,
                fii_conviction="NO_DATA", fii_sentiment="NO_DATA",
                fii_data_date="NO_DATA", fii_is_fallback=True,
                flow_regime="NEUTRAL",
                economic_events=[], veto_event_near=False,
                high_impact_event_near=False, suggested_square_off_time=None,
                risk_score=0.0
            )
        
        # Reconstruct all participants from cache (FII, DII, Pro, Client)
        fii_data = None
        if cache.get("fii_data"):
            fii_data = {}
            for k, v in cache["fii_data"].items():
                if v:
                    try:
                        fii_data[k] = ParticipantData(**v)
                    except Exception:
                        pass
            if not fii_data:
                fii_data = None
        
        fii_secondary = None
        if cache.get("fii_secondary"):
            fii_secondary = {}
            for k, v in cache["fii_secondary"].items():
                if v:
                    try:
                        fii_secondary[k] = ParticipantData(**v)
                    except Exception:
                        pass
        
        events = []
        now_ist = datetime.now(self.ist_tz)
        today_date = now_ist.date()
        for e_dict in cache.get("economic_events", []):
            e_dict_copy = e_dict.copy()

            if 'event_date' in e_dict_copy:
                if isinstance(e_dict_copy['event_date'], str):
                    e_dict_copy['event_date'] = datetime.fromisoformat(e_dict_copy['event_date'])

            if e_dict_copy.get('suggested_square_off_time'):
                if isinstance(e_dict_copy['suggested_square_off_time'], str):
                    e_dict_copy['suggested_square_off_time'] = datetime.fromisoformat(
                        e_dict_copy['suggested_square_off_time']
                    )

            event = EconomicEvent(**e_dict_copy)

            # Recalculate time fields against today — cached values are stale
            if event.event_date:
                event_dt = event.event_date
                if event_dt.tzinfo is None:
                    event_dt = self.ist_tz.localize(event_dt)
                event.days_until = (event_dt.date() - today_date).days
                event.hours_until = (event_dt - now_ist).total_seconds() / 3600

            events.append(event)
        
        fii_net_change = cache.get("fii_net_change", 0.0)
        
        if abs(fii_net_change) > DynamicConfig.get("FII_VERY_HIGH_CONVICTION"):
            conviction = "VERY_HIGH"
        elif abs(fii_net_change) > DynamicConfig.get("FII_HIGH_CONVICTION"):
            conviction = "HIGH"
        elif abs(fii_net_change) > DynamicConfig.get("FII_MODERATE_CONVICTION"):
            conviction = "MODERATE"
        else:
            conviction = "LOW"
        
        sentiment = "BULLISH" if fii_net_change > 0 else "BEARISH" if fii_net_change < 0 else "NEUTRAL"

        flow_regime = "NEUTRAL"
        if fii_data and fii_data.get("FII"):
            fii_obj = fii_data["FII"]
            fut_bullish = fii_obj.fut_net > 0
            opt_bullish = fii_obj.call_net > fii_obj.put_net
            if fut_bullish and opt_bullish:
                flow_regime = "AGGRESSIVE_BULL"
            elif not fut_bullish and not opt_bullish:
                flow_regime = "AGGRESSIVE_BEAR"
            elif fut_bullish and not opt_bullish:
                flow_regime = "GUARDED_BULL"
            elif not fut_bullish and opt_bullish:
                flow_regime = "CONTRARIAN_TRAP"
        
        veto_event_near = any(e.is_veto_event and e.days_until <= 1 for e in events)
        high_impact_event_near = any(
            e.impact_level == "HIGH" and e.days_until <= 2 for e in events
        )
        
        suggested_square_off = None
        for e in events:
            if e.is_veto_event and e.days_until == 1 and e.suggested_square_off_time:
                suggested_square_off = e.suggested_square_off_time
                break
        
        risk_score = 0.0
        if veto_event_near:
            risk_score += 5.0
        if high_impact_event_near:
            risk_score += 2.0
        if abs(fii_net_change) > DynamicConfig.get("FII_VERY_HIGH_CONVICTION"):
            risk_score += 1.0
        
        return ExternalMetrics(
            fii_data=fii_data,
            fii_secondary=fii_secondary,
            fii_net_change=fii_net_change,
            fii_conviction=conviction,
            fii_sentiment=sentiment,
            fii_data_date=cache.get("fii_data_date_str", "NO DATA"),
            fii_is_fallback=cache.get("fii_is_fallback", True),
            flow_regime=flow_regime,
            economic_events=events,
            veto_event_near=veto_event_near,
            high_impact_event_near=high_impact_event_near,
            suggested_square_off_time=suggested_square_off,
            risk_score=risk_score
        )
    
    async def schedule_daily_fetch(self):
        while True:
            try:
                now = datetime.now(self.ist_tz)
                current_hour = now.hour
                current_minute = now.minute

                # Evening refresh: any time from 9 PM onwards, force re-fetch for tomorrow
                if current_hour >= SystemConfig.DAILY_FETCH_TIME_IST.hour:
                    if not self.is_valid_for_today():
                        await asyncio.get_running_loop().run_in_executor(
                            self._executor, self.fetch_and_cache, True
                        )
                    await asyncio.sleep(300)  # check every 5 minutes at night

                # Pre-market warm: from 8:55 AM onwards, ensure cache is ready before open
                elif current_hour == SystemConfig.PRE_MARKET_WARM_TIME_IST.hour and \
                     current_minute >= SystemConfig.PRE_MARKET_WARM_TIME_IST.minute:
                    if not self.is_valid_for_today():
                        await asyncio.get_running_loop().run_in_executor(
                            self._executor, self.fetch_and_cache, True
                        )
                    await asyncio.sleep(60)

                else:
                    await asyncio.sleep(60)

            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)
    
    def __del__(self):
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)


# ============================================================================
# PARTICIPANT DATA FETCHER
# ============================================================================

class ParticipantDataFetcher:

    @staticmethod
    def _get_candidate_dates(days_back: int = 5):
        """Get recent business days to try fetching data from"""
        tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(tz)
        dates = []
        candidate = now
        # If before 6pm IST, today's data likely not released yet — start from yesterday
        if candidate.hour < 18:
            candidate -= timedelta(days=1)
        while len(dates) < days_back:
            if candidate.weekday() < 5:  # Monday=0, Friday=4
                dates.append(candidate)
            candidate -= timedelta(days=1)
        return dates

    @staticmethod
    def _fetch_csv_for_date(date_obj) -> Optional[pd.DataFrame]:
        """Fetch NSE participant OI CSV archive for a given date"""
        date_str = date_obj.strftime('%d%m%Y')
        url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv"
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "*/*",
                "Connection": "keep-alive"
            }
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                content = r.content.decode('utf-8')
                if "Future Index Long" in content:
                    lines = content.splitlines()
                    for idx, line in enumerate(lines[:20]):
                        if "Future Index Long" in line:
                            df = pd.read_csv(io.StringIO(content), skiprows=idx)
                            df.columns = df.columns.str.strip()
                            return df
        except Exception as e:
            logger.debug(f"CSV fetch failed for {date_obj.strftime('%d-%b')}: {e}")
        return None

    @staticmethod
    def _parse_csv_row(row) -> ParticipantData:
        """Parse a CSV row into ParticipantData with full call/put split"""
        def g(col):
            try:
                return float(str(row[col]).replace(',', '').strip())
            except Exception:
                return 0.0

        fut_long   = g('Future Index Long')
        fut_short  = g('Future Index Short')
        call_long  = g('Option Index Call Long')
        call_short = g('Option Index Call Short')
        put_long   = g('Option Index Put Long')
        put_short  = g('Option Index Put Short')
        stk_long   = g('Future Stock Long') if 'Future Stock Long' in row.index else 0.0
        stk_short  = g('Future Stock Short') if 'Future Stock Short' in row.index else 0.0

        fut_net  = fut_long - fut_short
        call_net = call_long - call_short
        put_net  = put_long - put_short
        stk_net  = stk_long - stk_short

        return ParticipantData(
            fut_long=fut_long,
            fut_short=fut_short,
            fut_net=fut_net,
            call_long=call_long,
            call_short=call_short,
            call_net=call_net,
            put_long=put_long,
            put_short=put_short,
            put_net=put_net,
            stock_net=stk_net,
            total_net=fut_net + call_net + put_net
        )

    @staticmethod
    def _parse_df(df) -> Dict[str, ParticipantData]:
        """Parse full CSV dataframe into all participant objects"""
        result = {}
        for p_label, p_key in [("FII", "FII"), ("DII", "DII"), ("Pro", "Pro"), ("Client", "Client")]:
            try:
                mask = df['Client Type'].astype(str).str.contains(p_label, case=False, na=False)
                row = df[mask].iloc[0]
                result[p_key] = ParticipantDataFetcher._parse_csv_row(row)
            except Exception:
                result[p_key] = None
        return result

    @staticmethod
    def fetch_smart_participant_data() -> Tuple[Optional[Dict], Optional[Dict], float, str, bool]:
        """
        Fetch FII/DII/Pro/Client participant data from NSE CSV archives.
        Tries recent business days until data is found.
        Returns: (primary_data, secondary_data, fii_net_change, date_str, is_fallback)
        """
        dates = ParticipantDataFetcher._get_candidate_dates()
        primary_data = None
        primary_date = None
        secondary_data = None

        for d in dates:
            logger.info(f"  📊 Trying NSE CSV for {d.strftime('%d-%b')}...")
            df = ParticipantDataFetcher._fetch_csv_for_date(d)
            if df is not None:
                primary_data = ParticipantDataFetcher._parse_df(df)
                primary_date = d
                logger.info(f"  ✅ NSE CSV found for {d.strftime('%d-%b')}")

                # Fetch previous day for net change calculation
                prev = d - timedelta(days=1)
                while prev.weekday() >= 5:
                    prev -= timedelta(days=1)
                df_prev = ParticipantDataFetcher._fetch_csv_for_date(prev)
                if df_prev is not None:
                    secondary_data = ParticipantDataFetcher._parse_df(df_prev)
                break
            else:
                logger.info(f"  ❌ NSE CSV missing for {d.strftime('%d-%b')}")

        if primary_data is None:
            logger.error("All NSE CSV dates failed — returning fallback")
            return None, None, 0.0, "FALLBACK", True

        # FII net change = today's fut_net minus yesterday's fut_net
        fii_net_change = 0.0
        if primary_data.get('FII') and secondary_data and secondary_data.get('FII'):
            fii_net_change = primary_data['FII'].fut_net - secondary_data['FII'].fut_net

        ist = pytz.timezone('Asia/Kolkata')
        today = datetime.now(ist).date()
        is_fallback = (primary_date.date() != today)
        date_str = primary_date.strftime('%d-%b-%Y')

        return primary_data, secondary_data, fii_net_change, date_str, is_fallback


# ============================================================================
# ECONOMIC CALENDAR ENGINE
# ============================================================================

class EconomicCalendarEngine:
    def __init__(self):
        self.utc_tz = pytz.UTC
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cache = {}
        self.cache_timestamp = None
        self.alert_service = None
    
    def set_alert_service(self, alert_service):
        self.alert_service = alert_service
    
    def get_square_off_for_event(self, event_date: datetime) -> Optional[datetime]:
        ist_date = event_date.astimezone(self.ist_tz)
        event_day = ist_date.date()
        event_time_ist = ist_date.time()
        today = datetime.now(self.ist_tz).date()

        days_until = (event_day - today).days

        # Time-aware logic for Indian traders:
        # FOMC and other US events happen late night IST (e.g. 2:30 AM IST).
        # For events AFTER market close (15:30 IST) or at night — the risk
        # actually starts building the previous evening. So we square off
        # 1 trading day before the event date at 14:00 IST in all cases.
        # This correctly handles:
        #   RBI MPC (10:00 AM IST, day of event) → square off day before
        #   FOMC    (02:30 AM IST, night after)   → square off day before
        MARKET_CLOSE = dt_time(15, 30)

        if event_time_ist > MARKET_CLOSE or days_until <= 0:
            # Night event or already here — square off 1 trading day before event date
            square_off_date = event_day - timedelta(days=1)
        elif days_until <= 1:
            # Event is tomorrow during market hours — square off today
            square_off_date = today
        else:
            # Event is 2+ days away — square off 1 day before event date
            square_off_date = event_day - timedelta(days=1)

        # Skip weekends — move back to Friday
        if square_off_date.weekday() == 5:   # Saturday
            square_off_date = square_off_date - timedelta(days=1)
        elif square_off_date.weekday() == 6:  # Sunday
            square_off_date = square_off_date - timedelta(days=2)

        # If square off date is already in the past, use today
        if square_off_date < today:
            square_off_date = today

        square_off = self.ist_tz.localize(
            datetime.combine(square_off_date, SystemConfig.PRE_EVENT_SQUARE_OFF_TIME)
        )

        return square_off
    
    def classify_event(self, title: str, country: str, importance: int, 
                       event_datetime: datetime) -> Tuple[str, bool, Optional[datetime]]:
        is_veto = False
        event_type = "OTHER"
        suggested_square_off = None
        
        title_upper = title.upper()
        
        for keyword in SystemConfig.VETO_KEYWORDS:
            if keyword.upper() in title_upper:
                is_veto = True
                if "RBI" in keyword or "REPO" in keyword or "MPC" in keyword:
                    event_type = "RBI_POLICY"
                elif "FOMC" in keyword or "FED" in keyword:
                    event_type = "FOMC"
                elif "BUDGET" in keyword:
                    event_type = "BUDGET"
                break
        
        if not is_veto:
            for keyword in SystemConfig.HIGH_IMPACT_KEYWORDS:
                if keyword.upper() in title_upper:
                    event_type = "HIGH_IMPACT"
                    break
        
        if importance == 1 and country in ["IN", "US"]:
            if event_type == "OTHER":
                event_type = "HIGH_IMPACT"
        
        if is_veto:
            suggested_square_off = self.get_square_off_for_event(event_datetime)
        
        return event_type, is_veto, suggested_square_off
    
    def fetch_calendar(self, days_ahead: int = 14) -> List[EconomicEvent]:
        """
        Fetch economic calendar events.
        Sources (in order):
          1. TradingView (primary — free, open, works from AWS, same as Colab)
          2. Investing.com (fallback)
        Cache: 6 hours in-memory. On failure, returns last known good data.
        """
        now = datetime.now(self.ist_tz)

        # Return in-memory cache if still fresh (6 hours)
        if self.cache_timestamp and (now - self.cache_timestamp).total_seconds() < 21600:
            if self.cache.get('events'):
                self.logger.info("📅 Calendar: returning cached events")
                return self.cache['events']

        events = []
        sources = [
            self._fetch_from_tradingview,
            self._fetch_from_investing_com,
        ]

        for source in sources:
            try:
                events = source(days_ahead)
                if events:
                    self.cache['events'] = events
                    self.cache_timestamp = now
                    self.logger.info(f"✅ Calendar: fetched {len(events)} events from {source.__name__}")
                    break
            except Exception as e:
                self.logger.warning(f"Calendar source {source.__name__} failed: {e}")
                continue

        if not events:
            # Return last known good data rather than empty list — important for veto safety
            if self.cache.get('events'):
                self.logger.warning("⚠️ Calendar: all sources failed — returning stale cache")
                return self.cache['events']

            self.logger.error("❌ Calendar: no events from any source and no cache")
            if self.alert_service:
                self.alert_service.send(
                    "🚨 Calendar API Failure",
                    "All economic calendar APIs failed — no veto events detected. Trading decisions may be at risk.",
                    AlertPriority.CRITICAL,
                    throttle_key="calendar_failure"
                )

        return events

    def _fetch_from_tradingview(self, days_ahead: int) -> List[EconomicEvent]:
        """
        TradingView economic calendar — same endpoint Colab uses.
        Free, open, no auth required, works from any IP including AWS.
        """
        events = []
        try:
            now_ist = datetime.now(self.ist_tz)
            now_utc = datetime.now(self.utc_tz)
            today = now_ist.date()
            end_utc = now_utc + timedelta(days=days_ahead)

            url = "https://economic-calendar.tradingview.com/events"
            params = {
                "from": now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to": end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "countries": "IN,US",
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Origin": "https://www.tradingview.com",
                "Accept": "application/json",
            }

            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code != 200:
                return events

            data = response.json()
            if 'result' not in data:
                return events

            for e in data['result']:
                try:
                    importance_code = e.get('importance', -1)
                    if importance_code < 0:
                        continue

                    utc_time = datetime.strptime(
                        e['date'], "%Y-%m-%dT%H:%M:%S.000Z"
                    ).replace(tzinfo=self.utc_tz)
                    ist_time = utc_time.astimezone(self.ist_tz)

                    days_until = (ist_time.date() - today).days
                    if days_until < 0 or days_until > days_ahead:
                        continue

                    title   = e.get('title', '')
                    country = e.get('country', '')

                    # importance_code: 1=HIGH, 0=MEDIUM
                    importance = 1 if importance_code == 1 else 2
                    event_type, is_veto, square_off = self.classify_event(
                        title, country, importance, ist_time
                    )

                    impact_label = "VETO" if is_veto else "HIGH" if importance == 1 else "MEDIUM"

                    events.append(EconomicEvent(
                        title=title,
                        country=country,
                        event_date=ist_time,
                        impact_level=impact_label,
                        event_type=event_type,
                        forecast=str(e.get('forecast', '-')),
                        previous=str(e.get('previous', '-')),
                        days_until=days_until,
                        hours_until=(ist_time - now_ist).total_seconds() / 3600,
                        is_veto_event=is_veto,
                        suggested_square_off_time=square_off
                    ))
                except Exception:
                    continue

            events.sort(key=lambda x: (0 if x.is_veto_event else 1, x.days_until, x.event_date))

        except Exception as e:
            self.logger.error(f"TradingView calendar fetch error: {e}")

        return events
    
    def _fetch_from_investing_com(self, days_ahead: int) -> List[EconomicEvent]:
        """Investing.com fallback calendar source"""
        events = []
        try:
            now_ist = datetime.now(self.ist_tz)
            today = now_ist.date()

            url = "https://api.investing.com/api/financialdata/events/economic"
            params = {
                "limit": 100,
                "from": today.isoformat(),
                "to": (today + timedelta(days=days_ahead)).isoformat(),
                "importance": "1,2,3"
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
            }

            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code != 200:
                return events

            data = response.json()
            for e in data.get('data', []):
                try:
                    date_str = e.get('date', e.get('time', ''))
                    if 'T' in date_str:
                        utc_time = datetime.strptime(
                            date_str.split('.')[0], "%Y-%m-%dT%H:%M:%S"
                        ).replace(tzinfo=self.utc_tz)
                    else:
                        utc_time = datetime.strptime(date_str, "%Y-%m-%d").replace(
                            hour=12, minute=0, tzinfo=self.utc_tz
                        )

                    ist_time = utc_time.astimezone(self.ist_tz)
                    days_until = (ist_time.date() - today).days
                    if days_until < 0 or days_until > days_ahead:
                        continue

                    title = e.get('title', e.get('event', 'Unknown Event'))
                    country = e.get('country', e.get('region', ''))
                    importance = int(e.get('importance', e.get('impact', 3)))

                    event_type, is_veto, square_off = self.classify_event(
                        title, country, importance, ist_time
                    )
                    impact_label = "VETO" if is_veto else "HIGH" if importance <= 2 else "MEDIUM"

                    events.append(EconomicEvent(
                        title=title,
                        country=country,
                        event_date=ist_time,
                        impact_level=impact_label,
                        event_type=event_type,
                        forecast=str(e.get('forecast', '-')),
                        previous=str(e.get('previous', '-')),
                        days_until=days_until,
                        hours_until=(ist_time - now_ist).total_seconds() / 3600,
                        is_veto_event=is_veto,
                        suggested_square_off_time=square_off
                    ))
                except Exception:
                    continue

            events.sort(key=lambda x: (0 if x.is_veto_event else 1, x.days_until, x.event_date))

        except Exception as e:
            self.logger.error(f"Investing.com fetch error: {e}")

        return events


# ============================================================================
# MARKET DATA STREAMER
# ============================================================================

@dataclass
class MarketUpdate:
    instrument_key: str
    ltp: float
    ltt: Optional[int] = None
    ltq: Optional[int] = None
    cp: Optional[float] = None
    volume: Optional[int] = None
    oi: Optional[int] = None
    bid_price: Optional[float] = None
    bid_qty: Optional[int] = None
    ask_price: Optional[float] = None
    ask_qty: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def from_feed(cls, instrument_key: str, feed_data: Any) -> 'MarketUpdate':
        update = cls(instrument_key=instrument_key, ltp=0.0)
        
        if hasattr(feed_data, 'ltpc'):
            update.ltp = getattr(feed_data.ltpc, 'ltp', 0.0) or 0.0
            update.ltt = getattr(feed_data.ltpc, 'ltt', None)
            update.ltq = getattr(feed_data.ltpc, 'ltq', None)
            update.cp = getattr(feed_data.ltpc, 'cp', None)
        
        if hasattr(feed_data, 'full'):
            full = feed_data.full
            update.ltp = getattr(full, 'ltp', update.ltp) or update.ltp
            update.volume = getattr(full, 'volume', None)
            update.oi = getattr(full, 'oi', None)
            
            if hasattr(full, 'market_quotes') and full.market_quotes:
                depth = full.market_quotes
                if hasattr(depth, 'bid'):
                    bids = depth.bid
                    if bids and len(bids) > 0:
                        update.bid_price = getattr(bids[0], 'price', None)
                        update.bid_qty = getattr(bids[0], 'quantity', None)
                if hasattr(depth, 'ask'):
                    asks = depth.ask
                    if asks and len(asks) > 0:
                        update.ask_price = getattr(asks[0], 'price', None)
                        update.ask_qty = getattr(asks[0], 'quantity', None)
        
        return update

class VolGuardMarketStreamer:
    MODE_MAP = {
        "ltpc": "ltpc",
        "full": "full", 
        "option_greeks": "option_greeks",
        "full_d30": "full_d30"
    }
    
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.streamer: Optional[upstox_client.MarketDataStreamerV3] = None
        self._callbacks: Dict[str, List[Callable]] = {
            "message": [],
            "open": [],
            "close": [],
            "error": [],
            "reconnecting": [],
            "autoReconnectStopped": []
        }
        self._lock = threading.RLock()
        self._subscribed_instruments: Dict[str, str] = {}
        self.is_connected = False
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._latest_prices: Dict[str, float] = {}
        self._latest_updates: Dict[str, MarketUpdate] = {}
        self._ws_request_timestamps = []
        self._ws_rate_limit_lock = threading.RLock()
        
    def _check_ws_rate_limit(self):
        with self._ws_rate_limit_lock:
            now = time.time()
            self._ws_request_timestamps = [t for t in self._ws_request_timestamps 
                                          if now - t < 1.0]
            
            if len(self._ws_request_timestamps) >= 50:
                sleep_time = self._ws_request_timestamps[0] + 1.0 - now
                if sleep_time > 0:
                    self.logger.warning(f"WebSocket rate limit reached, sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            
            self._ws_request_timestamps.append(now)
        
    def on(self, event: str, callback: Callable):
        with self._lock:
            if event in self._callbacks:
                self._callbacks[event].append(callback)
    
    def connect(self, instrument_keys: Optional[List[str]] = None, mode: str = "ltpc"):
        try:
            sdk_mode = self.MODE_MAP.get(mode, "ltpc")
            
            if instrument_keys:
                self.streamer = upstox_client.MarketDataStreamerV3(
                    self.api_client,
                    instrument_keys,
                    sdk_mode
                )
            else:
                self.streamer = upstox_client.MarketDataStreamerV3(
                    self.api_client
                )
            
            self.streamer.on("open", self._on_sdk_open)
            self.streamer.on("close", self._on_sdk_close)
            self.streamer.on("message", self._on_sdk_message)
            self.streamer.on("error", self._on_sdk_error)
            self.streamer.on("reconnecting", self._on_sdk_reconnecting)
            self.streamer.on("autoReconnectStopped", self._on_sdk_auto_reconnect_stopped)
            
            self.streamer.auto_reconnect(True, 2, 30)
            self.streamer.connect()
            self.logger.info(f"MarketDataStreamerV3 connecting with mode={sdk_mode}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect MarketDataStreamerV3: {e}")
            self._dispatch("error", {"type": "connection_error", "message": str(e)})
    
    def subscribe(self, instrument_keys: List[str], mode: str):
        with self._lock:
            if not self.streamer:
                self.logger.error("Streamer not connected")
                return False
            if not self.is_connected:
                self.logger.error("Cannot subscribe - streamer not connected")
                return False
            
            try:
                sdk_mode = self.MODE_MAP.get(mode, "ltpc")
                
                for key in instrument_keys:
                    if '|' not in key:
                        self.logger.error(f"Invalid instrument key format: {key}")
                        return False
                    self._subscribed_instruments[key] = sdk_mode
                
                self._check_ws_rate_limit()
                self.streamer.subscribe(instrument_keys, sdk_mode)
                self.logger.info(f"Subscribed to {len(instrument_keys)} instruments in {sdk_mode} mode")
                return True
                
            except Exception as e:
                self.logger.error(f"Subscribe failed: {e}")
                return False
    
    def unsubscribe(self, instrument_keys: List[str]):
        with self._lock:
            if not self.streamer:
                return False
            
            try:
                self._check_ws_rate_limit()
                self.streamer.unsubscribe(instrument_keys)
                
                for key in instrument_keys:
                    self._subscribed_instruments.pop(key, None)
                    self._latest_prices.pop(key, None)
                    self._latest_updates.pop(key, None)
                    
                self.logger.info(f"Unsubscribed from {len(instrument_keys)} instruments")
                return True
                
            except Exception as e:
                self.logger.error(f"Unsubscribe failed: {e}")
                return False
    
    def change_mode(self, instrument_keys: List[str], mode: str):
        with self._lock:
            if not self.streamer:
                return False
            
            try:
                sdk_mode = self.MODE_MAP.get(mode, "ltpc")
                self._check_ws_rate_limit()
                self.streamer.change_mode(instrument_keys, sdk_mode)
                
                for key in instrument_keys:
                    self._subscribed_instruments[key] = sdk_mode
                    
                self.logger.info(f"Changed mode to {sdk_mode} for {len(instrument_keys)} instruments")
                return True
                
            except Exception as e:
                self.logger.error(f"Change mode failed: {e}")
                return False
    
    def disconnect(self):
        with self._lock:
            if self.streamer:
                try:
                    self.streamer.disconnect()
                    self.is_connected = False
                    self.logger.info("MarketDataStreamerV3 disconnected")
                except Exception as e:
                    self.logger.error(f"Disconnect error: {e}")
    
    def get_ltp(self, instrument_key: str) -> Optional[float]:
        with self._lock:
            return self._latest_prices.get(instrument_key)
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        with self._lock:
            return {k: self._latest_prices.get(k) for k in instrument_keys}
    
    def get_subscribed_instruments(self) -> Dict[str, str]:
        with self._lock:
            return self._subscribed_instruments.copy()
    
    def _on_sdk_open(self):
        self.is_connected = True
        self._latest_prices = {}
        self._latest_updates = {}
        self.logger.info("MarketDataStreamerV3 connected")
        self._dispatch("open", {"status": "connected"})
    
    def _on_sdk_close(self, *args, **kwargs):
        self.is_connected = False
        self.logger.info("MarketDataStreamerV3 disconnected")
        self._dispatch("close", {"status": "disconnected"})
    
    def _on_sdk_message(self, message):
        try:
            if hasattr(message, 'feeds'):
                for instrument_key, feed_data in message.feeds.items():
                    update = MarketUpdate.from_feed(instrument_key, feed_data)
                    
                    with self._lock:
                        self._latest_prices[instrument_key] = update.ltp
                        self._latest_updates[instrument_key] = update
                    
                    self._dispatch("message", {
                        "type": "market_update",
                        "instrument_key": instrument_key,
                        "data": update
                    })
                
        except Exception as e:
            self.logger.error(f"Error processing market message: {e}")
    
    def _on_sdk_error(self, error):
        self.logger.error(f"MarketDataStreamerV3 error: {error}")
        self._dispatch("error", {"type": "sdk_error", "message": str(error)})
    
    def _on_sdk_reconnecting(self, attempt):
        self.logger.warning(f"MarketDataStreamerV3 reconnecting (attempt {attempt})")
        self._dispatch("reconnecting", {"attempt": attempt})
    
    def _on_sdk_auto_reconnect_stopped(self):
        self.logger.error("MarketDataStreamerV3 auto-reconnect stopped")
        self._dispatch("autoReconnectStopped", {"status": "stopped"})
    
    def _dispatch(self, event: str, data: Any):
        with self._lock:
            for callback in self._callbacks.get(event, []):
                try:
                    callback(data)
                except Exception as e:
                    self.logger.error(f"Callback error for {event}: {e}")


# ============================================================================
# PORTFOLIO DATA STREAMER
# ============================================================================

class VolGuardPortfolioStreamer:
    UPSTOX_STATUS_MAP = {
        "put order req received": "PENDING",
        "validation pending": "PENDING",
        "open pending": "PENDING",
        "open": "OPEN",
        "complete": "FILLED",
        "rejected": "REJECTED",
        "cancelled": "CANCELLED",
        "partial": "PARTIAL"
    }
    
    def __init__(self, api_client: upstox_client.ApiClient, fetcher):
        self.api_client = api_client
        self.fetcher = fetcher
        self.streamer: Optional[upstox_client.PortfolioDataStreamer] = None
        self._callbacks: Dict[str, List[Callable]] = {
            "message": [],
            "open": [],
            "close": [],
            "error": [],
            "reconnecting": [],
            "autoReconnectStopped": []
        }
        self._lock = threading.RLock()
        self.is_connected = False
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._latest_orders: Dict[str, Dict] = {}
        self._order_fills: Dict[str, Dict] = {}
        # GTT fill detection callbacks — registered by VolGuardSystem
        # Signature: callback(order_id: str, fill_info: Dict) -> None
        self._gtt_fill_callbacks: List[Callable] = []
    
    def register_gtt_fill_callback(self, cb: Callable) -> None:
        """Register a callback that fires whenever any order reaches FILLED status.
        Used by VolGuardSystem to auto-close TradeJournal when a GTT stop-loss fires."""
        with self._lock:
            self._gtt_fill_callbacks.append(cb)
    
    def on(self, event: str, callback: Callable):
        with self._lock:
            if event in self._callbacks:
                self._callbacks[event].append(callback)
    
    def connect(self, 
                order_update: bool = True,
                position_update: bool = True,
                holding_update: bool = True,
                gtt_update: bool = True):
        try:
            self.streamer = upstox_client.PortfolioDataStreamer(
                self.api_client,
                order_update=order_update,
                position_update=position_update,
                holding_update=holding_update,
                gtt_update=gtt_update
            )
            
            self.streamer.on("open", self._on_sdk_open)
            self.streamer.on("close", self._on_sdk_close)
            self.streamer.on("message", self._on_sdk_message)
            self.streamer.on("error", self._on_sdk_error)
            self.streamer.on("reconnecting", self._on_sdk_reconnecting)
            self.streamer.on("autoReconnectStopped", self._on_sdk_auto_reconnect_stopped)
            
            self.streamer.auto_reconnect(True, 2, 30)
            self.streamer.connect()
            
            update_types = []
            if order_update: update_types.append("order")
            if position_update: update_types.append("position")
            if holding_update: update_types.append("holding")
            if gtt_update: update_types.append("gtt_order")
            
            self.logger.info(f"PortfolioDataStreamer connecting - updates: {update_types}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect PortfolioDataStreamer: {e}")
            self._dispatch("error", {"type": "connection_error", "message": str(e)})
    
    def disconnect(self):
        with self._lock:
            if self.streamer:
                try:
                    self.streamer.disconnect()
                    self.is_connected = False
                    self.logger.info("PortfolioDataStreamer disconnected")
                except Exception as e:
                    self.logger.error(f"Disconnect error: {e}")
    
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        with self._lock:
            if order_id in self._latest_orders:
                return self._latest_orders[order_id]
            
            try:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop is not None and loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(
                        self._fetch_order_status_async(order_id),
                        loop
                    )
                    return future.result(timeout=5)
                else:
                    return self._fetch_order_status_sync(order_id)
                    
            except Exception as e:
                self.logger.error(f"Failed to fetch order {order_id} status: {e}")
                return None
    
    def _fetch_order_status_sync(self, order_id: str) -> Optional[Dict]:
        try:
            response = self.fetcher.order_api.get_order_details(
                api_version="2.0", 
                order_id=order_id
            )
            
            if response.status == "success" and response.data and len(response.data) > 0:
                data = response.data[0]
                status_info = {
                    "order_id": data.order_id,
                    "status": data.status,
                    "filled_quantity": data.filled_quantity,
                    "average_price": data.average_price,
                    "pending_quantity": data.pending_quantity,
                    "timestamp": datetime.now().isoformat(),
                    "source": "rest_fallback"
                }
                
                mapped_status = self.UPSTOX_STATUS_MAP.get(data.status, "UNKNOWN")
                status_info["mapped_status"] = mapped_status
                
                if data.filled_quantity > 0 and data.filled_quantity < data.quantity:
                    status_info["is_partial"] = True
                    status_info["remaining_quantity"] = data.quantity - data.filled_quantity
                else:
                    status_info["is_partial"] = False
                
                with self._lock:
                    self._latest_orders[order_id] = status_info
                    self._order_fills[order_id] = {
                        "filled_qty": data.filled_quantity,
                        "avg_price": data.average_price,
                        "last_update": datetime.now().isoformat()
                    }
                
                return status_info
                
        except Exception as e:
            self.logger.error(f"REST order status failed for {order_id}: {e}")
        
        return None
    
    async def _fetch_order_status_async(self, order_id: str) -> Optional[Dict]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._fetch_order_status_sync, order_id
        )
    
    def get_order_fills(self, order_id: str) -> Optional[Dict]:
        with self._lock:
            return self._order_fills.get(order_id)
    
    def _on_sdk_open(self):
        self.is_connected = True
        self.logger.info("PortfolioDataStreamer connected")
        self._dispatch("open", {"status": "connected"})
    
    def _on_sdk_close(self, *args, **kwargs):
        self.is_connected = False
        self.logger.info("PortfolioDataStreamer disconnected")
        self._dispatch("close", {"status": "disconnected"})
    
    def _on_sdk_message(self, message):
        try:
            if hasattr(message, 'update_type') and message.update_type == "order":
                order_data = message
                
                order_id = getattr(order_data, 'order_id', None)
                if order_id:
                    status_info = {
                        "order_id": order_id,
                        "status": getattr(order_data, 'status', 'UNKNOWN'),
                        "filled_quantity": getattr(order_data, 'filled_quantity', 0),
                        "average_price": getattr(order_data, 'average_price', 0),
                        "pending_quantity": getattr(order_data, 'pending_quantity', 0),
                        "timestamp": datetime.now().isoformat(),
                        "source": "websocket"
                    }
                    
                    mapped_status = self.UPSTOX_STATUS_MAP.get(status_info["status"], "UNKNOWN")
                    status_info["mapped_status"] = mapped_status
                    
                    if (status_info["filled_quantity"] > 0 and 
                        status_info["filled_quantity"] < getattr(order_data, 'quantity', 0)):
                        status_info["is_partial"] = True
                    else:
                        status_info["is_partial"] = False
                    
                    with self._lock:
                        self._latest_orders[order_id] = status_info
                        
                        if status_info["filled_quantity"] > 0:
                            self._order_fills[order_id] = {
                                "filled_qty": status_info["filled_quantity"],
                                "avg_price": status_info["average_price"],
                                "last_update": status_info["timestamp"]
                            }
                    
                    # ── GTT fill detection ────────────────────────────────────
                    # When a GTT stop-loss or target fires at the exchange,
                    # Upstox sends an order update with status "complete".
                    # Fire callbacks so VolGuardSystem can auto-close the trade.
                    if mapped_status == "FILLED":
                        tag = getattr(order_data, 'tag', '') or ''
                        variety = getattr(order_data, 'variety', '') or ''
                        is_gtt = (variety in ('gtt', 'amo') or 'GTT' in tag.upper()
                                  or 'SL' in tag.upper() or 'TGT' in tag.upper())
                        if is_gtt:
                            self.logger.info(
                                f"GTT fill detected: order_id={order_id} "
                                f"tag={tag} avg_price={status_info['average_price']}"
                            )
                            with self._lock:
                                cbs = list(self._gtt_fill_callbacks)
                            for cb in cbs:
                                try:
                                    cb(order_id, status_info)
                                except Exception as _cbe:
                                    self.logger.error(f"GTT fill callback error: {_cbe}")
            
            self._dispatch("message", message)
            
        except Exception as e:
            self.logger.error(f"Error processing portfolio message: {e}")
    
    def _on_sdk_error(self, error):
        self.logger.error(f"PortfolioDataStreamer error: {error}")
        self._dispatch("error", {"type": "sdk_error", "message": str(error)})
    
    def _on_sdk_reconnecting(self, attempt):
        self.logger.warning(f"PortfolioDataStreamer reconnecting (attempt {attempt})")
        self._dispatch("reconnecting", {"attempt": attempt})
    
    def _on_sdk_auto_reconnect_stopped(self):
        self.logger.error("PortfolioDataStreamer auto-reconnect stopped")
        self._dispatch("autoReconnectStopped", {"status": "stopped"})
    
    def _dispatch(self, event: str, data: Any):
        with self._lock:
            for callback in self._callbacks.get(event, []):
                try:
                    callback(data)
                except Exception as e:
                    self.logger.error(f"Callback error for {event}: {e}")


# ============================================================================
# UPSTOX FETCHER - WITH CONNECTION POOLING
# ============================================================================

class UpstoxFetcher:
    def __init__(self, token: str):
        if not token:
            raise ValueError("Upstox access token is required!")
        
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = token
        self.configuration.host = "https://api.upstox.com"
        
        self.api_client = upstox_client.ApiClient(self.configuration)
        
        pool_manager = urllib3.PoolManager(
            num_pools=10,
            maxsize=10,
            block=False,
            socket_options=[
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60),
                (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10),
                (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6),
            ]
        )
        self.api_client.rest_client.pool_manager = pool_manager
        
        self.history_api = upstox_client.HistoryV3Api(self.api_client)
        self.quote_api = upstox_client.MarketQuoteApi(self.api_client)
        self.options_api = upstox_client.OptionsApi(self.api_client)
        self.user_api = upstox_client.UserApi(self.api_client)
        self.order_api = upstox_client.OrderApi(self.api_client)
        self.order_api_v3 = upstox_client.OrderApiV3(self.api_client)
        self.quote_api_v3 = upstox_client.MarketQuoteV3Api(self.api_client)
        self.portfolio_api = upstox_client.PortfolioApi(self.api_client)
        self.charge_api = upstox_client.ChargeApi(self.api_client)
        self.pnl_api = upstox_client.TradeProfitAndLossApi(self.api_client)
        self.market_api = upstox_client.MarketHolidaysAndTimingsApi(self.api_client)
        
        self.market_streamer = VolGuardMarketStreamer(self.api_client)
        self.portfolio_streamer = VolGuardPortfolioStreamer(self.api_client, self)
        
        self.smart_fetcher = SmartDataFetcher(self)
        
        self.fill_tracker = FillQualityTracker()
        
        self._request_timestamps = []
        self._rate_limit_lock = threading.RLock()
        self._rate_limit_condition = threading.Condition(self._rate_limit_lock)
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="fetcher")
        
        self._lot_size_cache: Dict[str, int] = {}
        self._lot_size_cache_date: Optional[date] = None
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("✅ UpstoxFetcher initialized")

    def _reinit_apis(self):
        """Re-create all 11 API client instances from the current self.api_client.
        Called after a token hot-swap via /api/system/token/update so every subsequent
        SDK call immediately uses the new token — no restart needed."""
        self.history_api    = upstox_client.HistoryV3Api(self.api_client)
        self.quote_api      = upstox_client.MarketQuoteApi(self.api_client)
        self.options_api    = upstox_client.OptionsApi(self.api_client)
        self.user_api       = upstox_client.UserApi(self.api_client)
        self.order_api      = upstox_client.OrderApi(self.api_client)
        self.order_api_v3   = upstox_client.OrderApiV3(self.api_client)
        self.quote_api_v3   = upstox_client.MarketQuoteV3Api(self.api_client)
        self.portfolio_api  = upstox_client.PortfolioApi(self.api_client)
        self.charge_api     = upstox_client.ChargeApi(self.api_client)
        self.pnl_api        = upstox_client.TradeProfitAndLossApi(self.api_client)
        self.market_api     = upstox_client.MarketHolidaysAndTimingsApi(self.api_client)
        self.logger.info("✅ _reinit_apis complete — all 11 API clients re-initialized")

    def _check_rate_limit(self, max_requests: int = 50, window_seconds: int = 1):
        with self._rate_limit_condition:
            now = time.time()
            self._request_timestamps = [t for t in self._request_timestamps 
                                      if now - t < window_seconds]
            
            while len(self._request_timestamps) >= max_requests:
                sleep_time = self._request_timestamps[0] + window_seconds - now
                if sleep_time > 0:
                    self.logger.warning(f"Rate limit reached, waiting {sleep_time:.2f}s")
                    self._rate_limit_condition.wait(timeout=sleep_time)
                    now = time.time()
                    self._request_timestamps = [t for t in self._request_timestamps 
                                              if now - t < window_seconds]
            
            self._request_timestamps.append(now)
            self._rate_limit_condition.notify_all()
    
    def _sdk_call_with_retry(self, fn: Callable, *args, retries: int = 3,
                              base_delay: float = 1.0, label: str = "", **kwargs):
        """
        Execute an Upstox SDK call with exponential backoff retry.
        Retries on any exception (network timeout, 5xx, transient errors).
        Does NOT retry on 4xx client errors (bad request, auth failure).
        
        Usage:
            response = self._sdk_call_with_retry(
                self.order_api.get_order_details,
                api_version="2.0", order_id=oid,
                label="get_order_details"
            )
        """
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                self._check_rate_limit()
                return fn(*args, **kwargs)
            except Exception as e:
                last_exc = e
                err_str = str(e)
                # Don't retry client errors — they won't recover with a retry
                if any(code in err_str for code in ('400', '401', '403', '404')):
                    self.logger.error(f"SDK call {label} — client error (no retry): {e}")
                    raise
                delay = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s
                self.logger.warning(
                    f"SDK call {label} attempt {attempt}/{retries} failed: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
        self.logger.error(f"SDK call {label} failed after {retries} attempts: {last_exc}")
        raise last_exc

    def get_market_status(self) -> Dict:
        return self.smart_fetcher.get_market_status()
    
    def get_ltp_with_fallback(self, instrument_key: str) -> Optional[float]:
        return self.smart_fetcher.get_ltp(instrument_key)
    
    def get_bulk_ltp_with_fallback(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        return self.smart_fetcher.get_bulk_ltp(instrument_keys)
    
    def get_ohlc_with_fallback(self, instrument_key: str, interval: str = "1d") -> Optional[Dict]:
        return self.smart_fetcher.get_ohlc(instrument_key, interval)
    
    def get_full_quote_with_fallback(self, instrument_key: str) -> Optional[Dict]:
        return self.smart_fetcher.get_full_quote(instrument_key)
    
    def get_lot_size_for_expiry(self, expiry_date: date) -> int:
        today = date.today()
        
        if self._lot_size_cache_date != today:
            self._lot_size_cache = {}
            self._lot_size_cache_date = today
        
        expiry_str = expiry_date.strftime("%Y-%m-%d")
        
        if expiry_str in self._lot_size_cache:
            return self._lot_size_cache[expiry_str]
        
        try:
            self._check_rate_limit()
            response = self.options_api.get_option_contracts(
                instrument_key=SystemConfig.NIFTY_KEY
            )
            
            if response.status == "success" and response.data:
                for contract in response.data:
                    if hasattr(contract, 'expiry') and contract.expiry:
                        if isinstance(contract.expiry, str):
                            contract_expiry = datetime.strptime(contract.expiry, "%Y-%m-%d").date()
                        elif isinstance(contract.expiry, datetime):
                            contract_expiry = contract.expiry.date()
                        else:
                            continue
                            
                        if contract_expiry == expiry_date:
                            if hasattr(contract, 'lot_size'):
                                lot_size = contract.lot_size
                                self._lot_size_cache[expiry_str] = lot_size
                                self.logger.info(f"Lot size for {expiry_str}: {lot_size}")
                                return lot_size
            error_msg = f"API succeeded but no lot size found for expiry {expiry_str}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)
            
        except Exception as e:
            error_msg = f"Failed to fetch lot size for {expiry_str}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)
    
    def start_market_streamer(self, instrument_keys: List[str], mode: str = "ltpc"):
        self.market_streamer.connect(instrument_keys, mode)
        return self.market_streamer
    
    def start_portfolio_streamer(self, 
                                order_update: bool = True,
                                position_update: bool = True,
                                holding_update: bool = True,
                                gtt_update: bool = True):
        self.portfolio_streamer.connect(
            order_update=order_update,
            position_update=position_update,
            holding_update=holding_update,
            gtt_update=gtt_update
        )
        return self.portfolio_streamer
    
    def subscribe_market_data(self, instrument_keys: List[str], mode: str = "ltpc"):
        return self.market_streamer.subscribe(instrument_keys, mode)
    
    def unsubscribe_market_data(self, instrument_keys: List[str]):
        return self.market_streamer.unsubscribe(instrument_keys)
    
    def get_ltp(self, instrument_key: str) -> Optional[float]:
        if not self.market_streamer.is_connected:
            return None
        return self.market_streamer.get_ltp(instrument_key)
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        if not self.market_streamer.is_connected:
            return {}
        return self.market_streamer.get_bulk_ltp(instrument_keys)
    
    def get_live_positions(self) -> Optional[List[Dict]]:
        try:
            self._check_rate_limit()
            response = self.portfolio_api.get_positions(api_version="2.0")
            
            if response.status == "success" and response.data:
                positions = []
                for pos in response.data:
                    positions.append({
                        "instrument_token": pos.instrument_token,
                        "quantity": pos.quantity,
                        "buy_price": pos.average_price,
                        "current_price": pos.last_price,
                        "pnl": pos.pnl,
                        "unrealised": pos.unrealised if hasattr(pos, 'unrealised') else None,
                        "product": pos.product,
                        "symbol": pos.trading_symbol if hasattr(pos, 'trading_symbol') else pos.instrument_token.split('|')[-1]
                    })
                return positions
            
        except Exception as e:
            self.logger.error(f"Portfolio fetch error: {e}")
        
        return []
    
    def reconcile_positions_with_db(self, db: Session) -> Dict:
        try:
            db_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            db_instruments = set()
            db_quantities = {}
            
            for trade in db_trades:
                legs = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
                for leg in legs:
                    instrument = leg['instrument_token']
                    db_instruments.add(instrument)
                    qty = leg['quantity']
                    if leg['action'] == 'SELL':
                        qty = -qty
                    db_quantities[instrument] = db_quantities.get(instrument, 0) + qty
            
            broker_positions = self.get_live_positions()
            broker_instruments = {p['instrument_token']: p['quantity'] for p in broker_positions}
            
            discrepancies = []
            for instrument in set(db_instruments) | set(broker_instruments.keys()):
                db_qty = db_quantities.get(instrument, 0)
                broker_qty = broker_instruments.get(instrument, 0)
                if db_qty != broker_qty:
                    discrepancies.append({
                        "instrument": instrument,
                        "db_qty": db_qty,
                        "broker_qty": broker_qty,
                        "diff": db_qty - broker_qty
                    })
            
            reconciled = len(discrepancies) == 0
            
            return {
                "timestamp": datetime.now().isoformat(),
                "db_positions": len(db_instruments),
                "broker_positions": len(broker_instruments),
                "matched": len(set(db_instruments).intersection(set(broker_instruments.keys()))),
                "discrepancies": discrepancies,
                "reconciled": reconciled
            }
            
        except Exception as e:
            self.logger.error(f"Position reconciliation error: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "reconciled": False
            }
    
    def validate_margin_for_strategy(self, legs: List[OptionLeg]) -> Tuple[bool, float, float]:
        try:
            self._check_rate_limit()
            
            instruments = []
            for leg in legs:
                instruments.append(upstox_client.Instrument(
                    instrument_key=leg.instrument_token,
                    quantity=leg.quantity,
                    transaction_type="SELL" if leg.action == "SELL" else "BUY",
                    product=leg.product
                ))
            
            body = upstox_client.MarginRequest(instruments=instruments)
            response = self.charge_api.post_margin(body)
            
            if response.status == "success" and response.data:
                final_margin = float(response.data.final_margin)
                # Add a 5% buffer to guard against race-condition where another position
                # consumes capital between the margin-calculation call and the get_funds call.
                required_with_buffer = final_margin * 1.05
                available_margin = self.get_funds() or 0.0
                
                has_sufficient = available_margin >= required_with_buffer
                
                margin_details = ""
                if hasattr(response.data, 'margins') and response.data.margins:
                    m = response.data.margins[0]
                    span = getattr(m, 'span_margin', 0)
                    exp = getattr(m, 'exposure_margin', 0)
                    req = getattr(response.data, 'required_margin', final_margin)
                    margin_details = f" [Span: ₹{span:,.2f}, Exposure: ₹{exp:,.2f}, Pre-hedge: ₹{req:,.2f}]"
                
                self.logger.info(
                    f"Margin Check: Final=₹{final_margin:,.2f}{margin_details}, "
                    f"Available=₹{available_margin:,.2f}, "
                    f"Sufficient={has_sufficient}"
                )
                
                return has_sufficient, final_margin, available_margin
            
        except Exception as e:
            self.logger.error(f"Margin validation error: {e}")
        
        return False, 0.0, 0.0
    
    def get_broker_pnl_for_date(self, target_date: date) -> Optional[float]:
        try:
            self._check_rate_limit()
            
            date_str = target_date.strftime("%Y-%m-%d")
            segment = "FO"
            
            if target_date.month >= 4:
                fy = f"{str(target_date.year)[2:]}{str(target_date.year + 1)[2:]}"
            else:
                fy = f"{str(target_date.year - 1)[2:]}{str(target_date.year)[2:]}"
            
            response = self._sdk_call_with_retry(
                self.pnl_api.get_trade_wise_profit_and_loss_data,
                segment=segment,
                financial_year=fy,
                page_number=1,
                page_size=100,
                from_date=date_str,
                to_date=date_str,
                api_version="2.0",
                label="get_trade_wise_pnl"
            )
            
            if response.status == "success" and response.data:
                # SDK get_trade_wise_profit_and_loss_data returns buy_amount/sell_amount
                # Realized P&L = sell_amount - buy_amount per trade record
                total_pnl = sum([
                    (getattr(trade, 'sell_amount', 0.0) or 0.0) - (getattr(trade, 'buy_amount', 0.0) or 0.0)
                    for trade in response.data
                ])
                self.logger.info(f"Broker P&L for {date_str}: ₹{total_pnl:,.2f}")
                return total_pnl
            
        except Exception as e:
            self.logger.error(f"Broker P&L fetch error: {e}")
        
        return None
    
    def is_trading_day(self) -> bool:
        try:
            self._check_rate_limit()
            response = self.market_api.get_market_status(exchange="NSE")
            
            if response.status == "success" and response.data:
                status = getattr(response.data, 'status', '')
                return "open" in status.lower()
            
        except Exception as e:
            self.logger.error(f"Trading day check error: {e}")
        
        return True
    
    def get_market_status_detailed(self) -> str:
        try:
            self._check_rate_limit()
            response = self.market_api.get_market_status(exchange="NSE")
            
            if response.status == "success" and response.data:
                return getattr(response.data, 'status', "UNKNOWN")
            
        except Exception as e:
            self.logger.error(f"Market status error: {e}")
        
        return "UNKNOWN"
    
    def is_market_open_now(self) -> bool:
        status = self.get_market_status_detailed()
        return status.lower() == "normal_open"
    
    def get_market_holidays(self, days_ahead: int = 30) -> List[date]:
        """
        Fetch NSE trading holidays using the correct Upstox SDK v2 API.
        Correct method: get_holidays() — no exchange param.
        Correct method for single date: get_holiday(date_str).
        Filters for days where NSE is in closed_exchanges (TRADING_HOLIDAY).
        """
        try:
            self._check_rate_limit()
            # SDK v2: get_holidays() returns all holidays for current year
            response = self.market_api.get_holidays()

            if response and response.status == "success" and response.data:
                holidays = []
                today = date.today()
                cutoff = today + timedelta(days=days_ahead)

                for holiday in response.data:
                    # Only care about days NSE is fully closed for trading
                    if not hasattr(holiday, 'date'):
                        continue
                    holiday_type = getattr(holiday, 'holiday_type', '') or ''
                    if holiday_type not in ('TRADING_HOLIDAY', 'SETTLEMENT_HOLIDAY'):
                        continue
                    closed = getattr(holiday, 'closed_exchanges', []) or []
                    # closed_exchanges is a list of exchange name strings
                    nse_closed = any(
                        (e if isinstance(e, str) else getattr(e, 'exchange', '')) == 'NSE'
                        for e in closed
                    )
                    if not nse_closed:
                        continue
                    try:
                        holiday_date = datetime.strptime(holiday.date, "%Y-%m-%d").date()
                    except (ValueError, AttributeError):
                        continue
                    if today <= holiday_date <= cutoff:
                        holidays.append(holiday_date)

                return sorted(holidays)

        except Exception as e:
            self.logger.error(f"Market holidays fetch error: {e}")

        return []
    
    def emergency_exit_all_positions(self) -> Dict:
        try:
            self._check_rate_limit()
            
            response = self.order_api.exit_positions(api_version="2.0")
            
            if response.status == "success":
                order_ids = getattr(response.data, 'order_ids', []) if hasattr(response, 'data') else []
                self.logger.info("Emergency exit all positions successful")
                return {
                    "success": True,
                    "message": "Emergency exit orders placed successfully via Upstox API",
                    "orders_placed": len(order_ids),
                    "order_ids": order_ids
                }
            else:
                return {
                    "success": False,
                    "message": f"Emergency exit failed: {response}",
                    "orders_placed": 0
                }
                
        except Exception as e:
            self.logger.error(f"Emergency exit failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "orders_placed": 0
            }
    
    def get_funds(self) -> Optional[float]:
        try:
            self._check_rate_limit()
            response = self._sdk_call_with_retry(self.user_api.get_user_fund_margin, api_version="2.0", label="get_user_fund_margin")
            if response.status == "success" and response.data:
                return float(response.data.equity.available_margin)
        except Exception as e:
            self.logger.error(f"Fund fetch error: {e}")
        return None
    
    def get_order_status(self, order_id: str) -> Optional[str]:
        try:
            self._check_rate_limit()
            response = self._sdk_call_with_retry(self.order_api.get_order_details, api_version="2.0", order_id=order_id, label="get_order_details_status")
            if response.status == "success" and response.data and len(response.data) > 0:
                return response.data[0].status
        except Exception as e:
            self.logger.error(f"Order status fetch error: {e}")
        return None
    
    def get_order_details(self, order_id: str) -> Optional[Dict]:
        try:
            self._check_rate_limit()
            response = self.order_api.get_order_details(api_version="2.0", order_id=order_id)
            if response.status == "success" and response.data and len(response.data) > 0:
                data = response.data[0]
                return {
                    "order_id": data.order_id,
                    "status": data.status,
                    "filled_quantity": data.filled_quantity,
                    "average_price": data.average_price,
                    "order_timestamp": data.order_timestamp,
                    "exchange_timestamp": data.exchange_timestamp,
                    "instrument_token": data.instrument_token,
                    "quantity": data.quantity,
                    "price": data.price,
                    "transaction_type": data.transaction_type,
                    "pending_quantity": data.pending_quantity,
                    "status_message_raw": getattr(data, 'status_message_raw', None),
                    "exchange_order_id": getattr(data, 'exchange_order_id', None),
                    "order_ref_id": getattr(data, 'order_ref_id', None),
                    "variety": getattr(data, 'variety', None)
                }
        except Exception as e:
            self.logger.error(f"Order details fetch error: {e}")
        return None
    
    def history(self, key: str, days: int = 400) -> Optional[pd.DataFrame]:
        try:
            self._check_rate_limit()
            to_date = date.today().strftime("%Y-%m-%d")
            from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            response = self.history_api.get_historical_candle_data1(
                key,
                "days",
                "1",
                to_date,
                from_date
            )
            
            if response.status == "success" and response.data and response.data.candles:
                candles = response.data.candles
                df = pd.DataFrame(
                    candles, 
                    columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
                )
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                return df.astype(float).sort_index()
            
        except Exception as e:
            self.logger.error(f"History fetch error: {e}")
        
        return None
    
    def get_expiries(self) -> Tuple[Optional[date], Optional[date], Optional[date], int, List[date]]:
        try:
            self._check_rate_limit()
            response = self.options_api.get_option_contracts(
                instrument_key=SystemConfig.NIFTY_KEY
            )
            
            if response.status == "success" and response.data:
                data = response.data
                
                lot_size = 50
                if data and len(data) > 0:
                    lot_size = data[0].lot_size if hasattr(data[0], 'lot_size') else 50
                
                expiry_dates = []
                weekly_expiries = []
                monthly_expiries = []
                
                for contract in data:
                    if hasattr(contract, 'expiry') and contract.expiry:
                        if isinstance(contract.expiry, str):
                            expiry_date = datetime.strptime(contract.expiry, "%Y-%m-%d").date()
                        elif isinstance(contract.expiry, datetime):
                            expiry_date = contract.expiry.date()
                        else:
                            continue
                        
                        expiry_dates.append(expiry_date)
                        
                        is_weekly = getattr(contract, 'weekly', False)
                        if is_weekly:
                            weekly_expiries.append(expiry_date)
                        else:
                            monthly_expiries.append(expiry_date)
                
                expiry_dates = sorted(list(set(expiry_dates)))
                weekly_expiries = sorted(list(set(weekly_expiries)))
                monthly_expiries = sorted(list(set(monthly_expiries)))
                
                valid_dates = [d for d in expiry_dates if d >= date.today()]
                if not valid_dates:
                    return None, None, None, lot_size, []
                
                weekly = weekly_expiries[0] if weekly_expiries else valid_dates[0]
                monthly = monthly_expiries[0] if monthly_expiries else valid_dates[-1]
                
                if len(weekly_expiries) > 1:
                    next_weekly = weekly_expiries[1]
                else:
                    next_weekly = monthly_expiries[0] if len(monthly_expiries) > 0 else weekly
                
                return weekly, monthly, next_weekly, lot_size, expiry_dates
                
        except Exception as e:
            self.logger.error(f"Expiries fetch error: {e}")
        
        return None, None, None, 50, []
    
    def chain(self, expiry_date: date) -> Optional[pd.DataFrame]:
        try:
            self._check_rate_limit()
            expiry_str = expiry_date.strftime("%Y-%m-%d")
            
            response = self.options_api.get_put_call_option_chain(
                instrument_key=SystemConfig.NIFTY_KEY,
                expiry_date=expiry_str
            )
            
            if response.status == "success" and response.data:
                rows = []
                for item in response.data:
                    try:
                        call_opts = item.call_options
                        put_opts = item.put_options
                        
                        def get_val(obj, attr, sub_attr=None):
                            if not obj: return 0
                            if sub_attr and hasattr(obj, attr):
                                parent = getattr(obj, attr)
                                return getattr(parent, sub_attr, 0) if parent else 0
                            return getattr(obj, attr, 0)
                        
                        call_pop = 0.0
                        put_pop = 0.0
                        
                        if call_opts and hasattr(call_opts, 'option_greeks'):
                            call_pop = getattr(call_opts.option_greeks, 'pop', 0) or 0
                        
                        if put_opts and hasattr(put_opts, 'option_greeks'):
                            put_pop = getattr(put_opts.option_greeks, 'pop', 0) or 0
                        
                        call_market = getattr(call_opts, 'market_data', None) if call_opts else None
                        put_market = getattr(put_opts, 'market_data', None) if put_opts else None
                        
                        rows.append({
                            'strike': item.strike_price,
                            'ce_instrument_key': get_val(call_opts, 'instrument_key'),
                            'ce_ltp': get_val(call_market, 'ltp') if call_market else 0,
                            'ce_bid': get_val(call_market, 'bid_price') if call_market else 0,
                            'ce_ask': get_val(call_market, 'ask_price') if call_market else 0,
                            'ce_oi': get_val(call_market, 'oi') if call_market else 0,
                            'ce_iv': get_val(call_opts, 'option_greeks', 'iv'),
                            'ce_delta': get_val(call_opts, 'option_greeks', 'delta'),
                            'ce_gamma': get_val(call_opts, 'option_greeks', 'gamma'),
                            'ce_theta': get_val(call_opts, 'option_greeks', 'theta'),
                            'ce_vega': get_val(call_opts, 'option_greeks', 'vega'),
                            'ce_pop': call_pop,
                            'pe_instrument_key': get_val(put_opts, 'instrument_key'),
                            'pe_ltp': get_val(put_market, 'ltp') if put_market else 0,
                            'pe_bid': get_val(put_market, 'bid_price') if put_market else 0,
                            'pe_ask': get_val(put_market, 'ask_price') if put_market else 0,
                            'pe_oi': get_val(put_market, 'oi') if put_market else 0,
                            'pe_iv': get_val(put_opts, 'option_greeks', 'iv'),
                            'pe_delta': get_val(put_opts, 'option_greeks', 'delta'),
                            'pe_gamma': get_val(put_opts, 'option_greeks', 'gamma'),
                            'pe_theta': get_val(put_opts, 'option_greeks', 'theta'),
                            'pe_vega': get_val(put_opts, 'option_greeks', 'vega'),
                            'pe_pop': put_pop,
                        })
                    except Exception as e:
                        self.logger.error(f"Chain row error: {e}")
                        continue
                
                if rows:
                    return pd.DataFrame(rows)
                
        except Exception as e:
            self.logger.error(f"Chain fetch error: {e}")
        
        return None
    
    def get_greeks(self, instrument_keys: List[str]) -> Dict[str, Dict]:
        try:
            self._check_rate_limit()
            response = self.quote_api_v3.get_market_quote_option_greek(
                instrument_key=",".join(instrument_keys)
            )
            
            result = {}
            if response.status == "success" and response.data:
                for api_key, data in response.data.items():
                    actual_token = getattr(data, 'instrument_token', api_key)
                    
                    greeks_data = getattr(data, 'option_greeks', None) if hasattr(data, 'option_greeks') else None
                    
                    if greeks_data:
                        result[actual_token] = {
                            'iv': getattr(greeks_data, 'iv', 0) or 0,
                            'delta': getattr(greeks_data, 'delta', 0) or 0,
                            'gamma': getattr(greeks_data, 'gamma', 0) or 0,
                            'theta': getattr(greeks_data, 'theta', 0) or 0,
                            'vega': getattr(greeks_data, 'vega', 0) or 0,
                            'spot_price': 0
                        }
                    else:
                        result[actual_token] = {
                            'iv': 0,
                            'delta': 0,
                            'gamma': 0,
                            'theta': 0,
                            'vega': 0,
                            'spot_price': 0
                        }
            
            for key in result:
                spot = self.get_ltp_with_fallback(key)
                if spot:
                    result[key]['spot_price'] = spot
            
            return result
        
        except Exception as e:
            self.logger.error(f"Greeks fetch error: {e}")
            return {}
    
    def __del__(self):
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)


# ============================================================================
# UPSTOX ORDER EXECUTOR
# ============================================================================

class UpstoxOrderExecutor:
    PRODUCT_MAP = {
        "I": "I",
        "D": "D",
        "MTF": "MTF",
        "CO": "CO"
    }
    
    ORDER_TYPE_MAP = {
        "MARKET": "MARKET",
        "LIMIT": "LIMIT",
        "SL": "SL",
        "SL-M": "SL-M"
    }
    
    VALIDITY_MAP = {
        "DAY": "DAY",
        "IOC": "IOC"
    }
    
    def __init__(self, fetcher: UpstoxFetcher, alert_service=None):
        self.fetcher = fetcher
        self.alert_service = alert_service
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_delay = 1.0
        self.algo_name = "VOLGUARD"
    
    def _wait_for_fills(self, order_ids: List[str], expected_quantities: Dict[str, int] = None) -> Dict[str, Dict]:
        start_time = time.time()
        timeout = DynamicConfig.get("ORDER_FILL_TIMEOUT_SECONDS")
        check_interval = DynamicConfig.get("ORDER_FILL_CHECK_INTERVAL")
        
        filled_orders = {}
        terminal_orders = set()  # REJECTED / CANCELLED — will never fill
        
        while time.time() - start_time < timeout:
            for oid in order_ids:
                if oid in filled_orders or oid in terminal_orders:
                    continue
                
                status = self.fetcher.portfolio_streamer.get_order_status(oid)
                
                if status:
                    mapped = status.get('mapped_status')

                    # Terminal states — stop polling immediately
                    if mapped in ('REJECTED', 'CANCELLED'):
                        terminal_orders.add(oid)
                        self.logger.warning(
                            f"Order {oid} is terminal ({mapped}) — will not fill. "
                            f"Reason: {status.get('status_message_raw', 'N/A')}"
                        )
                        continue

                    if mapped in ['FILLED', 'PARTIAL']:
                        filled_qty = status.get('filled_quantity', 0)
                        
                        if mapped == 'FILLED':
                            filled_orders[oid] = {
                                "filled": True,
                                "partial": False,
                                "filled_quantity": filled_qty,
                                "average_price": status.get('average_price', 0),
                                "status": status
                            }
                            self.logger.info(f"Order {oid} fully filled: {filled_qty} units")
                            
                        elif mapped == 'PARTIAL':
                            filled_orders[oid] = {
                                "filled": False,
                                "partial": True,
                                "filled_quantity": filled_qty,
                                "average_price": status.get('average_price', 0),
                                "remaining": status.get('pending_quantity', 0),
                                "status": status
                            }
                            self.logger.info(f"Order {oid} partially filled: {filled_qty} units")
            
            # Stop when all orders are either filled or terminal
            pending = set(order_ids) - set(filled_orders.keys()) - terminal_orders
            if not pending:
                break
            if all(o.get('filled', False) for o in filled_orders.values()) and not pending:
                break
                
            time.sleep(check_interval)
        
        unfilled = set(order_ids) - set(filled_orders.keys()) - terminal_orders
        if unfilled:
            self.logger.warning(f"Orders not filled within timeout: {unfilled}")
        if terminal_orders:
            self.logger.error(f"Orders rejected/cancelled — position may be incomplete: {terminal_orders}")
        
        return filled_orders
    
    def _get_fill_prices(self, order_ids: List[str]) -> Dict[str, float]:
        fill_prices = {}
        for order_id in order_ids:
            try:
                order_details = self.fetcher.get_order_details(order_id)
                if order_details and order_details.get('average_price', 0) > 0:
                    fill_prices[order_id] = order_details['average_price']
                else:
                    status = self.fetcher.portfolio_streamer.get_order_status(order_id)
                    if status and status.get('average_price', 0) > 0:
                        fill_prices[order_id] = status['average_price']
            except Exception as e:
                self.logger.error(f"Failed to fetch fill price for order {order_id}: {e}")
        return fill_prices
    
    def _place_gtt_stop_losses(self, strategy: ConstructedStrategy, filled_orders: Dict[str, Dict]) -> List[str]:
        """
        Place two SINGLE GTT orders per SELL leg:
          1. Stop-loss GTT  — trigger_type=ABOVE, fires BUY when LTP rises above stop price
          2. Target GTT     — trigger_type=BELOW, fires BUY when LTP drops below target price
        Uses SINGLE (not MULTIPLE) GTT because the position is already open.
        """
        gtt_ids = []

        for leg in strategy.legs:
            if leg.action != "SELL":
                continue

            stop_price  = round(leg.entry_price * DynamicConfig.get("GTT_STOP_LOSS_MULTIPLIER"), 2)
            target_price = round(leg.entry_price * DynamicConfig.get("GTT_PROFIT_TARGET_MULTIPLIER"), 2)
            actual_qty  = leg.filled_quantity if leg.filled_quantity > 0 else leg.quantity

            # Stop-loss GTT
            try:
                sl_rule = upstox_client.GttRule(
                    strategy="ENTRY",
                    trigger_type="ABOVE",
                    trigger_price=stop_price
                )
                sl_body = upstox_client.GttPlaceOrderRequest(
                    type="SINGLE",
                    instrument_token=leg.instrument_token,
                    quantity=actual_qty,
                    product="D",
                    transaction_type="BUY",
                    rules=[sl_rule]
                )
                sl_response = self.fetcher._sdk_call_with_retry(
                    self.fetcher.order_api_v3.place_gtt_order,
                    body=sl_body,
                    label="place_gtt_sl"
                )

                if sl_response.status == "success" and sl_response.data:
                    sl_ids = list(getattr(sl_response.data, 'gtt_order_ids', None) or [])
                    gtt_ids.extend(sl_ids)
                    self.logger.info(
                        f"[GTT-SL] Placed for {leg.strike} {leg.option_type} | "
                        f"Trigger ABOVE ₹{stop_price} | Qty: {actual_qty} | GTT IDs: {sl_ids}"
                    )
                else:
                    self.logger.error(f"[GTT-SL] Failed for {leg.strike}: {sl_response}")

            except Exception as e:
                self.logger.error(f"[GTT-SL] Exception for {leg.strike}: {e}")

            # Target GTT
            try:
                tgt_rule = upstox_client.GttRule(
                    strategy="ENTRY",
                    trigger_type="BELOW",
                    trigger_price=target_price
                )
                tgt_body = upstox_client.GttPlaceOrderRequest(
                    type="SINGLE",
                    instrument_token=leg.instrument_token,
                    quantity=actual_qty,
                    product="D",
                    transaction_type="BUY",
                    rules=[tgt_rule]
                )
                tgt_response = self.fetcher._sdk_call_with_retry(
                    self.fetcher.order_api_v3.place_gtt_order,
                    body=tgt_body,
                    label="place_gtt_tgt"
                )

                if tgt_response.status == "success" and tgt_response.data:
                    tgt_ids = list(getattr(tgt_response.data, 'gtt_order_ids', None) or [])
                    gtt_ids.extend(tgt_ids)
                    self.logger.info(
                        f"[GTT-TGT] Placed for {leg.strike} {leg.option_type} | "
                        f"Trigger BELOW ₹{target_price} | Qty: {actual_qty} | GTT IDs: {tgt_ids}"
                    )
                else:
                    self.logger.error(f"[GTT-TGT] Failed for {leg.strike}: {tgt_response}")

            except Exception as e:
                self.logger.error(f"[GTT-TGT] Exception for {leg.strike}: {e}")

        return gtt_ids
    
    def place_multi_order(self, strategy: ConstructedStrategy, retries_left: int = 3) -> Dict:
        has_margin, required, available = self.fetcher.validate_margin_for_strategy(strategy.legs)
        if not has_margin:
            msg = f"INSUFFICIENT MARGIN. Required: ₹{required:,.2f}, Available: ₹{available:,.2f}"
            self.logger.error(msg)
            return {
                "success": False,
                "order_ids": [],
                "message": msg
            }
        
        strategy.required_margin = required
        
        max_risk_per_trade = DynamicConfig.get("BASE_CAPITAL") * (DynamicConfig.get("MAX_POSITION_RISK_PCT") / 100)
        if strategy.max_loss > max_risk_per_trade:
            msg = f"POSITION RISK TOO HIGH. Max loss: ₹{strategy.max_loss:,.2f} > Limit: ₹{max_risk_per_trade:,.2f}"
            self.logger.error(msg)
            return {
                "success": False,
                "order_ids": [],
                "message": msg
            }
        
        buy_legs = [leg for leg in strategy.legs if leg.action == "BUY"]
        sell_legs = [leg for leg in strategy.legs if leg.action == "SELL"]
        ordered_legs = buy_legs + sell_legs
        
        orders = []
        expected_quantities = {}
        correlation_map = {}
        
        for i, leg in enumerate(ordered_legs):
            correlation_id = f"{strategy.strategy_id[-8:]}_leg{i}_{uuid.uuid4().hex[:12]}"
            leg.correlation_id = correlation_id
            correlation_map[correlation_id] = leg
            
            if leg.action == "SELL" and leg.bid > 0:
                limit_price = leg.bid
            elif leg.action == "BUY" and leg.ask > 0:
                limit_price = leg.ask
            else:
                limit_price = leg.entry_price
            
            order = upstox_client.MultiOrderRequest(
                quantity=leg.quantity,
                product="D",
                validity="DAY",
                price=limit_price,
                tag=strategy.strategy_id[:40],
                instrument_token=leg.instrument_token,
                order_type=self.ORDER_TYPE_MAP.get("LIMIT", "LIMIT"),
                transaction_type=leg.action,
                disclosed_quantity=0,
                trigger_price=0.0,
                is_amo=False,
                slice=True,
                correlation_id=correlation_id
            )
            orders.append(order)
            expected_quantities[correlation_id] = leg.quantity
        
        try:
            # place_multi_order has stricter rate limits: 4/sec — pass to rate limiter
            response = self.fetcher._sdk_call_with_retry(
                self.fetcher.order_api.place_multi_order,
                body=orders,
                label="place_multi_order",
                retries=2,        # fewer retries for order placement — idempotency risk
                base_delay=2.0
            )
            
            if response.status in ["success", "partial_success"]:
                order_ids = []
                response_correlation_map = {}
                
                if hasattr(response, 'data') and response.data:
                    for item in response.data:
                        order_ids.append(item.order_id)
                        if hasattr(item, 'correlation_id'):
                            response_correlation_map[item.correlation_id] = item.order_id
                
                errors = []
                if hasattr(response, 'errors') and response.errors:
                    for error in response.errors:
                        errors.append({
                            "correlation_id": getattr(error, 'correlation_id', None),
                            "error_code": getattr(error, 'error_code', 'UNKNOWN'),
                            "message": getattr(error, 'message', 'Unknown error'),
                            "instrument_key": getattr(error, 'instrument_key', None)
                        })
                        self.logger.error(
                            f"Order failed: {getattr(error, 'error_code', 'UNKNOWN')} "
                            f"- {getattr(error, 'message', '')}"
                        )
                
                filled_orders = self._wait_for_fills(order_ids)
                fill_prices = self._get_fill_prices(order_ids)
                
                for correlation_id, leg in correlation_map.items():
                    if correlation_id in response_correlation_map:
                        order_id = response_correlation_map[correlation_id]
                        if order_id in filled_orders:
                            leg.filled_quantity = filled_orders[order_id].get('filled_quantity', 0)
                            leg.order_id = order_id
                            if order_id in fill_prices:
                                leg.fill_price = fill_prices[order_id]
                                leg.fill_time = datetime.now()
                
                instrument_keys = [leg.instrument_token for leg in strategy.legs if leg.filled_quantity > 0]
                greeks_snapshot = self.fetcher.get_greeks(instrument_keys) if instrument_keys else {}
                
                gtt_ids = []
                if filled_orders:
                    gtt_ids = self._place_gtt_stop_losses(strategy, filled_orders)
                
                for correlation_id, leg in correlation_map.items():
                    if correlation_id in response_correlation_map and leg.fill_price:
                        self.fetcher.fill_tracker.record_fill(
                            order_id=leg.order_id,
                            instrument=leg.instrument_token,
                            limit_price=leg.entry_price,
                            fill_price=leg.fill_price,
                            order_time=leg.fill_time or datetime.now(),
                            fill_time=leg.fill_time or datetime.now(),
                            filled_qty=leg.filled_quantity,
                            requested_qty=leg.quantity,
                            partial=(leg.filled_quantity < leg.quantity)
                        )
                
                return {
                    "success": True,
                    "order_ids": order_ids,
                    "filled_orders": filled_orders,
                    "fill_prices": fill_prices,
                    "gtt_order_ids": gtt_ids,
                    "entry_greeks": greeks_snapshot,
                    "errors": errors,
                    "message": f"Strategy executed. Orders: {len(order_ids)}, Fully filled: {len([o for o in filled_orders.values() if o.get('filled')])}, Partially filled: {len([o for o in filled_orders.values() if o.get('partial')])}, GTTs: {len(gtt_ids)}"
                }
            
            return {"success": False, "order_ids": [], "message": f"Failed: {response.status}"}
            
        except Exception as e:
            self.logger.error(f"Multi-order failed: {e}")
            # ── No recursive retry here ───────────────────────────────────────
            # The _sdk_call_with_retry wrapper already handles network-level
            # retries BEFORE any order reaches the exchange.
            # Retrying at this level after a partial or unknown exception risks
            # duplicate orders on already-filled legs — a direct money loss.
            # If we reach here, the exception escaped all SDK-level retries,
            # which means something structural failed (auth, margin, risk check).
            # Return failure and let the caller/monitor decide the next action.
            if self.alert_service:
                _alert_body = (
                    f"place_multi_order failed for {strategy.strategy_id}.\n"
                    f"Error: {e}\nNo retry attempted — check positions manually."
                )
                self.alert_service.send(
                    "Order Placement Failure",
                    _alert_body,
                    AlertPriority.CRITICAL,
                    throttle_key=f"order_fail_{strategy.strategy_id}"
                )
            return {
                "success": False,
                "order_ids": [],
                "message": f"Exception: {str(e)}"
            }
    
    def cancel_gtt_orders(self, gtt_ids: List[str]) -> bool:
        success = True
        for gtt_id in gtt_ids:
            try:
                self.fetcher.order_api_v3.cancel_gtt_order(
                    body=upstox_client.GttCancelOrderRequest(gtt_order_id=gtt_id)
                )
                self.logger.info(f"Cancelled GTT: {gtt_id}")
            except Exception as e:
                self.logger.error(f"Failed to cancel GTT {gtt_id}: {e}")
                success = False
        return success
    
    def exit_position(self, trade: TradeJournal, exit_reason: str, 
                     current_prices: Dict, db: Session) -> Dict:
        # ── Double-close guard ────────────────────────────────────────────────
        # _on_gtt_fill may have already closed this trade moments ago.
        # Re-querying ensures we have the freshest DB state before touching it.
        db.refresh(trade)
        if trade.status not in (TradeStatus.ACTIVE.value, TradeStatus.PENDING_EXIT.value):
            self.logger.warning(
                f"exit_position called on already-closed trade {trade.strategy_id} "
                f"(status={trade.status}). Skipping to prevent double-close."
            )
            return {
                "success": False,
                "skipped": True,
                "message": f"Trade already closed (status={trade.status})",
                "realized_pnl": trade.realized_pnl or 0.0,
                "exit_reason": trade.exit_reason or exit_reason
            }
        # ─────────────────────────────────────────────────────────────────────

        legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
        
        if trade.gtt_order_ids:
            gtt_ids = json.loads(trade.gtt_order_ids)
            self.cancel_gtt_orders(gtt_ids)
        
        orders_placed = []
        exit_success = True
        order_errors = []
        leg_to_order = {}

        # Exit shorts first to remove naked risk, then close wings
        legs_sorted = sorted(legs_data, key=lambda x: 0 if x['action'] == 'SELL' else 1)

        for leg in legs_sorted:
            try:
                qty = leg.get('filled_quantity', leg['quantity'])
                if qty == 0:
                    continue
                    
                transaction_type = "BUY" if leg['action'] == 'SELL' else "SELL"

                # LIMIT at LTP, fallback to MARKET if no price
                ltp = current_prices.get(leg['instrument_token'], 0) if current_prices else 0
                if ltp and ltp > 0:
                    order_type = "LIMIT"
                    limit_price = round(ltp, 1)
                    validity = "DAY"
                else:
                    order_type = "MARKET"
                    limit_price = 0.0
                    validity = "DAY"

                order = upstox_client.PlaceOrderV3Request(
                    quantity=abs(qty),
                    product="D",
                    validity=validity,
                    price=limit_price,
                    tag=f"EXIT_{trade.strategy_id[:15]}",
                    instrument_token=leg['instrument_token'],
                    order_type=order_type,
                    transaction_type=transaction_type,
                    disclosed_quantity=0,
                    trigger_price=0.0,
                    is_amo=False,
                    slice=True
                )
                
                response = self.fetcher._sdk_call_with_retry(
                    self.fetcher.order_api_v3.place_order,
                    order,
                    label="exit_place_order"
                )
                
                if response.status == "success" and response.data:
                    order_id = None
                    if hasattr(response.data, 'order_ids') and response.data.order_ids:
                        order_id = response.data.order_ids[0]
                        orders_placed.extend(response.data.order_ids)
                    elif hasattr(response.data, 'order_id'):
                        order_id = response.data.order_id
                        orders_placed.append(order_id)
                    
                    if order_id:
                        leg_to_order[leg['instrument_token']] = order_id
                    
                    self.logger.info(f"Exit order placed for {leg['instrument_token']} (qty: {qty})")
                else:
                    exit_success = False
                    order_errors.append(f"Exit order failed for {leg['instrument_token']}: {response}")
                    
            except Exception as e:
                exit_success = False
                order_errors.append(f"Exit order exception for {leg['instrument_token']}: {e}")
        
        if not exit_success:
            error_msg = "; ".join(order_errors)
            self.logger.error(f"Exit orders failed for trade {trade.strategy_id}: {error_msg}")
            
            if self.alert_service:
                self.alert_service.send(
                    "Exit Order Failure",
                    f"Trade {trade.strategy_id} exit failed. Errors: {error_msg}\nDB status NOT changed.",
                    AlertPriority.CRITICAL,
                    throttle_key=f"exit_failure_{trade.strategy_id}"
                )
            
            return {
                "success": False,
                "orders_placed": orders_placed,
                "realized_pnl": 0.0,
                "exit_reason": exit_reason,
                "errors": order_errors
            }
        
        exit_order_ids = orders_placed
        filled_exits = self._wait_for_fills(exit_order_ids)
        exit_fill_prices = self._get_fill_prices(exit_order_ids)
        
        realized_pnl = 0.0
        pnl_approximate = False
        fill_prices_map = {}
        
        for leg in legs_data:
            instrument_key = leg['instrument_token']
            qty = leg.get('filled_quantity', leg['quantity'])
            multiplier = -1 if leg['action'] == 'SELL' else 1
            
            leg_fill_price = None
            order_id = leg_to_order.get(instrument_key)
            if order_id and order_id in exit_fill_prices:
                leg_fill_price = exit_fill_prices[order_id]
            
            if leg_fill_price is not None and leg_fill_price > 0:
                exit_price = leg_fill_price
                fill_prices_map[instrument_key] = leg_fill_price
            else:
                exit_price = current_prices.get(instrument_key, leg['entry_price'])
                pnl_approximate = True
                self.logger.warning(f"Missing fill price for {instrument_key} - using LTP for P&L")
            
            leg_pnl = (exit_price - leg['entry_price']) * qty * multiplier
            realized_pnl += leg_pnl
        
        trade.exit_time = datetime.now()
        trade.status = exit_reason
        trade.exit_reason = exit_reason
        trade.realized_pnl = realized_pnl
        trade.pnl_approximate = pnl_approximate
        trade.fill_prices = json.dumps(fill_prices_map) if fill_prices_map else None
        trade.trade_outcome_class = classify_trade_from_obj(trade)
        
        db.commit()
        
        pnl_message = f"P&L=₹{realized_pnl:.2f}"
        if pnl_approximate:
            pnl_message += " (approximate - using LTP, not fill prices)"
        
        self.logger.info(
            f"Trade {trade.strategy_id} closed: {pnl_message}, Reason={exit_reason}"
        )
        
        return {
            "success": True,
            "orders_placed": orders_placed,
            "realized_pnl": realized_pnl,
            "pnl_approximate": pnl_approximate,
            "fill_prices": fill_prices_map,
            "exit_reason": exit_reason
        }


class MockExecutor:
    def __init__(self, fetcher: Optional[UpstoxFetcher] = None):
        self.fetcher = fetcher
        self.logger = logging.getLogger(self.__class__.__name__)
        self.order_counter = 1000
    
    def place_multi_order(self, strategy: ConstructedStrategy) -> Dict:
        order_ids = []
        gtt_ids = []
        
        spot_price = 22000.0
        if self.fetcher:
            real_spot = self.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY)
            if real_spot:
                spot_price = real_spot
        
        for leg in strategy.legs:
            order_id = f"MOCK_{self.order_counter}"
            self.order_counter += 1
            order_ids.append(order_id)
            
            self.logger.info(
                f"MOCK ORDER: {leg.action} {leg.quantity} {leg.option_type} {leg.strike} "
                f"@ ₹{leg.entry_price:.2f} | Order ID: {order_id}"
            )
            
            if leg.action == "SELL":
                gtt_id = f"MOCK_GTT_{self.order_counter}"
                gtt_ids.append(gtt_id)
        
        mock_greeks = {}
        for leg in strategy.legs:
            moneyness = (spot_price - leg.strike) / leg.strike if leg.strike > 0 else 0
            
            if leg.option_type == "CE":
                delta = 0.5 + moneyness * 2
                delta = max(0.01, min(0.99, delta))
                iv = 15.0 + abs(moneyness) * 10
            else:
                delta = -0.5 + moneyness * 2
                delta = max(-0.99, min(-0.01, delta))
                iv = 15.0 + abs(moneyness) * 10
            
            mock_greeks[leg.instrument_token] = {
                'iv': round(iv, 2),
                'delta': round(delta, 2),
                'gamma': 0.05,
                'theta': round(-iv / 10, 2),
                'vega': round(iv * 2, 2),
                'spot_price': round(spot_price, 2)
            }
        
        return {
            "success": True,
            "order_ids": order_ids,
            "filled_orders": {oid: {"filled": True, "filled_quantity": leg.quantity} 
                            for oid, leg in zip(order_ids, strategy.legs)},
            "gtt_order_ids": gtt_ids,
            "entry_greeks": mock_greeks,
            "message": "Mock orders placed successfully"
        }

    def cancel_gtt_orders(self, gtt_ids: List[str]) -> bool:
        """Mock: log and return success — no real GTT to cancel."""
        self.logger.info(f"MOCK CANCEL GTT: {gtt_ids}")
        return True

    def exit_position(self, trade, exit_reason: str, current_prices: Dict, db) -> Dict:
        """Mock: mark trade closed in DB without touching the broker."""
        try:
            legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
            realized_pnl = 0.0
            for leg in legs_data:
                instrument_key = leg['instrument_token']
                current_price = current_prices.get(instrument_key, leg['entry_price'])
                qty = leg.get('filled_quantity', leg['quantity'])
                multiplier = -1 if leg['action'] == 'SELL' else 1
                realized_pnl += (current_price - leg['entry_price']) * qty * multiplier

            trade.exit_time = datetime.now()
            trade.status = exit_reason
            trade.exit_reason = exit_reason
            trade.realized_pnl = round(realized_pnl, 2)
            trade.pnl_approximate = True
            trade.trade_outcome_class = classify_trade_from_obj(trade)
            db.commit()

            self.logger.info(f"MOCK EXIT: {trade.strategy_id} | Reason={exit_reason} | P&L=\u20b9{realized_pnl:.2f}")
            return {
                "success": True,
                "orders_placed": [],
                "realized_pnl": round(realized_pnl, 2),
                "pnl_approximate": True,
                "fill_prices": {},
                "exit_reason": exit_reason,
            }
        except Exception as e:
            self.logger.error(f"MockExecutor.exit_position error: {e}")
            return {"success": False, "orders_placed": [], "realized_pnl": 0.0, "exit_reason": exit_reason}


# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

class AnalyticsEngine:
    def __init__(self):
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)

        # ── GARCH Daily Cache ─────────────────────────────────────────────────
        # GARCH(1,1) is CPU-heavy. Refitting every 15-min cycle produces
        # inconsistent intraday forecasts due to optimizer convergence variance.
        # Strategy: fit once per trading day (first call of the day), cache the
        # forecasts, serve from cache for all subsequent intraday cycles.
        # Cache auto-invalidates at midnight IST (new trading day).
        self._garch_cache: Dict[str, float] = {}          # keys: "garch7", "garch28"
        self._garch_cache_date: Optional[date] = None     # date cache was last filled

    def _get_garch_forecasts(self, returns: pd.Series, rv7: float, rv28: float) -> tuple:
        """
        GARCH(1,1) with Student-t — fitted once per trading day, cached intraday.

        Why cached:
          • GARCH fitting is CPU-heavy (~0.5-2s per call). Running it every 15-min
            analytics cycle wastes CPU and gives slightly different forecasts each
            run due to optimizer convergence variance (especially near local optima).
          • Intraday GARCH parameters don't meaningfully change — the model is
            trained on daily returns, so refitting at 10:15 vs 14:30 IST produces
            near-identical parameters. The only time it matters is when a large
            overnight move (budget, RBI) has arrived in today's new daily candle.
          • Cache invalidates at midnight IST so each trading day gets a fresh fit
            incorporating yesterday's final close.

        Data window: all available daily returns (~280 trading days / 400 calendar days).
        Model: GARCH(1,1), dist=Student-t (correctly models Indian equity fat tails).
        Forecast: terminal variance at horizon h, not mean variance (avoids underestimation).
        Fallback: RV7 / RV28 if fit fails or insufficient data (<100 observations).
        """
        today_ist = datetime.now(self.ist_tz).date()

        # Serve from cache if same trading day
        if self._garch_cache_date == today_ist and self._garch_cache:
            self.logger.debug("GARCH: serving from daily cache")
            return self._garch_cache.get("garch7", rv7), self._garch_cache.get("garch28", rv28)

        # Fit fresh
        def _fit(horizon: int) -> float:
            try:
                from arch import arch_model
                if len(returns) < 100:
                    return 0.0
                model = arch_model(returns * 100, vol='Garch', p=1, q=1, dist='t')
                res = model.fit(disp='off', show_warning=False)
                forecast = res.forecast(horizon=horizon, reindex=False)
                # Terminal (horizon-th day) variance — not mean across days.
                # Averaging variances then annualizing underestimates longer horizons.
                terminal_variance = forecast.variance.values[-1, -1]
                return round(float(np.sqrt(terminal_variance * 252)), 2)
            except Exception as e:
                self.logger.warning(f"GARCH fit failed (horizon={horizon}): {e}")
                return 0.0

        garch7  = _fit(7)  or rv7
        garch28 = _fit(28) or rv28

        # Store in cache
        self._garch_cache = {"garch7": garch7, "garch28": garch28}
        self._garch_cache_date = today_ist
        self.logger.info(f"GARCH cache refreshed for {today_ist}: 7D={garch7:.2f}% 28D={garch28:.2f}%")

        return garch7, garch28
    
    def get_time_metrics(self, weekly: date, monthly: date, next_weekly: date, 
                        all_expiries: List[date]) -> TimeMetrics:
        today = date.today()
        now_ist = datetime.now(self.ist_tz)
        
        dte_w = (weekly - today).days
        dte_m = (monthly - today).days
        dte_nw = (next_weekly - today).days
        
        is_past_square_off = now_ist.time() >= SystemConfig.PRE_EXPIRY_SQUARE_OFF_TIME
        
        return TimeMetrics(
            current_date=today,
            current_time_ist=now_ist,
            weekly_exp=weekly,
            monthly_exp=monthly,
            next_weekly_exp=next_weekly,
            dte_weekly=dte_w,
            dte_monthly=dte_m,
            dte_next_weekly=dte_nw,
            is_expiry_day_weekly=SystemConfig.is_expiry_day(weekly, all_expiries),
            is_expiry_day_monthly=SystemConfig.is_expiry_day(monthly, all_expiries),
            is_expiry_day_next_weekly=SystemConfig.is_expiry_day(next_weekly, all_expiries),
            is_past_square_off_time=is_past_square_off
        )
    
    def get_vol_metrics(self, nifty_hist: pd.DataFrame, vix_hist: pd.DataFrame, 
                       spot_live: float, vix_live: float) -> VolMetrics:
        is_fallback = False
        spot = spot_live if spot_live > 0 else (nifty_hist.iloc[-1]['close'] if nifty_hist is not None and not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if vix_hist is not None and not vix_hist.empty else 0)
        
        spot = round(spot, 2) if spot else 0
        vix = round(vix, 2) if vix else 0
        
        if spot_live <= 0 or vix_live <= 0:
            is_fallback = True
        
        if nifty_hist is None or nifty_hist.empty:
            return self._fallback_vol_metrics(spot, vix, is_fallback)
        
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        
        rv7 = round(returns.rolling(7).std(ddof=1).iloc[-1] * np.sqrt(252) * 100, 2) if len(returns) >= 7 else 0
        rv28 = round(returns.rolling(28).std(ddof=1).iloc[-1] * np.sqrt(252) * 100, 2) if len(returns) >= 28 else 0
        rv90 = round(returns.rolling(90).std(ddof=1).iloc[-1] * np.sqrt(252) * 100, 2) if len(returns) >= 90 else 0
        
        garch7, garch28 = self._get_garch_forecasts(returns, rv7, rv28)
        
        const = 1.0 / (4.0 * np.log(2.0))
        park7 = round(np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(7).mean() * const) * np.sqrt(252) * 100, 2)
        park28 = round(np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(28).mean() * const) * np.sqrt(252) * 100, 2)
        
        if vix_hist is not None and not vix_hist.empty:
            vix_returns = np.log(vix_hist['close'] / vix_hist['close'].shift(1)).dropna()
            vix_vol_30d = vix_returns.rolling(30).std(ddof=1) * np.sqrt(252) * 100
            vov = round(vix_vol_30d.shift(1).iloc[-1], 2) if len(vix_vol_30d) > 1 else 0
            if len(vix_vol_30d) >= 60:
                vov_mean = vix_vol_30d.shift(1).rolling(60).mean().iloc[-1]
                vov_std = vix_vol_30d.shift(1).rolling(60).std(ddof=1).iloc[-1]
                vov_zscore = round((vov - vov_mean) / vov_std, 2) if vov_std > 0 else 0
            else:
                vov_mean, vov_std, vov_zscore = 0, 0, 0
        else:
            vov, vov_mean, vov_std, vov_zscore = 0, 0, 0, 0
        
        def calc_ivp(window: int) -> float:
            if vix_hist is None:
                return 50.0  # Neutral — no data, don't bias regime engine
            available = len(vix_hist)
            if available < window + 1:
                # Insufficient history: use what we have but warn.
                # Return 50 (neutral percentile) when history is too short to be
                # meaningful — avoids spurious CHEAP regime from a 0.0 fallback.
                if available < 30:
                    self.logger.warning(
                        f"calc_ivp(window={window}): only {available} rows available. "
                        f"Returning 50.0 (neutral) to avoid false CHEAP regime."
                    )
                    return 50.0
                # Enough for a rough estimate — use what we have, flag it
                history = vix_hist['close'].tail(available).iloc[:-1]
                self.logger.debug(
                    f"calc_ivp(window={window}): using {len(history)} rows (requested {window})"
                )
            else:
                # Exclude the current (latest) day — we are ranking today's VIX
                # against the historical window, not against itself.
                history = vix_hist['close'].tail(window + 1).iloc[:-1]
            if len(history) == 0:
                return 50.0
            raw_value = (history < vix).mean() * 100
            return round(raw_value, 1)
        
        ivp_30d = calc_ivp(30)
        ivp_90d = calc_ivp(90)
        ivp_1yr = calc_ivp(252)
        
        ma20 = round(nifty_hist['close'].rolling(20).mean().iloc[-1], 2) if len(nifty_hist) >= 20 else round(spot, 2)
        
        high_low = nifty_hist['high'] - nifty_hist['low']
        high_close = (nifty_hist['high'] - nifty_hist['close'].shift(1)).abs()
        low_close = (nifty_hist['low'] - nifty_hist['close'].shift(1)).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1).dropna()
        atr14 = round(true_range.rolling(14).mean().iloc[-1], 2) if len(true_range) >= 14 else 0
        
        trend_strength = round(abs(spot - ma20) / atr14, 2) if atr14 > 0 else 0
        
        vix_5d_ago = vix_hist['close'].iloc[-6] if vix_hist is not None and len(vix_hist) >= 6 else vix
        vix_change_5d = round(((vix / vix_5d_ago) - 1) * 100, 2) if vix_5d_ago > 0 else 0
        
        if vix_change_5d > DynamicConfig.get("VIX_MOMENTUM_BREAKOUT"):
            vix_momentum = "RISING"
        elif vix_change_5d < -DynamicConfig.get("VIX_MOMENTUM_BREAKOUT"):
            vix_momentum = "FALLING"
        else:
            vix_momentum = "STABLE"
        
        if vov_zscore > DynamicConfig.get("VOV_CRASH_ZSCORE"):
            vol_regime = "EXPLODING"
        elif ivp_1yr > DynamicConfig.get("HIGH_VOL_IVP") and vix_momentum == "FALLING":
            vol_regime = "MEAN_REVERTING"
        elif ivp_1yr > DynamicConfig.get("HIGH_VOL_IVP") and vix_momentum == "RISING":
            vol_regime = "BREAKOUT_RICH"
        elif ivp_1yr > DynamicConfig.get("HIGH_VOL_IVP"):
            vol_regime = "RICH"
        elif ivp_1yr < DynamicConfig.get("LOW_VOL_IVP"):
            vol_regime = "CHEAP"
        else:
            vol_regime = "FAIR"
        
        return VolMetrics(
            spot=spot, vix=vix,
            rv7=rv7, rv28=rv28, rv90=rv90,
            garch7=garch7, garch28=garch28,
            park7=park7, park28=park28,
            vov=vov, vov_zscore=vov_zscore,
            ivp_30d=ivp_30d, ivp_90d=ivp_90d, ivp_1yr=ivp_1yr,
            ma20=ma20, atr14=atr14, trend_strength=trend_strength,
            vol_regime=vol_regime, is_fallback=is_fallback,
            vix_change_5d=vix_change_5d, vix_momentum=vix_momentum
        )
    
    def _fallback_vol_metrics(self, spot: float, vix: float, is_fallback: bool) -> VolMetrics:
        return VolMetrics(
            spot=spot, vix=vix,
            rv7=0, rv28=0, rv90=0,
            garch7=0, garch28=0,
            park7=0, park28=0,
            vov=0, vov_zscore=0,
            ivp_30d=0, ivp_90d=0, ivp_1yr=0,
            ma20=spot, atr14=0, trend_strength=0,
            vol_regime="UNKNOWN", is_fallback=is_fallback,
            vix_change_5d=0, vix_momentum="UNKNOWN"
        )
    
    def get_struct_metrics(self, chain: pd.DataFrame, spot: float, lot_size: int) -> StructMetrics:
        if chain is None or chain.empty or spot == 0:
            return self._fallback_struct_metrics(lot_size)
        
        # Proximity weight: Gaussian kernel centred at spot.
        # Bandwidth 0.005 → ±8% OTM strikes get ~27% weight, ±12% get ~5%.
        # Previous bandwidth 0.02 gave ±16% deep-OTM strikes 24-44% weight,
        # diluting GEX signal with economically irrelevant far strikes.
        chain['proximity_weight'] = np.exp(-((chain['strike'] - spot) / spot) ** 2 / 0.005)
        
        # GEX Calculation — units must be consistent throughout.
        #
        # call_gex / put_gex are in raw INR:
        #   gamma (per-unit) × OI (contracts) × proximity_weight × spot (INR) × lot_size × 0.01
        #   The 0.01 converts "1% spot move" into an absolute INR gamma value per lot.
        #
        # net_gex → raw INR (sum of call and put GEX across all strikes)
        # gex_weighted → displayed figure in crore-scale millions (net_gex / 1_000_000)
        #
        # For the REGIME RATIO we compare net_gex (INR) against total_notional_oi_value (INR)
        # so units cancel cleanly → dimensionless ratio in [0, 1].
        # Previously gex_weighted (millions) was used in the numerator while the denominator
        # remained raw INR, making the ratio 1,000,000× too small and locking the regime
        # permanently to SLIPPERY.

        chain['call_gex'] = chain['ce_gamma'] * chain['ce_oi'] * chain['proximity_weight'] * spot * lot_size * 0.01
        chain['put_gex'] = -chain['pe_gamma'] * chain['pe_oi'] * chain['proximity_weight'] * spot * lot_size * 0.01
        net_gex = (chain['call_gex'] + chain['put_gex']).sum()
        # gex_weighted: human-readable display value in millions of INR (used in dashboard/UI only)
        gex_weighted = round(net_gex / 1_000_000, 2)

        total_oi_sum = (chain['ce_oi'].sum() + chain['pe_oi'].sum())
        # total_notional_oi_value in raw INR — same unit as net_gex
        total_notional_oi_value = total_oi_sum * spot * lot_size

        if total_notional_oi_value > 0:
            # Both numerator (net_gex, raw INR) and denominator (total_notional_oi_value, raw INR)
            # are in identical units → ratio is dimensionless, typically in [0.01, 0.20] range.
            gex_ratio_pct = round((abs(net_gex) / total_notional_oi_value) * 100, 4)
        else:
            gex_ratio_pct = 0

        # GEX_STICKY_RATIO default = 0.03 (3%) — STICKY when GEX/OI > 3%, SLIPPERY when < 1.5%
        sticky_threshold = DynamicConfig.get("GEX_STICKY_RATIO") or 0.03
        if gex_ratio_pct > sticky_threshold:
            gex_regime = "STICKY"
        elif gex_ratio_pct < (sticky_threshold * 0.5):
            gex_regime = "SLIPPERY"
        else:
            gex_regime = "NEUTRAL"
        
        total_ce_oi = chain['ce_oi'].sum()
        total_pe_oi = chain['pe_oi'].sum()
        pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 1.0
        
        atm_chain = chain[(chain['strike'] >= spot * 0.95) & (chain['strike'] <= spot * 1.05)]
        
        if not atm_chain.empty and atm_chain['ce_oi'].sum() > 0:
            atm_ce_oi = atm_chain['ce_oi'].sum()
            atm_pe_oi = atm_chain['pe_oi'].sum()
            pcr_atm = round(atm_pe_oi / atm_ce_oi, 2)
        else:
            pcr_atm = 1.0
        
        strikes = chain['strike'].values
        losses = []
        for s in strikes:
            call_pain = np.sum(np.maximum(0, s - strikes) * chain['ce_oi'].values)
            put_pain = np.sum(np.maximum(0, strikes - s) * chain['pe_oi'].values)
            losses.append(call_pain + put_pain)
        
        max_pain = strikes[np.argmin(losses)] if strikes.size > 0 else spot
        
        try:
            call_25d = chain.iloc[(chain['ce_delta'] - 0.25).abs().argsort()[:1]]
            put_25d = chain.iloc[(chain['pe_delta'] + 0.25).abs().argsort()[:1]]
            skew_25d = round(put_25d.iloc[0]['pe_iv'] - call_25d.iloc[0]['ce_iv'], 2)
        except Exception:
            skew_25d = 0
        
        if skew_25d > 5:
            skew_regime = "CRASH_FEAR"
        elif skew_25d < -2:
            skew_regime = "MELT_UP"
        else:
            skew_regime = "BALANCED"
        
        if pcr_atm > 1.2:
            oi_regime = "BULLISH"
        elif pcr_atm < 0.8:
            oi_regime = "BEARISH"
        else:
            oi_regime = "NEUTRAL"
        
        return StructMetrics(
            net_gex=net_gex,
            gex_ratio=gex_ratio_pct,
            total_oi_value=total_notional_oi_value,
            gex_regime=gex_regime,
            pcr=pcr,
            max_pain=max_pain,
            skew_25d=skew_25d,
            oi_regime=oi_regime,
            lot_size=lot_size,
            pcr_atm=pcr_atm,
            skew_regime=skew_regime,
            gex_weighted=gex_weighted
        )
        
    
    def _fallback_struct_metrics(self, lot_size: int) -> StructMetrics:
        return StructMetrics(
            net_gex=0, gex_ratio=0, total_oi_value=0,
            gex_regime="UNKNOWN", pcr=1.0, max_pain=0,
            skew_25d=0, oi_regime="UNKNOWN", lot_size=lot_size,
            pcr_atm=1.0, skew_regime="UNKNOWN", gex_weighted=0
        )
    
    def get_edge_metrics(self, weekly_chain: pd.DataFrame, monthly_chain: pd.DataFrame,
                        next_weekly_chain: pd.DataFrame, spot: float,
                        vol_metrics: VolMetrics, is_expiry_day: bool,
                        dte_weekly: int = 7, dte_monthly: int = 30,
                        dte_next_weekly: int = 14) -> EdgeMetrics:
        
        def get_iv(chain):
            if chain is None or chain.empty:
                return 0
            idx = (chain['strike'] - spot).abs().argsort()[:1]
            return round((chain.iloc[idx].iloc[0]['ce_iv'] + chain.iloc[idx].iloc[0]['pe_iv']) / 2, 2)
        
        iv_weekly = get_iv(weekly_chain)
        iv_monthly = get_iv(monthly_chain)
        iv_next_weekly = get_iv(next_weekly_chain)
        
        vrp_rv_weekly = round(iv_weekly - vol_metrics.rv7, 2)
        vrp_garch_weekly = round(iv_weekly - vol_metrics.garch7, 2)
        vrp_park_weekly = round(iv_weekly - vol_metrics.park7, 2)
        
        vrp_rv_monthly = round(iv_monthly - vol_metrics.rv28, 2)
        vrp_garch_monthly = round(iv_monthly - vol_metrics.garch28, 2)
        vrp_park_monthly = round(iv_monthly - vol_metrics.park28, 2)
        
        vrp_rv_next_weekly = round(iv_next_weekly - vol_metrics.rv7, 2)
        vrp_garch_next_weekly = round(iv_next_weekly - vol_metrics.garch7, 2)
        vrp_park_next_weekly = round(iv_next_weekly - vol_metrics.park7, 2)
        
        weighted_vrp_weekly = round((vrp_garch_weekly * 0.70 + vrp_park_weekly * 0.15 + vrp_rv_weekly * 0.15), 2)
        weighted_vrp_monthly = round((vrp_garch_monthly * 0.70 + vrp_park_monthly * 0.15 + vrp_rv_monthly * 0.15), 2)
        weighted_vrp_next_weekly = round((vrp_garch_next_weekly * 0.70 + vrp_park_next_weekly * 0.15 + vrp_rv_next_weekly * 0.15), 2)
        
        expiry_risk_discount_weekly = 0.2 if is_expiry_day else 0.0
        expiry_risk_discount_monthly = 0.0
        expiry_risk_discount_next_weekly = 0.0

        # ── Term Structure — DTE-adjusted (forward variance) ─────────────────
        # Comparing raw IVs across expiries is misleading near expiry.
        # A weekly at 6 DTE almost always shows lower IV than monthly at 19 DTE
        # purely due to near-term mean reversion — that is NOT backwardation.
        #
        # Correct method: convert each expiry to daily implied variance first:
        #   daily_var = (IV² × DTE) / 365
        # Then compare. If weekly daily_var > monthly daily_var, the market is
        # pricing MORE uncertainty per day in the near term → true backwardation.
        # term_structure_slope is expressed as annualised vol equivalent (%) for
        # display, so it stays comparable with the old value.
        if iv_weekly > 0 and iv_monthly > 0 and dte_weekly > 0 and dte_monthly > 0:
            daily_var_weekly  = (iv_weekly  ** 2 * dte_weekly)  / 365
            daily_var_monthly = (iv_monthly ** 2 * dte_monthly) / 365
            # Annualised vol equivalent of the difference — used for regime label only
            term_structure_slope = round(
                (np.sqrt(daily_var_monthly * 365) - np.sqrt(daily_var_weekly * 365)), 2
            )
            # Human-readable display: raw IV difference (e.g. 1.2%) — intuitive
            term_spread_display = round(iv_monthly - iv_weekly, 2)
        else:
            term_structure_slope = 0
            term_spread_display  = 0

        if term_structure_slope < -1:
            term_structure_regime = "BACKWARDATION"
        elif term_structure_slope > 1:
            term_structure_regime = "CONTANGO"
        else:
            term_structure_regime = "FLAT"

        return EdgeMetrics(
            iv_weekly=iv_weekly,
            vrp_rv_weekly=vrp_rv_weekly,
            vrp_garch_weekly=vrp_garch_weekly,
            vrp_park_weekly=vrp_park_weekly,
            iv_monthly=iv_monthly,
            vrp_rv_monthly=vrp_rv_monthly,
            vrp_garch_monthly=vrp_garch_monthly,
            vrp_park_monthly=vrp_park_monthly,
            iv_next_weekly=iv_next_weekly,
            vrp_rv_next_weekly=vrp_rv_next_weekly,
            vrp_garch_next_weekly=vrp_garch_next_weekly,
            vrp_park_next_weekly=vrp_park_next_weekly,
            expiry_risk_discount_weekly=expiry_risk_discount_weekly,
            expiry_risk_discount_monthly=expiry_risk_discount_monthly,
            expiry_risk_discount_next_weekly=expiry_risk_discount_next_weekly,
            term_structure_slope=term_structure_slope,
            term_spread_display=term_spread_display,
            term_structure_regime=term_structure_regime,
            weighted_vrp_weekly=weighted_vrp_weekly,
            weighted_vrp_monthly=weighted_vrp_monthly,
            weighted_vrp_next_weekly=weighted_vrp_next_weekly
        )


# ============================================================================
# REGIME ENGINE
# ============================================================================

class RegimeEngine:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_dynamic_weights(self, vol_metrics: VolMetrics, external_metrics: ExternalMetrics, dte: int) -> DynamicWeights:
        vol_weight = 0.40
        struct_weight = 0.30
        edge_weight = 0.30
        rationale = "Base: 40% Vol, 30% Struct, 30% Edge"
        
        if vol_metrics.vov_zscore >= DynamicConfig.get("VOV_WARNING_ZSCORE") or vol_metrics.vix_momentum == "RISING":
            vol_weight += 0.10
            struct_weight -= 0.05
            edge_weight -= 0.05
            rationale = "High Vol Environment: Vol↑ to 50%, Struct/Edge↓"
        elif vol_metrics.ivp_1yr < 25.0:
            edge_weight += 0.10
            vol_weight -= 0.10
            rationale = "Low Vol Environment: Edge↑ to 40%, Vol↓ to 30%"
        
        if dte <= 2:
            struct_weight += 0.10
            vol_weight -= 0.05
            edge_weight -= 0.05
            rationale += " | Low DTE: Struct↑ (Gamma dominant)"
        
        return DynamicWeights(vol_weight, struct_weight, edge_weight, rationale)
    
    def calculate_scores(self, vol_metrics: VolMetrics, struct_metrics: StructMetrics,
                        edge_metrics: EdgeMetrics, external_metrics: ExternalMetrics,
                        expiry_type: str, dte: int) -> RegimeScore:
        
        drivers = []
        
        if expiry_type == "WEEKLY":
            weighted_vrp = edge_metrics.weighted_vrp_weekly
        elif expiry_type == "NEXT_WEEKLY":
            weighted_vrp = edge_metrics.weighted_vrp_next_weekly
        else:
            weighted_vrp = edge_metrics.weighted_vrp_monthly
        
        # ── EDGE SCORE (0-10) ────────────────────────────────────────────────────
        # Three-tier VRP ladder — matches skeleton exactly.
        # Backwardation bonus also restored (was missing from v6).
        edge_score = 5.0
        if weighted_vrp > 4.0:
            edge_score += 3.0
            drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Excellent) +3.0")
        elif weighted_vrp > 2.0:
            edge_score += 2.0
            drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Good) +2.0")
        elif weighted_vrp > 1.0:
            edge_score += 1.0
            drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Moderate) +1.0")
        elif weighted_vrp < 0:
            edge_score -= 3.0
            drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Negative) -3.0")

        # Term structure bonus — steep backwardation signals short-vol edge
        if edge_metrics.term_structure_regime == "BACKWARDATION" and edge_metrics.term_structure_slope < -2.0:
            edge_score += 1.0
            drivers.append(f"Edge: Steep Backwardation ({edge_metrics.term_structure_slope:.1f}%) +1.0")
        elif edge_metrics.term_structure_regime == "CONTANGO":
            edge_score += 0.5
            drivers.append(f"Edge: Contango +0.5")

        edge_score = round(max(0.0, min(10.0, edge_score)), 2)

        # ── VOL SCORE (0-10) ─────────────────────────────────────────────────────
        # VOV crash gates the entire score — if vol-of-vol is exploding, selling
        # vol is dangerous regardless of IVP richness. IVP/VIX branch only runs
        # when VOV is non-threatening (elif chain ensures mutual exclusion).
        #
        # 4-band graduated VoV penalty (thresholds from DynamicConfig):
        #   < 1.5σ                    : +1.5 (stable, bonus)
        #   1.5σ – VOV_WARNING (2.25) : no change
        #   VOV_WARNING (2.25) – VOV_MEDIUM (2.50) : -1.0  WARNING
        #   VOV_MEDIUM  (2.50) – VOV_HEAVY  (2.75) : -2.0  ELEVATED
        #   VOV_HEAVY   (2.75) – VOV_CRASH  (3.00) : -3.5  DANGER
        #   >= VOV_CRASH (3.0)                      : → ZERO  BLOCKED
        vov_z       = vol_metrics.vov_zscore
        vov_crash   = DynamicConfig.get("VOV_CRASH_ZSCORE")    # 3.00
        vov_heavy   = DynamicConfig.get("VOV_HEAVY_ZSCORE")    # 2.75
        vov_medium  = DynamicConfig.get("VOV_MEDIUM_ZSCORE")   # 2.50
        vov_warning = DynamicConfig.get("VOV_WARNING_ZSCORE")  # 2.25

        vol_score = 5.0
        if vov_z >= vov_crash:
            vol_score = 0.0
            drivers.append(f"Vol: VOV Crash ({vov_z:.1f}σ) → ZERO")
        elif vov_z >= vov_heavy:
            vol_score -= 3.5
            drivers.append(f"Vol: VOV Danger ({vov_z:.1f}σ) -3.5")
        elif vov_z >= vov_medium:
            vol_score -= 2.0
            drivers.append(f"Vol: VOV Elevated ({vov_z:.1f}σ) -2.0")
        elif vov_z >= vov_warning:
            vol_score -= 1.0
            drivers.append(f"Vol: VOV Warning ({vov_z:.1f}σ) -1.0")
        elif vov_z < 1.5:
            vol_score += 1.5
            drivers.append(f"Vol: Stable VOV ({vov_z:.1f}σ) +1.5")

        # IVP + VIX direction — rich vol with falling VIX = ideal mean-reversion;
        # rich vol with rising VIX = premium rich but regime expanding (caution).
        if vol_metrics.ivp_1yr > 75:
            if vol_metrics.vix_momentum == "FALLING":
                vol_score += 1.5
                drivers.append(f"Vol: Rich IVP ({vol_metrics.ivp_1yr:.0f}%) + Falling VIX +1.5")
            elif vol_metrics.vix_momentum == "RISING":
                vol_score -= 1.0
                drivers.append(f"Vol: Rich IVP + Rising VIX -1.0")
            else:
                vol_score += 0.5
                drivers.append(f"Vol: Rich IVP ({vol_metrics.ivp_1yr:.0f}%) +0.5")
        elif vol_metrics.ivp_1yr < 25:
            vol_score -= 2.5
            drivers.append(f"Vol: Cheap IVP ({vol_metrics.ivp_1yr:.0f}%) -2.5")
        else:
            vol_score += 1.0
            drivers.append(f"Vol: Fair IVP ({vol_metrics.ivp_1yr:.0f}%) +1.0")

        vol_score = round(max(0.0, min(10.0, vol_score)), 2)

        # ── STRUCT SCORE (0-10) ──────────────────────────────────────────────────
        # PCR ATM: balanced (0.9-1.1) = healthy two-sided market = reward.
        # Extreme in either direction = crowded positioning = penalise.
        # (Previous v6 mistakenly rewarded extreme-bullish PCR as "bullish structure".)
        struct_score = 5.0
        if struct_metrics.gex_regime == "STICKY":
            struct_score += 2.5
            drivers.append(f"Struct: Sticky GEX ({struct_metrics.gex_ratio:.3%}) +2.5")
        elif struct_metrics.gex_regime == "SLIPPERY":
            struct_score -= 1.0
            drivers.append(f"Struct: Slippery GEX -1.0")

        if 0.9 < struct_metrics.pcr_atm < 1.1:
            struct_score += 1.5
            drivers.append(f"Struct: Balanced PCR ATM ({struct_metrics.pcr_atm:.2f}) +1.5")
        elif struct_metrics.pcr_atm > 1.3 or struct_metrics.pcr_atm < 0.7:
            struct_score -= 0.5
            drivers.append(f"Struct: Extreme PCR ATM ({struct_metrics.pcr_atm:.2f}) -0.5")

        if struct_metrics.skew_regime == "CRASH_FEAR":
            struct_score -= 1.0
            drivers.append(f"Struct: Crash Fear Skew ({struct_metrics.skew_25d:+.1f}%) -1.0")
        elif struct_metrics.skew_regime == "MELT_UP":
            struct_score -= 0.5
            drivers.append(f"Struct: Melt-Up Skew -0.5")
        else:
            struct_score += 0.5
            drivers.append(f"Struct: Balanced Skew +0.5")

        struct_score = round(max(0.0, min(10.0, struct_score)), 2)

        # ── EXTERNAL FACTORS → MANDATE VETO ONLY, not composite score ───────────
        # FII data and event risk are informational signals that influence the
        # MANDATE (veto / risk_notes) but must NOT pollute the 0-10 composite.
        # Adding raw -10 from a veto event would push composite negative, making
        # score comparisons meaningless. External factors are handled in
        # generate_mandate() via veto_reasons and risk_notes instead.
        external_score = 0.0  # kept for RegimeScore dataclass compatibility

        weights = self.calculate_dynamic_weights(vol_metrics, external_metrics, dte)

        total_score = round(
            vol_score * weights.vol_weight +
            struct_score * weights.struct_weight +
            edge_score * weights.edge_weight,
            2
        )

        # Score stability across alternative weight schemes — low variance = robust signal
        alt_weights = [(0.30, 0.35, 0.35), (0.50, 0.25, 0.25), (0.35, 0.30, 0.35)]
        alt_scores = [
            vol_score * wv + struct_score * ws + edge_score * we
            for wv, ws, we in alt_weights
        ]
        mean_alt = float(np.mean(alt_scores))
        std_alt = float(np.std(alt_scores))
        if abs(mean_alt) > 0.1:
            score_stability = max(0.0, 1.0 - (std_alt / abs(mean_alt)))
        else:
            score_stability = 0.5  # indeterminate — signal near zero
        
        # Confidence bands: stability-weighted, matching skeleton thresholds.
        # Score stability is computed below but confidence label is derived here.
        if total_score >= 8.0 and score_stability > 0.85:
            overall_signal = "STRONG_SELL"
            confidence = "VERY_HIGH"
        elif total_score >= 6.5 and score_stability > 0.75:
            overall_signal = "SELL"
            confidence = "HIGH"
        elif total_score >= 4.0:
            overall_signal = "CAUTIOUS"
            confidence = "MODERATE"
        else:
            overall_signal = "AVOID"
            confidence = "LOW"
        
        return RegimeScore(
            total_score=total_score,
            vol_score=round(vol_score, 2),
            struct_score=round(struct_score, 2),
            edge_score=round(edge_score, 2),
            external_score=round(external_score, 2),
            vol_signal="SELL_VOL" if vol_score > 6 else "BUY_VOL" if vol_score < 4 else "NEUTRAL",
            struct_signal="FAVORABLE" if struct_score > 6 else "UNFAVORABLE" if struct_score < 4 else "NEUTRAL",
            edge_signal="POSITIVE" if edge_score > 6 else "NEGATIVE" if edge_score < 4 else "NEUTRAL",
            external_signal="CLEAR" if external_score > -1 else "RISKY",
            overall_signal=overall_signal,
            confidence=confidence,
            weights_used=weights,
            weight_rationale=weights.rationale,
            score_stability=round(score_stability, 4),
            score_drivers=drivers
        )
    
    def generate_mandate(self, score: RegimeScore, vol_metrics: VolMetrics,
                        struct_metrics: StructMetrics, edge_metrics: EdgeMetrics,
                        external_metrics: ExternalMetrics, time_metrics: TimeMetrics,
                        expiry_type: str, expiry_date: date, dte: int) -> TradingMandate:
        
        veto_reasons = []
        risk_notes = []
        square_off_instruction = None
        bias = "NEUTRAL"
        
        if struct_metrics.pcr_atm > 1.3:
            bias = "BULLISH"
        elif struct_metrics.pcr_atm < 0.7:
            bias = "BEARISH"
        elif external_metrics.fii_sentiment == "BULLISH":
            bias = "MILDLY_BULLISH"
        elif external_metrics.fii_sentiment == "BEARISH":
            bias = "MILDLY_BEARISH"
        
        if score.total_score >= 6.0:
            if bias == "NEUTRAL":
                suggested_structure = "PROTECTED_STRANGLE"
                wing_protection = "Buy 5% OTM CE+PE wings for protection"
            elif "BULL" in bias:
                # High score + bullish bias → directional spread, not neutral Iron Fly
                # Iron Fly sells ATM straddle; in a bullish market the short call
                # carries excess directional risk that conflicts with the bias.
                suggested_structure = "BULL_PUT_SPREAD"
                wing_protection = "Risk-defined, bullish tilt, high-premium environment"
            elif "BEAR" in bias:
                suggested_structure = "BEAR_CALL_SPREAD"
                wing_protection = "Risk-defined, bearish tilt, high-premium environment"
            else:
                # MILDLY_BULLISH / MILDLY_BEARISH at very high score → Iron Condor
                # with bias-adjusted strike placement (skew toward the trend)
                suggested_structure = "IRON_CONDOR"
                wing_protection = "Bias-skewed condor — tighter wing on trend side"
            regime = "AGGRESSIVE_SHORT"
        elif score.total_score >= 4.0:
            if bias == "NEUTRAL":
                suggested_structure = "IRON_CONDOR"
                wing_protection = "±10% strikes, risk-defined"
            elif "BULL" in bias:
                suggested_structure = "BULL_PUT_SPREAD"
                wing_protection = "Risk-defined spread, bullish tilt"
            elif "BEAR" in bias:
                suggested_structure = "BEAR_CALL_SPREAD"
                wing_protection = "Risk-defined spread, bearish tilt"
            else:
                suggested_structure = "IRON_CONDOR"
                wing_protection = "±10% strikes, risk-defined"
            regime = "DEFENSIVE"
        else:
            suggested_structure = "CASH"
            wing_protection = "N/A"
            regime = "CASH"
        
        if dte == 0:
            is_trade_allowed = False
            veto_reasons.append(f"EXPIRY DAY BLOCKED: {expiry_type}")
        elif dte == 1:
            is_trade_allowed = False
            veto_reasons.append(f"STRICT 1 DTE EXIT RULE")
            square_off_instruction = f"Square off ALL {expiry_type} positions by 2:00 PM IST TODAY"
        else:
            if time_metrics.is_past_square_off_time:
                veto_reasons.append("PAST_SQUARE_OFF_TIME")
            
            if external_metrics.veto_event_near:
                veto_reasons.append("VETO: High Impact Event Tomorrow")
            
            if vol_metrics.vol_regime == "EXPLODING":
                veto_reasons.append("VOL_EXPLODING")
            
            is_trade_allowed = len(veto_reasons) == 0 and score.total_score > 3
        
        if is_trade_allowed:
            if expiry_type == "WEEKLY":
                deployment_pct = DynamicConfig.get("WEEKLY_ALLOCATION_PCT")
            elif expiry_type == "MONTHLY":
                deployment_pct = DynamicConfig.get("MONTHLY_ALLOCATION_PCT")
            else:
                deployment_pct = DynamicConfig.get("NEXT_WEEKLY_ALLOCATION_PCT")
        else:
            deployment_pct = 0.0
        
        deployment_amount = DynamicConfig.get("BASE_CAPITAL") * (deployment_pct / 100)
        
        if external_metrics.high_impact_event_near:
            risk_notes.append("HIGH_IMPACT_EVENT_AHEAD")
        
        if vol_metrics.vix_momentum == "RISING":
            risk_notes.append("VIX_RISING")
        
        if struct_metrics.skew_regime == "CRASH_FEAR":
            risk_notes.append("HIGH_CRASH_FEAR")
        
        regime_summary = f"{regime} - {bias} bias"
        
        return TradingMandate(
            expiry_type=expiry_type,
            expiry_date=expiry_date,
            is_trade_allowed=is_trade_allowed,
            suggested_structure=suggested_structure,
            deployment_amount=deployment_amount,
            risk_notes=risk_notes,
            veto_reasons=veto_reasons,
            regime_summary=regime_summary,
            confidence_level=score.confidence,
            directional_bias=bias,
            regime_name=regime,
            wing_protection=wing_protection,
            square_off_instruction=square_off_instruction
        )


# ============================================================================
# STRATEGY FACTORY
# ============================================================================

class StrategyFactory:
    def __init__(self, fetcher: UpstoxFetcher, spot: float):
        self.fetcher = fetcher
        self.spot = spot
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _get_margin_for_1_lot(self, leg_tokens_and_actions: List[tuple], lot_size: int) -> Optional[float]:
        """
        Calls margin API with all legs at 1-lot quantity (lot_size units each).
        Returns final_margin (after hedge benefit) for the whole basket.
        Falls back to None on any failure — callers must handle None.

        leg_tokens_and_actions: list of (instrument_token, action, product) tuples
        """
        try:
            instruments = [
                upstox_client.Instrument(
                    instrument_key=token,
                    quantity=lot_size,
                    transaction_type=action,
                    product=product
                )
                for token, action, product in leg_tokens_and_actions
            ]
            body = upstox_client.MarginRequest(instruments=instruments)
            response = self.fetcher.charge_api.post_margin(body)
            if response.status == "success" and response.data:
                return float(response.data.final_margin)
        except Exception as e:
            self.logger.warning(f"Margin API for lot sizing failed: {e} — falling back to premium-based sizing")
        return None

    def _validate_strategy(self, legs: List[OptionLeg]) -> List[str]:
        errors = []

        # Guard: premium must be positive for every leg before touching the exchange.
        # A zero or negative premium indicates a stale/broken quote and must block execution.
        for leg in legs:
            ltp = getattr(leg, 'ltp', None) or getattr(leg, 'premium', None) or 0
            if ltp <= 0:
                errors.append(
                    f"{leg.option_type} {leg.strike} has zero/negative LTP ₹{ltp:.2f} — "
                    f"stale or broken quote, cannot execute."
                )

        # Guard: GTT trigger price must be positive (0 would create an immediately-firing GTT).
        for leg in legs:
            gtt_price = getattr(leg, 'gtt_price', None)
            if gtt_price is not None and gtt_price <= 0:
                errors.append(
                    f"{leg.option_type} {leg.strike} GTT price ₹{gtt_price:.2f} is zero/negative — "
                    f"would create a broken GTT order."
                )

        for leg in legs:
            if leg.oi < DynamicConfig.get("MIN_OI"):
                errors.append(
                    f"{leg.option_type} {leg.strike} OI {leg.oi:,.0f} < "
                    f"{DynamicConfig.get('MIN_OI'):,.0f} minimum"
                )
        
        for leg in legs:
            if leg.ask > 0 and leg.bid > 0:
                spread_pct = ((leg.ask - leg.bid) / leg.ltp) * 100 if leg.ltp > 0 else 999
                if spread_pct > DynamicConfig.get("MAX_BID_ASK_SPREAD_PCT"):
                    errors.append(
                        f"{leg.option_type} {leg.strike} spread {spread_pct:.1f}% > "
                        f"{DynamicConfig.get('MAX_BID_ASK_SPREAD_PCT')}%"
                    )
        
        return errors
    
    def construct_iron_fly(self, expiry_date: date, allocation: float, expiry_type: ExpiryType = ExpiryType.WEEKLY) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            atm_strike = min(chain['strike'].values, key=lambda x: abs(x - self.spot))
            atm_row = chain[chain['strike'] == atm_strike].iloc[0]
            
            ce_premium = atm_row['ce_ltp']
            pe_premium = atm_row['pe_ltp']
            straddle_premium = ce_premium + pe_premium
            
            wing_distance = straddle_premium * DynamicConfig.get("IRON_FLY_WING_MULTIPLIER")
            call_wing_strike = atm_strike + wing_distance
            put_wing_strike = atm_strike - wing_distance
            
            call_wing_row = chain.iloc[(chain['strike'] - call_wing_strike).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['strike'] - put_wing_strike).abs().argsort()[:1]]
            
            # Margin-based lot sizing
            margin_1lot = self._get_margin_for_1_lot([
                (atm_row['ce_instrument_key'], "SELL", "D"),
                (atm_row['pe_instrument_key'], "SELL", "D"),
                (call_wing_row.iloc[0]['ce_instrument_key'], "BUY", "D"),
                (put_wing_row.iloc[0]['pe_instrument_key'], "BUY", "D"),
            ], lot_size)
            if not margin_1lot or margin_1lot <= 0:
                self.logger.error(
                    "Margin API call failed for Iron Fly — aborting construction to prevent unsafe sizing. "
                    "Premium-based fallback disabled: it ignores hedge benefit and would undersize position."
                )
                return None
            quantity_lots = int(allocation / margin_1lot)
            if quantity_lots == 0:
                return None

            quantity = quantity_lots * lot_size
            
            call_pop = atm_row.get('ce_pop', 50.0)
            put_pop = atm_row.get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=atm_row['ce_instrument_key'],
                    strike=atm_strike,
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['ce_delta'],
                    gamma=-atm_row['ce_gamma'],
                    vega=-atm_row['ce_vega'],
                    theta=atm_row['ce_theta'],
                    iv=atm_row['ce_iv'],
                    ltp=atm_row['ce_ltp'],
                    bid=atm_row['ce_bid'],
                    ask=atm_row['ce_ask'],
                    oi=atm_row['ce_oi'],
                    lot_size=lot_size,
                    entry_price=atm_row['ce_ltp'],
                    entry_bid=atm_row['ce_bid'],
                    entry_ask=atm_row['ce_ask'],
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=atm_row['pe_instrument_key'],
                    strike=atm_strike,
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['pe_delta'],
                    gamma=-atm_row['pe_gamma'],
                    vega=-atm_row['pe_vega'],
                    theta=atm_row['pe_theta'],
                    iv=atm_row['pe_iv'],
                    ltp=atm_row['pe_ltp'],
                    bid=atm_row['pe_bid'],
                    ask=atm_row['pe_ask'],
                    oi=atm_row['pe_oi'],
                    lot_size=lot_size,
                    entry_price=atm_row['pe_ltp'],
                    entry_bid=atm_row['pe_bid'],
                    entry_ask=atm_row['pe_ask'],
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_wing_row.iloc[0]['ce_instrument_key'],
                    strike=call_wing_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_wing_row.iloc[0]['ce_delta'],
                    gamma=call_wing_row.iloc[0]['ce_gamma'],
                    vega=call_wing_row.iloc[0]['ce_vega'],
                    theta=-call_wing_row.iloc[0]['ce_theta'],
                    iv=call_wing_row.iloc[0]['ce_iv'],
                    ltp=call_wing_row.iloc[0]['ce_ltp'],
                    bid=call_wing_row.iloc[0]['ce_bid'],
                    ask=call_wing_row.iloc[0]['ce_ask'],
                    oi=call_wing_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    entry_bid=call_wing_row.iloc[0]['ce_bid'],
                    entry_ask=call_wing_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_wing_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_wing_row.iloc[0]['pe_instrument_key'],
                    strike=put_wing_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_wing_row.iloc[0]['pe_delta'],
                    gamma=put_wing_row.iloc[0]['pe_gamma'],
                    vega=put_wing_row.iloc[0]['pe_vega'],
                    theta=-put_wing_row.iloc[0]['pe_theta'],
                    iv=put_wing_row.iloc[0]['pe_iv'],
                    ltp=put_wing_row.iloc[0]['pe_ltp'],
                    bid=put_wing_row.iloc[0]['pe_bid'],
                    ask=put_wing_row.iloc[0]['pe_ask'],
                    oi=put_wing_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    entry_bid=put_wing_row.iloc[0]['pe_bid'],
                    entry_ask=put_wing_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            net_premium = (ce_premium + pe_premium - 
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            max_profit = round(net_premium * quantity, 2)
            
            call_wing_spread = call_wing_row.iloc[0]['strike'] - atm_strike
            put_wing_spread = atm_strike - put_wing_row.iloc[0]['strike']
            wing_spread = max(call_wing_spread, put_wing_spread)
            max_loss = round((wing_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"IRON_FLY_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.IRON_FLY,
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Iron Fly: {e}")
            return None
    
    def construct_iron_condor(self, expiry_date: date, allocation: float, expiry_type: ExpiryType = ExpiryType.WEEKLY) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            call_20d_row = chain.iloc[(chain['ce_delta'] - 0.20).abs().argsort()[:1]]
            put_20d_row = chain.iloc[(chain['pe_delta'] + 0.20).abs().argsort()[:1]]
            call_5d_row = chain.iloc[(chain['ce_delta'] - 0.05).abs().argsort()[:1]]
            put_5d_row = chain.iloc[(chain['pe_delta'] + 0.05).abs().argsort()[:1]]
            
            net_premium = (call_20d_row.iloc[0]['ce_ltp'] + put_20d_row.iloc[0]['pe_ltp'] -
                          call_5d_row.iloc[0]['ce_ltp'] - put_5d_row.iloc[0]['pe_ltp'])
            
            # Margin-based lot sizing
            margin_1lot = self._get_margin_for_1_lot([
                (call_20d_row.iloc[0]['ce_instrument_key'], "SELL", "D"),
                (put_20d_row.iloc[0]['pe_instrument_key'], "SELL", "D"),
                (call_5d_row.iloc[0]['ce_instrument_key'], "BUY", "D"),
                (put_5d_row.iloc[0]['pe_instrument_key'], "BUY", "D"),
            ], lot_size)
            if not margin_1lot or margin_1lot <= 0:
                self.logger.error(
                    "Margin API call failed for Iron Condor — aborting construction to prevent unsafe sizing. "
                    "Premium-based fallback disabled: it ignores hedge benefit and would undersize position."
                )
                return None
            quantity_lots = int(allocation / margin_1lot)
            if quantity_lots == 0:
                return None

            quantity = quantity_lots * lot_size
            
            call_pop = call_20d_row.iloc[0].get('ce_pop', 50.0)
            put_pop = put_20d_row.iloc[0].get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=call_20d_row.iloc[0]['ce_instrument_key'],
                    strike=call_20d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-call_20d_row.iloc[0]['ce_delta'],
                    gamma=-call_20d_row.iloc[0]['ce_gamma'],
                    vega=-call_20d_row.iloc[0]['ce_vega'],
                    theta=call_20d_row.iloc[0]['ce_theta'],
                    iv=call_20d_row.iloc[0]['ce_iv'],
                    ltp=call_20d_row.iloc[0]['ce_ltp'],
                    bid=call_20d_row.iloc[0]['ce_bid'],
                    ask=call_20d_row.iloc[0]['ce_ask'],
                    oi=call_20d_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_20d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_20d_row.iloc[0]['ce_bid'],
                    entry_ask=call_20d_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=put_20d_row.iloc[0]['pe_instrument_key'],
                    strike=put_20d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-put_20d_row.iloc[0]['pe_delta'],
                    gamma=-put_20d_row.iloc[0]['pe_gamma'],
                    vega=-put_20d_row.iloc[0]['pe_vega'],
                    theta=put_20d_row.iloc[0]['pe_theta'],
                    iv=put_20d_row.iloc[0]['pe_iv'],
                    ltp=put_20d_row.iloc[0]['pe_ltp'],
                    bid=put_20d_row.iloc[0]['pe_bid'],
                    ask=put_20d_row.iloc[0]['pe_ask'],
                    oi=put_20d_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_20d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_20d_row.iloc[0]['pe_bid'],
                    entry_ask=put_20d_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_5d_row.iloc[0]['ce_instrument_key'],
                    strike=call_5d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_5d_row.iloc[0]['ce_delta'],
                    gamma=call_5d_row.iloc[0]['ce_gamma'],
                    vega=call_5d_row.iloc[0]['ce_vega'],
                    theta=-call_5d_row.iloc[0]['ce_theta'],
                    iv=call_5d_row.iloc[0]['ce_iv'],
                    ltp=call_5d_row.iloc[0]['ce_ltp'],
                    bid=call_5d_row.iloc[0]['ce_bid'],
                    ask=call_5d_row.iloc[0]['ce_ask'],
                    oi=call_5d_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_5d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_5d_row.iloc[0]['ce_bid'],
                    entry_ask=call_5d_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_5d_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_5d_row.iloc[0]['pe_instrument_key'],
                    strike=put_5d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_5d_row.iloc[0]['pe_delta'],
                    gamma=put_5d_row.iloc[0]['pe_gamma'],
                    vega=put_5d_row.iloc[0]['pe_vega'],
                    theta=-put_5d_row.iloc[0]['pe_theta'],
                    iv=put_5d_row.iloc[0]['pe_iv'],
                    ltp=put_5d_row.iloc[0]['pe_ltp'],
                    bid=put_5d_row.iloc[0]['pe_bid'],
                    ask=put_5d_row.iloc[0]['pe_ask'],
                    oi=put_5d_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_5d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_5d_row.iloc[0]['pe_bid'],
                    entry_ask=put_5d_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_5d_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            call_spread = call_5d_row.iloc[0]['strike'] - call_20d_row.iloc[0]['strike']
            max_loss = round((call_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"IRON_CONDOR_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.IRON_CONDOR,
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Iron Condor: {e}")
            return None
    
    def construct_protected_straddle(self, expiry_date: date, allocation: float, expiry_type: ExpiryType = ExpiryType.WEEKLY) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            atm_strike = min(chain['strike'].values, key=lambda x: abs(x - self.spot))
            atm_row = chain[chain['strike'] == atm_strike].iloc[0]
            
            wing_delta = DynamicConfig.get("PROTECTED_STRADDLE_WING_DELTA")
            call_wing_row = chain.iloc[(chain['ce_delta'] - wing_delta).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['pe_delta'] + wing_delta).abs().argsort()[:1]]
            
            net_premium = (atm_row['ce_ltp'] + atm_row['pe_ltp'] -
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            
            # Margin-based lot sizing
            margin_1lot = self._get_margin_for_1_lot([
                (atm_row['ce_instrument_key'], "SELL", "D"),
                (atm_row['pe_instrument_key'], "SELL", "D"),
                (call_wing_row.iloc[0]['ce_instrument_key'], "BUY", "D"),
                (put_wing_row.iloc[0]['pe_instrument_key'], "BUY", "D"),
            ], lot_size)
            if not margin_1lot or margin_1lot <= 0:
                self.logger.error(
                    "Margin API call failed for Protected Straddle — aborting construction to prevent unsafe sizing. "
                    "Premium-based fallback disabled: it ignores hedge benefit and would undersize position."
                )
                return None
            quantity_lots = int(allocation / margin_1lot)
            if quantity_lots == 0:
                return None

            quantity = quantity_lots * lot_size

            call_pop = atm_row.get('ce_pop', 50.0)
            put_pop = atm_row.get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=atm_row['ce_instrument_key'],
                    strike=atm_strike,
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['ce_delta'],
                    gamma=-atm_row['ce_gamma'],
                    vega=-atm_row['ce_vega'],
                    theta=atm_row['ce_theta'],
                    iv=atm_row['ce_iv'],
                    ltp=atm_row['ce_ltp'],
                    bid=atm_row['ce_bid'],
                    ask=atm_row['ce_ask'],
                    oi=atm_row['ce_oi'],
                    lot_size=lot_size,
                    entry_price=atm_row['ce_ltp'],
                    entry_bid=atm_row['ce_bid'],
                    entry_ask=atm_row['ce_ask'],
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=atm_row['pe_instrument_key'],
                    strike=atm_strike,
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['pe_delta'],
                    gamma=-atm_row['pe_gamma'],
                    vega=-atm_row['pe_vega'],
                    theta=atm_row['pe_theta'],
                    iv=atm_row['pe_iv'],
                    ltp=atm_row['pe_ltp'],
                    bid=atm_row['pe_bid'],
                    ask=atm_row['pe_ask'],
                    oi=atm_row['pe_oi'],
                    lot_size=lot_size,
                    entry_price=atm_row['pe_ltp'],
                    entry_bid=atm_row['pe_bid'],
                    entry_ask=atm_row['pe_ask'],
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_wing_row.iloc[0]['ce_instrument_key'],
                    strike=call_wing_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_wing_row.iloc[0]['ce_delta'],
                    gamma=call_wing_row.iloc[0]['ce_gamma'],
                    vega=call_wing_row.iloc[0]['ce_vega'],
                    theta=-call_wing_row.iloc[0]['ce_theta'],
                    iv=call_wing_row.iloc[0]['ce_iv'],
                    ltp=call_wing_row.iloc[0]['ce_ltp'],
                    bid=call_wing_row.iloc[0]['ce_bid'],
                    ask=call_wing_row.iloc[0]['ce_ask'],
                    oi=call_wing_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    entry_bid=call_wing_row.iloc[0]['ce_bid'],
                    entry_ask=call_wing_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_wing_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_wing_row.iloc[0]['pe_instrument_key'],
                    strike=put_wing_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_wing_row.iloc[0]['pe_delta'],
                    gamma=put_wing_row.iloc[0]['pe_gamma'],
                    vega=put_wing_row.iloc[0]['pe_vega'],
                    theta=-put_wing_row.iloc[0]['pe_theta'],
                    iv=put_wing_row.iloc[0]['pe_iv'],
                    ltp=put_wing_row.iloc[0]['pe_ltp'],
                    bid=put_wing_row.iloc[0]['pe_bid'],
                    ask=put_wing_row.iloc[0]['pe_ask'],
                    oi=put_wing_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    entry_bid=put_wing_row.iloc[0]['pe_bid'],
                    entry_ask=put_wing_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            wing_spread = max(
                call_wing_row.iloc[0]['strike'] - atm_strike,
                atm_strike - put_wing_row.iloc[0]['strike']
            )
            max_loss = round((wing_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"PROTECTED_STRADDLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.PROTECTED_STRADDLE,
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Protected Straddle: {e}")
            return None
    
    def construct_protected_strangle(self, expiry_date: date, allocation: float, expiry_type: ExpiryType = ExpiryType.WEEKLY) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            wing_delta = DynamicConfig.get("PROTECTED_STRANGLE_WING_DELTA")
            call_30d_row = chain.iloc[(chain['ce_delta'] - 0.30).abs().argsort()[:1]]
            put_30d_row = chain.iloc[(chain['pe_delta'] + 0.30).abs().argsort()[:1]]
            call_wing_row = chain.iloc[(chain['ce_delta'] - wing_delta).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['pe_delta'] + wing_delta).abs().argsort()[:1]]
            
            net_premium = (call_30d_row.iloc[0]['ce_ltp'] + put_30d_row.iloc[0]['pe_ltp'] -
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            
            # Margin-based lot sizing
            margin_1lot = self._get_margin_for_1_lot([
                (call_30d_row.iloc[0]['ce_instrument_key'], "SELL", "D"),
                (put_30d_row.iloc[0]['pe_instrument_key'], "SELL", "D"),
                (call_wing_row.iloc[0]['ce_instrument_key'], "BUY", "D"),
                (put_wing_row.iloc[0]['pe_instrument_key'], "BUY", "D"),
            ], lot_size)
            if not margin_1lot or margin_1lot <= 0:
                self.logger.error(
                    "Margin API call failed for Protected Strangle — aborting construction to prevent unsafe sizing. "
                    "Premium-based fallback disabled: it ignores hedge benefit and would undersize position."
                )
                return None
            quantity_lots = int(allocation / margin_1lot)
            if quantity_lots == 0:
                return None

            quantity = quantity_lots * lot_size

            call_pop = call_30d_row.iloc[0].get('ce_pop', 50.0)
            put_pop = put_30d_row.iloc[0].get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=call_30d_row.iloc[0]['ce_instrument_key'],
                    strike=call_30d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-call_30d_row.iloc[0]['ce_delta'],
                    gamma=-call_30d_row.iloc[0]['ce_gamma'],
                    vega=-call_30d_row.iloc[0]['ce_vega'],
                    theta=call_30d_row.iloc[0]['ce_theta'],
                    iv=call_30d_row.iloc[0]['ce_iv'],
                    ltp=call_30d_row.iloc[0]['ce_ltp'],
                    bid=call_30d_row.iloc[0]['ce_bid'],
                    ask=call_30d_row.iloc[0]['ce_ask'],
                    oi=call_30d_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_30d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_30d_row.iloc[0]['ce_bid'],
                    entry_ask=call_30d_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=put_30d_row.iloc[0]['pe_instrument_key'],
                    strike=put_30d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-put_30d_row.iloc[0]['pe_delta'],
                    gamma=-put_30d_row.iloc[0]['pe_gamma'],
                    vega=-put_30d_row.iloc[0]['pe_vega'],
                    theta=put_30d_row.iloc[0]['pe_theta'],
                    iv=put_30d_row.iloc[0]['pe_iv'],
                    ltp=put_30d_row.iloc[0]['pe_ltp'],
                    bid=put_30d_row.iloc[0]['pe_bid'],
                    ask=put_30d_row.iloc[0]['pe_ask'],
                    oi=put_30d_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_30d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_30d_row.iloc[0]['pe_bid'],
                    entry_ask=put_30d_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_wing_row.iloc[0]['ce_instrument_key'],
                    strike=call_wing_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_wing_row.iloc[0]['ce_delta'],
                    gamma=call_wing_row.iloc[0]['ce_gamma'],
                    vega=call_wing_row.iloc[0]['ce_vega'],
                    theta=-call_wing_row.iloc[0]['ce_theta'],
                    iv=call_wing_row.iloc[0]['ce_iv'],
                    ltp=call_wing_row.iloc[0]['ce_ltp'],
                    bid=call_wing_row.iloc[0]['ce_bid'],
                    ask=call_wing_row.iloc[0]['ce_ask'],
                    oi=call_wing_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    entry_bid=call_wing_row.iloc[0]['ce_bid'],
                    entry_ask=call_wing_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_wing_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_wing_row.iloc[0]['pe_instrument_key'],
                    strike=put_wing_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_wing_row.iloc[0]['pe_delta'],
                    gamma=put_wing_row.iloc[0]['pe_gamma'],
                    vega=put_wing_row.iloc[0]['pe_vega'],
                    theta=-put_wing_row.iloc[0]['pe_theta'],
                    iv=put_wing_row.iloc[0]['pe_iv'],
                    ltp=put_wing_row.iloc[0]['pe_ltp'],
                    bid=put_wing_row.iloc[0]['pe_bid'],
                    ask=put_wing_row.iloc[0]['pe_ask'],
                    oi=put_wing_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    entry_bid=put_wing_row.iloc[0]['pe_bid'],
                    entry_ask=put_wing_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            call_spread = call_wing_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            put_spread = put_30d_row.iloc[0]['strike'] - put_wing_row.iloc[0]['strike']
            max_spread = max(call_spread, put_spread)
            max_loss = round((max_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"PROTECTED_STRANGLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.PROTECTED_STRANGLE,
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Protected Strangle: {e}")
            return None
    
    def construct_bull_put_spread(self, expiry_date: date, allocation: float, expiry_type: ExpiryType = ExpiryType.WEEKLY) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            put_30d_row = chain.iloc[(chain['pe_delta'] + 0.30).abs().argsort()[:1]]
            put_10d_row = chain.iloc[(chain['pe_delta'] + 0.10).abs().argsort()[:1]]
            
            net_premium = put_30d_row.iloc[0]['pe_ltp'] - put_10d_row.iloc[0]['pe_ltp']
            
            # Margin-based lot sizing
            margin_1lot = self._get_margin_for_1_lot([
                (put_30d_row.iloc[0]['pe_instrument_key'], "SELL", "D"),
                (put_10d_row.iloc[0]['pe_instrument_key'], "BUY", "D"),
            ], lot_size)
            if not margin_1lot or margin_1lot <= 0:
                self.logger.error(
                    "Margin API call failed for Bull Put Spread — aborting construction to prevent unsafe sizing. "
                    "Premium-based fallback disabled: it ignores hedge benefit and would undersize position."
                )
                return None
            quantity_lots = int(allocation / margin_1lot)
            if quantity_lots == 0:
                return None

            quantity = quantity_lots * lot_size

            put_pop = put_30d_row.iloc[0].get('pe_pop', 70.0)
            strategy_pop = put_pop
            
            legs = [
                OptionLeg(
                    instrument_token=put_30d_row.iloc[0]['pe_instrument_key'],
                    strike=put_30d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-put_30d_row.iloc[0]['pe_delta'],
                    gamma=-put_30d_row.iloc[0]['pe_gamma'],
                    vega=-put_30d_row.iloc[0]['pe_vega'],
                    theta=put_30d_row.iloc[0]['pe_theta'],
                    iv=put_30d_row.iloc[0]['pe_iv'],
                    ltp=put_30d_row.iloc[0]['pe_ltp'],
                    bid=put_30d_row.iloc[0]['pe_bid'],
                    ask=put_30d_row.iloc[0]['pe_ask'],
                    oi=put_30d_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_30d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_30d_row.iloc[0]['pe_bid'],
                    entry_ask=put_30d_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=strategy_pop
                ),
                OptionLeg(
                    instrument_token=put_10d_row.iloc[0]['pe_instrument_key'],
                    strike=put_10d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_10d_row.iloc[0]['pe_delta'],
                    gamma=put_10d_row.iloc[0]['pe_gamma'],
                    vega=put_10d_row.iloc[0]['pe_vega'],
                    theta=-put_10d_row.iloc[0]['pe_theta'],
                    iv=put_10d_row.iloc[0]['pe_iv'],
                    ltp=put_10d_row.iloc[0]['pe_ltp'],
                    bid=put_10d_row.iloc[0]['pe_bid'],
                    ask=put_10d_row.iloc[0]['pe_ask'],
                    oi=put_10d_row.iloc[0]['pe_oi'],
                    lot_size=lot_size,
                    entry_price=put_10d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_10d_row.iloc[0]['pe_bid'],
                    entry_ask=put_10d_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_10d_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            put_spread = put_30d_row.iloc[0]['strike'] - put_10d_row.iloc[0]['strike']
            max_loss = round((put_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"BULL_PUT_SPREAD_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.BULL_PUT_SPREAD,
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Bull Put Spread: {e}")
            return None
    
    def construct_bear_call_spread(self, expiry_date: date, allocation: float, expiry_type: ExpiryType = ExpiryType.WEEKLY) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            call_30d_row = chain.iloc[(chain['ce_delta'] - 0.30).abs().argsort()[:1]]
            call_10d_row = chain.iloc[(chain['ce_delta'] - 0.10).abs().argsort()[:1]]
            
            net_premium = call_30d_row.iloc[0]['ce_ltp'] - call_10d_row.iloc[0]['ce_ltp']
            
            # Margin-based lot sizing
            margin_1lot = self._get_margin_for_1_lot([
                (call_30d_row.iloc[0]['ce_instrument_key'], "SELL", "D"),
                (call_10d_row.iloc[0]['ce_instrument_key'], "BUY", "D"),
            ], lot_size)
            if not margin_1lot or margin_1lot <= 0:
                self.logger.error(
                    "Margin API call failed for Bear Call Spread — aborting construction to prevent unsafe sizing. "
                    "Premium-based fallback disabled: it ignores hedge benefit and would undersize position."
                )
                return None
            quantity_lots = int(allocation / margin_1lot)
            if quantity_lots == 0:
                return None

            quantity = quantity_lots * lot_size

            call_pop = call_30d_row.iloc[0].get('ce_pop', 70.0)
            strategy_pop = call_pop
            
            legs = [
                OptionLeg(
                    instrument_token=call_30d_row.iloc[0]['ce_instrument_key'],
                    strike=call_30d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-call_30d_row.iloc[0]['ce_delta'],
                    gamma=-call_30d_row.iloc[0]['ce_gamma'],
                    vega=-call_30d_row.iloc[0]['ce_vega'],
                    theta=call_30d_row.iloc[0]['ce_theta'],
                    iv=call_30d_row.iloc[0]['ce_iv'],
                    ltp=call_30d_row.iloc[0]['ce_ltp'],
                    bid=call_30d_row.iloc[0]['ce_bid'],
                    ask=call_30d_row.iloc[0]['ce_ask'],
                    oi=call_30d_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_30d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_30d_row.iloc[0]['ce_bid'],
                    entry_ask=call_30d_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=strategy_pop
                ),
                OptionLeg(
                    instrument_token=call_10d_row.iloc[0]['ce_instrument_key'],
                    strike=call_10d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_10d_row.iloc[0]['ce_delta'],
                    gamma=call_10d_row.iloc[0]['ce_gamma'],
                    vega=call_10d_row.iloc[0]['ce_vega'],
                    theta=-call_10d_row.iloc[0]['ce_theta'],
                    iv=call_10d_row.iloc[0]['ce_iv'],
                    ltp=call_10d_row.iloc[0]['ce_ltp'],
                    bid=call_10d_row.iloc[0]['ce_bid'],
                    ask=call_10d_row.iloc[0]['ce_ask'],
                    oi=call_10d_row.iloc[0]['ce_oi'],
                    lot_size=lot_size,
                    entry_price=call_10d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_10d_row.iloc[0]['ce_bid'],
                    entry_ask=call_10d_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_10d_row.iloc[0].get('ce_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            call_spread = call_10d_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            max_loss = round((call_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"BEAR_CALL_SPREAD_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.BEAR_CALL_SPREAD,
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Bear Call Spread: {e}")
            return None


# ============================================================================
# ANALYTICS CACHE & SCHEDULER
# ============================================================================

class AnalyticsCache:
    def __init__(self):
        self._cache: Optional[Dict] = None
        self._last_spot: float = 0.0
        self._last_vix: float = 0.0
        self._last_calc_time: Optional[datetime] = None
        self._lock = threading.RLock()
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get(self) -> Optional[Dict]:
        with self._lock:
            if self._cache is None:
                return None
            return copy.deepcopy(self._cache)
    
    def should_recalculate(self, current_spot: float, current_vix: float) -> bool:
        with self._lock:
            if self._cache is None:
                return True
            
            now = datetime.now(self.ist_tz)
            last_time = self._last_calc_time
            
            if last_time is None:
                return True
            
            current_time = now.time()
            is_market_hours = (SystemConfig.MARKET_OPEN_IST <= current_time <= SystemConfig.MARKET_CLOSE_IST)
            
            if is_market_hours:
                interval = DynamicConfig.get("ANALYTICS_INTERVAL_MINUTES")
            else:
                interval = DynamicConfig.get("ANALYTICS_OFFHOURS_INTERVAL_MINUTES")
            
            elapsed_minutes = (now - last_time).total_seconds() / 60
            
            if elapsed_minutes >= interval:
                self.logger.info(f"Time-based recalculation: {elapsed_minutes:.1f}min elapsed")
                return True
            
            if self._last_spot > 0:
                spot_change_pct = abs(current_spot - self._last_spot) / self._last_spot * 100
                if spot_change_pct > DynamicConfig.get("SPOT_CHANGE_TRIGGER_PCT"):
                    self.logger.info(f"Spot-triggered recalculation: {spot_change_pct:.2f}% change")
                    return True
            
            if self._last_vix > 0:
                vix_change_pct = abs(current_vix - self._last_vix) / self._last_vix * 100
                if vix_change_pct > DynamicConfig.get("VIX_CHANGE_TRIGGER_PCT"):
                    self.logger.info(f"VIX-triggered recalculation: {vix_change_pct:.2f}% change")
                    return True
            
            return False
    
    def update(self, analysis_data: Dict, spot: float, vix: float):
        with self._lock:
            self._cache = copy.deepcopy(analysis_data)
            self._last_spot = spot
            self._last_vix = vix
            self._last_calc_time = datetime.now(pytz.UTC)
            self.logger.info(f"Analytics cache updated | Spot: {spot:.2f} | VIX: {vix:.2f}")


class AnalyticsScheduler:
    def __init__(self, volguard_system, cache: AnalyticsCache):
        self.system = volguard_system
        self.cache = cache
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self._running = False
        # Two separate executors prevent heavy analytics from starving the mandate evaluation
        # path. analytics_executor handles run_complete_analysis (CPU-heavy option chain math).
        # mandate_executor handles evaluate_all_mandates (lighter, I/O-bound decision logic).
        self._executor: Optional[ThreadPoolExecutor] = None           # analytics (kept for legacy refs)
        self._analytics_executor: Optional[ThreadPoolExecutor] = None
        self._mandate_executor: Optional[ThreadPoolExecutor] = None
        self._auto_trader = AutoTradingEngine(volguard_system, SessionLocal)
    
    async def start(self):
        self._running = True
        self._analytics_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="analytics")
        self._mandate_executor   = ThreadPoolExecutor(max_workers=2, thread_name_prefix="mandate")
        self._executor = self._analytics_executor  # backwards-compat alias
        self.logger.info("Analytics scheduler started — analytics executor (1 worker) + mandate executor (2 workers)")
        loop = asyncio.get_running_loop()
        
        self.logger.info("Waiting for first price data...")
        for i in range(30):
            current_spot = self.system.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY)
            current_vix = self.system.fetcher.get_ltp_with_fallback(SystemConfig.VIX_KEY)
            
            if current_spot is not None and current_spot > 0 and current_vix is not None and current_vix > 0:
                self.logger.info(f"Price data received after {i+1} seconds - Spot: {current_spot:.2f}, VIX: {current_vix:.2f}")
                break
                
            await asyncio.sleep(1)
        else:
            self.logger.warning("No price data after 30 seconds, continuing anyway...")
        
        while self._running:
            try:
                current_spot = self.system.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY)
                current_vix = self.system.fetcher.get_ltp_with_fallback(SystemConfig.VIX_KEY)
                
                if current_spot is None or current_spot <= 0 or current_vix is None or current_vix <= 0:
                    await asyncio.sleep(2)
                    continue
                
                if not self.system.fetcher.is_trading_day():
                    self.logger.debug("Not a trading day — analytics skipped")
                    await asyncio.sleep(3600)
                    continue

                should_run = self.cache.should_recalculate(current_spot, current_vix)
                
                if should_run:
                    self.logger.info(f"Running analytics - Spot: {current_spot:.2f}, VIX: {current_vix:.2f}")
                    try:
                        analysis = await loop.run_in_executor(
                            self._executor,
                            self.system.run_complete_analysis
                        )
                        self.cache.update(analysis, current_spot, current_vix)
                        
                        if DynamicConfig.get("AUTO_TRADING") or DynamicConfig.get("ENABLE_MOCK_TRADING"):
                            self.logger.info("Auto-trading enabled - evaluating mandates for execution")
                            results = await loop.run_in_executor(
                                self._mandate_executor,
                                self._auto_trader.evaluate_all_mandates,
                                analysis
                            )
                            for mandate_key, success in results.items():
                                if success:
                                    self.logger.info(f"Auto-executed {mandate_key}")
                        
                    except Exception as e:
                        self.logger.error(f"Analytics calculation failed: {e}")
                
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(10)
    
    def stop(self):
        self._running = False
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        self.logger.info("Analytics scheduler stopped")


# ============================================================================
# POSITION MONITOR
# ============================================================================

class PositionMonitor:
    def __init__(self, fetcher: UpstoxFetcher, db_session_factory, analytics_cache: AnalyticsCache, 
                 config, alert_service: Optional[TelegramAlertService] = None, executor=None):
        self.fetcher = fetcher
        self.db_session_factory = db_session_factory
        self.analytics_cache = analytics_cache
        self.config = config
        self.alert_service = alert_service
        # Use the provided executor (live or mock). If none supplied, create a live one.
        self._order_executor = executor if executor is not None else UpstoxOrderExecutor(fetcher)
        self.pnl_engine = PnLAttributionEngine(fetcher)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_running = False
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self._breach_count = 0
        self._breach_threshold = 3
        self._breach_lock = threading.Lock()
        self._subscribed_instruments = set()
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="monitor")
    
    async def start_monitoring(self):
        self.is_running = True
        self.logger.info(f"Position monitoring started ({DynamicConfig.get('MONITOR_INTERVAL_SECONDS')}s intervals - Smart Fallback)")
        
        while self.is_running:
            try:
                await self.check_all_positions()
                await asyncio.sleep(DynamicConfig.get("MONITOR_INTERVAL_SECONDS"))
            except Exception as e:
                self.logger.error(f"Monitor error: {e}")
                await asyncio.sleep(10)
    
    async def check_all_positions(self):
        with self.db_session_factory() as db:
            loop = asyncio.get_running_loop()
            
            try:
                active_trades = db.query(TradeJournal).filter(
                    TradeJournal.status == TradeStatus.ACTIVE.value
                ).all()
                
                if not active_trades:
                    return
                
                all_instruments = []
                for trade in active_trades:
                    legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
                    all_instruments.extend([leg['instrument_token'] for leg in legs_data])
                
                new_instruments = set(all_instruments) - self._subscribed_instruments
                if new_instruments:
                    await loop.run_in_executor(
                        self._executor,
                        self.fetcher.subscribe_market_data,
                        list(new_instruments), "ltpc"
                    )
                    self._subscribed_instruments.update(new_instruments)
                
                current_prices = await loop.run_in_executor(
                    self._executor,
                    self.fetcher.get_bulk_ltp_with_fallback,
                    list(set(all_instruments))
                )
                
                if not current_prices:
                    self.logger.error("PRICE DATA UNAVAILABLE")
                    return
                
                cached_analysis = self.analytics_cache.get()
                
                total_realized_pnl = 0.0
                total_unrealized_pnl = 0.0
                
                for trade in active_trades:
                    result = await self.check_single_position(trade, current_prices, db, loop)
                    if result is not None:
                        total_unrealized_pnl += result
                    else:
                        self.logger.warning(f"Skipping trade {trade.strategy_id} due to missing data")
                
                today = datetime.now().date()
                closed_trades_today = db.query(TradeJournal).filter(
                    TradeJournal.status != TradeStatus.ACTIVE.value,
                    TradeJournal.exit_time >= datetime.combine(today, dt_time.min)
                ).all()
                
                total_realized_pnl = sum(t.realized_pnl or 0 for t in closed_trades_today)
                
                self._update_daily_stats(db, total_realized_pnl, total_unrealized_pnl)
                
                total_mtm = total_realized_pnl + total_unrealized_pnl
                threshold = -DynamicConfig.get("BASE_CAPITAL") * DynamicConfig.get("CIRCUIT_BREAKER_PCT") / 100
                
                with self._breach_lock:
                    if total_mtm < threshold:
                        self._breach_count += 1
                        breach_triggered = self._breach_count >= self._breach_threshold
                    else:
                        self._breach_count = 0
                        breach_triggered = False
                
                if breach_triggered:
                    self.logger.critical(f"CIRCUIT BREAKER TRIGGERED! Total MTM: ₹{total_mtm:.2f} < ₹{threshold:.2f}")
                    await self.trigger_circuit_breaker(db, loop)
            
            except Exception as e:
                self.logger.error(f"Position check error: {e}")
    
    async def check_single_position(self, trade: TradeJournal, current_prices: Dict, db: Session, loop):
        """
        Computes unrealized P&L and triggers scheduled exits.
        Stop-loss and profit-target exits are handled exclusively by GTT orders at the exchange.
        This monitor handles: pre-expiry square-off and veto-event forced exit only.
        Circuit-breaker exits are handled in trigger_circuit_breaker().
        """
        legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data

        for leg in legs_data:
            instrument_key = leg['instrument_token']
            if instrument_key not in current_prices:
                self.logger.error(f"Missing price for {instrument_key} - aborting P&L for {trade.strategy_id}")
                return None
            if current_prices[instrument_key] is None or current_prices[instrument_key] <= 0:
                self.logger.error(f"Invalid price {current_prices[instrument_key]} for {instrument_key}")
                return None

        unrealized_pnl = 0.0
        for leg in legs_data:
            entry_price  = leg['entry_price']
            current_price = current_prices[leg['instrument_token']]
            qty          = leg.get('filled_quantity', leg['quantity'])
            multiplier   = -1 if leg['action'] == 'SELL' else 1
            unrealized_pnl += round((current_price - entry_price) * qty * multiplier, 2)

        exit_reason = None
        should_exit, reason = SystemConfig.should_square_off_position(trade)
        if should_exit:
            if "PRE_EXPIRY" in reason:
                exit_reason = TradeStatus.CLOSED_EXPIRY_EXIT.value
            else:
                exit_reason = TradeStatus.CLOSED_VETO_EVENT.value
            self.logger.info(f"Scheduled square-off triggered for {trade.strategy_id}: {reason}")

        if exit_reason:
            await self.exit_position(trade, exit_reason, current_prices, db, loop)

        return unrealized_pnl
    
    async def exit_position(self, trade: TradeJournal, exit_reason: str, 
                           current_prices: Dict, db: Session, loop):
        executor = self._order_executor
        result = await loop.run_in_executor(
            self._executor,
            executor.exit_position,
            trade, exit_reason, current_prices, db
        )
        
        if result["success"] and self.alert_service:
            try:
                msg = f"""
<b>Strategy Closed:</b> {trade.strategy_id}
<b>Type:</b> {trade.strategy_type}
<b>Total P&L:</b> ₹{result['realized_pnl']:.2f}{' (approximate)' if result.get('pnl_approximate') else ''}
<b>Exit Reason:</b> {exit_reason}
"""
                priority = AlertPriority.SUCCESS if result['realized_pnl'] > 0 else AlertPriority.HIGH
                throttle_key = f"exit_{trade.strategy_id}"
                self.alert_service.send("Trade Closed", msg, priority, throttle_key)
            except Exception as e:
                self.logger.error(f"Alert sending failed: {e}")
    
    async def trigger_circuit_breaker(self, db: Session, loop):
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        executor = self._order_executor
        
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
            instrument_tokens = [leg['instrument_token'] for leg in legs_data]
            
            current_prices = await loop.run_in_executor(
                self._executor,
                self.fetcher.get_bulk_ltp_with_fallback,
                instrument_tokens
            )
            
            if current_prices:
                await loop.run_in_executor(
                    self._executor,
                    executor.exit_position,
                    trade, TradeStatus.CLOSED_CIRCUIT_BREAKER.value, current_prices, db
                )
        
        today = datetime.now().date()
        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
        if stats:
            stats.circuit_breaker_triggered = True
            db.commit()
        
        self.logger.critical("ALL POSITIONS CLOSED - CIRCUIT BREAKER ACTIVE")
    
    def _update_daily_stats(self, db: Session, realized_pnl: float, unrealized_pnl: float):
        # Use IST-aware datetimes throughout to prevent bucketing errors at DST boundaries
        # and midnight rollovers. datetime.now() without tz is naive and unreliable.
        now_ist = datetime.now(IST_TZ)
        today = now_ist.date()
        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
        
        if not stats:
            stats = DailyStats(date=today)
            db.add(stats)
        
        stats.realized_pnl = round(realized_pnl, 2)
        stats.unrealized_pnl = round(unrealized_pnl, 2)
        stats.total_pnl = round(realized_pnl + unrealized_pnl, 2)

        # Compute trade-level stats from today's closed trades so that
        # trades_count, wins, losses, theta_pnl and vega_pnl are never left at 0.
        try:
            # Use IST-aware day boundaries so that 3:30 PM trades are not spill into tomorrow
            day_start = IST_TZ.localize(datetime.combine(today, dt_time.min))
            day_end   = IST_TZ.localize(datetime.combine(today, dt_time.max))

            today_trades = db.query(TradeJournal).filter(
                TradeJournal.exit_time >= day_start,
                TradeJournal.exit_time <= day_end,
                TradeJournal.realized_pnl.isnot(None),
            ).all()

            stats.trades_count = len(today_trades)
            stats.wins   = sum(1 for t in today_trades if (t.realized_pnl or 0) > 0)
            stats.losses = sum(1 for t in today_trades if (t.realized_pnl or 0) <= 0)
            stats.theta_pnl = round(sum((t.theta_pnl or 0) for t in today_trades), 2)
            stats.vega_pnl  = round(sum((t.vega_pnl  or 0) for t in today_trades), 2)
        except Exception as _stats_err:
            self.logger.warning(f"_update_daily_stats: could not compute trade counts: {_stats_err}")

        db.commit()
    
    def stop(self):
        self.is_running = False
        if self._subscribed_instruments:
            self.fetcher.unsubscribe_market_data(list(self._subscribed_instruments))
        if self._executor:
            self._executor.shutdown(wait=True)
        self.logger.info("Position monitoring stopped")


# ============================================================================
# COMPLETE SYSTEM ORCHESTRATOR
# ============================================================================

class VolGuardSystem:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if not UPSTOX_AVAILABLE:
            raise RuntimeError("Upstox SDK not installed. Cannot initialize VolGuardSystem.")
        
        self.fetcher = UpstoxFetcher(SystemConfig.UPSTOX_ACCESS_TOKEN)
        self.analytics = AnalyticsEngine()
        self.regime = RegimeEngine()
        
        self.json_cache = JSONCacheManager()
        self.analytics_cache = AnalyticsCache()
        self.correlation_manager = CorrelationManager(SessionLocal)
        
        if DynamicConfig.get("AUTO_TRADING") and SystemConfig.UPSTOX_ACCESS_TOKEN:
            self.logger.info("REAL TRADING MODE ENABLED - Using UpstoxOrderExecutor")
            self.executor = UpstoxOrderExecutor(self.fetcher, alert_service=None)
        else:
            self.logger.info("MOCK TRADING MODE ENABLED")
            self.executor = MockExecutor(self.fetcher)
        
        self.analytics_scheduler: Optional[AnalyticsScheduler] = None
        self.monitor: Optional[PositionMonitor] = None
        self.alert_service: Optional[TelegramAlertService] = None
        
        self.market_streamer_started = False
        self.portfolio_streamer_started = False
        self._cached_expiries: List[date] = []
        
        self.ws_manager = WebSocketManager()
        
        self.logger.info("VolGuard System initialized")
    
    def start_market_streamer(self, instrument_keys: List[str] = None, mode: str = "ltpc"):
        if instrument_keys is None:
            instrument_keys = [SystemConfig.NIFTY_KEY, SystemConfig.VIX_KEY]
        
        self.fetcher.start_market_streamer(instrument_keys, mode)
        self.market_streamer_started = True
        self.logger.info(f"Market streamer started with {len(instrument_keys)} instruments")
    
    def start_portfolio_streamer(self):
        self.fetcher.start_portfolio_streamer(
            order_update=True,
            position_update=False,
            holding_update=False,
            gtt_update=True
        )
        # Wire GTT fill detection — auto-close TradeJournal when SL/target fires
        if self.fetcher.portfolio_streamer:
            self.fetcher.portfolio_streamer.register_gtt_fill_callback(self._on_gtt_fill)
        self.portfolio_streamer_started = True
        self.logger.info("Portfolio streamer started")
    
    def _on_gtt_fill(self, order_id: str, fill_info: Dict) -> None:
        """
        Called by PortfolioStreamer when a GTT order reaches FILLED status.
        Finds the TradeJournal whose gtt_order_ids contains this order_id
        and closes it with realised P&L computed from fill prices.
        This is the fix for the GTT exit detection gap — previously a GTT
        firing at the exchange left the DB record stuck as ACTIVE forever.
        """
        try:
            with SessionLocal() as db:
                active_trades = db.query(TradeJournal).filter(
                    TradeJournal.status == TradeStatus.ACTIVE.value
                ).all()
                
                matched_trade = None
                gtt_ids = []
                for trade in active_trades:
                    if not trade.gtt_order_ids:
                        continue
                    try:
                        _ids = json.loads(trade.gtt_order_ids) if isinstance(trade.gtt_order_ids, str) else trade.gtt_order_ids
                        if order_id in (_ids or []):
                            matched_trade = trade
                            gtt_ids = _ids or []
                            break
                    except Exception:
                        continue
                
                if not matched_trade:
                    # Not an active trade — check if it's a subsequent fill on CLOSED_GTT
                    # (e.g., wing BUY legs closing after the short's SL fired)
                    closed_gtt_trades = db.query(TradeJournal).filter(
                        TradeJournal.status == TradeStatus.CLOSED_GTT.value
                    ).all()
                    for ct in closed_gtt_trades:
                        if not ct.gtt_order_ids:
                            continue
                        try:
                            _ct_ids = json.loads(ct.gtt_order_ids) if isinstance(ct.gtt_order_ids, str) else ct.gtt_order_ids
                            if order_id in (_ct_ids or []):
                                fill_avg = fill_info.get('average_price', 0) or 0
                                self.logger.info(
                                    f"Subsequent GTT fill on CLOSED_GTT trade {ct.strategy_id}: "
                                    f"order={order_id} price={fill_avg}"
                                )
                                # Reconcile outside this session to avoid nested session issues
                                threading.Thread(
                                    target=self._reconcile_gtt_pnl,
                                    args=(ct.strategy_id, order_id, fill_avg),
                                    daemon=True
                                ).start()
                                return
                        except Exception:
                            continue
                    self.logger.debug(f"GTT fill {order_id} — no matching trade found (active or CLOSED_GTT)")
                    return
                
                self.logger.info(
                    f"GTT fill matched trade {matched_trade.strategy_id} "
                    f"(order_id={order_id}, avg_price={fill_info.get('average_price', 0)})"
                )
                
                # Compute realised P&L using fill price for the triggered leg;
                # use entry price for remaining legs (they are still open or will
                # be closed by the exchange via other GTT legs).
                legs_data = json.loads(matched_trade.legs_data) if isinstance(matched_trade.legs_data, str) else matched_trade.legs_data
                fill_avg = fill_info.get('average_price', 0) or 0
                realized_pnl = 0.0
                for leg in legs_data:
                    qty = leg.get('filled_quantity', leg.get('quantity', 0))
                    entry = leg.get('entry_price', 0)
                    multiplier = -1 if leg['action'] == 'SELL' else 1
                    # Use the GTT fill price for the leg that triggered; LTP-approximate for others
                    exit_px = fill_avg if fill_avg > 0 else entry
                    realized_pnl += (exit_px - entry) * qty * multiplier
                
                matched_trade.status = TradeStatus.CLOSED_GTT.value
                matched_trade.exit_reason = f'GTT_FILL:{order_id}'
                matched_trade.exit_time = datetime.now()
                matched_trade.realized_pnl = round(realized_pnl, 2)
                matched_trade.pnl_approximate = True  # GTT fills other legs via exchange
                matched_trade.trade_outcome_class = classify_trade_from_obj(matched_trade)
                db.commit()
                
                self.logger.info(
                    f"Trade {matched_trade.strategy_id} auto-closed by GTT fill. "
                    f"P&L ≈ ₹{realized_pnl:.2f} (approximate)"
                )
                
                # Cancel the surviving twin GTT immediately.
                # Each SELL leg gets two GTTs: one SL (ABOVE) and one target (BELOW).
                # When one fires, the sibling must be cancelled or it may trigger
                # an unintended new position at a later price level.
                # Retry once (2s delay) before firing the critical alert.
                surviving_gtt_ids = [g for g in (gtt_ids or []) if g != order_id]
                if surviving_gtt_ids and hasattr(self, 'executor') and self.executor:
                    _cancel_success = False
                    for _attempt in range(2):
                        try:
                            self.executor.cancel_gtt_orders(surviving_gtt_ids)
                            self.logger.info(
                                f"Twin GTT cancelled after fill (attempt {_attempt+1}): {surviving_gtt_ids}"
                            )
                            _cancel_success = True
                            break
                        except Exception as _gtterr:
                            self.logger.warning(
                                f"Twin GTT cancel attempt {_attempt+1} failed for {surviving_gtt_ids}: {_gtterr}"
                            )
                            if _attempt == 0:
                                # Never sleep inside the WebSocket callback thread.
                                # Offload the 2-second wait + retry to a background daemon thread
                                # so the WS receive loop is never blocked.
                                import threading as _t
                                def _retry_cancel(ids=surviving_gtt_ids):
                                    time.sleep(2)
                                    try:
                                        self.executor.cancel_gtt_orders(ids)
                                        self.logger.info(f"Twin GTT cancelled on background retry: {ids}")
                                    except Exception as _e2:
                                        self.logger.error(
                                            f"CRITICAL: Twin GTT cancel background retry failed for {ids}: {_e2} "
                                            f"— manual cancellation in Upstox required immediately."
                                        )
                                        if self.alert_service:
                                            self.alert_service.send(
                                                "🚨 CRITICAL: Twin GTT Cancellation Failed",
                                                f"GTT fill fired but surviving twin GTTs {ids} "
                                                f"could NOT be cancelled after 2 attempts.\n"
                                                f"⚠️ MANUAL CANCELLATION IN UPSTOX REQUIRED IMMEDIATELY.",
                                                AlertPriority.CRITICAL,
                                                throttle_key=f"gtt_twin_fail_{order_id}"
                                            )
                                _t.Thread(target=_retry_cancel, daemon=True, name="gtt_cancel_retry").start()
                                _cancel_success = True  # treat as handled; background thread owns the retry
                                break
                    if not _cancel_success:
                        self.logger.error(
                            f"CRITICAL: Could not cancel twin GTTs {surviving_gtt_ids} "
                            f"after fill {order_id} — 2 attempts failed. Manual cancellation required immediately."
                        )
                        if self.alert_service:
                            self.alert_service.send(
                                "🚨 CRITICAL: Twin GTT Cancellation Failed",
                                f"GTT fill {order_id} fired but surviving twin GTTs {surviving_gtt_ids} "
                                f"could NOT be cancelled after 2 attempts.\n"
                                f"Trade: {matched_trade.strategy_id}\n"
                                f"⚠️ MANUAL CANCELLATION IN UPSTOX REQUIRED IMMEDIATELY.",
                                AlertPriority.CRITICAL,
                                throttle_key=f"gtt_twin_fail_{order_id}"
                            )
                
                if self.alert_service:
                    alert_msg = (
                        f"<b>{matched_trade.strategy_type}</b> auto-closed by GTT order.\n"
                        f"Order: {order_id} | Fill: \u20b9{fill_avg:,.2f}\n"
                        f"P&L \u2248 \u20b9{realized_pnl:,.2f} (approximate)"
                    )
                    self.alert_service.send(
                        "GTT Stop-Loss / Target Hit",
                        alert_msg,
                        AlertPriority.HIGH,
                        throttle_key=f"gtt_fill_{matched_trade.strategy_id}"
                    )
        except Exception as e:
            self.logger.error(f"_on_gtt_fill error for order {order_id}: {e}", exc_info=True)

    def _reconcile_gtt_pnl(self, strategy_id: str, order_id: str, fill_price: float) -> None:
        """
        Reconcile realized P&L for a CLOSED_GTT trade when additional GTT fills arrive
        (e.g., wing legs filled after the short leg's GTT triggered).
        Updates fill_prices JSON and recomputes realized_pnl from actual fills.
        Called by _on_gtt_fill for any subsequent fills on an already-closed trade.
        """
        try:
            with SessionLocal() as db:
                trade = db.query(TradeJournal).filter(
                    TradeJournal.strategy_id == strategy_id
                ).first()
                if not trade or trade.status != TradeStatus.CLOSED_GTT.value:
                    return
                
                # Merge new fill price into existing fill_prices map
                fill_prices = {}
                if trade.fill_prices:
                    try:
                        fill_prices = json.loads(trade.fill_prices)
                    except Exception:
                        pass
                fill_prices[order_id] = fill_price
                
                # Recompute realized P&L from all known fill prices
                legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
                realized_pnl = 0.0
                still_approximate = False
                for leg in legs_data:
                    qty = leg.get('filled_quantity', leg.get('quantity', 0))
                    entry = leg.get('entry_price', 0)
                    multiplier = -1 if leg['action'] == 'SELL' else 1
                    # Use actual fill if available, else still approximate
                    exit_px = fill_prices.get(leg['instrument_token'],
                                fill_prices.get(leg.get('order_id', ''), None))
                    if exit_px is None:
                        exit_px = entry  # still unknown
                        still_approximate = True
                    realized_pnl += (exit_px - entry) * qty * multiplier
                
                trade.realized_pnl = round(realized_pnl, 2)
                trade.pnl_approximate = still_approximate
                trade.fill_prices = json.dumps(fill_prices)
                db.commit()
                self.logger.info(
                    f"GTT P&L reconciled for {strategy_id}: "
                    f"₹{realized_pnl:.2f} (approximate={still_approximate})"
                )
        except Exception as e:
            self.logger.error(f"_reconcile_gtt_pnl error for {strategy_id}: {e}")

    def run_complete_analysis(self) -> Dict:
        try:
            nifty_hist = self.fetcher.history(SystemConfig.NIFTY_KEY)
            vix_hist = self.fetcher.history(SystemConfig.VIX_KEY)
            
            spot = self.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY) or 0
            vix = self.fetcher.get_ltp_with_fallback(SystemConfig.VIX_KEY) or 0
            
            if nifty_hist is None:
                raise ValueError("Failed to fetch Nifty historical data")
            if vix_hist is None:
                raise ValueError("Failed to fetch VIX historical data")
            if spot <= 0 or vix <= 0:
                raise ValueError(f"Invalid prices - Spot: {spot}, VIX: {vix}")
            
            weekly, monthly, next_weekly, lot_size, all_expiries = self.fetcher.get_expiries()
            self._cached_expiries = all_expiries
            
            if not weekly:
                raise ValueError("Cannot fetch expiries from Upstox SDK")
            
            weekly_chain = self.fetcher.chain(weekly)
            monthly_chain = self.fetcher.chain(monthly)
            next_weekly_chain = self.fetcher.chain(next_weekly)
            
            time_metrics = self.analytics.get_time_metrics(weekly, monthly, next_weekly, all_expiries)
            
            vol_metrics = self.analytics.get_vol_metrics(
                nifty_hist, vix_hist, spot, vix
            )
            
            struct_weekly = self.analytics.get_struct_metrics(weekly_chain, vol_metrics.spot, lot_size)
            struct_monthly = self.analytics.get_struct_metrics(monthly_chain, vol_metrics.spot, lot_size)
            struct_next_weekly = self.analytics.get_struct_metrics(next_weekly_chain, vol_metrics.spot, lot_size)
            
            edge_metrics = self.analytics.get_edge_metrics(
                weekly_chain, monthly_chain, next_weekly_chain,
                vol_metrics.spot, vol_metrics, time_metrics.is_expiry_day_weekly,
                dte_weekly=time_metrics.dte_weekly,
                dte_monthly=time_metrics.dte_monthly,
                dte_next_weekly=time_metrics.dte_next_weekly
            )
            
            external_metrics = self.json_cache.get_external_metrics()
            
            weekly_score = self.regime.calculate_scores(
                vol_metrics, struct_weekly, edge_metrics, external_metrics,
                "WEEKLY", time_metrics.dte_weekly
            )
            
            monthly_score = self.regime.calculate_scores(
                vol_metrics, struct_monthly, edge_metrics, external_metrics,
                "MONTHLY", time_metrics.dte_monthly
            )
            
            next_weekly_score = self.regime.calculate_scores(
                vol_metrics, struct_next_weekly, edge_metrics, external_metrics,
                "NEXT_WEEKLY", time_metrics.dte_next_weekly
            )
            
            weekly_mandate = self.regime.generate_mandate(
                weekly_score, vol_metrics, struct_weekly, edge_metrics,
                external_metrics, time_metrics, "WEEKLY", weekly, time_metrics.dte_weekly
            )
            
            monthly_mandate = self.regime.generate_mandate(
                monthly_score, vol_metrics, struct_monthly, edge_metrics,
                external_metrics, time_metrics, "MONTHLY", monthly, time_metrics.dte_monthly
            )
            
            next_weekly_mandate = self.regime.generate_mandate(
                next_weekly_score, vol_metrics, struct_next_weekly, edge_metrics,
                external_metrics, time_metrics, "NEXT_WEEKLY", next_weekly, time_metrics.dte_next_weekly
            )
            
            primary_mandate = weekly_mandate if weekly_mandate.is_trade_allowed else \
                             monthly_mandate if monthly_mandate.is_trade_allowed else \
                             next_weekly_mandate if next_weekly_mandate.is_trade_allowed else None
            
            professional_recommendation = {
                "primary": {
                    "expiry_type": primary_mandate.expiry_type if primary_mandate else "NONE",
                    "strategy": primary_mandate.suggested_structure if primary_mandate else "CASH",
                    "capital_deploy_formatted": f"₹{primary_mandate.deployment_amount:,.0f}" if primary_mandate else "₹0"
                }
            }
            
            return {
                "time_metrics": time_metrics,
                "vol_metrics": vol_metrics,
                "struct_weekly": struct_weekly,
                "struct_monthly": struct_monthly,
                "struct_next_weekly": struct_next_weekly,
                "edge_metrics": edge_metrics,
                "external_metrics": external_metrics,
                "weekly_score": weekly_score,
                "monthly_score": monthly_score,
                "next_weekly_score": next_weekly_score,
                "weekly_mandate": weekly_mandate,
                "monthly_mandate": monthly_mandate,
                "next_weekly_mandate": next_weekly_mandate,
                "lot_size": lot_size,
                "weekly_chain": weekly_chain,
                "monthly_chain": monthly_chain,
                "next_weekly_chain": next_weekly_chain,
                "all_expiries": all_expiries,
                "professional_recommendation": professional_recommendation
            }
        
        except Exception as e:
            self.logger.error(f"Analysis error: {e}")
            cached = self.analytics_cache.get()
            if cached:
                self.logger.info("Returning cached analysis due to error")
                return cached
            raise
    
    def construct_strategy_from_mandate(self, mandate: TradingMandate, 
                                       analysis_data: Dict) -> Optional[ConstructedStrategy]:
        if not mandate.is_trade_allowed:
            self.logger.info(f"Trade not allowed for {mandate.expiry_type}: {mandate.veto_reasons}")
            return None
        
        strategy_type_str = mandate.suggested_structure
        
        strategy_type_map = {
            "IRON_FLY": StrategyType.IRON_FLY,
            "IRON_CONDOR": StrategyType.IRON_CONDOR,
            "PROTECTED_STRADDLE": StrategyType.PROTECTED_STRADDLE,
            "PROTECTED_STRANGLE": StrategyType.PROTECTED_STRANGLE,
            "BULL_PUT_SPREAD": StrategyType.BULL_PUT_SPREAD,
            "BEAR_CALL_SPREAD": StrategyType.BEAR_CALL_SPREAD
        }
        
        strategy_type = strategy_type_map.get(strategy_type_str)
        if not strategy_type:
            self.logger.error(f"Unknown strategy type: {strategy_type_str}")
            return None
        
        factory = StrategyFactory(
            self.fetcher,
            analysis_data['vol_metrics'].spot
        )
        
        expiry_type_map = {
            "WEEKLY":      ExpiryType.WEEKLY,
            "MONTHLY":     ExpiryType.MONTHLY,
            "NEXT_WEEKLY": ExpiryType.NEXT_WEEKLY
        }
        resolved_expiry_type = expiry_type_map.get(mandate.expiry_type, ExpiryType.WEEKLY)

        if strategy_type == StrategyType.IRON_FLY:
            strategy = factory.construct_iron_fly(mandate.expiry_date, mandate.deployment_amount, resolved_expiry_type)
        elif strategy_type == StrategyType.IRON_CONDOR:
            strategy = factory.construct_iron_condor(mandate.expiry_date, mandate.deployment_amount, resolved_expiry_type)
        elif strategy_type == StrategyType.PROTECTED_STRADDLE:
            strategy = factory.construct_protected_straddle(mandate.expiry_date, mandate.deployment_amount, resolved_expiry_type)
        elif strategy_type == StrategyType.PROTECTED_STRANGLE:
            strategy = factory.construct_protected_strangle(mandate.expiry_date, mandate.deployment_amount, resolved_expiry_type)
        elif strategy_type == StrategyType.BULL_PUT_SPREAD:
            strategy = factory.construct_bull_put_spread(mandate.expiry_date, mandate.deployment_amount, resolved_expiry_type)
        elif strategy_type == StrategyType.BEAR_CALL_SPREAD:
            strategy = factory.construct_bear_call_spread(mandate.expiry_date, mandate.deployment_amount, resolved_expiry_type)
        else:
            return None
        
        if not strategy:
            return None

        # expiry_type is now set correctly at construction time.
        # The override below is kept as a safety net only.
        strategy.expiry_type = resolved_expiry_type

        allowed, violations = self.correlation_manager.can_take_position(strategy)
        if not allowed:
            self.logger.warning(f"Correlation violation - cannot take position: {violations[0].rule}")
            return None
        
        return strategy
    
    def execute_strategy(self, strategy: ConstructedStrategy, db: Session, external_metrics=None,
                          mandate=None, regime_score=None, vol_metrics=None,
                          struct_metrics=None, edge_metrics=None) -> Dict:
        if not strategy.validation_passed:
            return {
                "success": False,
                "message": "Strategy validation failed",
                "errors": strategy.validation_errors
            }

        # ── GLOBAL CAPITAL UTILIZATION CHECK ─────────────────────────────
        # Block new trades if total deployed capital (allocated_capital from
        # all active trades + this new trade) would exceed 80% of BASE_CAPITAL.
        # This prevents inadvertently being 120%+ deployed when weekly + monthly
        # + next-weekly all trigger on the same day.
        try:
            active_trades_for_capital = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            total_deployed = sum(t.allocated_capital or 0 for t in active_trades_for_capital)
            base_capital = DynamicConfig.get("BASE_CAPITAL")
            capital_limit = base_capital * 0.80
            new_allocation = strategy.allocated_capital or 0
            if total_deployed + new_allocation > capital_limit:
                utilization_pct = (total_deployed + new_allocation) / base_capital * 100
                msg = (
                    f"Capital utilization limit exceeded: "
                    f"₹{total_deployed:,.0f} deployed + ₹{new_allocation:,.0f} new = "
                    f"{utilization_pct:.1f}% > 80% limit (₹{capital_limit:,.0f}). "
                    f"Close existing positions before opening new ones."
                )
                self.logger.warning(msg)
                if self.alert_service:
                    self.alert_service.send(
                        "Capital Utilization Limit Reached",
                        msg,
                        AlertPriority.HIGH,
                        throttle_key="capital_limit"
                    )
                return {
                    "success": False,
                    "message": msg,
                    "capital_utilization_block": True,
                    "total_deployed": round(total_deployed, 2),
                    "new_allocation": round(new_allocation, 2),
                    "capital_limit": round(capital_limit, 2),
                    "utilization_pct": round(utilization_pct, 1)
                }
        except Exception as cap_err:
            self.logger.warning(f"Capital utilization check error (non-blocking): {cap_err}")
        # ─────────────────────────────────────────────────────────────────

        # ── PRE-TRADE INTELLIGENCE GATE ───────────────────────────────────
        # Fires before every real order. Can VETO or warn.
        # Only runs if intelligence agents are available (ANTHROPIC_API_KEY or GROQ_API_KEY set).
        v5_result = None
        try:
            if V5_LLM_READY and mandate and regime_score:
                nifty_spot = 0.0
                india_vix = 0.0
                try:
                    nifty_spot = self.fetcher.get_ltp_with_fallback("NSE_INDEX|Nifty 50") or 0.0
                    india_vix = self.fetcher.get_ltp_with_fallback("NSE_INDEX|India VIX") or 0.0
                except Exception:
                    pass
                fii_today = None
                upcoming_events = None
                try:
                    if self.json_cache:
                        _cache = self.json_cache.get_today_cache()
                        if _cache and _cache.get("is_valid"):
                            fii_today = _cache.get("fii_net_change")
                            # Reconstruct EconomicEvent objects properly
                            _ext = self.json_cache.get_external_metrics()
                            upcoming_events = _ext.economic_events if _ext else None
                except Exception:
                    pass
                morning_tone = "UNKNOWN"
                try:
                    brief = V5MorningBriefAgent.get().get_latest()
                    if brief:
                        morning_tone = brief.global_tone
                except Exception:
                    pass
                # struct_metrics and edge_metrics arrive from execute_mandate via the outer signature
                # They are expiry-specific and carry full VRP, GEX, skew, term structure data
                pretrade_agent = V5PreTradeAgent.get()
                v5_result = pretrade_agent.evaluate(
                    mandate=mandate,
                    regime_score=regime_score,
                    vol_metrics=vol_metrics,
                    nifty_spot=nifty_spot,
                    india_vix=india_vix,
                    fii_today=fii_today,
                    upcoming_events=upcoming_events,
                    morning_tone=morning_tone,
                    struct_metrics=struct_metrics,
                    edge_metrics=edge_metrics,
                )
                if self.alert_service:
                    try:
                        self.alert_service.send_raw(v5_result.to_telegram(
                            strategy=mandate.suggested_structure,
                            expiry=f"{mandate.expiry_type} {mandate.expiry_date}",
                            score=regime_score.total_score,
                        ))
                    except Exception:
                        pass
                if v5_result.is_veto:
                    self.logger.warning(
                        f"INTELLIGENCE VETO: {v5_result.veto_reason} | "
                        f"Revisit when: {v5_result.revisit_when}"
                    )
                    return {
                        "success": False,
                        "message": f"INTELLIGENCE VETO: {v5_result.veto_reason}",
                        "veto_reason": v5_result.veto_reason,
                        "revisit_when": v5_result.revisit_when,
                        "recommendation": "VETO",
                        "v5_rationale": v5_result.recommendation_rationale,
                    }
                if v5_result.is_caution:
                    self.logger.warning(f"INTELLIGENCE CAUTION: {v5_result.suggested_adjustments}")
                    _adj_applied = []

                    for adj in v5_result.suggested_adjustments:
                        adj_l = adj.lower()

                        # ── Size / allocation reduction ──────────────────────────────
                        pct_match = re.search(r"(\d{2,3})\s*%", adj)
                        reduce_pct = None
                        if pct_match:
                            p = int(pct_match.group(1))
                            if 30 <= p <= 90:   # sanity range
                                reduce_pct = p / 100.0
                        if reduce_pct is None and any(
                            w in adj_l for w in ["reduce", "cut", "half", "smaller", "less capital", "scale back"]
                        ):
                            reduce_pct = 0.75  # default: 75% of full

                        if reduce_pct is not None:
                            for leg in strategy.legs:
                                new_q = max(
                                    leg.lot_size,
                                    int(leg.quantity * reduce_pct // leg.lot_size) * leg.lot_size
                                )
                                if new_q != leg.quantity:
                                    leg.quantity = new_q
                                    _adj_applied.append(f"size→{int(reduce_pct*100)}%")
                            continue

                        # ── Expiry preference: prefer monthly → log only (cannot switch mid-execute) ──
                        if any(w in adj_l for w in ["prefer monthly", "choose monthly", "avoid weekly",
                                                     "monthly over weekly", "skip weekly"]):
                            self.logger.warning(
                                f"INTELLIGENCE CAUTION [EXPIRY]: {adj} — cannot change expiry at this stage; "
                                f"noted for next cycle"
                            )
                            _adj_applied.append("expiry-pref-logged")
                            continue

                        # ── Strike width: "widen by N points" — log + note for manual review ──
                        width_match = re.search(r"widen.*?(\d+)\s*point", adj_l)
                        if width_match:
                            widen_pts = int(width_match.group(1))
                            self.logger.warning(
                                f"INTELLIGENCE CAUTION [STRIKE WIDTH]: Agent recommends widening wings by "
                                f"{widen_pts}pt — cannot auto-widen on live strategy; "
                                f"apply manually or re-run construct_strategy"
                            )
                            _adj_applied.append(f"widen-{widen_pts}pt-manual")
                            continue

                    if _adj_applied:
                        self.logger.info(f"INTELLIGENCE CAUTION: applied adjustments: {_adj_applied}")
                    else:
                        self.logger.warning(
                            "INTELLIGENCE CAUTION: adjustments received but none auto-applied "
                            "(review suggested_adjustments in pre-trade log)"
                        )
        except Exception as v5_err:
            self.logger.error(f"Pre-trade gate error (non-blocking): {v5_err}")
        # ── END PRE-TRADE GATE ───────────────────────────────────────────────────────

        result = self.executor.place_multi_order(strategy)
        
        if result['success']:
            entry_premium = round(sum(
                leg.entry_price * (leg.filled_quantity if leg.filled_quantity > 0 else leg.quantity) * (1 if leg.action == 'SELL' else -1)
                for leg in strategy.legs
            ), 2)
            
            filled_quantities = {}
            fill_prices = {}
            if 'filled_orders' in result:
                for order_id, fill_info in result['filled_orders'].items():
                    filled_quantities[order_id] = fill_info.get('filled_quantity', 0)
            
            if 'fill_prices' in result:
                fill_prices = result['fill_prices']
            
            # ── Snapshot full context at entry for coaching layer ──────────────────────────
            _regime_score_val  = round(regime_score.total_score, 2) if regime_score else None
            _vov_zscore_val    = round(getattr(vol_metrics, "vov_zscore", 0) or 0, 2) if vol_metrics else None
            _ivp_val           = round(getattr(vol_metrics, "ivp_1yr", 50) or 50, 1) if vol_metrics else None
            _vol_regime_val    = getattr(vol_metrics, "vol_regime", None) if vol_metrics else None
            _morning_tone_val  = "UNKNOWN"
            try:
                _brief = V5MorningBriefAgent.get().get_latest()
                if _brief:
                    _morning_tone_val = _brief.global_tone
            except Exception:
                pass
            _pretrade_verdict  = getattr(v5_result, "recommendation", None) if v5_result else None
            _pretrade_rationale = (getattr(v5_result, "recommendation_rationale", "") or "")[:500] if v5_result else None
            _score_drivers     = getattr(regime_score, "score_drivers", []) if regime_score else []
            _weighted_vrp      = None
            try:
                if edge_metrics:
                    _exp_key = (strategy.expiry_type.value or "WEEKLY").upper()
                    _vrp_map = {"WEEKLY": "weighted_vrp_weekly", "MONTHLY": "weighted_vrp_monthly",
                                "NEXT_WEEKLY": "weighted_vrp_next_weekly"}
                    _weighted_vrp = getattr(edge_metrics, _vrp_map.get(_exp_key, "weighted_vrp_weekly"), None)
                    if _weighted_vrp:
                        _weighted_vrp = round(_weighted_vrp, 2)
            except Exception:
                pass
            _vix_at_entry = None
            try:
                _vix_at_entry = round(self.fetcher.get_ltp_with_fallback("NSE_INDEX|India VIX") or 0, 2)
            except Exception:
                pass
            # ──────────────────────────────────────────────────────────────────────────

            trade = TradeJournal(
                strategy_id=strategy.strategy_id,
                strategy_type=strategy.strategy_type.value,
                expiry_type=strategy.expiry_type.value,
                expiry_date=strategy.expiry_date,
                entry_time=datetime.now(),
                legs_data=json.dumps([asdict(leg) for leg in strategy.legs]),
                order_ids=json.dumps(result.get('order_ids', [])),
                filled_quantities=json.dumps(filled_quantities) if filled_quantities else None,
                fill_prices=json.dumps(fill_prices) if fill_prices else None,
                gtt_order_ids=json.dumps(result.get('gtt_order_ids', [])),
                entry_greeks_snapshot=json.dumps(result.get('entry_greeks', {})),
                max_profit=round(strategy.max_profit, 2),
                max_loss=round(strategy.max_loss, 2),
                required_margin=round(strategy.required_margin, 2),
                allocated_capital=round(strategy.allocated_capital, 2),
                entry_premium=entry_premium,
                status=TradeStatus.ACTIVE.value,
                is_mock=not DynamicConfig.get("AUTO_TRADING"),
                # Context snapshot — the WHY behind this trade
                regime_score_at_entry     = _regime_score_val,
                vix_at_entry              = _vix_at_entry,
                ivp_at_entry              = _ivp_val,
                vol_regime_at_entry       = _vol_regime_val,
                morning_tone_at_entry     = _morning_tone_val,
                pretrade_verdict_at_entry = _pretrade_verdict,
                vov_zscore_at_entry       = _vov_zscore_val,
                weighted_vrp_at_entry     = _weighted_vrp,
                score_drivers_at_entry    = _score_drivers,
                pretrade_rationale        = _pretrade_rationale,
            )
            if external_metrics and hasattr(external_metrics, 'economic_events'):
                for ev in external_metrics.economic_events:
                    if getattr(ev, 'is_veto_event', False) and hasattr(ev, 'event_date') and ev.event_date:
                        trade.associated_event_date = ev.event_date
                        trade.associated_event_name = getattr(ev, 'title', 'VETO_EVENT')
                        break
            try:
                db.add(trade)
                db.commit()
            except Exception as db_err:
                db.rollback()
                self.logger.critical(
                    f"POSITION OPEN AT BROKER BUT NOT RECORDED IN DB — MANUAL ACTION REQUIRED. "
                    f"Strategy: {strategy.strategy_id}. Error: {db_err}"
                )
                if self.alert_service:
                    self.alert_service.send(
                        "🚨 DB WRITE FAILURE — UNRECORDED POSITION",
                        f"Strategy {strategy.strategy_id} is live at broker but failed to save to DB.\n"
                        f"Error: {db_err}\nManual reconciliation required immediately.",
                        AlertPriority.CRITICAL,
                        throttle_key=f"db_fail_{strategy.strategy_id}"
                    )
                return result
            
            if self.alert_service:
                try:
                    msg = f"""
<b>New Position Opened</b>
<b>Strategy:</b> {strategy.strategy_type.value}
<b>Expiry:</b> {strategy.expiry_type.value}
<b>Max Profit:</b> ₹{strategy.max_profit:,.2f}
<b>Max Loss:</b> ₹{strategy.max_loss:,.2f}
<b>Required Margin:</b> ₹{strategy.required_margin:,.2f}
<b>Legs:</b> {len(strategy.legs)}
<b>POP:</b> {strategy.pop:.1f}%
"""
                    self.alert_service.send("Position Opened", msg, AlertPriority.MEDIUM)
                except Exception as e:
                    self.logger.error(f"Alert sending failed: {e}")
            
            self.logger.info(f"Strategy executed: {strategy.strategy_id}")
        
        return result


# ============================================================================
# JOURNAL COACH AGENT — Trading Psychology & Performance Intelligence
# ============================================================================

_V5_COACH_SYSTEM = """You are VolGuard's Personal Trading Coach — a senior derivatives risk manager with 15+ years on Indian options desks. You have seen every mistake an option seller makes. You are also a behavioral finance expert who understands the psychology of retail traders who have studied but still repeat structural errors.

YOUR JOB:
Analyze the trader's complete journal history. Answer the specific question with absolute precision — data first, then interpretation, then actionable recommendations. No fluff. No generic advice.

WHAT YOU HAVE ACCESS TO (USE ALL OF IT):
- Every trade: strategy type, expiry type, entry/exit premium, realized PnL
- Greek attribution per trade: theta PnL vs vega PnL vs gamma PnL
  -> Theta PnL = time decay earned = your REAL EDGE
  -> Vega PnL = IV movement impact = external risk you absorbed
  -> If vega loss > theta earned on a trade: you got punished by the market, not rewarded by your edge
- Market context at each trade entry:
  -> VIX level, IVP (implied volatility percentile), regime score
  -> Morning tone: CLEAR / CAUTIOUS_NEUTRAL / CAUTIOUS / RISK_OFF
  -> VoV z-score (vol-of-vol — the single most important regime stability signal)
  -> Pre-trade system verdict: PROCEED / PROCEED_WITH_CAUTION / VETO
- Exit reason: PROFIT_TARGET / STOP_LOSS / FOMC_VETO / EXPIRY / MANUAL

HOW TO STRUCTURE YOUR ANSWER:
1. DIRECT ANSWER — state the core finding in 1-2 sentences first. No preamble.
2. DATA EVIDENCE — cite specific trades by date and actual numbers. Show the math.
   Example: "Nov 3 IC entered with VoV 1.82σ -> Rs 68,000 loss. Nov 10 straddle on VETO override -> Rs 37,000 loss. Both times: VoV > 1.5σ and RISK_OFF/CAUTIOUS tone."
3. PATTERN IDENTIFICATION — name the exact repeating pattern if one exists.
   Compute the conditional win rates: "Win rate when VoV < 1.0σ: X%. Win rate when VoV > 1.5σ: Y%."
4. ROOT CAUSE — explain WHY the pattern exists in options mechanics terms.
   Example: "VoV > 1.5σ means the vol surface itself is unstable — your short vega is exposed to convexity in the IV surface, not just direction. IC short legs get crushed in both directions."
5. RECOMMENDATIONS — exactly 2-3 specific, concrete rules. Not "be careful." State exact numbers.
   Example: "Rule: No new IC entry when VoV z-score > 1.2σ. This would have saved Rs 1,28,000 in November alone."

GREEK ATTRIBUTION INTERPRETATION RULES:
- Theta/Vega ratio > 1.5 across a period = edge is real, regime was good
- Theta/Vega ratio < 0.8 = selling vol in the wrong conditions; losses were structural, not bad luck
- Gamma PnL large negative = held through spot movement after delta moved against the position
- If asked about "what attribution tells me" — compute the ratio, name the implication, cite specific trades

CRITICAL BEHAVIORS:
1. Work ONLY from the data provided. Never invent trades or numbers that don't appear in the journal.
2. When the data is from mock/demo trades: state it at the very start ("Note: analyzing demo trade data — patterns are realistic and the analysis is fully applicable") then analyze with the same rigor.
3. Skip definitions of theta, vega, delta — this trader knows options. Go straight to their specific pattern.
4. If data is too sparse: say "Insufficient data — I can see X trades but need at least 15-20 in this category to be statistically meaningful."
5. End EVERY response with "THE ONE CHANGE" — the single most impactful rule this trader should implement. One sentence. Specific. With numbers.

TONE: Prop desk mentor to a capable trader making fixable, pattern-based errors. Direct. Precise. Respectful of their intelligence. Zero hand-holding."""

_V5_COACH_USER = """
{data_source_note}TRADER JOURNAL — FULL HISTORY
================================
{trade_history}

PERFORMANCE SUMMARY
================================
Total Trades     : {total_trades}
Win Rate         : {win_rate}%
Profit Factor    : {profit_factor}
Total PnL        : Rs {total_pnl:,.0f}
Avg Win          : Rs {avg_win:,.0f}
Avg Loss         : Rs {avg_loss:,.0f}
Max Single Loss  : Rs {max_loss:,.0f}
Best Trade       : Rs {best_trade:,.0f}

GREEK ATTRIBUTION SUMMARY
================================
Total Theta PnL  : Rs {total_theta_pnl:,.0f}
Total Vega PnL   : Rs {total_vega_pnl:,.0f}
Total Gamma PnL  : Rs {total_gamma_pnl:,.0f}
Theta/Vega Ratio : {theta_vega_ratio}
Interpretation   : Ratio > 1.5 = edge is working. Ratio < 0.8 = wrong regime conditions.

CONTEXT PATTERN SUMMARY
================================
Trades on RISK_OFF mornings    : {risk_off_trades}
Trades on CLEAR mornings       : {clear_trades}
VETO overrides that lost money : {veto_losses}
Win rate when IVP > 75 (rich)  : {win_rate_rich}
Win rate when IVP < 25 (cheap) : {win_rate_cheap}
Win rate on CAUTION days       : {win_rate_caution}

SKILL vs LUCK BREAKDOWN
================================
SKILL_WIN    (edge worked, theta drove profit)     : {skill_wins}
LUCKY_WIN    (won but not through theta/good cond) : {lucky_wins}
UNLUCKY_LOSS (right conditions, vol shock hit)     : {unlucky_losses}
SKILL_LOSS   (bad conditions entered, lost)        : {skill_losses}
True Skill Win Rate (SKILL_WIN only / all trades)  : {true_skill_win_rate}
Note: Reported win rate {win_rate}% includes Lucky Wins. True edge = Skill Wins only.

TRADER'S QUESTION
================================
{question}

---
INSTRUCTIONS:
- Answer using ONLY the data above. Do not invent trades or numbers.
- Reference specific trade dates and exact PnL figures from the journal above.
- Compute conditional statistics where relevant (e.g. win rate by VoV band, by morning tone).
- Structure: Direct answer -> Evidence from data -> Pattern -> Root cause -> Recommendations.
- End with "THE ONE CHANGE" -- one specific, numbered rule this trader should add immediately.
"""


class V5JournalCoachAgent:
    """
    On-demand coaching layer that reads the full trade journal and answers
    specific questions about trading performance, psychology, and patterns.

    Requires ANTHROPIC_API_KEY (web search not needed — all data is in the journal).
    Can also use Groq for faster/free responses.
    """
    _instance = None
    _lock = _v5_threading.RLock()

    def __init__(self):
        self.logger = logging.getLogger("V5JournalCoachAgent")

    @classmethod
    def get(cls) -> "V5JournalCoachAgent":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def _build_trade_history_string(self, trades: list) -> str:
        """Format trade history for the LLM prompt."""
        if not trades:
            return "No trades recorded yet."
        lines = []
        for t in trades:
            ctx_parts = []
            if t.regime_score_at_entry is not None:
                ctx_parts.append(f"RegimeScore={t.regime_score_at_entry:.1f}")
            if t.vix_at_entry is not None:
                ctx_parts.append(f"VIX={t.vix_at_entry:.1f}")
            if t.ivp_at_entry is not None:
                ctx_parts.append(f"IVP={t.ivp_at_entry:.0f}%")
            if t.vol_regime_at_entry:
                ctx_parts.append(f"VolRegime={t.vol_regime_at_entry}")
            if t.morning_tone_at_entry:
                ctx_parts.append(f"MorningTone={t.morning_tone_at_entry}")
            if t.pretrade_verdict_at_entry:
                ctx_parts.append(f"PreTradeVerdict={t.pretrade_verdict_at_entry}")
            if t.vov_zscore_at_entry is not None:
                ctx_parts.append(f"VoVz={t.vov_zscore_at_entry:.2f}")
            if t.weighted_vrp_at_entry is not None:
                ctx_parts.append(f"WeightedVRP={t.weighted_vrp_at_entry:.2f}%")

            pnl = t.realized_pnl or 0
            theta_pnl = t.theta_pnl or 0
            vega_pnl = t.vega_pnl or 0
            gamma_pnl = t.gamma_pnl or 0

            entry_dt = t.entry_time.strftime("%Y-%m-%d") if t.entry_time else "N/A"
            exit_dt = t.exit_time.strftime("%Y-%m-%d") if t.exit_time else "open"

            line = (
                f"[{entry_dt}→{exit_dt}] {t.strategy_type} {t.expiry_type} | "
                f"Entry:₹{t.entry_premium or 0:.0f} Exit:₹{t.exit_premium or 0:.0f} | "
                f"PnL:₹{pnl:+,.0f} | "
                f"Greeks: Θ={theta_pnl:+,.0f} V={vega_pnl:+,.0f} Γ={gamma_pnl:+,.0f} | "
                f"Exit:{t.exit_reason or 'N/A'} | "
                f"Outcome:{t.trade_outcome_class or 'UNCLASSIFIED'} | "
                f"Context: {' | '.join(ctx_parts) if ctx_parts else 'No context snapshot (legacy trade)'}"
            )
            lines.append(line)
        return "\n".join(lines)

    def _compute_stats(self, trades: list) -> dict:
        """Compute performance statistics from trade history."""
        if not trades:
            return {}

        completed = [t for t in trades if t.realized_pnl is not None]
        pnls = [t.realized_pnl for t in completed]

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        total_pnl = sum(pnls)
        win_rate = round(len(wins) / len(pnls) * 100, 1) if pnls else 0
        avg_win = round(sum(wins) / len(wins), 0) if wins else 0
        avg_loss = round(sum(losses) / len(losses), 0) if losses else 0
        profit_factor = round(sum(wins) / abs(sum(losses)), 2) if losses and sum(losses) != 0 else float("inf")

        total_theta = sum(t.theta_pnl or 0 for t in completed)
        total_vega = sum(t.vega_pnl or 0 for t in completed)
        total_gamma = sum(t.gamma_pnl or 0 for t in completed)
        theta_vega_ratio = round(total_theta / abs(total_vega), 2) if total_vega and total_vega != 0 else None

        # Context-based win rates (only for trades with context snapshots)
        def _wr(subset):
            if not subset:
                return "N/A (no data)"
            w = [t for t in subset if (t.realized_pnl or 0) > 0]
            return f"{round(len(w)/len(subset)*100,1)}% ({len(w)}/{len(subset)})"

        with_ctx = [t for t in completed if t.morning_tone_at_entry]
        risk_off_trades = [t for t in with_ctx if t.morning_tone_at_entry == "RISK_OFF"]
        clear_trades = [t for t in with_ctx if t.morning_tone_at_entry == "CLEAR"]
        caution_trades = [t for t in with_ctx if t.morning_tone_at_entry in ("CAUTIOUS", "CAUTIOUS_NEUTRAL")]

        with_ivp = [t for t in completed if t.ivp_at_entry is not None]
        rich_trades = [t for t in with_ivp if t.ivp_at_entry > 75]
        cheap_trades = [t for t in with_ivp if t.ivp_at_entry < 25]

        veto_overrides = [t for t in completed
                          if t.pretrade_verdict_at_entry == "VETO" and (t.realized_pnl or 0) < 0]

        return {
            "total_trades": len(pnls),
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_pnl": total_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "max_loss": min(pnls) if pnls else 0,
            "best_trade": max(pnls) if pnls else 0,
            "total_theta_pnl": total_theta,
            "total_vega_pnl": total_vega,
            "total_gamma_pnl": total_gamma,
            "theta_vega_ratio": theta_vega_ratio if theta_vega_ratio else "N/A",
            "risk_off_trades": f"{len(risk_off_trades)} trades | WR: {_wr(risk_off_trades)}",
            "clear_trades": f"{len(clear_trades)} trades | WR: {_wr(clear_trades)}",
            "veto_losses": len(veto_overrides),
            "win_rate_rich": _wr(rich_trades),
            "win_rate_cheap": _wr(cheap_trades),
            "win_rate_caution": _wr(caution_trades),
            # Outcome classification breakdown
            "skill_wins": sum(1 for t in completed if getattr(t, 'trade_outcome_class', '') == 'SKILL_WIN'),
            "lucky_wins": sum(1 for t in completed if getattr(t, 'trade_outcome_class', '') == 'LUCKY_WIN'),
            "unlucky_losses": sum(1 for t in completed if getattr(t, 'trade_outcome_class', '') == 'UNLUCKY_LOSS'),
            "skill_losses": sum(1 for t in completed if getattr(t, 'trade_outcome_class', '') == 'SKILL_LOSS'),
            "true_skill_win_rate": (
                f"{round(sum(1 for t in completed if getattr(t,'trade_outcome_class','')=='SKILL_WIN') / len(completed) * 100, 1)}%"
                if completed else "N/A"
            ),
        }

    def ask(self, question: str, db) -> dict:
        """
        Main entry point. Pass the trader's question and a DB session.
        Returns a dict with the coaching response and stats.
        """
        if not V5_LLM_READY:
            return {
                "ok": False,
                "error": "No LLM configured. Set GROQ_API_KEY or ANTHROPIC_API_KEY in .env.",
                "response": None,
            }

        try:
            trades = db.query(TradeJournal).order_by(TradeJournal.entry_time.desc()).limit(200).all()
        except Exception as e:
            return {"ok": False, "error": f"DB read failed: {e}", "response": None}

        if not trades:
            return {
                "ok": False,
                "error": "No trades in journal yet. Run the seed script or place real trades first.",
                "response": None,
            }

        # ── Mock data detection ──────────────────────────────────────────────────────────────────────
        # Seed/demo trades are marked is_mock=True. Real trades are is_mock=False.
        # When DEMO_MODE=false and real trades exist, this flag goes away naturally.
        mock_count = sum(1 for t in trades if getattr(t, "is_mock", False))
        real_count  = len(trades) - mock_count
        has_mock_data = mock_count > 0
        all_mock = mock_count == len(trades)

        if all_mock:
            data_source_note = (
                "[DATA SOURCE: Demo/seed trade data — realistic NIFTY options simulation "
                "covering Oct 2025 – Mar 2026. Patterns are intentionally designed to be "
                "analytically meaningful. Analysis applies directly to real trading.]\n\n"
            )
        elif has_mock_data:
            data_source_note = (
                f"[DATA SOURCE: Mixed — {real_count} real trades + {mock_count} demo trades. "
                f"Analysis covers both.]\n\n"
            )
        else:
            data_source_note = ""  # Pure real data — no note needed
        # ── End mock detection ─────────────────────────────────────────────────────────────────────────

        stats = self._compute_stats(trades)
        history_str = self._build_trade_history_string(trades)

        user_prompt = _V5_COACH_USER.format(
            trade_history=history_str,
            question=question,
            data_source_note=data_source_note,
            **{k: (v if v is not None else "N/A") for k, v in stats.items()},
        )

        llm_client = V5ClaudeClient.get()
        # 2500 tokens: coach needs space to show the math and cite specific trades
        raw = llm_client.call(_V5_COACH_SYSTEM, user_prompt, max_tokens=2500, ctx="coach") if llm_client else None

        if not raw:
            return {
                "ok": False,
                "error": "LLM call failed. Check GROQ_API_KEY or ANTHROPIC_API_KEY and connectivity.",
                "response": None,
                "stats": stats,
            }

        return {
            "ok": True,
            "question": question,
            "response": raw,
            "stats": stats,
            "trades_analyzed": len(trades),
            "has_mock_data": has_mock_data,
            "all_mock": all_mock,
            "mock_count": mock_count,
            "real_count": real_count,
            "llm_provider": _V5_LLM_PROVIDER,
            "timestamp": datetime.now(IST_TZ).isoformat(),
        }


# ============================================================================
# BACKGROUND RECONCILIATION JOBS
# ============================================================================

async def position_reconciliation_job():
    logger.info("Position reconciliation job started")
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="recon")
    
    while True:
        try:
            if volguard_system and volguard_system.fetcher.is_market_open_now():
                def sync_reconcile():
                    with SessionLocal() as db:
                        return volguard_system.fetcher.reconcile_positions_with_db(db)
                
                report = await loop.run_in_executor(executor, sync_reconcile)
                
                if not report["reconciled"] and volguard_system and volguard_system.alert_service:
                    volguard_system.alert_service.send(
                        "Position Mismatch Detected",
                        f"DB: {report['db_positions']}, Broker: {report['broker_positions']}\n"
                        f"Discrepancies: {len(report.get('discrepancies', []))}",
                        AlertPriority.HIGH,
                        throttle_key="position_reconciliation"
                    )
                
                await asyncio.sleep(DynamicConfig.get("POSITION_RECONCILE_INTERVAL_MINUTES") * 60)
            else:
                await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Position reconciliation error: {e}")
            await asyncio.sleep(600)

async def daily_pnl_reconciliation():
    ist_tz = pytz.timezone('Asia/Kolkata')
    logger.info("Daily P&L reconciliation job started")
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pnl")
    
    while True:
        try:
            now = datetime.now(ist_tz)
            if now.time() >= SystemConfig.PNL_RECONCILE_TIME_IST:
                today = now.date()
                
                def sync_pnl_reconcile():
                    with SessionLocal() as db:
                        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
                        if stats and volguard_system:
                            our_pnl = stats.total_pnl or 0.0
                            broker_pnl = volguard_system.fetcher.get_broker_pnl_for_date(today)
                            return stats, our_pnl, broker_pnl
                        return None, None, None
                
                result = await loop.run_in_executor(executor, sync_pnl_reconcile)
                
                if result and result[0] and result[2] is not None:
                    stats, our_pnl, broker_pnl = result
                    discrepancy = round(abs(our_pnl - broker_pnl), 2)
                    
                    def sync_update():
                        with SessionLocal() as db:
                            stats = db.query(DailyStats).filter(DailyStats.date == today).first()
                            if stats:
                                stats.broker_pnl = round(broker_pnl, 2)
                                stats.pnl_discrepancy = discrepancy
                                db.commit()
                    
                    await loop.run_in_executor(executor, sync_update)
                    
                    if discrepancy > DynamicConfig.get("PNL_DISCREPANCY_THRESHOLD") and volguard_system.alert_service:
                        volguard_system.alert_service.send(
                            "P&L Mismatch Detected",
                            f"Our P&L: ₹{our_pnl:,.2f}\n"
                            f"Broker P&L: ₹{broker_pnl:,.2f}\n"
                            f"Difference: ₹{discrepancy:,.2f}",
                            AlertPriority.HIGH,
                            throttle_key="pnl_reconciliation"
                        )
                
                tomorrow = now.date() + timedelta(days=1)
                next_run = datetime.combine(tomorrow, SystemConfig.PNL_RECONCILE_TIME_IST)
                next_run = ist_tz.localize(next_run)
                sleep_seconds = (next_run - now).total_seconds()
                await asyncio.sleep(sleep_seconds)
            else:
                await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Daily P&L reconciliation error: {e}")
            await asyncio.sleep(3600)


# ============================================================================
# AUTOMATED TOKEN REFRESH SCHEDULER
# ============================================================================

async def scheduled_token_refresh():
    """
    Docker-native scheduled task.
    Runs continuously and triggers the Upstox v3 token request at 08:30 IST daily.
    """
    ist_tz = pytz.timezone('Asia/Kolkata')
    logger.info("✅ Token refresh scheduler started (Target: 08:30 AM IST daily)")
    
    while True:
        try:
            now = datetime.now(ist_tz)
            
            # Trigger exactly at 08:30 AM IST
            if now.hour == 8 and now.minute == 30:
                logger.info("⏰ Scheduled Token Refresh Triggered!")
                client_id = os.environ.get("UPSTOX_CLIENT_ID", "")
                client_secret = os.environ.get("UPSTOX_CLIENT_SECRET", "")
                
                if client_id and client_secret:
                    # Hitting the Upstox V3 Token Request endpoint
                    resp = requests.post(
                        f"https://api.upstox.com/v3/login/auth/token/request/{client_id}",
                        headers={"Content-Type": "application/json"},
                        json={"client_secret": client_secret},
                        timeout=15
                    )
                    
                    if resp.status_code == 200:
                        logger.info("✅ Token request sent to Upstox. Waiting for mobile approval...")
                        if volguard_system and volguard_system.alert_service:
                            volguard_system.alert_service.send(
                                "🟡 VolGuard: Upstox Token Renewal",
                                "Open your Upstox app and approve the token request.\nSystem will auto-update once approved.",
                                AlertPriority.HIGH
                            )
                    else:
                        logger.error(f"Failed to request token: {resp.status_code} - {resp.text}")
                
                # Sleep for 61 seconds to prevent double-firing
                await asyncio.sleep(61)
            else:
                await asyncio.sleep(30)
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Token refresh scheduler error: {e}")
            await asyncio.sleep(60)


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 70)
    logger.info("VolGuard Intelligence Edition starting...")
    logger.info("=" * 70)
    
    if not UPSTOX_AVAILABLE:
        raise RuntimeError("Upstox SDK not installed. Cannot start application.")
    
    DynamicConfig.initialize(SessionLocal)
    logger.info(f"Base Capital: ₹{DynamicConfig.get('BASE_CAPITAL'):,.2f}")
    logger.info(f"Auto Trading: {'ENABLED 🔴' if DynamicConfig.get('AUTO_TRADING') else 'DISABLED 🟡'}")
    logger.info(f"Mock Trading: {'ENABLED 🟡' if DynamicConfig.get('ENABLE_MOCK_TRADING') else 'DISABLED'}")
    logger.info(f"GTT Stop Loss Multiplier: {DynamicConfig.get('GTT_STOP_LOSS_MULTIPLIER')}x")
    logger.info(f"GTT Profit Target Multiplier: {DynamicConfig.get('GTT_PROFIT_TARGET_MULTIPLIER')}x  (captures {round((1 - DynamicConfig.get('GTT_PROFIT_TARGET_MULTIPLIER')) * 100)}% premium profit)")
    logger.info(f"GTT Trailing Gap: {DynamicConfig.get('GTT_TRAILING_GAP')}")
    logger.info(f"VoV Thresholds → WARNING:{DynamicConfig.get('VOV_WARNING_ZSCORE')}σ | ELEVATED:{DynamicConfig.get('VOV_MEDIUM_ZSCORE')}σ | DANGER:{DynamicConfig.get('VOV_HEAVY_ZSCORE')}σ | BLOCKED:{DynamicConfig.get('VOV_CRASH_ZSCORE')}σ")
    
    alert_service = None
    if SystemConfig.TELEGRAM_TOKEN and SystemConfig.TELEGRAM_CHAT_ID:
        alert_service = TelegramAlertService(
            SystemConfig.TELEGRAM_TOKEN,
            SystemConfig.TELEGRAM_CHAT_ID
        )
        await alert_service.start()
        alert_service.send(
            "VolGuard Online",
            "Quant Engine + Intelligence Layer Active",
            AlertPriority.SUCCESS
        )
        logger.info("Telegram Alerts Enabled")
    else:
        logger.warning("Telegram credentials not configured - alerts disabled")
    
    global volguard_system
    volguard_system = VolGuardSystem()
    volguard_system.alert_service = alert_service
    
    if hasattr(volguard_system.executor, 'alert_service'):
        volguard_system.executor.alert_service = alert_service
    
    if hasattr(volguard_system.json_cache, '_calendar_engine') and volguard_system.json_cache._calendar_engine:
        volguard_system.json_cache._calendar_engine.set_alert_service(alert_service)
    
    if not volguard_system.json_cache.is_valid_for_today():
        logger.info("Fetching initial daily cache...")
        await asyncio.get_running_loop().run_in_executor(
            None, volguard_system.json_cache.fetch_and_cache, True
        )
    
    volguard_system.start_market_streamer()
    volguard_system.start_portfolio_streamer()
    
    volguard_system.analytics_scheduler = AnalyticsScheduler(volguard_system, volguard_system.analytics_cache)
    analytics_task = asyncio.create_task(volguard_system.analytics_scheduler.start())
    
    cache_task = asyncio.create_task(volguard_system.json_cache.schedule_daily_fetch())
    
    # 1. Start the automated token renewal scheduler
    token_cron_task = asyncio.create_task(scheduled_token_refresh())
    
    if DynamicConfig.get("ENABLE_MOCK_TRADING") or DynamicConfig.get("AUTO_TRADING"):
        volguard_system.monitor = PositionMonitor(
            volguard_system.fetcher, 
            SessionLocal,
            volguard_system.analytics_cache,
            SystemConfig,
            alert_service,
            executor=volguard_system.executor   # pass live or mock executor explicitly
        )
        monitor_task = asyncio.create_task(volguard_system.monitor.start_monitoring())
    
    position_recon_task = asyncio.create_task(position_reconciliation_job())
    pnl_recon_task = asyncio.create_task(daily_pnl_reconciliation())

    # ── INTELLIGENCE LAYER STARTUP ────────────────────────────────────────
    v5_online = False
    try:
        _v5_init_tables()
        if V5_LLM_READY:
            # Initialize singletons (validates API key)
            V5ClaudeClient.get()
            V5MacroCollector.get()
            V5NewsScanner.get()
            # Pre-warm caches in background so first pre-trade gate is instant
            async def _v5_prewarm():
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None, lambda: V5MacroCollector.get().get_snapshot()
                    )
                    await asyncio.get_running_loop().run_in_executor(
                        None, lambda: V5NewsScanner.get().scan()
                    )
                    logger.info("✅ Intelligence caches pre-warmed (macro + news)")
                except Exception as e:
                    logger.warning(f"Intelligence pre-warm error (non-critical): {e}")
            asyncio.create_task(_v5_prewarm())
            # Start background monitor
            V5MonitorAgent.get().start()
            # Schedule morning brief at 08:30 IST daily
            async def _v5_morning_brief_scheduler():
                while True:
                    try:
                        now = datetime.now(IST_TZ)
                        target = now.replace(hour=8, minute=30, second=0, microsecond=0)
                        if now >= target:
                            from datetime import timedelta as _td
                            target += _td(days=1)
                        while target.weekday() >= 5:
                            from datetime import timedelta as _td2
                            target += _td2(days=1)
                        wait = (target - now).total_seconds()
                        logger.info(f"Morning Brief scheduled for {target.strftime('%Y-%m-%d %H:%M IST')} ({wait/3600:.1f}h)")
                        await asyncio.sleep(wait)
                        logger.info("⏰ 08:30 IST — Morning Brief firing")
                        loop = asyncio.get_running_loop()
                        india_vix = None
                        ivp = None
                        fii_net = None
                        upcoming_events = None
                        try:
                            india_vix = volguard_system.fetcher.get_ltp_with_fallback("NSE_INDEX|India VIX")
                        except Exception:
                            pass
                        try:
                            if volguard_system.json_cache:
                                _cache = volguard_system.json_cache.get_today_cache()
                                if _cache and _cache.get("is_valid"):
                                    fii_net = _cache.get("fii_net_change")
                                    _ext = volguard_system.json_cache.get_external_metrics()
                                    upcoming_events = _ext.economic_events if _ext else None
                            # IVP from analytics cache (most recent full analysis)
                            _anal = volguard_system.analytics_cache.get()
                            if _anal and _anal.get("vol_metrics"):
                                ivp = getattr(_anal["vol_metrics"], "ivp_1yr", None)
                        except Exception:
                            pass
                        await loop.run_in_executor(
                            None,
                            lambda: V5MorningBriefAgent.get().run(
                                india_vix=india_vix, ivp=ivp,
                                fii_net=fii_net, upcoming_events=upcoming_events, force=True
                            )
                        )
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.error(f"Morning Brief scheduler error: {e}")
                        await asyncio.sleep(300)

            v5_brief_task = asyncio.create_task(_v5_morning_brief_scheduler())
            v5_online = True
            logger.info("=" * 60)
            logger.info("✅ VolGuard Intelligence Layer ONLINE")
            logger.info("   Morning Brief : 08:30 IST daily (auto-scheduled)")
            logger.info("   Pre-Trade Gate: fires on every execute_strategy call")
            logger.info("   Monitor       : every 30 min during market hours")
            logger.info(f"   LLM Provider  : {_V5_LLM_PROVIDER.upper()} ({'free tier' if _V5_LLM_PROVIDER=='groq' else 'paid'})")
            logger.info("=" * 60)
            if alert_service:
                alert_service.send(
                    "VolGuard Online",
                    "Intelligence layer active. Morning brief at 08:30 IST.",
                    AlertPriority.SUCCESS
                )
        else:
            if not V5_GROQ and not V5_ANTHROPIC:
                logger.warning("⚠️ Intelligence layer offline — pip install groq  (or pip install anthropic)")
            elif not os.getenv("GROQ_API_KEY") and not os.getenv("ANTHROPIC_API_KEY"):
                logger.warning("⚠️ Intelligence layer offline — set GROQ_API_KEY (free) or ANTHROPIC_API_KEY")
    except Exception as v5_startup_err:
        logger.error(f"Intelligence layer startup error (system continues normally): {v5_startup_err}")
    # ── END INTELLIGENCE LAYER STARTUP ────────────────────────────────────────

    yield
    
    logger.info("VolGuard shutting down...")
    if alert_service:
        await alert_service.stop()
    if volguard_system.analytics_scheduler:
        volguard_system.analytics_scheduler.stop()
    if volguard_system.monitor:
        volguard_system.monitor.stop()
    try:
        V5MonitorAgent.get().stop()
    except Exception:
        pass
    if volguard_system.fetcher.market_streamer:
        volguard_system.fetcher.market_streamer.disconnect()
    if volguard_system.fetcher.portfolio_streamer:
        volguard_system.fetcher.portfolio_streamer.disconnect()

    # 2. Cancel ALL background async tasks gracefully to prevent zombie processes
    tasks_to_cancel = [position_recon_task, pnl_recon_task, analytics_task, cache_task, token_cron_task]
    if 'v5_brief_task' in locals():
        tasks_to_cancel.append(v5_brief_task)
    if 'monitor_task' in locals():
        tasks_to_cancel.append(monitor_task)

    for task in tasks_to_cancel:
        if task and not task.done():
            task.cancel()

    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    # Force SQLite WAL checkpoint for clean DB state before exit
    try:
        with SessionLocal() as _db:
            _db.execute(text("PRAGMA wal_checkpoint(FULL)"))
    except Exception as _e:
        logger.warning(f"WAL checkpoint failed on shutdown: {_e}")

    logger.info("VolGuard shutdown complete.")


app = FastAPI(
    title="VolGuard",
    description="Professional Options Trading System - Fully Automated",
    version="6.0.0",
    lifespan=lifespan
)

_ALLOWED_ORIGINS = [
    o.strip() for o in os.getenv("ALLOWED_ORIGINS", "http://localhost,http://localhost:80,http://localhost:5173,http://localhost:3000,http://localhost:8080").split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

volguard_system: Optional[VolGuardSystem] = None


# ============================================================================
# TOKEN REFRESH HELPERS
# ============================================================================
# ============================================================================

def _update_env_token(new_token: str) -> None:
    """Update UPSTOX_ACCESS_TOKEN in the .env file in-place."""
    env_path = ".env"
    if not os.path.exists(env_path):
        logger.warning(".env file not found — token not persisted to disk")
        return
    try:
        with open(env_path, "r") as f:
            lines = f.readlines()
        with open(env_path, "w") as f:
            replaced = False
            for line in lines:
                if line.startswith("UPSTOX_ACCESS_TOKEN="):
                    f.write(f"UPSTOX_ACCESS_TOKEN={new_token}\n")
                    replaced = True
                else:
                    f.write(line)
            if not replaced:
                f.write(f"\nUPSTOX_ACCESS_TOKEN={new_token}\n")
        logger.info("UPSTOX_ACCESS_TOKEN updated in .env file")
    except Exception as e:
        logger.error(f"Failed to persist new token to .env: {e}")



# ============================================================================
# DEMO MODE ENGINE
# ============================================================================
# Set DEMO_MODE=true in .env to inject a realistic mock NIFTY Iron Condor
# into the Live Desk so the system looks alive during demos with no capital.
# Every response that uses demo data carries  "mock": True  so the frontend
# can badge it clearly.  Set DEMO_MODE=false (or remove it) for live trading.
# ============================================================================

_DEMO_MODE: bool = os.environ.get("DEMO_MODE", "false").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# DEMO Iron Condor — grounded in real market data as of March 9, 2026
#
# Market snapshot used for calibration:
#   Spot  : 24,028   (fell ~422 pts from 24,450 on Mar 6 due to FII sell-off)
#   India VIX : 23.36  (jumped from 19.88 — risk-off environment)
#   Monthly expiry : March 27, 2026  (last Thursday of March, NSE calendar)
#   Lot size : 65  (revised by NSE effective Jan 2026)
#
# Strategy: Symmetric Iron Condor, ~17-19 delta short legs, 300-pt wings
#   Short legs placed ~4.3-4.5% OTM — just outside the 1σ expected move band
#   1σ expected move over 18 DTE = ±1,211 pts  →  wings at ±1,028 / ±1,072
#
# Greek calibration (Black-Scholes approximation, ATM IV ≈ 20.5%, put skew +2.5%):
#   Leg-level greeks verified against standard BS IV surface for 18 DTE, VIX 23.36
#
# Attribution story for demo (entered today, 0.6 simulated days, IV +0.15% intraday):
#   Theta earning steadily (+₹530).  IV ticked up slightly, vega costs (-₹293).
#   Net: +₹217 — theta winning, vega mildly against.  Exactly the right story for
#   a demo of a theta-selling system under a slightly elevated VIX environment.
# ─────────────────────────────────────────────────────────────────────────────

_IC_SPOT        = 24028.0               # real spot Mar 9, 2026
_IC_EXPIRY      = "2026-03-27"          # NSE NIFTY monthly expiry
_IC_EXPIRY_TYPE = "MONTHLY"
_IC_ENTRY_TIME  = "2026-03-09T09:32:00"
_IC_LOT_SIZE    = 65                    # revised lot size effective Jan 2026
_IC_LOTS        = 2                     # 2 lots each leg
_IC_QTY         = _IC_LOT_SIZE * _IC_LOTS   # 130 per leg

# ── Leg definitions: (strike, opt_type, action, entry_premium) ───────────────
# Premiums derived from BS model: ATM IV 20.5%, put skew 2.5%, r 6.5%, DTE 18
#   25100 CE  17Δ  →  ₹72.50    25400 CE  8Δ  →  ₹32.00
#   23000 PE  19Δ  →  ₹84.00    22700 PE  10Δ →  ₹49.50
# Wing width: 300 pts each side
_IC_LEGS = [
    (25100, "CE", "SELL", 72.50),   # short call — 17 delta, 1072 pts OTM
    (25400, "CE", "BUY",  32.00),   # long call hedge
    (23000, "PE", "SELL", 84.00),   # short put — 19 delta, 1028 pts OTM (skew premium)
    (22700, "PE", "BUY",  49.50),   # long put hedge
]

# Net credit = (72.50−32.00) + (84.00−49.50) = 40.50 + 34.50 = ₹75.00 per unit
_IC_NET_CREDIT = 75.00
_IC_MAX_PROFIT = round(_IC_NET_CREDIT * _IC_QTY, 2)           # ₹9,750
_IC_MAX_LOSS   = round((300 - _IC_NET_CREDIT) * _IC_QTY, 2)   # ₹29,250 (wing 300 − credit)
_IC_ALLOCATED  = 180000.0

# ── Per-leg Greeks (BS, 18 DTE, VIX 23.36) ───────────────────────────────────
# Format: (strike, opt_type, action): {delta, theta_per_day, vega_per_1pct, gamma_per_unit}
# Theta shown as option's own theta (negative — option loses value daily).
# For the SELLER the P&L theta is positive (we earn this decay).
_IC_LEG_GREEKS = {
    (25100, "CE", "SELL"): {"delta":  0.170, "theta": -6.80, "vega": 18.50, "gamma": 0.00080},
    (25400, "CE", "BUY"):  {"delta":  0.080, "theta": -3.40, "vega": 10.20, "gamma": 0.00040},
    (23000, "PE", "SELL"): {"delta": -0.190, "theta": -7.60, "vega": 19.80, "gamma": 0.00090},
    (22700, "PE", "BUY"):  {"delta": -0.100, "theta": -4.20, "vega": 13.10, "gamma": 0.00050},
}

# ── Portfolio-level net greeks (for 130 qty, sign-correct) ───────────────────
# Net delta  ≈  0.000  (symmetric IC centred on spot — balanced)
# Net theta  = +884 / day  (seller earns: short legs earn more than long legs cost)
# Net vega   = −1950 / 1% IV  (short vol position — IV rise hurts)
# Net gamma  = −0.104  (short gamma — adverse to large moves)
_IC_NET_THETA = +884.0    # ₹ per calendar day earned by the book
_IC_NET_VEGA  = -1950.0   # ₹ per 1% IV change (negative = lose on IV expansion)
_IC_NET_DELTA =  0.0      # near-zero for symmetric IC at entry
_IC_NET_GAMMA = -0.104    # per point² (negligible intraday, matters for big moves)


def _demo_ltp(strike: int, option_type: str, action: str, entry: float) -> float:
    """
    Simulates a realistic live LTP using:
      • Gentle sine oscillation  — mimics bid-ask bouncing (±2% amplitude, 3-min cycle)
      • Steady theta decay drift — premium erodes ~15% over a full session
      • IV-expansion nudge       — slight upward pressure on short legs (VIX 23.36 is elevated)

    For SELL legs decay is good (we collected, premium falls toward us).
    For BUY legs decay is also present (hedge cheapens, small benefit).
    Floor: 10% of entry to avoid implausible near-zero premiums mid-session.
    """
    t = time.time()
    # Oscillation: ±2% of entry, 3-minute period
    oscillation = math.sin(t / 180.0) * (entry * 0.02)
    # Theta drift: erodes up to 15% over a 6.5-hour session (390 min)
    session_progress = min(1.0, (t % 86400) / (390 * 60))
    theta_drift = 1.0 - (session_progress * 0.15)
    # IV nudge: VIX is elevated → slight premium inflation on OTM options
    # Short legs see ~1.5% extra, long legs ~1% extra (VIX 23.36 environment)
    iv_nudge = 1.015 if action == "SELL" else 1.010
    ltp = entry * theta_drift * iv_nudge + oscillation
    return round(max(ltp, entry * 0.10), 2)


def _build_demo_positions() -> dict:
    """
    Build the full /api/live/positions response for demo mode.
    Greeks are computed from the calibrated _IC_LEG_GREEKS table —
    not estimated from moneyness approximations.
    """
    legs_out     = []
    total_pnl    = 0.0
    total_delta  = 0.0
    total_theta  = 0.0
    total_vega   = 0.0
    total_gamma  = 0.0

    for strike, opt_type, action, entry in _IC_LEGS:
        ltp       = _demo_ltp(strike, opt_type, action, entry)
        direction = -1 if action == "SELL" else 1       # SELL = short position
        leg_pnl   = round((ltp - entry) * _IC_QTY * direction, 2)
        total_pnl += leg_pnl

        g = _IC_LEG_GREEKS[(strike, opt_type, action)]
        # Delta and gamma: position direction flips sign for short legs
        total_delta += g["delta"]  * _IC_QTY * direction
        total_gamma += g["gamma"]  * _IC_QTY * direction
        # Theta P&L: option theta is negative; seller's income = −theta × qty
        total_theta += (-g["theta"]) * _IC_QTY * direction
        # Vega P&L exposure: short legs have negative vega exposure (IV up = loss)
        total_vega  += g["vega"]   * _IC_QTY * direction

        legs_out.append({
            "symbol":      f"NIFTY {strike} {opt_type}",
            "action":      action,
            "option_type": opt_type,
            "strike":      strike,
            "qty":         _IC_QTY,
            "entry_price": entry,
            "ltp":         ltp,
            "pnl":         leg_pnl,
        })

    active_strategy = {
        "strategy_id":       "DEMO-IC-001",
        "strategy_type":     "IRON_CONDOR",
        "expiry_type":       _IC_EXPIRY_TYPE,
        "expiry_date":       _IC_EXPIRY,
        "entry_time":        _IC_ENTRY_TIME,
        "max_profit":        _IC_MAX_PROFIT,
        "max_loss":          _IC_MAX_LOSS,
        "net_credit":        _IC_NET_CREDIT,
        "wing_width":        300,
        "allocated_capital": _IC_ALLOCATED,
        "pnl":               round(total_pnl, 2),
        "legs":              legs_out,
        "mock":              True,
    }

    return {
        "mtm_pnl":   round(total_pnl, 2),
        "pnl_color": "GREEN" if total_pnl >= 0 else "RED",
        "spot":      _IC_SPOT,
        "vix":       23.36,
        "greeks": {
            "delta": round(total_delta, 3),
            "theta": round(total_theta, 1),
            "vega":  round(total_vega,  1),
            "gamma": round(total_gamma, 4),
        },
        "positions":         legs_out,
        "active_strategies": [active_strategy],
        "mock":              True,
    }


def _build_demo_attribution() -> dict:
    """
    Build the /api/pnl/attribution response for demo mode.

    Attribution is based on real market conditions:
      • 0.6 simulated trading days elapsed (entered this morning)
      • Intraday IV tick: +0.15%  (VIX elevated at 23.36, small intraday uptick)
      • Spot move contribution: near-zero (net delta ≈ 0 at entry for symmetric IC)
      • Gamma / other drag: small but present (short gamma position)

    Numbers computed from calibrated portfolio greeks:
      theta_pnl = _IC_NET_THETA × 0.6 days           = +530
      vega_pnl  = _IC_NET_VEGA  × 0.15% IV change    = −293
      delta_pnl = _IC_NET_DELTA × small spot move     ≈   0
      other_pnl = gamma + charm residual              ≈ −20
      total                                           = +217

    A gentle oscillation (±3%) is added so the chart feels live, not static.
    """
    t = time.time()
    # Small oscillation so the chart refreshes visibly without lying about the numbers
    wobble = math.sin(t / 300.0) * 0.03   # ±3%

    theta_pnl = round(530.0  * (1.0 + wobble), 2)           # +530 ± 16
    vega_pnl  = round(-293.0 * (1.0 + wobble * 0.5), 2)     # -293 ± 4   (less volatile)
    delta_pnl = round(0.0    + math.sin(t / 200.0) * 12, 2) # ~0, small random walk
    other_pnl = round(-20.0  * (1.0 + wobble * 0.2), 2)     # −20 ± 1
    total_pnl = round(theta_pnl + vega_pnl + delta_pnl + other_pnl, 2)

    # IV change that produced the vega component
    iv_change = round(0.15 + math.sin(t / 500.0) * 0.03, 2)  # ~0.15% ± 0.03%

    return {
        "total_pnl":      total_pnl,
        "theta_pnl":      theta_pnl,
        "vega_pnl":       vega_pnl,
        "delta_pnl":      delta_pnl,
        "other_pnl":      other_pnl,
        "iv_change":      iv_change,
        "days_held":      0.6,
        "spot":           _IC_SPOT,
        "vix":            23.36,
        "net_theta_rate": _IC_NET_THETA,
        "net_vega":       _IC_NET_VEGA,
        "mock":           True,
    }


@app.post("/webhook/upstox-token")
async def receive_upstox_token(payload: dict, x_webhook_secret: Optional[str] = Header(None, alias="X-Webhook-Secret")):
    """
    Upstox Notifier Webhook endpoint.
    Called automatically by Upstox after the user approves the daily token request.

    Payload from Upstox contains: access_token, issued_at, expires_at
    This endpoint hot-swaps the token in ALL 11 live API clients + streamer config
    without a restart.

    Set WEBHOOK_SECRET in .env to require X-Webhook-Secret header authentication.
    """
    # Validate webhook secret — REQUIRED, not optional.
    # If WEBHOOK_SECRET is not set in the environment, every request is rejected to prevent
    # unauthenticated token injection from any actor who knows the webhook URL.
    _expected_secret = os.environ.get("WEBHOOK_SECRET", "")
    if not _expected_secret:
        logger.error(
            "WEBHOOK_SECRET not configured — rejecting token webhook. "
            "Set WEBHOOK_SECRET in .env to enable this endpoint."
        )
        raise HTTPException(status_code=403, detail="Webhook not configured — WEBHOOK_SECRET missing on server")
    if not x_webhook_secret or x_webhook_secret != _expected_secret:
        logger.warning("Webhook token update rejected — invalid or missing X-Webhook-Secret")
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")

    new_token = payload.get("access_token")
    if not new_token:
        logger.error("Token webhook called but 'access_token' missing from payload")
        raise HTTPException(status_code=400, detail="access_token missing from payload")

    logger.info(f"New Upstox token received via webhook (expires: {payload.get('expires_at', 'unknown')})")

    if volguard_system:
        try:
            fetcher = volguard_system.fetcher

            # 1. Update the shared Configuration object in-place so all existing
            #    ApiClient references pick up the new token immediately.
            fetcher.configuration.access_token = new_token
            fetcher.api_client.configuration.access_token = new_token

            # 2. Rebuild a fresh ApiClient with the new token.
            cfg = upstox_client.Configuration()
            cfg.access_token = new_token
            cfg.host = "https://api.upstox.com"
            api_client = upstox_client.ApiClient(cfg)

            # 3. Replace ALL 11 API clients using the EXACT attribute names
            #    defined in UpstoxFetcher.__init__().
            fetcher.quote_api      = upstox_client.MarketQuoteApi(api_client)
            fetcher.quote_api_v3   = upstox_client.MarketQuoteV3Api(api_client)
            fetcher.options_api    = upstox_client.OptionsApi(api_client)
            fetcher.order_api      = upstox_client.OrderApi(api_client)
            fetcher.order_api_v3   = upstox_client.OrderApiV3(api_client)
            fetcher.portfolio_api  = upstox_client.PortfolioApi(api_client)
            fetcher.charge_api     = upstox_client.ChargeApi(api_client)
            fetcher.history_api    = upstox_client.HistoryV3Api(api_client)
            fetcher.user_api       = upstox_client.UserApi(api_client)
            fetcher.pnl_api        = upstox_client.TradeProfitAndLossApi(api_client)
            fetcher.market_api     = upstox_client.MarketHolidaysAndTimingsApi(api_client)

            # 4. Also replace the shared api_client reference so newly
            #    created SmartDataFetcher calls use the new token.
            fetcher.api_client = api_client

            # 5. Trigger a streamer reconnect so WebSocket sessions
            #    re-authenticate with the new token automatically.
            try:
                if fetcher.market_streamer.is_connected:
                    fetcher.market_streamer.streamer.disconnect()
                    logger.info("Market streamer disconnected — will auto-reconnect with new token")
            except Exception as se:
                logger.warning(f"Market streamer reconnect trigger failed (non-fatal): {se}")

            try:
                if fetcher.portfolio_streamer.is_connected:
                    fetcher.portfolio_streamer.streamer.disconnect()
                    logger.info("Portfolio streamer disconnected — will auto-reconnect with new token")
            except Exception as se:
                logger.warning(f"Portfolio streamer reconnect trigger failed (non-fatal): {se}")

            logger.info("✅ All 11 Upstox API clients updated with new token")
        except Exception as e:
            logger.error(f"Failed to hot-swap token in live clients: {e}")
            raise HTTPException(status_code=500, detail=f"Token hot-swap failed: {e}")

    # Also update environment variable in memory + persist to .env
    os.environ["UPSTOX_ACCESS_TOKEN"] = new_token
    SystemConfig.UPSTOX_ACCESS_TOKEN = new_token
    _update_env_token(new_token)

    return {
        "status": "success",
        "message": "Token updated successfully across all 11 API clients. No restart required.",
        "expires_at": payload.get("expires_at")
    }


@app.post("/api/token/refresh-request")
async def trigger_token_refresh_request(token: str = Depends(verify_token)):
    """
    Manually trigger a new token request to Upstox (sends push notification to your phone).
    Use this if the cron job didn't fire or you need to refresh early.
    Upstox will call /webhook/upstox-token automatically after you approve in the app.
    """
    client_id     = os.environ.get("UPSTOX_CLIENT_ID", "")
    client_secret = os.environ.get("UPSTOX_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="UPSTOX_CLIENT_ID / UPSTOX_CLIENT_SECRET not set in environment")

    try:
        resp = requests.post(
            f"https://api.upstox.com/v3/login/auth/token/request/{client_id}",
            headers={"Content-Type": "application/json"},
            json={"client_secret": client_secret},
            timeout=10
        )
        if resp.status_code == 200:
            logger.info("Token refresh request sent to Upstox — check phone to approve")
            return {"status": "success", "message": "Token request sent. Approve in Upstox app. Webhook will auto-update token."}
        else:
            raise HTTPException(status_code=resp.status_code, detail=f"Upstox returned: {resp.text}")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Could not reach Upstox API: {e}")


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/api/fii/summary")
def get_fii_summary(token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")

    ext = volguard_system.json_cache.get_external_metrics()

    participants = {}
    if ext.fii_data:
        for p_key in ["FII", "DII", "Pro", "Client"]:
            p = ext.fii_data.get(p_key)
            if p:
                participants[p_key] = {
                    "fut_long":  round(p.fut_long, 0),
                    "fut_short": round(p.fut_short, 0),
                    "fut_net":   round(p.fut_net, 0),
                    "call_long": round(p.call_long, 0),
                    "call_short":round(p.call_short, 0),
                    "call_net":  round(p.call_net, 0),
                    "put_long":  round(p.put_long, 0),
                    "put_short": round(p.put_short, 0),
                    "put_net":   round(p.put_net, 0),
                    "stock_net": round(p.stock_net, 0),
                    "total_net": round(p.total_net, 0),
                }

    return {
        "data_date": ext.fii_data_date,
        "is_fallback": ext.fii_is_fallback,
        "fii_net_change": ext.fii_net_change,
        "fii_direction": ext.fii_sentiment,
        "fii_conviction": ext.fii_conviction,
        "flow_regime": ext.flow_regime,
        "participants": participants,
        "veto_event_near": ext.veto_event_near,
        "high_impact_event_near": ext.high_impact_event_near,
        "risk_score": ext.risk_score,
    }


@app.post("/api/cache/refresh")
def refresh_daily_cache(token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    success = volguard_system.json_cache.fetch_and_cache(force=True)
    if not success:
        raise HTTPException(status_code=500, detail="Cache refresh failed — check logs")
    cache = volguard_system.json_cache.get_today_cache()
    return {
        "success": True,
        "cache_date": cache.get("cache_date") if cache else None,
        "fetch_timestamp": cache.get("fetch_timestamp") if cache else None,
        "fii_data_date": cache.get("fii_data_date_str") if cache else None,
        "is_fallback": cache.get("fii_is_fallback") if cache else None,
        "events_fetched": len(cache.get("economic_events", [])) if cache else 0,
    }



@app.get("/api/pnl/attribution")
def get_pnl_attribution(db: Session = Depends(get_db), token: str = Depends(verify_token)):
    # ── DEMO MODE ──────────────────────────────────────────────────────────────
    if _DEMO_MODE:
        return _build_demo_attribution()
    # ── END DEMO MODE ──────────────────────────────────────────────────────────
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    active_trades = db.query(TradeJournal).filter(TradeJournal.status == TradeStatus.ACTIVE.value).all()
    if not active_trades:
        return {"total_pnl": 0.0, "theta_pnl": 0.0, "vega_pnl": 0.0, "delta_pnl": 0.0, "other_pnl": 0.0, "iv_change": 0.0}
    
    all_instruments = []
    for trade in active_trades:
        legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
        all_instruments.extend([leg['instrument_token'] for leg in legs_data])
    
    all_instruments = list(set(all_instruments))
    prices = volguard_system.fetcher.get_bulk_ltp_with_fallback(all_instruments)
    greeks = volguard_system.fetcher.get_greeks(all_instruments)
    
    total_pnl = 0.0
    total_theta = 0.0
    total_vega = 0.0
    total_delta = 0.0
    total_other = 0.0
    total_iv_change = 0.0
    trade_count = 0
    
    engine = PnLAttributionEngine(volguard_system.fetcher)
    
    for trade in active_trades:
        attr = engine.calculate(trade, prices, greeks)
        if attr:
            total_pnl += attr.total_pnl
            total_theta += attr.theta_pnl
            total_vega += attr.vega_pnl
            total_delta += attr.delta_pnl
            total_other += attr.other_pnl
            total_iv_change += attr.iv_change
            trade_count += 1
    
    avg_iv_change = total_iv_change / trade_count if trade_count > 0 else 0
    
    return {
        "total_pnl": round(total_pnl, 2),
        "theta_pnl": round(total_theta, 2),
        "vega_pnl": round(total_vega, 2),
        "delta_pnl": round(total_delta, 2),
        "other_pnl": round(total_other, 2),
        "iv_change": round(avg_iv_change, 2)
    }


@app.get("/api/gtt/list")
def list_gtts(db: Session = Depends(get_db), token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    active_trades = db.query(TradeJournal).filter(TradeJournal.status == TradeStatus.ACTIVE.value).all()
    gtts = []
    
    for trade in active_trades:
        if trade.gtt_order_ids:
            gtt_ids = json.loads(trade.gtt_order_ids)
            for gtt_id in gtt_ids:
                legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
                instrument_info = next((leg for leg in legs_data if leg.get('action') == 'SELL'), legs_data[0] if legs_data else None)
                
                gtts.append({
                    "gtt_id": gtt_id,
                    "strategy_id": trade.strategy_id,
                    "instrument_token": instrument_info.get('instrument_token') if instrument_info else None,
                    "trading_symbol": instrument_info.get('instrument_token', '').split('|')[-1] if instrument_info else None,
                    "type": "SINGLE",
                    "status": "ACTIVE"
                })
    
    return {"gtt_orders": gtts}


@app.delete("/api/gtt/{gtt_id}")
def cancel_gtt(gtt_id: str, token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    success = volguard_system.executor.cancel_gtt_orders([gtt_id])
    return {"success": success, "gtt_id": gtt_id}


@app.websocket("/api/ws/subscribe")
async def websocket_subscribe(websocket: WebSocket, token: str = Depends(verify_token)):
    await volguard_system.ws_manager.connect(websocket)
    
    subscribed = set()
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data.get("action") == "subscribe":
                instruments = data.get("instruments", [])
                mode = data.get("mode", "ltpc")
                
                new_instruments = [i for i in instruments if i not in subscribed]
                if new_instruments:
                    success = volguard_system.fetcher.subscribe_market_data(new_instruments, mode)
                    if success:
                        subscribed.update(new_instruments)
                
                await websocket.send_json({
                    "type": "subscription_result",
                    "action": "subscribe",
                    "instruments": instruments,
                    "success": True,
                    "subscribed_count": len(subscribed)
                })
                
            elif data.get("action") == "unsubscribe":
                instruments = data.get("instruments", [])
                
                to_unsubscribe = [i for i in instruments if i in subscribed]
                if to_unsubscribe:
                    success = volguard_system.fetcher.unsubscribe_market_data(to_unsubscribe)
                    if success:
                        for i in to_unsubscribe:
                            subscribed.discard(i)
                
                await websocket.send_json({
                    "type": "subscription_result",
                    "action": "unsubscribe",
                    "instruments": instruments,
                    "success": True,
                    "subscribed_count": len(subscribed)
                })
                
            elif data.get("action") == "change_mode":
                instruments = data.get("instruments", [])
                mode = data.get("mode", "ltpc")
                
                to_change = [i for i in instruments if i in subscribed]
                if to_change:
                    volguard_system.fetcher.market_streamer.change_mode(to_change, mode)
                
                await websocket.send_json({
                    "type": "subscription_result",
                    "action": "change_mode",
                    "instruments": instruments,
                    "mode": mode,
                    "success": True
                })
                
    except WebSocketDisconnect:
        if subscribed:
            volguard_system.fetcher.unsubscribe_market_data(list(subscribed))
        volguard_system.ws_manager.disconnect(websocket)


@app.websocket("/api/ws/market/{instrument_key}")
async def websocket_market_data(websocket: WebSocket, instrument_key: str, token: str = Depends(verify_token)):
    await volguard_system.ws_manager.connect(websocket)
    try:
        volguard_system.fetcher.subscribe_market_data([instrument_key], "ltpc")
        
        while True:
            price = volguard_system.fetcher.get_ltp_with_fallback(instrument_key)
            
            await websocket.send_json({
                "type": "market_update",
                "instrument_key": instrument_key,
                "data": {
                    "ltp": price,
                    "timestamp": datetime.now().isoformat()
                }
            })
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        volguard_system.fetcher.unsubscribe_market_data([instrument_key])
        volguard_system.ws_manager.disconnect(websocket)


@app.websocket("/api/ws/portfolio")
async def websocket_portfolio(websocket: WebSocket, token: str = Depends(verify_token)):
    await volguard_system.ws_manager.connect(websocket)
    try:
        while True:
            # Build LivePosition-shaped data for frontend
            mtm_pnl = 0.0
            positions_list = []
            greeks = {"delta": 0.0, "theta": 0.0, "vega": 0.0, "gamma": 0.0}
            try:
                db = SessionLocal()
                try:
                    active_trades = db.query(TradeJournal).filter(
                        TradeJournal.status == TradeStatus.ACTIVE.value
                    ).all()
                    if active_trades and volguard_system:
                        all_instruments = []
                        for trade in active_trades:
                            legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
                            all_instruments.extend([leg['instrument_token'] for leg in legs_data])
                        unique_instruments = list(set(all_instruments))
                        current_prices = volguard_system.fetcher.get_bulk_ltp_with_fallback(unique_instruments)
                        for trade in active_trades:
                            legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
                            for leg in legs_data:
                                instrument_key = leg['instrument_token']
                                current_price = current_prices.get(instrument_key, leg['entry_price'])
                                qty = leg.get('filled_quantity', leg['quantity'])
                                multiplier = -1 if leg['action'] == 'SELL' else 1
                                leg_pnl = (current_price - leg['entry_price']) * qty * multiplier
                                mtm_pnl += leg_pnl
                                symbol = instrument_key.split("|")[-1] if "|" in instrument_key else instrument_key
                                positions_list.append({
                                    "symbol": symbol,
                                    "qty": qty * (-1 if leg['action'] == 'SELL' else 1),
                                    "ltp": round(current_price, 2) if current_price else 0.0,
                                    "pnl": round(leg_pnl, 2),
                                    "avg_price": round(leg['entry_price'], 2),
                                })
                finally:
                    db.close()
            except Exception as e:
                logger.warning(f"WS portfolio build error: {e}")

            await websocket.send_json({
                "type": "portfolio_update",
                "data": {
                    "mtm_pnl": round(mtm_pnl, 2),
                    "positions": positions_list,
                    "greeks": greeks,
                    "timestamp": datetime.now().isoformat()
                }
            })
            await asyncio.sleep(5)

    except WebSocketDisconnect:
        volguard_system.ws_manager.disconnect(websocket)


@app.get("/api/market/status")
def get_market_status(
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    return volguard_system.fetcher.smart_fetcher.get_market_status()

@app.get("/api/market/last-price/{instrument_key}")
def get_last_price(
    instrument_key: str,
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    price = volguard_system.fetcher.get_ltp_with_fallback(instrument_key)
    market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
    
    return {
        "success": True,
        "instrument_key": instrument_key,
        "last_price": price,
        "timestamp": datetime.now().isoformat(),
        "market_status": market_status,
        "data_source": "smart_fallback"
    }

@app.get("/api/market/bulk-last-price")
def get_bulk_last_price(
    instruments: str = "NSE_INDEX|Nifty 50,NSE_INDEX|India VIX",
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    instrument_list = [i.strip() for i in instruments.split(',')]
    prices = volguard_system.fetcher.get_bulk_ltp_with_fallback(instrument_list)
    market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
    
    return {
        "success": True,
        "prices": prices,
        "timestamp": datetime.now().isoformat(),
        "market_status": market_status,
        "data_source": "smart_fallback"
    }

@app.get("/api/market/ohlc/{instrument_key}")
def get_ohlc(
    instrument_key: str,
    interval: str = "1d",
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    ohlc = volguard_system.fetcher.get_ohlc_with_fallback(instrument_key, interval)
    
    return {
        "success": True,
        "instrument_key": instrument_key,
        "interval": interval,
        "data": ohlc,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/dashboard/analytics", response_model=DashboardAnalyticsResponse)
def get_dashboard_analytics(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    try:
        cached = volguard_system.analytics_cache.get()
        if cached:
            analysis = cached
        else:
            analysis = volguard_system.run_complete_analysis()
        
        market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
        
        market_status_display = {
            "nifty_spot": round(analysis['vol_metrics'].spot, 2) if analysis['vol_metrics'].spot is not None else ("Market Closed" if not market_status['is_open'] else "N/A"),
            "india_vix": round(analysis['vol_metrics'].vix, 2) if analysis['vol_metrics'].vix is not None else ("Market Closed" if not market_status['is_open'] else "N/A"),
            "market_open": market_status['is_open'],
            "message": market_status['message']
        }
        
        primary_mandate = analysis['weekly_mandate']
        if not primary_mandate.is_trade_allowed and analysis['monthly_mandate'].is_trade_allowed:
            primary_mandate = analysis['monthly_mandate']
        
        mandate = {
            "status": "ALLOWED" if primary_mandate.is_trade_allowed else "VETOED",
            "strategy": primary_mandate.suggested_structure if primary_mandate.is_trade_allowed else "CASH",
            "score": round(analysis['weekly_score'].total_score, 1),
            "reason": ", ".join(primary_mandate.veto_reasons) if primary_mandate.veto_reasons else primary_mandate.regime_summary
        }
        
        scores = {
            "volatility": round(analysis['weekly_score'].vol_score, 1),
            "structure": round(analysis['weekly_score'].struct_score, 1),
            "edge": round(analysis['weekly_score'].edge_score, 1)
        }
        
        events = []
        for event in analysis['external_metrics'].economic_events[:5]:
            events.append({
                "name": event.title,
                "type": "VETO" if event.is_veto_event else event.impact_level,
                "time": f"{event.days_until} days" if event.days_until > 0 else "Today"
            })
        
        return {
            "market_status": market_status_display,
            "mandate": mandate,
            "scores": scores,
            "events": events
        }
        
    except Exception as e:
        logger.error(f"Dashboard analytics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dashboard/professional")
def get_professional_dashboard(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    try:
        analytics = volguard_system.analytics_cache.get()
        if not analytics:
            try:
                analytics = volguard_system.run_complete_analysis()
            except Exception as e:
                logger.error(f"Analytics failed: {e}")
                raise HTTPException(status_code=503, detail="Analytics initializing...")
        
        daily_ctx = volguard_system.json_cache.get_context() or {}
        market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
        
        vol = analytics.get('vol_metrics')
        ext = analytics.get('external_metrics')
        
        def fmt_cr(val):
            if val is None:
                return "N/A"
            try:
                return f"₹{float(val)/10000000:+.2f} Cr"
            except (ValueError, TypeError):
                return "N/A"
            
        def fmt_pct(val):
            if val is None:
                return "N/A"
            try:
                return f"{float(val):.2f}%"
            except (ValueError, TypeError):
                return "N/A"

        # FII / DII / Pro / Client participant table
        participant_positions = {
            "data_date": ext.fii_data_date,
            "is_fallback": ext.fii_is_fallback,
            "fii_summary": {
                "direction": ext.fii_sentiment,         # BULLISH / BEARISH / NEUTRAL
                "conviction": ext.fii_conviction,       # VERY_HIGH / HIGH / MODERATE / LOW
                "flow_regime": ext.flow_regime,         # AGGRESSIVE_BEAR / AGGRESSIVE_BULL / GUARDED_BULL / CONTRARIAN_TRAP / NEUTRAL
                "net_change": ext.fii_net_change,
                "net_change_formatted": f"{ext.fii_net_change:+,.0f} contracts" if ext.fii_net_change != 0 else "0 contracts",
            },
            "participants": {}
        }

        # Full participant table — FII, DII, Pro, Client
        if ext.fii_data:
            for p_key in ["FII", "DII", "Pro", "Client"]:
                p = ext.fii_data.get(p_key)
                if p:
                    participant_positions["participants"][p_key] = {
                        "fut_long":   round(p.fut_long, 0),
                        "fut_short":  round(p.fut_short, 0),
                        "fut_net":    round(p.fut_net, 0),
                        "call_net":   round(p.call_net, 0),
                        "put_net":    round(p.put_net, 0),
                        "stock_net":  round(p.stock_net, 0),
                        "total_net":  round(p.total_net, 0),
                    }
        
        warnings = {
            "weekly": [],
            "next_weekly": [],
            "monthly": []
        }
        
        if vol.vov_zscore >= 3.0:
            warning = {
                "type": "VOL_OF_VOL",
                "message": f"VOL-OF-VOL: {vol.vov_zscore:.2f}σ — Market unstable. All trades BLOCKED.",
                "severity": "CRITICAL"
            }
            warnings["weekly"].append(warning)
            warnings["next_weekly"].append(warning)
            warnings["monthly"].append(warning)
        elif vol.vov_zscore >= 2.75:
            warning = {
                "type": "VOL_OF_VOL",
                "message": f"VOL-OF-VOL: {vol.vov_zscore:.2f}σ — DANGER: Highly unstable. Reduce size to 40%.",
                "severity": "DANGER"
            }
            warnings["weekly"].append(warning)
            warnings["next_weekly"].append(warning)
            warnings["monthly"].append(warning)
        elif vol.vov_zscore >= 2.50:
            warning = {
                "type": "VOL_OF_VOL",
                "message": f"VOL-OF-VOL: {vol.vov_zscore:.2f}σ — ELEVATED: Reduce size to 60%.",
                "severity": "HIGH"
            }
            warnings["weekly"].append(warning)
            warnings["next_weekly"].append(warning)
            warnings["monthly"].append(warning)
        elif vol.vov_zscore >= 2.25:
            warning = {
                "type": "VOL_OF_VOL",
                "message": f"VOL-OF-VOL: {vol.vov_zscore:.2f}σ — WARNING: Volatility elevated. Reduce size to 80%.",
                "severity": "MEDIUM"
            }
            warnings["weekly"].append(warning)
            warnings["next_weekly"].append(warning)
            warnings["monthly"].append(warning)
        
        if ext.fii_conviction == "VERY_HIGH" and ext.fii_sentiment == "BEARISH":
            warning = {
                "type": "FII",
                "message": "FII VERY HIGH BEARISH: Heavy selling pressure",
                "severity": "MEDIUM"
            }
            warnings["weekly"].append(warning)
            warnings["next_weekly"].append(warning)
            warnings["monthly"].append(warning)

        # ── DTE 1 expiry warnings — per expiry bucket ────────────────────────
        # When expiry is tomorrow (DTE == 1), push a DANGER banner so the
        # WarningsBanner at the top of the page is loud and visible — not just
        # the strategy card square-off instruction which requires scrolling.
        tm = analytics['time_metrics']
        if tm.dte_weekly == 1:
            warnings["weekly"].append({
                "type": "EXPIRY",
                "message": "WEEKLY EXPIRY TOMORROW — Square off ALL weekly positions by 14:00 IST TODAY",
                "severity": "DANGER"
            })
        if tm.dte_next_weekly == 1:
            warnings["next_weekly"].append({
                "type": "EXPIRY",
                "message": "NEXT WEEKLY EXPIRY TOMORROW — Square off ALL next-weekly positions by 14:00 IST TODAY",
                "severity": "DANGER"
            })
        if tm.dte_monthly == 1:
            warnings["monthly"].append({
                "type": "EXPIRY",
                "message": "MONTHLY EXPIRY TOMORROW — Square off ALL monthly positions by 14:00 IST TODAY",
                "severity": "DANGER"
            })

        return {
            "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat(),
            "market_status": market_status,
            "time_context": {
                "status": market_status['message'],
                "weekly_expiry": {
                    "date": str(analytics['time_metrics'].weekly_exp),
                    "dte": analytics['time_metrics'].dte_weekly,
                    "trading_blocked": analytics['time_metrics'].is_expiry_day_weekly,
                    "square_off_today": analytics['time_metrics'].dte_weekly == 1
                },
                "monthly_expiry": {
                    "date": str(analytics['time_metrics'].monthly_exp),
                    "dte": analytics['time_metrics'].dte_monthly,
                    "trading_blocked": analytics['time_metrics'].is_expiry_day_monthly,
                    "square_off_today": analytics['time_metrics'].dte_monthly == 1
                },
                "next_weekly_expiry": {
                    "date": str(analytics['time_metrics'].next_weekly_exp),
                    "dte": analytics['time_metrics'].dte_next_weekly,
                    "trading_blocked": analytics['time_metrics'].is_expiry_day_next_weekly,
                    "square_off_today": analytics['time_metrics'].dte_next_weekly == 1
                }
            },
            "economic_calendar": {
                "veto_events": [
                    {
                        "event_name": e.title,
                        "time": e.event_date.strftime("%H:%M") if hasattr(e, 'event_date') and e.event_date else "Today",
                        "square_off_by": e.suggested_square_off_time.strftime("%d-%b-%Y 14:00 IST") if e.suggested_square_off_time else "N/A",
                        "action_required": (
                            "SQUARE OFF NOW — Event is today" if e.days_until == 0
                            else "SQUARE OFF TODAY by 14:00 IST — Event tomorrow"  if e.days_until == 1
                            else f"SQUARE OFF by {e.suggested_square_off_time.strftime('%d-%b-%Y')} 14:00 IST — {e.days_until} days to event"
                            if e.suggested_square_off_time else f"Square off before event ({e.days_until} days away)"
                        )
                    }
                    for e in ext.economic_events if e.is_veto_event
                ],
                "other_events": [
                    {
                        "event_name": e.title,
                        "impact": e.impact_level,
                        "days_until": e.days_until
                    }
                    for e in ext.economic_events if not e.is_veto_event
                ][:10] 
            },
            "volatility_analysis": {
                # ── Spot & VIX ──────────────────────────────────────────────
                "spot": vol.spot if vol.spot is not None else None,
                "spot_ma20": vol.ma20 if vol.ma20 is not None else None,
                "vix": vol.vix if vol.vix is not None else None,
                "vix_trend": vol.vix_momentum,          # RISING / FALLING / STABLE
                "vix_change_5d": vol.vix_change_5d,     # e.g. +20.9%
                "vol_regime": vol.vol_regime,            # EXPLODING / RICH / CHEAP / FAIR / MEAN_REVERTING / BREAKOUT_RICH
                "is_fallback": vol.is_fallback,          # True if using historical close instead of live
                # ── IVP ─────────────────────────────────────────────────────
                "ivp_30d": vol.ivp_30d,
                "ivp_90d": vol.ivp_90d,
                "ivp_1y": vol.ivp_1yr,
                # ── Realized Vol ─────────────────────────────────────────────
                "rv_7d": vol.rv7,
                "rv_28d": vol.rv28,
                "rv_90d": vol.rv90,
                # ── Forecasted Vol ───────────────────────────────────────────
                "garch_7d": vol.garch7,
                "garch_28d": vol.garch28,
                # ── Range-Based Vol ──────────────────────────────────────────
                "parkinson_7d": vol.park7,
                "parkinson_28d": vol.park28,
                # ── Vol-of-Vol ───────────────────────────────────────────────
                "vov": vol.vov,
                "vov_zscore": vol.vov_zscore,
                # ── Trend ────────────────────────────────────────────────────
                "trend_strength": vol.trend_strength,
            },
            "participant_positions": participant_positions,
            "structure_analysis": {
                "weekly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_weekly'].net_gex),
                    "weighted_gex_formatted": fmt_cr(analytics['struct_weekly'].gex_weighted * 1_000_000),
                    "gex_regime": analytics['struct_weekly'].gex_regime,
                    "gex_ratio_pct": f"{analytics['struct_weekly'].gex_ratio:.4f}%",
                    "pcr_all": analytics['struct_weekly'].pcr,
                    "pcr_atm": analytics['struct_weekly'].pcr_atm,
                    "max_pain": analytics['struct_weekly'].max_pain,
                    "skew_25d": f"{analytics['struct_weekly'].skew_25d:+.2f}%",
                    "skew_regime": analytics['struct_weekly'].skew_regime
                },
                "next_weekly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_next_weekly'].net_gex),
                    "weighted_gex_formatted": fmt_cr(analytics['struct_next_weekly'].gex_weighted * 1_000_000),
                    "gex_regime": analytics['struct_next_weekly'].gex_regime,
                    "gex_ratio_pct": f"{analytics['struct_next_weekly'].gex_ratio:.4f}%",
                    "pcr_all": analytics['struct_next_weekly'].pcr,
                    "pcr_atm": analytics['struct_next_weekly'].pcr_atm,
                    "max_pain": analytics['struct_next_weekly'].max_pain,
                    "skew_25d": f"{analytics['struct_next_weekly'].skew_25d:+.2f}%",
                    "skew_regime": analytics['struct_next_weekly'].skew_regime
                },
                "monthly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_monthly'].net_gex),
                    "weighted_gex_formatted": fmt_cr(analytics['struct_monthly'].gex_weighted * 1_000_000),
                    "gex_regime": analytics['struct_monthly'].gex_regime,
                    "gex_ratio_pct": f"{analytics['struct_monthly'].gex_ratio:.4f}%",
                    "pcr_all": analytics['struct_monthly'].pcr,
                    "pcr_atm": analytics['struct_monthly'].pcr_atm,
                    "max_pain": analytics['struct_monthly'].max_pain,
                    "skew_25d": f"{analytics['struct_monthly'].skew_25d:+.2f}%",
                    "skew_regime": analytics['struct_monthly'].skew_regime
                }
            },
            "option_edges": {
                "weekly": {
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_weekly),
                    "vrp_vs_rv": fmt_pct(analytics['edge_metrics'].vrp_rv_weekly),
                    "vrp_vs_garch": fmt_pct(analytics['edge_metrics'].vrp_garch_weekly),
                    "vrp_vs_parkinson": fmt_pct(analytics['edge_metrics'].vrp_park_weekly),
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].weighted_vrp_weekly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].weighted_vrp_weekly > 0 else ("FAIR" if analytics['edge_metrics'].weighted_vrp_weekly == 0 else "CHEAP")
                },
                "next_weekly": {
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_next_weekly),
                    "vrp_vs_rv": fmt_pct(analytics['edge_metrics'].vrp_rv_next_weekly),
                    "vrp_vs_garch": fmt_pct(analytics['edge_metrics'].vrp_garch_next_weekly),
                    "vrp_vs_parkinson": fmt_pct(analytics['edge_metrics'].vrp_park_next_weekly),
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].weighted_vrp_next_weekly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].weighted_vrp_next_weekly > 0 else ("FAIR" if analytics['edge_metrics'].weighted_vrp_next_weekly == 0 else "CHEAP")
                },
                "monthly": {
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_monthly),
                    "vrp_vs_rv": fmt_pct(analytics['edge_metrics'].vrp_rv_monthly),
                    "vrp_vs_garch": fmt_pct(analytics['edge_metrics'].vrp_garch_monthly),
                    "vrp_vs_parkinson": fmt_pct(analytics['edge_metrics'].vrp_park_monthly),
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].weighted_vrp_monthly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].weighted_vrp_monthly > 0 else ("FAIR" if analytics['edge_metrics'].weighted_vrp_monthly == 0 else "CHEAP")
                },
                "term_spread_pct": fmt_pct(analytics['edge_metrics'].term_spread_display),
                "primary_edge": analytics['edge_metrics'].term_structure_regime
            },
            "regime_scores": {
                "weekly": {
                    "composite": {
                        "score": round(analytics['weekly_score'].total_score, 2),
                        "confidence": analytics['weekly_score'].confidence
                    },
                    "components": {
                        "volatility": {
                            "score": round(analytics['weekly_score'].vol_score, 2),
                            "weight": f"{analytics['weekly_score'].weights_used.vol_weight*100:.0f}%" if analytics['weekly_score'].weights_used else "40%",
                            "signal": analytics['weekly_score'].vol_signal
                        },
                        "structure": {
                            "score": round(analytics['weekly_score'].struct_score, 2),
                            "weight": f"{analytics['weekly_score'].weights_used.struct_weight*100:.0f}%" if analytics['weekly_score'].weights_used else "30%",
                            "signal": analytics['weekly_score'].struct_signal
                        },
                        "edge": {
                            "score": round(analytics['weekly_score'].edge_score, 2),
                            "weight": f"{analytics['weekly_score'].weights_used.edge_weight*100:.0f}%" if analytics['weekly_score'].weights_used else "30%",
                            "signal": analytics['weekly_score'].edge_signal
                        }
                    },
                    "weight_rationale": analytics['weekly_score'].weight_rationale,
                    "score_stability": f"{analytics['weekly_score'].score_stability:.1%}",
                    "score_drivers": analytics['weekly_score'].score_drivers
                },
                "next_weekly": {
                    "composite": {
                        "score": round(analytics['next_weekly_score'].total_score, 2),
                        "confidence": analytics['next_weekly_score'].confidence
                    },
                    "components": {
                        "volatility": {
                            "score": round(analytics['next_weekly_score'].vol_score, 2),
                            "weight": f"{analytics['next_weekly_score'].weights_used.vol_weight*100:.0f}%" if analytics['next_weekly_score'].weights_used else "40%",
                            "signal": analytics['next_weekly_score'].vol_signal
                        },
                        "structure": {
                            "score": round(analytics['next_weekly_score'].struct_score, 2),
                            "weight": f"{analytics['next_weekly_score'].weights_used.struct_weight*100:.0f}%" if analytics['next_weekly_score'].weights_used else "30%",
                            "signal": analytics['next_weekly_score'].struct_signal
                        },
                        "edge": {
                            "score": round(analytics['next_weekly_score'].edge_score, 2),
                            "weight": f"{analytics['next_weekly_score'].weights_used.edge_weight*100:.0f}%" if analytics['next_weekly_score'].weights_used else "30%",
                            "signal": analytics['next_weekly_score'].edge_signal
                        }
                    },
                    "weight_rationale": analytics['next_weekly_score'].weight_rationale,
                    "score_stability": f"{analytics['next_weekly_score'].score_stability:.1%}",
                    "score_drivers": analytics['next_weekly_score'].score_drivers
                },
                "monthly": {
                    "composite": {
                        "score": round(analytics['monthly_score'].total_score, 2),
                        "confidence": analytics['monthly_score'].confidence
                    },
                    "components": {
                        "volatility": {
                            "score": round(analytics['monthly_score'].vol_score, 2),
                            "weight": f"{analytics['monthly_score'].weights_used.vol_weight*100:.0f}%" if analytics['monthly_score'].weights_used else "40%",
                            "signal": analytics['monthly_score'].vol_signal
                        },
                        "structure": {
                            "score": round(analytics['monthly_score'].struct_score, 2),
                            "weight": f"{analytics['monthly_score'].weights_used.struct_weight*100:.0f}%" if analytics['monthly_score'].weights_used else "30%",
                            "signal": analytics['monthly_score'].struct_signal
                        },
                        "edge": {
                            "score": round(analytics['monthly_score'].edge_score, 2),
                            "weight": f"{analytics['monthly_score'].weights_used.edge_weight*100:.0f}%" if analytics['monthly_score'].weights_used else "30%",
                            "signal": analytics['monthly_score'].edge_signal
                        }
                    },
                    "weight_rationale": analytics['monthly_score'].weight_rationale,
                    "score_stability": f"{analytics['monthly_score'].score_stability:.1%}",
                    "score_drivers": analytics['monthly_score'].score_drivers
                }
            },
            "mandates": {
                "weekly": {
                    "trade_status": "ALLOWED" if analytics['weekly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['weekly_mandate'].suggested_structure,
                    "regime_name": analytics['weekly_mandate'].regime_name,
                    "directional_bias": analytics['weekly_mandate'].directional_bias,
                    "wing_protection": analytics['weekly_mandate'].wing_protection,
                    "square_off_instruction": analytics['weekly_mandate'].square_off_instruction,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['weekly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.get("WEEKLY_ALLOCATION_PCT")
                    },
                    "rationale": analytics['weekly_score'].score_drivers,
                    "veto_reasons": analytics['weekly_mandate'].veto_reasons,
                    "warnings": warnings["weekly"]
                },
                "next_weekly": {
                    "trade_status": "ALLOWED" if analytics['next_weekly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['next_weekly_mandate'].suggested_structure,
                    "regime_name": analytics['next_weekly_mandate'].regime_name,
                    "directional_bias": analytics['next_weekly_mandate'].directional_bias,
                    "wing_protection": analytics['next_weekly_mandate'].wing_protection,
                    "square_off_instruction": analytics['next_weekly_mandate'].square_off_instruction,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['next_weekly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.get("NEXT_WEEKLY_ALLOCATION_PCT")
                    },
                    "rationale": analytics['next_weekly_score'].score_drivers,
                    "veto_reasons": analytics['next_weekly_mandate'].veto_reasons,
                    "warnings": warnings["next_weekly"]
                },
                "monthly": {
                    "trade_status": "ALLOWED" if analytics['monthly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['monthly_mandate'].suggested_structure,
                    "regime_name": analytics['monthly_mandate'].regime_name,
                    "directional_bias": analytics['monthly_mandate'].directional_bias,
                    "wing_protection": analytics['monthly_mandate'].wing_protection,
                    "square_off_instruction": analytics['monthly_mandate'].square_off_instruction,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['monthly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.get("MONTHLY_ALLOCATION_PCT")
                    },
                    "rationale": analytics['monthly_score'].score_drivers,
                    "veto_reasons": analytics['monthly_mandate'].veto_reasons,
                    "warnings": warnings["monthly"]
                }
            },
            "professional_recommendation": analytics.get('professional_recommendation', {
                "primary": {
                    "expiry_type": "NONE",
                    "strategy": "CASH",
                    "capital_deploy_formatted": "₹0"
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Professional dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/live/positions", response_model=LivePositionsResponse)
def get_live_positions(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    # ── DEMO MODE ──────────────────────────────────────────────────────────────
    if _DEMO_MODE:
        return _build_demo_positions()
    # ── END DEMO MODE ──────────────────────────────────────────────────────────
    try:
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        if not active_trades:
            return {
                "mtm_pnl": 0.0,
                "pnl_color": "GRAY",
                "greeks": {
                    "delta": 0.0,
                    "theta": 0.0,
                    "vega": 0.0,
                    "gamma": 0.0
                },
                "positions": []
            }
        
        all_instruments = []
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
            all_instruments.extend([leg['instrument_token'] for leg in legs_data])
        unique_instruments = list(set(all_instruments))

        current_prices = volguard_system.fetcher.get_bulk_ltp_with_fallback(unique_instruments)
        market_status = volguard_system.fetcher.smart_fetcher.get_market_status()

        # Live greeks per instrument
        live_greeks_map = {}
        if unique_instruments:
            try:
                live_greeks_map = volguard_system.fetcher.get_greeks(unique_instruments)
            except Exception as ge:
                logger.warning(f"Live greeks fetch failed, falling back to entry snapshot: {ge}")

        total_mtm_pnl = 0.0
        positions_list = []
        total_delta = 0.0
        total_theta = 0.0
        total_vega = 0.0
        total_gamma = 0.0

        for trade in active_trades:
            legs_data = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
            trade_pnl = 0.0

            for leg in legs_data:
                instrument_key = leg['instrument_token']
                current_price = current_prices.get(instrument_key, leg['entry_price'])
                qty = leg.get('filled_quantity', leg['quantity'])
                multiplier = -1 if leg['action'] == 'SELL' else 1

                leg_pnl = (current_price - leg['entry_price']) * qty * multiplier
                trade_pnl += leg_pnl

                symbol = instrument_key.split("|")[-1] if "|" in instrument_key else instrument_key

                if instrument_key in live_greeks_map:
                    lg = live_greeks_map[instrument_key]
                else:
                    lg = {}
                    if trade.entry_greeks_snapshot:
                        try:
                            eg = json.loads(trade.entry_greeks_snapshot)
                            lg = eg.get(instrument_key, {})
                        except Exception:
                            pass

                leg_delta = lg.get('delta', 0) * qty * multiplier
                leg_theta = lg.get('theta', 0) * qty * multiplier
                leg_vega  = lg.get('vega', 0)  * qty * multiplier
                leg_gamma = lg.get('gamma', 0) * qty * multiplier
                total_delta += leg_delta
                total_theta += leg_theta
                total_vega  += leg_vega
                total_gamma += leg_gamma

                positions_list.append({
                    "symbol": symbol,
                    "qty": qty * (-1 if leg['action'] == 'SELL' else 1),
                    "ltp": round(current_price, 2) if current_price else None,
                    "pnl": round(leg_pnl, 2),
                    "avg_price": round(leg['entry_price'], 2),
                    "iv": round(lg.get('iv', 0) * 100, 2),  # as percentage
                    "delta": round(leg_delta, 3),
                    "theta": round(leg_theta, 2),
                    "vega": round(leg_vega, 2),
                })

            total_mtm_pnl += trade_pnl

        theta_vega_ratio = round(total_theta / total_vega, 3) if abs(total_vega) > 0.01 else 0.0

        if total_mtm_pnl > 0:
            pnl_color = "GREEN"
        elif total_mtm_pnl < 0:
            pnl_color = "RED"
        else:
            pnl_color = "GRAY"

        straddle_info = None
        if active_trades:
            try:
                primary_trade = active_trades[0]
                expiry_date = primary_trade.expiry_date.date() if hasattr(primary_trade.expiry_date, 'date') else primary_trade.expiry_date
                chain = volguard_system.fetcher.chain(expiry_date)
                spot = volguard_system.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY) or 0
                if chain is not None and not chain.empty and spot > 0:
                    atm_strike = min(chain['strike'].values, key=lambda x: abs(x - spot))
                    atm_row = chain[chain['strike'] == atm_strike].iloc[0]
                    atm_straddle = round(atm_row['ce_ltp'] + atm_row['pe_ltp'], 2)
                    straddle_info = {
                        "atm_strike": atm_strike,
                        "atm_straddle_price": atm_straddle,
                        "expected_move_upper": round(spot + atm_straddle, 2),
                        "expected_move_lower": round(spot - atm_straddle, 2),
                        "expiry": str(expiry_date)
                    }
            except Exception as se:
                logger.warning(f"Straddle info fetch failed: {se}")

        return {
            "mtm_pnl": round(total_mtm_pnl, 2),
            "pnl_color": pnl_color,
            "greeks": {
                "delta": round(total_delta, 2),
                "theta": round(total_theta, 2),
                "vega": round(total_vega, 2),
                "gamma": round(total_gamma, 2),
                "theta_vega_ratio": theta_vega_ratio
            },
            "positions": positions_list,
            "market_status": market_status,
            "straddle_info": straddle_info,
            "active_strategies": [
                {
                    "strategy_id": t.strategy_id,
                    "strategy_type": t.strategy_type,
                    "expiry_type": t.expiry_type,
                    "expiry_date": str(t.expiry_date),
                    "entry_time": t.entry_time.isoformat() if t.entry_time else None,
                    "max_profit": t.max_profit,
                    "max_loss": t.max_loss,
                    "allocated_capital": t.allocated_capital,
                    "pnl": round(sum(
                        (current_prices.get(leg['instrument_token'], leg['entry_price']) - leg['entry_price'])
                        * leg.get('filled_quantity', leg['quantity'])
                        * (-1 if leg['action'] == 'SELL' else 1)
                        for leg in json.loads(t.legs_data)
                    ), 2),
                    "legs": [
                        {
                            "symbol": leg['instrument_token'].split("|")[-1] if "|" in leg['instrument_token'] else leg['instrument_token'],
                            "action": leg['action'],
                            "option_type": leg.get('option_type', ''),
                            "strike": leg.get('strike', 0),
                            "qty": leg.get('filled_quantity', leg['quantity']),
                            "entry_price": round(leg['entry_price'], 2),
                            "ltp": round(current_prices.get(leg['instrument_token'], leg['entry_price']), 2),
                            "pnl": round(
                                (current_prices.get(leg['instrument_token'], leg['entry_price']) - leg['entry_price'])
                                * leg.get('filled_quantity', leg['quantity'])
                                * (-1 if leg['action'] == 'SELL' else 1), 2
                            ),
                        }
                        for leg in json.loads(t.legs_data)
                    ]
                }
                for t in active_trades
            ]
        }
        
    except Exception as e:
        logger.error(f"Live positions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/journal/history", response_model=List[TradeJournalEntry])
def get_journal_history(
    limit: int = 50,
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    try:
        trades = db.query(TradeJournal).filter(
            TradeJournal.status != TradeStatus.ACTIVE.value
        ).order_by(desc(TradeJournal.exit_time)).limit(limit).all()
        
        history = []
        for trade in trades:
            if trade.realized_pnl and trade.realized_pnl > 0:
                result = "WIN"
            elif trade.realized_pnl and trade.realized_pnl < 0:
                result = "LOSS"
            else:
                result = "BREAKEVEN"
            
            history.append({
                "id": str(trade.id),
                "date": trade.exit_time.strftime("%Y-%m-%d") if trade.exit_time else trade.entry_time.strftime("%Y-%m-%d"),
                "strategy": trade.strategy_type,
                "entry": str(round(trade.entry_premium or 0, 2)) if trade.entry_premium is not None else None,
                "exit": str(round(trade.exit_premium or 0, 2)) if trade.exit_premium is not None else None,
                "expiry_type": trade.expiry_type or "UNKNOWN",
                "result": result,
                "pnl": round(trade.realized_pnl or 0, 2),
                "exit_reason": trade.exit_reason or "UNKNOWN",
                "is_mock": bool(trade.is_mock),
                "trade_outcome_class": trade.trade_outcome_class or "UNCLASSIFIED",
            })
        
        return history
        
    except Exception as e:
        logger.error(f"Journal history error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/system/config")
def update_system_config(
    config_update: ConfigUpdateRequest,
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    try:
        updates = {}
        if config_update.max_loss is not None:
            updates["MAX_LOSS_PCT"] = config_update.max_loss
        if config_update.profit_target is not None:
            updates["PROFIT_TARGET"] = config_update.profit_target
        if config_update.base_capital is not None:
            updates["BASE_CAPITAL"] = config_update.base_capital
        if config_update.auto_trading is not None:
            updates["AUTO_TRADING"] = config_update.auto_trading
        if config_update.min_oi is not None:
            updates["MIN_OI"] = config_update.min_oi
        if config_update.max_spread_pct is not None:
            updates["MAX_BID_ASK_SPREAD_PCT"] = config_update.max_spread_pct
        if config_update.max_position_risk_pct is not None:
            updates["MAX_POSITION_RISK_PCT"] = config_update.max_position_risk_pct
        if config_update.max_concurrent_same_strategy is not None:
            updates["MAX_CONCURRENT_SAME_STRATEGY"] = config_update.max_concurrent_same_strategy
        if config_update.gtt_stop_loss_multiplier is not None:
            updates["GTT_STOP_LOSS_MULTIPLIER"] = config_update.gtt_stop_loss_multiplier
        if config_update.gtt_profit_target_multiplier is not None:
            updates["GTT_PROFIT_TARGET_MULTIPLIER"] = config_update.gtt_profit_target_multiplier
        if config_update.gtt_trailing_gap is not None:
            updates["GTT_TRAILING_GAP"] = config_update.gtt_trailing_gap
        
        changed = DynamicConfig.update(updates)
        
        logger.info(f"Configuration updated via API: {changed}")
        
        if "AUTO_TRADING" in changed and volguard_system and volguard_system.alert_service:
            status = "ENABLED" if changed["AUTO_TRADING"] else "DISABLED"
            volguard_system.alert_service.send(
                "Auto Trading Toggled",
                f"Auto trading has been {status} via system config",
                AlertPriority.HIGH if changed["AUTO_TRADING"] else AlertPriority.MEDIUM
            )
        
        return {
            "success": True,
            "updated": changed,
            "current_config": DynamicConfig.to_dict()
        }
        
    except Exception as e:
        logger.error(f"Config update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/system/logs", response_model=SystemLogsResponse)
def get_system_logs(
    lines: int = 50,
    level: Optional[str] = None,
    token: str = Depends(verify_token)
):
    try:
        logs = log_buffer.get_logs(lines=lines, level=level)
        return {
            "logs": logs,
            "total_lines": len(logs)
        }
    except Exception as e:
        logger.error(f"Logs fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/system/config/current")
def get_current_config(
    token: str = Depends(verify_token)
):
    return DynamicConfig.to_dict()

@app.get("/api/risk/correlation-report")
def get_correlation_report(
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    report = volguard_system.correlation_manager.get_correlation_report()
    
    return {
        "timestamp": datetime.now().isoformat(),
        "report": report
    }

@app.get("/api/risk/expiries")
def get_expiry_status(
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.date()
    
    weekly, monthly, next_weekly, lot_size, all_expiries = volguard_system.fetcher.get_expiries()
    
    return {
        "timestamp": now.isoformat(),
        "expiries": {
            "weekly": {
                "date": weekly.isoformat() if weekly else None,
                "dte": (weekly - today).days if weekly else None,
                "trading_blocked": (weekly == today) if weekly else False,
                "square_off_required": (weekly - today).days == 1 if weekly else False,
                "square_off_time": "14:00 IST"
            },
            "monthly": {
                "date": monthly.isoformat() if monthly else None,
                "dte": (monthly - today).days if monthly else None,
                "trading_blocked": (monthly == today) if monthly else False,
                "square_off_required": (monthly - today).days == 1 if monthly else False,
                "square_off_time": "14:00 IST"
            },
            "next_weekly": {
                "date": next_weekly.isoformat() if next_weekly else None,
                "dte": (next_weekly - today).days if next_weekly else None,
                "trading_blocked": (next_weekly == today) if next_weekly else False,
                "square_off_required": (next_weekly - today).days == 1 if next_weekly else False,
                "square_off_time": "14:00 IST"
            }
        },
        "all_expiries": [e.isoformat() for e in all_expiries]
    }

@app.get("/api/capital/structure")
def get_capital_structure(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    """
    Capital structure breakdown — shows how every rupee of capital is working.
    Designed around the G-Sec pledge model: capital earns bond yield even when
    the system says CASH. Fully dynamic — updates with BASE_CAPITAL config changes
    and live position deployment.
    """
    base_capital = DynamicConfig.get("BASE_CAPITAL")
    haircut_pct  = 10.0   # NSE current haircut on G-Secs pledged as margin
    gsec_yield   = 6.7    # Current approximate G-Sec yield p.a.
    reserve_pct  = 20.0   # Hard reserve — untouchable for adjustments/emergencies

    available_margin = round(base_capital * (1 - haircut_pct / 100), 2)
    annual_yield_inr  = round(base_capital * gsec_yield / 100, 2)
    monthly_yield_inr = round(annual_yield_inr / 12, 2)
    daily_yield_inr   = round(annual_yield_inr / 365, 2)
    hard_reserve      = round(base_capital * reserve_pct / 100, 2)

    # Live deployed capital from active positions
    active_trades = db.query(TradeJournal).filter(
        TradeJournal.status == TradeStatus.ACTIVE.value
    ).all()
    deployed = round(sum(t.allocated_capital or 0 for t in active_trades), 2)
    active_count = len(active_trades)

    # Available to deploy = available_margin - hard_reserve - deployed
    available_to_deploy = round(max(0.0, available_margin - hard_reserve - deployed), 2)
    deployment_utilization = round((deployed / available_margin * 100), 1) if available_margin > 0 else 0.0

    # Capital efficiency: every rupee is working even when not trading
    # Bond yield accrues daily regardless of trading activity
    idle_capital = round(available_margin - deployed, 2)  # not deployed but still earning yield
    idle_yield_monthly = round(idle_capital * gsec_yield / 100 / 12, 2)

    return {
        "base_capital": base_capital,
        "haircut_pct": haircut_pct,
        "available_margin": available_margin,
        "gsec_yield_pct": gsec_yield,
        "annual_yield_inr": annual_yield_inr,
        "monthly_yield_inr": monthly_yield_inr,
        "daily_yield_inr": daily_yield_inr,
        "hard_reserve_pct": reserve_pct,
        "hard_reserve": hard_reserve,
        "deployed_capital": deployed,
        "active_positions": active_count,
        "available_to_deploy": available_to_deploy,
        "deployment_utilization_pct": deployment_utilization,
        "idle_capital": idle_capital,
        "idle_yield_monthly": idle_yield_monthly,
        "is_mock": not DynamicConfig.get("AUTO_TRADING"),
        "note": "Capital pledged as G-Sec collateral earns bond yield continuously. System deploys selectively — idle capital is never truly idle.",
    }


@app.post("/api/emergency/exit-all")
def emergency_exit_all(token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    result = volguard_system.fetcher.emergency_exit_all_positions()
    if result["success"] and volguard_system.alert_service:
        volguard_system.alert_service.send(
            "EMERGENCY EXIT",
            f"Orders: {result['orders_placed']}",
            AlertPriority.CRITICAL,
            throttle_key="emergency_exit"
        )
    return result

@app.get("/")
def root():
    return {
        "system": "VolGuard Intelligence Edition",
        "version": "6.0.0",
        "status": "operational",
        "trading_mode": "OVERNIGHT OPTION SELLING",
        "product_type": "D (Delivery/Carryforward)",
        "square_off": "1 day before expiry @ 14:00 IST",
        "data_source": "Smart Fallback (WebSocket + REST)",
        "auto_trading": "FULLY AUTOMATED",
        "gtt_config": {
            "stop_loss_multiplier": DynamicConfig.get("GTT_STOP_LOSS_MULTIPLIER"),
            "profit_target_multiplier": DynamicConfig.get("GTT_PROFIT_TARGET_MULTIPLIER"),
            "trailing_gap": DynamicConfig.get("GTT_TRAILING_GAP")
        },
        "websocket": {
            "market_streamer": "ACTIVE" if volguard_system and volguard_system.market_streamer_started else "INACTIVE",
            "portfolio_streamer": "ACTIVE" if volguard_system and volguard_system.portfolio_streamer_started else "INACTIVE"
        },
        "endpoints": {
            "market_status": "/api/market/status",
            "last_price": "/api/market/last-price/{instrument_key}",
            "bulk_prices": "/api/market/bulk-last-price",
            "ohlc": "/api/market/ohlc/{instrument_key}",
            "analytics": "/api/dashboard/analytics",
            "professional": "/api/dashboard/professional",
            "live": "/api/live/positions",
            "journal": "/api/journal/history",
            "config": "/api/system/config",
            "logs": "/api/system/logs",
            "correlation": "/api/risk/correlation-report",
            "expiries": "/api/risk/expiries",
            "v5_intelligence": {
                "brief": "/api/intelligence/brief",
                "global_tone": "/api/intelligence/global-tone",
                "macro_snapshot": "/api/intelligence/macro-snapshot",
                "news": "/api/intelligence/news",
                "veto_log": "/api/intelligence/veto-log",
                "alerts": "/api/intelligence/alerts",
                "v5_status": "/api/v5/status"
            },
            "fii_summary": "/api/fii/summary",
            "fill_quality": "/api/orders/fill-quality",
            "pnl_attribution": "/api/pnl/attribution",
            "capital_structure": "/api/capital/structure",
            "gtt_list": "/api/gtt/list",
            "websocket": {
                "market": "/api/ws/market/{instrument_key}",
                "portfolio": "/api/ws/portfolio",
                "subscribe": "/api/ws/subscribe"
            }
        }
    }


# ============================================================================
# INTELLIGENCE API ENDPOINTS
# All under /api/intelligence/*
# ============================================================================

class V5VetoOverrideRequest(BaseModel):
    reason: str

class V5ForceEvalRequest(BaseModel):
    expiry_type: str = "WEEKLY"


@app.get("/api/intelligence/brief")
def v5_get_brief(token: str = Depends(verify_token)):
    """Latest morning intelligence brief. Generated at 08:30 IST."""
    return V5MorningBriefAgent.get().get_latest_dict()


@app.post("/api/intelligence/brief/generate")
async def v5_generate_brief(
    background_tasks: BackgroundTasks,
    token: str = Depends(verify_token),
):
    """Force-generate morning brief on demand."""
    def _run():
        india_vix = None
        ivp = None
        fii_net = None
        upcoming_events = None
        try:
            global volguard_system
            if volguard_system:
                india_vix = volguard_system.fetcher.get_ltp_with_fallback("NSE_INDEX|India VIX")
                if volguard_system.json_cache:
                    _cache = volguard_system.json_cache.get_today_cache()
                    if _cache and _cache.get("is_valid"):
                        fii_net = _cache.get("fii_net_change")
                        _ext = volguard_system.json_cache.get_external_metrics()
                        upcoming_events = _ext.economic_events if _ext else None
                _anal = volguard_system.analytics_cache.get()
                if _anal and _anal.get("vol_metrics"):
                    ivp = getattr(_anal["vol_metrics"], "ivp_1yr", None)
        except Exception:
            pass
        V5MorningBriefAgent.get().run(
            india_vix=india_vix, ivp=ivp,
            fii_net=fii_net, upcoming_events=upcoming_events, force=True
        )
    background_tasks.add_task(_run)
    return {"status": "generating", "message": "Brief generation started. Check /api/intelligence/brief in ~20s."}


@app.get("/api/intelligence/global-tone")
def v5_global_tone(token: str = Depends(verify_token)):
    """Lightweight — just the global tone. Used by HUD bar."""
    brief = V5MorningBriefAgent.get().get_latest()
    if brief:
        agent = V5MorningBriefAgent.get()
        return {
            "global_tone": brief.global_tone,
            "source": "morning_brief",
            "volguard_implication": brief.volguard_implication,
            "generated_at": agent._latest_time.isoformat() if agent._latest_time else None,
        }
    snap = V5MacroCollector.get().get_snapshot()
    return {
        "global_tone": snap.global_tone,
        "source": "macro_data_only",
        "risk_off_signals": snap.risk_off_signals,
        "risk_on_signals": snap.risk_on_signals,
        "timestamp": snap.timestamp,
    }


@app.get("/api/intelligence/macro-snapshot")
def v5_macro_snapshot(token: str = Depends(verify_token)):
    """Full live cross-asset data snapshot — flat format for frontend."""
    snap = V5MacroCollector.get().get_snapshot()
    assets = snap.assets if hasattr(snap, 'assets') else {}

    def _price(key):
        a = getattr(snap, key, None)
        return round(a.price, 4) if a and a.price is not None else None

    def _dir(key):
        a = getattr(snap, key, None)
        return a.direction if a else None

    def _chg(key):
        a = getattr(snap, key, None)
        return round(a.change_pct, 4) if a and a.change_pct is not None else None

    dxy = getattr(snap, 'dxy', None)
    us10y = getattr(snap, 'us_10y_yield', None)
    us_vix = getattr(snap, 'us_vix', None)
    crude_wti = getattr(snap, 'crude_wti', None)
    gold = getattr(snap, 'gold', None)
    bitcoin = getattr(snap, 'bitcoin', None)
    nifty_prev = getattr(snap, 'nifty_prev', None)

    return {
        "timestamp": snap.timestamp,
        "global_tone": snap.global_tone,
        "risk_off_signals": snap.risk_off_signals,
        "risk_on_signals": snap.risk_on_signals,
        "us_10y_elevated": snap.us_10y_elevated,
        # Flat fields for HUD
        "dxy_level": dxy.price if dxy and dxy.price else None,
        "dxy_direction": dxy.direction if dxy else None,
        "dxy_change_pct": dxy.change_pct if dxy and dxy.change_pct else None,
        "us_10y_yield": us10y.price if us10y and us10y.price else None,
        "vix_futures": us_vix.price if us_vix and us_vix.price else None,
        "vix_sentiment": (
            "HIGH" if (us_vix and us_vix.price and us_vix.price > 20) else
            "MODERATE" if (us_vix and us_vix.price and us_vix.price > 15) else
            "LOW"
        ) if us_vix else None,
        "crude_price": crude_wti.price if crude_wti and crude_wti.price else None,
        "crude_direction": crude_wti.direction if crude_wti else None,
        "gold_price": gold.price if gold and gold.price else None,
        "gold_sentiment": (
            "RISK_OFF" if (gold and gold.change_pct and gold.change_pct > 0.5) else
            "NEUTRAL"
        ) if gold else None,
        "btc_price": bitcoin.price if bitcoin and bitcoin.price else None,
        "btc_direction": bitcoin.direction if bitcoin else None,
        "sgx_nifty": nifty_prev.price if nifty_prev and nifty_prev.price else None,
        "sgx_signal": (
            "BULLISH" if (nifty_prev and nifty_prev.change_pct and nifty_prev.change_pct > 0) else
            "BEARISH"
        ) if nifty_prev else None,
        # Full nested assets for advanced use
        "assets": snap.to_dict().get("assets", {}),
    }


@app.get("/api/intelligence/news")
def v5_news(token: str = Depends(verify_token)):
    """Current RSS news scan result."""
    result = V5NewsScanner.get().scan()
    return {
        "timestamp": result.timestamp,
        "has_veto": result.has_veto,
        "has_high_impact": result.has_high_impact,
        "veto_items": [
            {"title": i.title, "source": i.source, "keywords": i.matched_keywords}
            for i in result.veto_items
        ],
        "high_impact_items": [
            {"title": i.title, "source": i.source, "keywords": i.matched_keywords}
            for i in result.high_impact_items
        ],
        "watch_items": [
            {"title": i.title, "source": i.source}
            for i in result.watch_items[:8]
        ],
        "total_scanned": result.total_scanned,
        "fetch_errors": result.fetch_errors,
    }


@app.get("/api/intelligence/veto-log")
def v5_veto_log(limit: int = 20, token: str = Depends(verify_token)):
    """Historical pre-trade evaluation decisions."""
    agent = V5PreTradeAgent.get()
    return {
        "veto_log": agent.get_log(limit=limit),
        "total_in_memory": len(agent._veto_log),
    }


@app.post("/api/intelligence/override")
def v5_override_veto(
    request: V5VetoOverrideRequest,
    token: str = Depends(verify_token),
):
    """Override the most recent VETO. Requires reason. Logged permanently."""
    if not request.reason or len(request.reason.strip()) < 10:
        raise HTTPException(
            status_code=400,
            detail="Override reason must be at least 10 characters. You are accountable for this."
        )
    result = V5PreTradeAgent.get().override_veto(reason=request.reason.strip())
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@app.get("/api/intelligence/alerts")
def v5_alerts(limit: int = 10, token: str = Depends(verify_token)):
    """Active context alerts from the monitor agent."""
    return {
        "alerts": V5MonitorAgent.get().get_alerts(limit=limit),
        "timestamp": datetime.now(IST_TZ).isoformat(),
    }


@app.post("/api/intelligence/monitor/scan")
def v5_force_scan(token: str = Depends(verify_token)):
    """Manually trigger a monitor scan."""
    return V5MonitorAgent.get().force_scan()


@app.get("/api/intelligence/claude-usage")
def v5_claude_usage(token: str = Depends(verify_token)):
    """Claude API usage and estimated cost."""
    client = V5ClaudeClient.get()
    if client is None:
        return {"error": f"LLM client not initialized. Provider: {_V5_LLM_PROVIDER}. Set GROQ_API_KEY or ANTHROPIC_API_KEY."}
    return client.usage()


@app.get("/api/v5/status")
def v5_status():
    """Intelligence layer status."""
    anthropic_configured = V5_LLM_READY  # True for Groq or Claude
    brief_agent = V5MorningBriefAgent.get()
    brief = brief_agent.get_latest()
    return {
        "system": "VolGuard",
        "version": "6.0.0",
        "intelligence_layer": "ONLINE" if anthropic_configured else "OFFLINE — set ANTHROPIC_API_KEY",
        "dependencies": {
            "anthropic":    V5_ANTHROPIC,
            "groq":         V5_GROQ,
            "twelve_data":  V5_TWELVEDATA,
            "fred":         V5_FRED,
            "feedparser":   V5_FEEDPARSER,
            "coingecko":    True,  # no key needed
        },
        "morning_brief": {
            "status": "AVAILABLE" if brief else "NOT_YET_GENERATED",
            "global_tone": brief.global_tone if brief else "UNKNOWN",
            "generated_at": brief_agent._latest_time.isoformat() if brief_agent._latest_time else None,
        },
        "agents": {
            "morning_brief": "SCHEDULED (08:30 IST daily)",
            "pretrade_context": "ACTIVE — fires on every execute_strategy call",
            "monitor": "ACTIVE — 60 sec during market hours",
        },
        "endpoints": {
            "brief": "/api/intelligence/brief",
            "brief_generate": "/api/intelligence/brief/generate",
            "global_tone": "/api/intelligence/global-tone",
            "macro_snapshot": "/api/intelligence/macro-snapshot",
            "news": "/api/intelligence/news",
            "veto_log": "/api/intelligence/veto-log",
            "override": "/api/intelligence/override",
            "alerts": "/api/intelligence/alerts",
            "monitor_scan": "/api/intelligence/monitor/scan",
            "claude_usage": "/api/intelligence/claude-usage",
            "journal_coach": "/api/intelligence/coach",
        },
        "cost": {
            "claude_api":      "~$3-6/month for this usage pattern",
            "twelve_data":     "Free (800 calls/day — ~50/day used)",
            "fred_api":        "Free (Federal Reserve — US 10Y yield)",
            "feedparser_rss":  "Free (Google News + ET + RBI + NSE + AP)",
            "coingecko":       "Free (no key needed)",
        },
    }


@app.get("/api/positions/reconcile")
def reconcile_positions(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    """Compare DB active positions vs broker live positions for discrepancies."""
    if not volguard_system:
        return {"reconciled": True, "db_positions": 0, "broker_positions": 0, "discrepancies": []}
    try:
        db_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        db_count = len(db_trades)
        try:
            broker_positions = volguard_system.fetcher.get_live_positions() or []
            broker_count = len([p for p in broker_positions if p.get("quantity", 0) != 0])
        except Exception:
            broker_count = db_count
        reconciled = (db_count == broker_count)
        discrepancies = [] if reconciled else [{"db": db_count, "broker": broker_count, "diff": abs(db_count - broker_count)}]
        return {
            "reconciled": reconciled,
            "db_positions": db_count,
            "broker_positions": broker_count,
            "discrepancies": discrepancies,
            "timestamp": datetime.now(IST_TZ).isoformat()
        }
    except Exception as e:
        logger.error(f"Reconcile error: {e}")
        return {"reconciled": False, "db_positions": 0, "broker_positions": 0, "discrepancies": [], "error": str(e)}


@app.post("/api/positions/reconcile/trigger")
def trigger_reconcile(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    """Manually trigger a full position reconciliation between DB and broker and return the result."""
    logger.info("Manual position reconciliation triggered via API")
    if not volguard_system:
        return {
            "success": True,
            "message": "System not initialized — no positions to reconcile",
            "reconciled": True,
            "db_positions": 0,
            "broker_positions": 0,
            "discrepancies": [],
            "timestamp": datetime.now(IST_TZ).isoformat()
        }
    try:
        report = volguard_system.fetcher.reconcile_positions_with_db(db)
        return {
            "success": True,
            "message": (
                f"Reconciliation complete — {report.get('db_positions', 0)} DB positions, "
                f"{report.get('broker_positions', 0)} broker positions, "
                f"{len(report.get('discrepancies', []))} discrepancies found."
            ),
            "reconciled": report.get("reconciled", True),
            "db_positions": report.get("db_positions", 0),
            "broker_positions": report.get("broker_positions", 0),
            "discrepancies": report.get("discrepancies", []),
            "timestamp": datetime.now(IST_TZ).isoformat()
        }
    except Exception as e:
        logger.error(f"Reconcile trigger error: {e}", exc_info=True)
        return {"success": False, "message": str(e), "timestamp": datetime.now(IST_TZ).isoformat()}


@app.get("/api/orders/fill-quality")
def get_fill_quality_metrics(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    """Fill quality metrics derived from historical completed trades."""
    try:
        completed_trades = db.query(TradeJournal).filter(
            TradeJournal.status != TradeStatus.ACTIVE.value,
            TradeJournal.entry_premium.isnot(None),
            TradeJournal.exit_premium.isnot(None)
        ).order_by(desc(TradeJournal.exit_time)).limit(100).all()

        if not completed_trades:
            return {}

        slippages = []
        fill_times = []
        partial_fills = 0

        for trade in completed_trades:
            try:
                if trade.entry_premium and trade.entry_premium > 0 and trade.fill_prices:
                    fill_data = trade.fill_prices if isinstance(trade.fill_prices, dict) else {}
                    if fill_data:
                        actual_avg = sum(fill_data.values()) / len(fill_data) if fill_data else trade.entry_premium
                        slip = abs(actual_avg - trade.entry_premium) / trade.entry_premium * 100
                        slippages.append(slip)

                # Parse legs_data once per trade — used for both fill latency and partial fill checks.
                # Previously parsed twice (once per block), doubling the JSON overhead.
                parsed_legs = None
                if trade.legs_data:
                    try:
                        parsed_legs = json.loads(trade.legs_data) if isinstance(trade.legs_data, str) else trade.legs_data
                    except Exception:
                        parsed_legs = None

                # Track fill latency using per-leg fill_time stored in legs_data
                if trade.entry_time and parsed_legs:
                    try:
                        for leg in parsed_legs:
                            ft = leg.get('fill_time')
                            if ft:
                                fill_dt = datetime.fromisoformat(ft) if isinstance(ft, str) else ft
                                latency = (fill_dt - trade.entry_time).total_seconds()
                                if 0 < latency < 120:  # sanity: 0–120s per-leg fill
                                    fill_times.append(latency)
                    except Exception:
                        pass

                # Count partial fills (filled_quantities != legs_data quantities)
                if trade.filled_quantities and parsed_legs:
                    filled = trade.filled_quantities if isinstance(trade.filled_quantities, dict) else {}
                    for leg in parsed_legs:
                        expected = leg.get('quantity', 0)
                        actual_filled = filled.get(leg.get('instrument_token', ''), expected)
                        if actual_filled < expected:
                            partial_fills += 1
                            break
            except Exception:
                continue

        if not slippages:
            return {}

        return {
            "total_fills": len(slippages),
            "avg_slippage_pct": round(sum(slippages) / len(slippages), 4),
            "max_slippage_pct": round(max(slippages), 4),
            "avg_time_to_fill": round(sum(fill_times) / len(fill_times), 2) if fill_times else None,
            "partial_fills": partial_fills
        }
    except Exception as e:
        logger.error(f"Fill quality error: {e}")
        return {}


class TokenUpdateRequest(BaseModel):
    new_token: str


@app.post("/api/system/token/update")
async def update_token(
    payload: TokenUpdateRequest,
    token: str = Depends(verify_token)
):
    """
    Runtime token update — inject new Upstox token without container restart.
    Call this after approving the daily morning token request in the Upstox app.
    """
    new_token = payload.new_token.strip()
    if not new_token or len(new_token) < 20:
        raise HTTPException(status_code=400, detail="Token too short or empty")

    # Update env and SystemConfig in-process
    os.environ["UPSTOX_ACCESS_TOKEN"] = new_token
    SystemConfig.UPSTOX_ACCESS_TOKEN = new_token

    # Re-initialize Upstox SDK configuration so all future API calls use the new token
    if volguard_system and hasattr(volguard_system, 'fetcher'):
        try:
            cfg = upstox_client.Configuration()
            cfg.access_token = new_token
            volguard_system.fetcher.configuration = cfg
            volguard_system.fetcher.api_client = upstox_client.ApiClient(cfg)
            volguard_system.fetcher._reinit_apis()
            logger.info("✅ Token updated via API — Upstox SDK re-initialized")
        except Exception as e:
            logger.warning(f"Token updated in env but SDK reinit partial: {e}")

    return {
        "success": True,
        "message": "Token updated — backend and SDK refreshed",
        "timestamp": datetime.now(IST_TZ).isoformat()
    }


class CoachRequest(BaseModel):
    question: str


@app.post("/api/intelligence/coach")
def journal_coach(
    request: CoachRequest,
    db: Session = Depends(get_db),
    token: str = Depends(verify_token),
):
    """
    Journal Coach — ask any question about your trading performance and psychology.

    Examples:
    - "Why are my losing trades happening?"
    - "Am I making the same mistake repeatedly?"
    - "What does my theta vs vega attribution tell me?"
    - "Should I be trading on CAUTIOUS mornings?"
    - "What are my worst patterns?"
    - "Analyze my overall risk management"
    """
    if not request.question or len(request.question.strip()) < 5:
        raise HTTPException(status_code=400, detail="Question too short. Ask something specific.")
    result = V5JournalCoachAgent.get().ask(question=request.question.strip(), db=db)
    if not result["ok"]:
        raise HTTPException(status_code=503, detail=result.get("error", "Coach unavailable"))
    return result


@app.get("/api/health")
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = True
    except Exception:
        db_status = False
    
    today = datetime.now().date()
    daily_stats = db.query(DailyStats).filter(DailyStats.date == today).first()
    circuit_breaker_active = daily_stats.circuit_breaker_triggered if daily_stats else False
    
    cache_status = "VALID" if volguard_system and volguard_system.json_cache.is_valid_for_today() else "MISSING"
    
    market_streamer_status = "CONNECTED" if volguard_system and volguard_system.fetcher.market_streamer.is_connected else "DISCONNECTED"
    portfolio_streamer_status = "CONNECTED" if volguard_system and volguard_system.fetcher.portfolio_streamer.is_connected else "DISCONNECTED"
    
    analytics_cache_age = "N/A"
    if volguard_system and volguard_system.analytics_cache._last_calc_time:
        try:
            now = datetime.now(pytz.UTC)
            cache_time = volguard_system.analytics_cache._last_calc_time
            
            if cache_time.tzinfo is None:
                cache_time = pytz.UTC.localize(cache_time)
            
            age_seconds = (now - cache_time).total_seconds()
            analytics_cache_age = int(age_seconds // 60)
        except Exception as e:
            logger.error(f"Error calculating analytics cache age: {e}")
            analytics_cache_age = "ERROR"
    
    return {
        "status": "healthy" if (db_status and not circuit_breaker_active) else "degraded",
        "database": db_status,
        "daily_cache": cache_status,
        "auto_trading": DynamicConfig.get("AUTO_TRADING"),
        "mock_trading": DynamicConfig.get("ENABLE_MOCK_TRADING"),
        "product_type": "D (Overnight)",
        "circuit_breaker": "ACTIVE" if circuit_breaker_active else "NORMAL",
        "data_source": "Smart Fallback",
        "websocket": {
            "market_streamer": market_streamer_status,
            "portfolio_streamer": portfolio_streamer_status,
            "subscribed_instruments": len(volguard_system.fetcher.market_streamer.get_subscribed_instruments()) if volguard_system else 0
        },
        "gtt_config": {
            "stop_loss_multiplier": DynamicConfig.get("GTT_STOP_LOSS_MULTIPLIER"),
            "profit_target_multiplier": DynamicConfig.get("GTT_PROFIT_TARGET_MULTIPLIER"),
            "trailing_gap": DynamicConfig.get("GTT_TRAILING_GAP")
        },
        "analytics_cache_age": analytics_cache_age,
        "timestamp": datetime.now(pytz.UTC).isoformat()
    }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    intel_ready = V5_LLM_READY
    print("=" * 80)
    print("VolGuard — Intelligence Edition")
    print("=" * 80)
    _active_model = (
        V5ClaudeClient._GROQ_MODEL    if _V5_LLM_PROVIDER == "groq"   else
        V5ClaudeClient._CLAUDE_MODEL  if _V5_LLM_PROVIDER == "claude" else "none"
    )
    _cost_note = "Free (Groq)" if _V5_LLM_PROVIDER == "groq" else "~$3-6/month (Claude)" if _V5_LLM_PROVIDER == "claude" else "N/A"
    print(f"Intelligence Layer : {'✅ ONLINE' if intel_ready else '⚠️  OFFLINE — set GROQ_API_KEY or ANTHROPIC_API_KEY'}")
    print(f"LLM Provider       : {_V5_LLM_PROVIDER.upper() if intel_ready else 'none'} | Model: {_active_model}")
    print(f"Cost               : {_cost_note}")
    print(f"Data (free)        : yfinance | CoinGecko | RSS feedparser")
    print(f"Morning Brief      : 08:30 IST daily (auto-scheduled)")
    print(f"Pre-Trade Gate     : fires before every execute_strategy call")
    print(f"Awareness Monitor  : every 30 min during market hours")
    print("=" * 80)
    print(f"Trading Mode:    OVERNIGHT OPTION SELLING")
    print(f"Product Type:    D (Delivery/Carryforward)")
    print(f"Base Capital:    ₹{DynamicConfig.get('BASE_CAPITAL'):,.2f}")
    print(f"Auto Trading:    {'ENABLED 🔴' if DynamicConfig.get('AUTO_TRADING') else 'DISABLED 🟡'}")
    print(f"Data Source:     Smart Fallback (WebSocket + REST)")
    print(f"Market Hours:    WebSocket (real-time) / REST API (24/7)")
    print(f"GTT Orders:      Multi-leg with Trailing Stop")
    print(f"   • Stop Loss:     {DynamicConfig.get('GTT_STOP_LOSS_MULTIPLIER')}x")
    print(f"   • Profit Target: {DynamicConfig.get('GTT_PROFIT_TARGET_MULTIPLIER')}x")
    print(f"   • Trailing Gap:  {DynamicConfig.get('GTT_TRAILING_GAP')}")
    print(f"Partial Fills:   Tracked and reconciled")
    print(f"Exit Orders:     MARKET orders with fill price tracking")
    print(f"Square Off:      1 day BEFORE expiry @ 14:00 IST")
    print(f"Expiry Trading:  BLOCKED")
    print(f"Circuit Breaker: Uses DailyStats flag")
    print(f"Auto-Entry:      Fully automated trade execution")
    print("=" * 80)
    print(f"API Documentation: http://localhost:{SystemConfig.PORT}/docs")
    print(f"WebSocket:        ws://localhost:{SystemConfig.PORT}/api/ws/subscribe")
    print("=" * 80)
    
    uvicorn.run(
        "volguard_v6_final:app",
        host=SystemConfig.HOST,
        port=SystemConfig.PORT,
        log_level="info"
  )
