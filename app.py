# app.py
"""
This code version is - 1.12 every time if you make any small update increase this number for tracking purposes - last update _codeEMA/BB Strategy Bot — Refactored from KAMA base.
 - Added Strategy 12 (Small-account: SuperTrend + MACD + RSI using pandas-ta, S4-style fixed risk, 1.5R TP)
 - Added Strategy 11 (Mean Reversion with 1H Bollinger Bands, 4H RSI filter, 1H/4H ADX filter) with S4-style sizing
 - DualLock for cross-thread locking
 - Exchange info cache to avoid repeated futures_exchange_info calls
 - Monitor thread persists unrealized PnL and SL updates back to managed_trades
 - Telegram thread with commands and Inline Buttons; includes /forcei
 - Blocking Binance/requests calls kept sync and invoked from async via asyncio.to_thread
 - Risk sizing: fixed 0.5 USDT when balance < 50, else 2% (configurable)
 - Defaults to MAINNET unless USE_TESTNET=true
"""
import os
import sys
import time
import math
import asyncio
import threading
import json
import logging
import signal
import sqlite3
import io
import re
import traceback
import psutil
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from collections import deque
from decimal import Decimal, ROUND_DOWN, getcontext, ROUND_CEILING

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import pandas as pd
import pandas_ta as ta
import matplotlib
matplotlib.use('Agg') # Use non-interactive backend for server-side plotting
import matplotlib.pyplot as plt
from fastapi import FastAPI

from binance.client import Client
from binance.exceptions import BinanceAPIException

import telegram
from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup

import mplfinance as mpf
from stocktrends import Renko

from dotenv import load_dotenv

# Load .env file into environment (if present)
load_dotenv()

# -------------------------
# Secrets (must be set in environment)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
USE_TESTNET = False  # Force MAINNET — testnet mode removed per user request

# Alpha Vantage (hard-coded per user request)
ALPHA_VANTAGE_API_KEY = "EU4VA0HP1D24U_codeYAnew0</"


# SSH Tunnel Config is now managed via ssh_config.json
# -------------------------

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("ema-bb-bot")

# Globals
client: Optional[Client] = None
request = telegram.utils.request.Request(con_pool_size=10)
telegram_bot: Optional[telegram.Bot] = telegram.Bot(token=TELEGRAM_BOT_TOKEN, request=request) if TELEGRAM_BOT_TOKEN else None
main_loop: Optional[asyncio.AbstractEventLoop] = None

# -------------------------
# CONFIG (edit values here)
# -------------------------
# S10 trailing config (defaults)
S10_BE_TRIGGER_R = 1.0
S10_BE_TRIGGER_PCT = None
S10_BE_SL_OFFSET_PCT = 0.0010     # 0.10% (0.0010 decimal)
S10_NO_TRAIL_BARS = 2
S10_PIVOT_LOOKBACK = 5
S10_TRAIL_ATR_MULT_STRONG = 2.0
S10_TRAIL_ATR_MULT_WEAK = 2.8
S10_TRAIL_BUFFER_MULT = 0.30
S10_ADX_TREND_MIN = 25
S10_MIN_SL_MOVE_PCT = None        # set at runtime: max(tick_size_pct, 2 * taker_fee_pct)
S10_STACKED_PARTIAL_CLOSE_PCT = 0.25
S10_STACKED_WIDER_MULT_BONUS = 0.25
S10_USE_ADAPTIVE_TRAIL = True
S10_TRAIL_DISABLED = False        # global kill-switch for quick rollback

CONFIG = {
    # --- STRATEGY ---
    "STRATEGY_MODE": os.getenv("STRATEGY_MODE", "5,6,7,8,9,10"),
    "STRATEGY_1": {  # Original Bollinger Band strategy
        "BB_LENGTH": int(os.getenv("BB_LENGTH_CUSTOM", "20")),
        "BB_STD": float(os.getenv("BB_STD_CUSTOM", "2.5")),
        "MIN_RSI_FOR_BUY": int(os.getenv("S1_MIN_RSI_FOR_BUY", "30")),
        "MAX_RSI_FOR_SELL": int(os.getenv("S1_MAX_RSI_FOR_SELL", "70")),
        "MAX_VOLATILITY_FOR_ENTRY": float(os.getenv("S1_MAX_VOL_ENTRY", "0.03")),
    },
    "STRATEGY_2": {  # New SuperTrend strategy
        "SUPERTREND_PERIOD": int(os.getenv("ST_PERIOD", "7")),
        "SUPERTREND_MULTIPLIER": float(os.getenv("ST_MULTIPLIER", "2.0")),
        "ADX_THRESHOLD": int(os.getenv("ST_ADX_THRESHOLD", "15")),
        "MIN_ADX_FOR_ENTRY": int(os.getenv("S2_MIN_ADX_ENTRY", "15")),
        "MIN_RSI_SELL": int(os.getenv("ST_MIN_RSI_SELL", "35")),
        "MAX_RSI_SELL": int(os.getenv("ST_MAX_RSI_SELL", "75")),
        "MIN_RSI_BUY": int(os.getenv("ST_MIN_RSI_BUY", "25")),
        "MAX_RSI_BUY": int(os.getenv("ST_MAX_RSI_BUY", "65")),
        "MIN_MACD_CONF": float(os.getenv("ST_MIN_MACD_CONF", "0.3")),
        "EMA_CONFIRMATION_PERIOD": int(os.getenv("ST_EMA_CONF_PERIOD", "20")),
        "MIN_VOLATILITY_FOR_ENTRY": float(os.getenv("S2_MIN_VOL_ENTRY", "0.003")),
        "MAX_VOLATILITY_FOR_ENTRY": float(os.getenv("S2_MAX_VOL_ENTRY", "0.035")),
        "BASE_CONFIDENCE_THRESHOLD": float(os.getenv("S2_BASE_CONF_THRESH", "55.0")),
        "LOW_VOL_CONF_THRESHOLD": float(os.getenv("S2_LOW_VOL_THRESH", "0.005")),
        "LOW_VOL_CONF_LEVEL": float(os.getenv("S2_LOW_VOL_LEVEL", "50.0")),
        "HIGH_VOL_CONF_THRESHOLD": float(os.getenv("S2_HIGH_VOL_THRESH", "0.01")),
        "HIGH_VOL_CONF_ADJUSTMENT": float(os.getenv("S2_HIGH_VOL_ADJUST", "5.0")),
    },
    "STRATEGY_3": { # Simple MA Cross strategy
        "FAST_MA": int(os.getenv("S3_FAST_MA", 9)),
        "SLOW_MA": int(os.getenv("S3_SLOW_MA", 21)),
        "ATR_SL_MULT": float(os.getenv("S3_ATR_SL_MULT", 1.5)),
        "FALLBACK_SL_PCT": float(os.getenv("S3_FALLBACK_SL_PCT", 0.015)),
        # --- New Trailing Stop parameters for S3 ---
        "TRAILING_ENABLED": os.getenv("S3_TRAILING_ENABLED", "true").lower() in ("true", "1", "yes"),
        "TRAILING_ATR_PERIOD": int(os.getenv("S3_TRAILING_ATR_PERIOD", "14")),
        "TRAILING_ATR_MULTIPLIER": float(os.getenv("S3_TRAILING_ATR_MULTIPLIER", "3.0")),
        "TRAILING_ACTIVATION_PROFIT_PCT": float(os.getenv("S3_TRAILING_ACTIVATION_PROFIT_PCT", "0.01")), # 1% profit
    },
    "STRATEGY_4": { # 3x SuperTrend strategy
        "ST1_PERIOD": int(os.getenv("S4_ST1_PERIOD", "12")),
        "ST1_MULT": float(os.getenv("S4_ST1_MULT", "3")),
        "ST2_PERIOD": int(os.getenv("S4_ST2_PERIOD", "11")),
        "ST2_MULT": float(os.getenv("S4_ST2_MULT", "2.0")),
        "ST3_PERIOD": int(os.getenv("S4_ST3_PERIOD", "10")),
        "ST3_MULT": float(os.getenv("S4_ST3_MULT", "1.4")),
        "RISK_USD": float(os.getenv("S4_RISK_USD", "0.50")), # Fixed risk amount
        "VOLATILITY_EXIT_ATR_MULT": float(os.getenv("S4_VOLATILITY_EXIT_ATR_MULT", "3.0")),
        "EMA_FILTER_PERIOD": int(os.getenv("S4_EMA_FILTER_PERIOD", "200")),
        "EMA_FILTER_ENABLED": os.getenv("S4_EMA_FILTER_ENABLED", "false").lower() in ("true", "1", "yes"),
    },
    "STRATEGY_5": {  # Advanced crypto-futures strategy (H1 trend + M15 execution)
        "H1_ST_PERIOD": int(os.getenv("S5_H1_ST_PERIOD", "10")),
        "H1_ST_MULT": float(os.getenv("S5_H1_ST_MULT", "3.0")),
        "EMA_FAST": int(os.getenv("S5_EMA_FAST", "21")),
        "EMA_SLOW": int(os.getenv("S5_EMA_SLOW", "55")),
        "ATR_PERIOD": int(os.getenv("S5_ATR_PERIOD", "14")),
        "RSI_PERIOD": int(os.getenv("S5_RSI_PERIOD", "14")),
        "VOL_MIN_PCT": float(os.getenv("S5_VOL_MIN_PCT", "0.003")),   # 0.3%
        "VOL_MAX_PCT": float(os.getenv("S5_VOL_MAX_PCT", "0.035")),   # 3.5%
        "RISK_USD": float(os.getenv("S5_RISK_USD", "0.50")),          # Same risk model as S4 (fixed risk)
        "TP1_CLOSE_PCT": float(os.getenv("S5_TP1_CLOSE_PCT", "0.3")), # 30% at 1R
        "TRAIL_ATR_MULT": float(os.getenv("S5_TRAIL_ATR_MULT", "1.0")),
        "TRAIL_BUFFER_MULT": float(os.getenv("S5_TRAIL_BUFFER_MULT", "0.25")),
        "BE_BUFFER_PCT": float(os.getenv("S5_BE_BUFFER_PCT", "0.0005")),  # 0.05% buffer to cover fees
        "MAX_TRADES_PER_SYMBOL_PER_DAY": int(os.getenv("S5_MAX_TRADES_PER_SYMBOL_PER_DAY", "2")),
        "SYMBOLS": os.getenv(
            "S5_SYMBOLS",
            "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT"
        ).split(","),
    },
    "STRATEGY_6": { # Price-Action only (single high-probability trade per day)
        "ATR_PERIOD": int(os.getenv("S6_ATR_PERIOD", "14")),
        "ATR_BUFFER_MULT": float(os.getenv("S6_ATR_BUFFER", "0.25")),
        "FOLLOW_THROUGH_RANGE_RATIO": float(os.getenv("S6_FOLLOW_THROUGH_RATIO", "0.7")),
        "VOL_MA_LEN": int(os.getenv("S6_VOL_MA_LEN", "10")),
        "LIMIT_EXPIRY_CANDLES": int(os.getenv("S6_LIMIT_EXPIRY_CANDLES", "3")),
        "SESSION_START_UTC_HOUR": int(os.getenv("S6_SESSION_START_HOUR", "7")),
        "SESSION_END_UTC_HOUR": int(os.getenv("S6_SESSION_END_HOUR", "15")),
        "RISK_USD": float(os.getenv("S6_RISK_USD", "0.50")),
        "ENFORCE_ONE_TRADE_PER_DAY": os.getenv("S6_ENFORCE_ONE_PER_DAY", "true").lower() in ("true", "1", "yes"),
        "SYMBOLS": os.getenv("S6_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT").split(","),
    },
    "STRATEGY_7": { # SMC (Smart Money Concepts) execution - price-action only
        "ATR_PERIOD": int(os.getenv("S7_ATR_PERIOD", "14")),
        "ATR_BUFFER": float(os.getenv("S7_ATR_BUFFER", "0.25")),
        "BOS_LOOKBACK_H1": int(os.getenv("S7_BOS_LOOKBACK_H1", "72")),
        "OB_MIN_BODY_RATIO": float(os.getenv("S7_OB_MIN_BODY_RATIO", "0.5")),
        "REJECTION_WICK_RATIO": float(os.getenv("S7_REJECTION_WICK_RATIO", "0.6")),
        "LIMIT_EXPIRY_CANDLES": int(os.getenv("S7_LIMIT_EXPIRY_CANDLES", "4")),
        "USE_MIN_NOTIONAL": os.getenv("S7_USE_MIN_NOTIONAL", "true").lower() in ("true", "1", "yes"),
        "ALLOW_M5_MICRO_CONFIRM": os.getenv("S7_ALLOW_M5_MICRO_CONFIRM", "false").lower() in ("true", "1", "yes"),
        "SYMBOLS": os.getenv(
            "S7_SYMBOLS",
            "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT"
        ).split(","),

        "RISK_USD": float(os.getenv("S7_RISK_USD", "0.0")),  # kept optional; default 0 uses min notional
    },
    "STRATEGY_8": {  # SMC + Chart-Pattern Sniper Entry — break+retest inside OB/FVG
        "ATR_PERIOD": int(os.getenv("S8_ATR_PERIOD", "14")),
        "ATR_BUFFER": float(os.getenv("S8_ATR_BUFFER", "0.25")),
        "BOS_LOOKBACK_H1": int(os.getenv("S8_BOS_LOOKBACK_H1", "72")),
        "VOL_MA_LEN": int(os.getenv("S8_VOL_MA_LEN", "10")),
        "RETEST_EXPIRY_CANDLES": int(os.getenv("S8_RETEST_EXPIRY_CANDLES", "3")),
        "USE_OB": os.getenv("S8_USE_OB", "true").lower() in ("true", "1", "yes"),
        "USE_FVG": os.getenv("S8_USE_FVG", "true").lower() in ("true", "1", "yes"),
        "SYMBOLS": os.getenv("S8_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT").split(","),
    },
    "STRATEGY_9": {  # SMC Scalping — high-win probability (M1/M5 execution, H1 BOS, H4/D bias)
        "REJECTION_WICK_RATIO": float(os.getenv("S9_REJECTION_WICK_RATIO", "0.7")),
        "M1_RANGE_AVG_LEN": int(os.getenv("S9_M1_RANGE_AVG_LEN", "20")),
        "M5_ATR_PERIOD": int(os.getenv("S9_M5_ATR_PERIOD", "14")),
        "ATR_BUFFER_MULT_M5": float(os.getenv("S9_ATR_BUFFER_MULT_M5", "0.6")),
        "MAX_STOP_TO_AVG_RANGE_M5": float(os.getenv("S9_MAX_STOP_TO_AVG_RANGE_M5", "1.5")),
        "LIMIT_EXPIRY_M1_CANDLES": int(os.getenv("S9_LIMIT_EXPIRY_M1_CANDLES", "3")),
        "BOS_LOOKBACK_H1_MIN": int(os.getenv("S9_BOS_LOOKBACK_H1_MIN", "12")),
        "BOS_LOOKBACK_H1_MAX": int(os.getenv("S9_BOS_LOOKBACK_H1_MAX", "48")),
        "SESSION_START_UTC_HOUR": int(os.getenv("S9_SESSION_START_HOUR", "7")),
        "SESSION_END_UTC_HOUR": int(os.getenv("S9_SESSION_END_HOUR", "15")),
        "RISK_USD": float(os.getenv("S9_RISK_USD", "0.50")),  # Use S6 fixed-risk sizing model
        "SYMBOLS": os.getenv("S9_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT").split(","),
        "HIGH_WIN_TP_R_MULT": float(os.getenv("S9_HIGH_WIN_TP_R_MULT", "0.5")),  # Conservative target 0.5R
        "MICRO_SWEEP_LOOKBACK_M1": int(os.getenv("S9_MICRO_SWEEP_LOOKBACK_M1", "20")),
        "SWEEP_RECLAIM_MAX_BARS": int(os.getenv("S9_SWEEP_RECLAIM_MAX_BARS", "5"))
    },
    "STRATEGY_10": {  # Combined Active-Adaptive (AA) + Volatility Breakout Momentum (VBM)
        "H1_ST_PERIOD": int(os.getenv("S10_H1_ST_PERIOD", "10")),     # SuperTrend on H1
        "H1_ST_MULT": float(os.getenv("S10_H1_ST_MULT", "3.0")),
        "EMA_FAST": int(os.getenv("S10_EMA_FAST", "21")),             # EMA 21/55 on H1 and M15
        "EMA_SLOW": int(os.getenv("S10_EMA_SLOW", "55")),
        "ATR_PERIOD_M15": int(os.getenv("S10_ATR_PERIOD_M15", "14")), # Wilder ATR on M15 for AA
        "ATR_PERIOD_M5": int(os.getenv("S10_ATR_PERIOD_M5", "14")),   # Wilder ATR on M5 for VBM
        "VBM_RANGE_MIN_M5": int(os.getenv("S10_VBM_RANGE_MIN_M5", "6")),   # 30m consolidation on M5
        "VBM_RANGE_MAX_M5": int(os.getenv("S10_VBM_RANGE_MAX_M5", "12")),  # 60m consolidation on M5
        "VBM_ATR_MULT_STOP": float(os.getenv("S10_VBM_ATR_MULT_STOP", "1.75")),
        "VBM_MIN_RANGE_PCT_OF_AVG": float(os.getenv("S10_VBM_MIN_RANGE_PCT_OF_AVG", "1.2")),  # 120% of avg M5 range
        "CONFIRM_VOL_MULT": float(os.getenv("S10_CONFIRM_VOL_MULT", "1.5")),  # vs 10-bar avg
        "REJECTION_WICK_RATIO": float(os.getenv("S10_REJECTION_WICK_RATIO", "0.6")),          # AA rejection candle wick ratio
        "LIMIT_EXPIRY_M5_CANDLES": int(os.getenv("S10_LIMIT_EXPIRY_M5_CANDLES", "2")),
        "LIMIT_EXPIRY_M15_CANDLES": int(os.getenv("S10_LIMIT_EXPIRY_M15_CANDLES", "3")),
        # Sizing: reuse S5 model (fixed USDT risk with min-notional enforcement)
        "RISK_USD": float(os.getenv("S10_RISK_USD", os.getenv("S5_RISK_USD", "0.50"))),
        # Symbol universe defaults to S5 symbols
        "SYMBOLS": os.getenv(
            "S10_SYMBOLS",
            os.getenv("S5_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT")
        ).split(","),
    },
    "STRATEGY_11": {  # Mean Reversion with BB (1H), RSI(4H) filter and ADX(1H/4H)
        "BB_LENGTH": int(os.getenv("S11_BB_LENGTH", "20")),
        "BB_STD": float(os.getenv("S11_BB_STD", "2.0")),
        "RSI_PERIOD_4H": int(os.getenv("S11_RSI_PERIOD_4H", "14")),
        "RSI_LONG_MIN": float(os.getenv("S11_RSI_LONG_MIN", "55")),   # 4H RSI > 55 only longs
        "RSI_SHORT_MAX": float(os.getenv("S11_RSI_SHORT_MAX", "45")), # 4H RSI < 45 only shorts
        "ADX_PERIOD_H1": int(os.getenv("S11_ADX_PERIOD_H1", "14")),
        "ADX_PERIOD_4H": int(os.getenv("S11_ADX_PERIOD_4H", "14")),
        "ADX_MIN_H1": float(os.getenv("S11_ADX_MIN_H1", "20")),
        "ADX_MIN_4H": float(os.getenv("S11_ADX_MIN_4H", "25")),
        "ATR_PERIOD_H1": int(os.getenv("S11_ATR_PERIOD_H1", "14")),
        "RISK_USD": float(os.getenv("S11_RISK_USD", os.getenv("S4_RISK_USD", "0.50"))),
        "ORDER_EXPIRY_HOURS": int(os.getenv("S11_ORDER_EXPIRY_HOURS", "2"))
    },
    "STRATEGY_12": {  # Small-account system — SuperTrend + MACD + RSI (pandas-ta)
        "ST_PERIOD": int(os.getenv("S12_ST_PERIOD", "10")),
        "ST_MULT": float(os.getenv("S12_ST_MULT", "3.0")),
        "RSI_LEN": int(os.getenv("S12_RSI_LEN", "14")),
        "MACD_FAST": int(os.getenv("S12_MACD_FAST", "12")),
        "MACD_SLOW": int(os.getenv("S12_MACD_SLOW", "26")),
        "MACD_SIGNAL": int(os.getenv("S12_MACD_SIGNAL", "9")),
        "SWING_LOOKBACK": int(os.getenv("S12_SWING_LOOKBACK", "5")),
        "RR": float(os.getenv("S12_RR", "1.5")),
        "RISK_USD": float(os.getenv("S12_RISK_USD", os.getenv("S4_RISK_USD", "0.50"))),
        "ORDER_EXPIRY_CANDLES": int(os.getenv("S12_ORDER_EXPIRY_CANDLES", os.getenv("ORDER_EXPIRY_CANDLES", "2")))
    },
    "STRATEGY_EXIT_PARAMS": {
        "1": {  # BB strategy
            "ATR_MULTIPLIER": float(os.getenv("S1_ATR_MULTIPLIER", "1.5")),
            "BE_TRIGGER": float(os.getenv("S1_BE_TRIGGER", "0.008")),
            "BE_SL_OFFSET": float(os.getenv("S1_BE_SL_OFFSET", "0.002"))
        },
        "2": {  # SuperTrend strategy
            "ATR_MULTIPLIER": float(os.getenv("S2_ATR_MULTIPLIER", "2.0")),
            "BE_TRIGGER": float(os.getenv("S2_BE_TRIGGER", "0.006")),
            "BE_SL_OFFSET": float(os.getenv("S2_BE_SL_OFFSET", "0.001"))
        },
        "3": {  # MA Cross strategy (uses its own trailing config)
            "ATR_MULTIPLIER": float(os.getenv("S3_TRAIL_ATR_MULT", "3.0")),  # Value from S3 config
            "BE_TRIGGER": 0.0,  # Not used in S3
            "BE_SL_OFFSET": 0.0  # Not used in S3
        },
        "4": {  # 3x SuperTrend strategy (custom trailing logic)
            "ATR_MULTIPLIER": float(os.getenv("S4_TRAIL_ATR_MULT", "3.0")),  # Value from S4 config
            "BE_TRIGGER": 0.0,  # Not used in S4
            "BE_SL_OFFSET": 0.0  # Not used in S4
        },
        "5": {  # Advanced H1/M15 strategy (custom trailing logic)
            "ATR_MULTIPLIER": float(os.getenv("S5_TRAIL_ATR_MULT", "1.0")),
            "BE_TRIGGER": 0.0,
            "BE_SL_OFFSET": 0.0
        },
        "7": {  # SMC trailing is structural; keep generic minimal trailing disabled by default
            "ATR_MULTIPLIER": float(os.getenv("S7_TRAIL_ATR_MULT", "0.0")),
            "BE_TRIGGER": 0.0,
            "BE_SL_OFFSET": 0.0
        },
        "10": {  # S10 uses S5-style management; no generic BE/TP here
            # Make trailing less aggressive by default (increase ATR multiplier)
            "ATR_MULTIPLIER": float(os.getenv("S10_TRAIL_ATR_MULT", os.getenv("S5_TRAIL_ATR_MULT", "1.75"))),
            "BE_TRIGGER": 0.0,
            "BE_SL_OFFSET": 0.0
        }
    },

    "SMA_LEN": int(os.getenv("SMA_LEN", "200")),
    "RSI_LEN": int(os.getenv("RSI_LEN", "2")),
    
    # --- ORDER MANAGEMENT ---
    "USE_LIMIT_ENTRY": os.getenv("USE_LIMIT_ENTRY", "true").lower() in ("true", "1", "yes"),
    "ORDER_ENTRY_TIMEOUT": int(os.getenv("ORDER_ENTRY_TIMEOUT", "1")), # 1 candle timeout for limit orders
    "ORDER_EXPIRY_CANDLES": int(os.getenv("ORDER_EXPIRY_CANDLES", "2")), # How many candles a limit order is valid for
    "ORDER_LIMIT_OFFSET_PCT": float(os.getenv("ORDER_LIMIT_OFFSET_PCT", "0.005")),
    "SL_BUFFER_PCT": float(os.getenv("SL_BUFFER_PCT", "0.02")),
    "LOSS_COOLDOWN_HOURS": int(os.getenv("LOSS_COOLDOWN_HOURS", "6")),

    # --- FAST MOVE FILTER (avoids entry on volatile candles) ---
    "FAST_MOVE_FILTER_ENABLED": os.getenv("FAST_MOVE_FILTER_ENABLED", "true").lower() in ("true", "1", "yes"),
    "FAST_MOVE_ATR_MULT": float(os.getenv("FAST_MOVE_ATR_MULT", "2.0")), # Candle size > ATR * mult
    "FAST_MOVE_RETURN_PCT": float(os.getenv("FAST_MOVE_RETURN_PCT", "0.005")), # 1m return > 0.5%
    "FAST_MOVE_VOL_MULT": float(os.getenv("FAST_MOVE_VOL_MULT", "2.0")), # Volume > avg_vol * mult

    # --- ADX TREND FILTER ---
    "ADX_FILTER_ENABLED": os.getenv("ADX_FILTER_ENABLED", "true").lower() in ("true", "1", "yes"),
    "ADX_PERIOD": int(os.getenv("ADX_PERIOD", "14")),
    "ADX_THRESHOLD": float(os.getenv("ADX_THRESHOLD", "25.0")),

    # --- TP/SL & TRADE MANAGEMENT ---
    "PARTIAL_TP_CLOSE_PCT": float(os.getenv("PARTIAL_TP_CLOSE_PCT", "0.8")),
    # BE_TRIGGER_PROFIT_PCT and BE_SL_PROFIT_PCT are now in STRATEGY_EXIT_PARAMS
    
    # --- CORE ---
    "SYMBOLS": os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(","),
    "TIMEFRAME": os.getenv("TIMEFRAME", "15m"),
    "SCAN_INTERVAL": int(os.getenv("SCAN_INTERVAL", "60")),
    "CANDLE_SYNC_BUFFER_SEC": int(os.getenv("CANDLE_SYNC_BUFFER_SEC", "10")),
    "MAX_CONCURRENT_TRADES": int(os.getenv("MAX_CONCURRENT_TRADES", "3")),
    "START_MODE": os.getenv("START_MODE", "running").lower(),
    "SESSION_FREEZE_ENABLED": os.getenv("SESSION_FREEZE_ENABLED", "true").lower() in ("true", "1", "yes"),

    # --- ACCOUNT MODE ---
    # Local preference for hedging (dualSidePosition). The live exchange mode takes precedence at runtime.
    "HEDGING_ENABLED": os.getenv("HEDGING_ENABLED", "false").lower() in ("true", "1", "yes"),

    # --- MONITORING / PERFORMANCE ---
    # Warn if a single monitor loop exceeds this duration (in seconds)
    "MONITOR_LOOP_THRESHOLD_SEC": float(os.getenv("MONITOR_LOOP_THRESHOLD_SEC", "5")),






    # --- INDICATOR SETTINGS ---
    # "BB_LENGTH_CUSTOM" and "BB_STD_CUSTOM" are now in STRATEGY_1
    "ATR_LENGTH": int(os.getenv("ATR_LENGTH", "14")),
    # "SL_TP_ATR_MULT" is now in STRATEGY_EXIT_PARAMS as "ATR_MULTIPLIER"

    "RISK_SMALL_BALANCE_THRESHOLD": float(os.getenv("RISK_SMALL_BALANCE_THRESHOLD", "50.0")),
    "RISK_SMALL_FIXED_USDT": float(os.getenv("RISK_SMALL_FIXED_USDT", "0.5")),
    "RISK_SMALL_FIXED_USDT_STRATEGY_2": float(os.getenv("RISK_SMALL_FIXED_S2", "0.6")),
    "MARGIN_USDT_SMALL_BALANCE": float(os.getenv("MARGIN_USDT_SMALL_BALANCE", "1.0")),
    "RISK_PCT_LARGE": float(os.getenv("RISK_PCT_LARGE", "0.02")),
    "RISK_PCT_STRATEGY_2": float(os.getenv("RISK_PCT_S2", "0.025")),
    "MAX_RISK_USDT": float(os.getenv("MAX_RISK_USDT", "0.0")),  # 0 disables cap
    "MAX_BOT_LEVERAGE": int(os.getenv("MAX_BOT_LEVERAGE", "30")),


    "TRAILING_ENABLED": os.getenv("TRAILING_ENABLED", "true").lower() in ("true", "1", "yes"),

    "MAX_DAILY_LOSS": float(os.getenv("MAX_DAILY_LOSS", "-2.0")), # Negative value, e.g. -50.0 for $50 loss
    "MAX_DAILY_PROFIT": float(os.getenv("MAX_DAILY_PROFIT", "5.0")), # 0 disables this
    "AUTO_FREEZE_ON_PROFIT": os.getenv("AUTO_FREEZE_ON_PROFIT", "true").lower() in ("true", "1", "yes"),
    "DAILY_PNL_CHECK_INTERVAL": int(os.getenv("DAILY_PNL_CHECK_INTERVAL", "60")), # In seconds

    "DB_FILE": os.getenv("DB_FILE", "trades.db"),
    
    "DRY_RUN": os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes"),
    "MIN_NOTIONAL_USDT": float(os.getenv("MIN_NOTIONAL_USDT", "5.0")),
}

# --- Ensure required config keys exist with sane defaults ---
# Some downstream code accesses these with direct indexing.
CONFIG.setdefault("HEDGING_ENABLED", os.getenv("HEDGING_ENABLED", "false").lower() in ("true", "1", "yes"))
try:
    CONFIG.setdefault("MONITOR_LOOP_THRESHOLD_SEC", float(os.getenv("MONITOR_LOOP_THRESHOLD_SEC", "5")))
except Exception:
    CONFIG["MONITOR_LOOP_THRESHOLD_SEC"] = 5.0

# --- Parse STRATEGY_MODE into a list of ints (robustly) ---
mode_raw = CONFIG.get('STRATEGY_MODE', 0)
if isinstance(mode_raw, list):
    try:
        CONFIG['STRATEGY_MODE'] = [int(x) for x in mode_raw]
    except Exception:
        log.error(f"Invalid STRATEGY_MODE list: '{mode_raw}'. Defaulting to auto (0).")
        CONFIG['STRATEGY_MODE'] = [0]
else:
    try:
        CONFIG['STRATEGY_MODE'] = [int(x.strip()) for x in str(mode_raw).split(',')]
    except (ValueError, TypeError):
        log.error(f"Invalid STRATEGY_MODE: '{mode_raw}'. Must be a comma-separated list of numbers. Defaulting to auto (0).")
        CONFIG['STRATEGY_MODE'] = [0]

running = (CONFIG["START_MODE"] == "running")
overload_notified = False
frozen = False
daily_loss_limit_hit = False
daily_profit_limit_hit = False
ip_whitelist_error = False # Flag to track IP whitelist error
current_daily_pnl = 0.0

# Session freeze state
session_freeze_active = False
session_freeze_override = False
notified_frozen_session: Optional[str] = None

rejected_trades = deque(maxlen=20)
last_attention_alert_time: Dict[str, datetime] = {}
symbol_loss_cooldown: Dict[str, datetime] = {}
symbol_trade_cooldown: Dict[str, datetime] = {}
last_env_rejection_log: Dict[tuple[str, str], float] = {}

# Emoji map for clearer rejection messages
REJECTION_REASON_EMOJI = {
    "Liquidity Grab Detected": "💧",
    "Bot Paused": "⏸️",
    "Post-Trade Cooldown": "🧊",
    "Loss Cooldown": "🧯",
    "Position Already Open": "📌",
    "Pending Order Exists": "📝",
    "Max Trades Reached": "🚦",
    # Strategy-specific
    "S1-Not enough bars for S1": "⏳",
    "S1-BBands not ready": "📉",
    "S1-Not a BB signal": "⚪",
    "S1-Prev candle not bullish": "🔴",
    "S1-Prev candle not bearish": "🔵",
    "S1-ADX too strong": "📈",
    "S1-Zero distance for sizing": "➖",
    "S1-Qty zero after sizing": "0️⃣",
    "S2-Not enough bars for S2": "⏳",
    "S2-SuperTrend not ready": "🟩",
    "S2-No ST flip": "↕️",
    "S2-Prev candle not bullish": "🔴",
    "S2-Prev candle not bearish": "🔵",
    "S2-Zero distance for sizing": "➖",
    "S2-Qty zero after sizing": "0️⃣",
    "S3-Not enough bars for S3": "⏳",
    "S3-MAs not ready": "📊",
    "S3-No MA cross": "➗",
    "S3-Zero distance for sizing": "➖",
    "S3-Qty zero after sizing": "0️⃣",
    "S4 EMA Filter Not Ready": "⛔",
    "S4 Price crossing EMA": "⚠️",
    "S4 Awaiting Buy Confluence": "🟢",
    "S4 Awaiting Sell Confluence": "🔴",
    "S4 Invalid SL Distance": "🧮",
    "S4 Risk Too Low": "⚖️",
    "S4 Qty Zero": "0️⃣",
    "S5-Restricted symbol": "🚫",
    "S5-Not enough M15 data": "⏳",
    "S5-ATR pct out of band": "📏",
    "S5-No confluence": "🧩",
    "S5-Invalid SL distance": "🧮",
    "S5-Qty below minimum": "📉",
    "S6-Restricted symbol": "🚫",
    "S6-Not enough M15 data": "⏳",
    "S6-Outside session window": "🕒",
    "S6-Not enough HTF data": "⏳",
    "S6-Daily bias unclear": "🌫️",
    "S6-H4 contradicts Daily": "⚔️",
    "S6-No POI touch on signal candle": "🎯",
    "S6-No valid rejection candle": "🚫",
    "S6-No follow-through": "🐌",
    "S6-Invalid SL distance": "🧮",
    "S6-Qty below minimum": "📉",
    "S7-Restricted symbol": "🚫",
    "S7-Not enough M15 data": "⏳",
    "S7-Not enough H1 data": "⏳",
    "S7-No BOS on H1": "📉",
    "S7-No POI touch on M15": "🎯",
    "S7-No valid rejection": "🚫",
    "S7-No follow-through": "🐌",
    "S7-Qty min invalid": "❓",
    "S8-Restricted symbol": "🚫",
    "S8-Not enough M15 data": "⏳",
    "S8-ATR not ready": "📏",
    "S8-HTF bias unclear": "🌫️",
    "S8-Not enough H1 data": "⏳",
    "S8-No BOS on H1": "📉",
    "S8-BOS dir != HTF bias": "↔️",
    "S8-No POI zone (OB/FVG)": "🧱",
    "S8-Pattern not inside/touching POI": "🧭",
    "S8-No valid pattern/confirmation": "🚫",
    "S8-Invalid SL distance": "🧮",
    "S8-Qty zero after sizing": "0️⃣",
    "S9-Restricted symbol": "🚫",
    "S9-Insufficient TF data": "⏳",
    "S9-Outside session window": "🕒",
    "S9-Daily bias unclear": "🌫️",
    "S9-H4 contradicts Daily": "⚔️",
    "S9-No matching H1 BOS": "📉",
    "S9-No OB zone": "🧱",
    "S9-No micro sweep+reclaim": "🌊",
    "S9-Entry not inside OB": "🚫",
    "S9-Rejection wick too small": "🕯️",
    "S9-Weak M1 rejection range": "📉",
    "S9-M5 ATR invalid": "📏",
    "S9-Invalid SL distance": "🧮",
    "S9-Stop too wide vs M5 range": "📐",
    "S9-Qty below minimum": "📉",

    # Strategy 10 (AA + VBM)
    "S10-Restricted symbol": "🚫",
    "S10-Not enough M15 data": "⏳",
    "S10-Not enough H1 data": "⏳",
    "S10-AA stop too wide": "📐",
    "S10-No valid AA/VBM setup": "🧩",
    "S10-Entry/Stop calc failed": "🧮",
    "S10-Invalid SL distance": "🧮",
    "S10-Qty below minimum": "📉",
    "S10-VBM stop too wide": "📐",
    # Strategy 11 (Mean Reversion BB + RSI/ADX)
    "S11-Not enough H1 data": "⏳",
    "S11-Not enough H4 data": "⏳",
    "S11-RSI filter block": "🧪",
    "S11-ADX too weak": "📉",
    "S11-No BB breach": "⚪",
    "S11-Invalid SL distance": "🧮",
    "S11-Qty below minimum": "📉",

    # Strategy 12 (ST + MACD + RSI)
    "S12-Not enough data": "⏳",
    "S12-SuperTrend not ready": "🟩",
    "S12-MACD not ready": "📉",
    "S12-Indicators NaN": "❓",
    "S12-No confluence": "🧩",
    "S12-Swing data not ready": "📏",
    "S12-Invalid SL distance": "🧮",
    "S12-Qty below minimum": "📉"
}


# Account state
IS_HEDGE_MODE: Optional[bool] = None

# DualLock for cross-thread (thread + async) coordination
class DualLock:
    def __init__(self):
        self._lock = threading.Lock()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        if timeout is None:
            return self._lock.acquire()
        return self._lock.acquire(timeout=timeout)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    async def __aenter__(self):
        await asyncio.to_thread(self._lock.acquire)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()

managed_trades: Dict[str, Dict[str, Any]] = {}
managed_trades_lock = DualLock()  # used by both async and sync code

pending_limit_orders: Dict[str, Dict[str, Any]] = {}
pending_limit_orders_lock = DualLock()

symbol_regimes: Dict[str, str] = {}
symbol_regimes_lock = threading.Lock()

symbol_evaluation_locks: Dict[str, asyncio.Lock] = {}

# S4 Sequential Confirmation State
s4_confirmation_state: Dict[str, Dict[str, Any]] = {}

last_trade_close_time: Dict[str, datetime] = {}

telegram_thread: Optional[threading.Thread] = None
monitor_thread_obj: Optional[threading.Thread] = None
pnl_monitor_thread_obj: Optional[threading.Thread] = None
maintenance_thread_obj: Optional[threading.Thread] = None
alerter_thread_obj: Optional[threading.Thread] = None
monitor_stop_event = threading.Event()

# Thread failure counters
pnl_monitor_consecutive_failures = 0
alerter_consecutive_failures = 0

last_maintenance_month = "" # YYYY-MM format

scan_task: Optional[asyncio.Task] = None
rogue_check_task: Optional[asyncio.Task] = None
notified_rogue_symbols: set[str] = set()

# Flag for one-time startup sync
initial_sync_complete: bool = False

# Scan cycle state
scan_cycle_count: int = 0
next_scan_time: Optional[datetime] = None

# Exchange info cache
EXCHANGE_INFO_CACHE = {"ts": 0.0, "data": None, "ttl": 300}  # ttl seconds

def infer_strategy_for_open_trade_sync(symbol: str, side: str) -> Optional[int]:
    """
    Try to infer the entry timestamp from the actual fill time of the position,
    using account trades first (most accurate), then fall back to the position's
    updateTime, then finally to the most recent closed candle.

    This improves strategy inference accuracy (e.g. distinguishing S6 from S4)
    because using updateTime can reflect later updates (like SL/TP changes)
    rather than the original entry fill time.
    """
    ts_ms: Optional[int] = None
    try:
        if client is not None:
            desired_pos_side = 'LONG' if side == 'BUY' else 'SHORT'

            # 1) Prefer the latest trade fill that contributed to the current position side
            #    This is the most accurate signal time for the entry.
            try:
                trades = client.futures_account_trades(symbol=symbol, limit=50)
                # Find last trade that increased the position on this side
                # For LONG: side == 'BUY' and positionSide == 'LONG'
                # For SHORT: side == 'SELL' and positionSide == 'SHORT'
                contributing = [
                    t for t in trades
                    if str(t.get('positionSide', '')).upper() == desired_pos_side
                    and str(t.get('side', '')).upper() == side
                ]
                if contributing:
                    # Pick the most recent by time
                    last_trade = max(contributing, key=lambda x: int(x.get('time', 0)))
                    if last_trade.get('time'):
                        ts_ms = int(last_trade['time'])
            except Exception:
                # Ignore and fall back to position info
                pass

            # 2) Fallback: use the position's update time (can be later than entry)
            if ts_ms is None:
                positions = client.futures_position_information(symbol=symbol)
                pos = next((p for p in positions if p.get('positionSide') in (desired_pos_side, 'BOTH')), None)
                if pos:
                    ut = pos.get('updateTime') or pos.get('updateTimeMs') or pos.get('time')
                    if ut:
                        ts_ms = int(float(ut))
    except Exception:
        ts_ms = None

    return infer_strategy_for_open_trade_at_time_sync(symbol, side, ts_ms)

def _nearest_closed_index_for_time(df: pd.DataFrame, ts_ms: Optional[int]) -> Optional[int]:
    if df is None or df.empty:
        return None
    if ts_ms is None:
        return len(df) - 1  # last closed
    ts = pd.to_datetime(ts_ms, unit='ms', utc=True)
    # find index of last candle closed at or before ts
    idx = df.index.get_indexer([ts], method='pad')[0]
    if idx is None or idx < 2:
        return None
    return idx

def infer_strategy_for_open_trade_at_time_sync(symbol: str, side: str, ts_ms: Optional[int]) -> Optional[int]:
    """
    Infers strategy likely responsible for an open trade, using signals around a given timestamp (ms).
    Priority: 5, 6, 7, 4, 3, 2, 1.
    If ts_ms is None, uses last closed candles.
    """
    try:
        # Fetch M15
        df = fetch_klines_sync(symbol, CONFIG["TIMEFRAME"], 300)
        if df is None or len(df) < 80:
            return None
        df_ind = calculate_all_indicators(df.copy())

        sig_idx = _nearest_closed_index_for_time(df_ind, ts_ms)
        if sig_idx is None or sig_idx < 3:
            return None

        sig = df_ind.iloc[sig_idx - 1]
        prev = df_ind.iloc[sig_idx - 2]
        ft = df_ind.iloc[sig_idx] if sig_idx < len(df_ind) else df_ind.iloc[-1]

        # 1) S5 check
        try:
            s5 = CONFIG.get('STRATEGY_5', {})
            df_h1 = fetch_klines_sync(symbol, '1h', 300)
            if df_h1 is not None and len(df_h1) >= 80:
                h1_idx = _nearest_closed_index_for_time(df_h1, ts_ms)
                if h1_idx is None or h1_idx < 2:
                    raise RuntimeError("no h1 index")
                df_h1 = df_h1.copy()
                df_h1['ema_fast'] = ema(df_h1['close'], s5.get('EMA_FAST', 21))
                df_h1['ema_slow'] = ema(df_h1['close'], s5.get('EMA_SLOW', 55))
                df_h1['st_h1'], df_h1['st_h1_dir'] = supertrend(df_h1, period=s5.get('H1_ST_PERIOD', 10), multiplier=s5.get('H1_ST_MULT', 3.0))
                h1_last = df_h1.iloc[h1_idx - 1]
                h1_bull = (h1_last['ema_fast'] > h1_last['ema_slow']) and (h1_last['close'] > h1_last['st_h1'])
                h1_bear = (h1_last['ema_fast'] < h1_last['ema_slow']) and (h1_last['close'] < h1_last['st_h1'])

                df_ind['s5_m15_ema_fast'] = ema(df_ind['close'], s5.get('EMA_FAST', 21))
                df_ind['s5_m15_ema_slow'] = ema(df_ind['close'], s5.get('EMA_SLOW', 55))
                df_ind['s5_atr'] = atr(df_ind, s5.get('ATR_PERIOD', 14))
                df_ind['s5_rsi'] = rsi(df_ind['close'], s5.get('RSI_PERIOD', 14))
                df_ind['s5_vol_ma10'] = df_ind['volume'].rolling(10).mean()
                sig = df_ind.iloc[sig_idx - 1]; prev = df_ind.iloc[sig_idx - 2]
                m15_bull_pullback = (sig['s5_m15_ema_fast'] >= sig['s5_m15_ema_slow']) and (prev['low'] <= prev['s5_m15_ema_fast']) and (sig['close'] > sig['s5_m15_ema_fast']) and (sig['close'] > sig['open'])
                m15_bear_pullback = (sig['s5_m15_ema_fast'] <= sig['s5_m15_ema_slow']) and (prev['high'] >= prev['s5_m15_ema_fast']) and (sig['close'] < sig['s5_m15_ema_fast']) and (sig['close'] < sig['open'])
                vol_spike = (sig['volume'] >= 1.2 * sig['s5_vol_ma10']) if pd.notna(sig['s5_vol_ma10']) else False
                rsi_ok = 35 <= sig['s5_rsi'] <= 65
                if (side == 'BUY' and h1_bull and m15_bull_pullback and vol_spike and rsi_ok) or \
                   (side == 'SELL' and h1_bear and m15_bear_pullback and vol_spike and rsi_ok):
                    return 5
        except Exception:
            pass

        # 2) S6 check
        try:
            s6 = CONFIG.get('STRATEGY_6', {})
            df_ind['s6_atr'] = atr(df_ind, s6.get('ATR_PERIOD', 14))
            df_h4 = fetch_klines_sync(symbol, '4h', 200)
            df_d = fetch_klines_sync(symbol, '1d', 200)
            if df_h4 is not None and df_d is not None and len(df_h4) >= 50 and len(df_d) >= 50:
                bias_d = _s6_trend_from_swings(df_d, swing_lookback=20)
                direction = 'BUY' if bias_d == 'BULL' else ('SELL' if bias_d == 'BEAR' else None)
                sig = df_ind.iloc[sig_idx - 1]; prev = df_ind.iloc[sig_idx - 2]; ft = df_ind.iloc[min(sig_idx, len(df_ind)-1)]
                if direction == side:
                    is_pin = _s6_is_pin_bar(sig, direction)
                    is_engulf = _s6_is_engulfing_reclaim(sig, prev, direction, float(sig['close']))
                    vol_ma = float(df_ind['volume'].rolling(int(s6.get('VOL_MA_LEN', 10))).mean().iloc[sig_idx - 1])
                    if (is_pin or is_engulf) and _s6_follow_through_ok(sig, ft, direction, vol_ma, float(s6.get('FOLLOW_THROUGH_RATIO', 0.7))):
                        return 6
        except Exception:
            pass

        # 3) S7 check
        try:
            s7 = CONFIG.get('STRATEGY_7', {})
            df_h1 = fetch_klines_sync(symbol, '1h', 300)
            if df_h1 is not None and len(df_h1) >= 120:
                lookback = int(s7.get('BOS_LOOKBACK_H1', 72))
                h1_idx = _nearest_closed_index_for_time(df_h1, ts_ms)
                if h1_idx is None or h1_idx < lookback + 2:
                    raise RuntimeError("no h1 idx")
                sig_h1 = df_h1.iloc[h1_idx - 1]
                prev_window_high = float(df_h1['high'].iloc[(h1_idx - lookback - 2):(h1_idx - 1)].max())
                prev_window_low = float(df_h1['low'].iloc[(h1_idx - lookback - 2):(h1_idx - 1)].min())
                dir_detected = 'BUY' if float(sig_h1['close']) > prev_window_high else ('SELL' if float(sig_h1['close']) < prev_window_low else None)
                if dir_detected == side:
                    sig = df_ind.iloc[sig_idx - 1]; prev = df_ind.iloc[sig_idx - 2]
                    is_pin = _s6_is_pin_bar(sig, side)
                    is_engulf = _s6_is_engulfing_reclaim(sig, prev, side, float(sig['close']))
                    if is_pin or is_engulf:
                        return 7
        except Exception:
            pass

        # 4) Fallback sims
        try:
            sim4 = simulate_strategy_4(symbol, df_ind)
            if sim4 and sim4.get('side') == side:
                return 4
        except Exception:
            pass
        try:
            sim3 = simulate_strategy_3(symbol, df_ind)
            if sim3 and sim3.get('side') == side:
                return 3
        except Exception:
            pass
        try:
            sim2 = simulate_strategy_supertrend(symbol, df_ind)
            if sim2 and sim2.get('side') == side:
                return 2
        except Exception:
            pass
        try:
            sim1 = simulate_strategy_bb(symbol, df_ind)
            if sim1 and sim1.get('side') == side:
                return 1
        except Exception:
            pass

        return None
    except Exception:
        return None
    except Exception:
        return None

async def _import_rogue_position_async(symbol: str, position: Dict[str, Any]) -> Optional[tuple[str, Dict[str, Any]]]:
    """
    Imports a single rogue position, places a default SL order, and returns the trade metadata.
    """
    try:
        log.info(f"❗️ Rogue position for {symbol} detected. Importing for management...")
        entry_price = float(position['entryPrice'])
        qty = abs(float(position['positionAmt']))
        side = 'BUY' if float(position['positionAmt']) > 0 else 'SELL'
        leverage = int(position.get('leverage', CONFIG.get("MAX_BOT_LEVERAGE", 20)))
        notional = qty * entry_price

        try:
            # default_sl_tp_for_import returns three values now
            stop_price, _, current_price = await asyncio.to_thread(default_sl_tp_for_import, symbol, entry_price, side)
        except RuntimeError as e:
            log.error(f"Failed to calculate default SL for {symbol}: {e}")
            return None

        # --- Safety Check for SL Placement ---
        if side == 'BUY' and stop_price >= current_price:
            log.warning(f"Rogue import for {symbol} calculated an invalid SL ({stop_price}) which is >= current price ({current_price}). Skipping SL placement.")
            stop_price = None # Do not place an SL
        elif side == 'SELL' and stop_price <= current_price:
            log.warning(f"Rogue import for {symbol} calculated an invalid SL ({stop_price}) which is <= current price ({current_price}). Skipping SL placement.")
            stop_price = None # Do not place an SL

        # Infer strategy for better in-trade management. The sync function returns only an int (or None).
        inferred_strategy = await asyncio.to_thread(infer_strategy_for_open_trade_sync, symbol, side)
        infer_src = "last_closed"
        if inferred_strategy is None:
            inferred_strategy = 4  # fallback

        trade_id = f"{symbol}_imported_{int(time.time())}"
        meta = {
            "id": trade_id, "symbol": symbol, "side": side, "entry_price": entry_price,
            "initial_qty": qty, "qty": qty, "notional": notional, "leverage": leverage,
            "sl": stop_price if stop_price is not None else 0.0, "tp": 0.0, "open_time": datetime.utcnow().isoformat(),
            "sltp_orders": {}, "trailing": CONFIG["TRAILING_ENABLED"],
            "dyn_sltp": False, "tp1": None, "tp2": None, "tp3": None,
            "trade_phase": 0, "be_moved": False, "risk_usdt": 0.0,
            "strategy_id": inferred_strategy,
            "inference_time_source": infer_src,
        }

        # Initialize S5-specific management state if inferred as S5
        if inferred_strategy == 5 and stop_price is not None and stop_price > 0:
            r_dist = abs(entry_price - stop_price)
            meta['s5_initial_sl'] = stop_price
            meta['s5_r_per_unit'] = r_dist
            meta['s5_tp1_price'] = (entry_price + r_dist) if side == 'BUY' else (entry_price - r_dist)
            meta['trade_phase'] = 0
            meta['be_moved'] = False
            meta['trailing_active'] = False

        await asyncio.to_thread(add_managed_trade_to_db, meta)

        await asyncio.to_thread(cancel_close_orders_sync, symbol)
        
        if stop_price is not None:
            log.info(f"Attempting to place SL for imported trade {symbol}. SL={stop_price}, Qty={qty}")
            # Pass tp_price=None to prevent placing a Take Profit order
            await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=stop_price, tp_price=None, qty=qty)
            
            msg = (f"ℹ️ **Position Imported**\n\n"
                   f"Found and imported a position for **{symbol}**.\n\n"
                   f"**Side:** {side}\n"
                   f"**Entry Price:** {entry_price}\n"
                   f"**Quantity:** {qty}\n\n"
                   f"**Inferred Strategy:** S{inferred_strategy}\n"
                   f"A default SL has been calculated and placed:\n"
                   f"**SL:** `{round_price(symbol, stop_price)}`\n\n"
                   f"The bot will now manage this trade.")
            await asyncio.to_thread(send_telegram, msg)
        else:
            log.warning(f"No valid SL placed for imported trade {symbol}. Please manage manually.")
            msg = (f"ℹ️ **Position Imported (No SL)**\n\n"
                   f"Found and imported a position for **{symbol}** but could not place a valid SL.\n\n"
                   f"**Inferred Strategy:** S{inferred_strategy}\n"
                   f"**Please manage this trade manually.**")
            await asyncio.to_thread(send_telegram, msg)

        return trade_id, meta
    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"Failed to import rogue position for {symbol}. Please manage it manually.", e)
        return None

async def reconcile_open_trades():
    global managed_trades
    log.info("--- Starting Trade Reconciliation (with DB data) ---")

    db_trades = {}
    async with managed_trades_lock:
        db_trades = dict(managed_trades)
    
    log.info(f"Found {len(db_trades)} managed trade(s) in DB to reconcile.")

    try:
        if client is None:
            log.warning("Binance client not initialized. Cannot fetch positions for reconciliation.")
            return
        
        positions = await asyncio.to_thread(client.futures_position_information)
        open_positions = {
            pos['symbol']: pos for pos in positions if float(pos.get('positionAmt', 0.0)) != 0.0
        }
        log.info(f"Found {len(open_positions)} open position(s) on Binance.")

    except Exception as e:
        log.exception("Failed to fetch Binance positions during reconciliation.")
        await asyncio.to_thread(send_telegram, f"⚠️ **CRITICAL**: Failed to fetch positions from Binance during startup reconciliation: {e}. The bot may not manage existing trades correctly.")
        managed_trades = {}
        return

    retained_trades = {}
    
    # 1. Reconcile trades that are already in the database
    for trade_id, trade_meta in db_trades.items():
        symbol = trade_meta['symbol']
        if symbol in open_positions:
            log.info(f"✅ Reconciled DB trade: {trade_id} ({symbol}) is active. Restoring.")
            retained_trades[trade_id] = trade_meta
        else:
            log.warning(f"ℹ️ Reconciled DB trade: {trade_id} ({symbol}) is closed on Binance. Archiving.")
            # This part could be enhanced to fetch last trade details for accurate PnL
            await asyncio.to_thread(
                record_trade,
                {
                    'id': trade_id, 'symbol': symbol, 'side': trade_meta['side'],
                    'entry_price': trade_meta['entry_price'], 'exit_price': None, # Exit price is unknown
                    'qty': trade_meta['initial_qty'], 'notional': trade_meta['notional'], 
                    'pnl': 0.0, 'open_time': trade_meta['open_time'], 
                    'close_time': datetime.utcnow().isoformat(),
                    'risk_usdt': trade_meta.get('risk_usdt', 0.0)
                }
            )
            await asyncio.to_thread(remove_managed_trade_from_db, trade_id)

    # 2. Import "rogue" positions that are on the exchange but not in the DB
    managed_symbols = {t['symbol'] for t in retained_trades.values()}
    for symbol, position in open_positions.items():
        if symbol not in managed_symbols:
            result = await _import_rogue_position_async(symbol, position)
            if result:
                trade_id, meta = result
                retained_trades[trade_id] = meta

    async with managed_trades_lock:
        managed_trades.clear()
        managed_trades.update(retained_trades)
    
    log.info(f"--- Reconciliation Complete. {len(managed_trades)} trades are now being managed. ---")


async def check_and_import_rogue_trades():
    """
    Periodically checks for and imports "rogue" positions that exist on the
    exchange but are not managed by the bot.
    """
    global managed_trades, notified_rogue_symbols
    log.info("Checking for rogue positions...")

    try:
        if client is None:
            log.warning("Binance client not initialized. Cannot check for rogue trades.")
            return

        # Get all open positions from the exchange
        positions = await asyncio.to_thread(client.futures_position_information)
        open_positions = {
            pos['symbol']: pos for pos in positions if float(pos.get('positionAmt', 0.0)) != 0.0
        }

        # Get symbols of trades currently managed by the bot
        async with managed_trades_lock:
            managed_symbols = {t['symbol'] for t in managed_trades.values()}
        
        # Determine which open positions are "rogue"
        rogue_symbols = set(open_positions.keys()) - managed_symbols

        if not rogue_symbols:
            log.info("No rogue positions found.")
            return

        for symbol in rogue_symbols:
            if symbol in notified_rogue_symbols:
                log.debug(f"Ignoring already notified rogue symbol: {symbol}")
                continue

            # Mark as notified BEFORE attempting import to prevent spam on repeated failures.
            notified_rogue_symbols.add(symbol)
            position = open_positions[symbol]
            
            result = await _import_rogue_position_async(symbol, position)
            if result:
                trade_id, meta = result
                async with managed_trades_lock:
                    managed_trades[trade_id] = meta
    
    except Exception as e:
        log.exception("An unhandled exception occurred in check_and_import_rogue_trades.")


async def periodic_rogue_check_loop():
    """
    A background task that runs periodically to check for and import rogue trades.
    """
    log.info("Starting periodic rogue position checker loop.")
    while True:
        try:
            # Wait for 1 hour before the next check
            await asyncio.sleep(3600)

            if not running:
                log.debug("Bot is not running, skipping hourly rogue position check.")
                continue
            
            await check_and_import_rogue_trades()

        except asyncio.CancelledError:
            log.info("Periodic rogue position checker loop cancelled.")
            break
        except Exception as e:
            log.exception("An unhandled error occurred in the periodic rogue check loop.")
            # Wait a bit before retrying to avoid spamming errors
            await asyncio.sleep(60)


def load_state_from_db_sync():
    """
    Loads pending orders and managed trades from the SQLite DB into memory on startup.
    """
    global pending_limit_orders, managed_trades
    log.info("--- Loading State from Database ---")
    
    # Load managed trades
    db_trades = load_managed_trades_from_db()
    if db_trades:
        with managed_trades_lock:
            managed_trades.update(db_trades)
        log.info(f"Loaded {len(db_trades)} managed trade(s) from DB.")
    else:
        log.info("No managed trades found in DB.")

    # Load pending orders
    db_orders = load_pending_orders_from_db()
    if db_orders:
        with pending_limit_orders_lock:
            pending_limit_orders.update(db_orders)
        log.info(f"Loaded {len(db_orders)} pending order(s) from DB.")
    else:
        log.info("No pending orders found in DB.")


# -------------------------
# App Lifespan Manager
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scan_task, telegram_thread, monitor_thread_obj, pnl_monitor_thread_obj, client, monitor_stop_event, main_loop
    log.info("EMA/BB Strategy Bot starting up...")
    
    # --- Startup Logic ---
    init_db()
    
    await asyncio.to_thread(load_state_from_db_sync)

    main_loop = asyncio.get_running_loop()

    ok, err = await asyncio.to_thread(init_binance_client_sync)
    
    if ok:
        await reconcile_open_trades()

    await asyncio.to_thread(validate_and_sanity_check_sync, True)

    # --- Initialize per-symbol evaluation locks ---
    global symbol_evaluation_locks
    for symbol in CONFIG.get("SYMBOLS", []):
        symbol_evaluation_locks[symbol] = asyncio.Lock()
    log.info(f"Initialized evaluation locks for {len(symbol_evaluation_locks)} symbols.")

    # --- Initialize S4 confirmation state ---
    global s4_confirmation_state
    for symbol in CONFIG.get("SYMBOLS", []):
        s4_confirmation_state[symbol] = {
            'buy_sequence_started': False,
            'sell_sequence_started': False,
            'buy_trade_taken': False,
            'sell_trade_taken': False,
        }
    log.info(f"Initialized S4 stateful confirmation state for {len(s4_confirmation_state)} symbols.")

    if client is not None:
        scan_task = main_loop.create_task(scanning_loop())
        monitor_stop_event.clear()
        monitor_thread_obj = threading.Thread(target=monitor_thread_func, daemon=True)
        monitor_thread_obj.start()
        log.info("Started monitor thread.")

        pnl_monitor_thread_obj = threading.Thread(target=daily_pnl_monitor_thread_func, daemon=True)
        pnl_monitor_thread_obj.start()
        log.info("Started daily PnL monitor thread.")

        maintenance_thread_obj = threading.Thread(target=monthly_maintenance_thread_func, daemon=True)
        maintenance_thread_obj.start()
        log.info("Started monthly maintenance thread.")

        alerter_thread_obj = threading.Thread(target=performance_alerter_thread_func, daemon=True)
        alerter_thread_obj.start()
        log.info("Started performance alerter thread.")
    else:
        log.warning("Binance client not initialized -> scanning and monitor threads not started.")

    if telegram_bot:
        telegram_thread = threading.Thread(target=telegram_polling_thread, args=(main_loop,), daemon=True)
        telegram_thread.start()
        log.info("Started telegram polling thread.")
    else:
        log.info("Telegram not configured; telegram thread not started.")
    
    try:
        await asyncio.to_thread(send_telegram, "EMA/BB Strategy Bot started. Running={}".format(running))
    except Exception:
        log.exception("Failed to send startup telegram")

    yield

    # --- Shutdown Logic ---
    log.info("EMA/BB Strategy Bot shutting down...")
    if scan_task:
        scan_task.cancel()
        try:
            await scan_task
        except asyncio.CancelledError:
            log.info("Scanning loop task cancelled successfully.")

    if rogue_check_task:
        rogue_check_task.cancel()
        try:
            await rogue_check_task
        except asyncio.CancelledError:
            log.info("Rogue position checker task cancelled successfully.")

    monitor_stop_event.set()
    if monitor_thread_obj and monitor_thread_obj.is_alive():
        monitor_thread_obj.join(timeout=5)
    if pnl_monitor_thread_obj and pnl_monitor_thread_obj.is_alive():
        pnl_monitor_thread_obj.join(timeout=5)
    
    if telegram_thread and telegram_thread.is_alive():
        # The telegram thread is daemon, so it will exit automatically.
        # We already set the monitor_stop_event which the telegram thread also checks.
        pass

    try:
        await asyncio.to_thread(send_telegram, "EMA/BB Strategy Bot shut down.")
    except Exception:
        pass
    log.info("Shutdown complete.")


app = FastAPI(lifespan=lifespan)

# ------------- Debug Endpoints -------------
@app.get("/debug/scan_once")
async def debug_scan_once(symbols: Optional[str] = None):
    """
    Runs a one-time evaluation-and-enter pass for the provided symbols in DRY_RUN mode
    so no real orders are placed. Returns a simple JSON summary.
    Usage: GET /debug/scan_once?symbols=BTCUSDT,ETHUSDT
    """
    # Parse symbol list or default to configured symbols
    if symbols:
        sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    else:
        sym_list = [s.strip().upper() for s in CONFIG.get("SYMBOLS", []) if s.strip()]

    # Force DRY_RUN for safety during debug scan
    original_dry = CONFIG.get("DRY_RUN", False)
    CONFIG["DRY_RUN"] = True

    results = {}
    try:
        for s in sym_list:
            try:
                await evaluate_and_enter(s)
                results[s] = "ok"
            except Exception as e:
                results[s] = f"error: {type(e).__name__}: {e}"
        return {"status": "ok", "dry_run": True, "symbols": sym_list, "results": results}
    finally:
        # Restore original DRY_RUN configuration
        CONFIG["DRY_RUN"] = original_dry

# -------------------------
# Utilities
# -------------------------
# Utilities
# -------------------------
def _shorten_for_telegram(text: str, max_len: int = 3500) -> str:
    if not isinstance(text, str):
        text = str(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 200] + "\n\n[...] (truncated)\n\n" + text[-200:]


def format_timedelta(td) -> str:
    """Formats a timedelta object into a human-readable string."""
    from datetime import timedelta
    if not isinstance(td, timedelta) or td.total_seconds() < 0:
        return "N/A"

    seconds = int(td.total_seconds())
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} day" + ("s" if days != 1 else ""))
    if hours > 0:
        parts.append(f"{hours} hour" + ("s" if hours != 1 else ""))
    if minutes > 0:
        parts.append(f"{minutes} minute" + ("s" if minutes != 1 else ""))
    if seconds > 0 or not parts:
        parts.append(f"{seconds} second" + ("s" if seconds != 1 else ""))

    return ", ".join(parts)


def get_public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=5).text
    except Exception:
        return "unable-to-fetch-ip"

def default_sl_tp_for_import(symbol: str, entry_price: float, side: str) -> tuple[float, float, float]:
    """
    Derive a safe default SL for an imported position.
    Primary: use S4 ST2 supertrend as stop. If invalid (wrong side of price), fall back to ATR-based distance.
    Returns: (stop_price, take_price, current_price). take_price is 0.0 for imports (no TP by default).
    """
    df = fetch_klines_sync(symbol, CONFIG["TIMEFRAME"], 300)
    if df is None or df.empty:
        raise RuntimeError("No kline data to calc default SL/TP")

    # Compute Supertrend (S4 ST2) and ATR
    s4_params = CONFIG['STRATEGY_4']
    st2, _ = supertrend(df.copy(), period=s4_params['ST2_PERIOD'], multiplier=s4_params['ST2_MULT'])
    df['atr'] = atr(df, CONFIG.get("ATR_LENGTH", 14))

    current_price = safe_last(df['close'])
    atr_now = max(1e-9, safe_last(df['atr']))  # avoid zero
    stop_price = safe_last(st2)

    # Validate side and correct if invalid
    if side == 'BUY':
        if stop_price >= current_price or stop_price <= 0:
            stop_price = current_price - 1.5 * atr_now
    else:  # SELL
        if stop_price <= current_price or stop_price <= 0:
            stop_price = current_price + 1.5 * atr_now

    # Final safety: ensure non-negative and reasonable distance
    if not np.isfinite(stop_price) or stop_price <= 0:
        pct = 0.02
        stop_price = current_price * (1 - pct) if side == 'BUY' else current_price * (1 + pct)

    take_price = 0.0
    return stop_price, take_price, current_price

def timeframe_to_timedelta(tf: str) -> Optional[timedelta]:
    """Converts a timeframe string like '1m', '5m', '1h', '1d' to a timedelta object."""
    match = re.match(r'(\d+)([mhd])', tf)
    if not match:
        return None
    val, unit = match.groups()
    val = int(val)
    if unit == 'm':
        return timedelta(minutes=val)
    elif unit == 'h':
        return timedelta(hours=val)
    elif unit == 'd':
        return timedelta(days=val)
    return None

def send_telegram(msg: str, document_content: Optional[bytes] = None, document_name: str = "error.html", parse_mode: Optional[str] = None):
    """
    Synchronously sends a message to Telegram. Can optionally attach a document.
    This is a blocking call.
    """
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; message: %s", msg[:200])
        return
    
    safe_msg = _shorten_for_telegram(msg)
    try:
        if document_content:
            doc_stream = io.BytesIO(document_content)
            doc_stream.name = document_name
            telegram_bot.send_document(
                chat_id=int(TELEGRAM_CHAT_ID),
                document=doc_stream,
                caption=safe_msg,
                timeout=30,
                parse_mode=parse_mode
            )
        else:
            telegram_bot.send_message(
                chat_id=int(TELEGRAM_CHAT_ID), 
                text=safe_msg,
                timeout=30,
                parse_mode=parse_mode
            )
    except Exception:
        log.exception("Failed to send telegram message")


def _symbol_base_asset(symbol: str) -> str:
    """
    Try to infer the base asset for a symbol like BTCUSDT -> BTC.
    Falls back to stripping a stable-quote suffix.
    """
    try:
        si = get_symbol_info(symbol)
        if si and 'baseAsset' in si:
            return str(si['baseAsset']).upper()
    except Exception:
        pass
    # Fallback heuristics
    for quote in ("USDT", "BUSD", "USDC", "FDUSD", "TUSD", "BTC", "ETH"):
        if symbol.upper().endswith(quote):
            return symbol.upper()[: -len(quote)]
    return symbol.upper()


def _alphavantage_time_from(dt: datetime) -> str:
    """
    Format datetime for Alpha Vantage NEWS_SENTIMENT time_from param: YYYYMMDDTHHMM
    """
    return dt.strftime("%Y%m%dT%H%M")


def fetch_recent_news_impact(symbol: str, hours: int = 24, max_items: int = 3) -> Dict[str, Any]:
    """
    Fetch recent news using Alpha Vantage NEWS_SENTIMENT for the base asset and
    provide a lightweight impact classification and reason text.
    Returns dict: {impact, reason, articles:[{title,url,time,summary}]}
    """
    base = _symbol_base_asset(symbol)
    api_key = ALPHA_VANTAGE_API_KEY or ""
    if not api_key:
        return {"impact": "None", "reason": "No API key configured for news.", "articles": []}
    # Try a few ticker formats to increase hit rate
    candidates = [f"CRYPTO:{base}", base]
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    time_from = _alphavantage_time_from(cutoff)
    articles: list[dict] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "ema-bb-bot/1.0"})
    for t in candidates:
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "NEWS_SENTIMENT",
                "tickers": t,
                "sort": "LATEST",
                "time_from": time_from,
                "apikey": api_key,
            }
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            feed = data.get("feed") or []
            for item in feed:
                try:
                    ts = item.get("time_published")
                    # format: 20250101T120000
                    dt = datetime.strptime(ts, "%Y%m%dT%H%M%S") if ts else None
                    title = item.get("title") or ""
                    url_i = item.get("url") or ""
                    summary = item.get("summary") or ""
                    # Vendor sentiment may exist
                    overall = item.get("overall_sentiment_label") or ""
                    articles.append({
                        "time": dt.isoformat() if dt else ts,
                        "title": title[:180],
                        "url": url_i,
                        "summary": summary[:280],
                        "sentiment": overall
                    })
                except Exception:
                    continue
            if articles:
                break
        except Exception:
            continue

    if not articles:
        return {"impact": "None", "reason": "No recent news found.", "articles": []}

    # Simple impact scoring
    positive_kw = [
        "etf approval", "etf inflow", "partnership", "integration", "upgrade",
        "mainnet", "testnet", "milestone", "adoption", "institutional", "launch", "listing", "raised"
    ]
    negative_kw = [
        "hack", "exploit", "outage", "downtime", "regulatory", "ban", "lawsuit",
        "sec sues", "delist", "bug", "halt", "penalty", "warning", "vulnerability"
    ]
    pos, neg = 0, 0
    reasons = []
    for a in articles[:max_items]:
        text = f"{a.get('title','')} {a.get('summary','')}".lower()
        # bias by vendor sentiment if present
        s = (a.get("sentiment") or "").lower()
        if "positive" in s:
            pos += 1
        elif "negative" in s:
            neg += 1
        # keyword scoring
        for kw in positive_kw:
            if kw in text:
                pos += 1
                reasons.append(f"+ {kw}")
        for kw in negative_kw:
            if kw in text:
                neg += 1
                reasons.append(f"- {kw}")

    if pos > neg and (pos - neg) >= 1:
        impact = "Positive"
    elif neg > pos and (neg - pos) >= 1:
        impact = "Negative"
    else:
        impact = "Neutral"

    reason = ", ".join(reasons[:5]) if reasons else "Mixed/low-signal headlines in the last 24h."
    return {"impact": impact, "reason": reason, "articles": articles[:max_items]}


def log_and_send_error(context_msg: str, exc: Optional[Exception] = None):
    """
    Logs an exception and sends a formatted error message to Telegram.
    This is a synchronous, blocking call.
    """
    # Log the full traceback to the console/log file
    if exc:
        log.exception(f"Error during '{context_msg}': {exc}")
    else:
        log.error(f"Error during '{context_msg}' (no exception details).")

    # For Binance API exceptions, extract more details
    if exc and isinstance(exc, BinanceAPIException):
        error_details = f"Code: {exc.code}, Message: {exc.message}"
    elif exc:
        error_details = str(exc)
    else:
        error_details = "N/A"

    # Consolidate dynamic content into a single block to avoid parsing errors
    details_text = (
        f"Context: {context_msg}\n"
        f"Error Type: {type(exc).__name__ if exc else 'N/A'}\n"
        f"Details: {error_details}"
    )

    telegram_msg = (
        f"🚨 **Bot Error** 🚨\n\n"
        f"```\n{details_text}\n```\n\n"
        f"Check the logs for the full traceback if available."
    )
    
    # Send the message, using Markdown for formatting
    send_telegram(telegram_msg, parse_mode='Markdown')


def _json_native(val: Any) -> Any:
    """Convert numpy/pandas/scalars to JSON-serializable native Python types."""
    try:
        # Numpy scalars -> native
        if isinstance(val, np.generic):
            return val.item()
        # Pandas Timestamp -> ISO string
        if isinstance(val, pd.Timestamp):
            return val.isoformat()
        # Numpy arrays -> list
        if isinstance(val, np.ndarray):
            return val.tolist()
        # Fallback: leave as-is (json.dumps will handle str, int, float, bool, None, dict, list)
        return val
    except Exception:
        # Last resort: stringify
        return str(val)

def _record_rejection(symbol: str, reason: str, details: dict, signal_candle: Optional[pd.Series] = None):
    """Adds a rejected trade event to the deque and persists it to a file."""
    global rejected_trades

    # Enrich details with key indicator values from the signal candle if available
    if signal_candle is not None:
        indicator_keys = ['close', 'rsi', 'adx', 's1_bbu', 's1_bbl', 's2_st', 's4_st1', 's4_st2', 's4_st3']
        for key in indicator_keys:
            if key in signal_candle and pd.notna(signal_candle[key]):
                details[key] = signal_candle[key]

    # Normalize to JSON-native types first (so numpy types don't break json.dumps)
    native_details = {k: _json_native(v) for k, v in details.items()}

    # Format floats in details to a reasonable precision for display
    formatted_details = {
        k: (f"{v:.4f}" if isinstance(v, float) else v)
        for k, v in native_details.items()
    }

    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "reason": reason,
        "details": formatted_details
    }
    
    # 1. Append to in-memory deque
    rejected_trades.append(record)
    
    # 2. Persist to file
    try:
        with open("rejections.jsonl", "a") as f:
            f.write(json.dumps(record) + "\n")
    except Exception as e:
        log.error(f"Failed to write rejection to file: {e}")

    # Use info level for rejection logs to make them visible.
    log.info(f"Rejected trade for {symbol}. Reason: {reason}, Details: {formatted_details}")

    # --- Conditional Telegram Notification ---
    if CONFIG.get("TELEGRAM_NOTIFY_REJECTIONS", False):
        details_str = ", ".join([f"{k}: {v}" for k, v in formatted_details.items()])
        msg = (
            f"🚫 Trade Rejected\n\n"
            f"Symbol: {symbol}\n"
            f"Reason: {reason}\n"
            f"Details: {details_str}"
        )
        send_telegram(msg)


def handle_reject_cmd():
    """Formats the last 20 in-memory rejections for Telegram with emojis and cleaner layout."""
    global rejected_trades
    if not rejected_trades:
        send_telegram("No rejected trades have been recorded in memory since the last restart.")
        return

    header = "🧾 Last 20 Rejected Trades (memory)"
    sections = [f"*{header}*"]

    # Newest first
    for reject in reversed(list(rejected_trades)):
        try:
            ts = datetime.fromisoformat(reject['timestamp']).strftime('%H:%M:%S')
        except Exception:
            ts = "N/A"
        symbol = reject.get('symbol', 'N/A')
        reason = reject.get('reason', 'Unknown')
        details = reject.get('details') or {}

        # Pick an emoji based on the reason
        emoji = REJECTION_REASON_EMOJI.get(reason, "⚠️")

        # If details is empty, add helpful defaults
        if not details:
            details = {
                "note": "No extra diagnostics captured for this rejection.",
                "timeframe": CONFIG.get("TIMEFRAME", "N/A"),
            }

        # Build details block as bullet list
        detail_lines = []
        for k, v in details.items():
            # Normalize to simple scalars/strings
            try:
                if isinstance(v, float):
                    v_fmt = f"{v:.4f}"
                else:
                    v_fmt = str(v)
            except Exception:
                v_fmt = str(v)
            detail_lines.append(f"   - {k}: {v_fmt}")

        section = [
            f"{emoji} `{ts}` — *{symbol}*",
            f"• Reason: {reason}",
            *detail_lines
        ]
        sections.append("\n".join(section))

    # Join and send as a single Telegram message
    message = "\n\n".join(sections)
    send_telegram(message, parse_mode="Markdown")


SESSION_FREEZE_WINDOWS = {
    "London": (7, 9),
    "New York": (12, 14),
    "Tokyo": (23, 1)  # Crosses midnight
}


def get_merged_freeze_intervals() -> list[tuple[datetime, datetime, str]]:
    """
    Calculates and merges all freeze windows for the current and next day.
    This handles overlaps and contiguous sessions, returning a clean list of
    absolute (start_datetime, end_datetime, session_name) intervals.
    """
    from datetime import timedelta

    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    tomorrow = today + timedelta(days=1)
    day_after = today + timedelta(days=2)

    intervals = []
    # Get all intervals for today and tomorrow
    for name, (start_hour, end_hour) in SESSION_FREEZE_WINDOWS.items():
        if start_hour < end_hour:  # Same day window
            # Today's window
            intervals.append((
                datetime(today.year, today.month, today.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(today.year, today.month, today.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))
            # Tomorrow's window
            intervals.append((
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))
        else:  # Overnight window
            # Today into Tomorrow
            intervals.append((
                datetime(today.year, today.month, today.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))
            # Tomorrow into Day After
            intervals.append((
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(day_after.year, day_after.month, day_after.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))

    # Sort intervals by start time
    intervals.sort(key=lambda x: x[0])

    if not intervals:
        return []

    # Merge overlapping intervals
    merged = []
    current_start, current_end, current_names = intervals[0]
    current_names = {current_names}

    for next_start, next_end, next_name in intervals[1:]:
        if next_start <= current_end:
            # Overlap or contiguous, merge them
            current_end = max(current_end, next_end)
            current_names.add(next_name)
        else:
            # No overlap, finish the current merged interval
            merged.append((current_start, current_end, " & ".join(sorted(list(current_names)))))
            # Start a new one
            current_start, current_end, current_names = next_start, next_end, {next_name}

    # Add the last merged interval
    merged.append((current_start, current_end, " & ".join(sorted(list(current_names)))))
    
    # Filter out intervals that have already completely passed
    final_intervals = [m for m in merged if now_utc < m[1]]

    return final_intervals


def get_session_freeze_status(now: datetime) -> tuple[bool, Optional[str]]:
    """
    Checks if the current time is within a session freeze window using the merged intervals.
    Returns a tuple of (is_frozen, session_name).
    """
    merged_intervals = get_merged_freeze_intervals()
    for start, end, name in merged_intervals:
        if start <= now < end:
            return True, name
    return False, None


# (The rest of the file from DB Helpers to the end remains the same, except for removing the old startup/shutdown events)
# ... I will paste the full code below ...

# -------------------------
# DB helpers
# -------------------------
def init_db():
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    # Historical trades table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        symbol TEXT,
        side TEXT,
        entry_price REAL,
        exit_price REAL,
        qty REAL,
        notional REAL,
        risk_usdt REAL,
        pnl REAL,
        open_time TEXT,
        close_time TEXT
    )
    """)
    # Add column if it doesn't exist for backward compatibility
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN risk_usdt REAL")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            raise
    
    # Add new columns for enhanced reporting
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN entry_reason TEXT")
    except sqlite3.OperationalError: pass # Ignore if column exists
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN exit_reason TEXT")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN tp1 REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN tp2 REAL")
    except sqlite3.OperationalError: pass

    # --- New columns for SuperTrend Strategy ---
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN strategy_id INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN signal_confidence REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN adx_confirmation REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN rsi_confirmation REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN macd_confirmation REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN atr_at_entry REAL")
    except sqlite3.OperationalError: pass

    # Persistent open trades table for crash recovery
    cur.execute("""
    CREATE TABLE IF NOT EXISTS managed_trades (
        id TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_price REAL NOT NULL,
        initial_qty REAL NOT NULL,
        qty REAL NOT NULL,
        notional REAL NOT NULL,
        leverage INTEGER NOT NULL,
        sl REAL NOT NULL,
        tp REAL NOT NULL,
        open_time TEXT NOT NULL,
        sltp_orders TEXT,
        trailing INTEGER NOT NULL,
        dyn_sltp INTEGER NOT NULL,
        tp1 REAL,
        tp2 REAL,
        tp3 REAL,
        trade_phase INTEGER NOT NULL,
        be_moved INTEGER NOT NULL,
        risk_usdt REAL NOT NULL,
        strategy_id INTEGER,
        atr_at_entry REAL
    )
    """)
    # Add strategy_id for strategy-specific logic in monitor thread
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN strategy_id INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN atr_at_entry REAL")
    except sqlite3.OperationalError: pass
    # --- New columns for Strategy 3 ---
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s3_trailing_active INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s3_trailing_stop REAL")
    except sqlite3.OperationalError: pass
    # --- New columns for Strategy 4 ---
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s4_trailing_stop REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s4_last_candle_ts TEXT")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s4_trailing_active INTEGER")
    except sqlite3.OperationalError: pass

    # Table for symbols that require manual attention
    cur.execute("""
    CREATE TABLE IF NOT EXISTS attention_required (
        symbol TEXT PRIMARY KEY,
        reason TEXT,
        details TEXT,
        timestamp TEXT
    )
    """)

    # --- New table for pending limit orders ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending_limit_orders (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        qty REAL NOT NULL,
        limit_price REAL NOT NULL,
        stop_price REAL NOT NULL,
        take_price REAL NOT NULL,
        leverage INTEGER NOT NULL,
        risk_usdt REAL NOT NULL,
        place_time TEXT NOT NULL,
        expiry_time TEXT,
        strategy_id INTEGER,
        atr_at_entry REAL,
        trailing INTEGER
    )
    """)

    conn.commit()
    conn.close()

    # --- Ensure rejections file exists ---
    try:
        # "touch" the file to ensure it's created on startup if it doesn't exist
        with open("rejections.jsonl", "a"):
            pass
        log.info("Ensured rejections.jsonl file exists.")
    except Exception as e:
        log.error(f"Could not create rejections.jsonl file: {e}")


def add_pending_order_to_db(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()

    # Coalesce NOT NULL columns to safe defaults
    stop_val = rec.get('stop_price', 0.0)
    if stop_val is None:
        stop_val = 0.0
    take_val = rec.get('take_price', 0.0)
    if take_val is None:
        take_val = 0.0

    values = (
        rec.get('id'), rec.get('order_id'), rec.get('symbol'), rec.get('side'),
        rec.get('qty'), rec.get('limit_price'), stop_val, take_val,
        rec.get('leverage'), rec.get('risk_usdt'), rec.get('place_time'), rec.get('expiry_time'),
        rec.get('strategy_id'), rec.get('atr_at_entry'), int(rec.get('trailing', False))
    )
    cur.execute("""
    INSERT OR REPLACE INTO pending_limit_orders (
        id, order_id, symbol, side, qty, limit_price, stop_price, take_price,
        leverage, risk_usdt, place_time, expiry_time, strategy_id, atr_at_entry, trailing
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()

def remove_pending_order_from_db(pending_order_id: str):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("DELETE FROM pending_limit_orders WHERE id = ?", (pending_order_id,))
    conn.commit()
    conn.close()

def load_pending_orders_from_db() -> Dict[str, Dict[str, Any]]:
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM pending_limit_orders")
    rows = cur.fetchall()
    conn.close()

    orders = {}
    for row in rows:
        rec = dict(row)
        rec['trailing'] = bool(rec.get('trailing'))
        orders[rec['id']] = rec
    return orders

def record_trade(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO trades (
        id,symbol,side,entry_price,exit_price,qty,notional,risk_usdt,pnl,
        open_time,close_time,entry_reason,exit_reason,tp1,tp2,
        strategy_id, signal_confidence, adx_confirmation, rsi_confirmation, macd_confirmation,
        atr_at_entry
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        rec.get('id'), rec.get('symbol'), rec.get('side'), rec.get('entry_price'), rec.get('exit_price'),
        rec.get('qty'), rec.get('notional'), rec.get('risk_usdt'), rec.get('pnl'), 
        rec.get('open_time'), rec.get('close_time'), rec.get('entry_reason'), rec.get('exit_reason'),
        rec.get('tp1'), rec.get('tp2'),
        rec.get('strategy_id'), rec.get('signal_confidence'), rec.get('adx_confirmation'),
        rec.get('rsi_confirmation'), rec.get('macd_confirmation'),
        rec.get('atr_at_entry')
    ))
    conn.commit()
    conn.close()

def add_managed_trade_to_db(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()

    # Coalesce required NOT NULL fields to safe defaults
    sl_val = rec.get('sl', 0.0) if rec.get('sl', None) is not None else 0.0
    tp_val = rec.get('tp', 0.0) if rec.get('tp', None) is not None else 0.0

    values = (
        rec['id'], rec['symbol'], rec['side'], rec['entry_price'], rec['initial_qty'],
        rec['qty'], rec['notional'], rec['leverage'], sl_val, tp_val,
        rec['open_time'], json.dumps(rec.get('sltp_orders')),
        int(rec.get('trailing', False)), int(rec.get('dyn_sltp', False)),
        rec.get('tp1'), rec.get('tp2'), rec.get('tp3'),
        rec.get('trade_phase', 0), int(rec.get('be_moved', False)),
        rec.get('risk_usdt'), rec.get('strategy_id', 1),
        rec.get('atr_at_entry'),
        # S3 specific fields
        int(rec.get('s3_trailing_active', False)),
        rec.get('s3_trailing_stop'),
        # S4 specific fields
        rec.get('s4_trailing_stop'),
        rec.get('s4_last_candle_ts'),
        int(rec.get('s4_trailing_active', False))
    )
    cur.execute("""
    INSERT OR REPLACE INTO managed_trades (
        id, symbol, side, entry_price, initial_qty, qty, notional,
        leverage, sl, tp, open_time, sltp_orders, trailing, dyn_sltp,
        tp1, tp2, tp3, trade_phase, be_moved, risk_usdt, strategy_id, atr_at_entry,
        s3_trailing_active, s3_trailing_stop, s4_trailing_stop, s4_last_candle_ts,
        s4_trailing_active
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()

def remove_managed_trade_from_db(trade_id: str):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("DELETE FROM managed_trades WHERE id = ?", (trade_id,))
    conn.commit()
    conn.close()

def mark_attention_required_sync(symbol: str, reason: str, details: str):
    """Adds or updates an attention required flag for a symbol in the database."""
    try:
        conn = sqlite3.connect(CONFIG["DB_FILE"])
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO attention_required (symbol, reason, details, timestamp) VALUES (?, ?, ?, ?)",
                    (symbol, reason, details, datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()
        log.info(f"Marked '{symbol}' for attention. Reason: {reason}")
    except Exception as e:
        log.exception(f"Failed to mark attention for {symbol}: {e}")

def prune_trades_db(year: int, month: int):
    """Deletes all trades from the database for a specific month."""
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    
    start_date = f"{year}-{month:02d}-01"
    next_month_val = month + 1
    next_year_val = year
    if next_month_val > 12:
        next_month_val = 1
        next_year_val += 1
    end_date = f"{next_year_val}-{next_month_val:02d}-01"

    log.info(f"Pruning trades in DB from {start_date} up to {end_date}")
    try:
        cur.execute("DELETE FROM trades WHERE close_time >= ? AND close_time < ?", (start_date, end_date))
        conn.commit()
        count = cur.rowcount
        log.info(f"Successfully pruned {count} trades from the database.")
        if count > 0:
            send_telegram(f"🧹 Database Maintenance: Pruned {count} old trade records from {year}-{month:02d}.")
    except Exception as e:
        log.exception(f"Failed to prune trades from DB for {year}-{month:02d}")
        send_telegram(f"⚠️ Failed to prune old database records for {year}-{month:02d}. Please check logs.")
    finally:
        conn.close()

def load_managed_trades_from_db() -> Dict[str, Dict[str, Any]]:
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM managed_trades")
    rows = cur.fetchall()
    conn.close()

    trades = {}
    for row in rows:
        rec = dict(row)
        rec['sltp_orders'] = json.loads(rec.get('sltp_orders', '{}') or '{}')
        rec['trailing'] = bool(rec.get('trailing'))
        rec['dyn_sltp'] = bool(rec.get('dyn_sltp'))
        rec['be_moved'] = bool(rec.get('be_moved'))
        rec['s3_trailing_active'] = bool(rec.get('s3_trailing_active'))
        rec['s4_trailing_active'] = bool(rec.get('s4_trailing_active'))
        trades[rec['id']] = rec
    return trades

# -------------------------
# Indicators
# -------------------------
def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df['high']; low = df['low']; close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=1).mean()

def atr_wilder(df: pd.DataFrame, length: int) -> pd.Series:
    """Calculates the Average True Range (ATR) using Wilder's smoothing."""
    high = df['high']; low = df['low']; close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # Wilder's smoothing is an EMA with alpha = 1/length
    return tr.ewm(alpha=1/length, adjust=False).mean()

def hhv(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Highest High Value over a given period."""
    return series.rolling(window=length).max()

def llv(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Lowest Low Value over a given period."""
    return series.rolling(window=length).min()


# --- New Strategy Indicators ---

def sma(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Simple Moving Average (SMA)."""
    return series.rolling(window=length).mean()

def ema(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Exponential Moving Average (EMA)."""
    return series.ewm(span=length, adjust=False).mean()

def swing_low(series_low: pd.Series, lookback: int = 5) -> float:
    """Returns the most recent swing low over a lookback window."""
    if series_low is None or len(series_low) < lookback:
        return float('nan')
    return float(series_low.iloc[-lookback:].min())

def swing_high(series_high: pd.Series, lookback: int = 5) -> float:
    """Returns the most recent swing high over a lookback window."""
    if series_high is None or len(series_high) < lookback:
        return float('nan')
    return float(series_high.iloc[-lookback:].max())

def rsi(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Relative Strength Index (RSI)."""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=length).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=length).mean()
    
    # Avoid division by zero
    rs = gain / loss
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    return 100 - (100 / (1 + rs))

def bollinger_bands(series: pd.Series, length: int, std: float) -> tuple[pd.Series, pd.Series]:
    """Calculates Bollinger Bands."""
    ma = series.rolling(window=length).mean()
    std_dev = series.rolling(window=length).std()
    upper_band = ma + (std_dev * std)
    lower_band = ma - (std_dev * std)
    return upper_band, lower_band


def safe_latest_atr_from_df(df: Optional[pd.DataFrame]) -> float:
    """Return the latest ATR value from df or 0.0 if df is None/empty or ATR can't be computed."""
    try:
        if df is None or getattr(df, 'empty', True):
            return 0.0
        atr_series = atr(df, CONFIG.get("ATR_LENGTH", 14))
        if atr_series is None or atr_series.empty:
            return 0.0
        return float(atr_series.iloc[-1])
    except Exception:
        log.exception("safe_latest_atr_from_df failed; returning 0.0")
        return 0.0


def safe_last(series: pd.Series, default=0.0) -> float:
    """Safely get the last value of a series, returning a default if it's empty or the value is NaN."""
    if series is None or series.empty:
        return float(default)
    last_val = series.iloc[-1]
    if pd.isna(last_val):
        return float(default)
    return float(last_val)


def adx(df: pd.DataFrame, period: int = 14):
    """
    Calculates ADX, +DI, and -DI and adds them to the DataFrame.
    """
    high = df['high']
    low = df['low']
    close = df['close']

    # Calculate +DM, -DM and TR
    plus_dm = high.diff()
    minus_dm = low.diff().mul(-1)
    
    plus_dm[plus_dm < 0] = 0
    plus_dm[plus_dm < minus_dm] = 0
    
    minus_dm[minus_dm < 0] = 0
    minus_dm[minus_dm < plus_dm] = 0

    tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)

    # Smoothed values using Wilder's smoothing (approximated by EMA with alpha=1/period)
    alpha = 1 / period
    atr_smooth = tr.ewm(alpha=alpha, adjust=False).mean()
    
    # To avoid division by zero
    atr_smooth.replace(0, 1e-10, inplace=True)

    df['+DI'] = 100 * (plus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth)
    df['-DI'] = 100 * (minus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth)
    
    dx_denominator = (df['+DI'] + df['-DI']).replace(0, 1e-10)
    dx = 100 * (abs(df['+DI'] - df['-DI']) / dx_denominator)
    df['adx'] = dx.ewm(alpha=alpha, adjust=False).mean()


# -------------------------
# S10 helper utilities (small and self-contained)
# -------------------------
def compute_r_multiple(entry_price: float, current_price: float, initial_stop_price: float) -> float:
    """
    Compute R-multiple from entry and current price relative to initial stop.
    Returns absolute R (unsigned). Caller can apply direction if needed.
    """
    try:
        risk = abs(float(entry_price) - float(initial_stop_price))
        if risk <= 0:
            return 0.0
        return abs(float(current_price) - float(entry_price)) / risk
    except Exception:
        return 0.0


def _last_closed_slice(df: pd.DataFrame, lookback: int) -> pd.DataFrame:
    if df is None or df.empty or lookback <= 0:
        return pd.DataFrame()
    # Use only fully closed candles: exclude the latest forming one if any
    if len(df) >= lookback + 1:
        return df.iloc[-(lookback+1):-1]
    return df.iloc[:-1]


def get_last_pivot_low(symbol: str, lookback: int = 5) -> float:
    """
    Return last swing low across lookback closed bars on CONFIG['TIMEFRAME'].
    """
    try:
        df = fetch_klines_sync(symbol, CONFIG.get("TIMEFRAME", "15m"), max(lookback + 10, 50))
        sl = _last_closed_slice(df, lookback)
        if sl is None or sl.empty:
            return float('nan')
        return float(sl['low'].min())
    except Exception:
        return float('nan')


def get_last_pivot_high(symbol: str, lookback: int = 5) -> float:
    """
    Return last swing high across lookback closed bars on CONFIG['TIMEFRAME'].
    """
    try:
        df = fetch_klines_sync(symbol, CONFIG.get("TIMEFRAME", "15m"), max(lookback + 10, 50))
        sl = _last_closed_slice(df, lookback)
        if sl is None or sl.empty:
            return float('nan')
        return float(sl['high'].max())
    except Exception:
        return float('nan')


def compute_s10_mult(adx_value: float, is_stacked: bool) -> float:
    """
    Determine ATR trailing multiplier per S10 rules.
    """
    mult = S10_TRAIL_ATR_MULT_WEAK
    try:
        if S10_USE_ADAPTIVE_TRAIL and float(adx_value) >= float(S10_ADX_TREND_MIN):
            mult = S10_TRAIL_ATR_MULT_STRONG
        elif bool(is_stacked) and float(adx_value) < float(S10_ADX_TREND_MIN):
            mult = S10_TRAIL_ATR_MULT_WEAK + S10_STACKED_WIDER_MULT_BONUS
    except Exception:
        pass
    return float(mult)


def get_tick_info(symbol: str, ref_price: Optional[float] = None) -> tuple[float, float]:
    """
    Returns (tick_size, tick_size_pct_at_ref_price). If ref_price is not provided or <=0, pct is 0.
    """
    tick = get_price_tick_size(symbol) or 0.0
    if ref_price and ref_price > 0:
        return float(tick), float(tick) / float(ref_price)
    return float(tick), 0.0


def apply_min_sl_move_filter(current_sl: float, candidate_sl: float, price: float, tick_size: float, min_sl_move_pct: Optional[float]) -> bool:
    """
    Returns True if abs move >= min_move, where min_move = max(tick_size, min_sl_move_pct * price) if pct provided.
    """
    try:
        if current_sl is None or candidate_sl is None:
            return False
        min_move = float(tick_size)
        if min_sl_move_pct is not None:
            min_move = max(min_move, float(min_sl_move_pct) * float(price))
        return abs(float(candidate_sl) - float(current_sl)) >= max(min_move, 0.0)
    except Exception:
        return False


def supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0, atr_series: Optional[pd.Series] = None, source: Optional[pd.Series] = None) -> tuple[pd.Series, pd.Series]:
    """
    Calculates the SuperTrend indicator using the pandas-ta library.
    Returns two series: supertrend and supertrend_direction.
    """
    # Calculate SuperTrend using pandas_ta.
    # It returns a DataFrame with columns like 'SUPERT_10_3.0', 'SUPERTd_10_3.0', etc.
    st_df = df.ta.supertrend(period=period, multiplier=multiplier)

    if st_df is None or st_df.empty:
        log.error(f"Pandas-TA failed to generate SuperTrend for period={period}, mult={multiplier}.")
        return pd.Series(dtype='float64', index=df.index), pd.Series(dtype='float64', index=df.index)

    # --- Robustly find column names to avoid fragility with float formatting ---
    # The main supertrend line, e.g., 'SUPERT_7_3.0'
    supertrend_col = next((col for col in st_df.columns if col.startswith('SUPERT_') and 'd' not in col and 'l' not in col and 's' not in col), None)
    # The direction column, e.g., 'SUPERTd_7_3.0'
    direction_col = next((col for col in st_df.columns if col.startswith('SUPERTd_')), None)

    # Check if the expected columns were found
    if supertrend_col is None or direction_col is None:
        log.error(f"Pandas-TA did not generate expected SuperTrend columns for period={period}, mult={multiplier}. Got columns: {st_df.columns}")
        return pd.Series(dtype='float64', index=df.index), pd.Series(dtype='float64', index=df.index)

    # Extract the required series.
    st_series = st_df[supertrend_col]
    st_dir_series = st_df[direction_col]
    
    return st_series, st_dir_series

