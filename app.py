# backtest_s4.py
"""
Backtester for S4 (DEMA + SuperTrend) from app.py.
- Asks only for backtest day count (no args).
- Uses BINANCE_API_KEY / BINANCE_API_SECRET from .env if present (optional for public klines).
- Scans all symbols in CONFIG["SYMBOLS"] from app.py defaults.
- Implements:
    * Entry rules (ST flip, DEMA filter, DEMA-cross rejection, candle-size rejection, signal expiry)
    * Quantity sizing (risk_usd, min_notional, round to step)
    * Trailing Stop logic:
        - Use TSL supertrend when it "agrees" with trade direction
        - Fallback to main SuperTrend when TSL disagrees
    * Full trade report and metrics
"""
import os
import math
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, getcontext
import json

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client

# Load .env
load_dotenv()

# Try to reuse the same default symbol list from app.py
DEFAULT_SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(",")
TIMEFRAME = os.getenv("TIMEFRAME", "15m")  # S4 expects 15m
S4_CONFIG = {
    "DEMA_PERIOD": int(os.getenv("S4_DEMA_PERIOD", "200")),
    "SUPERTREND_PERIOD": int(os.getenv("S4_SUPERTREND_PERIOD", "20")),
    "SUPERTREND_MULTIPLIER": float(os.getenv("S4_SUPERTREND_MULTIPLIER", "6.0")),
    "RISK_USD": float(os.getenv("S4_RISK_USD", "0.50")),
    "TSL_ATR_PERIOD": int(os.getenv("S4_TSL_ATR_PERIOD", "2")),
    "TSL_ATR_MULTIPLIER": float(os.getenv("S4_TSL_ATR_MULTIPLIER", "4.6")),
}
# Sizing/fallback defaults (from app.py defaults)
MIN_NOTIONAL_USDT = float(os.getenv("MIN_NOTIONAL_USDT", "5.0"))
RISK_SMALL_BALANCE_THRESHOLD = float(os.getenv("RISK_SMALL_BALANCE_THRESHOLD", "50.0"))
RISK_SMALL_FIXED_USDT = float(os.getenv("RISK_SMALL_FIXED_USDT", "0.5"))
MARGIN_USDT_SMALL_BALANCE = float(os.getenv("MARGIN_USDT_SMALL_BALANCE", "1.0"))
MAX_BOT_LEVERAGE = int(os.getenv("MAX_BOT_LEVERAGE", "30"))

# Binance client (keys optional for klines)
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
client = Client(API_KEY, API_SECRET) if API_KEY else Client()

# Helpers: timeframe -> seconds
TF_SECONDS_MAP = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900,
    "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400, "1d": 86400
}
CANDLE_SECONDS = TF_SECONDS_MAP.get(TIMEFRAME, 900)

# ---------- Indicator implementations ----------
def dema(series: pd.Series, length: int):
    """Double EMA as used in app.py."""
    ema1 = series.ewm(span=length, adjust=False).mean()
    ema2 = ema1.ewm(span=length, adjust=False).mean()
    return 2 * ema1 - ema2

def atr(df: pd.DataFrame, length=14):
    high = df['high']
    low = df['low']
    close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=1).mean()

def supertrend(df: pd.DataFrame, period: int = 7, multiplier: float = 3.0, atr_series: pd.Series = None, source=None):
    """
    SuperTrend implementation that matches the typical approach used in app.py.
    Returns (supertrend_value_series, direction_series) where direction=1 (bull) or -1 (bear).
    source optionally provided (hl2 etc).
    """
    if source is None:
        source = (df['high'] + df['low']) / 2
    if atr_series is None:
        atr_series = atr(df, length=period)

    hl2 = source
    basic_upper = hl2 + multiplier * atr_series
    basic_lower = hl2 - multiplier * atr_series

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()

    st = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=int)

    # Iterate to build final bands and ST (vectorized implementation is possible but loop mirrors app logic)
    prev_final_upper = None
    prev_final_lower = None
    prev_st = None
    prev_dir = None

    for i in range(len(df)):
        if i == 0:
            prev_final_upper = basic_upper.iat[0]
            prev_final_lower = basic_lower.iat[0]
            st.iat[0] = prev_final_upper  # initial value (we'll treat as neutral)
            direction.iat[0] = 1
            prev_st = st.iat[0]
            prev_dir = 1
            continue

        bu = basic_upper.iat[i]
        bl = basic_lower.iat[i]

        # final upper/lower follow rules
        fu = bu if (bu < prev_final_upper or df['close'].iat[i-1] > prev_final_upper) else prev_final_upper
        fl = bl if (bl > prev_final_lower or df['close'].iat[i-1] < prev_final_lower) else prev_final_lower

        prev_final_upper = fu
        prev_final_lower = fl

        # determine trend
        if prev_st == prev_final_upper:
            if df['close'].iat[i] <= fu:
                st_val = fu
                dir_val = -1
            else:
                st_val = fl
                dir_val = 1
        else:
            if df['close'].iat[i] >= fl:
                st_val = fl
                dir_val = 1
            else:
                st_val = fu
                dir_val = -1

        st.iat[i] = st_val
        direction.iat[i] = dir_val

        prev_st = st_val

    return st, direction

def candle_body_crosses_dema(candle: pd.Series, dema_val: float) -> bool:
    # Body only (open-close) crosses dema
    op = candle['open']
    cl = candle['close']
    lowb = min(op, cl)
    highb = max(op, cl)
    return (lowb <= dema_val <= highb)

# ---------- Exchange rounding / min notional ----------
def get_exchange_info_cached():
    try:
        info = client.futures_exchange_info()
        return info
    except Exception:
        return None

EXCHANGE_INFO = get_exchange_info_cached()

def get_min_notional(symbol: str) -> float:
    try:
        if not EXCHANGE_INFO:
            return MIN_NOTIONAL_USDT
        s = next((x for x in EXCHANGE_INFO.get('symbols', []) if x['symbol'] == symbol), None)
        if not s:
            return MIN_NOTIONAL_USDT
        for f in s.get('filters', []):
            if f.get('filterType') == 'MIN_NOTIONAL':
                val = f.get('notional')
                if val:
                    return float(val) * 1.01
        return MIN_NOTIONAL_USDT
    except Exception:
        return MIN_NOTIONAL_USDT

def get_step_size(symbol: str):
    try:
        if not EXCHANGE_INFO:
            return None
        s = next((x for x in EXCHANGE_INFO.get('symbols', []) if x['symbol'] == symbol), None)
        if not s:
            return None
        for f in s.get('filters', []):
            if f.get('filterType') == 'LOT_SIZE':
                return Decimal(str(f.get('stepSize', '1')))
    except Exception:
        return None

def round_qty(symbol: str, qty: float, ceiling=False) -> float:
    # Uses exchange step size (LOT_SIZE). Default is round down to avoid exceeding risk.
    step = get_step_size(symbol)
    if step is None or step == 0:
        return float(qty)
    getcontext().prec = 28
    q = Decimal(str(qty))
    if ceiling:
        q_adj = (q / step).to_integral_value(rounding=ROUND_CEILING) * step
    else:
        q_adj = (q / step).to_integral_value(rounding=ROUND_FLOOR) * step
    qf = float(q_adj)
    return max(qf, 0.0)

# ---------- Klines fetch ----------
def fetch_klines(symbol: str, interval: str, limit: int):
    # Use futures klines to match app.py behaviour
    try:
        raw = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        if not raw:
            return None
        cols = ['open_time','open','high','low','close','volume','close_time','qav','num_trades','taker_base','taker_quote','ignore']
        df = pd.DataFrame(raw, columns=cols)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        df.set_index('close_time', inplace=True)
        return df
    except Exception as e:
        print("fetch_klines error:", e)
        return None

# ---------- S4 signal detection (mirror of simulate_strategy_4/evaluate_strategy_4 filters) ----------
def signal_s4_from_df(df: pd.DataFrame):
    # Assumes df has necessary indicator columns
    if df is None or len(df) < 4:
        return None

    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]
    current_candle = df.iloc[-1]
    entry_price = float(current_candle['open'])

    side = None
    if signal_candle['s4_st_dir'] == 1 and prev_candle['s4_st_dir'] == -1:
        side = 'BUY'
    elif signal_candle['s4_st_dir'] == -1 and prev_candle['s4_st_dir'] == 1:
        side = 'SELL'
    if not side:
        return None

    dema_value_signal = float(signal_candle['s4_dema'])
    if side == 'BUY' and entry_price <= dema_value_signal:
        return None
    if side == 'SELL' and entry_price >= dema_value_signal:
        return None

    if candle_body_crosses_dema(signal_candle, dema_value_signal):
        return None
    prev_dema_value = float(prev_candle['s4_dema'])
    if candle_body_crosses_dema(prev_candle, prev_dema_value):
        return None

    signal_body_size = abs(signal_candle['close'] - signal_candle['open'])
    prev_body_size = abs(prev_candle['close'] - prev_candle['open'])
    if prev_body_size > 0 and signal_body_size > (3 * prev_body_size):
        return None

    sl_price = float(signal_candle['s4_st'])
    tp_price = 0.0
    return {
        "strategy": "S4-DEMA-ST",
        "side": side,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "timestamp": signal_candle.name.isoformat()
    }

# ---------- Trailing & trade management ----------
def manage_trade_until_closed(trade: dict, df_full_slice: pd.DataFrame):
    """
    Simulate management of a single trade from its entry candle (already included) using later candles.
    Returns event dict with exit info or None if still open at end of df slice.
    """
    symbol = trade['symbol']
    side = trade['side']
    sl = trade['sl']
    qty = trade['qty']
    entry_idx = trade['entry_index']  # integer index in df_full_slice where entry happened
    entry_time = trade['entry_time']
    # Start checking from the next candle after entry
    for idx in range(entry_idx + 1, len(df_full_slice)):
        df_slice = df_full_slice.iloc[:idx+1].copy()
        df_ind = calculate_all_indicators_for_backtest(df_slice)
        last = df_ind.iloc[-1]
        current_price = float(last['close'])
        current_high = float(last['high'])
        current_low = float(last['low'])

        main_st_val = float(last['s4_st'])
        tsl_st_val = float(last['s4_tsl_st'])
        tsl_dir = int(last['s4_tsl_st_dir'])
        current_sl = sl

        # 1-minute grace logic: in real bot they have a 1-minute grace (skips TSL check if open < 60s).
        # Because we are on 15m candles, there is no need to wait; but to mirror app.py, we skip TSL for first candle after entry.
        seconds_since_entry = (df_slice.index[-1].to_pydatetime() - entry_time).total_seconds()
        tsl_agrees = (side == 'BUY' and tsl_dir == 1) or (side == 'SELL' and tsl_dir == -1)
        new_sl = None

        if seconds_since_entry >= 60:  # allow tsl to apply after 60s similar to app.py
            if tsl_agrees:
                if side == 'BUY' and tsl_st_val > current_sl and tsl_st_val < current_price:
                    new_sl = tsl_st_val
                elif side == 'SELL' and tsl_st_val < current_sl and tsl_st_val > current_price:
                    new_sl = tsl_st_val
            else:
                # fallback to main ST (but ensure not immediate stop-out)
                if main_st_val != current_sl:
                    if side == 'BUY' and main_st_val < current_price:
                        new_sl = main_st_val
                    elif side == 'SELL' and main_st_val > current_price:
                        new_sl = main_st_val

        if new_sl is not None and new_sl != current_sl:
            # Update SL
            sl = new_sl
            trade.setdefault('sl_history', []).append({'time': df_slice.index[-1].isoformat(), 'sl': sl})

        # Check SL hit on this candle
        if side == 'BUY':
            # if candle low <= sl -> stop hit (assuming stop executed at sl price)
            if current_low <= sl:
                exit_price = sl
                pnl = (exit_price - trade['entry_price']) * qty
                trade['exit_time'] = df_slice.index[-1].to_pydatetime()
                trade['exit_price'] = exit_price
                trade['pnl'] = pnl
                trade['reason'] = 'SL_HIT'
                return trade
        else:
            if current_high >= sl:
                exit_price = sl
                pnl = (trade['entry_price'] - exit_price) * qty
                trade['exit_time'] = df_slice.index[-1].to_pydatetime()
                trade['exit_price'] = exit_price
                trade['pnl'] = pnl
                trade['reason'] = 'SL_HIT'
                return trade

    # Not closed within available data
    return None

def calculate_all_indicators_for_backtest(df: pd.DataFrame):
    # Compute required S4 indicators only (fast)
    df2 = df.copy()
    s4p = S4_CONFIG
    df2['s4_dema'] = dema(df2['close'], length=s4p['DEMA_PERIOD'])
    df2['s4_st'], df2['s4_st_dir'] = supertrend(df2, period=s4p['SUPERTREND_PERIOD'], multiplier=s4p['SUPERTREND_MULTIPLIER'])
    # TSL ST uses shorter ATR period and hl2 source
    tsl_atr = atr(df2, length=s4p['TSL_ATR_PERIOD'])
    df2['s4_tsl_st'], df2['s4_tsl_st_dir'] = supertrend(df2, period=s4p['TSL_ATR_PERIOD'], multiplier=s4p['TSL_ATR_MULTIPLIER'], atr_series=tsl_atr, source=(df2['high'] + df2['low']) / 2)
    return df2

# ---------- Main simulation ----------
def run_backtest_for_symbol(symbol: str, days: int, starting_balance=100.0):
    print(f"\n--- Backtesting S4 for {symbol} over last {days} day(s) ---")
    candles_per_day = int(86400 / CANDLE_SECONDS)
    total_candles = candles_per_day * days
    lookback = 250
    fetch_limit = total_candles + lookback

    df_full = fetch_klines(symbol, TIMEFRAME, limit=fetch_limit)
    if df_full is None or len(df_full) < fetch_limit:
        print(f"Not enough data for {symbol}. Requested {fetch_limit} candles, got {None if df_full is None else len(df_full)}")
        return None

    trades = []
    simulated_open_trades = []
    balance = starting_balance
    s4p = S4_CONFIG
    min_notional = get_min_notional(symbol)

    # iterate through candles after lookback
    for i in range(lookback, len(df_full)):
        df_slice = df_full.iloc[:i+1].copy()
        df_ind = calculate_all_indicators_for_backtest(df_slice)

        # check if any active trade for this symbol (we allow only 1 concurrent per symbol like app)
        if not any(t['symbol'] == symbol and t.get('closed') is False for t in simulated_open_trades):
            # Evaluate signal
            sig = signal_s4_from_df(df_ind)
            if sig:
                # Build trade object
                entry_price = sig['entry_price']
                stop_loss = sig['sl_price']
                price_distance = abs(entry_price - stop_loss)
                if price_distance <= 0:
                    # reject
                    continue
                # Determine risk_usd (S4 uses fixed RISK_USD)
                risk_usd = s4p['RISK_USD']
                ideal_qty = risk_usd / price_distance
                # round down to step
                final_qty = round_qty(symbol, ideal_qty, ceiling=False)  # default floor to be safe

                # ensure min notional
                qty_min = round_qty(symbol, min_notional / entry_price if entry_price > 0 else 0.0, ceiling=True)
                if final_qty < qty_min or final_qty <= 0:
                    # rejected due to too small qty
                    continue

                simulated = {
                    "symbol": symbol,
                    "strategy": sig['strategy'],
                    "side": sig['side'],
                    "entry_price": entry_price,
                    "sl": stop_loss,
                    "qty": final_qty,
                    "entry_index": i,  # index in df_full
                    "entry_time": df_slice.index[-1].to_pydatetime(),
                    "closed": False
                }
                # run management until closed or data end
                result = manage_trade_until_closed(simulated, df_full.iloc[i:])  # pass slice starting from entry index
                if result:
                    result['closed'] = True
                    trades.append(result)
                    # bookkeeping: update balance by realized pnl
                    balance += result['pnl']
                else:
                    # not closed -> open position at end
                    simulated['closed'] = False
                    simulated_open_trades.append(simulated)

    # At end, append still-open trades to trades list as open
    for ot in simulated_open_trades:
        ot['closed'] = False
        ot['pnl'] = None
        trades.append(ot)

    # Produce report
    total_closed = [t for t in trades if t.get('closed') and t.get('pnl') is not None]
    wins = [t for t in total_closed if t['pnl'] > 0]
    losses = [t for t in total_closed if t['pnl'] <= 0]
    total_pnl = sum(t['pnl'] for t in total_closed) if total_closed else 0.0
    win_rate = (len(wins) / len(total_closed) * 100) if total_closed else 0.0
    summary = {
        "symbol": symbol,
        "days": days,
        "candles_tested": total_candles,
        "trades_total": len(trades),
        "trades_closed": len(total_closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": win_rate,
        "total_pnl": total_pnl,
        "ending_balance_est": balance,
        "open_trades": [t for t in trades if not t.get('closed')]
    }
    return {"summary": summary, "trades": trades}

def pretty_print_report(rep):
    if rep is None:
        print("No report.")
        return
    s = rep['summary']
    print("\n=== SUMMARY ===")
    for k, v in s.items():
        if k == 'open_trades':
            print(f"{k}: {len(v)}")
        else:
            print(f"{k}: {v}")
    print("\n=== TRADES ===")
    for t in rep['trades']:
        print(json.dumps({
            "symbol": t.get('symbol'),
            "side": t.get('side'),
            "entry_price": t.get('entry_price'),
            "sl": t.get('sl'),
            "qty": t.get('qty'),
            "entry_time": str(t.get('entry_time')),
            "exit_time": str(t.get('exit_time')) if t.get('exit_time') else None,
            "exit_price": t.get('exit_price'),
            "pnl": t.get('pnl'),
            "reason": t.get('reason'),
            "sl_history": t.get('sl_history', [])
        }, default=str))

# ---------- Run flow ----------
def main():
    print("S4 Backtester (DEMA + SuperTrend) — uses same filters & trailing logic as app.py")
    days_input = input("Enter backtest day count (integer, e.g. 7): ").strip()
    try:
        days = int(days_input)
        if days <= 0:
            raise ValueError()
    except Exception:
        print("Invalid days. Exiting.")
        return

    # Use same symbols as app.py default unless overridden in .env
    symbols = [s.strip().upper() for s in DEFAULT_SYMBOLS if s.strip()]
    print(f"Symbols to scan: {symbols}")
    overall_results = {}

    for sym in symbols:
        rep = run_backtest_for_symbol(sym, days)
        if rep:
            pretty_print_report(rep)
            overall_results[sym] = rep['summary']

    # Save results
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outname = f"s4_backtest_report_{ts}.json"
    with open(outname, "w") as f:
        json.dump({"meta": {"symbols": symbols, "days": days, "generated_at": ts}, "results": overall_results}, f, default=str, indent=2)
    print(f"\nSaved JSON report to: {outname}")

if __name__ == "__main__":
    main()
