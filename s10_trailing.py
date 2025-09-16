"""
S10 Trailing Stop — Plan A implementation helpers (config-driven).

This module provides:
- load_s10_trailing_config: load YAML config with defaults and compute per-run derived values.
- compute_s10_trailing_update: pure function to compute BE move, trailing SL update, and partial-close signal
  according to the Plan A spec, mirroring for longs/shorts.

Note:
- All percentages are decimals (e.g., 0.10% = 0.0010).
- This module is intentionally side-effect free; placing/cancelling orders remains the caller's responsibility.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any
import math
import os

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # Allow import even if PyYAML is not installed yet


@dataclass
class S10TrailInputs:
    side: str  # 'BUY' or 'SELL'
    entry_price: float
    current_price: float
    current_sl: Optional[float]
    initial_stop: float  # the stop placed at entry time
    adx: Optional[float]
    atr: float
    last_swing_low: Optional[float]  # for long
    last_swing_high: Optional[float]  # for short
    time_since_entry_bars: int  # on the S10 execution timeframe (likely M15)
    be_applied: bool  # has BE move already been performed?
    trailing_activated: bool  # allow trailing or still gated
    is_stacked_trade: bool  # true if AA + VBM stacked
    tick_size_abs: float  # absolute tick size (price increment)
    taker_fee_pct: float  # e.g., 0.0007 for 0.07%
    adx_period: int = 14  # informational only; not used directly


@dataclass
class S10TrailOutputs:
    be_stop: Optional[float]           # If set, move SL to this BE(+buffer) level once
    new_trailing_sl: Optional[float]   # If set, update SL to this tighter level
    partial_close_pct: Optional[float] # If set (0-1), close this portion at 1R the first time r>=1
    activate_trailing: bool            # Whether trailing should be activated (post-BE gate)
    reason: str                        # Human-readable reason for decision


def _get(cfg: Dict[str, Any], key: str, default: Any) -> Any:
    v = cfg.get(key, default)
    # Normalize common string-bool cases
    if isinstance(v, str) and v.lower() in ("true", "false"):
        return v.lower() == "true"
    return v


def load_s10_trailing_config(path: str) -> Dict[str, Any]:
    """
    Loads the YAML config file if present, fills defaults, and returns a dict.
    This function does not raise if the file is missing; it returns defaults.
    """
    # Defaults (matches docs/config file)
    cfg: Dict[str, Any] = {
        "S10_BE_TRIGGER_R": 1.0,
        "S10_BE_TRIGGER_PCT": None,
        "S10_BE_SL_OFFSET_PCT": 0.0010,
        "S10_NO_TRAIL_BARS": 2,
        "S10_PIVOT_LOOKBACK": 5,
        "S10_TRAIL_ATR_MULT_STRONG": 2.0,
        "S10_TRAIL_ATR_MULT_WEAK": 2.8,
        "S10_TRAIL_BUFFER_MULT": 0.30,
        "S10_ADX_TREND_MIN": 25,
        "S10_USE_ADAPTIVE_TRAIL": True,
        "S10_MIN_SL_MOVE_PCT": None,
        "S10_TICK_SIZE_PCT": 0.0001,
        "S10_TAKER_FEE_PCT": 0.0007,
        "S10_STACKED_PARTIAL_CLOSE_PCT": 0.25,
        "S10_STACKED_WIDER_MULT_BONUS": 0.25,
        "S10_COOLDOWN_ENABLED": False,
        "S10_COOLDOWN_SMALL_LOSS_R": 0.5,
        "S10_COOLDOWN_N": 3,
        "S10_COOLDOWN_M": 10,
        "S10_COOLDOWN_DURATION_H": 24,
    }

    if yaml is not None and path and os.path.exists(path):
        try:
            with open(path, "r") as f:
                loaded = yaml.safe_load(f) or {}
            if isinstance(loaded, dict):
                cfg.update(loaded)
        except Exception:
            # Fall back to defaults
            pass

    return cfg


def _r_multiple(side: str, entry: float, price: float, initial_risk_per_unit: float) -> float:
    if initial_risk_per_unit <= 0:
        return 0.0
    if side.upper() == "BUY":
        return (price - entry) / initial_risk_per_unit
    else:
        return (entry - price) / initial_risk_per_unit


def _min_sl_move_threshold(cfg: Dict[str, Any], current_price: float, tick_size_abs: float) -> float:
    min_sl_move_pct = cfg.get("S10_MIN_SL_MOVE_PCT")
    if min_sl_move_pct is None:
        # compute from tick + fees
        tick_pct = float(cfg.get("S10_TICK_SIZE_PCT", 0.0001))
        taker_fee_pct = float(cfg.get("S10_TAKER_FEE_PCT", 0.0007))
        min_sl_move_pct = max(tick_pct, 2.0 * taker_fee_pct)
    return max(tick_size_abs, float(min_sl_move_pct) * float(current_price))


def _round_to_tick(price: float, tick: float, mode: str) -> float:
    if tick <= 0:
        return price
    # mode: "down" or "up"
    if mode == "down":
        return math.floor(price / tick) * tick
    return math.ceil(price / tick) * tick


def compute_s10_trailing_update(cfg: Dict[str, Any], inputs: S10TrailInputs) -> S10TrailOutputs:
    """
    Core Plan A implementation. Returns what action to take, if any.

    The caller is responsible for:
     - Calculating last swing low/high over cfg['S10_PIVOT_LOOKBACK'] on the S10 timeframe
     - Supplying ATR (Wilder) and ADX values aligned to the S10 timeframe
     - Placing/cancelling/modifying orders based on outputs
    """
    side = (inputs.side or "").upper()
    assert side in ("BUY", "SELL"), "side must be 'BUY' or 'SELL'"

    # Initial R per unit from the original stop
    if side == "BUY":
        r_per_unit = max(1e-12, inputs.entry_price - inputs.initial_stop)
    else:
        r_per_unit = max(1e-12, inputs.initial_stop - inputs.entry_price)

    r_mult = _r_multiple(side, inputs.entry_price, inputs.current_price, r_per_unit)
    be_trigger_r = cfg.get("S10_BE_TRIGGER_R", 1.0)
    be_trigger_pct = cfg.get("S10_BE_TRIGGER_PCT")  # optional alternative to R

    # Gate: do not trail until BE trigger is reached
    # Two possible gates: R-based or absolute pct-based
    passed_gate = False
    if be_trigger_pct is not None:
        # Compare absolute return vs entry
        ret_pct = (inputs.current_price / inputs.entry_price) - 1.0
        ret_pct = ret_pct if side == "BUY" else -ret_pct
        passed_gate = (ret_pct >= float(be_trigger_pct))
    else:
        passed_gate = (r_mult >= float(be_trigger_r))

    # Optionally set BE as soon as gate passes (preferred behavior)
    be_stop = None
    if passed_gate and not inputs.be_applied:
        be_offset = float(cfg.get("S10_BE_SL_OFFSET_PCT", 0.0010))
        if side == "BUY":
            be_stop = inputs.entry_price * (1.0 + be_offset)
        else:
            be_stop = inputs.entry_price * (1.0 - be_offset)

    # No-trail initial bars guard
    if inputs.time_since_entry_bars < int(cfg.get("S10_NO_TRAIL_BARS", 2)):
        return S10TrailOutputs(
            be_stop=be_stop,
            new_trailing_sl=None,
            partial_close_pct=None,
            activate_trailing=bool(passed_gate),
            reason=f"no-trail initial bars; bars={inputs.time_since_entry_bars}"
        )

    # If gate not passed, still nothing to trail (but can return BE suggestion if newly passed)
    if not passed_gate:
        return S10TrailOutputs(
            be_stop=be_stop,
            new_trailing_sl=None,
            partial_close_pct=None,
            activate_trailing=False,
            reason=f"gate not passed; r={r_mult:.3f} < {be_trigger_r}"
        )

    # If we passed gate and BE applied, we can consider trailing now
    # Determine ATR multiplier
    use_adaptive = bool(cfg.get("S10_USE_ADAPTIVE_TRAIL", True))
    adx_min = float(cfg.get("S10_ADX_TREND_MIN", 25))
    mult_strong = float(cfg.get("S10_TRAIL_ATR_MULT_STRONG", 2.0))
    mult_weak = float(cfg.get("S10_TRAIL_ATR_MULT_WEAK", 2.8))
    atr_mult = mult_strong if (use_adaptive and inputs.adx is not None and inputs.adx >= adx_min) else (mult_weak if use_adaptive else mult_strong)

    # Stacked temporary wider bonus until ADX confirms strength
    if inputs.is_stacked_trade and (inputs.adx is None or inputs.adx < adx_min):
        atr_mult += float(cfg.get("S10_STACKED_WIDER_MULT_BONUS", 0.25))

    atr_val = max(1e-12, float(inputs.atr))
    price = float(inputs.current_price)

    # Compute candidate from pivot and ATR
    if side == "BUY":
        pivot_stop = float(inputs.last_swing_low) if inputs.last_swing_low is not None else (price - 4.0 * atr_val)
        atr_stop = price - atr_mult * atr_val
        candidate = max(pivot_stop, atr_stop)
        new_sl = candidate - float(cfg.get("S10_TRAIL_BUFFER_MULT", 0.30)) * atr_val
        # Monotonic tightening
        if inputs.current_sl is not None:
            new_sl = max(new_sl, float(inputs.current_sl))
        # Never above current price (safety), round down to tick
        new_sl = min(new_sl, price - inputs.tick_size_abs)
        new_sl = _round_to_tick(new_sl, inputs.tick_size_abs, mode="down")
    else:
        pivot_stop = float(inputs.last_swing_high) if inputs.last_swing_high is not None else (price + 4.0 * atr_val)
        atr_stop = price + atr_mult * atr_val
        candidate = min(pivot_stop, atr_stop)
        new_sl = candidate + float(cfg.get("S10_TRAIL_BUFFER_MULT", 0.30)) * atr_val
        # Monotonic tightening (downwards)
        if inputs.current_sl is not None:
            new_sl = min(new_sl, float(inputs.current_sl))
        # Never below current price for shorts, round up to tick
        new_sl = max(new_sl, price + inputs.tick_size_abs)
        new_sl = _round_to_tick(new_sl, inputs.tick_size_abs, mode="up")

    # Anti-whipsaw: minimum SL move threshold
    min_move = _min_sl_move_threshold(cfg, price, inputs.tick_size_abs)
    curr_sl = float(inputs.current_sl) if inputs.current_sl is not None else None
    if curr_sl is not None and abs(new_sl - curr_sl) < min_move:
        return S10TrailOutputs(
            be_stop=be_stop,
            new_trailing_sl=None,
            partial_close_pct=None,
            activate_trailing=True,
            reason=f"min-move guard; delta={abs(new_sl - curr_sl):.8f} < {min_move:.8f}"
        )

    # Partial close at 1R for stacked setups (first time only; caller tracks if already done)
    partial_close = None
    if inputs.is_stacked_trade and r_mult >= 1.0:
        partial_close = float(cfg.get("S10_STACKED_PARTIAL_CLOSE_PCT", 0.25))

    return S10TrailOutputs(
        be_stop=be_stop,
        new_trailing_sl=new_sl,
        partial_close_pct=partial_close,
        activate_trailing=True,
        reason="trail-update"
    )