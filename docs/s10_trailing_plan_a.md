# S10 Trailing Stop — Plan A (Config + Process, No Code)

This document specifies how to implement the “Plan A” trailing-stop adjustments for S10 with configuration only. The goal is to keep changes minimal, safe, and testable while reducing over-aggressive trailing that cuts small winners after fees.

Key outcomes:
- Delay trailing until the trade proves itself (≥ 1R or configured threshold).
- Use a conservative, hybrid pivot + ATR + buffer trail, adaptive to trend strength.
- Add anti-whipsaw guards: minimum SL move, no-trail initial bars, fee-aware BE, and partial-close for stacked setups.
- Optional cooldown if a symbol experiences a streak of small losses.

Config file: config/s10_trailing_config.yml

## 1) Configuration Keys (Defaults)

- S10_BE_TRIGGER_R: 1.0
- S10_BE_TRIGGER_PCT: null
- S10_BE_SL_OFFSET_PCT: 0.0010
- S10_NO_TRAIL_BARS: 2
- S10_PIVOT_LOOKBACK: 5
- S10_TRAIL_ATR_MULT_STRONG: 2.0
- S10_TRAIL_ATR_MULT_WEAK: 2.8
- S10_TRAIL_BUFFER_MULT: 0.30
- S10_ADX_TREND_MIN: 25
- S10_USE_ADAPTIVE_TRAIL: true
- S10_MIN_SL_MOVE_PCT: null
- S10_TICK_SIZE_PCT: 0.0001
- S10_TAKER_FEE_PCT: 0.0007
- S10_STACKED_PARTIAL_CLOSE_PCT: 0.25
- S10_STACKED_WIDER_MULT_BONUS: 0.25
- S10_COOLDOWN_ENABLED: false
- S10_COOLDOWN_SMALL_LOSS_R: 0.5
- S10_COOLDOWN_N: 3
- S10_COOLDOWN_M: 10
- S10_COOLDOWN_DURATION_H: 24

Notes:
- All percentages are decimals. Example: 0.10% = 0.0010.
- If S10_MIN_SL_MOVE_PCT is null, compute it dynamically as max(tick_size_pct, 2 × taker_fee_pct), using S10_TICK_SIZE_PCT and S10_TAKER_FEE_PCT per symbol/venue.

## 2) Implementation Steps (Engine Logic)

Terminology:
- For longs: initial_risk_per_unit = entry_price − initial_stop.
- For shorts: initial_risk_per_unit = initial_stop − entry_price.
- r_multiple = (current_price − entry_price) / initial_risk_per_unit for longs (mirror sign for shorts).

1) Gating
- If r_multiple < S10_BE_TRIGGER_R (or current gain < S10_BE_TRIGGER_PCT if you use that), do not trail yet.
- If r_multiple ≥ trigger and BE not applied:
  - Set SL to entry ± S10_BE_SL_OFFSET_PCT (directional; + for long, − for short).
  - Mark trailing_activated = true.

2) No-trail initial bars
- If time_since_entry_bars < S10_NO_TRAIL_BARS, skip trailing updates (except the BE move above).

3) Determine ATR multiplier
- If S10_USE_ADAPTIVE_TRAIL:
  - If ADX ≥ S10_ADX_TREND_MIN: mult = S10_TRAIL_ATR_MULT_STRONG.
  - Else: mult = S10_TRAIL_ATR_MULT_WEAK.
- If is_stacked_trade and ADX < S10_ADX_TREND_MIN:
  - mult += S10_STACKED_WIDER_MULT_BONUS (temporarily until ADX confirms).

4) Compute conservative candidate stop
- pivot_stop (long) = last swing low over S10_PIVOT_LOOKBACK bars. For short, last swing high.
- atr_stop (long) = current_price − mult × ATR. For short, current_price + mult × ATR.
- candidate (long) = max(pivot_stop, atr_stop). For short, candidate = min(pivot_stop, atr_stop).
- new_sl (long) = candidate − S10_TRAIL_BUFFER_MULT × ATR.
  - For short, new_sl = candidate + S10_TRAIL_BUFFER_MULT × ATR.

5) Anti-whipsaw guards
- Minimum SL move:
  - Let min_move_pct = S10_MIN_SL_MOVE_PCT if set; else max(S10_TICK_SIZE_PCT, 2 × S10_TAKER_FEE_PCT).
  - Only update SL if abs(new_sl − current_sl) ≥ max(tick_size_abs, min_move_pct × current_price).
- Monotonicity:
  - Never loosen the stop. For longs, only move SL upward; for shorts, only downward.
- Safety:
  - Do not set SL above/below live price invalidly; round to exchange tick.

6) Partial close for stacked setups
- If is_stacked_trade and first time r_multiple ≥ 1.0:
  - Close S10_STACKED_PARTIAL_CLOSE_PCT (e.g., 25%) via market/limit.
  - Activate trailing only on remaining size. Keep the wider mult bonus until ADX confirms.

7) Cooldown (optional)
- If S10_COOLDOWN_ENABLED:
  - If in the last M trades, there are N consecutive small losses (< S10_COOLDOWN_SMALL_LOSS_R), pause S10 for that symbol for S10_COOLDOWN_DURATION_H hours.

## 3) Break-even (BE) Nuance

Preferred:
- Move SL to entry + S10_BE_SL_OFFSET_PCT (long; subtract for short) as soon as BE trigger hits.
- This ensures a small net profit after taker fees/slippage.

Alternative:
- Only apply BE when trailing will be allowed soon; otherwise skip early BE to reduce margin pressure. Choose and keep consistent.

## 4) Rollout and Testing Plan

Pre-flight
- Verify ATR and ADX sources align with S10 timeframe.
- Confirm pivot calculation and rounding to exchange tick.
- Confirm symbol-level tick size and taker fee inputs.

Backtest A/B
- Run before/after over recent 3–6 months for representative symbols.
- Metrics to compare:
  - Net PnL after fees.
  - Share of winners stopped at ≤ 0.3–0.7R.
  - Median and 75th percentile R of winners.
  - Profit factor, win rate, average R.
- Plots:
  - Distribution of realized R pre vs post.
  - Equity curve with drawdown.
  - Example trades showing SL path vs price.

Sensitivity
- Try:
  - S10_TRAIL_ATR_MULT_WEAK: 2.6–3.2
  - S10_ADX_TREND_MIN: 20–30
  - S10_TRAIL_BUFFER_MULT: 0.20–0.40
  - S10_BE_SL_OFFSET_PCT: 0.0005–0.0015 (0.05%–0.15%)
- Confirm stability of results across symbols and regimes.

Go-live checklist
- Enable in paper/sandbox first for 1–2 weeks.
- Monitor per-symbol:
  - Average realized R of winners vs prior.
  - Frequency of micro-adjust SL (< tick + fee threshold).
  - Incidence of SL invalid rejections due to price proximity/ticks.
- Gradual rollout to production per cohort of symbols.

## 5) Integration Notes

- Price symmetry: All steps mirror for shorts.
- Rounding:
  - Always round SL to nearest allowed tick level.
- Data:
  - Use consistent timeframe data for ATR/ADX and pivot lookbacks.
- Per-symbol overrides:
  - If your engine supports symbol-specific config, these keys can be overridden per instrument/venue.
- Idempotency:
  - If computing new_sl equals current_sl after rounding or fails the min-move test, skip updates cleanly.

## 6) Acceptance Criteria

- Reduction in “small-winner stopped” events (≤ 0.5R) without increasing average loss size.
- Improved net PnL after fees with similar or lower drawdown.
- No increase in order rejection or SL invalid placement errors.
- SL change frequency reduced by micro-ratchet guard.