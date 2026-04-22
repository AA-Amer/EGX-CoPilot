"""
technical.py — Technical Analysis Engine for EGX Copilot
Reads from SQLite prices table. Used by longterm_agent and swing_agent.
All indicators follow standard definitions used in EGX trading.
"""

import pandas as pd
import numpy as np

try:
    from ta.momentum import RSIIndicator, StochasticOscillator
    from ta.trend import EMAIndicator, MACD, SMAIndicator, ADXIndicator
    from ta.volatility import BollingerBands, AverageTrueRange
    from ta.volume import OnBalanceVolumeIndicator
    _TA_AVAILABLE = True
except ImportError:
    _TA_AVAILABLE = False

from backend.data.db import get_connection


# ─────────────────────────────────────────────────
# 1. DATA LOADER
# ─────────────────────────────────────────────────

def get_ohlcv(ticker: str, lookback: int = 200) -> pd.DataFrame:
    """
    Load OHLCV data for a ticker from the prices table.
    Returns DataFrame with columns: date, open, high, low, close, volume.
    Sorted ascending by date. Returns empty DataFrame if no data found.

    Args:
        ticker:   EGX ticker symbol (e.g. "AMOC")
        lookback: maximum number of rows to load (most recent)
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT date, open, high, low, close, volume
            FROM prices
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            conn,
            params=(ticker.upper(), lookback),
        )
    finally:
        conn.close()

    if df.empty:
        return df

    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["close"])


# ─────────────────────────────────────────────────
# 2. TREND INDICATORS
# ─────────────────────────────────────────────────

def get_ema(ticker: str, periods: list = [9, 21, 50, 200], lookback: int = 200) -> pd.DataFrame:
    """
    Exponential Moving Averages for given periods.
    EMA9/21 used for swing signals (crossover = entry/exit signal).
    EMA50/200 used for long-term trend direction.

    Returns DataFrame with date, close, ema_9, ema_21, ema_50, ema_200 columns.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    for p in periods:
        try:
            df[f"ema_{p}"] = EMAIndicator(close=df["close"], window=p).ema_indicator()
        except Exception:
            df[f"ema_{p}"] = np.nan
    return df


def get_sma(ticker: str, periods: list = [20, 50, 200], lookback: int = 200) -> pd.DataFrame:
    """
    Simple Moving Averages.
    SMA20 used for Bollinger Band midline.
    SMA50/200 used for long-term support/resistance zones.

    Returns DataFrame with date, close, sma_20, sma_50, sma_200 columns.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    for p in periods:
        try:
            df[f"sma_{p}"] = SMAIndicator(close=df["close"], window=p).sma_indicator()
        except Exception:
            df[f"sma_{p}"] = np.nan
    return df


def get_macd(
    ticker: str,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    lookback: int = 200,
) -> pd.DataFrame:
    """
    MACD (Moving Average Convergence Divergence).
    macd_line:      fast EMA - slow EMA
    signal_line:    9-period EMA of macd_line
    histogram:      macd_line - signal_line

    Bullish: histogram > 0 and rising.
    Bearish: histogram < 0 and falling.

    Returns DataFrame with date, close, macd, macd_signal, macd_histogram.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    try:
        macd = MACD(
            close=df["close"],
            window_fast=fast,
            window_slow=slow,
            window_sign=signal,
        )
        df["macd"]           = macd.macd()
        df["macd_signal"]    = macd.macd_signal()
        df["macd_histogram"] = macd.macd_diff()
    except Exception:
        df["macd"] = df["macd_signal"] = df["macd_histogram"] = np.nan
    return df


def get_adx(ticker: str, period: int = 14, lookback: int = 200) -> pd.DataFrame:
    """
    Average Directional Index — measures trend strength, not direction.
    ADX > 25: strong trend.  ADX < 20: weak / ranging market.
    +DI > -DI: bullish trend.  -DI > +DI: bearish trend.

    Returns DataFrame with date, close, adx, di_plus, di_minus.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    try:
        adx = ADXIndicator(
            high=df["high"], low=df["low"], close=df["close"], window=period
        )
        df["adx"]      = adx.adx()
        df["di_plus"]  = adx.adx_pos()
        df["di_minus"] = adx.adx_neg()
    except Exception:
        df["adx"] = df["di_plus"] = df["di_minus"] = np.nan
    return df


# ─────────────────────────────────────────────────
# 3. MOMENTUM INDICATORS
# ─────────────────────────────────────────────────

def get_rsi(ticker: str, period: int = 14, lookback: int = 200) -> pd.DataFrame:
    """
    Relative Strength Index (0–100).
    RSI > 70: overbought — consider selling or avoiding new longs.
    RSI < 30: oversold — potential bounce opportunity.
    RSI 40–60: neutral zone.

    Returns DataFrame with date, close, rsi.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    try:
        df["rsi"] = RSIIndicator(close=df["close"], window=period).rsi()
    except Exception:
        df["rsi"] = np.nan
    return df


def get_stochastic(
    ticker: str, k: int = 14, d: int = 3, lookback: int = 200
) -> pd.DataFrame:
    """
    Stochastic Oscillator.
    stoch_k: raw stochastic value (0–100).
    stoch_d: signal line (3-period SMA of stoch_k).
    Overbought > 80.  Oversold < 20.

    Returns DataFrame with date, close, stoch_k, stoch_d.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    try:
        stoch = StochasticOscillator(
            high=df["high"], low=df["low"], close=df["close"],
            window=k, smooth_window=d,
        )
        df["stoch_k"] = stoch.stoch()
        df["stoch_d"] = stoch.stoch_signal()
    except Exception:
        df["stoch_k"] = df["stoch_d"] = np.nan
    return df


# ─────────────────────────────────────────────────
# 4. VOLATILITY INDICATORS
# ─────────────────────────────────────────────────

def get_bollinger_bands(
    ticker: str, period: int = 20, std: float = 2.0, lookback: int = 200
) -> pd.DataFrame:
    """
    Bollinger Bands: upper/middle/lower bands around SMA20.
    Price at upper band: overbought.  Price at lower band: oversold.
    Band squeeze (low width): breakout likely approaching.
    bb_pct: where price sits within bands (0 = lower band, 1 = upper band).

    Returns DataFrame with date, close, bb_upper, bb_middle, bb_lower, bb_pct, bb_width.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    try:
        bb = BollingerBands(close=df["close"], window=period, window_dev=std)
        df["bb_upper"]  = bb.bollinger_hband()
        df["bb_middle"] = bb.bollinger_mavg()
        df["bb_lower"]  = bb.bollinger_lband()
        df["bb_pct"]    = bb.bollinger_pband()
        df["bb_width"]  = bb.bollinger_wband()
    except Exception:
        for col in ["bb_upper", "bb_middle", "bb_lower", "bb_pct", "bb_width"]:
            df[col] = np.nan
    return df


def get_atr(ticker: str, period: int = 14, lookback: int = 200) -> pd.DataFrame:
    """
    Average True Range — measures volatility in price units (EGP).
    Used for stop-loss placement: stop = entry - (1.5 × ATR).
    Higher ATR = more volatile = wider stops needed.

    Returns DataFrame with date, close, atr.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    try:
        df["atr"] = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"], window=period
        ).average_true_range()
    except Exception:
        df["atr"] = np.nan
    return df


# ─────────────────────────────────────────────────
# 5. VOLUME INDICATORS
# ─────────────────────────────────────────────────

def get_volume_analysis(ticker: str, lookback: int = 200) -> pd.DataFrame:
    """
    Volume indicators:
    vol_sma20: 20-day average volume baseline.
    vol_ratio: today's volume / vol_sma20.  > 1.5 = high-volume confirmation.
    obv:       On-Balance Volume — cumulative volume direction indicator.

    Bullish: price up + high vol ratio.
    Bearish: price down + high vol ratio (distribution).

    Returns DataFrame with date, close, volume, vol_sma20, vol_ratio, obv.
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty:
        return df
    df["vol_sma20"] = df["volume"].rolling(window=20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_sma20"].replace(0, np.nan)
    try:
        df["obv"] = OnBalanceVolumeIndicator(
            close=df["close"], volume=df["volume"]
        ).on_balance_volume()
    except Exception:
        df["obv"] = np.nan
    return df


# ─────────────────────────────────────────────────
# 6. FIBONACCI RETRACEMENT
# ─────────────────────────────────────────────────

def get_fibonacci_levels(ticker: str, lookback: int = 90) -> dict:
    """
    Fibonacci retracement levels from the highest high and lowest low
    over the lookback period.

    Standard levels: 0%, 23.6%, 38.2%, 50%, 61.8%, 78.6%, 100%
    Used for: support/resistance zones, entry and exit price targets.

    current_zone: which Fibonacci band the current price sits in.

    Returns dict with swing_high, swing_low, fib level prices, and current_zone.
    Returns empty dict if insufficient data (< 10 rows).
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty or len(df) < 10:
        return {}

    swing_high = float(df["high"].max())
    swing_low  = float(df["low"].min())
    diff       = swing_high - swing_low
    close      = float(df["close"].iloc[-1])

    if diff == 0:
        return {}

    levels = {
        "swing_high": round(swing_high, 2),
        "swing_low":  round(swing_low,  2),
        "fib_0":      round(swing_high, 2),
        "fib_236":    round(swing_high - 0.236 * diff, 2),
        "fib_382":    round(swing_high - 0.382 * diff, 2),
        "fib_50":     round(swing_high - 0.500 * diff, 2),
        "fib_618":    round(swing_high - 0.618 * diff, 2),
        "fib_786":    round(swing_high - 0.786 * diff, 2),
        "fib_100":    round(swing_low,  2),
        "current_price": round(close, 2),
    }

    if   close >= levels["fib_0"]:   zone = "above_swing_high"
    elif close >= levels["fib_236"]: zone = "above_236"
    elif close >= levels["fib_382"]: zone = "between_236_382"
    elif close >= levels["fib_50"]:  zone = "between_38_50"
    elif close >= levels["fib_618"]: zone = "between_50_61"
    elif close >= levels["fib_786"]: zone = "between_61_78"
    elif close >= levels["fib_100"]: zone = "below_61"
    else:                            zone = "below_swing_low"

    levels["current_zone"] = zone
    return levels


# ─────────────────────────────────────────────────
# 7. SUPPORT & RESISTANCE
# ─────────────────────────────────────────────────

def get_support_resistance(
    ticker: str, lookback: int = 90, tolerance: float = 0.02
) -> dict:
    """
    Identifies key support and resistance levels from local price pivots.

    tolerance: price within tolerance% of a level = touching that level.
    pivot_highs: local resistance levels (price reversed downward from here).
    pivot_lows:  local support levels (price reversed upward from here).
    nearest_support:    closest support level below current price.
    nearest_resistance: closest resistance level above current price.

    Returns dict with pivot_highs, pivot_lows, nearest_support, nearest_resistance.
    Returns empty dict if insufficient data (< 10 rows).
    """
    df = get_ohlcv(ticker, lookback)
    if df.empty or len(df) < 10:
        return {}

    close = float(df["close"].iloc[-1])

    pivot_highs: list[float] = []
    pivot_lows:  list[float] = []
    for i in range(2, len(df) - 2):
        h = float(df["high"].iloc[i])
        l = float(df["low"].iloc[i])
        if h == float(df["high"].iloc[i - 2 : i + 3].max()):
            pivot_highs.append(round(h, 2))
        if l == float(df["low"].iloc[i - 2 : i + 3].min()):
            pivot_lows.append(round(l, 2))

    def _cluster(levels: list[float]) -> list[float]:
        if not levels:
            return []
        levels = sorted(set(levels))
        clustered = [levels[0]]
        for lv in levels[1:]:
            if abs(lv - clustered[-1]) / clustered[-1] > tolerance:
                clustered.append(lv)
        return clustered

    pivot_highs = _cluster(pivot_highs)
    pivot_lows  = _cluster(pivot_lows)

    supports    = [l for l in pivot_lows  if l < close]
    resistances = [h for h in pivot_highs if h > close]

    return {
        "pivot_highs":        pivot_highs,
        "pivot_lows":         pivot_lows,
        "nearest_support":    round(max(supports),    2) if supports    else None,
        "nearest_resistance": round(min(resistances), 2) if resistances else None,
        "current_price":      round(close, 2),
    }


# ─────────────────────────────────────────────────
# 8. MASTER SIGNAL SNAPSHOT
# ─────────────────────────────────────────────────

def get_signal_snapshot(ticker: str, lookback: int = 200) -> dict:
    """
    Primary interface for agents. Returns ALL indicator values for a ticker
    in a single call, minimising DB round-trips.

    Computes: EMAs, SMA, MACD, ADX, RSI, Stochastic, Bollinger Bands, ATR,
              Volume analysis, Fibonacci levels, Support/Resistance, price targets.

    Returns a flat dict with latest values for every indicator.
    Returns empty dict if insufficient data (< 30 rows) or ta library unavailable.

    Args:
        ticker:   EGX ticker symbol
        lookback: rows of OHLCV history to load (default 200)
    """
    if not _TA_AVAILABLE:
        return {}

    df = get_ohlcv(ticker, lookback)
    if df.empty or len(df) < 30:
        return {}

    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]

    def _last(series) -> float | None:
        try:
            v = series.iloc[-1]
            return round(float(v), 4) if pd.notna(v) else None
        except Exception:
            return None

    # ── Trend ──
    try:
        ema9   = EMAIndicator(close=close, window=9).ema_indicator()
        ema21  = EMAIndicator(close=close, window=21).ema_indicator()
        ema50  = EMAIndicator(close=close, window=50).ema_indicator()
        ema200 = EMAIndicator(close=close, window=200).ema_indicator()
        sma20  = SMAIndicator(close=close, window=20).sma_indicator()
    except Exception:
        ema9 = ema21 = ema50 = ema200 = sma20 = pd.Series([np.nan] * len(df))

    try:
        macd_obj = MACD(close=close)
    except Exception:
        macd_obj = None

    try:
        adx_obj = ADXIndicator(high=high, low=low, close=close)
    except Exception:
        adx_obj = None

    # ── Momentum ──
    try:
        rsi = RSIIndicator(close=close, window=14).rsi()
    except Exception:
        rsi = pd.Series([np.nan] * len(df))

    try:
        stoch = StochasticOscillator(high=high, low=low, close=close)
    except Exception:
        stoch = None

    # ── Volatility ──
    try:
        bb  = BollingerBands(close=close, window=20)
        atr = AverageTrueRange(high=high, low=low, close=close).average_true_range()
    except Exception:
        bb  = None
        atr = pd.Series([np.nan] * len(df))

    # ── Volume ──
    vol_sma20 = volume.rolling(20).mean()
    vol_ratio = volume / vol_sma20.replace(0, np.nan)
    try:
        obv = OnBalanceVolumeIndicator(close=close, volume=volume).on_balance_volume()
    except Exception:
        obv = pd.Series([np.nan] * len(df))

    # ── Derived values ──
    latest_close = _last(close)
    latest_ema9  = _last(ema9)
    latest_ema21 = _last(ema21)
    latest_ema50 = _last(ema50)
    latest_ema200= _last(ema200)

    ema9_above_21  = (latest_ema9  > latest_ema21)  if (latest_ema9  is not None and latest_ema21  is not None) else None
    price_above_50 = (latest_close > latest_ema50)  if (latest_close is not None and latest_ema50  is not None) else None
    price_above_200= (latest_close > latest_ema200) if (latest_close is not None and latest_ema200 is not None) else None

    ema_slope_5d = None
    if len(ema9) >= 5 and pd.notna(ema9.iloc[-1]) and pd.notna(ema9.iloc[-5]):
        base = float(ema9.iloc[-5])
        if base != 0:
            ema_slope_5d = round((float(ema9.iloc[-1]) - base) / base * 100, 4)

    macd_hist    = _last(macd_obj.macd_diff())    if macd_obj else None
    macd_bullish = (macd_hist > 0)                if macd_hist is not None else None

    latest_rsi = _last(rsi)
    if   latest_rsi is None:    rsi_zone = "unknown"
    elif latest_rsi > 70:       rsi_zone = "overbought"
    elif latest_rsi < 30:       rsi_zone = "oversold"
    else:                       rsi_zone = "neutral"

    latest_atr = _last(atr)

    # ── Fibonacci & S/R ──
    fib = get_fibonacci_levels(ticker, lookback=90)
    sr  = get_support_resistance(ticker, lookback=90)

    # ── Price targets (dynamic — Fib + EMA + fair value levels) ──
    target_1m = target_6m = target_12m = None
    target_1m_pct = target_6m_pct = target_12m_pct = None

    def _calc_dynamic_targets(current_price, snap, fv_mid=None):
        """All targets strictly above current_price."""
        if not current_price or current_price <= 0:
            return {
                "target_1m": 0, "target_6m": 0, "target_12m": 0,
                "target_1m_pct": 5.0, "target_6m_pct": 15.0, "target_12m_pct": 25.0,
            }

        candidates = []
        for key in ["fib_236", "fib_382", "fib_50",
                    "fib_618", "fib_786", "fib_100",
                    "swing_high", "nearest_resistance"]:
            val = snap.get(key)
            if val:
                try:
                    f = float(val)
                    if f > current_price * 1.02:
                        candidates.append(round(f, 2))
                except (ValueError, TypeError):
                    pass

        if fv_mid and float(fv_mid) > current_price * 1.10:
            candidates.append(round(float(fv_mid), 2))

        candidates = sorted(set(candidates))

        # 1M: nearest level 3–12% above current
        t1_cands = [c for c in candidates
                    if current_price * 1.03 <= c <= current_price * 1.12]
        t1 = t1_cands[0] if t1_cands else round(current_price * 1.05, 2)

        # 6M: next level after 1M, min 10% above
        t6_cands = [c for c in candidates if c > max(t1, current_price * 1.10)]
        t6 = t6_cands[0] if t6_cands else round(current_price * 1.15, 2)

        # 12M: fair value if >15%, else highest level, else 25% fallback
        t12 = None
        if fv_mid and float(fv_mid) > current_price * 1.15:
            t12 = round(float(fv_mid), 2)
        if not t12:
            t12_cands = [c for c in candidates
                         if c > max(t6, current_price * 1.20)]
            t12 = t12_cands[0] if t12_cands else round(current_price * 1.25, 2)

        # Ensure ascending order
        t1  = min(t1,  t6  * 0.98)
        t6  = min(t6,  t12 * 0.98)

        def pct(target):
            return round((target - current_price) / current_price * 100, 1)

        return {
            "target_1m":      round(t1,  2),
            "target_6m":      round(t6,  2),
            "target_12m":     round(t12, 2),
            "target_1m_pct":  pct(t1),
            "target_6m_pct":  pct(t6),
            "target_12m_pct": pct(t12),
        }

    if fib and latest_close:
        _snap_ctx = {
            **fib,
            "nearest_resistance": sr.get("nearest_resistance") if sr else None,
        }
        _dyn = _calc_dynamic_targets(latest_close, _snap_ctx, fv_mid=None)
        target_1m      = _dyn["target_1m"]
        target_6m      = _dyn["target_6m"]
        target_12m     = _dyn["target_12m"]
        target_1m_pct  = _dyn["target_1m_pct"]
        target_6m_pct  = _dyn["target_6m_pct"]
        target_12m_pct = _dyn["target_12m_pct"]

    # ── Entry zone (EMA50 ± 0.5 ATR, floored by Fib if applicable) ──
    try:
        ema50   = float(latest_ema50 or 0)
        atr_val = float(latest_atr   or 0)

        # ATR fallback: if missing or zero, estimate as 2% of price
        if not atr_val or atr_val <= 0:
            atr_val = ema50 * 0.02

        zone_low_raw  = ema50 - (atr_val * 0.5)
        zone_high_raw = ema50 + (atr_val * 1.5)

        # Cap zone_high at EMA21 if in uptrend
        ema20_val = float(latest_ema21 or latest_ema9 or 0)
        if ema20_val and ema20_val > ema50:
            zone_high_raw = min(zone_high_raw, ema20_val)

        # Enforce minimum zone width = 3% of EMA50, centered on mid
        min_width    = ema50 * 0.03
        actual_width = zone_high_raw - zone_low_raw
        if actual_width < min_width:
            mid           = (zone_low_raw + zone_high_raw) / 2
            zone_low_raw  = mid - (min_width / 2)
            zone_high_raw = mid + (min_width / 2)

        # Fib floor
        fib_floor = float(fib.get("fib_382") or fib.get("fib_236") or 0)
        if fib_floor and fib_floor > zone_low_raw:
            zone_low_raw = fib_floor

        entry_zone_low  = round(zone_low_raw,  2)
        entry_zone_high = round(zone_high_raw, 2)
        entry_zone_mid  = round((entry_zone_low + entry_zone_high) / 2, 2)
    except Exception:
        _ema50_fb = float(latest_ema50 or 0)
        entry_zone_low  = round(_ema50_fb * 0.97, 2) if _ema50_fb else None
        entry_zone_high = round(_ema50_fb * 1.02, 2) if _ema50_fb else None
        entry_zone_mid  = round(_ema50_fb,        2) if _ema50_fb else None

    return {
        # Price
        "ticker":             ticker,
        "close":              latest_close,
        "prev_close":         _last(close.iloc[:-1]) if len(close) > 1 else None,
        "change_pct":         round((float(close.iloc[-1]) / float(close.iloc[-2]) - 1) * 100, 2)
                              if len(close) >= 2 else None,

        # EMAs
        "ema9":               latest_ema9,
        "ema21":              latest_ema21,
        "ema50":              latest_ema50,
        "ema200":             latest_ema200,
        "sma20":              _last(sma20),
        "ema9_above_21":      ema9_above_21,
        "price_above_50":     price_above_50,
        "price_above_200":    price_above_200,
        "ema_slope_5d":       ema_slope_5d,

        # MACD
        "macd":               _last(macd_obj.macd())        if macd_obj else None,
        "macd_signal":        _last(macd_obj.macd_signal()) if macd_obj else None,
        "macd_histogram":     macd_hist,
        "macd_bullish":       macd_bullish,

        # ADX
        "adx":                _last(adx_obj.adx())     if adx_obj else None,
        "di_plus":            _last(adx_obj.adx_pos()) if adx_obj else None,
        "di_minus":           _last(adx_obj.adx_neg()) if adx_obj else None,

        # RSI
        "rsi":                latest_rsi,
        "rsi_zone":           rsi_zone,

        # Stochastic
        "stoch_k":            _last(stoch.stoch())        if stoch else None,
        "stoch_d":            _last(stoch.stoch_signal()) if stoch else None,

        # Bollinger Bands
        "bb_upper":           _last(bb.bollinger_hband()) if bb else None,
        "bb_middle":          _last(bb.bollinger_mavg())  if bb else None,
        "bb_lower":           _last(bb.bollinger_lband()) if bb else None,
        "bb_pct":             _last(bb.bollinger_pband()) if bb else None,
        "bb_width":           _last(bb.bollinger_wband()) if bb else None,

        # ATR
        "atr":                latest_atr,
        "stop_loss_1x":       round(latest_close - float(latest_atr or 0), 2) if latest_close else None,
        "stop_loss_15x":      round(latest_close - 1.5 * float(latest_atr or 0), 2) if latest_close else None,

        # Volume
        "volume":             _last(volume),
        "vol_sma20":          _last(vol_sma20),
        "vol_ratio":          _last(vol_ratio),
        "obv":                _last(obv),
        "vol_confirmation":   (_last(vol_ratio) > 1.5) if _last(vol_ratio) is not None else None,

        # Fibonacci (flattened)
        **{k: v for k, v in fib.items()},

        # Support / Resistance
        "nearest_support":    sr.get("nearest_support"),
        "nearest_resistance": sr.get("nearest_resistance"),

        # Price targets
        "target_1m":          target_1m,
        "target_6m":          target_6m,
        "target_12m":         target_12m,
        "target_1m_pct":      target_1m_pct,
        "target_6m_pct":      target_6m_pct,
        "target_12m_pct":     target_12m_pct,

        # Entry zone (EMA50 ± 0.5 ATR, Fib floor)
        "entry_zone_low":     entry_zone_low,
        "entry_zone_high":    entry_zone_high,
        "entry_zone_mid":     entry_zone_mid,
        "entry_trigger":      entry_zone_mid,   # backward compat

        # Data quality
        "data_rows":          len(df),
        "date_from":          str(df["date"].iloc[0].date()),
        "date_to":            str(df["date"].iloc[-1].date()),
    }


# ─────────────────────────────────────────────────
# 9. SWING SCORE (6 indicators, 0–100)
# ─────────────────────────────────────────────────

def get_swing_score(ticker: str) -> dict:
    """
    Swing trading signal score (0–100) across 6 indicators.
    Minimum score to trade: 70 (per SPEC.md).
    Each indicator contributes a weighted sub-score.

    Components:
      rsi     0–20  best entry zone 40–60; penalise overbought > 70
      ema     0–20  EMA9 > EMA21 + price above EMA50
      macd    0–20  histogram positive = bullish momentum
      volume  0–20  vol_ratio > 1.5 = confirmation
      support 0–10  price near support = low-risk entry
      bb      0–10  price in lower half of bands = room to run

    Returns dict with total_score, component scores, tradeable flag, and key levels.
    """
    snap = get_signal_snapshot(ticker)
    if not snap:
        return {"ticker": ticker, "total_score": 0, "error": "insufficient data"}

    scores: dict[str, int] = {}

    # 1. RSI
    rsi = snap.get("rsi") or 50
    if   40 <= rsi <= 60: scores["rsi"] = 20
    elif 30 <= rsi < 40:  scores["rsi"] = 15
    elif 60 < rsi <= 70:  scores["rsi"] = 10
    elif rsi < 30:        scores["rsi"] = 12   # oversold bounce opportunity
    else:                 scores["rsi"] = 2    # overbought > 70

    # 2. EMA9/21 crossover
    if snap.get("ema9_above_21"):
        scores["ema"] = 20 if snap.get("price_above_50") else 12
    else:
        scores["ema"] = 5

    # 3. MACD histogram
    hist = snap.get("macd_histogram") or 0
    if   hist > 0:    scores["macd"] = 20
    elif hist > -0.1: scores["macd"] = 10
    else:             scores["macd"] = 2

    # 4. Volume confirmation
    vol_ratio = snap.get("vol_ratio") or 1.0
    if   vol_ratio >= 2.0: scores["volume"] = 20
    elif vol_ratio >= 1.5: scores["volume"] = 15
    elif vol_ratio >= 1.0: scores["volume"] = 8
    else:                  scores["volume"] = 2

    # 5. Support proximity
    support = snap.get("nearest_support")
    close   = snap.get("close") or 0
    if support and close > 0:
        pct_from_support = (close - support) / close * 100
        if   pct_from_support <= 2: scores["support"] = 10
        elif pct_from_support <= 5: scores["support"] = 7
        else:                       scores["support"] = 3
    else:
        scores["support"] = 5

    # 6. Bollinger Bands position
    bb_pct = snap.get("bb_pct") or 0.5
    if   0.2 <= bb_pct <= 0.5: scores["bb"] = 10   # lower half — room to run
    elif bb_pct < 0.2:         scores["bb"] = 7    # near lower band — bounce zone
    elif 0.5 < bb_pct <= 0.8:  scores["bb"] = 5
    else:                      scores["bb"] = 1    # near upper band — overbought

    total = sum(scores.values())

    return {
        "ticker":             ticker,
        "total_score":        total,
        "scores":             scores,
        "tradeable":          total >= 70,
        "rsi":                snap.get("rsi"),
        "ema_bullish":        snap.get("ema9_above_21"),
        "macd_bullish":       snap.get("macd_bullish"),
        "vol_ratio":          snap.get("vol_ratio"),
        "fib_zone":           snap.get("current_zone"),
        "close":              snap.get("close"),
        "target_1m":          snap.get("target_1m"),
        "atr":                snap.get("atr"),
        "stop_loss":          snap.get("stop_loss_15x"),
        "nearest_support":    snap.get("nearest_support"),
        "nearest_resistance": snap.get("nearest_resistance"),
    }


# ─────────────────────────────────────────────────
# 10. FAIR VALUE  (P/E-based intrinsic estimate)
# ─────────────────────────────────────────────────

SECTOR_PE = {
    # ── EGX Emerging Market Multiples ─────────────────────────
    # Based on EGX30 historical averages + sector risk premium

    "Financials":   8.0,   # Banks compress due to credit risk + rates
    "Industrials":  9.0,   # Your holdings: ORAS, SWDY
    "Materials":    7.0,   # Commodity-linked, cyclical discount
    "Energy":       7.5,   # Oil/gas, commodity risk: AMOC
    "Consumer":    10.0,   # Staples premium but EM discount: ORWE, OLFI, SUGR
    "Real Estate":  8.0,   # Illiquidity + development risk
    "Healthcare":  12.0,   # Growth sector, higher multiple: MPCI
    "Technology":  14.0,   # Scarce on EGX, slight premium
    "Utilities":    8.0,   # Regulated, stable but low growth
    "Telecom":      9.0,   # Mature sector, limited upside
    "Other":        8.0,   # Conservative default
    "default":      8.0,   # Fallback
}


def get_fair_value(ticker: str, eps_egp: float = None, sector: str = None, **kwargs) -> dict:
    """
    Estimate intrinsic fair value using sector P/E multiples.
    Returns fv_low / fv_mid / fv_high bands and upside from current price.
    eps_egp must already be in EGP — caller is responsible for conversion.
    """
    snap = get_signal_snapshot(ticker)
    close = (snap or {}).get("close") or 0

    if not eps_egp or not close:
        return {
            "ticker": ticker,
            "fv_low": None, "fv_mid": None, "fv_high": None,
            "upside_pct": None, "valuation_status": "NO_DATA",
        }

    pe_mid  = SECTOR_PE.get(sector or "default", SECTOR_PE["default"])
    pe_low  = pe_mid * 0.75
    pe_high = pe_mid * 1.35

    fv_low  = round(eps_egp * pe_low,  2)
    fv_mid  = round(eps_egp * pe_mid,  2)
    fv_high = round(eps_egp * pe_high, 2)
    upside  = round((fv_mid - close) / close * 100, 1) if close > 0 else None

    if upside is None:
        status = "NO_DATA"
    elif upside >= 20:
        status = "UNDERVALUED"
    elif upside >= 0:
        status = "FAIR_VALUE"
    elif upside >= -15:
        status = "SLIGHTLY_RICH"
    else:
        status = "OVERVALUED"

    quality        = kwargs.get("data_quality", "HIGH")
    is_annualized  = kwargs.get("annualized", False)

    if quality == "LOW":
        confidence_pct = 60
        quality_note   = "⚠️ Based on Q1 only — low confidence"
    elif quality == "MEDIUM":
        confidence_pct = 80
        quality_note   = "Based on Q2 data — medium confidence"
    elif quality == "HIGH" and is_annualized:
        confidence_pct = 90
        quality_note   = "Based on Q3 data — good confidence"
    else:
        confidence_pct = 100
        quality_note   = "Based on full year — high confidence"

    return {
        "ticker":           ticker,
        "fv_low":           fv_low,
        "fv_mid":           fv_mid,
        "fv_high":          fv_high,
        "upside_pct":       upside,
        "valuation_status": status,
        "pe_used":          pe_mid,
        "eps_used":         round(eps_egp, 4),
        "current_price":    round(close, 2),
        "confidence_pct":   confidence_pct,
        "quality_note":     quality_note,
        "period_label":     kwargs.get("period_label", ""),
    }


# ─────────────────────────────────────────────────
# 11. RSI DIVERGENCE DETECTOR
# ─────────────────────────────────────────────────

def get_rsi_divergence(ticker: str, period: int = 14, lookback: int = 60) -> dict:
    """
    Detect bullish (price lower low / RSI higher low) and bearish
    (price higher high / RSI lower high) divergence over recent bars.
    """
    df = get_ohlcv(ticker, lookback=lookback + period + 5)
    if df.empty or len(df) < period + 10:
        return {"ticker": ticker, "divergence": "NONE", "detail": "insufficient data"}

    if not _TA_AVAILABLE:
        return {"ticker": ticker, "divergence": "NONE", "detail": "ta library unavailable"}

    close = df["close"]
    rsi_series = RSIIndicator(close=close, window=period).rsi().dropna()
    close_aligned = close.loc[rsi_series.index]

    # Use last `lookback` bars only
    rsi_w   = rsi_series.iloc[-lookback:]
    close_w = close_aligned.iloc[-lookback:]

    if len(rsi_w) < 10:
        return {"ticker": ticker, "divergence": "NONE", "detail": "insufficient rsi data"}

    # Find local lows (window=5) for bullish divergence
    def _local_lows(series, win=5):
        idx = []
        vals = series.values
        for i in range(win, len(vals) - win):
            if vals[i] == min(vals[i - win: i + win + 1]):
                idx.append(i)
        return idx

    def _local_highs(series, win=5):
        idx = []
        vals = series.values
        for i in range(win, len(vals) - win):
            if vals[i] == max(vals[i - win: i + win + 1]):
                idx.append(i)
        return idx

    close_arr = close_w.values
    rsi_arr   = rsi_w.values

    # Bullish divergence: last two price lows (price lower), RSI lows (RSI higher)
    lows = _local_lows(close_w)
    if len(lows) >= 2:
        p1, p2 = lows[-2], lows[-1]
        if close_arr[p2] < close_arr[p1] and rsi_arr[p2] > rsi_arr[p1]:
            return {
                "ticker":     ticker,
                "divergence": "BULLISH",
                "detail":     f"Price lower low at bar {p2}, RSI higher low — buying pressure building",
                "rsi_low1":   round(float(rsi_arr[p1]), 1),
                "rsi_low2":   round(float(rsi_arr[p2]), 1),
            }

    # Bearish divergence: last two price highs (price higher), RSI highs (RSI lower)
    highs = _local_highs(close_w)
    if len(highs) >= 2:
        p1, p2 = highs[-2], highs[-1]
        if close_arr[p2] > close_arr[p1] and rsi_arr[p2] < rsi_arr[p1]:
            return {
                "ticker":     ticker,
                "divergence": "BEARISH",
                "detail":     f"Price higher high at bar {p2}, RSI lower high — momentum fading",
                "rsi_high1":  round(float(rsi_arr[p1]), 1),
                "rsi_high2":  round(float(rsi_arr[p2]), 1),
            }

    return {"ticker": ticker, "divergence": "NONE", "detail": "no divergence detected"}


# ─────────────────────────────────────────────────
# 12. ENHANCED VOLUME ANALYSIS
# ─────────────────────────────────────────────────

def get_volume_analysis_enhanced(ticker: str, lookback: int = 20) -> dict:
    """
    OBV trend + volume/price confirmation label.
    Labels: CONFIRMED_UP, WEAK_UP, CONFIRMED_DOWN, WEAK_DOWN, NEUTRAL
    """
    df = get_ohlcv(ticker, lookback=lookback + 50)
    if df.empty or len(df) < lookback + 5:
        return {"ticker": ticker, "signal": "NEUTRAL", "obv_trend": "FLAT", "detail": "insufficient data"}

    if not _TA_AVAILABLE:
        return {"ticker": ticker, "signal": "NEUTRAL", "obv_trend": "FLAT", "detail": "ta unavailable"}

    close  = df["close"]
    volume = df["volume"]

    obv = OnBalanceVolumeIndicator(close=close, volume=volume).on_balance_volume()
    obv_w   = obv.iloc[-lookback:]
    close_w = close.iloc[-lookback:]

    price_up = close_w.iloc[-1] > close_w.iloc[0]
    obv_up   = obv_w.iloc[-1]   > obv_w.iloc[0]

    avg_vol   = volume.iloc[-lookback:].mean()
    last_vol  = volume.iloc[-1]
    vol_ratio = round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0

    # OBV slope
    obv_arr  = obv_w.values.astype(float)
    x        = np.arange(len(obv_arr))
    slope    = float(np.polyfit(x, obv_arr, 1)[0])
    obv_trend = "RISING" if slope > 0 else ("FALLING" if slope < 0 else "FLAT")

    if price_up and obv_up:
        signal = "CONFIRMED_UP"
        detail = "Price and OBV both rising — genuine buying interest"
    elif price_up and not obv_up:
        signal = "WEAK_UP"
        detail = "Price up but OBV diverging — rally may not sustain"
    elif not price_up and not obv_up:
        signal = "CONFIRMED_DOWN"
        detail = "Price and OBV both falling — distribution"
    elif not price_up and obv_up:
        signal = "WEAK_DOWN"
        detail = "Price down but OBV holding — possible accumulation"
    else:
        signal = "NEUTRAL"
        detail = "No clear directional conviction"

    return {
        "ticker":    ticker,
        "signal":    signal,
        "obv_trend": obv_trend,
        "vol_ratio": vol_ratio,
        "detail":    detail,
    }


# ─────────────────────────────────────────────────
# 13. MULTI-TIMEFRAME SIGNAL
# ─────────────────────────────────────────────────

def get_multi_timeframe_signal(ticker: str) -> dict:
    """
    Daily + weekly RSI and EMA alignment check.
    confidence_boost: +15 if daily and weekly both agree (bullish or bearish).
    """
    # Daily (last 120 bars)
    df_daily = get_ohlcv(ticker, lookback=120)
    if df_daily.empty or not _TA_AVAILABLE:
        return {"rsi_daily": None, "rsi_weekly": None, "mtf_agreement": False,
                "bullish_ema_alignment": False, "bearish_ema_alignment": False,
                "mtf_signal": "NEUTRAL", "confidence_boost": 0,
                "ema20": None, "ema50": None, "ema200": None, "current_price": None}

    import math

    close_d = df_daily["close"]
    rsi_d   = RSIIndicator(close=close_d, window=14).rsi().dropna()

    def _safe_ema(series, span, min_p):
        val = float(series.ewm(span=span, adjust=False, min_periods=min_p).mean().iloc[-1])
        return val if not math.isnan(val) else None

    e20 = _safe_ema(close_d, 20, 10)
    e50 = _safe_ema(close_d, 50, 20)
    # EMA200 needs ≥100 rows; fall back to EMA100 if fewer
    if len(close_d) >= 100:
        e200 = _safe_ema(close_d, 200, 50)
    elif len(close_d) >= 50:
        e200 = _safe_ema(close_d, 100, 30)
    else:
        e200 = None

    daily_rsi  = round(float(rsi_d.iloc[-1]), 1) if not rsi_d.empty else None
    last_close = float(close_d.iloc[-1])

    # EMA alignment — degrade gracefully when EMA200 unavailable
    if e20 and e50 and e200:
        bullish_ema = last_close > e20 > e50 > e200
        bearish_ema = last_close < e20 < e50 < e200
    elif e20 and e50:
        bullish_ema = last_close > e20 > e50
        bearish_ema = last_close < e20 < e50
    else:
        bullish_ema = False
        bearish_ema = False

    # Resample to weekly (every 5 bars ~ weekly close)
    df_weekly = df_daily.copy()
    df_weekly = df_weekly.set_index("date").resample("W")["close"].last().dropna().reset_index()
    rsi_weekly = None
    if len(df_weekly) >= 15:
        rsi_w = RSIIndicator(close=df_weekly["close"], window=14).rsi().dropna()
        if not rsi_w.empty:
            rsi_weekly = round(float(rsi_w.iloc[-1]), 1)

    rsi_daily = daily_rsi  # already computed above

    # Agreement: both timeframes bullish (RSI 40–70) and EMA aligned
    daily_bull  = rsi_daily  is not None and 40 <= rsi_daily  <= 70
    weekly_bull = rsi_weekly is not None and 40 <= rsi_weekly <= 70
    agreement   = bullish_ema and daily_bull and weekly_bull

    mtf_signal = (
        "BULLISH"  if agreement else
        "BEARISH"  if bearish_ema else
        "NEUTRAL"
    )

    return {
        "rsi_daily":             rsi_daily,
        "rsi_weekly":            rsi_weekly,
        "ema20":                 round(e20,  2) if e20  else None,
        "ema50":                 round(e50,  2) if e50  else None,
        "ema200":                round(e200, 2) if e200 else None,
        "current_price":         round(last_close, 2),
        "bullish_ema_alignment": bullish_ema,
        "bearish_ema_alignment": bearish_ema,
        "mtf_agreement":         agreement,
        "mtf_signal":            mtf_signal,
        "confidence_boost":      15 if agreement else 0,
    }
