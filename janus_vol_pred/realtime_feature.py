"""
realtime_feature.py - 实盘实时特征计算

使用环形缓冲区 + Numba 增量计算，每 20ms 调用一次，延迟 < 1ms。
与 features_label.py 的 Polars 批处理保持语义一致。
"""

from __future__ import annotations

import math
from itertools import combinations

import numpy as np
from numba import njit


# ============================================================
# Numba 核心（需要编译期常量，用全局变量传入）
# ============================================================

@njit
def _rolling_std(buffer: np.ndarray, head: int, count: int, window: int, max_ticks: int) -> float:
    if count < window:
        return np.nan
    s = 0.0
    s2 = 0.0
    for i in range(window):
        v = buffer[(head - 1 - i) % max_ticks]
        s += v
        s2 += v * v
    mean = s / window
    var = s2 / window - mean * mean
    return math.sqrt(var) if var > 1e-30 else 0.0


@njit
def _compute_vols(
    buffer: np.ndarray,
    head: int,
    count: int,
    window_ticks: np.ndarray,
    max_ticks: int,
) -> np.ndarray:
    n = len(window_ticks)
    result = np.empty(n, dtype=np.float64)
    for i in range(n):
        result[i] = _rolling_std(buffer, head, count, window_ticks[i], max_ticks)
    return result


# ============================================================
# 波动率比率
# ============================================================

def _vol_ratio(short: float, long_: float) -> float:
    if math.isnan(short) or math.isnan(long_):
        return math.nan
    if short == 0.0 and long_ == 0.0:
        return 1.0
    if long_ == 0.0:
        return math.nan
    res = short / long_
    if math.isinf(res):
        return math.nan
    return res


# ============================================================
# RealtimeFeatureComputer
# ============================================================

class RealtimeFeatureComputer:
    """
    单个 symbol 的实时特征计算器，参数从 config 传入。

    使用方式：
        computer = RealtimeFeatureComputer(
            tick_size=0.0001,
            vol_windows=[1,3,5,10,15,30,60,120],
            interval_ns=20_000_000,
            warmup_minutes=120,
        )
        features = computer.update(bid, ask, timestamp_ns)
        # 返回 None 表示预热期未结束
    """

    def __init__(
        self,
        tick_size: float,
        vol_windows: list[int],
        interval_ns: int,
        warmup_minutes: int,
    ):
        self.tick_size = tick_size
        self.vol_windows = vol_windows

        ticks_per_min = 60 * 1_000_000_000 // interval_ns

        max_vol_minutes = max(vol_windows)  # 最大波动率窗口，决定缓冲区大小

        self._max_ticks = max_vol_minutes * ticks_per_min    # 缓冲区大小（由特征决定）
        self._warmup_ticks = warmup_minutes * ticks_per_min  # 预热门槛（由配置决定
        self._window_ticks = np.array(
            [w * ticks_per_min for w in vol_windows], dtype=np.int64
        )

        self._buffer = np.zeros(self._max_ticks, dtype=np.float64)
        self._head = 0
        self._count = 0
        self._prev_mid_tick: float | None = None

    def update(self, bid: float, ask: float, timestamp_ns: int) -> dict | None:
        """
        写入一个新 tick，返回特征字典；预热期返回 None。

        Args:
            bid: 买一价
            ask: 卖一价
            timestamp_ns: tradeTime × 1000（纳秒）
        """
        mid_price = (bid + ask) / 2.0
        mid_tick = round(mid_price / self.tick_size, 1)

        mid_chg = 0.0 if self._prev_mid_tick is None else mid_tick - self._prev_mid_tick
        self._prev_mid_tick = mid_tick

        self._buffer[self._head] = mid_chg
        self._head = (self._head + 1) % self._max_ticks
        self._count = min(self._count + 1, self._max_ticks)

        if self._count < self._warmup_ticks:
            return None

        # 波动率
        vols = _compute_vols(self._buffer, self._head, self._count, self._window_ticks, self._max_ticks)
        vol_dict = {f"vol_{w}m": float(vols[i]) for i, w in enumerate(self.vol_windows)}

        # 波动率比率
        ratio_dict = {
            f"vol_ratio_{w1}m_{w2}m": _vol_ratio(vol_dict[f"vol_{w1}m"], vol_dict[f"vol_{w2}m"])
            for w1, w2 in combinations(self.vol_windows, 2)
        }

        # 时间特征
        sec_of_day = (timestamp_ns // 1_000_000_000) % 86400
        hour = float(sec_of_day // 3600)
        minute = float((sec_of_day % 3600) // 60) # (s % 86400) % 3600 == s % 3600
        tod = 2 * math.pi * (hour * 60 + minute) / 1440

        return {
            **vol_dict,
            **ratio_dict,
            "hour": hour,
            "minute": minute,
            "tod_sin": math.sin(tod),
            "tod_cos": math.cos(tod),
        }

    @property
    def is_warmed_up(self) -> bool:
        return self._count >= self._warmup_ticks

    @property
    def buffer_count(self) -> int:
        return self._count
