"""
model_loader.py - 模型加载与热更新

功能：
- 按日期查找最新模型（当天 → 往前 model_lookback_days 天）
- 加载 model.txt + quantile_transformer.pkl + feature_cols.json
- 单次预测：特征 dict → pred_vol
- 热更新：检测 model.txt mtime，自动 reload
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
from loguru import logger


def find_model_dir(results_dir: Path, symbol: str, lookback_days: int) -> Path | None:
    """从今天往前找，返回第一个有效的模型目录"""
    today = datetime.now(timezone.utc).date()
    for i in range(lookback_days):
        date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        model_dir = results_dir / date / symbol
        if (model_dir / "model.txt").exists():
            return model_dir
    return None


class ModelLoader:
    """
    单个 symbol 的模型加载器。

    使用方式：
        loader = ModelLoader(symbol="XRPUSDT", results_dir=Path("results"), lookback_days=30)
        pred_vol = loader.predict(features_dict)  # None 表示无模型
    """

    def __init__(self, symbol: str, results_dir: Path, lookback_days: int):
        self.symbol = symbol
        self.results_dir = results_dir
        self.lookback_days = lookback_days

        self._model = None
        self._qt = None
        self._feature_cols: list[str] = []
        self._model_dir: Path | None = None
        self._mtime: float = 0.0

        self._load()

    def _load(self) -> None:
        model_dir = find_model_dir(self.results_dir, self.symbol, self.lookback_days)
        if model_dir is None:
            logger.warning(f"[{self.symbol}] 未找到模型，往前找了 {self.lookback_days} 天")
            return
        try:
            model = lgb.Booster(model_file=str(model_dir / "model.txt"))
            qt = joblib.load(model_dir / "quantile_transformer.pkl")
            import json
            with open(model_dir / "feature_cols.json") as f:
                feature_cols = json.load(f)

            self._model = model
            self._qt = qt
            self._feature_cols = feature_cols
            self._model_dir = model_dir
            self._mtime = (model_dir / "model.txt").stat().st_mtime
            logger.info(f"[{self.symbol}] 模型加载成功: {model_dir}")
        except Exception as e:
            logger.error(f"[{self.symbol}] 模型加载失败: {e}")

    def check_and_reload(self) -> None:
        """检测是否有新模型，有则热更新（每次采样调用一次）"""
        model_dir = find_model_dir(self.results_dir, self.symbol, self.lookback_days)
        if model_dir is None:
            return
        try:
            mtime = (model_dir / "model.txt").stat().st_mtime
            if mtime != self._mtime:
                logger.info(f"[{self.symbol}] 检测到新模型，开始 reload: {model_dir}")
                self._load()
        except Exception:
            pass

    def predict(self, features: dict) -> float | None:
        """
        输入特征 dict，返回预测波动率。
        无模型时返回 None。
        """
        if self._model is None:
            return None
        try:
            X = np.array([[features[c] for c in self._feature_cols]])
            y_qt = self._model.predict(X)
            y_pred = self._qt.inverse_transform(y_qt.reshape(1, -1)).flatten()[0]
            return float(y_pred)
        except Exception as e:
            logger.error(f"[{self.symbol}] 预测失败: {e}")
            return None

    @property
    def is_ready(self) -> bool:
        return self._model is not None
