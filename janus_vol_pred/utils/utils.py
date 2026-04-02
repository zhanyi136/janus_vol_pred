"""
utils/utils.py - 通用工具函数

包含：
- YAML配置文件加载
- binance_assets.json加载
- tick_size获取
- 项目路径工具
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import yaml
import json
from datetime import datetime, timedelta


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    加载YAML配置文件

    Args:
        config_path: 配置文件路径 (绝对路径或相对于项目根目录)

    Returns:
        配置字典
    """
    config_path = Path(config_path)

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def load_asset_info(
    symbol: str,
    assets_path: str,
    instrument_type: str = "future"
) -> Dict[str, float]:
    """
    加载指定币种的交易规范
    
    Args:
        symbol: 币种名称，如 "XRPUSDT"
        assets_path: binance_assets.json 文件路径
        instrument_type: "future" 或 "spot"，默认 "future"
    
    Returns:
        包含 tick_size 和 lot_size 的字典
    
    Example:
        >>> info = load_asset_info("XRPUSDT", "/path/to/assets.json")
        >>> info = load_asset_info("BTCUSDT", "/path/to/assets.json", "spot")
        >>> tick_size = load_asset_info("XRPUSDT", "/path/to/assets.json")["tick_size"]
    """
    with open(assets_path, 'r') as f:
        assets = json.load(f)
    
    if instrument_type not in assets:
        raise KeyError(f"instrument_type '{instrument_type}' not found in assets")
    
    if symbol not in assets[instrument_type]:
        raise KeyError(f"Symbol {symbol} not found in {instrument_type}")
    
    return assets[instrument_type][symbol]


def generate_date_list(start_date: str, end_date: str) -> list[str]:
    """
    生成日期列表
    
    Args:
        start_date: 开始日期 "YYYY-MM-DD"
        end_date: 结束日期 "YYYY-MM-DD"
    
    Returns:
        日期字符串列表
    
    Example:
        >>> generate_date_list("2026-01-01", "2026-01-03")
        ["2026-01-01", "2026-01-02", "2026-01-03"]
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return dates
