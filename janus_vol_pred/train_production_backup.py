"""
train_production.py - 每日生产重训入口

目标：
- 每天 UTC 00:00 外部触发一次
- 先补齐 train_days + val_days 所需特征
- 使用 train_days 训练、latest_complete_day 验证
- 产出给实盘服务直接加载的最新模型
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import lightgbm as lgb
import joblib
import polars as pl
from loguru import logger
import itertools

from train import downsample_df, load_date, train_one_window
from training_preparer import prepare_daily_training_features
from utils.utils import load_yaml_config

os.environ["POLARS_MAX_THREADS"] = "20"


def latest_complete_day(reference_dt: datetime | None = None) -> str:
    reference_dt = reference_dt or datetime.now(timezone.utc)
    return (reference_dt.date() - timedelta(days=1)).strftime("%Y-%m-%d")


def production_train_dates(target_date: str, train_days: int, val_days: int) -> tuple[list[str], list[str]]:
    """
    以 target_date 作为验证集结束日，向前切分生产训练所需日期。

    规则：
    - 验证集包含 target_date 在内的最近 val_days 天
    - 训练集紧接验证集之前，向前取 train_days 天
    - 返回结果按日期升序排列
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    val_dates = [
        (target_dt - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(val_days)
    ]
    train_dates = [
        (target_dt - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(val_days, val_days + train_days)
    ]
    train_dates.reverse()
    val_dates.reverse()
    return train_dates, val_dates


def production_output_dir(base_dir: str | Path, model_date: str, symbol: str) -> Path:
    return Path(base_dir) / model_date / symbol

def temp_production_output_dir(output_dir: str | Path) -> Path:
    output_dir = Path(output_dir)
    return output_dir.with_name(output_dir.name + ".tmp")

def save_production_artifacts(
    output_dir: Path,
    symbol: str,
    model_date: str,
    train_dates: list[str],
    val_dates: list[str],
    train_result: dict,
    lgb_params: dict,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    train_result["model"].booster_.save_model(str(output_dir / "model.txt"))
    joblib.dump(train_result["qt"], output_dir / "quantile_transformer.pkl")

    with open(output_dir / "feature_cols.json", "w") as f:
        json.dump(train_result["feature_cols"], f)

    summary = {
        "symbol": symbol,
        "model_date": model_date,
        "train_dates": train_dates,
        "val_dates": val_dates,
        "mode": "production",
        "features_used": train_result["feature_cols"],
        "lgb_params": lgb_params,
    }
    if train_result["results"]:
        summary["validation_metrics"] = train_result["results"][0]["metrics"]

    with open(output_dir / "run_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

def verify_production_artifacts(out_dir: Path) -> bool:
    try:
        lgb.Booster(model_file=str(out_dir / "model.txt"))
        joblib.load(out_dir / "quantile_transformer.pkl")

        with open(out_dir / "feature_cols.json", "r", encoding="utf-8") as f:
            feature_cols = json.load(f)
        if not feature_cols:
            return False

        return True
    except Exception:
        return False

def train_symbol_production(
    symbol: str,
    model_date: str,
    features_input_dir: Path,
    results_output_dir: Path,
    train_days: int,
    val_days: int,
    train_freq: int,
    lgb_params: dict,
    vol_windows: list[int],
    label_vol_window: int,
    interval_ns: int,
) -> Path:
    train_dates, val_dates = production_train_dates(model_date, train_days, val_days)
    logger.info(f"[{symbol}] 生产重训 | Train: {train_dates} | Val: {val_dates}")

    train_parts = []
    for d in train_dates:
        df = load_date(str(features_input_dir), symbol, d)
        if df is None:
            raise ValueError(f"[{symbol}] {d} 训练数据不存在")
        train_parts.append(df)
    df_train_raw = pl.concat(train_parts, how="vertical")

    val_parts = []
    for d in val_dates:
        df = load_date(str(features_input_dir), symbol, d)
        if df is None:
            raise ValueError(f"[{symbol}] {d} 验证数据不存在")
        val_parts.append(df)
    df_val_raw = pl.concat(val_parts, how="vertical")

    if train_freq != interval_ns:
        df_train = downsample_df(df_train_raw, train_freq)
        df_val = downsample_df(df_val_raw, train_freq)
    else:
        df_train = df_train_raw
        df_val = df_val_raw

    vol_cols = [f"vol_{w}m" for w in vol_windows]
    ratio_cols = [f"vol_ratio_{w1}m_{w2}m" for w1, w2 in itertools.combinations(vol_windows, 2)]
    time_cols = ["hour", "minute", "tod_sin", "tod_cos"]
    feature_cols = vol_cols + ratio_cols + time_cols
    available_features = [c for c in feature_cols if c in df_train.columns]
    target_col = f"y_vol_{label_vol_window}m"

    train_result = train_one_window(
        df_train=df_train,
        df_val=df_val,
        df_test_list=[df_val],
        feature_cols=available_features,
        target_col=target_col,
        lgb_params=lgb_params,
    )

    out_dir = production_output_dir(results_output_dir, model_date, symbol)
    tmp_out_dir = temp_production_output_dir(out_dir)

    if tmp_out_dir.exists():
        logger.warning(f"Deleting stale temp production output directory: {tmp_out_dir}")
        shutil.rmtree(tmp_out_dir)

    save_production_artifacts(
        output_dir=tmp_out_dir,
        symbol=symbol,
        model_date=model_date,
        train_dates=train_dates,
        val_dates=val_dates,
        train_result=train_result,
        lgb_params=lgb_params,
    )
    return tmp_out_dir


if __name__ == "__main__":
    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = load_yaml_config(config_path)

    exec_cfg = config["execution"]
    train_cfg = config["train"]
    production_cfg = config["production_train"]
    log_dir = Path(production_cfg["log_dir"]) / datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.add(log_dir / "train_production.log", rotation="100 MB", level=config["logging"]["level"])

    target_date = latest_complete_day() # 这一天在生产里通常作为 val_days=1 的验证日，也是模型产出的归档日期。
    prep_reference_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # 准备器的实现是“上界不含当天”，准备 “从今天往前 train_days+val_days 天”的特征
    failed_prepare_symbols, prepared_dates = prepare_daily_training_features(config, reference_date=prep_reference_date)
    logger.info(f"生产重训目标日: {target_date}")
    logger.info(f"准备特征日期 (reference_date={prep_reference_date}): {prepared_dates}")
    if failed_prepare_symbols:
        logger.warning(f"特征准备失败的 symbol 将被跳过: {sorted(failed_prepare_symbols)}")

    features_input_dir = Path(production_cfg["output_root"]) / production_cfg["features_output_dir"]
    results_output_dir = Path(production_cfg["output_root"]) / production_cfg["results_output_dir"]
    max_retries = train_cfg["max_retries"]

    failed_symbols: list[str] = []
    for symbol in exec_cfg["symbols"]:
        if symbol in failed_prepare_symbols:
            failed_symbols.append(symbol)
            logger.error(f"[{symbol}] skipped production training because feature preparation failed")
            continue

        success = False
        out_dir = production_output_dir(results_output_dir, target_date, symbol)
        tmp_out_dir = temp_production_output_dir(out_dir)
        for attempt in range(1, max_retries + 1):
            try:
                tmp_out_dir = train_symbol_production(
                    symbol=symbol,
                    model_date=target_date,
                    features_input_dir=features_input_dir,
                    results_output_dir=results_output_dir,
                    train_days=train_cfg["train_days"],
                    val_days=train_cfg["val_days"],
                    train_freq=train_cfg["train_downsample_freq"],
                    lgb_params=train_cfg["lgb_params"],
                    vol_windows=config["features"]["vol_windows"],
                    label_vol_window=config["label"]["vol_window"],
                    interval_ns=config["sampling"]["interval_ns"],
                )
                if verify_production_artifacts(tmp_out_dir):
                    if out_dir.exists():
                        logger.warning(f"Deleting existing production output directory before replace: {out_dir}")
                        shutil.rmtree(out_dir)

                    tmp_out_dir.replace(out_dir)
                    logger.success(f"[{symbol}] 生产模型已保存并验证通过: {out_dir}")
                    success = True
                    break

                logger.warning(f"[{symbol}] production artifact verification failed (attempt {attempt}/{max_retries})")
                if tmp_out_dir.exists():
                    logger.warning(f"Deleting invalid temp production output directory: {tmp_out_dir}")
                    shutil.rmtree(tmp_out_dir)

            except Exception as exc:
                logger.error(f"[{symbol}] production training failed (attempt {attempt}/{max_retries}): {exc}")
                if tmp_out_dir.exists():
                    logger.warning(f"Deleting failed temp production output directory before retry: {tmp_out_dir}")
                    shutil.rmtree(tmp_out_dir)

        if not success:
            failed_symbols.append(symbol)
            logger.error(f"[{symbol}] production training failed after {max_retries} attempts")

    if failed_symbols:
        raise RuntimeError(f"Production training failed for symbols: {failed_symbols}")
