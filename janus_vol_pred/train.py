"""
train.py - 波动率预测模型训练脚本

支持滚动预测：
- test_days = 1: 每日单独训练和预测
- test_days = 2: 每2天训练一次，分别保存每天的结果
"""

from __future__ import annotations
import os
os.environ["POLARS_MAX_THREADS"] = "20"

import json
from datetime import datetime, timedelta
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl
from loguru import logger
from sklearn.preprocessing import QuantileTransformer
from tqdm import tqdm
import itertools
import joblib

import shutil

def load_date(features_input_dir: str, symbol: str, date_str: str) -> pl.DataFrame | None:
    path = Path(features_input_dir) / symbol / date_str / "features_label.parquet"
    if not path.exists():
        return None
    try:
        return pl.read_parquet(path)
    except Exception:
        return None


def downsample_df(df: pl.DataFrame, freq_ns: int | None, ts_col: str = "timestamp") -> pl.DataFrame:
    """
    100ms 窗口内的 5 个 tick：
    100ms  tick: 特征(100ms) → 标签(100ms)  
    120ms tick: 特征(120ms) → 标签(120ms)
    140ms tick: 特征(140ms) → 标签(140ms)
    160ms tick: 特征(160ms) → 标签(160ms)
    180ms tick: 特征(180ms) → 标签(180ms)  ← 用 last 选中这个

    用 last = 选 180ms 的特征和标签
    训练时：用 180ms 的特征预测 180ms 的 y_vol_5m
    结论：特征和标签均来自同一时刻 t 的计算值，使用 last 保持配对的纯净性，没有引入任何额外信息。
    """
    # 时间戳对齐到 freq_ns 的整数倍（向下取整）。
    df = df.with_columns([
        ((pl.col(ts_col) // freq_ns) * freq_ns).alias("ts_window")
    ])

    agg_cols = [c for c in df.columns if c not in [ts_col, "ts_window"]]

    # 按窗口分组求平均，重命名排序列。
    result = df.group_by("ts_window").agg([
        pl.col(c).last() for c in agg_cols
    ]).rename({"ts_window": ts_col}).sort(ts_col)
    return result


def evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    mask = ~(np.isnan(y_true) | np.isnan(y_pred))
    y_true = y_true[mask]
    y_pred = y_pred[mask]
    
    if len(y_true) == 0:
        return {"rmse": float('nan'), "mae": float('nan'), "corr": float('nan'), "qlike": float('nan')}
    
    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
    mae = np.mean(np.abs(y_true - y_pred))
    corr = np.corrcoef(y_true, y_pred)[0, 1] if len(y_true) > 1 else float('nan') # 只有 1 个样本时无法计算，返回 nan
    qlike = np.mean(y_true / y_pred - np.log(y_true / y_pred) - 1)
    
    return {"rmse": float(rmse), "mae": float(mae), "corr": float(corr), "qlike": float(qlike)}


def train_one_window(
    df_train: pl.DataFrame,
    df_val: pl.DataFrame,
    df_test_list: list[pl.DataFrame],
    feature_cols: list[str],
    target_col: str,
    lgb_params: dict,
) -> dict:
    """
    训练单个窗口，返回所有测试天的预测结果
    """
    # 只过滤了标签，特征列的空值没有处理。LightGBM 能自动处理 NaN。不过输入的数据，就是没有nan的。
    df_train = df_train.filter(pl.col(target_col).is_not_null())
    df_val = df_val.filter(pl.col(target_col).is_not_null())
    
    if df_train.is_empty() or df_val.is_empty():
        raise ValueError("Train or Val data is empty")
    
    X_train = df_train.select(feature_cols).to_numpy()
    y_train = df_train.select(target_col).to_numpy().flatten()
    X_val = df_val.select(feature_cols).to_numpy()
    y_val = df_val.select(target_col).to_numpy().flatten()
    
    qt = QuantileTransformer(
        output_distribution='normal',
        n_quantiles=min(len(y_train), 1000),
        random_state=42
    )
    y_train_qt = qt.fit_transform(y_train.reshape(-1, 1)).flatten()
    y_val_qt = qt.transform(y_val.reshape(-1, 1)).flatten()
    
    model = lgb.LGBMRegressor(**lgb_params)
    # 训练时用变换后的 y（正态分布），验证集也做同样变换。早停 50 轮防止过拟合。
    model.fit(
        X_train, y_train_qt,
        eval_set=[(X_val, y_val_qt)],
        callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)]
    )
    
    all_results = []
    for df_test in df_test_list:
        df_test_clean = df_test.filter(pl.col(target_col).is_not_null())
        if df_test_clean.is_empty():
            continue
        
        X_test = df_test_clean.select(feature_cols).to_numpy()
        y_test = df_test_clean.select(target_col).to_numpy().flatten()
        
        y_pred_qt = model.predict(X_test)
        y_pred = qt.inverse_transform(y_pred_qt.reshape(-1, 1)).flatten()
        
        all_results.append({
            "timestamp": df_test_clean.select("timestamp").to_numpy().flatten(),
            "y_true": y_test,
            "y_pred": y_pred,
            "metrics": evaluate(y_test, y_pred),
        })
    
    return {
        "model": model,
        "qt": qt,
        "results": all_results,
        "feature_cols": feature_cols,
    }


def save_daily_results(
    results_output_dir: str,
    symbol: str,
    test_date: str,
    train_result: dict,
    lgb_params: dict,
) -> None:
    """保存单天的结果到对应日期目录"""
    out_dir = Path(results_output_dir) / test_date / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # 找到对应天的结果
    idx = None
    for i, r in enumerate(train_result["results"]):
        date_from_ts = datetime.fromtimestamp(r["timestamp"][0] / 1e9).strftime("%Y-%m-%d")
        if date_from_ts == test_date:
            idx = i
            break
    
    if idx is None:
        logger.warning(f"[{symbol}] {test_date} 没有对应预测结果")
        return
    
    r = train_result["results"][idx]
    
    # # 保存预测结果
    # res_df = pl.DataFrame({
    #     "timestamp": r["timestamp"],
    #     "y_true": r["y_true"],
    #     "y_pred": r["y_pred"]
    # })
    # res_df.write_parquet(out_dir / "results.parquet")

    # 同时保存一份 npz 格式
    np_results = np.zeros(len(r["timestamp"]), dtype=[
        ('timestamp', '<i8'),
        ('y_true', '<f8'),
        ('y_pred', '<f8')
    ])
    np_results['timestamp'] = r["timestamp"].astype(np.int64)
    np_results['y_true'] = r["y_true"].astype(np.float64)
    np_results['y_pred'] = r["y_pred"].astype(np.float64)
    np.savez_compressed(out_dir / "results.npz", data=np_results)
    
    # 保存特征重要性（每天相同）
    # LightGBM 的 feature_importance() 返回的重要性数组，顺序和训练时传入特征的顺序完全一致。
    importance_df = pl.DataFrame({
        "feature": train_result["feature_cols"],
        "importance_gain": train_result["model"].booster_.feature_importance(importance_type="gain").astype(float),
        "importance_split": train_result["model"].booster_.feature_importance(importance_type="split").astype(int),
    }).sort("importance_gain", descending=True)
    importance_df.write_parquet(out_dir / "feature_importance.parquet")
    
    # 摘要
    summary = {
        "symbol": symbol,
        "test_date": test_date,
        "metrics": r["metrics"],
        "features_used": train_result["feature_cols"],
        "lgb_params": lgb_params,
    }
    with open(out_dir / "run_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    # 保存模型
    train_result["model"].booster_.save_model(str(out_dir / "model.txt"))

    # 保存 QuantileTransformer
    joblib.dump(train_result["qt"], out_dir / "quantile_transformer.pkl")

    # 保存特征列名
    with open(out_dir / "feature_cols.json", "w") as f:
        json.dump(train_result["feature_cols"], f)


def train(
    symbol: str,
    test_dates: list[str],
    features_input_dir: str,
    results_output_dir: str,
    train_days: int,
    val_days: int,
    train_freq: int,
    lgb_params: dict,
    vol_windows: list[int],
    label_vol_window: int = 5,
    interval_ns: int = 20_000_000,
) -> dict:
    """训练并预测多个测试天（同一模型），分别保存每天的结果"""
    first_test_date = test_dates[0]
    first_test_dt = datetime.strptime(first_test_date, "%Y-%m-%d")
    
    # 验证集
    val_date_list = [(first_test_dt - timedelta(days=i)).strftime("%Y-%m-%d") 
                    for i in range(1, val_days + 1)]
    
    # 训练集
    train_start_offset = val_days + 1
    train_date_list = [(first_test_dt - timedelta(days=i)).strftime("%Y-%m-%d")
                    for i in range(train_start_offset, train_start_offset + train_days)]
    
    logger.info(f"[{symbol}] 测试: {test_dates} | Train: {train_date_list[-1]}~{train_date_list[0]} | Val: {val_date_list[0]}")
    
    # 生成特征列名
    vol_cols = [f"vol_{w}m" for w in vol_windows]
    # itertools.combinations(seq, r) 按原序列的顺序不重复地选r个元素，生成的是有序组合。
    # seq = [1, 2, 3] # list(itertools.combinations(seq, 2)) # 输出: [(1, 2), (1, 3), (2, 3)]
    ratio_cols = [f"vol_ratio_{w1}m_{w2}m" for w1, w2 in itertools.combinations(vol_windows, 2)]
    time_cols = ["hour", "minute", "tod_sin", "tod_cos"]
    feature_cols = vol_cols + ratio_cols + time_cols
    target_col = f"y_vol_{label_vol_window}m"
    
    # 加载训练集
    train_parts = []
    for d in train_date_list:
        df = load_date(features_input_dir, symbol, d)
        if df is None: # 训练集：7天中任意1天不存在 → 报错，跳过
            raise ValueError(f"[{symbol}] {d} 训练数据不存在")
        train_parts.append(df)
    df_train_raw = pl.concat(train_parts, how="vertical")

    # 加载验证集
    val_parts = []
    for d in val_date_list:
        df = load_date(features_input_dir, symbol, d)
        if df is None: # 验证集：1天中任意1天不存在 → 报错，跳过
            raise ValueError(f"[{symbol}] {d} 验证数据不存在")
        val_parts.append(df)
    df_val_raw = pl.concat(val_parts, how="vertical")

    # 加载测试集
    df_test_list = []
    for d in test_dates:
        df = load_date(features_input_dir, symbol, d)
        if df is None: # 测试集：窗口内任意1天不存在 → 报错，跳过
            raise ValueError(f"[{symbol}] {d} 测试数据不存在")
        df_test_list.append(df)

    # 降频
    if train_freq != interval_ns:
        df_train = downsample_df(df_train_raw, train_freq)
        df_val = downsample_df(df_val_raw, train_freq)
    else:
        df_train = df_train_raw
        df_val = df_val_raw
    
    available_features = [c for c in feature_cols if c in df_train.columns]
    
    # 训练
    train_result = train_one_window(
        df_train, df_val, df_test_list,
        available_features, target_col, lgb_params
    )
    
    # 分别保存每天的结果
    for test_date in test_dates:
        try:
            save_daily_results(
                results_output_dir=results_output_dir,
                symbol=symbol,
                test_date=test_date,
                train_result=train_result,
                lgb_params=lgb_params,
            )
        except Exception as e:
            logger.error(f"[{symbol}] {test_date} 保存失败: {e}")
    
    logger.success(f"[{symbol}] {'~'.join(test_dates)} 完成")
    
    return {"test_dates": test_dates, "train_result": train_result}


def verify_train_result(out_dir: Path) -> bool:
    """验证训练结果是否完整有效"""
    try:
        # 验证 results.npz
        data = np.load(out_dir / "results.npz")["data"]
        if len(data) == 0:
            return False

        # 验证 model.txt
        lgb.Booster(model_file=str(out_dir / "model.txt"))

        # 验证 quantile_transformer.pkl
        joblib.load(out_dir / "quantile_transformer.pkl")

        return True
    except Exception:
        return False

def load_verified_records(csv_path: str) -> set:
    """加载已验证的记录，返回 {(symbol, date), ...} 集合"""
    path = Path(csv_path)
    if not path.exists():
        return set()
    try:
        df = pl.read_csv(csv_path)
        return set(zip(df["symbol"].to_list(), df["date"].to_list()))
    except Exception:
        return set()


def append_verified_record(csv_path: str, symbol: str, date: str) -> None:
    """追加一条验证记录"""
    path = Path(csv_path)
    header = not path.exists()
    with open(path, "a", encoding="utf-8") as f:
        if header:
            f.write("symbol,date,verified_at\n")
        f.write(f"{symbol},{date},{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}\n")



if __name__ == "__main__":
    from utils.utils import load_yaml_config, generate_date_list

    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = load_yaml_config(config_path)
    
    exec_cfg = config["execution"]
    train_cfg = config["train"]
    path_cfg = config["paths"]
    
    log_dir = Path(path_cfg["log_root"]) / datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.add(log_dir / "train.log", rotation="100 MB", level=config["logging"]["level"])
    
    symbols = exec_cfg["symbols"]
    dates = generate_date_list(exec_cfg["start_date"], exec_cfg["end_date"])
    
    features_input_dir = Path(path_cfg["output_root"]) / train_cfg["features_input_dir"]
    results_output_dir = Path(path_cfg["output_root"]) / train_cfg["results_output_dir"]
    
    train_days = train_cfg["train_days"]
    val_days = train_cfg["val_days"]
    test_days = train_cfg["test_days"]
    train_freq = train_cfg["train_downsample_freq"]
    lgb_params = train_cfg["lgb_params"]
    vol_windows = config["features"]["vol_windows"]
    label_vol_window = config["label"]["vol_window"]
    
    incremental_enabled = train_cfg["incremental"]
    max_retries = train_cfg["max_retries"]
    
    # 验证记录文件
    verified_csv = str(results_output_dir / "verified_records.csv")
    verified_records = load_verified_records(verified_csv)
    
    logger.info(f"币种: {symbols}")
    logger.info(f"日期: {dates}")
    logger.info(f"训练: {train_days}天 | 验证: {val_days}天 | 测试: {test_days}天/窗口")
    logger.info(f"已验证记录: {len(verified_records)} 条")
    
    # 按 test_days 分组
    windows = []
    i = 0
    while i < len(dates):
        window_dates = dates[i:i + test_days]
        windows.append(window_dates)
        i += test_days
    
    total = len(symbols) * len(windows)
    skipped = 0
    failed = 0
    
    for symbol in symbols:
        for window_dates in tqdm(windows, desc=f"{symbol}"):
            
            # 增量检查：看验证记录
            all_verified = all((symbol, d) in verified_records for d in window_dates)
            if incremental_enabled and all_verified:
                skipped += 1
                continue
            
            # 如果目录存在但不在验证记录中，先尝试验证
            all_pass = True
            for d in window_dates:
                out_dir = results_output_dir / d / symbol
                if (symbol, d) in verified_records:
                    continue
                if out_dir.exists():
                    if verify_train_result(out_dir):
                        append_verified_record(verified_csv, symbol, d)
                        verified_records.add((symbol, d))
                        logger.info(f"[{symbol}] {d} 已有结果验证通过，跳过")
                    else:
                        shutil.rmtree(out_dir)
                        logger.warning(f"[{symbol}] {d} 已有结果验证失败，已删除")
                        all_pass = False
                else:
                    all_pass = False
            
            # 如果所有天都验证通过了，跳过
            if all_pass and all((symbol, d) in verified_records for d in window_dates):
                skipped += 1
                continue
            
            # 最多重试 max_retries 次
            success = False
            for attempt in range(1, max_retries + 1):
                try:
                    train(
                        symbol=symbol,
                        test_dates=window_dates,
                        features_input_dir=str(features_input_dir),
                        results_output_dir=str(results_output_dir),
                        train_days=train_days,
                        val_days=val_days,
                        train_freq=train_freq,
                        lgb_params=lgb_params,
                        vol_windows=vol_windows,
                        label_vol_window=label_vol_window,
                        interval_ns=config["sampling"]["interval_ns"],
                    )
                    
                    # 验证所有天的结果
                    all_ok = True
                    for d in window_dates:
                        out_dir = results_output_dir / d / symbol
                        if verify_train_result(out_dir):
                            append_verified_record(verified_csv, symbol, d)
                            verified_records.add((symbol, d))
                        else:
                            all_ok = False
                            if out_dir.exists():
                                shutil.rmtree(out_dir)
                            logger.warning(f"[{symbol}] {d} 验证失败 (第{attempt}次)")
                    
                    if all_ok:
                        success = True
                        break
                    
                except Exception as e:
                    logger.error(f"[{symbol}] {window_dates} 失败 (第{attempt}次): {e}")
                    # 删除可能的残留
                    for d in window_dates:
                        out_dir = results_output_dir / d / symbol
                        if out_dir.exists():
                            shutil.rmtree(out_dir)
            
            if not success:
                failed += 1
                logger.error(f"[{symbol}] {window_dates} {max_retries}次重试后仍失败")
    
    logger.info(f"完成 | 总: {total} | 跳过: {skipped} | 失败: {failed}")