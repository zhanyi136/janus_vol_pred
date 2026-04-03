"""
features.py - 波动率预测特征计算模块

包含两种模式：
1. 批量处理 (build_features_batch): 用 Polars 向量化，用于训练/回测
2. 实时推理 (RealtimeFeatureComputer): 用 Numba + 环形缓冲，用于实盘

特征列表（基于重要性分析）:
- 波动率因子: vol_1m, vol_3m, vol_5m, vol_10m, vol_15m, vol_30m, vol_60m, vol_120m
- 波动率比率: vol_ratio_5_30, vol_ratio_30_120
- 时间特征: hour, minute, tod_sin, tod_cos
"""

from __future__ import annotations

import os
os.environ["POLARS_MAX_THREADS"] = "10"
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import numpy as np
import polars as pl
from numba import njit
from loguru import logger
import zstandard as zstd
from itertools import combinations

def generate_time_grid(
    date: str,
    interval_ns: int = 20_000_000,
    use_prev_day: bool = False,
) -> pl.DataFrame:
    """
    生成规整的20ms时间网格

    Args:
        date: 日期字符串，如 "2026-03-19"
        interval_ns: 间隔（纳秒），默认20ms
        use_prev_day: 是否包含前一天完整时间网格（用于波动率计算需要完整历史）

    Returns:
        DataFrame with single column 'ts', 规整的时间戳网格
    """
    from datetime import timedelta as td

    day_start = int(datetime.strptime(date, "%Y-%m-%d").timestamp() * 1e9)
    day_end = day_start + 24 * 60 * 60 * 1_000_000_000

    if use_prev_day:
        prev_date = (datetime.strptime(date, "%Y-%m-%d") - td(days=1)).strftime("%Y-%m-%d")
        prev_day_start = int(datetime.strptime(prev_date, "%Y-%m-%d").timestamp() * 1e9)
        prev_day_end = prev_day_start + 24 * 60 * 60 * 1_000_000_000

        prev_grid = np.arange(prev_day_start, prev_day_end, interval_ns, dtype=np.int64)
        today_grid = np.arange(day_start, day_end, interval_ns, dtype=np.int64)
        grid = np.concatenate([prev_grid, today_grid])
        return pl.DataFrame({"timestamp": grid})
    else:
        grid = np.arange(day_start, day_end, interval_ns, dtype=np.int64)
        return pl.DataFrame({"timestamp": grid})

# ============================================================
# 数据加载（支持两种压缩格式）
# ============================================================

def load_book_ticker(
    symbol: str,
    date: str,
    data_root: str,
) -> pl.DataFrame:
    """
    用 Polars 高效加载 book_ticker 数据

    自动处理两种压缩格式：.csv.gz 和 .csv.zst
    """
    base_path = Path(data_root) / symbol / "book_ticker"

    # 尝试两种压缩格式
    for ext in ['.csv.gz', '.csv.zst']:
        filename = f"binance-futures_book_ticker_{date}_{symbol}{ext}"
        filepath = base_path / filename
        if filepath.exists():
            logger.debug(f"加载book_ticker: {filepath}")
            break
    else:
        raise FileNotFoundError(
            f"未找到文件: {base_path}/binance-futures_book_ticker_{date}_{symbol}.csv.{{gz,zst}}"
        )

    if str(filepath).endswith('.zst'):
        # zstd压缩格式：先解压到内存
        with open(filepath, 'rb') as f:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(f) as dr:
                text_stream = dr.read()
                text = text_stream.decode('utf-8')
                from io import StringIO
                df = pl.read_csv(StringIO(text))
    else:
        df = pl.read_csv(filepath)

    # 3. 确保数据类型正确
    df = df.with_columns([
        pl.col('timestamp').cast(pl.Int64),
        pl.col('local_timestamp').cast(pl.Int64),
        pl.col('ask_amount').cast(pl.Float64),
        pl.col('ask_price').cast(pl.Float64),
        pl.col('bid_price').cast(pl.Float64),
        pl.col('bid_amount').cast(pl.Float64),
    ])

    # ============================================================
    # 微秒转纳秒（数据源是微秒，统一转成纳秒）
    # ============================================================
    df = df.with_columns([
        (pl.col('timestamp') * 1000).alias('timestamp'),
        (pl.col('local_timestamp') * 1000).alias('local_timestamp'),
    ])

    # 按时间排序
    df = df.sort(['timestamp', 'local_timestamp'])

    logger.debug(f"加载完成: {len(df)} 行, 时间范围: {df['timestamp'].min()} ~ {df['timestamp'].max()}")

    return df


def load_book_ticker_with_prev_day(
    symbol: str,
    date: str,
    data_root: str,
) -> pl.DataFrame:
    """
    加载当天的数据，并额外加载前一天完整的数据（用于补充开头）

    当 need_prev_day=True 时，会加载前一天完整数据，
    这样可以保证120分钟波动率等需要充足历史的场景不会丢失开头数据

    当 need_prev_day=False 时，只加载当天的数据
    """
    from datetime import timedelta

    prev_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    # 加载前一天完整的数据
    df_prev = load_book_ticker(symbol, prev_date, data_root)

    # 加载当天的数据
    df_today = load_book_ticker(symbol, date, data_root)

    # 合并
    df = pl.concat([df_prev, df_today], how="vertical")
    df = df.sort(['timestamp', 'local_timestamp'])
    logger.debug(f"合并后总数据: {len(df)} 行")

    return df

# ============================================================
# 采样：使用 Polars asof_join 实现"严格小于T"的条件
# ============================================================

def sample_book_ticker_asof(
    book_ticker_df: pl.DataFrame,
    time_grid_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    功能：对原始 book_ticker 数据按时间网格进行高效采样。

    采样逻辑：
    对于网格时间 T，找到所有 local_timestamp < T 的记录
    取其中 local_timestamp 最大的那条
    同一 local_timestamp 有多条时，取 timestamp 最大的那条
    关键保证：

    local_timestamp < T — 通过 asof_join 的 backward 策略
    timestamp < T — 因为 local_timestamp >= timestamp 是过滤前置条件
    同一 local_timestamp 取最大 timestamp — 通过排序 + asof_join 的"取最后一条"特性
    为什么用 local_timestamp 而不是 timestamp：

    如果用 timestamp 做 join key，asof_join 只保证 bt.timestamp < T，但不保证 bt.local_timestamp < T
    用 local_timestamp 做 join key，因为 local_timestamp >= timestamp，自然同时满足两者都 < T

    Args:
        book_ticker_df: 原始book_ticker数据
        time_grid_df: 规整的20ms时间网格

    Returns:
        采样后的DataFrame
    """
    # 确保列名正确
    bt = book_ticker_df.select([
        'timestamp',
        'local_timestamp',
        'bid_price',
        'ask_price',
        'bid_amount',
        'ask_amount',
    ])

    # 过滤"正常"数据：local_timestamp >= timestamp
    bt_filtered = bt.filter(pl.col('local_timestamp') >= pl.col('timestamp'))

    # join_asof 要求右边表按 key 排好序
    bt_filtered = bt_filtered.sort(['local_timestamp', 'timestamp'])

    # asof_join：自动取 < T 的最近记录，同一 local_timestamp 取最后一条（即最大 timestamp）
    # sampled的列名：['timestamp', 'timestamp_bt', 'local_timestamp', 'bid_price', 'ask_price', 'bid_amount', 'ask_amount']
    sampled = time_grid_df.join_asof(
        bt_filtered, # 右边表
        left_on='timestamp',      # 左边表的key，即time_grid 的时间（纳秒）
        right_on='local_timestamp',  # 右边表的key，改用 local_timestamp
        strategy='backward', # 取 < T 的最近记录
        suffix='_bt',  # 只加在右边表中与左边表同名的列上，bt_filtered.timestamp → timestamp_bt（被加后缀）
    )
    print(sampled.columns)

        # 整理列名
    sampled = sampled.select([
        'timestamp', 'bid_price', 'ask_price', 'bid_amount', 'ask_amount',
    ])

    logger.debug(f"采样完成: {len(sampled)} 行")

    return sampled


def compute_volatility_features(
    df: pl.DataFrame,
    vol_windows: List[int],
) -> pl.DataFrame:
    """
    计算波动率特征和波动率比率

    Args:
        df: 包含 mid_chg 列的 DataFrame
        vol_windows: 波动率窗口列表（分钟），如 [1, 3, 5, 10, 15, 30, 60, 120]

    波动率计算，一开始因为数据不足，那么就为nan

    波动率比率计算：
    | short | long | 条件触发 | 结果 | 原因 |
    |:-----:|:----:|:--------:|:----:|:-----|
    | > 0 | > 0 | 无 | short/long | 正常比率 |
    | 0 | 0 | both_zero | 1 | 完全无波动，比率为1 |
    | 0 | > 0 | 无 | 0 | 波动率在下降 |
    | > 0 | 0 | is_inf | nan | 理论上不可能出现，长周期波动率为0，短周期波动率一定为0，设为nan |
    | nan | 任意 | either_nan | nan | 数据不足，不确定 |
    | 任意 | nan | either_nan | nan | 数据不足，不确定 |


    Returns:
        添加了 vol_{w}m 和 vol_ratio_* 列的 DataFrame
    """
    # 计算各窗口滚动波动率
    for w_min in vol_windows:
        w_ticks = w_min * 60 * 1000 // 20
        df = df.with_columns([
            pl.col('mid_chg')
                .rolling_std(window_size=w_ticks, min_samples=w_ticks)
                .fill_null(float('nan'))
                .alias(f'vol_{w_min}m')
        ])

    # 生成波动率比率（如 vol_ratio_1m_3m, vol_ratio_1m_5m, vol_ratio_3m_5m）
    for w1, w2 in combinations(vol_windows, 2):
        if w2 > w1:  # w1 是短期，w2 是长期
            short_col = pl.col(f'vol_{w1}m')
            long_col = pl.col(f'vol_{w2}m')

            # 基础比率
            ratio = short_col / long_col

            # 条件：两个都是 0 → 设为 1
            both_zero = (short_col == 0) & (long_col == 0)

            # 条件：有一个是 nan 或 结果是 inf → 结果为 nan
            either_nan = short_col.is_nan() | long_col.is_nan()
            is_inf = (ratio == float('inf')) | (ratio == float('-inf')) | ((short_col > 0) & (long_col == 0))

            ratio = pl.when(both_zero).then(1.0) \
                    .when(either_nan | is_inf).then(pl.lit(float('nan'))) \
                    .otherwise(ratio)

            df = df.with_columns([
                ratio.alias(f'vol_ratio_{w1}m_{w2}m')
            ])

    return df


def compute_time_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    从 timestamp 列计算时间特征

    Args:
        df: 包含 timestamp 列的 DataFrame

    Returns:
        添加了 hour, minute, tod_sin, tod_cos 列的 DataFrame
    """
    df = df.with_columns([
        ((pl.col('timestamp') // 1_000_000_000) % 86400 // 3600).cast(pl.Float64).alias('hour'),
        ((pl.col('timestamp') // 1_000_000_000) % 3600 // 60).cast(pl.Float64).alias('minute'),
    ])
    # sin 和 cos 组合才能区分任意两个不同时间点
    df = df.with_columns([
        (2 * np.pi * (pl.col('hour') * 60 + pl.col('minute')) / (24 * 60)).sin().alias('tod_sin'),
        (2 * np.pi * (pl.col('hour') * 60 + pl.col('minute')) / (24 * 60)).cos().alias('tod_cos'),
    ])
    return df


# ============================================================
# 批量处理：Polaras 向量化（用于训练/回测）
# ============================================================

def build_features_batch(
    symbol: str,
    date: str,
    data_root: str,
    assets_path: str,
    instrument_type: str = "future",
    warmup_minutes: int = 0,
    use_prev_day: bool = True,
    interval_ns: int = 20_000_000,
    vol_windows: List[int] = [1, 3, 5],
    label_vol_window: int = 5,
) -> pl.DataFrame:
    """
    批量构建一天的特征和标签（Polars向量化）

    流程：
    1. 加载 book_ticker 数据（支持跨天拼接）
    2. 生成规整的20ms时间网格
    3. 用 asof_join 高效采样
    4. 向量化计算 mid_price, mid_tick, diff
    5. Polars rolling.std() 计算滚动波动率
    6. shift() 生成标签
    7. 添加时间特征

    速度：几秒~几十秒完成一天数据

    Args:
        symbol: 币种名称
        date: 日期
        data_root: 数据根目录
        assets_path: binance_assets.json 路径
        warmup_minutes: 预热期（分钟），默认15
        use_prev_day: 是否加载前一天数据补充开头

    Returns:
        Polars DataFrame，特征和标签数据
    """
    logger.info(f"开始构建特征: {symbol} {date}")

    tick_size = load_asset_info(symbol=symbol, assets_path=assets_path, instrument_type=instrument_type)["tick_size"]
    print(f"symbol: {symbol}, date: {date}, tick_size: {tick_size}")
    warmup_ns = warmup_minutes * 60 * 1_000_000_000

    # 1. 加载数据（可选跨天拼接）
    if use_prev_day:
        df = load_book_ticker_with_prev_day(symbol=symbol, date=date, data_root=data_root)
    else:
        df = load_book_ticker(symbol=symbol, date=date, data_root=data_root)

    # 2. 生成时间网格
    time_grid = generate_time_grid(date=date, interval_ns=interval_ns, use_prev_day=use_prev_day)

    # 3. 采样
    df = sample_book_ticker_asof(df, time_grid)

    if len(df) == 0:
        logger.warning(f"采样后无数据，请检查数据是否正确: {date}")
        return df

    # 4. 向量化计算
    # 注意：Polars的with_columns里不能引用刚创建的列，所以分成两步
    df = df.with_columns([
        ((pl.col('ask_price') + pl.col('bid_price')) / 2).alias('mid_price'),
    ])
    df = df.with_columns([
        (pl.col('mid_price') / tick_size).round(1).alias('mid_tick'),
    ])

    # 5. 计算 mid_tick 差分
    df = df.with_columns([
        pl.col('mid_tick').diff().fill_null(0.0).alias('mid_chg')
    ])

    # 5. 计算波动率特征
    df = compute_volatility_features(df, vol_windows)

    # 6. 计算时间特征
    df = compute_time_features(df)

    # 7. 生成标签（forward shift）
    label_vol_col = f'vol_{label_vol_window}m'
    forward_shift_ticks = label_vol_window * 60 * 1000_000_000 // interval_ns

    if label_vol_window in vol_windows:
        df = df.with_columns([
            pl.col(label_vol_col).shift(-forward_shift_ticks).alias(f'y_vol_{label_vol_window}m')
        ])
    else:
        raise ValueError(f"vol_windows 必须包含 {label_vol_window}，以生成 y_vol_{label_vol_window}m 标签")
    

    # 8. 数据裁剪
    today_start = int(datetime.strptime(date, "%Y-%m-%d").timestamp() * 1e9)
    warmup_ns = warmup_minutes * 60 * 1_000_000_000

    # 步骤1：去掉 NaN 的行（先清理数据不足的行）
    df = df.drop_nulls()

    # 步骤2：去掉前一天的数据（只保留今天的）
    df = df.filter(pl.col("timestamp") >= today_start)

    # 步骤3：去掉预热期的数据
    if warmup_ns > 0:
        df = df.filter(pl.col("timestamp") >= today_start + warmup_ns)

    # 9. 动态生成 FEATURE_NAMES
    vol_cols = [f'vol_{w}m' for w in vol_windows]
    ratio_cols = [f'vol_ratio_{w1}m_{w2}m' for w1, w2 in combinations(vol_windows, 2) if w2 > w1]
    time_cols = ['hour', 'minute', 'tod_sin', 'tod_cos']
    feature_names = vol_cols + ratio_cols + time_cols

    # 10. 选择最终列
    df = df.select([
        'timestamp',
        *feature_names,
        f'y_vol_{label_vol_window}m',
    ])

    logger.info(f"特征构建完成: {len(df)} 行")

    return df


def save_features(
    df: pl.DataFrame,
    output_path: str,
    format: str = "parquet",
) -> None:
    """
    保存特征数据

    格式选择：
    - parquet: 列式存储，压缩率高，支持按列查询，推荐用于训练/回测
    - npz: numpy压缩格式，适合小数据或需要numpy快速加载的场景

    Args:
        df: 特征DataFrame
        output_path: 输出路径（不含扩展名）
        format: "parquet" 或 "npz"
    """
    if format == "parquet":
        save_path = f"{output_path}.parquet"
        df.write_parquet(save_path)
        logger.info(f"已保存为Parquet: {save_path}")
    elif format == "npz":
        save_path = f"{output_path}.npz"
        # 转换为numpy结构化数组
        np_data = df.to_numpy()
        np.savez_compressed(save_path, data=np_data)
        logger.info(f"已保存为NPZ: {save_path}")
    else:
        raise ValueError(f"不支持的格式: {format}")

def verify_parquet(path: str) -> int:
    """验证 parquet 文件是否有效，返回行数，失败返回 -1"""
    try:
        df = pl.read_parquet(path)
        return len(df) if len(df) > 0 else -1
    except Exception:
        return -1


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


def append_verified_record(csv_path: str, symbol: str, date: str, rows: int) -> None:
    """追加一条验证记录"""
    path = Path(csv_path)
    header = not path.exists()
    with open(path, "a", encoding="utf-8") as f:
        if header:
            f.write("symbol,date,rows,verified_at\n")
        f.write(f"{symbol},{date},{rows},{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}\n")


# ============================================================
# 测试入口
# ============================================================

if __name__ == "__main__":
    from utils.utils import load_yaml_config, generate_date_list, load_asset_info
    import time
    from tqdm import tqdm

    logger.info("测试特征构建...")

    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = load_yaml_config(config_path)

    # 从配置读取参数
    symbols = config['execution']['symbols']
    dates = generate_date_list(
        config['execution']['start_date'],
        config['execution']['end_date']
    )

    # 从配置读取输出目录
    output_root = Path(config['paths']['output_root'])
    features_output_dir = output_root / config['paths']['features_output_dir']
    features_output_dir.mkdir(parents=True, exist_ok=True)

    # 验证记录文件
    verified_csv = str(features_output_dir / "verified_records.csv")
    verified_records = load_verified_records(verified_csv)

    incremental_enabled = config['execution']['incremental_features_label']
    max_retries = config['execution']['incremental_features_max_retries']

    logger.info(f"币种: {symbols}")
    logger.info(f"日期: {dates}")
    logger.info(f"输出目录: {features_output_dir}")
    logger.info(f"已验证记录: {len(verified_records)} 条")

    total = len(symbols) * len(dates)
    current = 0
    skipped = 0
    failed = 0

    for symbol in symbols:
        for date in tqdm(dates, desc=f"{symbol}"):
            current += 1
            save_path = features_output_dir / symbol / date / "features_label"
            parquet_path = save_path.with_suffix('.parquet')

            # 增量检查：看验证记录，而非文件是否存在
            if incremental_enabled and (symbol, date) in verified_records:
                skipped += 1
                continue

            # 如果文件存在但不在验证记录中，先尝试验证
            if incremental_enabled and parquet_path.exists():
                rows = verify_parquet(str(parquet_path))
                if rows > 0:
                    append_verified_record(verified_csv, symbol, date, rows)
                    verified_records.add((symbol, date))
                    logger.info(f"[{current}/{total}] {symbol} {date} 已有文件验证通过 ({rows} 行)，跳过")
                    skipped += 1
                    continue
                else:
                    logger.warning(f"[{symbol}] {date} 已有文件验证失败，删除后重新生成")
                    parquet_path.unlink()


            # 最多重试 max_retries 次
            success = False
            for attempt in range(1, max_retries + 1):
                t0 = time.time()
                try:
                    df = build_features_batch(
                        symbol=symbol,
                        date=date,
                        data_root=config['paths']['data_root'],
                        assets_path=config['paths']['binance_assets'],
                        instrument_type=config['execution']['instrument_type'],
                        warmup_minutes=config['sampling']['warmup_minutes'],
                        use_prev_day=config['execution']['use_prev_day'],
                        interval_ns=config['sampling']['interval_ns'],
                        vol_windows=config['features']['vol_windows'],
                        label_vol_window=config['label']['vol_window'],
                    )

                    save_path.parent.mkdir(parents=True, exist_ok=True)
                    save_features(df, str(save_path), format="parquet")

                    # 验证
                    rows = verify_parquet(str(parquet_path))
                    if rows > 0:
                        append_verified_record(verified_csv, symbol, date, rows)
                        verified_records.add((symbol, date))
                        t1 = time.time()
                        logger.info(f"[{current}/{total}] {symbol} {date} 完成 ({rows} 行), 耗时: {t1 - t0:.2f}秒")
                        success = True
                        break
                    else:
                        logger.warning(f"[{symbol}] {date} 验证失败 (第{attempt}次), 删除后重试")
                        if parquet_path.exists():
                            parquet_path.unlink()

                except Exception as e:
                    t1 = time.time()
                    logger.error(f"[{symbol}] {date} 生成失败 (第{attempt}次), 耗时: {t1 - t0:.2f}秒, 错误: {e}")
                    if parquet_path.exists():
                        parquet_path.unlink()

            if not success:
                failed += 1
                logger.error(f"[{symbol}] {date} {max_retries}次重试后仍失败")

    if skipped > 0:
        logger.info(f"增量模式：跳过了 {skipped} 个已验证的文件")
    if failed > 0:
        logger.warning(f"失败 {failed} 个任务")

    logger.success(f"全部完成! 共 {total} 个任务")

