"""
临时脚本：扫描已有的 features_label.parquet 文件，验证后写入 verified_records.csv
只需跑一次。
"""
from pathlib import Path
import polars as pl
from datetime import datetime
from tqdm import tqdm
from multiprocessing import Pool


features_dir = Path("/data/sigma/zzy/janus/results/vol_pred_prod/features")
verified_csv = features_dir / "verified_records.csv"

# 扫描所有 parquet 文件
parquet_files = sorted(features_dir.glob("*/*/features_label.parquet"))
print(f"找到 {len(parquet_files)} 个文件")

# 加载已有记录（避免重复）
existing = set()
if verified_csv.exists():
    try:
        df = pl.read_csv(str(verified_csv))
        existing = set(zip(df["symbol"].to_list(), df["date"].to_list()))
    except Exception:
        pass

# 过滤掉已有记录
to_verify = [
    (str(p), p.parent.parent.name, p.parent.name)
    for p in parquet_files
    if (p.parent.parent.name, p.parent.name) not in existing
]
print(f"需验证 {len(to_verify)} 个")


def verify_one(args):
    path, symbol, date = args
    try:
        df = pl.read_parquet(path)
        rows = len(df)
        if rows > 0:
            return (symbol, date, rows, None)
        return (symbol, date, 0, "空文件")
    except Exception as e:
        return (symbol, date, 0, str(e))


if __name__ == "__main__":
    with Pool(20) as pool:
        results = list(tqdm(pool.imap_unordered(verify_one, to_verify), total=len(to_verify)))

    added = 0
    failed = 0
    header = not verified_csv.exists() or verified_csv.stat().st_size == 0

    with open(verified_csv, "a", encoding="utf-8") as f:
        if header:
            f.write("symbol,date,rows,verified_at\n")
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        for symbol, date, rows, err in sorted(results):
            if err is None:
                f.write(f"{symbol},{date},{rows},{now}\n")
                added += 1
            else:
                failed += 1
                print(f"失败: {symbol}/{date} - {err}")

    print(f"完成 | 新增: {added} | 失败: {failed} | 已有: {len(existing)}")
