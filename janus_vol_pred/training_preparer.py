from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from book_ticker_source import prepare_book_ticker_for_date
from features_label import (
    append_verified_record,
    build_features_batch,
    load_verified_records,
    save_features,
    verify_parquet,
)


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def compute_required_feature_dates(reference_date: str, train_days: int, val_days: int) -> list[str]:
    ref_dt = datetime.strptime(reference_date, "%Y-%m-%d")
    required_days = train_days + val_days
    start_dt = ref_dt - timedelta(days=required_days)
    return [_date_str(start_dt + timedelta(days=i)) for i in range(required_days)]


def required_book_ticker_dates(feature_date: str, use_prev_day: bool) -> list[str]:
    feature_dt = datetime.strptime(feature_date, "%Y-%m-%d")
    dates = [feature_date]
    if use_prev_day:
        dates.insert(0, _date_str(feature_dt - timedelta(days=1)))
    return dates


def _verify_or_cleanup_feature(parquet_path: Path) -> int:
    rows = verify_parquet(str(parquet_path))
    if rows > 0:
        return rows
    if parquet_path.exists():
        logger.warning(f"Deleting invalid feature parquet: {parquet_path}")
        parquet_path.unlink()
    return -1


def prepare_feature_date(
    symbol: str,
    feature_date: str,
    config: dict,
    verified_records: set[tuple[str, str]],
) -> bool:

    production_cfg = config["production_train"]
    features_output_dir = Path(production_cfg["output_root"]) / production_cfg["features_output_dir"]
    fallback_book_ticker_root = Path(production_cfg["output_root"]) / production_cfg["fallback_book_ticker_output_dir"]
    verified_csv = str(features_output_dir / "verified_records.csv")
    max_retries = config["execution"].get("incremental_features_max_retries", 2)

    parquet_path = features_output_dir / symbol / feature_date / "features_label.parquet"
    if (symbol, feature_date) in verified_records:
        if parquet_path.exists():
            return True
        logger.warning(f"[{symbol}] {feature_date} found in verified_records but parquet is missing, regenerating.")


    if parquet_path.exists():
        rows = _verify_or_cleanup_feature(parquet_path)
        if rows > 0:
            append_verified_record(verified_csv, symbol, feature_date, rows)
            verified_records.add((symbol, feature_date))
            logger.info(f"[{symbol}] {feature_date} existing feature file verified.")
            return True

    for raw_date in required_book_ticker_dates(feature_date, config["execution"]["use_prev_day"]):
        try:
            prepare_book_ticker_for_date(symbol, raw_date, config, fallback_book_ticker_root)
        except Exception as exc:
            logger.error(f"[{symbol}] {feature_date} raw book_ticker {raw_date} prepare failed: {exc}")
            return False

    for attempt in range(1, max_retries + 1):
        try:
            df = build_features_batch(
                symbol=symbol,
                date=feature_date,
                data_root=config["paths"]["data_root"],
                assets_path=config["paths"]["binance_assets"],
                fallback_book_ticker_root=fallback_book_ticker_root,
                instrument_type=config["execution"]["instrument_type"],
                warmup_minutes=config["sampling"]["warmup_minutes"],
                use_prev_day=config["execution"]["use_prev_day"],
                interval_ns=config["sampling"]["interval_ns"],
                vol_windows=config["features"]["vol_windows"],
                label_vol_window=config["label"]["vol_window"],
            )
            if df.is_empty():
                raise RuntimeError(f"[{symbol}] {feature_date} generated empty features.") # 跳转到 except 进行重试或最终失败记录

            save_path = parquet_path.with_suffix("")
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_features(df, str(save_path), format="parquet")

            rows = _verify_or_cleanup_feature(parquet_path)
            if rows <= 0:
                raise RuntimeError(f"[{symbol}] {feature_date} feature verification failed.")

            append_verified_record(verified_csv, symbol, feature_date, rows)
            verified_records.add((symbol, feature_date))
            logger.info(f"[{symbol}] {feature_date} feature prepared with {rows} rows.")
            return True
        except Exception as exc:
            if parquet_path.exists():
                logger.warning(f"Deleting failed feature parquet before retry: {parquet_path}")
                parquet_path.unlink()
            logger.error(f"[{symbol}] {feature_date} feature prepare failed (attempt {attempt}/{max_retries}): {exc}")

    return False


def prepare_daily_training_features(config: dict, reference_date: str | None = None) -> tuple[set[str], list[str]]:
    production_cfg = config["production_train"]

    reference_date = reference_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    train_cfg = config["train"]
    symbols = config["execution"]["symbols"]

    required_dates = compute_required_feature_dates(
        reference_date=reference_date,
        train_days=train_cfg["train_days"],
        val_days=train_cfg["val_days"],
    )

    features_output_dir = Path(production_cfg["output_root"]) / production_cfg["features_output_dir"]
    verified_records = load_verified_records(str(features_output_dir / "verified_records.csv"))

    failed_dates: list[str] = []
    failed_symbols: set[str] = set() # 同一个 symbol 就算失败很多天，也只会保留一份
    for symbol in symbols:
        for feature_date in required_dates:
            if not prepare_feature_date(symbol, feature_date, config, verified_records):
                failed_dates.append(f"{symbol}:{feature_date}")
                failed_symbols.add(symbol)

    if failed_dates:
        logger.error(f"Daily feature preparation failed for {failed_dates}")
        if production_cfg.get("fail_if_feature_dependency_missing", True):
            logger.warning(f"Skipping production training for failed symbols: {sorted(failed_symbols)}")

    return failed_symbols, required_dates
