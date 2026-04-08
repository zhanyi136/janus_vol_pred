from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import StringIO, TextIOWrapper
from pathlib import Path
import os
import time

import numpy as np
import pandas as pd
import polars as pl
import zstandard as zstd
from loguru import logger
import clickhouse_connect


SUPPORTED_EXTENSIONS = (".csv.zst", ".csv.gz")


def build_book_ticker_filename(symbol: str, date: str, ext: str = ".csv.zst") -> str:
    return f"binance-futures_book_ticker_{date}_{symbol}{ext}"

def temp_output_path(output_path: str | Path) -> Path:
    output_path = Path(output_path)
    return output_path.with_name(f"{output_path.stem}.tmp{output_path.suffix}")


def _candidate_paths(root: str | Path | None, symbol: str, date: str) -> list[Path]:
    if not root:
        return []

    base = Path(root) / symbol / "book_ticker"
    candidates: list[Path] = []
    for ext in SUPPORTED_EXTENSIONS:
        filename = build_book_ticker_filename(symbol, date, ext)
        candidates.append(base / filename)
    return candidates


def resolve_book_ticker_path(
    symbol: str,
    date: str,
    tardis_root: str | Path | None,
    fallback_root: str | Path | None = None,
) -> Path | None:
    for candidate in _candidate_paths(tardis_root, symbol, date):
        if candidate.exists():
            return candidate
    for candidate in _candidate_paths(fallback_root, symbol, date):
        if candidate.exists():
            return candidate
    return None


def read_book_ticker_file(filepath: str | Path) -> pl.DataFrame:
    filepath = Path(filepath)
    if filepath.suffix == ".zst":
        with open(filepath, "rb") as f:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(f) as reader:
                text = reader.read().decode("utf-8")
        df = pl.read_csv(StringIO(text))
    else:
        df = pl.read_csv(filepath)

    df = df.with_columns([
        pl.col("timestamp").cast(pl.Int64),
        pl.col("local_timestamp").cast(pl.Int64),
        pl.col("ask_amount").cast(pl.Float64),
        pl.col("ask_price").cast(pl.Float64),
        pl.col("bid_price").cast(pl.Float64),
        pl.col("bid_amount").cast(pl.Float64),
    ])
    df = df.with_columns([
        (pl.col("timestamp") * 1000).alias("timestamp"),
        (pl.col("local_timestamp") * 1000).alias("local_timestamp"),
    ])
    return df.sort(["timestamp", "local_timestamp"])


def fallback_book_ticker_output_path(output_root: str | Path, symbol: str, date: str) -> Path:
    output_root = Path(output_root)
    return (
        output_root
        / symbol
        / "book_ticker"
        / build_book_ticker_filename(symbol, date, ".csv.zst")
    )


def _vectorized_to_microseconds(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(index=series.index, dtype="Int64")

    numeric = pd.to_numeric(series, errors="coerce")
    result = pd.Series(index=series.index, dtype="Int64")

    seconds_mask = numeric < 1e11
    millis_mask = (numeric >= 1e11) & (numeric < 1e14)
    micros_mask = (numeric >= 1e14) & (numeric < 1e17)
    nanos_mask = numeric >= 1e17

    result.loc[seconds_mask] = (numeric.loc[seconds_mask] * 1_000_000).round().astype("Int64")
    result.loc[millis_mask] = (numeric.loc[millis_mask] * 1_000).round().astype("Int64")
    result.loc[micros_mask] = numeric.loc[micros_mask].round().astype("Int64")
    result.loc[nanos_mask] = (numeric.loc[nanos_mask] // 1_000).astype("Int64")

    return result


def convert_best_price_to_tardis_bbo(
    df: pd.DataFrame,
    hangqing_cfg: dict | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "exchange",
                "symbol",
                "timestamp",
                "local_timestamp",
                "ask_amount",
                "ask_price",
                "bid_price",
                "bid_amount",
            ]
        )

    hangqing_cfg = hangqing_cfg or {}

    exchange_cfg = hangqing_cfg.get("exchange_name")
    if exchange_cfg:
        exchange_value = exchange_cfg
    elif "exchange" in df.columns:
        exchange_series = df["exchange"]
        exchange_value = np.where(
            exchange_series == 0,
            "binance-futures",
            np.where(exchange_series.isna(), "unknown", exchange_series.astype(str)),
        )
    else:
        exchange_value = "binance-futures"

    default_local_ts = int(time.time() * 1_000_000)
    local_ts = _vectorized_to_microseconds(df["receiveTime"])
    local_ts = local_ts.fillna(default_local_ts).astype("int64")

    result = pd.DataFrame({
        "exchange": exchange_value,
        "symbol": df["symbol"],
        "timestamp": _vectorized_to_microseconds(df["eventTimeUnix"]),
        "local_timestamp": local_ts,
        "ask_amount": pd.to_numeric(df["askQty"], errors="coerce") if "askQty" in df.columns else pd.Series(np.nan, index=df.index),
        "ask_price": pd.to_numeric(df["askPrice"], errors="coerce") if "askPrice" in df.columns else pd.Series(np.nan, index=df.index),
        "bid_price": pd.to_numeric(df["bidPrice"], errors="coerce") if "bidPrice" in df.columns else pd.Series(np.nan, index=df.index),
        "bid_amount": pd.to_numeric(df["bidQty"], errors="coerce") if "bidQty" in df.columns else pd.Series(np.nan, index=df.index),
    })

    result = result[result["timestamp"].notna()].copy()
    result["timestamp"] = result["timestamp"].astype("int64")
    result["local_timestamp"] = result["local_timestamp"].astype("int64")

    return result.sort_values(["timestamp", "local_timestamp"]).reset_index(drop=True)


def fetch_best_price_day(symbol: str, date: str, hangqing_cfg: dict) -> pd.DataFrame:
    start_time = f"{date} 00:00:00"
    end_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)
    end_time = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    client = clickhouse_connect.get_client(
        host=hangqing_cfg["clickhouse_host"],
        port=hangqing_cfg["clickhouse_port"],
        username=hangqing_cfg["clickhouse_username"],
        password=hangqing_cfg["clickhouse_password"],
        database=hangqing_cfg["clickhouse_database"],
    )
    sql = """
    SELECT *
    FROM perpBestPrice
    WHERE (symbol = %(symbol)s) AND (receiveTime >= %(start_time)s) AND (receiveTime < %(end_time)s)
    """
    resp = client.query(sql, parameters={
        "symbol": symbol,
        "start_time": start_time,
        "end_time": end_time,
    })
    df = pd.DataFrame(resp.named_results())
    if not df.empty and "updateId" in df.columns:
        df = df.sort_values("updateId").reset_index(drop=True)
    return df


def write_tardis_book_ticker(df: pd.DataFrame, output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pl_df = pl.from_pandas(df)
    with open(output_path, "wb") as raw_file:
        cctx = zstd.ZstdCompressor(level=3)
        with cctx.stream_writer(raw_file) as compressed:
            with TextIOWrapper(compressed, encoding="utf-8", newline="") as text_stream:
                pl_df.write_csv(text_stream)


def verify_book_ticker_file(filepath: str | Path) -> bool:
    try:
        df = read_book_ticker_file(filepath)
        return len(df) > 0
    except Exception:
        return False


def prepare_book_ticker_for_date(
    symbol: str,
    date: str,
    config: dict,
    fallback_root: str | Path,
) -> Path:
    path_cfg = config["paths"]
    data_cfg = config.get("data_sources", {})
    max_retries = config["execution"].get("incremental_features_max_retries", 2)

    resolved = resolve_book_ticker_path(
        symbol=symbol,
        date=date,
        tardis_root=path_cfg["data_root"],
        fallback_root=fallback_root,
    )
    if resolved is not None:
        return resolved

    if not data_cfg.get("enable_hangqing_fallback", False):
        raise FileNotFoundError(f"Missing book_ticker for {symbol} {date} and hangqing fallback is disabled.")

    output_path = fallback_book_ticker_output_path(fallback_root, symbol, date)
    logger.info(f"[{symbol}] {date} Tardis data missing, downloading from hangqing.")

    tmp_output_path = temp_output_path(output_path)

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            raw_df = fetch_best_price_day(symbol, date, config["hangqing"])
            standardized = convert_best_price_to_tardis_bbo(raw_df, config["hangqing"])
            if standardized.empty:
                raise RuntimeError(f"[{symbol}] {date} hangqing best price is empty.")

            if tmp_output_path.exists():
                logger.warning(f"Deleting stale temp book_ticker file: {tmp_output_path}")
                tmp_output_path.unlink()

            write_tardis_book_ticker(standardized, tmp_output_path)

            if not verify_book_ticker_file(tmp_output_path):
                if tmp_output_path.exists():
                    logger.warning(f"Deleting invalid generated temp book_ticker file: {tmp_output_path}")
                    tmp_output_path.unlink()
                raise RuntimeError(f"[{symbol}] {date} generated book_ticker failed verification.")

            tmp_output_path.replace(output_path) # 把临时文件 tmp_output_path直接替换/重命名成正式文件 output_path
            logger.info(f"[{symbol}] {date} fallback book_ticker saved to {output_path}")
            return output_path

        except Exception as exc:
            last_exc = exc
            if tmp_output_path.exists():
                logger.warning(f"Deleting failed temp book_ticker artifact before retry: {tmp_output_path}")
                tmp_output_path.unlink()
            logger.error(f"[{symbol}] {date} fallback book_ticker prepare failed (attempt {attempt}/{max_retries}): {exc}")

    raise RuntimeError(f"[{symbol}] {date} fallback book_ticker prepare failed after {max_retries} attempts: {last_exc}")


def utc_today_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")
