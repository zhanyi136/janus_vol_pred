from __future__ import annotations

import shlex
import stat
import subprocess
from pathlib import Path

from loguru import logger

def _check_local_artifact_dir(source_dir: Path, required_files: list[str]) -> None:
    if not source_dir.exists():
        raise FileNotFoundError(f"Local model directory does not exist: {source_dir}")

    missing_files = [name for name in required_files if not (source_dir / name).exists()]
    if missing_files:
        raise FileNotFoundError(f"Missing required model files in {source_dir}: {missing_files}")


def build_rsync_command(
    source_dir: Path,
    remote_dir: str,
    publish_cfg: dict,
) -> list[str]:
    ssh_key_path = str(Path(publish_cfg["ssh_key_path"]).expanduser())
    ssh_bin = publish_cfg.get("ssh_bin", "ssh")
    rsync_bin = publish_cfg.get("rsync_bin", "rsync")

    cmd = [
        rsync_bin,
        "-avz",
        "--mkpath",
        '--include=*/',
    ]
    for filename in publish_cfg["files_to_sync"]:
        cmd.append(f"--include={filename}")
    cmd.extend([
        "--exclude=*",
        str(source_dir) + "/", # 会把里面的文件发到目标目录，而不是额外嵌套一层目录。
        f'{publish_cfg["remote_user"]}@{publish_cfg["remote_host"]}:{remote_dir}/',
        "-e",
        f"{ssh_bin} -i {shlex.quote(ssh_key_path)}", # 如果路径里有特殊字符或空格，它会帮你做合适的 shell 转义
    ])
    return cmd


def publish_symbol_model(
    model_date: str,
    symbol: str,
    production_cfg: dict,
    publish_cfg: dict,
) -> None:
    source_dir = (
        Path(production_cfg["output_root"])
        / production_cfg["results_output_dir"]
        / model_date
        / symbol
    )
    # rstrip("/")把字符串末尾多余的 / 去掉
    remote_dir = f'{publish_cfg["remote_results_root"].rstrip("/")}/{model_date}/{symbol}'

    # 把配置里的 SSH 私钥路径，转换成一个真正可用的本地路径对象。
    files_to_sync = publish_cfg["files_to_sync"]

    _check_local_artifact_dir(source_dir, files_to_sync)

    cmd = build_rsync_command(source_dir, remote_dir, publish_cfg)
    logger.info(f"[{symbol}] Publishing production model for {model_date} to {remote_dir}")
    logger.debug(f"[{symbol}] rsync command: {' '.join(cmd)}")

    completed = subprocess.run(
        cmd,
        capture_output=True, # 把命令执行时的：标准输出（stdout）和标准错误（stderr）都捕获到 completed 对象里，而不是直接显示在控制台上。
        text=True, # 把输出结果按文本字符串处理，而不是字节。
        timeout=publish_cfg.get("publish_timeout_sec", 300),
        check=False, # 即使命令返回非 0，也先不要自动抛异常。
    )
    if completed.stdout:
        logger.info(f"[{symbol}] rsync stdout: {completed.stdout.strip()}")
    if completed.stderr:
        logger.warning(f"[{symbol}] rsync stderr: {completed.stderr.strip()}")

    if completed.returncode != 0:
        raise RuntimeError(f"[{symbol}] rsync failed with exit code {completed.returncode}")


def publish_models(
    model_date: str,
    successful_symbols: list[str],
    config: dict,
) -> dict:
    publish_cfg = config["publish"]
    production_cfg = config["production_train"]

    if not publish_cfg.get("enabled", False):
        logger.info("Model publishing is disabled in config.")
        return {
            "published_symbols": [],
            "failed_symbols": [],
        }

    published_symbols: list[str] = []
    failed_symbols: list[str] = []
    for symbol in successful_symbols:
        try:
            publish_symbol_model(
                model_date=model_date,
                symbol=symbol,
                production_cfg=production_cfg,
                publish_cfg=publish_cfg,
            )
            published_symbols.append(symbol)
            logger.success(f"[{symbol}] Model publish succeeded.")
        except Exception as exc:
            failed_symbols.append(symbol)
            logger.error(f"[{symbol}] Model publish failed: {exc}")

    return {
        "published_symbols": published_symbols,
        "failed_symbols": failed_symbols,
    }
