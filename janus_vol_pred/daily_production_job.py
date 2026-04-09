from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

from model_publish import publish_models
from train_production import run_production_training, setup_production_logger
from utils.utils import load_yaml_config


def run_daily_production_job(config: dict) -> dict:
    training_result = run_production_training(config)
    publish_result = publish_models(
        model_date=training_result["model_date"],
        successful_symbols=training_result["successful_symbols"],
        config=config,
    )

    result = dict(training_result)
    result["published_symbols"] = publish_result["published_symbols"]
    result["publish_failed_symbols"] = publish_result["failed_symbols"]

    return result


def exit_code_from_result(result: dict) -> int:
    train_failed = bool(result["failed_symbols"])
    publish_failed = bool(result["publish_failed_symbols"])
    trained_any = bool(result["successful_symbols"])
    published_any = bool(result["published_symbols"])

    if not train_failed and not publish_failed: # 训练和发布都成功
        return 0
    if trained_any and published_any: # 部分成功，部分失败
        return 1
    return 2 # 训练全失败 或 发布全失败


if __name__ == "__main__":
    config_path = Path(__file__).parent / "config" / "config.yaml"
    config = load_yaml_config(config_path)
    setup_production_logger(config)
    result = run_daily_production_job(config)

    logger.info(
        "Daily production job finished | "
        f"date={result['model_date']} | "
        f"train_success={result['successful_symbols']} | "
        f"train_failed={result['failed_symbols']} | "
        f"published={result['published_symbols']} | "
        f"publish_failed={result['publish_failed_symbols']}"
    )
    sys.exit(exit_code_from_result(result))
