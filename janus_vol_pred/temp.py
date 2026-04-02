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
