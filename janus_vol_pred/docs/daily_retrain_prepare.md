# 每日定时重训前的数据补齐与特征准备

## 目标

在每天 `UTC 00:00` 触发训练前，先自动补齐训练依赖的 `book_ticker` 原始数据和 `features_label` 特征文件；如果依赖最终仍不完整，则当天训练失败退出。

## 当前实现

- 训练前准备器位于 `janus_vol_pred/training_preparer.py`
- 原始 `book_ticker` 多来源解析与 fallback 下载位于 `janus_vol_pred/book_ticker_source.py`
- `features_label.py` 已支持：
  - 优先读取同事维护的只读 Tardis 数据
  - 若缺失则读取自己历史补齐的数据

## 数据来源顺序

对于任意 `symbol/date` 的 `book_ticker`：

1. 先查只读 Tardis 根目录
2. 再查自己历史补齐的 fallback 目录
3. 若都不存在，且 `enable_hangqing_fallback=true`，则从公司 ClickHouse 行情库下载 `perpBestPrice`
4. 下载后转换为 Tardis 风格 `book_ticker` 文件并保存到 fallback 目录

## 保存路径

自己补齐的原始数据保存到：

```text
${output_root}/raw_book_ticker/{symbol}/book_ticker/{YYYY-MM-DD}/binance-futures_book_ticker_{YYYY-MM-DD}_{SYMBOL}.csv.zst
```

## use_prev_day

是否需要前一天原始数据，完全由配置中的 `execution.use_prev_day` 决定：

- `true`：生成 `D` 日特征时，需要 `D-1` 和 `D` 的原始 `book_ticker`
- `false`：只需要 `D` 日原始 `book_ticker`

## 配置项

新增配置：

```yaml
data_sources:
  tardis_book_ticker_root: "/data/market_data/binance/future"
  fallback_book_ticker_root: "/data/sigma/zzy/janus/results/vol_pred_prod/raw_book_ticker"
  enable_hangqing_fallback: true

daily_retrain:
  enabled: false
  trigger_utc_hour: 0
  trigger_utc_minute: 0
  fail_if_feature_dependency_missing: true

hangqing:
  clickhouse_host: "13.231.173.109"
  clickhouse_port: 8123
  clickhouse_database: "marketData"
  clickhouse_user_env: "HANGQING_CH_USER"
  clickhouse_password_env: "HANGQING_CH_PASSWORD"
```

## 环境变量

运行 fallback 下载前，需要先设置：

```bash
export HANGQING_CH_USER="your_user"
export HANGQING_CH_PASSWORD="your_password"
```

## 集成方式

`train_production.py` 启动时，如果 `daily_retrain.enabled=true`，会先调用训练前准备器：

1. 根据 `train_days + val_days` 计算需要准备的历史特征日期
2. 检查这些日期的特征文件是否已验证
3. 缺失时补齐原始数据并生成特征
4. 若任意关键依赖失败，则直接终止训练

## 训练入口划分

- `train.py`
  - research / 回测入口
  - 保留 `train + val + test`
  - 适合历史评估、调参、研究

- `train_production.py`
  - 生产重训入口
  - 每天外部定时触发
  - 自动取 `UTC` 前一日作为最新完整日
  - 使用 `train_days` 训练、`val_days` 验证
  - 输出目录为 `train.production_results_output_dir`

## 注意事项

- 同事维护的 Tardis 目录只读，不会写入
- fallback 数据只写到自己目录
- 当前 fallback 只覆盖 `book_ticker`
- 如需启用该流程，请先确认已安装 `clickhouse-connect`
