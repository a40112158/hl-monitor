# Hyperliquid 钱包监控

这是一个只读的 Hyperliquid 钱包研究与信号监控项目。它定期读取公开钱包数据，生成资金流、短期异动、长期候选、风险、生命周期和回测报告，并可通过 Telegram 推送结果。

本项目只做监控和研究分析，不连接交易私钥，也不会自动下单。报告不构成投资建议。

## 运行方式

线上工作流是 `.github/workflows/monitor.yml`。仓库自身不包含定时计划，而是由外部 Cloudflare Cron/Worker 调用 GitHub Actions 的 `workflow_dispatch` 接口：

```text
POST /repos/{owner}/{repo}/actions/workflows/monitor.yml/dispatches
{"ref":"main"}
```

Cloudflare 端需要保存一个有权触发该工作流的 GitHub Token。触发频率、重试和告警也应在 Cloudflare 端配置；GitHub 工作流使用仓库中的固定配置和 GitHub Secrets。

Cloudflare 不传 `inputs` 时，`send_start_test` 默认为 `false`，因此每次定时触发不会额外发送“启动测试”Telegram 消息。需要人工验证 Telegram 时，可在 GitHub Actions 手动运行并勾选该输入。

## 本地安装与运行

建议使用 Python 3.11：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -r requirements-ai.txt
python hl_monitor_final.py --rpm 200 --concurrency 5 --note "local"
```

Windows 也可以在当前终端设置好环境变量后运行 `local_run_windows.bat`。Gemini 不是核心扫描的必需条件；不需要 AI 分析时可以不安装 `requirements-ai.txt`。

如需延续已有历史数据，请先把 `hl_monitor.db.gz` 解压为 `hl_monitor.db`；没有数据库时程序会从新快照开始。

## GitHub Secrets

按需在仓库的 Actions Secrets 中配置：

- `TG_BOT_TOKEN`、`TG_CHAT_ID`：Telegram 推送。
- `GEMINI_API_KEY`：Google AI Studio / Gemini 分析。
- `GCP_SA_KEY_JSON`、`GOOGLE_CLOUD_PROJECT`、`GOOGLE_CLOUD_LOCATION`：可选的 Vertex AI 备用通道。

Cloudflare 用于触发 GitHub Actions 的 Token 应保存在 Cloudflare Secret 中，不要写入仓库。

## Signal Model V2

默认使用 `SIGNAL_MODEL_VERSION=2`。V2 把短期事件与正式长期事件分开统计：

- 短期事件：`alert_score` 达到 `alert push` 阈值，且不是正式长期候选。
- 长期事件：同时满足 `candidate_gate=PASS`、`candidate_state=CANDIDATE`，并且计划动作明确为“可进入低杠杆长期观察”。

V1 历史记录继续保留在数据库中，但 V2 报告和回测只统计 V2 事件，不与 V1 混算。

回测净收益按事件计算：每个事件都从原始收益中扣除 `0.12` 个百分点的往返交易成本。该值可通过 `BACKTEST_ROUNDTRIP_COST_PCT` 调整。

### 收益面板 V3

V3 不改变 V2 信号定义，只升级统计和展示：

- 短期信号只展示 `1h / 4h / 24h`。
- 正式长期候选只展示 `72h / 7d / 15d / 30d`。
- 多头、空头分别统计，并明确区分成熟、待评估和超时缺值样本。
- 生命周期记录 `active → grace → closed`，同时记录扫描采样口径的当前收益、MFE 和 MAE。
- 原数据库中的 V1 短期事件继续作为“历史参考”显示，但不会进入 V2 指标或自动阈值优化。
- 原 V1 长期事件如果与短期事件重叠，会明确提示，不能当作独立长期样本。

主要文件为 `reports/signal_performance_dashboard_v3.txt` 和 `reports/details/signal_performance_dashboard_v3_latest.csv`。

V2 相关环境变量：

| 变量 | 默认值 | 作用 |
| --- | ---: | --- |
| `SIGNAL_MODEL_VERSION` | `2` | 当前信号与报告模型版本 |
| `LONGTERM_EVENT_COOLDOWN_HOURS` | `24` | 同币种、同方向正式长期事件的冷却时间 |
| `SIGNAL_EVENT_KEEP_DAYS` | `90` | 信号事件历史保留天数 |
| `LEGACY_BASELINE_MODE` | `1` | 单独展示 V1 历史基线，不混入 V2 |
| `STRONG_SIGNAL_MAX_HOLD_HOURS` | `24` | 短期信号生命周期最大跟踪时长 |
| `LONGTERM_SIGNAL_MAX_HOLD_HOURS` | `720` | 长期候选生命周期最大跟踪时长 |

## 阈值配置

阈值保存在 `coin_thresholds.json`，可在 `DEFAULT` 或单个币种下使用以下 V2 键：

- `alert_score_push`：短期强事件阈值。
- `alert_min_watch_score`：短期观察阈值。
- `long_score_push`：长期 `CANDIDATE` 阈值。
- `long_min_watch_score`：长期形成/观察阈值。

旧配置仍兼容：

- `score_push` 同时作为缺省的 `alert_score_push` 和 `long_score_push`。
- `min_watch_score` 同时作为缺省的 `alert_min_watch_score` 和 `long_min_watch_score`。

如果同时提供 V2 新键和旧键，优先使用对应的 V2 新键。建议新配置显式拆分短期与长期阈值，避免调整一侧时意外影响另一侧。

运行结果主要位于 `reports/`，详细 CSV 和状态文件位于 `reports/details/`。
