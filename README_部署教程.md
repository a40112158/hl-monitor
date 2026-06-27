# Hyperliquid Wallet Monitor FINAL 部署教程

这是 GitHub Actions + Telegram 推送版。

## 你需要准备

1. GitHub 账号
2. Telegram 机器人 Token
3. Telegram 个人或群 Chat ID
4. 两个钱包地址文件：
   - `money_printer_all_addresses.txt`
   - `smart_money_all_addresses.txt`

## 文件说明

```text
hl_monitor_final.py                 主脚本
requirements.txt                    Python依赖
coin_thresholds.json                币种专属阈值
money_printer_all_addresses.txt     Money Printer地址，一行一个
smart_money_all_addresses.txt       Smart Money地址，一行一个
.github/workflows/monitor.yml       GitHub Actions定时任务
reports/                            运行后自动生成报告
```

## 第一步：替换钱包地址

打开：

```text
money_printer_all_addresses.txt
smart_money_all_addresses.txt
```

把示例注释删掉，改成你自己的地址，一行一个：

```text
0x1234567890abcdef1234567890abcdef12345678
0xabcdefabcdefabcdefabcdefabcdefabcdefabcd
```

## 第二步：创建 Telegram Bot

在 Telegram 搜索：

```text
@BotFather
```

发送：

```text
/newbot
```

按提示创建机器人，拿到 `TG_BOT_TOKEN`。

## 第三步：获取 TG_CHAT_ID

### 推送到个人

先给你的机器人发一句话，比如：

```text
hi
```

然后浏览器打开：

```text
https://api.telegram.org/bot你的TOKEN/getUpdates
```

找到：

```json
"chat":{"id":123456789}
```

这个数字就是 `TG_CHAT_ID`。

### 推送到群

把机器人拉进群，群里发一句：

```text
/test
```

然后浏览器打开：

```text
https://api.telegram.org/bot你的TOKEN/getUpdates
```

找到类似：

```json
"chat":{"id":-1001234567890}
```

这个负数就是群 ID。群 ID 通常是负数，很多是 `-100` 开头。

## 第四步：创建 GitHub 仓库

建议仓库名：

```text
hl-monitor
```

建议设置为：

```text
Private
```

因为里面会保存钱包列表、数据库和报告。

## 第五步：上传文件

用 Git 命令：

```bash
git init
git add .
git commit -m "init hl monitor"
git branch -M main
git remote add origin https://github.com/你的用户名/hl-monitor.git
git push -u origin main
```

也可以直接在 GitHub 网页上传整个文件夹里的文件。

## 第六步：设置 GitHub Secrets

进入仓库：

```text
Settings → Secrets and variables → Actions → New repository secret
```

添加两个 Secret：

```text
TG_BOT_TOKEN = 你的机器人Token
TG_CHAT_ID = 你的个人ID或群ID
```

不要把 Token 写进代码。

## 第七步：打开 Actions 写权限

进入仓库：

```text
Settings → Actions → General → Workflow permissions
```

选择：

```text
Read and write permissions
```

保存。

这个权限用于让 GitHub Actions 把 `hl_monitor.db` 和 `reports/` 自动 commit 回仓库。

## 第八步：手动运行两次

进入仓库：

```text
Actions → Hyperliquid Wallet Monitor FINAL → Run workflow
```

第一次运行：建立快照。

第二次运行：开始出现趋势对比。

## 运行后会生成什么

```text
hl_monitor.db
reports/final_latest_report.txt
reports/watchlist_long.txt
reports/watchlist_short.txt
reports/watchlist_observe.txt
reports/wallet_states_latest.csv
reports/perp_positions_latest.csv
reports/spot_balances_latest.csv
reports/coin_signals_latest.csv
```

你主要看：

```text
reports/final_latest_report.txt
reports/watchlist_long.txt
reports/watchlist_short.txt
reports/watchlist_observe.txt
```

## 默认推送规则

默认配置：

```text
PUSH_EVERY_RUN=0
```

意思是：

```text
不是每小时都推送。
只有强信号或每日复盘时间才推送。
```

每日复盘默认：

```text
DAILY_PUSH_HOUR_UTC=0
```

也就是：

```text
北京时间 08:00 附近
日本时间 09:00 附近
```

如果你想每次运行都推送，把 `.github/workflows/monitor.yml` 里的：

```yaml
PUSH_EVERY_RUN: "0"
```

改成：

```yaml
PUSH_EVERY_RUN: "1"
```

## 调整推送多寡

打开：

```text
coin_thresholds.json
```

如果推送太少，降低：

```json
"score_push"
```

如果推送太多，提高：

```json
"score_push"
```

例子：

```json
"HYPE": {
  "score_push": 7,
  "perp": 2000000,
  "spot": 800000,
  "min_watch_score": 5
}
```

## 本地测试

Windows CMD：

```cmd
set TG_BOT_TOKEN=你的token
set TG_CHAT_ID=你的chat_id
python hl_monitor_final.py --rpm 200 --concurrency 5 --note "local test"
```

PowerShell：

```powershell
$env:TG_BOT_TOKEN="你的token"
$env:TG_CHAT_ID="你的chat_id"
python hl_monitor_final.py --rpm 200 --concurrency 5 --note "local test"
```

测试 Telegram：

```bash
python test_tg.py
```

## 常见问题

### 1. 第一次没有趋势

正常。第一次只是建立数据库快照，第二次开始才有对比。

### 2. 没有 TG 推送

检查：

```text
TG_BOT_TOKEN 是否正确
TG_CHAT_ID 是否正确
群 ID 是否带负号
机器人是否在群里有发言权限
是否默认没有强信号
```

想强制每次都推送，就把：

```text
PUSH_EVERY_RUN=1
```

### 3. 找不到地址文件

确认这两个文件在仓库根目录：

```text
money_printer_all_addresses.txt
smart_money_all_addresses.txt
```

### 4. 数据库没有保存回来

检查：

```text
Settings → Actions → General → Workflow permissions → Read and write permissions
```

### 5. 运行太慢或超时

把 workflow 里的参数调低：

```yaml
HL_RPM: "120"
HL_CONCURRENCY: "3"
```

### 6. 想跑得快一点

可以试：

```yaml
HL_RPM: "300"
HL_CONCURRENCY: "10"
```

如果失败率升高，就调回 200 / 5。

## 重要说明

这个脚本不是自动下单系统，只是监控和提醒。

`资金流 Lite` 不是 Binance / OKX / Bybit 充值提现路径，只是根据钱包内 USDC 和现货余额变化推断。

如果你以后要真正做交易所充值路径，需要接链上标签数据源。
