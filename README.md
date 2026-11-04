# SOL 信号与风控系统

本源码副本基于 bb2ea1b，保留行情扫描、策略、纸面交易、账户风控、执行、对账与测试模块。默认入口查询公开行情，无需 API 密钥。

## Windows 快速开始

安装 Python 3.12，在本目录打开 PowerShell 后运行

    python -X utf8 -B -m src.main --once --no-toast --no-dashboard

运行离线测试

    python -X utf8 -B -m unittest discover -s tests

## 支持范围

当前策略、账户风控和执行按 OKX 的 SOL-USDT-SWAP 构建。BTC 与 ETH 尚未适配。两者需要各自的信号参数、合约规格及模拟验收。

- **公开行情扫描**使用上面的命令。纸面仓位按配置中的参考净值估算。默认的 2% 是计划风险预算，实际成交损失可能更大。
- **本地离线演练**使用 `demo_semi_auto.cmd`。它会展示固定的模拟卡片和本地确认流程，零交易所订单。单独的 `scripts/demo_semi_auto.py` 提供 OKX 模拟盘只读预检，其模拟盘下单入口仍关闭。
- **OKX 模拟自动执行**的源码供开发和审阅。S27 短线技术测试走通了模拟开仓、保护单核验和计划平仓，最终权益对账未通过。波段和常驻自动交易仍待验收。本仓库不含已验收的部署包装器、运行状态或账户凭据。后继版本的准备与激活流程依赖 Windows。
- **旧账户工具**中的 `scripts/prepare_account.py`、`scripts/emergency_flat.py` 等包含真实账户写操作入口。分享版的使用范围是公开行情、离线演练和代码审阅。请将账户凭据独立保存在本地。

详见 [策略说明](STRATEGY.md)。
