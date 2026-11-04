# SOL 信号与风控系统

本源码副本基于 bb2ea1b，保留行情扫描、策略、纸面交易、账户风控、执行、对账与测试模块。默认入口查询公开行情，无需 API 密钥。

## Windows 快速开始

安装 Python 3.12，在本目录打开 PowerShell 后运行

    python -X utf8 -B -m src.main --once --no-toast --no-dashboard

运行离线测试

    python -X utf8 -B -m unittest discover -s tests

## 支持范围

当前策略、账户风控和执行按 OKX 的 SOL-USDT-SWAP 构建。BTC 与 ETH 尚未适配。两者需要各自的信号参数、合约规格及模拟验收。

模拟执行源码供开发和审阅。短线模拟测试已验证开仓、保护和计划平仓，最终权益对账未通过。波段与常驻自动交易仍待验收。账户凭据由使用者独立配置并存放在本地。

详见 [策略说明](STRATEGY.md)。
