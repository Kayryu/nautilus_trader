# NautilusTrader 金融英语术语表

本文档概述 NautilusTrader 项目的金融业务领域，并按主题整理源码和交易业务中常见的
英文术语、中文翻译及含义。它是一份面向源码阅读的学习参考，不是对仓库标识符的机械
穷举。

## 项目概览

NautilusTrader 是一个面向量化交易、事件驱动回测和实盘交易的高性能交易平台，主要
覆盖以下领域：

- 行情数据：报价、成交、K 线和订单簿。
- 金融工具建模：股票、期货、期权、外汇和加密资产。
- 订单管理与执行：下单、改单、撤单、成交和执行回报。
- 投资组合管理：账户余额、持仓、保证金和盈亏。
- 风险管理：订单限制、风险敞口、杠杆和资金检查。
- 回测与仿真：历史行情重放、撮合与执行模拟。
- 实盘适配器：连接交易所、经纪商和行情提供商。

项目以 Rust 实现高性能核心，并提供 Python 接口。金融领域概念主要集中在 `model`、
`data`、`execution`、`portfolio`、`risk`、`backtest` 和 `adapters` 等模块。

## 市场与交易场所

| English | 中文 | 含义 |
| --- | --- | --- |
| market | 市场 | 买卖金融资产的环境。 |
| venue | 交易场所 | 交易所、经纪商或其他执行场所。 |
| exchange | 交易所 | 集中组织交易的机构或系统。 |
| broker | 经纪商 | 代表客户提交和执行订单的机构。 |
| counterparty | 交易对手方 | 一笔交易中的另一方。 |
| liquidity | 流动性 | 资产能否快速成交且不明显影响价格。 |
| liquidity provider | 流动性提供者 | 持续提供买卖报价的参与者。 |
| market maker | 做市商 | 同时提供买价和卖价的参与者。 |
| trading session | 交易时段 | 市场允许交易的时间范围。 |
| auction | 集合竞价 | 集中订单后统一确定成交价格。 |
| matching engine | 撮合引擎 | 根据规则匹配买卖订单的系统。 |

`venue` 通常翻译为“交易场所”，含义比“交易所”更广。交易所是一种交易场所，但经纪商
或其他执行系统也可能被建模为交易场所。

## 金融工具

| English | 中文 | 含义 |
| --- | --- | --- |
| instrument | 金融工具、交易品种 | 可交易资产的统一抽象。 |
| asset | 资产 | 具有经济价值的对象。 |
| equity | 股票、权益类资产 | 代表企业所有权的证券。 |
| stock | 股票 | 企业发行的所有权凭证。 |
| bond | 债券 | 固定收益债务工具。 |
| futures | 期货 | 在未来按约定条件交割的标准化合约。 |
| option | 期权 | 在规定条件下买入或卖出标的的权利。 |
| call option | 看涨期权 | 赋予持有者买入标的的权利。 |
| put option | 看跌期权 | 赋予持有者卖出标的的权利。 |
| perpetual | 永续合约 | 通常没有到期日的衍生品合约。 |
| swap | 掉期、互换 | 交换现金流或资产风险的合约。 |
| currency | 货币 | 用于计价、结算或交易的货币。 |
| currency pair | 货币对 | 外汇市场中的两种货币组合。 |
| cryptocurrency | 加密货币 | 基于分布式账本的数字资产。 |
| underlying | 标的资产 | 衍生品价格所依赖的资产。 |
| derivative | 衍生品 | 价值来源于其他资产的金融工具。 |
| contract | 合约 | 规定交易权利与义务的协议。 |
| contract size | 合约规模 | 一张合约所代表的标的数量。 |
| multiplier | 合约乘数 | 将报价变化转换为合约价值变化的系数。 |
| lot size | 手数单位 | 允许交易的标准数量单位。 |
| tick size | 最小价格变动单位 | 报价可以变化的最小间隔。 |
| expiry | 到期时间 | 合约失效或进入结算的时间。 |
| strike price | 行权价 | 期权可以买入或卖出标的的价格。 |
| settlement | 结算 | 交易完成后的资金或资产交收。 |
| settlement currency | 结算货币 | 计算和支付结算金额的货币。 |

常见源码标识符包括：

- `instrument_id`：金融工具标识符。
- `venue`：交易场所。
- `symbol`：交易代码。
- `base_currency`：基础货币。
- `quote_currency`：计价货币。
- `settlement_currency`：结算货币。

例如，在 `BTC/USDT` 中，`BTC` 是 base currency（基础货币），`USDT` 是 quote
currency（计价货币）。

## 行情数据

| English | 中文 | 含义 |
| --- | --- | --- |
| market data | 行情数据 | 市场报价、成交和订单簿等数据。 |
| quote | 报价 | 买卖双方当前愿意交易的价格。 |
| bid | 买价、买盘 | 买方愿意支付的最高价格。 |
| ask | 卖价、卖盘 | 卖方愿意接受的最低价格。 |
| offer | 卖价 | 通常与 `ask` 同义。 |
| spread | 买卖价差 | 卖价减去买价。 |
| midpoint | 中间价 | 买价与卖价的平均值。 |
| last price | 最新成交价 | 最近一次成交的价格。 |
| trade | 成交 | 买卖订单成功匹配的结果。 |
| tick | 行情跳动、Tick 数据 | 单次报价或成交变化。 |
| bar | K 线、柱状行情 | 某时间或数量区间的聚合行情。 |
| candlestick | 蜡烛图、K 线 | 开高低收行情的图形表示。 |
| open | 开盘价 | 聚合周期内的第一笔价格。 |
| high | 最高价 | 聚合周期内的最高价格。 |
| low | 最低价 | 聚合周期内的最低价格。 |
| close | 收盘价 | 聚合周期内的最后一笔价格。 |
| volume | 成交量 | 某区间内成交的总数量。 |
| turnover | 成交额 | 成交数量乘以成交价格后的金额。 |
| order book | 订单簿 | 按价格层级排列的未成交订单。 |
| book level | 订单簿档位 | 某个价格上的买卖数量。 |
| depth | 市场深度 | 多个价格档位上的可成交数量。 |
| snapshot | 快照 | 某一时刻的完整状态。 |
| delta | 增量更新 | 相对于前一状态发生的变化。 |
| aggregation | 聚合 | 将多个数据点合并成更高层数据。 |
| subscription | 订阅 | 请求持续接收某种行情。 |

中间价通常为：

$$
P_{\text{mid}} = \frac{P_{\text{bid}} + P_{\text{ask}}}{2}
$$

买卖价差为：

$$
S = P_{\text{ask}} - P_{\text{bid}}
$$

## 订单

| English | 中文 | 含义 |
| --- | --- | --- |
| order | 订单、委托 | 买入或卖出金融工具的指令。 |
| buy | 买入 | 获取资产或建立多头。 |
| sell | 卖出 | 出售资产或建立空头。 |
| side | 买卖方向 | `Buy` 或 `Sell`。 |
| market order | 市价单 | 按当前市场可用价格立即成交。 |
| limit order | 限价单 | 只在指定价格或更优价格成交。 |
| stop order | 止损触发单 | 到达触发价后激活的订单。 |
| stop-limit order | 止损限价单 | 触发后变为限价单。 |
| trailing stop | 跟踪止损单 | 触发价随有利价格方向移动。 |
| trigger price | 触发价 | 激活条件订单的价格。 |
| limit price | 限价 | 限价订单允许成交的价格边界。 |
| quantity | 数量 | 订单需要交易的资产数量。 |
| filled quantity | 已成交数量 | 已经完成成交的订单数量。 |
| leaves quantity | 剩余数量 | 尚未成交的数量。 |
| order type | 订单类型 | 市价、限价、止损等类型。 |
| time in force | 有效期类型 | 订单在市场中保持有效的规则。 |
| good till canceled | 撤销前有效 | `GTC`，持续有效直至成交或撤销。 |
| immediate or cancel | 立即成交否则撤销 | `IOC`，立即成交可成交部分。 |
| fill or kill | 全部成交否则撤销 | `FOK`，必须立即全部成交。 |
| post-only | 只挂单 | 防止订单立即吃掉现有流动性。 |
| reduce-only | 只减仓 | 只允许减少已有持仓。 |
| cancel | 撤单 | 撤销尚未成交的订单。 |
| modify | 修改订单 | 改变价格、数量等参数。 |
| replace | 替换订单 | 用新订单参数替换旧订单。 |
| pending | 等待中 | 请求尚未最终确认。 |
| accepted | 已接受 | 交易场所已接受订单。 |
| rejected | 已拒绝 | 订单未被接受。 |
| canceled | 已撤销 | 订单不再有效。 |
| expired | 已过期 | 订单因有效期结束而失效。 |
| triggered | 已触发 | 条件订单满足了触发条件。 |

`order` 是委托指令，`trade` 或 `fill` 才是实际成交。撤单也不保证成功，因为订单可能
在撤单请求到达前已经成交。剩余数量通常表示为：

$$
Q_{\text{leaves}} = Q_{\text{order}} - Q_{\text{filled}}
$$

## 成交与执行

| English | 中文 | 含义 |
| --- | --- | --- |
| execution | 执行 | 将订单提交并在市场中完成交易的过程。 |
| fill | 成交、成交回报 | 订单全部或部分成交的记录。 |
| partial fill | 部分成交 | 订单只有一部分数量成交。 |
| full fill | 完全成交 | 订单要求的数量全部成交。 |
| execution report | 执行报告 | 交易场所返回的订单状态或成交信息。 |
| average price | 平均成交价 | 多笔成交按数量加权后的价格。 |
| slippage | 滑点 | 预期成交价与实际成交价之间的差异。 |
| latency | 延迟 | 消息发送、处理和返回所需的时间。 |
| commission | 佣金 | 经纪商收取的交易费用。 |
| fee | 手续费 | 交易产生的费用。 |
| rebate | 返佣 | 交易场所返还给参与者的费用。 |
| maker | 挂单方 | 向订单簿提供流动性的一方。 |
| taker | 吃单方 | 与已有订单立即成交的一方。 |
| routing | 路由 | 选择订单执行目的地。 |
| reconciliation | 对账、状态核对 | 将本地状态与交易场所状态进行比较。 |
| execution algorithm | 执行算法 | 按规则拆分和执行大额订单。 |
| child order | 子订单 | 从主订单拆分出的实际执行订单。 |
| parent order | 母订单 | 管理一组子订单的上层订单。 |

多次成交的平均成交价为：

$$
P_{\text{avg}} =
\frac{\sum_{i=1}^{n} P_i Q_i}
     {\sum_{i=1}^{n} Q_i}
$$

## 持仓与投资组合

| English | 中文 | 含义 |
| --- | --- | --- |
| portfolio | 投资组合 | 账户中资产、持仓和资金的整体。 |
| position | 持仓 | 某金融工具上的净交易状态。 |
| long | 多头 | 价格上涨时通常获利的持仓。 |
| short | 空头 | 价格下跌时通常获利的持仓。 |
| flat | 空仓、持仓为零 | 没有方向性净持仓。 |
| exposure | 风险敞口 | 对某种资产或风险因素的暴露程度。 |
| net position | 净持仓 | 多头数量减去空头数量。 |
| gross exposure | 总敞口 | 不抵消方向时的全部风险规模。 |
| entry price | 入场价 | 建立持仓时的价格。 |
| average open price | 平均开仓价 | 当前持仓的加权平均开仓价格。 |
| mark price | 标记价格 | 用于盈亏和强平计算的参考价格。 |
| notional value | 名义价值 | 根据数量、价格等参数计算的合约价值。 |
| realized PnL | 已实现盈亏 | 已平仓交易产生的盈亏。 |
| unrealized PnL | 未实现盈亏 | 当前持仓按参考价格计算的浮动盈亏。 |
| return | 收益率 | 收益相对于投入资本的比例。 |
| allocation | 资金配置 | 在不同资产或策略之间分配资金。 |
| holding | 持有资产 | 投资组合中当前拥有的资产。 |

简单现货多头的未实现盈亏为：

$$
\text{PnL}_{\text{unrealized}}
= \left(P_{\text{mark}} - P_{\text{entry}}\right)Q
$$

空头通常为：

$$
\text{PnL}_{\text{unrealized}}
= \left(P_{\text{entry}} - P_{\text{mark}}\right)Q
$$

实际项目中还必须考虑合约乘数、汇率、手续费和结算规则。

## 账户与资金

| English | 中文 | 含义 |
| --- | --- | --- |
| account | 账户 | 保存资金、持仓和交易状态的实体。 |
| cash account | 现金账户 | 主要使用已有现金交易的账户。 |
| margin account | 保证金账户 | 可以使用保证金或杠杆的账户。 |
| balance | 余额 | 账户中某种货币的金额。 |
| total balance | 总余额 | 包含冻结或占用资金的余额。 |
| free balance | 可用余额 | 当前可以用于交易或提取的余额。 |
| locked balance | 冻结余额 | 被订单或其他用途占用的资金。 |
| equity | 账户权益 | 账户净资产；此处不是“股票”。 |
| margin | 保证金 | 为承担杠杆头寸而提供的担保资金。 |
| initial margin | 初始保证金 | 建仓所需的保证金。 |
| maintenance margin | 维持保证金 | 保持仓位所需的最低保证金。 |
| margin call | 追加保证金通知 | 保证金不足时要求补充资金。 |
| leverage | 杠杆 | 头寸价值与自有资本的比例。 |
| collateral | 抵押品 | 为交易风险提供担保的资产。 |
| liquidation | 强制平仓 | 风险过高时由系统关闭持仓。 |
| funding rate | 资金费率 | 永续合约多空双方定期支付的费率。 |
| funding payment | 资金费用 | 按资金费率实际支付或收取的金额。 |

杠杆可以近似表示为：

$$
L = \frac{\text{Position Notional}}{\text{Account Equity}}
$$

`equity` 有两个常见含义：在金融工具语境中表示股票或权益类资产，在账户语境中表示
账户权益或净资产。阅读源码时必须根据所在模块和类型判断。

## 风险管理

| English | 中文 | 含义 |
| --- | --- | --- |
| risk | 风险 | 结果偏离预期并造成损失的可能性。 |
| risk engine | 风险引擎 | 检查订单、资金和风险限制的组件。 |
| risk limit | 风险限额 | 系统允许的最大风险边界。 |
| order limit | 订单限制 | 对订单数量、金额或频率的约束。 |
| position limit | 持仓限额 | 允许持有的最大仓位。 |
| max drawdown | 最大回撤 | 净值从峰值到后续低点的最大跌幅。 |
| volatility | 波动率 | 价格或收益变化的离散程度。 |
| variance | 方差 | 衡量收益分布波动程度的统计量。 |
| standard deviation | 标准差 | 方差的平方根。 |
| value at risk | 风险价值 | 给定置信度和期间下的潜在损失估计。 |
| stop loss | 止损 | 达到损失条件时退出持仓。 |
| take profit | 止盈 | 达到收益目标时退出持仓。 |
| hedging | 对冲 | 使用相关头寸降低风险。 |
| concentration | 集中度 | 风险是否集中在少量资产上。 |
| diversification | 分散化 | 通过配置不同资产降低集中风险。 |
| liquidation risk | 强平风险 | 保证金不足导致强制平仓的风险。 |

最大回撤可以表示为：

$$
\text{MDD}
= \max_t
\left(
\frac{\max_{s \le t} E_s - E_t}
     {\max_{s \le t} E_s}
\right)
$$

其中 $E_t$ 是时刻 $t$ 的账户权益。

## 策略与回测

| English | 中文 | 含义 |
| --- | --- | --- |
| strategy | 策略 | 根据行情和状态产生交易决策的程序。 |
| signal | 信号 | 触发交易决策的指标或事件。 |
| alpha | 超额收益来源、Alpha | 相对于基准的策略优势。 |
| indicator | 技术指标 | 从行情数据计算出的统计特征。 |
| backtest | 回测 | 使用历史数据测试策略。 |
| simulation | 仿真 | 模拟市场、账户和执行过程。 |
| historical data | 历史数据 | 已经发生的行情或交易记录。 |
| benchmark | 基准 | 用来比较策略表现的标准。 |
| lookback period | 回看周期 | 指标计算使用的历史窗口。 |
| warm-up | 预热 | 积累足够历史数据以初始化指标。 |
| overfitting | 过拟合 | 策略过度适应历史样本。 |
| survivorship bias | 幸存者偏差 | 只使用仍然存在的资产造成的偏差。 |
| look-ahead bias | 前视偏差 | 回测错误使用未来信息。 |
| transaction cost | 交易成本 | 手续费、价差和滑点等成本。 |
| performance | 绩效 | 策略收益和风险表现。 |
| Sharpe ratio | 夏普比率 | 单位波动风险对应的超额收益。 |
| win rate | 胜率 | 盈利交易占全部交易的比例。 |
| profit factor | 盈亏比因子 | 总盈利除以总亏损绝对值。 |

夏普比率通常写作：

$$
\text{Sharpe}
= \frac{E[R - R_f]}
       {\sigma(R - R_f)}
$$

阅读和验证回测代码时应特别注意：

- 不得使用尚未发生的行情，避免 look-ahead bias（前视偏差）。
- 必须模拟手续费、滑点、订单延迟和部分成交。
- 行情事件时间与本地接收时间可能不同。
- 历史回测结果不等于实盘收益。

## 时间与事件

| English | 中文 | 含义 |
| --- | --- | --- |
| event | 事件 | 驱动系统状态变化的消息。 |
| event-driven | 事件驱动 | 通过事件传递和处理组织系统。 |
| timestamp | 时间戳 | 事件发生或记录的时间。 |
| event time | 事件时间 | 事件在来源系统实际发生的时间。 |
| receive time | 接收时间 | 本地收到事件的时间。 |
| latency | 延迟 | 消息传播和处理所需的时间。 |
| clock | 时钟 | 为系统提供时间的组件。 |
| timer | 定时器 | 在指定时间触发事件。 |
| interval | 时间间隔 | 两次事件或采样之间的时间。 |
| resolution | 时间粒度 | 数据的时间精度或聚合周期。 |
| sequence number | 序列号 | 标识消息顺序的递增编号。 |
| stale | 过期的、陈旧的 | 数据已不能代表当前市场状态。 |
| replay | 重放 | 按顺序重新处理历史事件。 |

事件时间和接收时间不能混用。常见关系为：

$$
\text{latency} = T_{\text{receive}} - T_{\text{event}}
$$

对于高频交易系统，错误处理时间顺序可能导致错误订单簿、错误信号或不真实的回测
结果。

## 常见缩写

| 缩写 | 完整英文 | 中文 |
| --- | --- | --- |
| PnL | Profit and Loss | 盈亏。 |
| OHLC | Open, High, Low, Close | 开、高、低、收。 |
| OHLCV | Open, High, Low, Close, Volume | 开、高、低、收、成交量。 |
| L1 | Level 1 | 一档行情。 |
| L2 | Level 2 | 多档订单簿行情。 |
| MBO | Market By Order | 按订单展示的市场深度。 |
| MBP | Market By Price | 按价格聚合的市场深度。 |
| GTC | Good Till Canceled | 撤销前有效。 |
| GTD | Good Till Date | 指定日期前有效。 |
| IOC | Immediate Or Cancel | 立即成交否则撤销。 |
| FOK | Fill Or Kill | 全部成交否则撤销。 |
| TP | Take Profit | 止盈。 |
| SL | Stop Loss | 止损。 |
| TIF | Time In Force | 订单有效期规则。 |
| VWAP | Volume-Weighted Average Price | 成交量加权平均价。 |
| TWAP | Time-Weighted Average Price | 时间加权平均价。 |
| ROI | Return on Investment | 投资回报率。 |
| MDD | Maximum Drawdown | 最大回撤。 |
| VaR | Value at Risk | 风险价值。 |
| FX | Foreign Exchange | 外汇。 |
| CEX | Centralized Exchange | 中心化交易所。 |
| DEX | Decentralized Exchange | 去中心化交易所。 |

## 建议的学习顺序

```mermaid
flowchart LR
    A[Instrument<br/>金融工具] --> B[Market Data<br/>行情数据]
    B --> C[Order<br/>订单]
    C --> D[Execution / Fill<br/>执行与成交]
    D --> E[Position<br/>持仓]
    E --> F[Portfolio / Account<br/>投资组合与账户]
    F --> G[Risk<br/>风险管理]
    G --> H[Backtest / Strategy<br/>回测与策略]
```

阅读源码时，可以按以下关系建立整体认识：

1. `Instrument` 定义交易对象。
2. `QuoteTick`、`TradeTick` 和 `Bar` 表示行情。
3. `Order` 表示交易意图。
4. `Fill` 表示实际成交。
5. 成交改变 `Position`。
6. 持仓和余额共同构成 `Portfolio`。
7. `RiskEngine` 在订单进入执行系统前进行检查。
8. 回测系统使用历史事件模拟以上完整流程。