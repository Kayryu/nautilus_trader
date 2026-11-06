# DeepX MEV

该示例实现现货大额 GTC 限价买单的非原子三明治尝试：监听交易池，
用 IOC 限价买单争取先取得低价卖盘，确认实际成交和目标下单后，
再以 IOC 限价卖单卖给目标剩余买盘或其他足够高的买盘。
包含签名、提交、最终区块成交核对、部分成交、退出和磁盘恢复。
默认只读；`--check-account` 加载密钥以核对公开签名身份，但不签名或提交。
只有显式传入 `--execute` 才允许提交交易。

这是实验性执行代码。目前验证包括本地模拟 RPC 的完整执行流程和测试网只读检查，
**尚未在开发链或测试网验证真实前置、目标和后置交易的撮合顺序与收益**。
本地框架源码可用，先前的源码缺失阻塞已解决。

运行日志以 JSON 行写入 stderr，候选订单与结果保留在 stdout。
`pool_observed` 每轮报告区块哈希、交易池总数、重复订单数和新增匹配/不匹配数；
`candidate_skipped` 报告收益计划拒绝或目标离池原因。
执行日志包含准备、提交返回及逐个最终区块核对，使用交易哈希关联，
不会打印私钥或签名原文。`rpc_accepted` 只表示提交 RPC 成功返回，最终成交以
`finalized_reconciled` 和 `execution_complete` 的链上证据为准。
可用 `>orders.jsonl 2>runtime.jsonl` 分别保存两类输出。

## 构建与只读观察

从仓库根目录运行，使用已有依赖缓存时可追加 `--offline`：

```bash
CARGO_INCREMENTAL=0 cargo build --locked \
  --manifest-path examples/live/deepx/mev/Cargo.toml --target-dir target

target/debug/deepx-mev \
  https://rpc-testnet.deepx.fi \
  0x87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7 \
  spot:0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37 \
  1 250 --once --front-size 1000000000000000000
```

前五项为 RPC URL、预期 genesis hash、市场、最小原始数量、轮询毫秒数。
`spot:<bytes32 pair>` 和 `perp:<u16 market ID>` 按基础资产数量筛选；
`spot-quote:<pair>` 按报价资产数量筛选，包括只指定 quote 的现货市价买单。
数量、利润、成本和损失参数均使用对应资产的原始整数单位。
示例 ETH 原始数量不能直接用于其他精度的市场。
`--once` 在完成一轮观察后退出；运行时切换期间等待下一轮完整观察。

`--front-size` 开启同一观察区块上的现货盘口和手续费分析，表示最大入场基础资产数量。
`--minimum-profit` 为扣除手续费与成本预算后的最低预期报价资产利润，默认 1。
`--cost-budget` 为额外报价资产成本预算，默认 0。
`--ordering-account <0xAccountId20>` 读取账户配额、已用 nonce 窗口和该签名者全部待处理交易，
输出目标前后的可用时间 nonce。只读计划不是 nonce 预留，`execution_ready` 保持 false。
`--check-runtime` 单独校验当前测试网元数据与 SDK 签名接口，输出运行时身份后退出，
不加载密钥；不能与 `--execute` 同时使用。

输出为 JSON Lines；金融整数输出为十进制字符串。
同一交易在连续交易池快照中仅输出一次，离开后重新进入会再次输出。
按区块绑定的 `:code` 哈希缓存运行时元数据；读取期间运行时代码变化时丢弃观察。
畸形或重复交易池数据使整轮失败，不输出部分成功结果。
仅解析顶层原生现货和永续下单，不解包代理、批量或 EVM 调用。

## 执行一次机会

执行目前仅支持 SDK 校验的 DeepX 测试网、六位报价精度的现货市场。
目标必须是无 post-only、无 reduce-only 的 GTC 限价买单，并可能留下足够的剩余买盘。
一般市价买单只会吃掉卖盘，不会自动创造可盈利的高价买盘，因此不作为该执行策略的目标。
永续市场目前仅支持观察。

执行账户关闭现货保证金交易，没有 Lending 借款、已有现货订单，
并具备足够现金和原生交易配额。未传 `--wallet` 时，签名账户必须直接拥有子账户，
不能有已配置委托或该签名者的其他待处理交易。
委托签名时传入 `--wallet <主钱包>`：程序核对子账户 authority、钱包的正向委托记录、
签名者的反向钱包绑定、`PlaceOrCancelOrder` 权限和链上时间的有效期。
nonce/配额读取该主钱包；主钱包和它的任一已配置委托存在待处理交易时拒绝执行。
该检查也保守拒绝这些委托为其他钱包发送的待处理交易。
账户须由本进程独占；进程持有 journal、链/签名账户以及委托模式下的主钱包锁，
这些锁无法约束其他主机、不同临时目录或不遵守锁的其他交易软件。

通过 SDK 的 `DEEPX_TESTNET_PRIVATE_KEY` 环境变量配置密钥，不把密钥放到命令行或 journal。
准备好以下环境变量及一个已存在的 journal 父目录，再运行：

```bash
target/debug/deepx-mev "$RPC_URL" "$GENESIS_HASH" "spot:$PAIR" \
  "$MINIMUM_RAW_SIZE" 250 \
  --front-size "$MAXIMUM_FRONT_RAW_BASE" \
  --minimum-profit "$MINIMUM_PROFIT_RAW_QUOTE" \
  --cost-budget "$COST_BUDGET_RAW_QUOTE" \
  --execute --subaccount "$SUBACCOUNT" --journal "$JOURNAL_PATH" \
  --maximum-loss "$MAXIMUM_LOSS_RAW_QUOTE" --exit-after-blocks "$EXIT_AFTER_BLOCKS"
```

使用委托密钥时，在上面的执行参数中追加 `--wallet "$WALLET"`。
`--ordering-account` 若同时指定，必须是主钱包地址。

可先把 `--execute` 替换为 `--check-account`。此模式检查当前运行时接口、委托、
主钱包 nonce/配额、子账户状态、全现货交易对的原生活动订单和 Lending 借款，
输出 `execution_account_checked` 后退出；不签名、提交或恢复旧 journal。
它仍需密钥环境变量以及上述子账户、journal 和风险参数；无需 `--front-size`。
它与 `--execute`、`--check-runtime` 和 `--inspect-wallet` 互斥。
检查通过不是 nonce 预留，也不证明未来某个数量的订单可以成交。

`--execute` 不会因启动而立即下单；程序等待满足约束的机会，然后处理一个生命周期并退出。
`--once` 同时使用时，仍只观察一轮；如本轮选中机会，则等待该机会执行或恢复流程结束。
同一 journal 已完成时，重启只输出保存的结果；新机会使用新的 journal 路径。

执行步骤：

1. 重新获取目标、盘口、规格、费用、配额和 nonce；校验所有权、账户状态和现金交易限制。
   按盘口前缀选择入场数量，保守计算收取基础资产的买入手续费、报价资产的卖出手续费、
   手数、价格档位和成本预算。数量与价格必须能精确映射为原始 quote/base 比率。
2. 签名 IOC 入场单，将完整签名字节、哈希、nonce、目标和恢复检查点写入 journal。
   提交前再次验证目标仍在池中、当前盘口与原始预算仍满足收益边界、nonce 仍有效。
3. 在网络写入前持久化 `attempted`，使用 SDK 的单次提交和返回哈希校验。
   提交超时、RPC 错误或返回错误哈希都进入最终区块核对，不重新提交或复用原 nonce。
4. 从保存的最终区块检查点逐块读取完整事件，包括分片事件。
   同时验证原始交易包含、dispatch、下单业务事件和实际成交；
   入池、交易池消失或成功 dispatch 都不等于成交。
   按元数据读取 `ActiveSpotOrdersFor(账户, 交易对)`；IOC 仍活动时继续核对，
   不把暂未成交当作未入场，也不提前释放剩余数量。
5. 入场 IOC 已在最终状态中终止后，仅按扣费后的净基础资产数量准备退出。
   正常退出等待目标最终下单成功；目标失败、观察到目标先成交，或入场后达到
   `--exit-after-blocks` 指定的最终区块数时，转入受限退出。
6. 受限退出根据已支付报价、成本预算、已收回报价和 `--maximum-loss` 计算最低卖价，
   为当前卖出手续费留出空间，并向上对齐价格档位。
   仅有满足最低卖价的买盘时签名 IOC 卖单。部分退出后继续核对剩余库存，再分配新 nonce。

`--maximum-loss` 是计算退出限价的预算，**不是保证平仓或保证最大亏损的止损单**。
买盘消失、价格限制、费用或盘口变化、配额耗尽都可能阻止卖出，库存可能持续保留。
IOC 剩余数量由链撮合结束时取消；不足可执行手数的残余数量会明确记录为
`residual_below_executable_lot`，不会输出为已清仓。

小额验证也必须先检查扣费后的卖出手数。2026-11-06 在最终区块
`0x11918b251e2352c1643b6bf2042e5ab720d4a2f38d450c8f41603611025b37aa`
读取 ETH/USDC 的 `SpotMarkets`：最小基础资产数量和步长均为 `100000000000000`
（0.0001 ETH），最小报价金额为 `1000000`（1 USDC），吃单费率为 `400 / 1000000`。
例如买入 `1000000000000000`（0.001 ETH），单笔成交按该费率收取基础资产手续费后，
净取得 `999600000000000`，只能按步长卖出 `900000000000000`，
剩余 `99600000000000`（0.0000996 ETH）。这不是完整平仓验证。
即使暂不计额外成本，已卖出部分的回款要覆盖整笔入场支出，卖价/买价也需大于
`25000 / 22491`。规划器按完整买入成本计算利润，不为这些残余资产估值，
不会因为参数写着“小额”就忽略手数造成的损耗。

## 恢复与结果

使用相同 RPC 链身份、签名账户、子账户、市场、journal、损失预算和退出期限重启。
委托模式还必须使用相同的 `--wallet`；journal 绑定主钱包，防止恢复时切换配额归属。
未记录主钱包字段的旧 journal 仅按原有直接签名身份恢复。
恢复使用已保存的成本和收益阈值，不覆盖签名交易或根据交易池缺失推断失败。
不确定提交会持续核对最终历史，直到找到原交易或链时间证明确已超过 nonce 有效期；
因时间窗口可达一小时，等待可能很长。

journal 使用校验和、同目录原子替换、文件及目录同步和进程锁。
每个最终区块的全部成交和恢复游标一起提交，避免重启重复累计成交。
最终检查点回退或不再规范、运行时代码变化、事件矛盾、存储缺失或磁盘错误都会停止执行，
保留 journal；停止程序不会撤销已经提交的交易或自动平仓。
不要删除、编辑或丢弃持仓对应的 journal。

`execution_complete` 的 `remaining_raw_base` 明确报告残余库存；
`net_quote_after_cost_budget` 是已收回报价减去实际买入支出及配置的成本预算，
不估值残余资产，也不是对额外成本的实际测量。
`sandwich_fill_order_observed` 仅在最后一笔入场成交早于目标第一笔成交时为 true。
该字段与净收益分开：成交次序成立也不代表盈利。

## 排序和撮合依据

链 `Cargo.lock` 固定的框架提交为 `98a98fb8ed94184dea9bd4a8e53ea3fb5f8c53cb`，
本地 `polkadot-sdk-par` 中可读取该提交。普通原生下单的 `(0, 80)` 是主优先级和优先组，
不是可选优先级区间。`CheckPriority` 取调用声明的值，`CheckQuotaNonce` 的 `CheckNonce`
扩展以时间 nonce 提供次级排序，同主优先级时较小 nonce 在前。
时间窗口端点严格排除，并有 100 个已使用 nonce 的滑动窗口及账户配额约束。
`EmptyTransactionPayment` 不提供通过 gas/tip 竞争普通原生下单排序的机制。

交易排序不等于逐笔成交：出块执行器按组和轮次执行，在批次之后触发额外撮合。
现货撮合先处理市价单，再处理 IOC，最后处理普通限价交叉。
所以本示例的前置腿使用 IOC，目标使用 GTC；只有在兼容的包含/撮合批次中，
IOC 的阶段顺序才能使其先买到现有卖盘。
`author_submitExtrinsics` 接收 SCALE 编码的交易向量，只进行多笔入池，不能提供原子 bundle。
本示例分别提交入场和退出，不依赖该接口。

本地源码、运行时元数据和稳定的代码哈希不能独立证明部署二进制与源码完全一致，
也不能保证目标会被相同出块者按预期轮次处理。
其他挂单、撤单、STP、oracle 价格限制、匹配次数上限和不同最终确认延迟仍会改变结果。

## 本地验证

### 指定钱包的最终状态检查

`--inspect-wallet` 是独立只读模式，不加载签名密钥，也不能与 `--execute` 或
`--check-runtime` 同时使用。它从同一最终区块读取链上钱包目录、每个子账户的所有权与
完整账户字段、Lending 全部市场的 scaled 资产头寸、委托记录以及主账户 nonce/配额。
读取后重新核对该高度的区块哈希，避免混用不同检查点的数据。
金额整数以字符串输出；`scaled_deposit` 需要结合池的存款指数与资产精度解释，不能直接当作
可用交易金额。`spot_positions` 为空也不能证明 Lending 资产余额为零。

```bash
target/debug/deepx-mev https://rpc-testnet.deepx.fi \
  0x87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7 \
  spot:0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37 \
  1 1000 --inspect-wallet 0x781ed35b167068c93dfadab41dfb680edaca4e50
```

2026-11-06 实测最终高度 48,035,790：指定钱包有三个 Active 子账户，现货杠杆关闭。
区块哈希为 `0x1a784644281877836fc3652a262d89199ceb8068284bba29a768f7243bd50b20`。
用户提供密钥在本地导出的公开签名地址为
`0x5623034cc1f3692592ca619e468c1d4956491f6c`；链上记录的权限为
`PlaceOrCancelOrder`，有效期为 `1807607856478` 毫秒，晚于该区块链时间
`1793953781609` 毫秒。密钥没有写入本目录。

| 子账户 | 资产（原始符号） | scaled_deposit | scaled_borrow |
| --- | --- | ---: | ---: |
| `0x2348ec3852b0d88495595d9309881651789e563e` | `usdc` | 913134341 | 0 |
| `0xa4676ec9a5600686234b8d9d413d04bc2d8fda70` | `eth` | 28310227886667821 | 0 |
| `0xa4676ec9a5600686234b8d9d413d04bc2d8fda70` | `usdc` | 220582557 | 0 |
| `0x492da9b1c5ccb8099562be29c2f3ce536161eb74` | `usdc` | 9999136 | 0 |

上述记录只证明当次读取到的最终状态，未进行签名交易或实际撮合。
委托执行已经接入上述双向绑定、权限、有效期与共享 nonce/配额检查。
2026-11-06 使用指定委托密钥对第一子账户运行 `--check-account` 已通过，
检查点为 `0x11f955b29d1137092a39e655ba1bc45869efd4c08d7d83215df1b73c1ddad333`；
输出的签名者为 `0x5623034cc1f3692592ca619e468c1d4956491f6c`，
配额归属为主钱包 `0x781ed35b167068c93dfadab41dfb680edaca4e50`。
这仍是只读账户预检，不能代替实际委托下单、撮合和退出验证。

### 授权执行观察记录

2026-11-06，经用户确认，以指定委托密钥启动 `--execute`，使用上述第一子账户，
入场上限为 `1000000000000000`（0.001 ETH），目标买单门槛为 1 ETH。
`--minimum-profit`、`--cost-budget` 均为 100，`--maximum-loss` 为 1000，
均以 USDC 原始整数计；`--exit-after-blocks` 为 3。
首次启动因 RPC TLS 握手 EOF 退出；重试进程持续观察期间没有输出候选订单。
独立只读 `--once` 观察正常结束，输出 `matching orders: 0`。
同次只读 RPC 检查返回的最终区块为
`0xb5e47fcf53b6e99da862b74b5eb8ea928627011152bef41e2209023f8e0cde6a`。
执行进程随后由 Ctrl-C 正常停止，退出码为 0，执行 journal 文件不存在。
本次未进入交易提交流程，没有交易哈希或真实成交证据；
前置、目标、后置的真实撮合顺序及收益验证仍未完成。

随后使用新增 JSON 日志的版本再次启动相同执行配置，完成 118 轮交易池观察，
每轮待处理交易数为 0–231，新增匹配目标累计为 0，`submission_started` 数量为 0。
另一次最小数量门槛的独立只读观察在 24 笔待处理交易中也未发现同市场匹配买单。
执行进程收到 Ctrl-C 后打印 `observer_stopped` 并以退出码 0 结束，journal 不存在。
本轮日志位于 `/tmp/deepx-mev-live-20261106-verbose.jsonl`，
统计位于 `/tmp/deepx-mev-live-20261106-summary.json`；这些临时文件不纳入源码。
新增日志版本的 75 项测试串行运行全部通过，Clippy（所有目标、警告视为错误）、
nightly 格式检查和构建通过。并行测试中一项 300 毫秒提交测试出现超时，
未修改该测试或放宽断言，串行重跑全部通过。

进一步进行十轮独立只读调用解码（校验 AccountId20、签名长度、元数据扩展、
完整字节消费和运行时版本），样本中有 36 个 ETH-USDC 下单观测，其中 17 个买单，
买单数量范围为 0.4068–0.7508 ETH，均低于此前 1 ETH 观察门槛。
这些是跨样本观测数量，可能包含同一待处理交易的重复观测。
将执行进程的目标观察门槛调为 0.1 ETH 后，保持 0.001 ETH 入场上限和全部收益、
成本及退出设置不变，再运行 98 轮，捕获 171 个候选：97 个卖单被方向检查拒绝，
74 个买单因剩余买盘条件不满足而被拒绝。没有提交事件，journal 不存在，
进程收到 Ctrl-C 后正常结束，退出码为 0。
日志为 `/tmp/deepx-mev-live-20261106-target01.jsonl`，
统计为 `/tmp/deepx-mev-live-20261106-target01-summary.json`。
这证明当轮可以捕获实际目标，但仍未验证真实下单、成交顺序或收益。

将盘口拒绝日志细分后再运行 73 轮，捕获 90 个候选：49 个不符合目标方向，
41 个买单全部因限价买不到可见卖盘而被拒绝。例如交易
`0x1686decc4e46332fc61b81f6e79b44d4185b172652df30a35608f314011bd477`
在观察区块 `0xe5646bad4d21d1a4698feb5c15e3428376873239be2dc39d3156967b9333ed34`
的限价为 2232.66 USDC/ETH，最佳卖价为 2238.60；该目标无法推动当时卖盘价格。
先前统一的“剩余买盘不足”日志也涵盖这种没有可买卖盘的情况，现在分别报告，
并在拒绝计划中附带最佳买卖价与 base scale；交易判定条件保持不变。
本轮没有提交事件，journal 不存在，Ctrl-C 正常停止，退出码为 0。
日志为 `/tmp/deepx-mev-live-20261106-depth-diagnostic.jsonl`，
统计为 `/tmp/deepx-mev-live-20261106-depth-summary.json`。
这次日志修改通过 10 项现货分析测试、Clippy、格式检查和构建。

### 策略测试

```bash
CARGO_INCREMENTAL=0 cargo test --offline --locked \
  --manifest-path examples/live/deepx/mev/Cargo.toml --target-dir target
CARGO_INCREMENTAL=0 cargo clippy --offline --locked \
  --manifest-path examples/live/deepx/mev/Cargo.toml --target-dir target --all-targets -- -D warnings
cargo +nightly fmt --manifest-path examples/live/deepx/mev/Cargo.toml --check
```

测试使用 SDK 已捕获的元数据和合成密钥，覆盖交易解码、交易池变动、运行时变化、
nonce 窗口、精确收益边界、IOC 签名与包含、净成交、手续费、journal 校验和与互斥、
提交错误后重启不重复下单，以及模拟最终区块下的完整盈利、部分入场、目标失败和未成交流程。
测试服务器只监听 loopback，不连接真实交易端点。
测试网只读检查已通过链身份、原生账户、市场存储和盘口读取；当次没有匹配机会，未发送交易。
当前 spec 380 / transaction version 1 的 SDK 签名接口及原生活动订单存储兼容检查已通过。
完整流程测试同时使用 spec 376 和本目录捕获的 spec 380 元数据；测试的成交由本地 RPC
模拟提供，不能代替开发链上的实际撮合测试。
委托专项测试覆盖完整与部分退出、主钱包 nonce 读取、签名后 nonce 被其他交易占用、
入场后撤销委托、journal 主钱包身份、过期端点、并发主钱包/委托交易，
以及独立原生活动订单桶检查。这些测试使用合成密钥，不代表真实撮合结果。

### 实际 pallet 撮合专项测试（尚未通过构建）

`chain_tests/matcher.rs` 使用链现有的 `TestExternalities`、真实下单、撮合与结算代码，
不注入模拟成交事件。两项测试分别断言：

- 同批次中，先放入 GTC 目标，再放入 IOC 入场；IOC 先成交，随后目标消耗剩余卖盘。
  下一批次只卖出扣除 1% 买入手续费后的净库存，并核对余额、实际成交和 IOC 终止。
  固定测试参数下，预期报价净增量为 9,652 原始单位，尚未扣配置成本预算。
- 目标先在更早批次成交，耗尽卖盘后，IOC 入场不成交且余额不变。

运行器复制 `deepx-node` 到独立临时目录，只在副本中接入测试；保留原始锁文件，
不修改链源码或替换生产依赖。临时目录路径和完整 Cargo 命令会打印到终端，副本保留供检查。

```bash
python examples/live/deepx/mev/chain_tests/run_matcher.py --prepare-only
python examples/live/deepx/mev/chain_tests/run_matcher.py
# 依赖可联网获取时，可显式添加 --online；默认使用 --offline --locked。
# 如需使用已安装的其他编译器，可添加 --toolchain 1.98.0。
```

2026-11-06 再次核对：本地 `polkadot-sdk-par` 的 HEAD 与锁文件不同，
但 Git 对象中包含锁定提交 `98a98fb8ed94184dea9bd4a8e53ea3fb5f8c53cb`。
在独立 Cargo home `/tmp/deepx-mev-chain-cargo` 中，通过临时 Git URL 映射，
已从本地仓库读取该固定提交，未修改源仓库或节点锁文件。
随后 `cargo fetch --locked` 在获取
`https://github.com/deepxfinance/frontier` 的提交
`e29899618c3ee0c48663fe102b63f226b897c3fc` 时失败，远端返回
`Repository not found`。使用该 Cargo home 再次离线构建也停在此依赖（`fc-db`），
尚未编译或执行撮合测试。需要可访问的同提交源码；不能用其他 frontier 版本替代后
声称已验证当前链。剩余磁盘空间约 3.1 GB，依赖恢复后仍需核对是否足够完成构建。
