# Phase 7r：Raft 处理链路实测

> 2026-11-04 校正：Phase7s 复核发现默认 perf 时钟与 Python CLOCK_MONOTONIC
> 存在偏差，而两轮 perf 文件均未记录 CLOCKID/CLOCK_DATA。因此本文 CPU 部分
> 的“建连窗口 1685 样本”及其热点百分比不能作为准确的阶段归因，予以撤回；
> 原始数据保留供审计。`/proc` CPU 核数、连接延迟、Raft 链路及队列计时不受影响。
> 未来采样显式使用 `--clockid mono`；本次没有为了修正采样而重跑负载。

结论：本轮慢建连主要表现为控制写入排队，入口拒绝的原因也已确认。每条控制
写入平均 975.87ms，其中等待队列容量 528.75ms、入队后等待执行 416.59ms，
合计占 96.87%。串行 Raft 批次服务几乎占满整个建连阶段；每个新连接通常还
产生 5 次独立 ReadIndex，放大为约 10 次空 AppendEntries RPC。

这次仅增加默认关闭的诊断并实测，没有改变一致性、持久化、ACK、限额或超时，
也没有实施性能优化。不能把带诊断的结果当成最终容量或优化前后性能结论。

## 环境和负载

- Broker：47.84.224.186 / 172.16.240.180；三个 Controller/data 双角色进程共处
  一台 8 逻辑 CPU、约 14.96GiB RAM 的 Intel Xeon Platinum 虚机。
- 压测端：47.84.241.200 / 172.16.240.181，8 逻辑 CPU。使用私网 MQTT
  18831/18832/18833，均匀分配连接；控制节点之间为本机 loopback RPC。
- Linux 7.0.0-38-generic x86_64。Broker 每进程 4 Tokio workers，共 12 workers。
  每节点 handshake 1024，connections/sessions 50000，state 256MiB，FD 250000；
  与上轮相同。认证任务上限 128、认证限速 0；没有调整系统端口范围。
- 4 个独立 Rust probe，各 500/s、2500 个连接、2 workers、2048 in-flight，
  不重试；共 2000/s、10000 个连接。共享开始时刻和 PING 时刻，客户端 ID 不重复。
- 开始时间 Unix ms `1793719535088`，PING 时间 `1793719570088`。
  本轮没有业务消息发布订阅；连接生命周期系统事件仍走消息路径。
- Rust stable 1.98.1，本地 `cargo zigbuild --locked --release --target
  x86_64-unknown-linux-musl -p lyrnormq`；开启 release debug symbols 并保留符号，
  优化级别未变。服务器没有编译。
- Broker 源码到 `a626877`，二进制 SHA256
  `3afae822833de60e3a3443f72abae622b569625e1d25b7c56d5606191ac94e3b`。
  Probe SHA256
  `556b3d560b3c79a4c77e9f06edc93052a876f6fb8a4be86eeaf163f70784b4ad`。
- 诊断开启 `LYRNORMQ_RAFT_PROFILE=1`，一秒一次固定基数累计直方图；批次、RPC、
  存储阶段各限制前 4096 条关联记录。`perf` 软件 cpu-clock 49Hz、45 秒有界采样。

环境、限额、启动命令和哈希见 [environment.json](environment.json)、
[deployment.json](deployment.json) 和 [raw](raw/)。

## 连接结果和资源

| 指标 | 实测 |
|---|---:|
| 计划 / 实际尝试 / 未开始 | 10000 / 10000 / 0 |
| CONNACK 成功 / 最后 PING 存活 | 7505 / 7505 |
| 失败 | 2495：2479 RST、16 EOF |
| 成功连接平均 / p99 / 最大延迟 | 2060.88 / 2792.50 / 2825.79ms |
| 客户端调度滞后 p99 | 1.635ms |
| 建连及排空时间 | 6.87418s |
| 成功连接共同保持时间 | 28.126s |
| Broker 建连期 CPU / 保持期 CPU | 6.998 / 0.154 个核 |
| Broker 三进程同时 RSS 峰值 | 1065.71MiB |
| 四个 probe 建连 CPU / RSS 峰值 | 0.270 个核 / 11.91MiB |

连接验收失败，未继续 4000/s、消息 QoS0/1/2 或十万连接测试。1091.77 completed/s
只是本轮成功数除以建连及排空时间，不能当成最大稳定吞吐。

入口诊断将错误定位到了握手并发限制，而不是仅从 RST 猜测：

| 节点 | 获得握手许可 | 拒绝 | 合计尝试 | 观测峰值 / 上限 |
|---|---:|---:|---:|---:|
| 1 | 2498 | 836 | 3334 | 1024 / 1024 |
| 2 | 2501 | 832 | 3333 | 1024 / 1024 |
| 3 | 2506 | 827 | 3333 | 1024 / 1024 |
| 合计 | 7505 | 2495 | 10000 | — |

拒绝数与失败数完全一致，许可数与 CONNACK/PING 数完全一致，保持阶段握手 active
均归零。更深层原因是握手等待控制面太久、占着入口许可；单纯放大入口只会允许
更多请求等待，不能提高后端处理能力。

资源来自 `/proc` 相邻同 PID 的完整采样区间，CPU 按 ticks/单调时钟计算。
Broker 建连部分为 5 个完整区间、5.28006s，合计 36.95 CPU 秒；未插值补边界。
RSS 是同一次采样的进程总和。跨机器对齐依赖 wall clock，没有独立测量时钟偏差。
详见 [aggregate.json](aggregate.json)、[resource-summary.json](resource-summary.json)。

## 写请求排队

采用建连前及排空后的静稳累计快照差分，共 9.000003s，较建连窗口前后分别多
0.736s 和 1.390s，包含少量心跳。15058 条完成的控制写入统计如下：

| 阶段 | 平均耗时 |
|---|---:|
| 整条 ControlWrite | 975.866ms |
| 等待 mailbox 容量 EnqueueWait | 528.752ms |
| 入队至 worker 取出 QueueResidence | 416.589ms |
| 返回结果后的调用方唤醒 ReplyWake | 1.702ms |

两个排队阶段相加为 945.342ms，占平均写延迟 96.872%。这些是耗时，不是 CPU
占用。诊断定时器包含失败和取消路径；本轮相关计数和直方图桶已校验一致。
少数 carry 项在首次取出后的等待不计入 QueueResidence，因此不能把剩余差值
严格定义成另一项服务时间。

## 同一批次完整 Raft 链路

在实际 6.87418s 建连及排空窗口中，用同节点、log index 和单调时间顺序，唯一匹配
了 239 个成功批次的提交、Append、SaveCommitted、Apply 和返回事件，未匹配数 0。
231/239 个批次达到 64 条上限，合计 15034 条命令，平均 62.904 条/批。
串行批次服务合计 6.809823s，约占建连窗口 99.06%。

| 顺序阶段 | 平均 ms | p99 ms |
|---|---:|---:|
| 提交至开始本地 Append | 0.705 | 6.122 |
| 本地 WAL Append 完成 | 4.678 | 8.585 |
| Append 完成至开始 SaveCommitted | 13.463 | 27.522 |
| SaveCommitted：提交位置持久化 | 3.898 | 8.055 |
| SaveCommitted 完成至开始 Apply | 1.517 | 6.993 |
| ApplyQueued：应用及其调度 | 3.636 | 9.804 |
| Apply 完成至批次返回 | 0.596 | 4.835 |
| 整批 | **28.493** | **48.085** |

均值阶段可以相加得到整批均值；各阶段 p99 不能相加。13.463ms 包含副本复制、
多数派确认及 Raft Core 调度等待，当前记录还不能在三者之间准确分摊，更不能
说全部是网络或 ReadIndex 的耗时。这些控制 RPC 实际在同一台机器的 loopback 上。

9 秒直方图窗口中有 263 批，不能将它和上表 239 批混为同一个总体。该窗口进一步显示：

- WAL durable callback 等待 526 次（每批 Append 与 SaveCommitted 各一次），
  平均 3.405ms。它包含后台 flush 和线程唤醒，不是直接测得的 fsync 系统调用时间。
- WAL worker 调度平均 0.400ms，WAL mutex 等待 0.019ms；slot 等待接近零。
- 应用前重新读取、解码及校验 WAL 输入平均 0.540ms/批；状态变更 1.195ms/批，
  history 更新 0.087ms/次；状态锁等待平均小于 1µs。
- 没有发生 Checkpoint，不能将本轮排队归因于 bbolt 快照持久化。

这些子阶段有嵌套关系，不能再加到上表。详见
[chain-analysis.json](chain-analysis.json)；[stage-summary.json](stage-summary.json)
保留旧的稀疏诊断输出，不能用其最大样本代替总体 p99。

## ReadIndex 请求放大

静稳差分窗口共 37531 次 ReadIndex，平均 11.110ms；76146 次 AppendEntries RPC，
平均 RPC 往返 10.283ms，编码平均 4.910µs、解码 0.300µs。RPC 时间包含调度和
远端执行，不是纯线速 RTT。

源码路径和计数对应如下：

1. `Durable::inspect → rpc.owner → Control::leader`：1 次 ReadIndex。
2. `AcquireOnce → barrier → Control::applied`：进入和 ready 返回前各 1 次。
3. 新会话 reset 改变 logical_generation，随后 `DurableFilters → barrier`：再 2 次。

本轮 `7505 × 5 + 6 = 37531`，与正常新连接路径吻合；重试或 ready 重检可能产生
更多轮，不能将 5 当作所有连接场景的固定常数。相关实现见
[controller.rs](../../../crates/mqtt-cluster/src/controller.rs)、
[applied.rs](../../../crates/mqtt-cluster/src/controller/applied.rs) 和
[durable.rs](../../../crates/mqtt-cluster/src/broker/durable.rs)。

已核对 Cargo.lock 固定的 OpenRaft 0.9.25 源码：`ensure_linearizable` 每次发送独立
`CheckIsLeaderRequest`；Core 对每个其他 voter 创建空 entries 的 AppendEntries
任务。三节点情况下每轮派出两个 RPC，没有合并并发读取或复用普通心跳。
本机加一个远端即可构成多数派，但两个任务已经派出，完成多数派后不会自动取消
另一个任务。因此约 75062 次 RPC 可由这些 ReadIndex 解释；剩余 1084 次不能仅凭
减法完整区分成数据复制或心跳。

空 AppendEntries 不等于落盘一次：本轮 leader 记录的是 526 次 WAL flush 等待，
不是 76146 次 fsync。也不能直接把 37531 × 11.110ms 当成串行耗时，读取是并发的。

## CPU 热点和证据限制

perf 共记录 2374 个样本，报告 lost 0。建连窗口共 1685 个原始指令样本，flat IP
全部解析成功；仅 323 个样本有可用调用栈。故叶子符号比例使用**全部 1685 个样本的
period 权重**，而非只使用能展开调用栈的子集。

| 叶子符号 / 路径 | 加权采样占比 |
|---|---:|
| `compiler_rt.memcpy.memcpyFast` | 5.88% |
| 经 `Durable::recipients` 的迭代、过滤、收集路径 | 5.88% |
| `heap.SmpAllocator.alloc` | 4.45% |
| `_raw_spin_unlock_irqrestore` | 3.44% |
| durable owner BTreeMap iterator `next` | 2.08% |
| `c.malloc.free` | 2.02% |

`Durable::recipients` 对每条待分发消息遍历完整 durable owner 目录，再逐 owner
匹配订阅；没有订阅的连接也在这个目录。连接成功后的 `$SYS/events/client/connected`
会走消息分发，所以本轮没有业务发布也仍触发该路径。这是独立于 ReadIndex 的优化点。
这个符号覆盖迭代过滤收集整体，不能误称为单独 Vec 分配或读锁成本。

调用栈缺失使 memcpy/allocator 无法可靠分摊到完整上层调用链，也不能把两者全部
归因于 ReadIndex。`inclusive` 只统计已展开栈，是相对全部样本的下界，且互相重叠。
perf period 合计 34.388 秒只是采样权重，不替代 `/proc` 的 CPU 核数计算。虚机不
支持本轮硬件 cycles/instructions 事件，因此使用软件 cpu-clock。
详见 [cpu-analysis.json](cpu-analysis.json) 和 raw/perf*.tar.gz。

## 后续修复优先级（尚未实施）

1. 合并已经排队的一组一致性检查，共享一次**新的** ReadIndex 轮次。晚到请求
   进入下一组；不能给新请求复用请求发起前的旧轮次或 TTL 缓存。保留 term、
   applied revision、所有在线节点可见性及返回前重新检查，补故障切主测试。
2. 为 recipient 查询建立订阅索引和无订阅快速路径，保留 Topic、会话 generation、
   共享订阅及持久化转发语义，避免每次发布扫描所有连接。
3. 在前两项后复测剩余批次服务瓶颈，再评估有界批次流水线和批次大小；不能删去
   quorum、WAL 或 Apply 等待来换取提前 ACK，也不能重新向 Core 无界灌入写请求。
4. WAL 输入重复读取/解码的 0.540ms/批属于较低优先级，优化需保留崩溃恢复校验。

目前不能证明 13.463ms 中有多少能通过 ReadIndex 合并消除。必须在实现后用相同
负载、相同限额重新测量；本轮没有继续压力测试。

## 实现、验证和复现

冻结契约：[phase7r-raft-chain-diagnosis.md](../../contracts/phase7r-raft-chain-diagnosis.md)。
Control、Storage、Transport 三个真实 Subagents 分别在独立 Git worktree/分支实现，
统一集成后又独立复核了原始队列、批次、ReadIndex 以及 CPU 计算。

- `cargo test --locked -p mqtt-cluster -p mqtt-transport`：178 + 203 = **381** 通过。
- `cargo clippy --locked --workspace --all-targets -- -D warnings`、fmt：通过。
- `LYRNORMQ_RAFT_PROFILE=1` 的真实进程黑盒：**2** 通过，包含双版本跨节点
  QoS0/1/2、顺序以及 leader 崩溃后的元数据/离线消息恢复。
- 本地 native 和锁定依赖的 amd64 musl release 构建通过。日志保存在
  [verification](verification/)，摘要见 [validation.json](validation.json)。
- 上传完整符号二进制曾在负载开始前超时，改压缩上传完成；压测结束后的流式
  perf 归档传输超时，改为远端完整归档再 SCP。只运行了**一次**压测，没有重跑负载。
- 用户态栈展开不完整后，对同一 perf.data 补导出 `perf script -G`，没有再次采样。
  历史补充结果存 raw/perf-flat.tar.gz；当前 run.py 会自动归档 flat 导出。
- 所有 Broker/probe/perf 已停止，两个服务器复核为空，见
  [raw/post-stop.json](raw/post-stop.json)。源端口范围保持 32768..60999。

离线复算不访问服务器、不产生负载：

```sh
mkdir -p target/raft-chain-offline/server target/raft-chain-offline/client target/raft-chain-offline/perf
tar -xzf docs/performance/raft-chain-20261103/raw/server.tar.gz -C target/raft-chain-offline/server
tar -xzf docs/performance/raft-chain-20261103/raw/client.tar.gz -C target/raft-chain-offline/client
tar -xzf docs/performance/raft-chain-20261103/raw/perf.tar.gz -C target/raft-chain-offline/perf
tar -xzf docs/performance/raft-chain-20261103/raw/perf-flat.tar.gz -C target/raft-chain-offline/perf
cp docs/performance/raft-chain-20261103/aggregate.json target/raft-chain-offline/
python3 docs/performance/raft-chain-20261103/analyze.py target/raft-chain-offline
```

归档及 perf.data SHA256 见 [raw/checksums.json](raw/checksums.json)。实际运行入口是
[run.py](run.py)，复用上一轮 probe/harness，依赖原服务器部署脚本和 SSH 配置。
它使用独占新目录、防重复开始和进程身份校验；此次目录已经使用，不能直接重跑。
