# Phase7s：ReadIndex 合并、订阅索引与同负载复测

已实现并集成两项优化。相同 2000/s、10000 次连接尝试下，成功并保持存活的连接
由 7505 增加到 9264，平均建连延迟下降 38.92%，p99 下降 31.15%。**仍有 736 次
入口拒绝，验收仍为 FAIL**。没有提高限额、继续更高负载或重跑这一组。

## 实现和一致性边界

冻结契约：[phase7s-readindex-recipient-optimization.md](../../contracts/phase7s-readindex-recipient-optimization.md)。
控制面、Runtime、Quality 三个真实 Subagents 在独立 worktree/分支工作，提交后
由主控集成并执行真实进程验证；Quality 另外审查了两份实现。

- `ReadIndex`：一个 worker 收集已经排队的调用，再发起一次新的
  `ensure_linearizable`。正在进行的轮次不接收迟到调用，不缓存旧检查结果。
  每轮最多 1024 个已到达请求，不等待凑满；handoff 等待与结果等待共用原有
  一秒期限。调用方取消不影响同组其他人，关闭 Controller 时结束 worker。
  各调用方原有 term、revision、all-live visibility 检查均保留。
- `RecipientIndex`：复用现有 Router 建立仅包含有订阅客户端的派生索引。
  快照安装重建一次，增量更新仅触及变更 owner；查询不再扫描全体会话。
  元数据锁始终在索引锁之外，发布与查询保持 filters/logical_generation 一致。
  保留通配符、根 `$` 排除、字面 `$share/` 匹配、去重和客户端排序。
- 没有修改协议、磁盘 schema、Raft WAL、bbolt checkpoint、ACK、batch64/512KiB、
  handshake1024/节点或超时设置。每连接两次控制写入仍保留。

实现提交：ReadIndex `2aaf3be`；recipient index `3bde0ab`；独立黑盒测试 `3e6cf14`。
三份原始 agent 提交分别为 `c723bb4`、`965f1ac`、`8e62cdc`。

## 环境与方法

- Broker 47.84.224.186，私网 172.16.240.180：三个双角色进程共处一台 8 逻辑核、
  约 14.96GiB Intel Xeon Platinum 虚机，Linux 7.0.0-38-generic。
  每进程 4 Tokio workers；Controller RPC 为 loopback。
- 压测机 47.84.241.200，私网 172.16.240.181：4 个 Rust probe，各 500/s、2500
  连接、2 workers、2048 in-flight；均匀连接三个 MQTT 端口。只跑一组，不重试。
- 使用 Phase7r 相同探针、部署脚本、参数和诊断设置。部署脚本 SHA256
  `a74f6d32f91716c21c9e898ed30d9dcbfb0a66a75465b3708f3154cf5bed61b3`，
  与旧版本本地副本一致。独立审查确认规范化后的 Broker 命令及 limits 相同。
- 本地 `cargo zigbuild --locked --release --target x86_64-unknown-linux-musl -p rmqtt`，
  `CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none`，与基线相同。
  服务器没有编译。二进制为无动态依赖的静态 ELF，253738192 bytes，SHA256
  `725a696754b8d385374b1bc00ba29c3b4da42bf2057a8bca470d461302f55f8c`。
- 开始 Unix ms `1793749479964`，共同 PING `1793749514964`；实际成功连接共同保持
  28.805 秒。两次诊断均 `RMQTT_RAFT_PROFILE=1`，软件 cpu-clock 49Hz 有界采样。

配置、哈希和原始数据见 [deployment.json](deployment.json)、
[environment.json](environment.json)、[raw](raw/)。

## 同负载结果

| 指标 | 优化前 Phase7r | 优化后 Phase7s |
|---|---:|---:|
| 尝试 / 未开始 | 10000 / 0 | 10000 / 0 |
| CONNACK 成功 / 最终 PING 存活 | 7505 / 7505 | 9264 / 9264 |
| 失败 / 握手入口拒绝 | 2495 / 2495 | 736 / 736 |
| 成功建连平均 | 2060.884ms | 1258.875ms |
| 成功建连 p99 | 2792.502ms | 1922.663ms |
| 客户端调度滞后 p99 | 1.635ms | 1.596ms |
| 建连及排空时间 | 6.874s | 6.196s |
| 本轮成功数/建连及排空时间 | 1091.77/s | 1495.21/s |
| Broker 建连 CPU | 6.998 核 | 6.845 核 |
| Broker 保持阶段 CPU | 0.154 核 | 0.147 核 |
| 三个 Broker 同时 RSS 峰值 | 1065.715MiB | 1220.969MiB |
| probe 建连 CPU | 0.270 核 | 0.289 核 |

优化后失败为 733 RST + 3 EOF。节点 1/2/3 的握手获得数为 3080/3094/3090，
拒绝数为 254/239/243，逐节点总量 3334/3333/3333，峰值均 1024，最后 active=0。
没有未开始的客户端尝试，失败计数与服务端入口拒绝计数完全相符。

成功数增加 23.44%，失败数减少 70.50%。CPU 仅小幅下降，不能宣称 CPU 瓶颈已消除。
RSS 对应不同成功连接数，不能直接据总量上升推断单连接内存退化。两轮延迟均只包含
成功连接；单轮对比不能证明稳定方差、最大容量或业务消息吞吐。

## ReadIndex 和队列

| 指标 | 优化前 | 优化后 |
|---|---:|---:|
| 逻辑 ReadIndex 调用 | 37531 | 46326 |
| 实际 ReadIndex 轮次 | 37531 | **1123** |
| 每轮平均逻辑调用 | 1 | **41.252** |
| AppendEntries RPC | 76146 | **3518** |
| 控制写入平均 | 975.866ms | 587.144ms |
| 等待队列容量平均 | 528.752ms | 276.018ms |
| 入队后等待平均 | 416.589ms | 288.870ms |
| 两段排队合计 | 945.342ms | 564.888ms |

逻辑调用仍满足 `9264 × 5 + 6 = 46326`，分组成员数总和也恰为 46326。实际轮次
比这些逻辑调用少 97.576%，没有通过删除原有一致性检查获得提升。RPC 总量比旧组
减少 95.38%，即使成功处理了更多连接。

两轮累计直方图差分窗口分别为 9 秒和约 8 秒，均覆盖整个建连阶段并含静稳边界及
心跳。新窗口比实际建连前后各多 0.261 秒、1.543 秒；不能将直方图批次数与下面
严格建连窗口的批次数混用。RPC trace 达到前 4096 条上限，完整计数取直方图，
不能凭截断的 trace 推算所有 RPC 类型构成。

## 剩余瓶颈：串行控制写批次

优化后 293 批全部按同节点/log index/单调时间唯一关联，285 批满 64 条，合计
18558 条命令。串行服务时间 **6.124104s / 6.195797s = 98.843%**，仍接近持续忙碌。

| 同一批次的顺序阶段 | 优化前平均 | 优化后平均 |
|---|---:|---:|
| 提交至开始 Append | 0.705ms | 0.476ms |
| 本地 WAL Append | 4.678ms | 4.456ms |
| Append 完成至开始 SaveCommitted | 13.463ms | 7.933ms |
| 提交位置持久化 SaveCommitted | 3.898ms | 3.812ms |
| 提交位置完成至开始 Apply | 1.517ms | 1.006ms |
| 状态应用及调度 ApplyQueued | 3.636ms | 3.062ms |
| Apply 完成至返回 | 0.596ms | 0.157ms |
| **整批平均** | **28.493ms** | **20.901ms** |
| **整批 p99** | **48.085ms** | **32.644ms** |

7.933ms 仍包含复制、多数派确认和 Core 调度，不能归为纯网络 RTT。WAL 回调等待
平均 3.373ms/次，应用前重复校验 0.491ms/批；本轮仍无 checkpoint。控制写入的
96.209% 仍花在排队。两次写/连接、每批约 64 条、每批约 20.9ms 的串行处理方式，
解释了当前负载下约 1.5k/s 的完成速度；这是本轮服务节奏，不是容量上限证明。

下一轮应冻结**有界写批次流水线**的顺序、结果归属及故障恢复契约，再评估流水线
或合并必要控制写入。必须保留 quorum、WAL、Apply 完成后的确认，避免再次无界
向 Core 投递；加大握手额度不能消除这一服务瓶颈。本轮没有实施这部分，也未继续加压。

最终三个节点均 term1、leader1、applied784、metadata owners9264。建连至共同 PING
期间没有记录复制超时；PING 之后批量断连时有 4 次 150ms 复制超时，原始日志保留，
不能将本轮表述为完全无错误日志。

## perf 时钟复核与勘误

独立复核发现 perf 默认时间戳与 Python CLOCK_MONOTONIC 不能直接对齐。文件缺少
CLOCKID/CLOCK_DATA；采样后的只读校准测得 MONOTONIC 与 MONOTONIC_RAW 相差约
3.330 秒。该校准不是采样时的精确映射，不能用它事后强行修正阶段边界。

因此，两轮按旧方法选择的“建连阶段 CPU 样本数/热点百分比”撤回。旧报告已添加
勘误，原始文件保留；当前 [cpu-analysis.json](cpu-analysis.json) 明确标注
`phase_alignment_valid=false`。未来 harness 使用 `--clockid mono`，这只是后续采样
修正，本轮没有为此重跑压力测试。连接延迟、内部 Raft/队列计时与 `/proc` CPU 核数
不依赖这种 perf 时钟映射，不受影响。

完整采样文件有 2260 个样本，全部属于三个 Broker PID，无外部 DSO、未解析样本或
lost samples；含预热、保持和清理阶段。旧全目录扫描符号未出现，仅见 1 次
RecipientIndex::replace。此观察和实现审查支持消除扫描，但不用于量化阶段 CPU 收益。
`sccp` 是 musl 的 syscall 包装函数，并非外部编译器污染。调用栈展开依然不完整。

## 验证和离线复算

- 集成 `cargo test --locked -p mqtt-cluster -p mqtt-transport`：**191 + 203 = 394** 通过。
- workspace strict Clippy、fmt：通过。
- 优化版 release，profiling 开启：**4 项真实进程测试通过，20.659s**，覆盖双版本
  跨节点 QoS0/1/2/顺序、leader 崩溃后元数据与离线恢复、订阅删除/重加/去重、
  Clean Start 跨 owner 代次替换、真实 `$SYS` 连接事件及根 `#` 排除。
- 三个 agent 分别独立复算连接/资源、Raft/分组、CPU 证据。
- 两台服务器全部 Broker/probe/perf 测试进程已停止，源端口范围仍 32768..60999。

测试日志、CPU 时钟勘误依据及停机复核见 [verification](verification/)。
只跑一组同负载测试，未运行更高速率、业务消息 TPS 或十万连接容量测试。

```sh
mkdir -p target/raft-optimization-offline/server target/raft-optimization-offline/client
tar -xzf docs/performance/raft-optimization-20261104/raw/server.tar.gz -C target/raft-optimization-offline/server
tar -xzf docs/performance/raft-optimization-20261104/raw/client.tar.gz -C target/raft-optimization-offline/client
cp docs/performance/raft-optimization-20261104/raw/perf.tar.gz target/raft-optimization-offline/
cp docs/performance/raft-optimization-20261104/raw/observers.json target/raft-optimization-offline/
cp docs/performance/raft-optimization-20261104/aggregate.json target/raft-optimization-offline/
python3 docs/performance/raft-optimization-20261104/compare.py target/raft-optimization-offline
```

复算不访问服务器、不启动负载。完整结果见 [comparison.json](comparison.json)，
原始归档及 perf.data SHA256 见 [raw/checksums.json](raw/checksums.json)。
