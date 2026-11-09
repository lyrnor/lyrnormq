# Phase7t：有界 FIFO 控制写流水线

**相同2000连接/秒、总计1万连接的验收通过：10000成功、10000保持后PING存活、零失败。**
平均建连延迟由1258.875ms降至365.662ms，p99由1922.663ms降至527.243ms，
分别下降70.95%和72.58%。本轮只跑这一组，没有增加限额、重跑或继续加压。

## 实现与恢复语义

冻结契约：[phase7t-control-write-pipeline.md](../../contracts/phase7t-control-write-pipeline.md)。
三个真实 Subagents 分别在独立 worktree/分支实现控制写流水线、存储恢复回归和
真实进程故障测试；提交后由主控统一集成，另做交叉源码审查。

- 一个 worker 最多保留 4 个未完成批次，逐次等待 OpenRaft 0.9.25 的
  `client_write_ff` 完成入队，保证提交顺序。取得结果 receiver 不代表提交成功；
  仍等待多数派耐久提交与状态应用结果，再按 FIFO 返回对应命令的结果。
- 已就绪的最老批次完成优先于新输入；第五批等待空位。调用方取消不能取消已接受
  的工作，关闭输入后排空 carry 和已提交批次，单批失败不能丢弃其他批次的结果。
- 租约时间同时考虑已提交状态及更早未完成批次的 grant，保持单调；临时 grant
  记录随批次释放，不累积历史节点。提交后结果不明继续按 unknown outcome 处理。
- 批次 64 条/512KiB、mailbox1024、调用方 2 秒期限、handshake1024/节点均未改变。
  WAL flush、committed-only replay、状态应用串行 gate、bbolt checkpoint 和磁盘 schema
  全部复用。没有 OpenRaft fork，也没有用放宽 ACK 或一致性检查换取性能。
- 新增固定指标 `BatchSubmit` 和 `BatchWindow`。批次现在可以重叠，忙碌时间取区间
  并集，不能将批次耗时之和当作实际耗时。一次 Append/Apply 可覆盖多个批次，
  共享区间单独列出，不推导“一批一次 fsync”。

实现提交 `5a0c29b`（agent `c30ac0e`）；存储测试 `227a2b7`（`5c945de`）；
独立进程测试 `8b246d6`（`4c67f82`）；契约 `9edeb2c`，测量脚本 `fee2954`。

## 环境和复现方法

沿用 [Phase7s](../raft-optimization-20261104/README.md) 的负载和参数：

- Broker 47.84.224.186，私网172.16.240.180，三个双角色进程共用一台 8 逻辑核、
  约14.96GiB Intel Xeon Platinum 虚机，每进程4个 Tokio workers。
  MQTT18831/18832/18833；Controller RPC 为 loopback。压测机47.84.241.200。
- 4 个 Rust probe，各500连接/秒、2500连接、2workers、2048inflight，均匀连接
  三个节点；统一开始时间及35秒后的 PING，无重试。本轮只运行这一组。
- 服务端部署脚本 SHA256
  `a74f6d32f91716c21c9e898ed30d9dcbfb0a66a75465b3708f3154cf5bed61b3`，
  与上轮一致。probe SHA256
  `556b3d560b3c79a4c77e9f06edc93052a876f6fb8a4be86eeaf163f70784b4ad`。
- 本地 `cargo zigbuild --locked --release --target x86_64-unknown-linux-musl -p lyrnormq`，
  使用 `CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none`。
  静态 ELF 无解释器/动态库依赖，253791184 bytes，SHA256
  `60753c36961bf63658451e5ff6d7f90ac6dd97a17326f1b67752e0b1472044f6`。
  服务器没有编译。链接器仅提示忽略已弃用的优化设置 `1`，构建成功。
- 两轮均开启 `LYRNORMQ_RAFT_PROFILE=1`。本轮49Hz有界 perf capture 明确使用
  `--clockid mono` 并保存 header，修复旧采样阶段时钟归属不可靠的问题。
  `/proc` CPU、RSS 和内部 Raft 计时不依赖旧 perf 的时钟假设。

## 本地验证

- 原单批 worker 无法满足“首批未完成时已提交四批”的回归，新实现通过。
- 7项新增流水线边界回归：四批窗口、第五批等待、FIFO、禁止提前 ACK、取消/
  失败隔离、关闭排空、错误结果归属，以及跨批租约递增与记录释放。
- 3项存储回归：多批次逐条结果/稀疏修订、仅恢复已提交前缀与快照清理、
  取消调用后应用执行权仍由 owned continuation 持有。
- 集成 `cargo test --locked -p mqtt-cluster -p mqtt-transport`：**201+203=404** 通过。
- `cargo clippy --locked --workspace --all-targets -- -D warnings` 和 fmt 检查通过。
- 优化版 release、profiling开启：**5项真实进程测试通过，28.730秒**，覆盖双版本
  跨节点 QoS0/1/2、leader崩溃/重启和离线恢复、订阅代次和 `$SYS` 边界，
  以及并发已确认 SUBACK 的故障后恢复与消息归属。

验证日志见 [verification](verification/)。

## 同负载结果

| 指标 | 上轮 Phase7s | 本轮 Phase7t |
|---|---:|---:|
| 尝试 / 未开始 | 10000 / 0 | 10000 / 0 |
| 成功 / 保持后 PING 存活 | 9264 / 9264 | **10000 / 10000** |
| 失败 / 握手入口拒绝 | 736 / 736 | **0 / 0** |
| 成功建连平均 | 1258.875ms | **365.662ms** |
| 成功建连 p99 | 1922.663ms | **527.243ms** |
| 成功建连最大 | 1938.798ms | 582.658ms |
| 客户端调度滞后 p99 | 1.596ms | 1.555ms |
| 建连及排空 | 6.196s | 5.250s |
| 成功数 / 建连及排空时间 | 1495.21/s | 1904.92/s |
| Broker 建连平均 CPU | 6.845核 | 6.951核 |
| Broker 保持阶段平均 CPU | 0.147核 | 0.153核 |
| 三个 Broker 同时 RSS 峰值 | 1220.969MiB | 1256.914MiB |
| 四个 probe 建连平均 CPU | 0.289核 | 0.345核 |

本轮开始 Unix ms1793751074710，共同 PING1793751109710。全部成功连接共同保持
至少29.751秒。节点1/2/3取得握手许可3334/3333/3333，拒绝0/0/0，inflight峰值
368/326/324，限额仍1024，最终active均0。四个probe所有错误和PING错误集合为空。
最终直接查询三个 Controller，均 term1、leader1、applied901、owners10000。

CPU没有下降：同一轮完成更多连接且排空更快，三节点共处8核虚机，建连阶段仍占
约6.95核。RSS是三个进程同时采样的和，不能把前后不同成功人数的总量直接作为
单连接内存变化。Broker建连CPU使用完整相邻采样区间，共4段/4.709秒；保持阶段
26段/27.938秒。跨主机阶段用墙钟近似对齐，未测量跨主机时钟偏移。

上述p99是**建连延迟**，不是业务消息发布订阅延迟。单轮2000/s通过不能证明更高
峰值容量、长期稳定性或QoS消息TPS；1904.92/s是含尾部排空的本轮完成速度。

## 写入排队与 Raft 流水线

| 指标 | 上轮 | 本轮 |
|---|---:|---:|
| 逻辑 ReadIndex | 46326 | 50006 |
| 实际新轮次 | 1123 | 414 |
| 每轮平均调用数 | 41.252 | 120.787 |
| AppendEntries RPC | 3518 | 1755 |
| 控制写入平均 | 587.144ms | **78.247ms** |
| 等待 mailbox 容量平均 | 276.018ms | <0.001ms |
| 入队后等待平均 | 288.870ms | **22.475ms** |
| 两段排队合计 | 564.888ms | **22.475ms** |

控制写入平均下降86.67%，排队占比由96.21%降至28.72%。本轮直方图差分窗口约
7秒，覆盖20069次控制写入及心跳/静稳边界；它不是下面的严格建连窗口。
50006逻辑读取等于10000×5+6，分组成员之和也等于50006。没有删掉原有检查；
流水线改变到达节奏后，现有fresh-round分组接纳更多已排队请求。
1755次RPC原始trace可全部复算，其中1305空AppendEntries、450携日志调用，
合计携带858个日志项。空RPC还包含Raft心跳，不能全当成ReadIndex。

严格建连窗口有411批/20051条命令，252批满64条；全部能唯一匹配Append和Apply。
批次间隔有重叠，**区间并集5.250669秒，耗时之和20.686719秒，峰值4批同时在途**。
自身观测跨度为5.250669秒，平均在途3.940批。该跨度与客户端5.249561秒的窗口
来自不同观测端，不能相除宣称超过100%利用率。按提交次数观测的窗口占用均值
3.247，亦不能与按时间加权的3.940混用。4批同时活动5.060139秒，占自身
跨度96.371%；提交时窗口1/2/3/4的样本为17/83/106/223。

`client_write_ff`入队平均2.093µs，取得句柄不算成功；仍等quorum/apply完成。
整批壁钟平均20.901→50.333ms，p9932.644→80.658ms，但多批可以同时推进，
不再串行承担全部耗时。不能将批耗时增加等同吞吐回退，也不能把20.687秒作为
CPU或实际运行时间。

| 全部已关联批次的5段壁钟 | 上轮平均 | 本轮平均 |
|---|---:|---:|
| 提交至开始 Append | 0.476ms | 4.955ms |
| Append | 4.456ms | 5.030ms |
| Append 完成至开始 Apply | 12.751ms | 32.623ms |
| ApplyQueued | 3.062ms | 5.948ms |
| Apply 完成至 FIFO 回复 | 0.157ms | 1.777ms |

本轮0批共享Append区间，288批共享覆盖多个日志项的Apply区间。这些均为各批
经历的壁钟，并非独占服务成本。精确log-index的SaveCommitted细分只有229/411批，
另外182批由更高索引水位覆盖；不能把这229批的子集均值拼入全体411批的分段图。
详见comparison.json的stage_sample_counts及共享range记录。按区间去重，411批
对应229次Apply：123次单项、30次双项、
76次三项，其中106个范围覆盖上述288批。

leader在直方图窗口有676次WalFlushWait，平均3.595ms；follower分别471/474次，
平均3.690/3.692ms。该指标数的是WAL flush等待操作，不是系统调用次数。
没有checkpoint，不能将本轮耗时归因于bbolt快照。存储耐久边界未变。

单轮ReadIndex平均5.289→12.597ms，逻辑等待9.651→24.326ms，RPC平均
3.941→7.623ms：轮次减少但每轮变慢，剩余执行链仍有压力。Append结束至Apply
开始占单批壁钟64.814%，包含复制、提交推进、其他批次处理及调度；需要继续
拆分这一段，不能归结为纯网络或存储耗时。

## CPU证据、告警与剩余工作

本轮perf header明确`clockid: monotonic (1)`并带CLOCK_DATA参考点，参考墙钟与
观察器的wall−monotonic偏移一致。1584个建连阶段CPU样本、32.327采样CPU秒，
全部叶地址可解析，但只有396个调用栈成功展开，且未得到lyrnormq的Rust用户调用链。
因此叶热点用全部1584个样本计算，已展开系统栈不能作为Rust业务CPU归因。

叶热点包括memcpy7.765%、分配3.598%、memcmp2.462%、释放2.083%；网络协议栈和
任务调度也明显。复制/分配的上层调用方尚未定位，不能直接断言某个业务容器或
序列化步骤占用了这些全部成本。旧perf阶段时钟不可靠，不比较前后叶百分比。

建连窗口还有43条h2 reset-limit WARN（节点1/2/3为34/4/5），其中40条远端重置
pending-accept streams达到20，3条本地重置streams达到1024。这些没有造成MQTT
连接失败，但暴露内部RPC流生命周期压力，下一步应按调用点定位取消/重建路径，
并修复用户调用栈采集，再针对已定位的冗余RPC、复制、分配和任务切换优化，
保留一致性检查。

启动至共同PING没有复制超时；PING后集中断连出现一次1→3 AppendEntries150ms
超时，分别记录WARN/ERROR，随后还有progress WARN。不能将本次通过表述为全程
零错误日志。需单独验证集中断连下的调度/复制尾延迟。

下一轮先处理上述RPC生命周期和CPU热点，再逐项恢复更高连接速率、十万连接和
业务消息QoS0/1/2验收。当前仅证明这一组连接负载通过，没有运行更高或消息负载。

## 证据与离线复算

三个agent分别从原始数据独立复算连接/资源、Raft/流水线、CPU/时钟/存储指标。
两台服务器所有Broker/probe/perf测试进程均已停止，源端口范围仍32768..60999。
归档、配置、最终状态及SHA见[raw](raw/)、[deployment.json](deployment.json)、
[state.json](state.json)和[verification](verification/)。

```sh
mkdir -p target/raft-pipeline-offline/server target/raft-pipeline-offline/client
tar -xzf docs/performance/raft-pipeline-20261104/raw/server.tar.gz -C target/raft-pipeline-offline/server
tar -xzf docs/performance/raft-pipeline-20261104/raw/client.tar.gz -C target/raft-pipeline-offline/client
cp docs/performance/raft-pipeline-20261104/raw/perf.tar.gz target/raft-pipeline-offline/
cp docs/performance/raft-pipeline-20261104/raw/observers.json target/raft-pipeline-offline/
cp docs/performance/raft-pipeline-20261104/aggregate.json target/raft-pipeline-offline/
python3 docs/performance/raft-pipeline-20261104/compare.py target/raft-pipeline-offline
```

复算不访问服务器、不启动负载。完整结果见[comparison.json](comparison.json)；
原始归档和perf.data校验值见[raw/checksums.json](raw/checksums.json)。
