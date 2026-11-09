# 2000/s、10万连接、5分钟保持：验收失败

三节点双角色的Raft集群**成功组成并最终重新收敛**，但本轮连接负载失败：
TCP采样峰值59296，约开始后39秒已全断，未完成10万连接和至少300秒保持。
看到批量断连后人工中止，没有继续更高负载，也没有修改Broker或继续CPU优化。

## 环境与实际参数

- Broker47.84.224.186，private172.16.240.180，8逻辑CPU、约14.96GiB内存。
  同一虚机运行3个双角色进程，每进程4Tokio workers。MQTT18831/18832/18833，
  Controller1/2/3分别17001/17002/17003，data101/102/103分别17101/17102/17103。
- 压测机47.84.241.200，private172.16.240.181。4个Rust探针各500/s、25000连接，
  两workers、2048在途握手、唯一前缀/offset，按3个端口均分。
- Broker复用7u静态musl版本SHA
  `eb6f4be285dc7b8af33e820de4f8b4bed64772c9c6961a581b3423c72f4b55b4`，未重新编译。
  探针本地zigbuild，SHA
  `dac46888dfb38b0ee1fc4ec38e67074b00eddb37728271bc392767256bc1b528`。
  部署后先用已存在的输出文件阻止网络执行，确认实际binary能接受+370s的参数。
- 计划开始Unix ms1793759724570，PING1793760094570：50秒ramp+20秒排空余量+
  300秒保持。聚合器依据真实最后CONNACK和共同PING验证300秒，未用探针内部
  固定10秒最低标记冒充5分钟。MQTT5 KeepAlive0；本轮没有消息发布订阅负载。
- connection/session50000每节点、state256MiB每节点、handshake1024每节点保持原值。
  每节点目标约33333。进程FD250000；压测机源端口临时10000..65535，随后恢复
  32768..60999。没有改变Broker配额、metadata/wire界限、Raft/W2/ACK/超时。
- 资源每秒采样，TCP每5秒扫描，Controller每10秒新鲜target-only查询。
  LYRNORMQ_RAFT_PROFILE=1提供固定计数和有限trace；未采集新的perf压力剖析。

契约：[phase7v-100k-hold.md](../../contracts/phase7v-100k-hold.md)。
Broker实现694bfbb；长保持探针修复8a798f9；压测契约/脚本c4177f3。
配置/哈希见[deployment.json](deployment.json)、[settings.json](settings.json)。

## 连接与资源结果

| 指标 | 原始记录复算 |
|---|---:|
| 计划连接数 / 速率 | 100000 / 2000每秒 |
| TCP采样峰值 | **59296** |
| 峰值各端口 | 19503 / 19805 / 19988 |
| 峰值扫描时段 | 开始后33.012–34.012秒 |
| 首次全断的新TCP扫描 | 开始后38.645秒 |
| 服务端入场计数 | 100000 |
| 获得握手许可 / 被拒绝 | 86454 / **13546** |
| 最终客户端成功 / 平均 / p99 | **未完成，不可计算** |
| 10万共同保持300秒 | **未通过** |
| 峰值前完整区间平均CPU | 6.606核，覆盖33.104秒 |
| 整个失败案例CPU区间峰值 | 7.477核 |
| 三个进程同时RSS峰值 | 7652.625MiB（7.473GiB） |
| FD同时和峰值 | 59420 |
| 系统可用内存最低 | 6975.688MiB（6.812GiB） |

TCP是采样到的ESTABLISHED，不等于成功CONNACK；5秒内的重复行复用TCP计数，
必须看tcp_begin/end_wall_ns。FD在开始后35.182→37.620秒从57147降至136，
新TCP扫描38.645秒为0，证明先前缓存59296不能被读成一直保持在线。

服务端计数按节点分别：许可28417/28936/29101，拒绝4917/4397/4232；总计10万。
三个节点握手在途峰值都达到1024。许可不是CONNACK，不能用86454替代成功连接数。

人工中止时，探针尚在共同PING前的等待阶段，其4个最终report/raw文件均为0字节。
进程退出-9，故客户端成功数和延迟未知；原聚合器部分统计中的0不能当实测0。
[result-summary.json](result-summary.json)将这些值明确设为null。
RSS峰值发生在掉线后，不能称作稳定10万连接的内存。三个Broker PID始终存在，
未触发512MiB内存保护或进程退出保护，无OOM迹象。

## 三节点Raft的证据与压力表现

1. 三个Controller初始WAL各有校验正确的Membership，configs均为`[[1,2,3]]`，
   membership log index0。三个目标节点均已应用到27，term1、leader1。
   这是实际日志成员及应用进度证据，不只是配置文件列出三个地址。
2. 每个probe配置的其他种子都改成关闭端口，Status不能回退到另一个健康Controller。
   启动target-revision门禁通过，各节点读取时间独立记录。
3. 承压期间记录到ReadIndex/AppendEntries/Vote超时，leader由1切到2，term从1
   发展到3、最终5。10秒采样不能覆盖每次短暂选举状态，原始错误日志保留。
4. 停机前fresh gate捕获target4187，三者最终均term5、leader2、applied4187，
   data nodes3、excluded空、repair空，门禁通过。控制面owner数均73099，
   这是保留的会话owner条目，不能当作在线连接数或成功CONNACK数。
5. full/hold-end门禁缺失，没有100000共同保持的证据。初始化和最终收敛通过，
   不能覆盖压力期间的失稳，整体结果保持FAIL。

原始Membership和各轮gate见[raw](raw/)内server.tar.gz；
[raft-startup.json](raft-startup.json)、[raft-final.json](raft-final.json)。
最终进度收敛不等于做过完整元数据逐字节hash比较。

## 已定位的长停顿

只读审查从同节点trace、累计计数差分和代码确认了第2次checkpoint的长持锁路径：

| 节点 | CheckpointTotal | CheckpointPersist | index2000..2001 ApplyQueued |
|---|---:|---:|---:|
| 1 | 3.268s | 2.473s | 5.128s |
| 2 | 3.362s | 2.806s | 5.176s |
| 3 | 3.469s | 2.855s | 5.142s |

ApplyQueued前段分别等待串行gate约3.367/3.390/3.588秒。`build_snapshot()`在
整个快照构建/保存期间持apply_gate；`run_checkpoint()`持WAL和checkpoint mutex
执行`verify_wal_tail`、`read_snapshot`、`write_snapshot`，因此会阻塞append/commit。
这一路径的checkpoint持锁工作约2.47–2.86秒，WAL锁等待同步出现约1.55–1.78秒峰值。

首轮时间线（2026-11-04，Asia/Shanghai）：

- 10:35:58.382：node1 index2000..2001开始等待应用，直到10:36:03.510。
- 10:35:59.364：首次约1秒ReadIndex超时，随后Owner/Applied失败。
- 10:35:59.562：Acquire/Mutation约2秒超时；Heartbeat相继失败。
- 10:36:00.749：确认领导权的AppendEntries150ms超时。
- 10:36:01.800：复制到node2/node3超时。
- 10:36:03.214：新TCP扫描已全断；期间出现Vote500ms超时。

日志和源码强烈支持checkpoint暂停引发控制链路超时这一机制。Heartbeat只在
确认后续租，本地3000ms租约失效会由lease_loop触发fence_all，与全断时序吻合；
但缺少实际fence调用/deadline日志，不能声称每一条断连都已直接归因。
也不能把CheckpointPersist全部说成fsync耗时或bbolt库本身慢。未发现metadata
capacity/byte limit/存储失败日志，没有证据把本次故障归因于元数据字节限额。
相关代码：[storage.rs](../../../crates/mqtt-cluster/src/storage.rs)，未在本任务修改。

下一步应解决checkpoint对WAL/应用的长时间阻塞，并保留快照耐久、提交水位及
snapshot-before-purge顺序后重新验收。用户已暂停CPU优化，本轮未实施该修复。

## 探针预检失败与修复

第一次准备使用旧探针，其共享PING上限60秒，+370秒在任何TCP连接前即被拒绝。
该次TCP采样峰值0，源端口已恢复；记录独立保留在[setup-failure](setup-failure/)，
不算成一次10万连接实测，也未隐藏成成功。随后仅修改探针：将有限共享时间窗口
上限扩至600秒，未来开始仍最多60秒；连接/超时/报文/速率算法不变。

先复现失败测试，再通过压测工具60项测试、workspace严格Clippy和fmt；本地
zigbuild探针通过静态ELF检查。新case使用独立目录和哈希验证，实际参数预检
在预留输出文件处结束，确保网络发起前已通过参数解析。没有服务器编译。

## 清理和离线复算

两台服务器Broker/探针/perf测试进程均清空，专用观察器退出；源端口已恢复。
见[verification](verification/)、[raw/checksums.json](raw/checksums.json)。

```sh
mkdir -p target/100k-offline/server target/100k-offline/client
tar -xzf docs/performance/cluster-100k-hold-20261104/raw/server.tar.gz -C target/100k-offline/server
tar -xzf docs/performance/cluster-100k-hold-20261104/raw/client.tar.gz -C target/100k-offline/client
cp docs/performance/cluster-100k-hold-20261104/settings.json target/100k-offline/
cp docs/performance/cluster-100k-hold-20261104/verification/post-stop.json target/100k-offline/
python3 docs/performance/cluster-100k-hold-20261104/analyze.py target/100k-offline
```

复算不启动负载或访问服务器；客户端未落报告的指标仍为null。
