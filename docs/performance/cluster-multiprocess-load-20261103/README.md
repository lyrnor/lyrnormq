# 四实例建连复测：全部到达，服务端连接验收失败

用户要求启动多个压测实例。本轮在原压测机启动4个独立Rust进程，各500/s、2500连接，
总计划仍为2000/s、10000连接。**全部10000次连接实际发出，7747成功并保持存活，
2253失败。整体验收未通过；未继续4000/s、消息QoS0/1/2或10万连接测试。**

## 环境与负载

- Broker：47.84.224.186；压测：47.84.241.200；内网172.16.240.180:18831/18832/18833。
- 沿用两台8逻辑CPU Intel Xeon Platinum机器；Broker约14.96GiB RAM。
  Broker三进程各4Tokio worker，每进程同时承担Controller/data角色，共用一台主机。
- Broker SHA `87ced3e81da295b4273b2757c8fc4472409d88472e5c77cca77ba472fd28854b`，
  保持配置、持久化、超时不变；每节点max_handshakes=1024、max_connections=50000。
- 新探针在本地cargo zigbuild编译amd64 musl，SHA
  `556b3d560b3c79a4c77e9f06edc93052a876f6fb8a4be86eeaf163f70784b4ad`。
  每进程2Tokio worker、2048在途连接槽，总8192；CONNECT超时15秒，无重试。
- 独立Client ID前缀和offset避免互相接管，ID最终长度17字节；配置轮转分布3334/3333/3333。
  这个分布是计划连接，不是三个节点的成功连接计数。
- 共同开始2026-11-03 14:41:16.440 UTC，实际四进程开始偏差1ms；共同PING在35秒后。
  新目录`/opt/lyrnormq-multiprocess/20261103`，未复用旧Broker数据。
- 四条500/s流约每2ms产生4次到达，与旧单进程2000/s流的微观到达模式不同；
  总在途窗口也从2048增至8192。这不是只改变CPU进程数量的严格单变量对比。

## 实测结果

| 指标 | 结果 |
| --- | ---: |
| 计划 / 实际尝试 / 跳过 | 10000 / 10000 / 0 |
| CONNACK成功 / PING存活 | 7747 / 7747 |
| 连接失败 | 2253：2238次TCP reset，15次EOF |
| PING失败 | 0 |
| 成功连接平均 / p99 / 最大延迟 | 1906.363 / 2690.278 / 2726.583ms |
| 所有尝试的实际任务开始调度延迟平均 / p99 | 0.988 / 1.639ms |
| 从共同开始至最后完成 | 6.689s |
| 该失败cohort的完成比率 | 1158.221成功连接/s |
| 成功连接共同保持时长 | 约28.312s |
| Broker三进程建连CPU平均 / 采样区间峰值 | 6.964 / 7.148核 |
| Broker保持阶段CPU平均 | 0.116核 |
| Broker合计RSS采样峰值 | 1095.344MiB（约1.070GiB） |
| 压测四进程建连CPU平均 / RSS采样峰值 | 0.264核 / 12.031MiB |

延迟仅统计7747次成功连接，失败没有补零；p99从原始样本合并计算，没有平均四个分片p99。
独立Agent重新计算得到完全相同的计数和百分位。分片成功数1938/1985/1910/1914，
四片各2500次实际尝试、无跳过。共同保持的毫秒时间戳差为28312ms；用更精确的
单调时钟最大ramp保守计算为28311.293ms，均超过10秒，仅适用于成功连接。
1158.221/s不是持续稳定吞吐或最大建连能力；本轮没有发布/订阅TPS或消息丢失测量。

CPU按`/proc`累计tick、完整相邻采样区间的单调时钟差计算，不插值。
Broker建连阶段6样本/5区间共5.280秒，保持27样本/26区间共27.486秒；客户端建连
34样本/33区间共6.617秒。RSS取同一采样时刻的进程之和，不相加各进程独立峰值。
阶段以共同开始墙钟对齐；未测量跨主机时钟偏移。数据及边界见resource-summary.json。

停止前直接查询三个Controller：均term1/leader1、applied785、7747个metadata owner。
owner计数不代表在线数，存活另由PING确认。两台机器已确认没有lyrnormq/connections/
lyrnormq-loadgen测试进程；源端口范围保持32768..60999，未修改sysctl。

## 能确定的结论与待定位部分

上一轮单进程只发出7009次、跳过2991次；本轮全部发出，压测端调度p99约1.64ms且CPU低。
因此上轮2048在途槽截断到达的问题已经排除；扩大压测并发后暴露了服务端RST/EOF。

当前配置仍是每节点1024个握手槽。`crates/mqtt-transport/src/durable.rs`的TCP accept
路径在无法取得握手permit时直接关闭stream，没有读取CONNECT；`admission.rs`使用
try_acquire_owned。该路径能产生观察到的RST/EOF，是本轮失败的重要嫌疑。
成功连接响应接近2秒，使握手更易积压。**没有采集入口拒绝/握手在途计数，不能断言
全部2253次都由此分支产生，也不能认定提高限制就能解决服务端处理瓶颈。**

leader建连窗口中，稀疏阶段日志的最高已记录样本：

| 阶段 | 样本数 | 最大观测 |
| --- | ---: | ---: |
| Controller Write Acquire | 13 | 1085.612ms |
| Controller Write Mutation | 13 | 1293.945ms |
| Broker TakeMutate | 12 | 1349.654ms |
| RaftResult Mutation | 6 | 44.275ms |
| WAL append | 1 | 5.329ms |
| ReadIndex Authority | 12 | 18.920ms |

这些不是阶段p99，也不是配对请求，不能相减归因或据此排除IO。控制写队列驻留、
批量利用率及CPU热点仍需直接测量。启动阶段有一次vote transport错误；共同PING
之后收尾阶段有三次150ms AppendEntries timeout，均保留原文，不与建连时失败混淆。

下一步应补握手在途/拒绝计数、Controller队列驻留与批量统计、CPU profile，确认
瓶颈后处理。未擅自放宽Broker限制或在失败后继续新一轮压力。

## 实现、验证与复现

契约[phase7q](../../contracts/phase7q-multiprocess-load.md)先冻结。
独立worktree的实现提交dd11cbc、质量提交421794c分别集成为e995bed、9b672b6；
编排脚本e241bf8。探针新增每步数量、前缀、offset、共同开始/PING、原始样本参数，
修复同主机同秒默认Client ID碰撞；保留旧CLI行为。Broker代码和依赖未变。

58项loadgen测试、fmt、严格all-target Clippy通过；两个独立黑盒用例通过，其中
四个真实本地探针进程连接三个MQTT stub，验证ID隔离、分流、共同保持及原始百分位。
另一个拒绝用例验证失败样本计数。黑盒并非Broker互操作验收；实际Broker结果如上。
独立Agent只读复算了实际四实例原始结果，没有再次施压。

- `run.py`编排远程单次测试；再次运行前必须使用全新BASE/OUT，不覆盖本轮数据。
- `client.py`启动四实例；`aggregate.py`校验配置/样本并聚合；`summarize.py`生成资源与阶段汇总。
- 从raw/client.tar.gz及raw/server.tar.gz解压得到client/、server/后，可离线复算；
  该操作不访问服务器，不启动负载。
- [聚合结果](aggregate.json)、[资源](resource-summary.json)、[阶段原文](stage-summary.json)、
  [验证记录](validation.json)；raw/保留日志、原始延迟、命令、状态和停止证据。
  verification/保留本地测试、Clippy与交叉编译日志，不含令牌或密钥。
