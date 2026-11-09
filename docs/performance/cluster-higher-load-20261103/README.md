# 更高建连负载与消息矩阵：首档未通过

**2000/s→10000计划连接未通过；按预先冻结的首失败停止规则，4000/s、消息QoS0/1/2
和递增10万连接均未运行。消息TPS、消息延迟及丢失量没有本轮有效测量值。**

## 场景与实际结果

沿用Phase7o修复版Broker87ced3e8和原Rust探针；没有重新编译、改超时、改耐久性或
放宽Broker限制。两台8逻辑CPU Intel Xeon Platinum服务器，Broker约14.96GiB RAM，
三进程各4Tokio worker、每进程同时运行Controller/data角色。Broker47.84.224.186，
压测47.84.241.200，通过172.16.240.180的18831/18832/18833轮转建连。
新数据目录 `/opt/lyrnormq-higher-load/20261103/c2000`，前次元数据未复用。

| 指标 | 实测 |
| --- | ---: |
| 计划到达速率 / 计划连接 | 2000/s / 10000 |
| 实际发出 / CONNACK成功 | 7009 / 7009 |
| 未发出 | 2991 |
| 已发出连接失败 / PING失败 | 0 / 0 |
| 保持10秒后存活 | 7009 |
| 成功建连平均 / p99 / 最大 | 1431.223 / 1867.884 / 1906.529ms |
| 已发出请求的调度延迟平均 / p99 | 0.803 / 1.650ms |
| ramp与drain总时长 | 6.131s |
| 该窗口CONNACK完成速率 | 1143.251/s |
| 三进程建连CPU平均 / 采样峰值 | 6.936 / 7.091核 |
| 保持及PING阶段CPU平均 | 0.134核 |
| 三进程合计RSS采样峰值 | 984.910MiB |
| 压测进程全窗口CPU平均 / RSS峰值 | 0.062核 / 6.766MiB |

CPU平均按`/proc` tick和单调时间差分；阶段边界以客户端启动墙钟+ramp/drain近似划分。
建连统计含6个资源样本/5个完整间隔，保持阶段10个样本/9个间隔。完整客户端窗口
17.014秒，包含保持与收尾。表内连接延迟只包含成功的7009次，2991次未发出另列。
1143.251/s是这个受限到达实验的完成率，不是测得的Broker最大建连能力。

停止前直接查询三个Controller，三者均term1/leader1、last_applied485、7009个owner。
owner计数不是在线连接计数，存活由逐连接PING确认。全部Broker和压测进程已停止。
未进入cscale，未更改源端口范围，仍为32768..60999。

## 失败原因与证据边界

直接失败原因是Rust连接探针的2048个并发握手槽位占满。绝对到达时刻没有空槽时，
探针计入not_started并跳过，未进行隐式重试。本轮2991次属于未施加到Broker的负载，
不是Broker拒绝的连接，更不是丢失的MQTT消息。工具exit0仅表示完成运行，
plateau_passed=false和严格外层判据正确停止了整个矩阵。

服务器响应变慢使在途请求长期占槽；压测进程CPU很低，已启动请求调度p99约1.65ms。
控制写入等待是下一步重点，连接ramp窗口中leader日志的**最高已记录样本**为：

| 阶段 | 采样数 | 最高观测 |
| --- | ---: | ---: |
| Controller Write Acquire | 13 | 837.722ms |
| Controller Write Mutation | 13 | 878.392ms |
| Broker TakeMutate | 12 | 842.407ms |
| 单批 RaftResult Mutation | 4 | 31.572ms |
| WAL append | 2 | 7.386ms |
| ReadIndex Authority | 12 | 14.782ms |
| TakeBarrier | 12 | 46.968ms |

这些是指数抽样，非全量分布，也不是同一请求的配对时间，不能相减得到精确队列耗时，
不能称为阶段p99。结合单worker批量提交结构，控制写队列积压是有证据支持的排查方向；
具体CPU热点、平均批大小、队列驻留时间与耐久IO占比仍需直接profiling。
窗口末尾另有两条150ms AppendEntries timeout，原始日志保留；没有CONNACK/PING失败
或任期变化。没有声称整份日志零错误。

## 后续与复现

优先补充控制写队列驻留、批大小/批完成率和CPU profile，区分串行提交排队与编码/
状态应用/同步开销。当前测量不能推导单纯扩大2048握手窗口后的成功率。后续消息
QoS0/1/2及更高建连档仍需实际执行，本轮没有把它们标记为通过。

契约：[phase7p](../../contracts/phase7p-higher-load-acceptance.md)。run.py/client.py
固定本轮矩阵、校验三种二进制SHA、按场景建新目录、失败即停并采集证据。复现须修改
BASE/OUT为全新目录；不要覆盖本轮数据。生产源码未变，沿用上一轮已通过的完整
正确性验收；本轮仅核验新Python脚本语法、连接成功/失败判据并由独立Agent只读审查。

结果：[c2000-report.json](c2000-report.json)，资源：[resource-summary.json](resource-summary.json)，
阶段原文：[stage-summary.json](stage-summary.json)，未执行项：[validation.json](validation.json)。
raw/保留服务端、客户端日志与资源压缩包、状态查询、停止记录和编排日志，不含令牌和密钥。
