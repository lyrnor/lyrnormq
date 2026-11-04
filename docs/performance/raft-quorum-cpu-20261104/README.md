# Phase7u：quorum RPC 生命周期与 CPU 优化

## 修改和验证边界

契约：[phase7u-quorum-cpu.md](../../contracts/phase7u-quorum-cpu.md)。三个真实
Subagents 在独立worktree/分支实现、测试和审查，主控统一集成。

- RF3/W2改为一个owned driver轮询三份peer future，减少原先每操作三次spawn。
  前两个成功响应组成同一quorum并选择版本/检查同版本正文冲突；同一poll同时
  就绪按配置顺序消费，不声称具有外部精确完成时钟顺序。
- W2决定后立即回复，第三份响应不能改写结果。其余已接受工作继续执行，tail期限
  为min(决定+3s,开始+8s)，保留原1s连接/2sRPC期限；调用方取消不会取消owner。
  每个peer的poll保留unwind panic隔离。超时仍可能主动取消，不能保证任意网络零reset。
- 原Request move到本地副本；借用请求只编码一次，两个远端共享不可变Bytes。
  protobuf bytes字段/tag、JSON payload、MAX_WIRE限制及durable schema不变。
  Bytes入站可复用接收slice，最终出站protobuf编码仍可能复制。
- cluster/Bearer metadata初始化时预计算，逐次请求clone，仍逐请求校验token字节和
  cluster绑定。没有调整channel cache、访问策略、配额、四批写流水线或ACK条件。
- 原JoinSet在第二票后Drop会abort第三任务，h2可能发送CANCEL，reset限制告警会
  触发GOAWAY。受控真实HTTP2代理在旧二进制复现此路径；不把全部43条旧告警
  都归因于quorum，Raft等调用仍可能取消RPC。

RPC实现c23f996（agent b774446），quorum实现694bfbb（agent cac4e11），
独立进程回归71080f0（agent1978b3e）；契约2809bf0、指标6e848a0。

## 方法

基线是[Phase7t](../raft-pipeline-20261104/README.md)。Broker47.84.224.186，
private172.16.240.180；压测机47.84.241.200。三双角色进程共处8逻辑核/约14.96GiB
虚机，每进程4Tokio workers；四probe各500/s×2500、2workers、2048inflight，
相同端口/限额/脚本/探针哈希，统一35秒PING，无重试。只跑一次2000/s10k。
本地cargo zigbuild optimized+debuginfo amd64musl，服务器不编译，49Hzperf使用
CLOCK_MONOTONIC，RMQTT_RAFT_PROFILE=1。新增固定数值型quorum指标亦有诊断开销。

CPU同时报告/proc完整相邻区间平均核数和原始ticks包围窗口的工作量。
后一方法选最后一个开始前样本至首个排空后样本，相同PID、ticks/hz；不把部分
区间的平均核数乘以完整ramp时间。还在两个版本都增加3秒tail allowance后取首个
包围样本，避免把尾请求的CPU移出统计。每种窗口分别披露前后静稳余量；这些值
含余量，不称作精确纯建连CPU。连接p99与业务消息p99分开；单轮不证明容量上限。

已有musl ELF含用户CFI，服务器perf编入libdw/dwarf-unwind且采集了REGS_USER/
STACK_USER。旧数据导出明确报“address range overlaps an existing module”，因此
用户栈问题不能归因于缺debuginfo。本轮保持同样构建参数；泛型memcpy/alloc的
业务来源在缺少有效用户调用链时仍不确定。诊断日志保存在verification目录。

本轮静态ELF253877776bytes，SHA256
`eb6f4be285dc7b8af33e820de4f8b4bed64772c9c6961a581b3423c72f4b55b4`，
无interpreter或shared-library依赖。探针SHA256仍为
`556b3d560b3c79a4c77e9f06edc93052a876f6fb8a4be86eeaf163f70784b4ad`。

## 正确性验证

- 旧JoinSet注入回归失败，新13项quorum测试通过：W2后第三请求仍存在/正常完成、
  caller取消、tail/overall绝对期限、重复wake不能续期、同版本冲突、版本/None选择、
  迟到高版本/冲突不改结果、单peer panic隔离、一次借用编码与共享正文。
- 6项RPC测试通过：旧/新protobuf双向字节兼容、clone同正文地址、入站解码共享
  接收slice、16MiB边界/恶意JSON、共享认证分配与错误token/cluster拒绝。
- 主控集成220cluster+203transport=423项测试、workspace严格all-targets Clippy、fmt通过。
- 优化版release、profiling开启，8项真实进程测试通过34.781秒。包括第三副本gate
  下W2早回复/无成功取消RST、双版本QoS0/1/2与顺序、跨owner未确认QoS2恢复、
  leader故障/重启/离线恢复、recipient generation及系统topic边界。
- 新的真实HTTP2排空回归在旧release上明确失败；原有两项兼容测试在旧release通过。
  所有验收都由实际协议应答确定，不用未确认请求推断已提交。

## 实测结论：CPU目标未达到，按用户要求暂停

同一组2000/s10k全部10000成功/PING，零失败；平均364.028ms、p99550.442ms，
旧组365.662/527.243ms。CPU完整区间平均6.951→6.988核；完整ramp包围CPU时间
36.82→36.48秒，每连接3.682→3.648ms；含3秒tail allowance的包围CPU时间
37.15→36.99秒（约下降0.43%），每连接3.715→3.699ms。两组静稳余量不同、
单次测量无方差估计，不能将其表述为显著CPU改善。RSS峰值1256.914→1261.773MiB。
本轮h2 reset-limit告警为0，旧ramp43条/清理4条；这证明该组行为修复的效果，
不代表所有CPU开销均来自取消RPC。首个包围窗口前/后余量：旧616.305/928.849ms、
新970.130/309.001ms；tail窗口旧616.305/63.991ms、新970.130/542.665ms。

CPU没有明显下降；用户要求先暂停CPU优化，未实施后续barrier分组设想。
下一任务为2000/s→100000连接并保持至少300秒，独立新目录/契约记录。
旧raw数据、最终状态和完整Raft/CPU计数保留在本目录，不能把连接通过当作CPU目标通过。
