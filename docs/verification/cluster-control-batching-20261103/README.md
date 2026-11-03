# Phase7o：控制批处理与会话获取恢复

本轮修复 Phase7n 已复现的两条故障链：大量独立 Raft 写入阻塞完成通知，
以及 Acquire 回复丢失后重新获取 epoch、反复关闭连接。契约见
[phase7o](../../contracts/phase7o-control-batching-recovery.md)。

## 实现与审查

- Controller 只有一个提交 worker，FIFO 聚合已经就绪的请求，每批最多64条、
  编码512KiB；不等待凑批。前批完成耐久 quorum/apply 后才提交后批。
- 每条命令独立保留结果和 epoch，合并稀疏元数据更新；失败子命令不回滚其他命令。
  一个 Raft entry 不再等于一个管理请求，验收检查实际业务状态。
- AcquireOnce 在 owner 中记录前驱 epoch，相同身份重试返回原 epoch。
  Actor 保留未知结果的获取身份，后台恢复经同一 FIFO 执行；已经恢复的健康连接
  不再被迟到的恢复任务抢占。
- 同 epoch 恢复保留 physical_revision，Filters 屏障之后再次核对 authority。
  Host/system ingress 复用同一恢复路径，取消的调用不取消已提交工作。

实现分别来自独立 worktree：控制 `154985f`、运行时 `4e1b35b`、质量
`fbbdbc3`/`2a6a1f4`/`0336d37`，已统一集成到 `9c2798e`。
控制与运行时作者互相只读审查，主控审查并运行集成验收。
没有修改 OpenRaft、提高超时、削弱 W2/ReadIndex/all-live 屏障或重新引入容量拒绝。

## 已完成的可复现验证

- 三项先失败后修复的回归：丢失 Acquire 回复后身份改变；同 epoch 物理修订号9
  被重置为0；Filters 屏障期间发生接管后仍安装旧缓存。
- 受控 RPC 覆盖重复屏障失败、CAS 冲突、foreign owner、Host、取消回复、延迟恢复。
- 真实单节点 Raft fixture 中514个已入队并取消回复的请求全部应用，使用9条耐久
  Raft 日志；阻塞状态机应用时没有提前回复。这是批处理证明，不是吞吐量基准。
- committed batch 重放、未提交尾部忽略、AcquireOnce 重复、快照/purge 后恢复。
- 工作区1047项 Rust 测试通过，1项外部 PostgreSQL 测试跳过；fmt、严格
  `cargo clippy --locked --workspace --all-targets -- -D warnings` 通过。

36项真实进程测试全部通过：新增功能/控制恢复2项、事件驱动故障10项、内存副本
恢复17项、增量控制恢复1项、WAL故障及快照恢复6项。新测试直接查询重启后的目标
Controller，并证明该节点停机时查询失败，避免种子回退掩盖未追平的副本。

相同900连接诊断脚本结果：

| 指标 | 修复前 | 修复后 |
| --- | ---: | ---: |
| CONNACK 成功 | 0/900 | 900/900 |
| 保持1秒后 PING 存活 | 0/900 | 900/900 |
| 成功连接平均/p99 | 无有效样本 | 117.883/154.508ms |
| 实际启动延迟 p99 | 2651.85ms | 9.984ms |
| ramp 与 drain 总时间 | 3.2065s | 1.0328s |

三节点各300连接，新运行无客户端重试，进程全部清理。旧运行期间有其他编译负载，
因此只把失败→成功作为回归证据，不以此计算吞吐提升倍数。新运行独占本轮测试窗口；
计划1000/s、Python384 worker，实际启动延迟已单列。此处延迟是TCP连接至CONNACK，
不是消息投递延迟，也不是Linux服务器容量结论。节点1有一条启动阶段 peer3
requesting-vote transport error，未见本次Acquire/control结果失败；不宣称日志零错误。

详细机器可读结果和日志见 [validation.json](validation.json)、
[修复前](old900/summary.json)、[修复后](new900/summary.json)。
服务器首个1000/s→10000连接 cohort 已通过，10000成功且保持10秒后全部PING正常。
建连均值175.323ms、p99319.228ms，具体资源及两个尾部Raft超时见
[服务器报告](../../performance/cluster-control-batching-20261103/README.md)。

## 兼容性边界

旧 singleton WAL 和没有 acquired_from 的 owner/checkpoint 保持可读。写入新的
Batch/AcquireOnce 前需要一起升级全部 Controller，旧二进制不能读取新命令。
数据副本仍为内存模式：丢失全部副本的内存不能从控制元数据恢复会话正文。
