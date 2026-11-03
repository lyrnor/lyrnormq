# 集群副本修复与空盘替换

本阶段采用**维护窗口修复**，不是在线增量复制。修复期间暂停耐久发布、连接接入和
普通副本投票；控制面租约、状态查询和修复 RPC 继续运行。要求三个 Controller 有
多数派、两个原始数据盘可读。数据副本组和地址保持不变。

## 操作顺序

1. 停止故障目标 Broker，保留其原盘。等待旧节点租约到期，确认控制面的数据节点数
   已减少；不能在旧目标仍持有有效租约时替换磁盘。
2. 为替换目标准备配置：沿用 cluster_id、node_id、advertise、controllers、durable
   组和 peer 认证/TLS，data_dir 指向新的独立目录。不要复制或改写旧 disk_id。
3. 启动 learner。它只开放认证后的私有修复 RPC，不注册普通数据副本，也不启动
   MQTT、Dashboard 或规则配置数据库。

```sh
./rmqtt cluster-repair-learner --cluster-config broker103-replacement.json
```

在另一终端执行修复。配置可以引用现有 Controller 或 donor 的集群配置，使用既有
peer token；目标 ID 必须属于已经冻结的三副本组。

```sh
./rmqtt cluster-repair --cluster-config controller1.json --target-node 103
./rmqtt cluster-status --cluster-config controller1.json
```

协调器持久冻结两个原盘，流式合并最高完整版本和最大 min_epoch，校验 SHA256 后
分批导入新盘。会话/QoS 状态、转发 outbox、去重水位及逻辑会话代际都随记录复制。
导入安装完成仍不能投票，必须先取得 Controller 修复提交并完成本地代际恢复。

命令返回 state=committed 后，停止 learner，用**同一替换目录和配置**启动正常
Broker，并沿用原 MQTT/TLS/认证启动参数。正常启动会根据控制面的终态验证并恢复
Journal；随后该新盘才能参与多数派确认。原旧盘再返回也不能用旧身份投票。

## 中断与安全中止

协调器或 learner 退出不会自动解除维护冻结。恢复进程、保留同一 data_dir 后，
重复同一个修复命令即可继续；已提交页面按序号和内容摘要幂等处理。

若目标容量不足、需要放弃此次目标盘，执行：

```sh
./rmqtt cluster-repair-abort --cluster-config controller1.json
```

中止只恢复两个原盘的 W2 服务。目标继续被排除，不能以空盘或未追平盘充数。
恢复后再停掉一个原盘，应明确拒绝新的耐久确认。已完成的修复不能撤销；提交后
协调器崩溃时，正常节点可根据持久终态自行解除相应冻结。

状态输出的 repair/last_repair 包含任务 ID、目标、代际、是否封存和终态。
修复前滞留的普通 RPC、旧修复令牌和旧磁盘身份都被拒绝，不能通过重启绕过。

## 范围与预算

分页最多 64 条、12 MiB；单记录 8 MiB、记录正文总额 256 MiB。Manifest 字节数
还包含 key 和 JSON 包装，不等于记录正文配额。Staging 有独立有限预算，SQLite
记录加 staging 页上限约 768 MiB，WAL 和文件系统开销另计。合并结果过大时完整
拒绝，不能截断后当作修复成功。

本阶段不改变成员拓扑、不做并发在线追日志、不清理 tombstone，也不增加集群
Retained、Will 或共享订阅支持。单台 Linux 上的多进程故障注入不代表跨机器高可用
或物理断电认证。契约见 [Phase7c](../contracts/phase7c-replica-repair.md)。
