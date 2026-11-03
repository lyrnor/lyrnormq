# 集群内存副本修复与替换

当前以 [Phase7f](../contracts/phase7f-memory-replicas.md) 为准，替代旧 Phase7c 的
数据盘恢复语义。采用维护窗口修复：暂停数据写入、连接接入和普通副本投票，控制面
及修复 RPC 继续运行。需要 Controller 多数派和两个仍存活、保有原数据的 donor。

1. 停止故障目标，等待其租约失效；保留旧目录，不删除 SQLite 数据。
2. 目标沿用原 node_id、地址、三副本组和 peer 凭据；使用独立新目录。
3. 用正常 Broker 命令启动目标，沿用 MQTT/TLS/认证参数：

```sh
./rmqtt --cluster-config broker103-replacement.json --listen 127.0.0.1:1886
```

目标先提供私有修复 RPC，MQTT 接入等待最多 600 秒。另一个终端执行：

```sh
./rmqtt cluster-repair --cluster-config controller1.json --target-node 103
./rmqtt cluster-status --cluster-config controller1.json
```

驱动冻结两个 donor，合并最高完整版本和 fencing 水位，分页导入并核对摘要；安装后
仍不能投票，必须经 Controller 提交修复证明。**保持目标同一进程运行**，提交后该进程
完成注册并开放 MQTT。不要停止 learner 再启动 Broker；旧的独立
`cluster-repair-learner` 命令在内存模式明确拒绝。

目标重启会丢失导入暂存并生成新 boot identity，旧修复任务不能提交。需保持两个
原 donor 存活，安全中止旧任务，再让新目标进程重新修复：

```sh
./rmqtt cluster-repair-abort --cluster-config controller1.json
./rmqtt cluster-repair --cluster-config controller1.json --target-node 103
```

中止不会让空副本获得投票资格。修复驱动单独中断、目标仍存活时，可用原任务重试。
全组三个内存副本丢失后不能从 Controller 元数据恢复消息或会话；不会自动组建空多数派。

分页最多 64 条、12 MiB；单记录 8 MiB，普通数据计费 256 MiB，修复暂存独立有限预算
768 MiB。计费不等于进程 RSS。修复不是在线增量追日志，也不增加集群 Retained、Will、
共享订阅或磁盘崩溃耐久性。未来 RocksDB 后端尚未实现。
