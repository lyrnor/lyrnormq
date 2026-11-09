# Kafka 出站

Dashboard 的「规则与出站」中新建出站，类型选择 Kafka，填写名称、Broker 地址和
目标 Topic，再将出站绑定到 SQL 规则。Broker 使用 `host:port` 或 `[IPv6]:port`，
支持多个地址；Topic 必须预先创建并配置合适的副本数和 min.insync.replicas。
规则与 Webhook 共用现有 SQLite 配置库和分段追加队列，无需额外日志目录。

安全协议可选普通连接、TLS、SASL、SASL+TLS。SASL 支持 PLAIN、SCRAM-SHA-256、
SCRAM-SHA-512；TLS 校验证书与主机名，可填 CA 和成对的客户端证书/私钥，支持
私钥密码。密码和私钥读取时显示占位符，保留占位符可继续使用原凭据。
配置库包含凭据，应限制访问并保护备份；凭据不会写入浏览器持久存储。

高级设置默认折叠：默认并发128、单次投递超时30秒、最多尝试3次、退避基数200ms、
LZ4 压缩、聚合等待1ms、聚合大小64KiB。并发范围1–512，所有 Kafka 出站共用512个
尝试槽，独立于 Webhook 的16个 HTTP 槽；最多启用8个 Kafka 出站。系统管理有界
缓冲区。引擎每次投递一条，Kafka 生产者内部可以聚合，不会因一个 Client 重试
阻塞其他 Client。

Kafka value 为 SQL 转换结果的 JSON，不含 Webhook 的外层封装。默认 key 是来源
MQTT Client ID；可在高级设置指定 SQL 结果中的顶层字符串字段，缺失或类型错误
会终止该条投递。相同规则、出站、Client 的尝试保序；不同 Client/规则可以并发。
更换 key 或 Topic 分区数会改变映射，不能保证跨分区或全局顺序。

记录带有 `lyrnormq-delivery-id`、`lyrnormq-event-id`、`lyrnormq-rule-id`、`lyrnormq-client-id`、
`lyrnormq-topic` headers，以及原始事件时间。单条 value/key/headers 含保守开销上限1MiB。
消费者可按稳定的 delivery ID 去重。

系统等待 Kafka 成功确认后才持久化完成标记。失败重试只阻塞当前规则/出站/Client，
最终失败后继续下一条。成功确认和本地完成记录之间崩溃可能重复发送；启用生产者
幂等和全部同步副本确认也不代表跨进程 exactly-once。Kafka 的实际耐久性还取决于
Topic 副本和服务端设置。重启会恢复已落盘的未完成投递和重试状态。

规则入口仍为有界 best effort，MQTT ACK 不等待出站落盘；入口过载或磁盘配额耗尽
可能拒绝新事件。分段队列刷盘后才进入出站投递。不要把它理解为 MQTT 到 Kafka
端到端无损事务。未实现动态 Topic、事务、Schema Registry、OAuth 或 Kerberos。

本地编译 Linux amd64 musl 静态版本（需 Rust 目标、Zig、cargo-zigbuild、CMake、Node）：

```sh
./scripts/build-linux-amd64-musl.sh
```

原生 Kafka 依赖随二进制静态链接。MQTT TLS 仍使用现有 rustls；Kafka TLS 由 Kafka
客户端的原生库处理。真实 Kafka、重启、崩溃和安全模式验收见
[验收记录](verification/kafka-outbound-20261102/README.md)。
