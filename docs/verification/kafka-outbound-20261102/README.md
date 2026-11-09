# Kafka 出站集成验收（2026-11-02）

先通过 rdkafka 静态交叉编译探针，再冻结
[phase5j 契约](../../contracts/phase5j-kafka-outbound.md)，随后三个真实 Subagent
在独立 Git worktree/分支实施后端、React 配置和独立真实 Kafka 验收。主控审查后
按依赖 cherry-pick 集成到 `codex/kafka-outbound`，未合并 main、未部署服务器。

后端工作区 `/private/tmp/lyrnormq-kafka-backend`，UI 工作区
`/private/tmp/lyrnormq-kafka-dashboard`，质量工作区 `/private/tmp/lyrnormq-kafka-quality`。
集成实现：`a590af6`、`646d07c`；UI：`0c8a199`；质量：`9054992`、`21f34c1`、`1fbbd1a`。
旧绑定字节兼容修复：`975b969`（独立 RED → GREEN）；嵌入测试夹具隔离修正：`1822825`。
构建探针和冻结契约：`e8779df`；构建工具/生命周期边界：`ef0e2f4`。

## 构建与环境

[环境记录](environment.json)：本机 macOS27 arm64、Rust1.98.1、Zig0.16.0、
cargo-zigbuild0.20.1、CMake4.4.2、Node24.12.0。rdkafka 固定0.39.0，原生
librdkafka2.12.1；Tokio、CMake、静态 curl/zlib/zstd、vendored OpenSSL。

探针保留两条真实失败路径：无 curl 的 CMake 构建遇到上游 OIDC 宏/头文件条件；
configure/make 误用宿主编译器。完整 vendored 特性集成功；同特性宿主探针确认
TLS、PLAIN/SCRAM、LZ4/Zstd/Gzip 能力。见 [探针](build-probe)。没有绕过失败隐瞒依赖。

完整嵌入 React 的 Broker 也通过 `cargo zigbuild --locked --offline --release
--target x86_64-unknown-linux-musl -p lyrnormq`。最终
[ELF 检查](cross-elf-final.json)记录大小、SHA256、无动态解释器/共享库依赖。
Linux 产物仅编译/链接/静态检查，未在 Linux 上执行或部署，不声称运行验证通过。
原有锁定包没有升级/移除；Cargo.lock 仅增加 Kafka 所需依赖。

复现最终构建：

```sh
./scripts/build-linux-amd64-musl.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

## 实际测试结果

| 验收 | 最终结果 | 证据 |
| --- | --- | --- |
| Workspace Rust | 743通过、0失败 | [完整日志](rust-final.log) |
| fmt / 严格 workspace Clippy | 通过 | [fmt](fmt-final.log)、[Clippy](clippy-final.log) |
| React 单元/组件 | 96通过，TypeScript/生产构建通过 | [测试](ui-tests.log)、[构建](ui-build.log) |
| 真实 Apache Kafka | 14通过，65.385秒 | [日志](kafka-real-final.log) |
| 共享流/历史升级 | 14通过 | [日志](shared-stream-regression.log) |
| 耐久恢复/同Client保序/HTTP并发 | 12 / 9 / 10通过 | [耐久](durable-regression.log)、[顺序](order-regression.log)、[并发](concurrency-regression.log) |
| 规则/Webhook、管理HTTP | 18 / 14通过 | [规则](rules-regression.log)、[管理](dashboard-regression.log) |
| 复制二进制、无前端文件/Node运行 | 4通过 | [日志](dashboard-embedded-regression.log) |
| 嵌入React的宿主Release | 构建通过 | [日志](native-build-final.log) |
| Linux amd64 musl Release | 交叉编译及静态ELF检查通过 | [构建](cross-build-final.log)、[ELF](cross-elf-final.json) |
| 独立Kafka helper | fmt / Clippy / test通过 | [fmt](helper-fmt.log)、[Clippy](helper-clippy.log)、[test](helper-tests.log) |

Rust 数量已包含65项 egress 和11项 Kafka，不重复相加；真实 Kafka 与 Python 黑盒
是独立进程验收。Python原有回归含新升级回归共81项，加真实Kafka14项共95项。
嵌入测试夹具显式将默认启用的规则数据库放到运行目录之外，保留空目录/无解压
断言并验证规则服务运行；保留修正前的 [失败证据](dashboard-embedded-fixture-red.log)。
CI 已新增，但尚未在远端运行，不将本地结果冒充 CI 运行记录。

## 行为与故障审查

- 复用规则结果共享正文日志、独立消费者引用、稳定 ID、Begin/Retry/Complete 和
  Group Commit；没有按 Kafka 出站复制日志或改变 MQTT ACK 路径。
- 默认 Client ID key、原始 SQL JSON value、固定 Murmur2 分区、稳定身份 headers。
  同规则/出站/Client 一次只尝试一条，其他 Client 并发；重试结束再推进当前 lane。
- 每出站共享一个有界生产者。全局 Kafka 尝试512，与 HTTP16隔离；默认单出站128。
  仅 Kafka delivery report 成功后才写本地完成；入 native 队列不算投递成功。
- 原生创建在 blocking 初始化路径，最后一个 producer 引用由有界 owner 线程持有，
  异步 shutdown 等待该线程完成。先取消/收敛旧任务和 native producer，再启用新代。
  对不显式 await shutdown 的直接 Runtime Drop 仅异步安排清理，不提供同步返回屏障。
- 审查发现并以 RED 回归修复：并发 shutdown 提前销毁最后引用、generation 残留
  Webhook 连接池，以及 delivery 字段转 Value 改变旧绑定 descriptor 字节导致恢复失败。
  旧字节相等校验和磁盘 schema 未放宽。RED 日志随本报告保留。
- Dashboard 复用组件，支持类型/目标/安全协议及条件凭据，调优折叠、自动 ID、隐藏
  系统队列和可靠性内部选项。真实浏览器完成创建、保存、重载和取消编辑；见
  [浏览器检查](browser-checks.json)，使用独立临时 Broker，未动用户8766服务。

## 真实 Kafka 与可复现边界

独立 fixture 使用 SHA 校验的官方 JRE21.0.13+9 与 Apache Kafka4.2.2，见
[下载清单](fixture-manifest.json)。单 broker/controller KRaft、256MiB Java heap、
3分区、RF1、minISR1、逐消息 flush；临时 loopback 明文/TLS/SASL/mTLS 监听。
测试后进程停止、临时数据和凭据删除；缓存工具包留在临时目录，不安装系统服务。

真实 Kafka 黑盒覆盖 MQTT3.1.1/5 与 QoS0/1/2、key/value/headers、同 Client FIFO、
多规则/多出站共享正文、HTTP 槽饱和隔离、Kafka 故障重试、正常重启、SIGKILL 未确认
重放和部分进度恢复、PLAIN/SCRAM256/512、TLS 错 CA、错误密码、强制客户端证书、
加密私钥/占位符保存、缺证书拒绝、key 类型错误最终失败和配置原子性。

精确的「远端成功→本地 Complete 故障」窗口另由可注入 Sink 单元故障测试验证；
真实 Kafka 的部分进度 SIGKILL 不声称每次恰好命中那个毫秒窗口。允许相同稳定 ID
重复，消费者须去重。没有认证多副本故障、物理断电或端到端 exactly-once。

复现真实集成：

```sh
python3 tests/kafka_fixture.py --download
CARGO_TARGET_DIR="$PWD/target" cargo build --release --locked --manifest-path tests/kafka-client/Cargo.toml
LYRNORMQ_BIN="$PWD/target/release/lyrnormq" \
KAFKA_CLIENT_BIN="$PWD/target/release/lyrnormq-kafka-test-client" \
python3 tests/kafka_outbound_blackbox.py -v
```

测试全部使用临时凭据、消息和端口，不能指向生产集群。此轮为功能/恢复验收，
没有进行吞吐/p99基准，也没有虚构性能指标。后续需隔离 Linux/Kafka 多副本实测。
动态 Topic、OAuth/Kerberos、事务和 Schema Registry 未实现。
