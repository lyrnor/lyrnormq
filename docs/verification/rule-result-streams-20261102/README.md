# 规则结果共享日志验收（2026-11-02）

契约：[phase5g](../../contracts/phase5g-rule-result-streams.md)，冻结提交 `d9442fc`。
存储、运行时、独立测试分别在三个独立 Git worktree/分支实施，主控统一集成。
存储提交 `c75676e`、`41164e9`；运行时 `84c95af`；独立测试 `68da513`、`377f568`。

## 最终行为

每条规则对一条输入求值一次，转换后的正文只保存一份，并原子保存所有获准的出站引用。
不同规则形成逻辑流，共享物理分段日志；未被 SQL 选中的原始字段不会额外写入日志。
每个消费者独立保存投递 ID、目标版本、失败次数、重试时间和不确定的进行中状态。
最后一个消费者终结后，正文才可回收；物理空间在检查点/压缩时回收。

保序键为 `(出站, 规则, 精确 Client ID)`，重启后仍保持。同一 Client 的不同规则可以
并发，批次不混合规则或 Client；每个出站默认 8、全局 16 个 HTTP 尝试的上限保持不变。
重试只阻塞当前消费者所在的队列。Webhook 请求格式及稳定投递 ID 不变，公共
RawValue 编解码恢复逐字节一致的请求封装。MQTT ACK 不等待出站持久化或 HTTP。

`pending_records/recovered_records` 统计未完成投递引用，`pending_results` 统计唯一
未完成结果，`pending_bytes` 按唯一正文计一次。例如一个结果发往 X/Y：两次投递、
一个结果、一份正文大小。Dashboard 已调整为“待完成投递”，并说明计数单位。

## 恢复与升级

v2 写入支持 v1/v2 恢复。启动时，在新入队和领取投递前，将 v1 记录原子迁移到新一代
日志；仅合并同规则、事件、Client 且正文完全相同的记录，保留尚未完成消费者的旧 ID、
重试和进行中状态。损坏或冲突记录拒绝迁移。检查点/迁移会重建旧 API 索引及计数，
已用失败测试复现并修复旧索引残留问题。旧公共 API 保留，生产路径及基准使用共享 API。

接近容量上限的 v1 队列可能需要提高 `--egress-queue-max-bytes`，因为正文和引用的
计费方式发生变化；发布新 CURRENT 前遇到 Full 会保留旧数据。测试验证提高容量后可迁移。
旧版本不支持降级读取 v2：实际旧 Broker 已验证拒绝启动，所有日志文件 SHA256 不变
（`downgrade-final.log`）。升级前应停机备份配置 SQLite 和日志；降级需恢复备份。

## 验收结果

- Workspace fmt、严格 Clippy 通过，690 项 Rust 测试通过，包含 43 项日志和 46 项出站测试。
- 83 项前端测试、TypeScript/Vite、Prettier 和嵌入前端的 Release 构建通过。
- 独立基准的 3 项测试、fmt、Clippy、Release 和 64 条记录读写冒烟通过：
  `pending_results=64`、`pending_records=64`，64 份正文全部核对成功。
- 132 项 Release 黑盒测试通过：共享流 13、耐久队列 12、并发 10、Client 保序 9、
  规则/Webhook 18、Dashboard 14、会话持久化 14、WS/WSS 18、TLS 12、准入/mTLS 12。

新测试通过不同 SQL 分别提取温度和湿度，验证 A→X/Y、B→X 时，磁盘上每份结果正文
各出现一次，原始输入独有标记不存在，引用/结果计数为 3/2。覆盖 A/X 阻塞或重试时
B/X 与 A/Y 继续发送、批次隔离、规则内 FIFO、终结 X 后仍为 Y 保留正文、部分 ACK 后
SIGKILL 恢复、规则修改/删除后保留已保存结果，以及容量不足时整组扇出原子拒绝。

v1 测试夹具先由实际旧 Broker 验证，再验证稀疏 ID、部分完成、重试期限、有限失败预算、
不确定尝试及损坏/冲突记录的升级行为。功能测试观测到并发上限为默认 8、全局 16、
配置 3；16 次请求复用 8 条 TCP 连接。这些是功能边界验证，不是生产 TPS/p99。

完整性能矩阵尚未重跑，旧性能报告属于 phase5f 历史基线。测试服务均为临时实例，
没有重启用户 Broker，也没有合并 main 或部署。jsdom scrollTo、Radix 构建及 Python
临时目录清理提示不影响测试结果。

## 复现

```sh
(cd dashboard/web && npm test && npm run build && npx prettier --check src tests)
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
cargo build --release --offline -p rmqtt
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/rule_stream_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_durable_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_client_order_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_concurrency_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/rules_webhook_blackbox.py -v
CARGO_TARGET_DIR="$PWD/target" cargo test --offline --manifest-path benches/egress-log/Cargo.toml
```

环境：Darwin arm64 / Apple M4、16 GiB、Rust 1.98.1、Node 24.12.0。SIGKILL 测试
验证进程崩溃恢复，不代表物理断电验证。剩余工作：新模型 Linux 性能基线及队列 profiling；
ACK 耦合准入、死信重放和复制队列仍为独立后续工作。
