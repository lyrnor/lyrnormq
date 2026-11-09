# 日志

Broker 使用 tracing，进程统一安装 subscriber。默认输出 UTC 时间、级别、模块和
结构化字段到 stderr，不写入消息耐久日志，也不生成额外日志文件。stdout 仍保留给
`--help` 和 `export-state` 的输出。systemd/Docker 可采集 stderr 并管理轮转。

```sh
# 默认：Broker INFO，依赖库 WARN
./target/release/lyrnormq --listen 127.0.0.1:1883

# 调整级别；支持 error/warn/info/debug/trace/off 和 target=level
RUST_LOG='warn,lyrnormq=debug' ./target/release/lyrnormq

# JSON 日志事件，便于日志平台采集
LYRNORMQ_LOG_FORMAT=json RUST_LOG='warn,lyrnormq=info' ./target/release/lyrnormq

# 仅错误日志
RUST_LOG=error ./target/release/lyrnormq
```

格式只接受 `text`（默认）或 `json`。RUST_LOG 最长4096字节，采用静态模块/级别过滤，
不支持 span/字段匹配。无效配置启动失败，错误不会回显环境变量内容。
默认没有 ANSI 颜色；JSON 事件包含 timestamp、level、target、fields。

当前接入应用生命周期事件：`management_listening`、`rules_started`、
`mqtt_listening`、`shutdown_requested`、`broker_stopped`、`broker_failed` 和
`signal_failed`。字段仅包含监听地址、传输类型、事件代码和必要的错误种类，不主动
记录消息正文、认证令牌、密码或私钥。CLI 启动失败仍保留原错误诊断；它不是 JSON
事件。这里没有新增逐包/逐连接 INFO 日志，出站每条重试仍通过现有状态/计数查询，
尚未在每个业务模块增加日志埋点。显式打开依赖库 debug/trace 时也应检查其输出。

stderr 写入由 tracing-appender 专用线程完成，队列上限8192条。队列满时丢弃日志，
避免反压阻塞 MQTT 工作线程；它是运行日志，不具有业务消息队列的耐久性。
写入 guard 位于 Tokio runtime 外，正常退出时完成库提供的有限等待刷新；输出端
阻塞、进程强杀或崩溃不能保证全部日志写出。默认 Ctrl-C 走现有优雅关闭流程。

实现位于 `bin/lyrnormq/src/logging.rs`，不在 lib.rs 放业务逻辑。原有会话、outbox、
Kafka 幂等与 ACK 语义没有更改。

库文档：[tracing-appender 非阻塞输出](https://docs.rs/tracing-appender/latest/tracing_appender/non_blocking/struct.NonBlockingBuilder.html)。
