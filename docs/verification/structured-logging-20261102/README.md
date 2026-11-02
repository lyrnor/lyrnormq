# 结构化运行日志验收（2026-11-02）

在独立分支 codex/structured-logging 接入 tracing0.1.44、tracing-subscriber0.3.23、
tracing-appender0.2.5。仅改协调者拥有的bin/rmqtt、依赖锁和文档；没有修改其他模块、
会话或消息磁盘格式。模块初始化仅在进程入口安装一次，crate lib.rs保持模块定义/导出。

默认warn,rmqtt=info，支持RUST_LOG静态target/level过滤和RMQTT_LOG_FORMAT=text|json。
有界8192条非阻塞stderr写入，拥堵丢日志，不与业务耐久队列混用。Guard在Tokio runtime
外，避免正常结束时在async worker执行写出等待。应用生命周期字段不包含消息正文、
密码/私钥/令牌；CLI原始错误诊断保留。尚未给每个业务模块补充日志埋点。

748项workspace测试（含新增2项配置、3项独立进程日志测试）、fmt和严格Clippy通过。
[workspace](workspace.log)、[Clippy](clippy.log)。新增进程测试验证stdout帮助、非法配置
不回显输入、JSON事件/字段/无ANSI、ERROR过滤保持MQTT连接可用、Ctrl-C关闭后的日志
刷新。首条就绪日志顺序先RED后GREEN，见[RED](readiness-red.log)与workspace最终日志。

Release黑盒30项通过：管理14、复制二进制嵌入4、接纳/mTLS12。
[管理](dashboard.log)、[嵌入](embedded.log)、[接纳](admission-final.log)。
原工具环境RUST_LOG=warn隐藏INFO导致旧监督器等待就绪超时，见
[环境诊断](admission-warn-environment.log)；正式接纳验收明确设置
RUST_LOG=warn,rmqtt=info RMQTT_LOG_FORMAT=text。不是放宽TLS或协议要求。

完整宿主Release与Linux amd64 musl交叉构建通过，分别见[宿主](release.log)、
[交叉编译](zigbuild.log)与[ELF检查](elf.json)。Linux文件无解释器/共享库依赖，
只完成构建和静态检查，未在Linux执行/部署。锁文件仅新增17个包，没有升级/移除旧包，
见[依赖检查](dependency-review.json)。本轮没有进行吞吐/p99测量，不声明零性能开销。

复现：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --release --locked -p rmqtt
RUST_LOG=warn,rmqtt=info RMQTT_LOG_FORMAT=text RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/admission_mtls_blackbox.py -v
cargo zigbuild --release --locked --target x86_64-unknown-linux-musl -p rmqtt
```

正常退出尝试刷新；队列满、输出端阻塞、强杀/崩溃可能丢日志。操作说明见
[日志配置](../../logging.md)。本轮不包含文件轮转、热更新或全模块埋点。
