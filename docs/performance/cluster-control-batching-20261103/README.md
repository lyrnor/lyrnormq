# Phase7o 修复后服务器首个建连 cohort

**通过：1000/s 计划建连，10000/10000 CONNECT 成功，保持10秒后10000/10000
PING 正常。没有重试、跳过或失败。** 本轮只验证原先失败的第一个 cohort，
没有运行更高速率、更大连接数或消息吞吐测试。

## 环境与版本

- Broker：47.84.224.186，内网172.16.240.180；8逻辑CPU，约14.96GiB RAM，
  Linux7.0.0-38-generic x86_64。三进程，每个同时运行Controller和data角色，
  各4个Tokio worker，共用服务器CPU和磁盘。
- 压测：47.84.241.200，内网172.16.240.181；8逻辑CPU，独立Rust Tokio探针，
  2048最大并发握手，15秒超时，绝对时间调度，三个端点轮转分配。
- 实现基线9c2798e；本机 `cargo zigbuild --locked --release --target
  x86_64-unknown-linux-musl -p lyrnormq`，未在服务器编译。静态ELF没有解释器或动态依赖。
  SHA256：`87ced3e81da295b4273b2757c8fc4472409d88472e5c77cca77ba472fd28854b`。
- 探针沿用此前二进制，SHA256：
  `d0acf9899b9ea9d34fd68c8a5234b4f741b97bef816987b00b49f4ad6a28c2ae`。
- 沿用原来的会话/状态/报文限制、握手1024、认证任务128、禁用认证速率限制和
  FD250000；没有提高RPC截止时间。具体配置见 [limits.json](limits.json) 和
  raw/server-processes.json.gz，生成脚本为 scripts/cluster_scale_server.py。
- 新部署目录 `/opt/lyrnormq-control-batch/20261103` 和
  `/opt/lyrnormq-control-batch-load/20261103`，旧部署数据保留。

## 实测结果

| 指标 | 结果 |
| --- | ---: |
| 计划/尝试/成功 | 10000/10000/10000 |
| 跳过/CONNECT失败/PING失败 | 0/0/0 |
| 保持10秒后存活 | 10000 |
| 建连平均 / p99 / 最大 | 175.323 / 319.228 / 370.489ms |
| 调度延迟平均 / p99 / 最大 | 0.476 / 1.104 / 9.955ms |
| ramp与drain | 10.125s |
| 该窗口完成速率 | 987.646连接/s |
| 三进程CPU平均 / 采样峰值 | 3.423 / 7.032核 |
| 建连阶段CPU平均 | 6.829核 |
| 保持及PING阶段CPU平均 | 0.206核 |
| 三进程RSS合计峰值 | 1278.168MiB，约1.25GiB |
| TCP ESTABLISHED / FD峰值 | 10000 / 10100 |
| 压测进程CPU平均 / RSS峰值 | 0.081核 / 6.641MiB |

CPU按`/proc` tick差分/单调时间计算，1核即一个逻辑CPU完全忙；不是整机CPU百分比。
总平均使用完整客户端进程21.009s窗口，19个服务器样本、18个间隔。阶段划分使用
客户端启动墙钟+ramp/drain时长近似边界，分别8/9个完整采样间隔，不应当作精确剖析。
RSS是三个进程同一时刻的合计采样峰值。建连计时从TCP开始到CONNACK，不是消息延迟。

收尾时逐个直接查询Controller，三者均term1/leader1且有10000个元数据owner；
last_applied分别667/668/668，为顺序采样且包含继续提交的心跳，不据此声称同一时刻
完全相同。客户端PING才是存活验收依据，owner数量不是在线连接计数。

窗口末尾有两条150ms AppendEntries RPC timeout（14:08:14.068/14.190 UTC），
没有CONNECT/PING失败或term变化。启动/关闭期间另有peer断连日志；没有宣称日志
零错误。建连阶段CPU接近8核机器容量，后续更高速率前仍需测尾延迟和收尾复制尖峰。

## 复现与清理

先运行本地正确性验收，构建上述来源版本和静态ELF，校验探针哈希。配置已核对的
SSH私钥/known_hosts文件后，在deploy.py中选择**新的**远端日期/运行目录；
同步修改reproduce.py、fetch.py、summarize.py的路径。依次运行deploy.py、
reproduce.py、fetch.py、summarize.py。脚本拒绝复用拓扑目录，遇失败停止；
不要直接复用此报告已有数据目录。

本轮所有Broker与压测进程已停止，并按`/proc/*/exe`路径复核。
仅保留二进制、配置与结果，未设置常驻服务。

原始计数见 [report.json](report.json)，资源见
[resource-summary.json](resource-summary.json)，日志审计见
[load-log-audit.json](load-log-audit.json)。原始日志、采样、控制状态和停止记录
保存在raw/，可直接复核。这里不代表2000/4000/s、10万连接或QoS消息性能已经验收。
