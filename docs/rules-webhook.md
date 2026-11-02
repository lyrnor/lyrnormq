# SQL 规则与 Webhook 出站

原生 SQL 筛选/转换，Webhook 为唯一出站类型。不支持插件、Kafka、PostgreSQL、
聚合/窗口或任意代码。现阶段为 **best_effort 异步旁路**：不等待 HTTP 来确认
MQTT，队列与计数器在内存中；崩溃/队列满可能丢弃规则事件或出站副本。配置会
持久化，出站队列没有耐久 outbox。HTTP 超时可能已被接收方执行，重试可能重复。

## 启动

```sh
./scripts/build-rmqtt.sh
# 默认启用规则引擎，配置保存在工作目录的 rmqtt-rules.sqlite。
./target/release/rmqtt --listen 127.0.0.1:1883 \
  --admin-listen 127.0.0.1:8766 --admin-token-file /tmp/rmqtt-admin.token
```

管理令牌文件按 Dashboard 文档生成；不要放入静态资源目录。规则配置仅使用
SQLite，默认 `rmqtt-rules.sqlite`；`--rules-config PATH` 可指定其他 SQLite 路径。
规则和出站由 Dashboard 管理，保存后以事务提交并应用。
不读取、不导入 `rules.json`，也不支持 JSON 文件存储或导入参数。
独立配置库不修改会话数据库表结构；新数据库及所有权锁文件权限为0600。
配置可能包含 Webhook 凭据，数据库与附属文件均须作为敏感文件保护。
`export-state` 不加载或创建规则配置。保存失败时会暂停规则服务，避免配置状态不确定；
排查后重启即可从已提交配置恢复。此持久化仅针对配置，不提供消息队列持久化。

## SQL

```sql
SELECT client_id, topic AS source_topic,
       payload.device_id AS device_id,
       payload.temperature AS temperature
FROM "devices/+/telemetry"
WHERE payload.temperature >= 80
```

FROM 为一个双引号 MQTT Topic Filter，遵守 `+`/`#` 和系统 Topic 匹配规则。
支持别名、JSON 点路径、WHERE 比较、AND/OR/NOT、IS NULL、简单算术及
COALESCE/LOWER/UPPER。仅支持单条 SELECT；不支持 JOIN、子查询、GROUP BY、
ORDER BY、DML、正则或任意函数。缺失/不兼容字段按 NULL 规则处理，WHERE 仅
true 命中；非法计算返回受控错误。数值有浮点精度边界，设备标识建议用字符串。
SQL 在配置或测试时编译，不在每条消息上解析。二进制消息可按元信息匹配；
读取 payload JSON 字段需合法 JSON。SELECT * 输出事件元信息与解析后的 payload。

规则 SQL 测试只计算结果，绝不发 Webhook。SQL/表达式/输出有大小与复杂度限制。
事件触发自接入消息发布及 Will，出站重传、保留消息回放不触发；QoS1 输入重复
仍可能产生重复事件，不能根据 DUP 简单去重。规则不阻断/修改普通 MQTT 路由。

## 出站

Dashboard 只需管理「规则」和「出站」。创建出站时选择类型，并在同一处配置
目标地址、凭据和发送策略；规则直接选择一个或多个出站，无需先创建连接器。
当前可用类型为 Webhook；Kafka、PostgreSQL、Redis 尚未实现，不能保存为可用类型。

Webhook 基础表单只显示名称、地址和鉴权/请求头。批量、等待、超时和尝试次数
放在默认折叠的“高级设置”，不展开也能保存；新建默认每批1条、等待0ms，逐条发送。需要批量时在高级设置调整；
已有保存的策略不会自动覆盖，展开/折叠也不会重置。队列
容量由默认值管理，不提供表单编辑，已有容量保留。
配置原子替换并校验引用关系。API 返回请求头 `[redacted]`，相同出站 ID 和类型
原样提交该占位符会保留旧值，新建凭据必须提供真实值。URL 不接受用户信息、
查询串或 fragment；鉴权信息放 Authorization 等请求头，禁止自定义传输控制头。

Webhook 以 POST application/json 发送，不跟随重定向，HTTPS 校验证书。
出站各自拥有有限队列，另有总字节和并发限制。默认同一出站最多同时发送8个请求，所有出站合计最多16个HTTP请求。
并发请求数可在高级设置中调整为1–16。同一出站按来源 Client ID 保序，
不同 Client 可并发，不保证全局顺序。请求最多1MiB（含封装），会自动按条数和字节拆批。

请求为 `{outbound_id, events:[...]}`。每个事件包含 event_id、delivery_id、rule_id、
topic、client_id、qos、retain、timestamp_ms 和 SQL 输出 data。同一投递重试保留
delivery_id，接收方据此幂等；event_id 标识源事件，可被多个规则/出站共享。
Webhook 的2xx为成功，网络错误、超时、408、429、5xx有限退避重试，其余响应为
永久失败。其他出站适配器将各自定义错误分类，公共调度器不判断 HTTP 状态码。

通用执行框架位于 mqtt-egress，独立 sink 接口位于 mqtt-egress-core；mqtt-webhook
仅实现 Webhook 配置与HTTP协议，Kafka/PostgreSQL/Redis适配器无需修改 SQL 或通用
调度器。配置仍使用 SQLite，原有 SQLite 中的连接器/动作配置会在启动时校验并
事务转换为新版出站配置，保留动作ID作为出站ID及对应凭据、规则绑定。
不读取任何旧 JSON 文件，也不通过新 API 接受旧连接器/动作配置。

## 管理 API

所有请求使用独立 Bearer 管理令牌，同源、no-store。PUT/POST 必须 application/json，
请求体最多64KiB；凭据不写日志，配置读取只返回脱敏值。

- GET /api/v1/rules/config：`schema_version:2`、`outbounds` 和 `rules`。
- PUT /api/v1/rules/config：整体校验、持久化并替换配置；规则使用 `outbound_ids` 绑定出站。
- GET /api/v1/rules/status：实际入队、命中、失败、丢弃、重试与积压，以及带类型的 `outbounds` 统计。
- POST /api/v1/rules/test：`{sql,event}` → `{matched,output}`，无出站副作用。

测试 Event 的 payload 为 UTF-8 字节数组；Dashboard 负责把样例字符串编码。
其余字段为 topic、client_id、qos、retain、timestamp_ms。出站“保存”只验证
配置，不代表目标网络连通。本阶段不执行任意地址的隐式连接测试。

配置版本替换会取消旧版本积压并计入丢弃，避免无限保留旧任务。极端情况下
SQLite 写入或提交失败时暂停规则运行并返回存储错误；running=false 时页面
禁止继续编辑或测试，需排查后重启并从已提交配置恢复。
HTTP 连接池按配置版本管理，旧版本停止后释放，不为过去任意多个目标保留连接池。

新配置请求示例见 [API 请求体](examples/rules-webhook.json)，仅供 HTTP API 使用，不是运行时配置文件。

## 并发请求

每个出站默认8个并发请求，后台仍受全局16个HTTP尝试限制；单条模式下，一个
请求携带一条事件。同一出站按精确来源 Client ID 分组，每组最多一个批次正在处理（包括重试）。
同一 Client 跨主题、跨匹配规则的事件按入队顺序发送，批次不混合不同 Client。
慢请求只占用其实际请求槽位；重试退避释放槽位，其他 Client 可继续发送。
并发不按MQTT客户端数量累加，MQTT正常路由与ACK不等待Webhook。
旧配置没有 `delivery.concurrency` 时按8处理，原有批量、等待、队列、超时、
重试、标识与凭据保持不变。并发设为1限制同时发送的请求数，仍不承诺跨 Client
的全局顺序；退避时允许其他 Client 发送。同一 Client 的前一批次成功或最终
失败后才发送下一批，最终失败记入失败统计，不会永久阻塞该 Client。
状态的 `inflight_requests`（进行中）按实际请求计数，不包括排队和重试退避；
已发送、失败、丢弃等仍按消息条数统计。配置替换和停止会取消旧批次的所有子任务。
HTTP连接池由同一出站共享，保留空闲连接上限与该出站并发度一致；空闲回收仍30秒。
响应体处理策略未在本阶段更改，不提供任意响应体收集。

按 Client 保序仅适用于同一运行配置版本内成功接纳的事件；过载仍可能丢弃新事件，
配置替换/重启仍沿用易失队列语义。超时或取消后远端业务可能仍在运行，
HTTP 接收端需自行保证异步处理顺序并按 delivery_id 去重。
