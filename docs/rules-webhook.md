# SQL 规则与 Webhook 出站

原生 SQL 筛选/转换，Webhook 为唯一出站类型。不支持插件、Kafka、PostgreSQL、
聚合/窗口或任意代码。现阶段为 **best_effort 异步旁路**：不等待 HTTP 来确认
MQTT，队列与计数器在内存中；崩溃/队列满可能丢弃规则事件或动作副本。配置会
持久化，动作队列没有耐久 outbox。HTTP 超时可能已被接收方执行，重试可能重复。

## 启动

```sh
./scripts/build-rmqtt.sh
# 默认启用规则引擎，配置保存在工作目录的 rmqtt-rules.sqlite。
./target/release/rmqtt --listen 127.0.0.1:1883 \
  --admin-listen 127.0.0.1:8766 --admin-token-file /tmp/rmqtt-admin.token
```

管理令牌文件按 Dashboard 文档生成；不要放入静态资源目录。规则配置仅使用
SQLite，默认 `rmqtt-rules.sqlite`；`--rules-config PATH` 可指定其他 SQLite 路径。
规则、连接器和出站动作由 Dashboard 管理，保存后以事务提交并应用。
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

## 连接器与动作

连接器管理地址与请求头；动作独立设置批量、等待、排队容量、超时、尝试次数。
配置原子替换并校验引用关系。API 返回请求头 `[redacted]`；原样提交该占位符
保留旧值，新建凭据必须提供真实值。URL 不接受用户信息、查询串或 fragment；
鉴权信息放 Authorization 等请求头。禁止自定义 Host/Content-Length 等传输头。

仅 POST application/json，目标由已认证管理员配置，可指向内网。不会跟随重定向，
HTTPS 校验证书。多个动作使用独立有界队列，另有限制总字节和总请求并发；
同一动作初版最多一个在途请求，不保证跨动作完成顺序。单个慢目标仍会占用其
额度，但不能无限消耗其他动作队列。状态中的失败/丢弃为事件数，重试不是业务去重。

请求为 `{action_id, events:[...]}`，每个事件包含 event_id、delivery_id、rule_id、topic、
client_id、qos、retain、timestamp_ms 和 SQL 输出 data。同一任务重试保留 delivery_id，
接收方按 delivery_id 幂等；event_id 是源事件 ID，多个规则/动作可共享。2xx 为成功，网络错误/超时/408/429/5xx 有限退避重试，其他失败
进入失败计数；不保存无限的 Payload 死信历史。本阶段没有人工重放死信功能。

## 管理 API

所有请求使用独立 Bearer 管理令牌，同源、no-store。PUT/POST 必须 application/json，
请求体最多64KiB；凭据不写日志，配置读取只返回脱敏值。

- GET /api/v1/rules/config：连接器、动作、规则。
- PUT /api/v1/rules/config：整体校验、持久化并替换配置。
- GET /api/v1/rules/status：实际入队、命中、失败、丢弃、重试与积压。
- POST /api/v1/rules/test：`{sql,event}` → `{matched,output}`，无出站副作用。

测试 Event 的 payload 为 UTF-8 字节数组；Dashboard 负责把样例字符串编码。
其余字段为 topic、client_id、qos、retain、timestamp_ms。连接器“保存”只验证
配置，不代表目标网络连通。本阶段不执行任意地址的隐式连接测试。

配置版本替换会取消旧版本积压并计入丢弃，避免无限保留旧任务。极端情况下
SQLite 写入或提交失败时暂停规则运行并返回存储错误；running=false 时页面
禁止继续编辑或测试，需排查后重启并从已提交配置恢复。
HTTP 连接池按配置版本管理，旧版本停止后释放，不为过去任意多个目标保留连接池。
