# HTTP 认证与授权接口 v1

状态：外部接口 v1 已定义，供业务服务与 Broker 后续实现；**当前尚未实现 HTTP 后端**。
本轮仅冻结外部请求/响应和语义，不修改现有认证接口、会话格式或管理 API。
机器可读定义：[OpenAPI 3.1](api/http-auth-v1.openapi.json)。本文的字节上限、权限和
故障规则也是规范要求，不能仅依靠 JSON Schema 校验。

## 1. 调用方向与公共约定

**Broker 调用业务方的认证服务**。以下为建议路径，两个完整 URL 可在 Broker 中分别
配置，也可由不同服务提供；它们不是 Dashboard 或 MQTT 客户端调用的 Broker API。

| 接口 | 方法及建议路径 | 用途 |
| --- | --- | --- |
| 认证 | `POST /mqtt/authenticate` | 验证本次连接凭据，返回稳定身份 |
| 权限策略 | `POST /mqtt/acl` | 根据稳定身份返回完整 ACL 快照 |

请求示例头：

```http
Content-Type: application/json
Accept: application/json
Accept-Encoding: identity
Cache-Control: no-store
Authorization: Bearer <Broker访问业务服务的令牌>
X-Request-ID: auth-01
```

- 请求/业务响应均为 UTF-8 JSON 对象，必须包含 `api_version: 1` 和 `request_id`。
  Broker 为每次请求生成不可预测的至少128位随机关联标识；响应必须原样回传。
  文中短 ID 仅为可读示例。`X-Request-ID` 与请求体相同，响应头无需重复它。
- 服务响应应设置 `Cache-Control: no-store`，避免中间HTTP缓存；Broker编译后的ACL
  采用本文独立的应用策略有效期，不复用HTTP缓存。
- **业务判定一律 HTTP 200**。包括密码错误、账号禁用、用户不存在和无权限。
  HTTP 401/403 表示 Broker 访问服务的凭据/服务访问被拒绝，不能解释为 MQTT 用户
  密码错误。404 表示地址错误，不能解释为用户不存在。
- 只接受 HTTP 200、`Content-Type: application/json`（可带 UTF-8 charset）的完整
  合法响应。204/3xx/4xx/5xx、DNS/TLS/连接错误、超时、超大/截断响应、重复 JSON key、
  未知字段/枚举、版本或关联 ID 不匹配，均为后端不可用/协议错误，绝不隐式放行。
- `null`、空字符串和字段缺失不等价；除注明的可选规则 ID 外，Schema 内列为必填的
  字段都必须出现。不得添加自由文本错误详情、密码回显或任意用户元数据。
- 请求上限1MiB，响应上限64KiB；流式读取到上限即停止，不相信 Content-Length。
  禁止压缩响应；不得通过 Content-Encoding 或压缩膨胀绕过限制。
- 默认 HTTPS，验证证书与主机名，支持自定义 CA、mTLS 和固定鉴权请求头。明文 HTTP
  必须显式启用，不提供跳过证书验证开关；禁止重定向、URL 内凭据、凭据查询参数，
  不将 MQTT 用户输入替换进 URL 或 HTTP header。
- 第一版单次请求默认总超时1000ms，可配置100..5000ms；包括连接、等待及响应读取，
  还受现有 MQTT 握手/重认证总期限约束。默认不自动重试。429/Retry-After 不延长
  当前连接期限，也不触发无界重试。两个接口应无业务副作用、可重复查询。

## 2. 认证接口

```http
POST /mqtt/authenticate
```

```json
{
  "api_version": 1,
  "request_id": "auth-01",
  "stage": "connect",
  "client_id": "device-001",
  "username": "alice",
  "password_base64": "c2VjcmV0",
  "mqtt_version": "5.0",
  "transport": "tls",
  "peer_ip": "192.0.2.10",
  "peer_port": 56789,
  "authentication_method": null,
  "current_principal": null
}
```

以上密码为虚构示例 `secret`。Base64 **不是加密**，凭据仍需受保护的网络传输。

| 字段 | 类型与含义 |
| --- | --- |
| `api_version` | 整数，固定1 |
| `request_id` | 1..64个 ASCII 字母、数字、`_` 或 `-`，关联本次 HTTP 请求 |
| `stage` | `connect` 或 `reauthenticate` |
| `client_id` | 完成协议校验和服务端 ID 分配后的有效 Client ID；非空，最多65535 UTF-8字节 |
| `username` | string 或 null，最多65535 UTF-8字节；null=MQTT未提供，空字符串=明确提供空用户名 |
| `password_base64` | string 或 null；原始密码字节按 RFC4648 标准字母表、规范 padding 编码；不换行、不使用URL-safe编码。null=未提供，`""`=0字节密码；解码后最多65535字节，不按UTF-8转换 |
| `mqtt_version` | `3.1.1` 或 `5.0` |
| `transport` | `tcp`、`tls`、`ws`、`wss` |
| `peer_ip` / `peer_port` | Broker实际TCP对端的IPv4/IPv6地址与1..65535端口，无方括号/zone；不信任客户端传来的X-Forwarded-For。反向代理时默认看到代理地址 |
| `authentication_method` | 普通CONNECT为null；已有MQTT5 PLAIN适配时为`PLAIN`，后端仍接收解析后的用户名/密码；不传原始AUTH挑战数据 |
| `current_principal` | 初次CONNECT固定null；重认证为该连接当前稳定身份，由Broker生成，不能由MQTT客户端自报 |

`reauthenticate` 仅适用于 MQTT5、`authentication_method: "PLAIN"` 和非空
`current_principal`。HTTP v1 不实现 SCRAM、自定义多轮挑战或 CN/SAN 账号映射。
新CONNECT即使恢复旧会话，仍使用 `stage: "connect"`、`current_principal: null`。

成功响应：

```json
{"api_version":1,"request_id":"auth-01","result":"allow","principal":"user:42"}
```

拒绝与用户不存在响应，均为 HTTP 200：

```json
{"api_version":1,"request_id":"auth-01","result":"deny","reason":"bad_credentials"}
```

```json
{"api_version":1,"request_id":"auth-01","result":"deny","reason":"not_authorized"}
```

```json
{"api_version":1,"request_id":"auth-01","result":"not_found"}
```

- `bad_credentials`：用户名/密码不匹配；`not_authorized`：账号禁用、来源/Client ID
  不允许等。密码错误必须 deny，不能 not_found。业务错误不向 MQTT 客户端暴露详情。
- `not_found` 仅表示该身份源没有该用户；单后端时拒绝。在未来显式配置的认证链中，
  只有它允许继续下一个身份源；deny 或不可用均不能自动回退到匿名/其他身份源。
- `principal` 是服务生成的全局稳定非秘密业务身份，1..256 UTF-8字节，无控制字符。
  不要使用随机登录会话 ID。后端迁移应保持已有持久会话的 owner 完全一致，例如已有
  `user:alice` 不能无迁移措施突然改为 `user:42`。Broker不自动加后端前缀或改写大小写。
- 重认证成功返回的 principal 必须与 current_principal 完全相同，不能借机更换会话
  所有者。错误的身份响应拒绝本次认证，不接管其他连接。
- 每次CONNECT实际验证凭据，不缓存认证成功，不保存客户端密码。显式匿名访问开关
  默认关闭；即使后端对空凭据返回allow，也不能越过Broker的匿名访问禁用配置。
- allow 只证明身份有效；还必须取得有效ACL并通过连接/Will权限检查，才可提交会话
  接纳、接管和CONNACK。失败认证不能断开同Client ID的旧连接或修改其耐久状态。

## 3. ACL 快照接口

```http
POST /mqtt/acl
```

```json
{"api_version":1,"request_id":"acl-01","principal":"user:42"}
```

该接口根据身份返回**完整权限快照**，不接收密码，也不针对某一条PUBLISH查询。
不依赖当前在线连接，Broker可用它为离线会话、恢复中的消息和Will重新获取权限。
授权来源可以独立配置，但必须理解认证来源返回的同一 principal 命名空间。

成功响应：

```json
{
  "api_version": 1,
  "request_id": "acl-01",
  "result": "ok",
  "principal": "user:42",
  "revision": "policy-17",
  "ttl_seconds": 30,
  "connect": {
    "allow_client_id_prefixes": ["device-"],
    "deny_client_id_prefixes": ["device-blocked-"]
  },
  "rules": [
    {"id":"telemetry","effect":"allow","actions":["publish"],"topic_filter":"devices/device-001/telemetry"},
    {"id":"commands","effect":"allow","actions":["subscribe","receive"],"topic_filter":"devices/device-001/commands/#"},
    {"id":"system","effect":"deny","actions":["publish","subscribe","receive"],"topic_filter":"$SYS/#"}
  ]
}
```

身份不存在、禁用或明确撤销全部权限时返回：

```json
{"api_version":1,"request_id":"acl-01","result":"deny","principal":"user:42","reason":"not_authorized"}
```

- `principal` 必须与请求完全一致。`revision` 是1..128 ASCII可见字符的策略版本，
  不具有数值或字典序大小关系。同一身份、同一revision对应相同connect/rules内容。
  `ttl_seconds` 为1..300整数；Broker实际有效期取它与本地上限的较小值，本地上限默认30秒。
- `connect` 必填。两个前缀数组各最多128项，每项最多1024 UTF-8字节、无控制字符；大小写敏感，
  按完整字面前缀匹配。允许列表含 `""` 表示所有Client ID；空允许列表表示全部拒绝；
  拒绝列表含 `""` 表示全部拒绝。先检查拒绝，再检查允许。
- `rules` 必填，0..128条。空数组表示没有Topic权限，不等于允许全部。
  `id` 可选，1..64个ASCII字母/数字/`_`/`-`，同一快照内若提供必须唯一，仅用于诊断。
- `effect` 为 `allow|deny`；`actions` 为非空、不重复的 `publish|subscribe|receive`
  数组；`topic_filter` 是合法MQTT Topic Filter，1..1024 UTF-8字节。
  不支持正则、模板替换、顺序覆盖、superuser或任意自定义action。

### 本地权限判定

1. 无匹配允许规则即拒绝，显式拒绝优先于所有允许，规则数组顺序不影响结果。
2. Publish/Receive 按精确Topic与Filter匹配，遵循MQTT `+`、`#`、空层级和根层`$`
   规则。`#`不隐式允许`$SYS/#`。Publish许可不自动产生Receive许可。
3. Subscribe必须由**同一条**allow规则完整覆盖请求Filter，且请求Filter与任何适用
   deny规则匹配的Topic集合没有交集。不能用普通字符串匹配Filter；不推导多个allow
   的并集覆盖。交集判定不确定时保守拒绝，不得过度放行。
4. 共享订阅使用去掉`$share/<group>/`后的内部Filter鉴权；ACL中不使用共享订阅包装。
   v1不区分订阅组权限，Receive仍检查实际Topic。
5. 接受订阅与实际收消息分别检查Subscribe/Receive。Retained回放、离线队列和重发
   都检查Receive；Will使用其持久身份的Connect和Publish权限，不借用当前连接用户。
6. HTTP v1没有按单连接IP、QoS、retain或消息正文表达的ACL条件。后端可在认证阶段
   按连接上下文拒绝；完整ACL本身只依赖principal，才允许跨该身份连接共享快照。

例：allow Subscribe `devices/#`、deny Subscribe `devices/private/#` 时，请求
`devices/#`必须拒绝，请求`devices/public/#`可以允许（还需对应Receive许可才能收消息）。

## 4. 快照、撤销与恢复语义

- ACL按“授权来源配置代次 + principal”缓存，同一key查询合并；原始响应不落盘为可信
  权限，不保存密码。配置、服务访问凭据或来源改变必须换代，旧请求不能更新新代。
- TTL从完整验证响应时的单调时钟计算，命中不能续期；后台在到期前带抖动刷新。
  本地有效期上限可配置1..300秒，默认30秒；缓存预算默认64MiB，满时有界拒绝新接纳，
  不能为释放缓存把在线身份误判成已撤权。有效期内的旧策略可能继续授权，撤销传播
  不是瞬时的，业务应据TTL评估；新连接的凭据仍逐次认证。
- 新的合法ok快照整体编译后按代次发布，不逐条修改；同key仅一个当前查询，拒绝晚到
  响应覆盖新配置/新查询结果。不得假定revision字符串天然可排序。
- 明确deny立即废弃该身份旧allow；第一版不缓存deny结果。合法新快照中的具体权限
  撤销通过原有事务路径协调订阅、待发送消息和Will，保留QoS ACK/outbox原子性。
- **不可用不等于deny**。有效快照可继续用到TTL，过期后拒绝新的连接/权限操作并暂停
  依赖该策略的应用消息派发；不自动延长旧权限，也不把超时转换成永久撤权清理。
  维持协议控制流/必要ACK，不能把已接纳QoS状态简化成重新投递。
- 恢复中的principal须独立取得ACL，不使用存储的密码或旧快照冒充新授权。
  有待恢复身份而策略不可用时，Broker不能宣布相关恢复就绪或执行破坏性撤权清理。
  运行时故障不得丢弃离线消息、重置重试状态或删除Will意图来“恢复服务”。
- 后端不可用时的Will截止时间、会话/消息过期及恢复屏障，须在**内部异步集成契约**
  中逐一冻结并通过测试；该HTTP文档不授权延长MQTT到期时间或宣称此部分已实现。
  不得在现有同步AccessControl/会话actor内执行HTTP或等待后台请求。

## 5. MQTT错误与服务故障

| 初次连接结果 | MQTT3.1.1 CONNACK返回码 | MQTT5 CONNACK原因码 |
| --- | --- | --- |
| bad_credentials / 最终not_found | 4 | 0x86 |
| not_authorized / ACL明确拒绝 | 5 | 0x87 |
| HTTP后端不可用/响应非法 | 3 | 0x88 |
| 本地接纳容量或速率耗尽 | 3 | 0x89，按既有接纳契约 |

按已协商报文大小等协议限制发送。不能把HTTP状态原样作为MQTT Reason Code。
已建立连接重认证失败使用DISCONNECT，而不是非法AUTH失败码：拒绝0x87、后端不可用
0x89；既有重认证接纳配额失败仍遵循phase4e的0x97。**0x86和0x88只用于CONNACK，
不能放入DISCONNECT**。依据[OASIS MQTT5 §2.4](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)。

HTTP服务通过429或503表达不可用，不要伪造deny以避免告警；Broker日志只记录后端标识、
请求ID、耗时、HTTP状态和受控错误代码，不记录请求/响应正文、密码、Authorization、
TLS私钥或原始服务错误页。错误日志应限频，后端并发/等待/连接池均有界且与出站隔离。

## 6. 后续实现验收清单

- 两个MQTT版本、null/空用户名/二进制密码、错误凭据/禁用/不存在/非法响应分类。
- 200 allow/deny/not_found与401/403/404/429/503，超时、压缩/超大/重复key/错ID响应。
- 无凭据匿名开关、PLAIN重认证身份不变、失败不接管旧连接、取消与迟到响应代次隔离。
- 前缀、allow/deny优先级、Subscribe包含与deny交集、`$`、共享订阅、Receive独立权限。
- ACL完整更新、TTL命中不续期、并发查询合并、内存限额、不可用与明确撤权分别处理。
- 持久会话、Retained、Will、离线/重传及崩溃恢复，现有QoS1/2和ACK/outbox回归。
- 真实HTTP/HTTPS/mTLS、慢服务/断网/拥堵、热路径无逐消息HTTP；实测而非承诺TPS/p99。

外部接口实现不得自称以上内部集成验收已完成；内部契约、代码与页面为下一步工作。
