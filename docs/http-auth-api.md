# HTTP 认证与授权接口 v1（简化版）

状态：**接口文档，HTTP 后端尚未实现**。本稿取代此前未实现的复杂草案，不兼容旧草案
中的body字段。保留两个POST接口，由Broker调用业务服务，URL可以分别配置。
[OpenAPI定义](api/http-auth-v1.openapi.json)。

## 1. 认证：POST /mqtt/authenticate

常见请求只发三个字段：

```json
{"client_id":"device-001","username":"alice","password":"secret"}
```

成功返回：

```json
{"result":"allow","user_id":"42"}
```

拒绝返回：

```json
{"result":"deny"}
```

| 字段 | 约定 |
| --- | --- |
| `client_id` | 请求必填，Broker解析和分配后的有效Client ID，最多65535 UTF-8字节 |
| `username` | 可省略或null表示未提供；`""`表示明确提供空用户名，最多65535 UTF-8字节 |
| `password` | 普通密码使用JSON字符串，按原样UTF-8编码传递，最多65535字节；不裁剪、不归一化 |
| `user_id` | allow时必填，业务系统的稳定用户ID，1..256 UTF-8字节、无控制字符；不能是每次变化的登录令牌 |
| `reason` | deny时可选，`bad_credentials`（默认）或`not_authorized`；业务方无需为普通拒绝填写原因 |

没有用户名/密码时可以省略相应字段，null也表示未提供；空密码用`""`。第一版不区分
“用户不存在”和“密码错误”，均返回deny，不提供自动回退到其他身份源的not_found结果。

只有密码无法表示为合法UTF-8时，改用Base64：

```json
{"client_id":"device-001","username":"alice","password_base64":"/wA="}
```

`password`与`password_base64`不能同时出现，即使其中一个为null。Base64采用RFC4648
标准字母表与规范padding，不换行、不用URL-safe形式，解码后最多65535字节。Base64
不是加密。以上均为虚构示例，实际凭据需要受保护的传输。

认证成功只确认身份，Broker还需取得ACL并通过连接/Will检查，才提交会话与CONNACK。
每次连接重新认证，不保存密码、不缓存认证成功。匿名访问必须由Broker显式允许。
`user_id`在Broker内作为稳定会话身份使用，不自动增加前缀；迁移已有账号时需保持原
owner一致，例如已有`user:alice`应返回同值。重认证复用同一请求格式，Broker在内部
检查返回user_id与当前身份一致，不向服务传stage、current_principal或AUTH状态。
HTTP v1只承接普通凭据及已有PLAIN解析结果，不实现自定义多轮挑战。

## 2. 权限：POST /mqtt/acl

请求只发用户ID：

```json
{"user_id":"42"}
```

常见响应只需发布和订阅权限：

```json
{
  "publish": ["devices/device-001/telemetry"],
  "subscribe": ["devices/device-001/commands/#"]
}
```

数组可省略，省略或空数组均代表没有该类权限。例如`{}`允许已认证身份保持连接，
但没有任何Topic发布或订阅/接收权限，不能解释为全部允许。

需要显式拒绝或Client ID范围时才增加可选配置：

```json
{
  "client_id_prefix": "device-",
  "publish": ["devices/#"],
  "subscribe": ["devices/#"],
  "deny": {
    "publish": ["devices/private/#"],
    "subscribe": ["devices/private/#"]
  }
}
```

账号禁用或需要撤销全部访问（包括连接）时返回：

```json
{"result":"deny"}
```

该拒绝响应不能混入权限数组。正常权限响应不需要result、user_id回显、版本、TTL或
规则ID。权限只依赖请求中的user_id，不能依赖某条连接或最后登录的Client ID，这样
同一身份的连接、离线会话和Will才能共用完整策略。

权限规则：

- 默认拒绝，显式deny优先，数组顺序不影响结果。四个权限数组总计最多128项，每项
  为1..1024 UTF-8字节的合法MQTT Topic Filter，支持`+`和`#`，不做变量或正则替换。
- `publish`控制发布以及Will发布。`subscribe`同时控制订阅与实际消息接收，沿用现有
  本地策略语义；Retained回放、离线消息和重发也检查接收权限。v1不单独暴露receive字段。
- 订阅Filter须被同一条允许Filter完整覆盖，且与任何拒绝Filter匹配的Topic集合没有
  交集；不推导多个allow的并集覆盖，不能用普通字符串匹配。无法确定时保守拒绝。
- `#`不隐式允许`$SYS/#`。共享订阅按去掉`$share/<group>/`后的Filter鉴权，规则中不
  写共享订阅包装；v1不区分订阅组权限。
- `client_id_prefix`可省略，默认`""`，表示不增加Client ID范围限制。填写时为最多1024
  UTF-8字节、无控制字符的字面前缀，大小写敏感，不支持通配符。该限制也作用于恢复
  和Will的Connect检查；初次认证仍可按请求Client ID执行更具体的业务限制。
- v1不提供按单连接IP、QoS、retain或消息正文的条件表达式，不返回superuser开关。

例：允许订阅`devices/#`、拒绝订阅`devices/private/#`，请求`devices/#`必须被拒绝，
请求`devices/public/#`可以通过；实际接收也遵循同一组允许/拒绝规则。

## 3. HTTP与故障约定

- 业务允许和拒绝都返回**HTTP 200 + application/json**。401/403属于Broker访问服务
  的凭据/访问问题，404属于地址问题，429/503属于服务不可用，不是MQTT用户判定。
- 不要求body中的api_version/request_id，也不要求服务回传请求ID。Broker可附带
  `X-Request-ID`用于排障，服务可以忽略。TLS、协议版本、IP/端口等连接元信息不放入v1 body。
- 服务鉴权用固定Authorization等请求头，可选mTLS，与MQTT用户名/密码分开。
  默认HTTPS并验证证书与主机名，明文HTTP须显式允许，不提供忽略证书验证开关。
  禁止重定向、URL内凭据和查询参数凭据，不将用户输入替换进URL/header。
- 请求最多1MiB、响应最多64KiB，按流式读取计量，不相信Content-Length；只接受UTF-8
  JSON对象，拒绝压缩响应、重复key、未知字段/枚举及非法字段类型。响应应设置
  `Cache-Control: no-store`，Broker编译后的策略不属于HTTP缓存。
- 单次请求默认总超时1000ms，可配置100..5000ms，同时受现有握手/重认证期限约束。
  默认不自动重试，不用Retry-After延长当前期限。后端请求、等待与连接池有界并与出站隔离。
- 只接受合法HTTP200业务响应；其他状态、DNS/TLS故障、超时、截断/超大/非法JSON都归为
  后端不可用，不能转为allow、匿名访问或明确撤权。日志不记录凭据或请求/响应正文。

## 4. 缓存、撤销和恢复

ACL有效期由Broker配置，默认30秒，可配置1..300秒；业务服务不用返回ttl或revision。
按“权限来源配置代次 + user_id”合并查询、编译完整快照；Broker自行管理更新代次和
内容变化，不接受晚到响应覆盖新配置。有效期按单调时钟计算，命中不续期，后台抖动刷新。
缓存预算默认64MiB，资源不足时有界拒绝新接纳，不能把逐出或缺失当成永久撤权。

合法的新快照整体替换旧权限。明确的`result:deny`立即撤销旧授权，不缓存deny。
有效旧快照最多用到有效期结束；过期且刷新失败时停止依赖该策略的新授权和应用消息
派发，保留必要协议控制流，不自动延长旧权限，也不调用永久撤权清理去删除耐久数据。
离线会话和Will可仅凭user_id重新查询权限，不需要持久化客户端密码。

MQTT3.1.1初次拒绝使用CONNACK4（凭据错误）、5（无权限）、3（服务不可用）；MQTT5
对应0x86、0x87、0x88，本地接纳过载按既有0x89规则。已建立连接重认证拒绝使用
DISCONNECT0x87，后端不可用使用0x89；接纳配额错误沿用既有0x97。不能将CONNACK专属
的0x86/0x88放入DISCONNECT。依据[OASIS MQTT5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)。

内部异步接入、Will截止时间、到期/恢复屏障和QoS ACK/outbox原子性仍须在实现前单独
冻结并测试。本文不授权延长MQTT到期时间，不允许在同步AccessControl/会话actor中
执行HTTP，也不声称HTTP后端已完成。
