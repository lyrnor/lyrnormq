# HTTP 认证与授权接口 v1

状态：**接口文档，HTTP 后端尚未实现**。本稿替代此前未实现的草案。
两个POST接口由Broker调用业务服务，URL可分别配置。设备由username唯一标识，
不要求客户端或服务提供额外身份字段。[OpenAPI定义](api/http-auth-v1.openapi.json)。

## 1. 认证：POST /mqtt/authenticate

```json
{"client_id":"device-001","username":"device-001","password":"secret"}
```

| 字段 | 约定 |
| --- | --- |
| `client_id` | 必填，Broker解析和分配后的有效MQTT Client ID，最多65535 UTF-8字节 |
| `username` | 必填，唯一且稳定的设备标识，1..256 UTF-8字节，无控制字符；不能为null或空字符串 |
| `password` | 普通密码使用JSON字符串，按原样UTF-8编码，最多65535字节；不裁剪、不归一化 |

密码未提供时可省略或用null，空密码用`""`，由业务服务判断是否允许。HTTP后端要求
username，不支持匿名设备；不改变其他既有认证模式的匿名访问能力。

仅密码无法表示为合法UTF-8时，改用Base64：

```json
{"client_id":"device-001","username":"device-001","password_base64":"/wA="}
```

`password`与`password_base64`不能同时出现，包括null。Base64采用RFC4648标准字母表
与规范padding，不换行、不用URL-safe形式，解码后最多65535字节。Base64不是加密。
以上为虚构凭据，实际需要受保护的传输。

认证成功返回`204 No Content`，凭据错误或设备未通过认证返回`401 Unauthorized`，均无需JSON响应body。
成功表示服务已经验证该username的凭据并批准使用本次Client ID连接，Broker才信任
这个设备身份。如果有Will，还需对Will Topic完成publish鉴权后再提交会话和CONNACK。
每次连接实际认证，不缓存成功或保存密码。账号不存在、禁用、密码错误统一返回401。

Broker内部身份沿用`user:<username>`，外部接口不传内部身份。username大小写敏感，
不裁剪或归一化。重认证复用同一格式，但username必须与当前认证身份一致，不能借机
切换设备；不传stage或AUTH状态。HTTP v1只承接普通凭据与已有PLAIN解析结果。
username不要求与Client ID相同。username不可直接改名/复用给另一设备而继承原持久会话；迁移需处理旧会话归属。

## 2. 权限：POST /mqtt/acl

发布鉴权请求：

```json
{
  "client_id": "device-001",
  "username": "device-001",
  "action": "publish",
  "topic": "devices/device-001/telemetry"
}
```

订阅鉴权请求：

```json
{
  "client_id": "device-001",
  "username": "device-001",
  "action": "subscribe",
  "topic": "devices/device-001/commands/#"
}
```

四项均必填：username来自已认证连接或可信持久身份，不信任消息正文里的同名字段；
client_id为实际操作的MQTT客户端；action为`publish|subscribe`；topic为1..65535 UTF-8
字节，publish为具体Topic Name，subscribe为Topic Filter。

允许返回204，拒绝返回403，不返回JSON、Topic列表或完整策略。拒绝只针对本次请求，
不代表设备全局撤权，不得因此清理其他Topic或整个持久会话。

- Publish的topic必须合法且没有`+`、`#`。Will按原持久设备身份/Client ID，以publish
  检查真实Will Topic，不借用后来连接的username。
- Subscribe每个Filter分别查询，支持`+`、`#`；SUBACK保持原顺序。允许表示批准请求
  Filter的整个范围。服务不能把Filter当成具体Topic简单匹配：若允许`devices/#`但
  拒绝`devices/private/#`，请求`devices/#`必须拒绝。
- 实际接收复用`action:subscribe`、传具体消息Topic；Retained、离线消息和应用数据
  重发也按接收者鉴权。订阅成功不意味着永久允许接收。
- 共享订阅的初始请求由Broker去掉`$share/<group>/`后传内部Filter；服务不再次剥离。
  实际接收仍传真实Topic，v1不区分订阅组。
- `subscribe devices/#`的允许不能当成`subscribe devices/a`或publish的缓存命中。
  v1不增加QoS、retain、IP、消息正文、superuser等条件或返回字段。

## 3. 响应：仅看HTTP状态码

不需要JSON响应body，按接口分别解释：

| HTTP状态 | 认证 `/mqtt/authenticate` | ACL `/mqtt/acl` |
| --- | --- | --- |
| `204 No Content` | 认证通过 | 允许当前操作 |
| `401 Unauthorized` | 认证失败，账号/密码错误、不存在或禁用 | Broker访问ACL服务的凭据问题，Unavailable |
| `403 Forbidden` | 服务访问被禁止或不符合认证接口约定，Unavailable | 拒绝当前操作 |
| `429`、`5xx` | 服务过载/不可用 | 服务过载/不可用 |
| 其他状态，包括`200`、其他`2xx`、`3xx`、`400`、`404` | 请求/路由/协议问题，Unavailable | 请求/路由/协议问题，Unavailable |
| DNS/TLS/连接错误、超时、HTTP协议错误 | Unavailable | Unavailable |

认证不缓存结果。ACL只缓存204的allow和403的deny，401或服务故障不缓存成deny。
只认204允许，不把所有2xx视为成功，不兼容旧的200+JSON方案。

通过示例：

```http
HTTP/1.1 204 No Content
Cache-Control: no-store
```

204按[HTTP规范](https://www.rfc-editor.org/rfc/rfc9110.html#name-204-no-content)不携带
body；401/403也无需业务JSON，附带的错误正文不参与判定、不记录。最多有界丢弃4KiB
用于连接复用，超限或无法在期限内排空则关闭连接，不能为读错误页拖延决定。
响应不要求Content-Type或request ID回显；401按HTTP规范携带适用的WWW-Authenticate
challenge，Broker不会据此自动重试、更换服务凭据或转发设备密码。

仅凭状态码无法区分认证端点的设备401与服务鉴权网关401，Broker统一拒绝本次登录，
不据此认定设备永久禁用或清理已有会话。ACL端点的网关403同样会被视为当前操作拒绝，
不触发全局撤权。服务/网关故障应使用429/5xx，并保留独立运维诊断，不伪造业务拒绝。

## 4. 传输与限额

- 请求为UTF-8 JSON、`Content-Type: application/json`，最多1MiB，拒绝重复key、未知
  字段和非法类型。响应不要求JSON，不再兼容旧的200+result方案。
- 固定Authorization等服务请求头与设备密码分开，不提供自定义 CA 或客户端证书配置；默认HTTPS，使用默认信任根验证证书和
  主机名。明文HTTP须显式允许，不提供跳过证书验证开关。禁止重定向、URL内凭据、
  凭据查询参数和用户输入替换URL/header。请求ID仅为可忽略的`X-Request-ID` header。
- 单次请求默认总超时1000ms，可配置100..5000ms，受现有握手/重认证总期限约束；
  默认不自动重试，不用Retry-After延长期限。请求、等待和连接池有界并与出站隔离。
- 响应应设置`Cache-Control: no-store`，HTTP层不缓存设备判定。下述Broker应用缓存
  单独管理。日志只记录后端标识、关联ID、HTTP状态、耗时和受控代码，不记录密码、
  Authorization或请求/响应正文。

## 5. ACL决策缓存与恢复

缓存key为“来源配置代次 + username + client_id + action + topic”，全部精确匹配。
默认TTL30秒，可配置0..300秒，0表示每次操作查询；只缓存204/403，认证从不缓存。
按单调时钟计算有效期，命中不续期；同key未命中可以合并。配置换代/失效后的晚到响应
不能回填缓存，后台不拉取完整权限。默认总预算64MiB，条目、在途元数据和等待有界。

有效期内可能沿用旧判定，权限变化与新权限生效延迟取决于TTL。未命中有HTTP往返，
高基数Topic、多Client或扇出会增加查询，TPS/p99需实测，不能宣称消息路径没有远程IO。
不可用不缓存为deny，不沿用过期allow，不执行永久撤权清理。新操作不得当成授权成功，
必要ACK/控制流不能被全局HTTP等待阻塞，不能根据一个Topic的403清理其他授权。

重启不信任旧缓存。连接重新认证；离线数据/Will按可信持久身份恢复原username后，对
具体操作重新鉴权，不存储设备密码。无法恢复设备身份或服务不可用不能用作删除离线
消息/Will的理由，也不能延长Session/Message Expiry或Will截止时间。

MQTT错误映射：初次认证401统一为凭据错误（MQTT3.1.1 CONNACK4、MQTT5 0x86）；
Will ACL403为无权限（5、0x87）；后端不可用为服务不可用（3、0x88）；本地接纳过载
沿用既有规则。已连接重认证401使用DISCONNECT0x87，后端不可用使用0x89，重认证
接纳配额错误沿用0x97，不将CONNACK专属码放入DISCONNECT。其他操作拒绝按其合法
包类型处理，不把HTTP401/403直接作为MQTT码。

本轮只有外部接口。内部异步prepare/commit、缓存失效、Receive/扇出、QoS/outbox、
Will/到期/恢复屏障仍须先冻结再测试；禁止在同步AccessControl或会话actor内执行HTTP。
