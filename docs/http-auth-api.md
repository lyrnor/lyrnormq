# HTTP 认证与授权接口 v1（简化版）

状态：**接口文档，HTTP 后端尚未实现**。本稿取代此前未实现的草案，ACL改为按具体操作查询，不返回权限列表。保留两个POST接口，由Broker调用业务服务，URL可以分别配置。
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

认证allow批准该身份使用本次Client ID连接；如果配置了Will，Broker还需对Will的
Topic完成publish鉴权后，才能提交会话与CONNACK。
每次连接重新认证，不保存密码、不缓存认证成功。匿名访问必须由Broker显式允许。
`user_id`在Broker内作为稳定会话身份使用，不自动增加前缀；迁移已有账号时需保持原
owner一致，例如已有`user:alice`应返回同值。重认证复用同一请求格式，Broker在内部
检查返回user_id与当前身份一致，不向服务传stage、current_principal或AUTH状态。
HTTP v1只承接普通凭据及已有PLAIN解析结果，不实现自定义多轮挑战。

## 2. 权限：POST /mqtt/acl

Broker提交“谁要对哪个Topic做什么”，服务只返回允许或拒绝。

发布鉴权请求：

```json
{
  "user_id": "42",
  "client_id": "device-001",
  "action": "publish",
  "topic": "devices/device-001/telemetry"
}
```

订阅鉴权请求：

```json
{
  "user_id": "42",
  "client_id": "device-001",
  "action": "subscribe",
  "topic": "devices/device-001/commands/#"
}
```

允许或拒绝，均为HTTP200：

```json
{"result":"allow"}
```

```json
{"result":"deny"}
```

| 字段 | 约定 |
| --- | --- |
| `user_id` | 必填，认证成功返回的稳定用户ID，Broker从已认证身份读取，不信任客户端自报值 |
| `client_id` | 必填，实际发起操作的有效Client ID；不能只用user_id省略设备维度 |
| `action` | 必填，`publish`或`subscribe` |
| `topic` | 必填，1..65535 UTF-8字节；publish为具体Topic Name，subscribe为Topic Filter |

响应仅有result，不能返回publish/subscribe数组、用户回显、规则ID、TTL或完整策略。
空对象、未知result或混入旧版权限列表属于非法响应，不能默认允许。

操作语义：

- Publish的topic必须是合法具体Topic，不包含`+`、`#`。Will使用其原始持久身份和
  Client ID，以`publish`检查实际Will Topic，不借用后来连接的用户。
- Subscribe按每个订阅Filter分别查询，允许`+`、`#`；多个Filter分别取得结果，SUBACK
  保持原顺序。allow只批准该次请求的完整Filter范围，不修改请求Filter。
- 业务服务如果使用内部Topic规则，必须判断请求Filter的完整权限范围，不能把Filter
  当成具体Topic做简单匹配。例：允许`devices/#`但拒绝`devices/private/#`时，请求
  `devices/#`必须拒绝，不能因它与某条allow规则匹配就放行。
- 对实际消息接收，复用`action:subscribe`，topic填具体的消息Topic，不展开出receive
  字段。Retained回放、离线消息和应用数据重发也按接收者身份/Client ID检查。服务必须
  将该具体Topic视为单Topic接收权限；仅订阅成功不代表后续一直允许接收。
- 共享订阅的初始请求由Broker去掉`$share/<group>/`后发送内部Filter，服务不再剥离
  包装。实际接收仍发送真实Topic。v1不区分共享订阅组权限。
- 缓存的`subscribe devices/#`决策不能当成`subscribe devices/a`或任何publish的
  缓存命中；服务只对本次精确请求作答，Broker不根据通配符推导其他请求的权限。
- 默认拒绝；请求中的信息仅限这四项。v1不提供按QoS、retain、IP或消息正文的条件。
  不返回superuser开关，也没有user_id全局撤权响应；deny只拒绝该次具体操作。

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
  `Cache-Control: no-store`，Broker自己的短期鉴权结果缓存不属于HTTP缓存。
- 单次请求默认总超时1000ms，可配置100..5000ms，同时受现有握手/重认证期限约束。
  默认不自动重试，不用Retry-After延长当前期限。后端请求、等待与连接池有界并与出站隔离。
- 只接受合法HTTP200业务响应；其他状态、DNS/TLS故障、超时、截断/超大/非法JSON都归为
  后端不可用，不能转为allow、匿名访问或明确撤权。日志不记录凭据或请求/响应正文。

## 4. 决策缓存、故障和恢复

缓存由Broker管理，服务不返回TTL/策略版本。缓存key精确包含：

```text
权限来源配置代次 + user_id + client_id + action + topic
```

默认有效期30秒，可配置0..300秒，0表示每次操作实际查询。allow和deny均可缓存；
有效期按单调时钟计算，命中不续期。后台不再拉取用户的完整策略，同key未命中查询
可以合并；配置换代/显式失效后的晚到响应不得回填缓存。默认总预算64MiB，包含key
与在途元数据，限制条目数、并发和等待，不能因Topic变化无限扩张。

有效期内可能继续使用旧决策，权限变化的传播延迟取决于TTL；deny也可能短暂阻止
刚新增的权限。配置换代使旧缓存失效。一个Topic的deny不得撤销同用户其他Topic的
授权，也不能当成账号全局禁用或触发整份会话清理。

**缓存未命中会增加HTTP往返时间**。高基数Topic、多Client或大量扇出时会产生更多
查询，不能继续承诺消息路径没有远程IO或TPS/p99不受影响。并发、合并、缓存命中率、
高基数/扇出及后端故障下的实测，是下一阶段验收项，不用缓存跨Topic推导换取吞吐。

超时/非200/非法响应为Unavailable，不缓存成deny，不沿用过期allow，也不执行永久
撤权清理。对应新操作不得当成授权成功；恢复/应用消息派发应保持有界等待或返回
合法失败，必要协议控制流不因HTTP后台等待而阻塞。具体QoS失败/暂存行为、路由扇出
和outbox事务屏障必须在内部异步集成契约中逐项冻结，不能只把异步HTTP塞进同步回调。

重启不信任磁盘上的旧决策。恢复连接重新认证，待重放/派发数据按其具体操作和持久
身份重新鉴权，不需要持久化客户端密码。Unavailable不能用来删除离线消息或Will意图，
也不授权延长原有Session/Message Expiry或Will的截止时间。

初次连接的认证拒绝或Will鉴权拒绝，MQTT3.1.1使用CONNACK4（凭据错误）、5（无权限）、
3（服务不可用）；MQTT5
对应0x86、0x87、0x88，本地接纳过载按既有0x89规则。已建立连接重认证拒绝使用
DISCONNECT0x87，后端不可用使用0x89；接纳配额错误沿用既有0x97。不能将CONNACK专属
的0x86/0x88放入DISCONNECT。依据[OASIS MQTT5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)。

内部异步接入、Will截止时间、到期/恢复屏障和QoS ACK/outbox原子性仍须在实现前单独
冻结并测试。本文不授权延长MQTT到期时间，不允许在同步AccessControl/会话actor中
执行HTTP，也不声称HTTP后端已完成。
