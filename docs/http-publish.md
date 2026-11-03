# HTTP downlink publishing

`POST /api/v1/mqtt/publish` uses the same listener and Bearer token as Dashboard.
Plain HTTP is supported. The existing admin listener accepts loopback addresses;
use an HTTP or HTTPS reverse proxy when a remote service needs access. No MQTT
username/password, Basic authentication, source Client ID or authentication cache
is involved. Configured MQTT HTTP/PostgreSQL/local authentication and Topic ACL do
not constrain this privileged server-origin publication and are not called by it.
MQTT clients still need their configured connection/subscription permissions.

```http
POST /api/v1/mqtt/publish HTTP/1.1
Authorization: Bearer <dashboard-token>
Content-Type: application/json

{
  "topic": "devices/device001/commands",
  "payload": "{\"command_id\":\"cmd001\",\"action\":\"restart\"}",
  "qos": 1,
  "retain": false
}
```

| Field | Required | Meaning |
| --- | --- | --- |
| topic | yes | Nonempty MQTT Topic, no wildcards or reserved `$SYS` namespace |
| payload | yes | UTF8 string; encode a JSON payload as a string |
| qos | no |0,1,2; default1 |
| retain | no | defaultfalse; true plus empty payload deletes a retained message |

Success:202 `{"accepted":true}`. This is broker acceptance, not device receipt or
execution. No matching subscriber still succeeds. Existing offline persistent
sessions, recipient QoS, retained storage and shared subscriptions are reused.
HTTP publishing creates no MQTT connection/session or lifecycle event. Broker-origin
messages are not suppressed by MQTT5 No Local. Existing SQL rules see observer
client_id=`http` and may forward to generic Webhook/Kafka outbounds as usual.

| HTTP status | Meaning |
| --- | --- |
|400 | malformed/unknown fields, invalid topic/QoS/query/content type |
|401 | missing/wrong/duplicate Bearer credentials; Basic is not accepted |
|403 | reserved `$SYS` or `$SYS/` descendants cannot be forged |
|405 | only POST supported |
|413 | JSON body exceeds1MiB or MQTT packet exceeds configured size |
|503 | publishing runtime unavailable, capacity exhausted or publication timeout |

Errors retain the existing management API envelope:
`{"error":{"code":"invalid_request","message":"..."}}`. No credentials,
payloads or backend errors are echoed/logged. The administrative token has broad
publication privilege; use the existing management token file and rotate using its
normal deployment process. This API does not introduce a second token store.

The host publication API has32 active slots and16MiB resident input budget, a5s
operation deadline and finite shutdown draining. Existing HTTP listener limits
also apply. A timeout or lost HTTP response can have an ambiguous outcome; a retry
may duplicate messages. There is no cross-recipient transaction or automatic
command deduplication. The existing rule-admission/durable-outbound boundary remains
unchanged. No throughput or P99 improvement is claimed without a load measurement.
