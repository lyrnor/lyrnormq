# Phase5a — SQL rules and Webhook outbound

Frozen before implementation2026-10-30. No plugins/Kafka/Postgres/windowing. Native
bounded asynchronous best-effort side path; MQTT ACK/QoS/session/outbox/storage schema
unchanged. No durable-event admission or durable action outbox claimed this phase.
Config persists on successful management mutation; queued tasks/history/counters are
process-local and may be lost on crash. Failures/overflow explicit, no silent success.

## Core crate mqtt-rules (agent owns crates/mqtt-rules + narrow transport hook)

Thinlib. Types serde Serialize/Deserialize denyunknownfields whereconfiguration:
Event {topic:String,payload:Vec<u8>,client_id:String,qos:u8,retain:bool,timestamp_ms:u64}.
Rule {id:String,name:String,sql:String,enabled:bool,action_ids:Vec<String>};
Rule compilation via `CompiledRule::compile(sql:&str)->Result<Self,RuleError>`,
`matches_topic(&self,topic:&str)->bool`, `evaluate(&self,event:&Event)->Result<Option<serde_json::Value>,RuleError>`.
`topic_filter(&self)->&str`. NoIO. IDs1..64 ASCII alnum/_/-, name1..128bytes;
SQL<=8192bytes, exactlyone SELECT, single double-quoted MQTT TopicFilter FROM,
projection expressions with AS or field/defaultnames; WHERE boolean/3valuedNULL.
Fields client_id,topic,qos,retain,timestamp and payload JSON dotpaths. SELECT *
returns envelope metadata+decodedJSON payload (null ifbinary); payload nestedobject
allowed. Comparison = != <> < <= > >=, AND/OR/NOT, IS NULL/IS NOT NULL,
parentheses, simple + - * / and unaryminus; arithmetic overflow/divzero yields typed
error; wrongtypes/absentfield NULL. Numeric int/float finite, use f64 with documented
precision boundary (reject literals/nonfinite, no silentnan). Functions initially
COALESCE, LOWER, UPPER; no regex/JOINS/order/group/subqueries/DML. Maxexpressions256,
depth32, projection64, output<=1MiB. Payload parses lazily once/eventevaluation;
metadata-only rule supports opaque binary. Topicvalidate reuse mqtt-router rules.
Use sqlparser AST whitelist, do not execute arbitrary SQL or database calls. Compile
onlyconfig/test, notmessagehotpath. Deterministic no wallclock/randomfunctions.

Transport extension public trait PublicationObserver: Send+Sync+Debug with
`fn observe(&self,topic:&str,payload:&[u8],client_id:&str,qos:u8,retain:bool)`.
BrokerConfig.publication_observer:Option<Arc<dyn PublicationObserver>> defaultNone.
Call once at Broker::dispatch publication routing entry (incomingacceptedpublication,
includingWill, excludingretainedsubscriptionreplay/outboundresends). Do notinventQoS1
inputdedup; existingrecoverymayduplicate. Hook MUST be nonblocking bounded try-admit,
no await/IO/JSONSQLcompute/network. Observerfailure neverchangesMQTT routeorACK.
Skip expiredpublications. Document originclient forWill existingPublication publisher.
No publicprotocol/state schema changes; only transportconfig/lib/dispatchandtests.
Agent may own crates/mqtt-transport for this narrow hook, no overlapping runtimeedits.

## Webhook crate mqtt-webhook (agent owns crates/mqtt-webhook)

Depends mqtt-rules, mqtt-transport for observertrait, tokio,serde,serde_json,reqwest
rustls. No rootCargo edits. Runtime::open(path:PathBuf)->async ioResult<Arc<Runtime>>
loads/validates or creates initialemptyconfig onblockingpool. Runtime::observer()
returns Arc<dyn PublicationObserver>. Runtime async fn configuration()->PublicConfig,
replace(config:Config)->Result<PublicConfig,RuntimeError>, test(sql:&str,event:Event)
->Result<Option<Value>,RuntimeError>; status()->Status; shutdown()->async.
Use explicit public aliases/types ifmethodsignaturesneedadapt notifycoordinatorfirst.

Config {connectors:Vec<Connector>,actions:Vec<Action>,rules:Vec<Rule>};
Connector {id,name,url,headers:BTreeMap<String,String>} headersdefaultempty;
Action {id,name,connector_id,enabled:bool,batch_size:usize,batch_wait_ms:u64,
queue_capacity:usize,timeout_ms:u64,max_attempts:u32,retry_base_ms:u64}.
PublicConfig samecollections butConnectorheadersredacted (values "[redacted]");
replace roundtrip preserves existingsecretifexact [redacted], rejectsnewredacted.
All update in one PUT config; referential integrity rejects missing action/connector,
duplicate IDs, referenced disabled actionsallowed butnotdispatch. Maxconnectors32,
actions64,rules128,totalbindings512,config<=64KiB,headers<=16entries,total8192bytes.
URL http/https only,no userinfo/query/fragment; redactedsecrets in headers only.
Reject hopbyhop/Host/ContentLength/ContentType customheaders andCRLF; alwaysJSON.
Configsuccessfulreplace writes fullconfig atomically (0600,regularfile/nosymlink,
max64KiB, same-dirtemp+sync+rename) before swapping compiledsnapshot; never return
headers/URLsecrets in errors/logs. NewnameandID validation sharedcoreorlocalexplicit.
Startupinvalidfilefailclosed. Settingchanges create new bounded generation workers;
oldgeneration queuedtasks drained ifpossible or countedcancelled, bounded maxoneold
andnewgeneration with deadline; no unbounded orphanworkers onrapidupdates.

Admission: topicindex/snapshot match topic before clonepayload; try_send bounded
inputqueue1024 and maxpayload1MiB/globalqueuedbytes16MiB (includeactionfanout).
Independent fixed ruleworker/dispatcher, no task permessage; CPU execution off
Tokio networkruntime using dedicatedbounded worker thread or bounded spawnblocking
jobs(max4). TotalcandidateSQLwork bounded rules128; no globalpermessage SQLparse.
One action queue/worker each, maxinflight1 peraction initially; batches from same
actiongeneration maxsize1..500,maxrequest1MiB,maxwait0..1000ms. actionqueue1..4096,
timeout100..30000ms,maxattempts1..5,retrybase10..10000ms. Globaloutboundconcurrency16,
tryacquire or boundedwait withshutdownawareness; reqwestpoolfinite,redirectdisabled,
TLSverifyenabled, no implicitnetworkcall fromtest/configvalidation. POST JSON envelope
{action_id,rule_id,events:[{event_id,topic,client_id,qos,retain,timestamp_ms,data:Value}]};
batches MUST same rule_id or each eventincludesrule_id (freeze choose eventincludes
rule_id, envelopeonlyaction_id+events). UUIDnotneeded stableprocessid+sequence event_id
sameacrossretry; actiontaskidoptional. Multipleactions independent failurestates;
inputevent canmatchmultiple rules. maxoutput1MiB; overlimitcountfailednotretry.
SuccessHTTP2xx; retrytransport/timeout/408/429/5xx withboundedexponential+jitter;
other4xx permanent. Atleastonceattempts: timeout mayalreadyapplyatpeer, stableID
forreceiveridempotency; no distributedexactlyonce. No responsebody/payload/headers
logged; statuslast_error onlyclass HTTPcodeortransport category. Largebodyignored
or boundedread16KiB, notunboundedcollect. TTL/capacity counts clear; overflow drops
onlyrule/actioncopy and incrementscounter; ordinaryMQTT unaffected.

Status Serialize JSON: {mode:"best_effort",queued_events:usize,queued_bytes:usize,
received:u64,matched:u64,dropped:u64,errors:u64,actions:Vec<ActionStatus>};
ActionStatus {id:String,queued:usize,delivered:u64,failed:u64,retried:u64,dropped:u64,
last_error:Option<String>}. Countsper event notperHTTPrequest (documentreties).
configuration/status no bodypayload/credentials; attempts bounded no unlimiteddeadletter
payloadhistory. shutdown bounded5s abandonsremainingwithcounters. Configruntime
works independentofmanagementserve; noadmin mayrun startupconfig. No filesystemIO
async task; no locks await; bounded resources acrossgenerations.

## Coordinator CLI/admin/frontend

CLI `--rules-config PATH` optin, nonempty/nonduplicate and invalidwithexport-state;
filecontainsConfig noothersecrets; loadedbeforeaccept. Defaultabsentdisabled(nohook).
--admin enablescontrolonlynotrules. Configpath neverAPIexposed orstaticserved;
add configuredruleconfig tostatic sensitivefileidentity rejection. ExistingCSP/token
unchanged. New API requiresruntime flag, otherwise503 {error:{code:"rules_disabled"...}}:
GET /api/v1/rules/config -> PublicConfig; PUT same Config atomicallyreplace;
GET /api/v1/rules/status -> Status; POST /api/v1/rules/test body{sql,event}->
{matched:bool,output:Value|null}. GETconfig/status noquery. Rule test limits payload
bytes1MiB butbody64KiB => maxbody64KiB,sql8KiB. TestneverHTTPsideeffect.
All /api authbeforebodyread; write onlythesepaths, noCORS, requireapplication/json,
maxbody64KiB; unknownfields reject; errorsstructuredredacted400/409/503. ExistingGET
bodyrejection/staticGETunchanged; no enabling managementmutationselsewhere.

Frontendowner dashboard/web/**: implementrulespage with3tabsRules/Connectors/Actions,
SQLeditingandlocaltestsample, enabletoggles, CRUDform(viafullconfigPUT), refintegrity
APIerrors, Webhookheaderspasswordvalues preserve redactedroundtrip. No arbitraryJSON
secret output/toast; no credentiallocalstorage. Separate statuspanelshowsbest_effort
crashloss/overflow andreal counters, nofakedata. ReactTailwind/shadcn reusecontrols.
Existing Route.rules nowimplemented, eventsstillunsupported; capabilitymutations flag
reflectnewruleswriteonly (coordinatoradds rules:true whenruntime, existingmutations:false
means broker-sessionmutationsfalse). API clientextend allowPUT/POSTJSON, existingAPI
callsignaturecompatible auth401clearstate. Boundpoll5s/pagevisible abortonroute/logout,
forms explicitSave/test noautowrites. Don'ttouchAPIsemanticsotherpages.

## Parallel ownership and gates

Core agent: newmqtt-rules + narrowtransporthook. Webhook agent:newmqtt-webhook only.
Frontend agent:dashboard/web only. Coordinator bin/rmqtt/rootlock/docs and integration.
Aftercore agentfinishes, reuse slotqualitytests/rules_webhook_blackbox.py +CI only.
Eachwriterexplicitindependentworktree/branchcommitsverifiedhashes. Standalone
SQLmalformed/type/NULL/ASTsecurity/depth/propertytests, fakeHTTPsuccess/retry/permanent/
slow/batch/overflow/configrestore/nosecret; dualversionQoS0/1/2 actualSQL→HTTP andMQTT
subscriberunaffected, binarypayloadJSONerror, 401writes, persistrestartconfig. Browser
CRUD/test/status againstrealbroker, nofake outputs. fmt/strictclippy/workspacetest,
Reacttests/build, existingMQTTregressions. Reportrealnumbersonly; nodurableactionclaim.
