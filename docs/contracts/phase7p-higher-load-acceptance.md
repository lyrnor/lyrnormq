# Phase7p — higher connection and message load acceptance

User authorizes continuation after the successful Phase7o1000/s10k gate. Reuse
Broker87ced3e81da295b4273b2757c8fc4472409d88472e5c77cca77ba472fd28854b;
no production-code/configuration/timing/durability changes in this measurement.
Broker47.84.224.186/private172.16.240.180, load47.84.241.200/private172.16.240.181.
Three combined Controller/data processes,4Tokio workers each on one8CPU host.

Run serially, each with fresh cluster data and client output directories:
1. c2000:2000new connections/s,10000total,10s hold then PING every connection.
2. c4000:4000/s,10000total, same hold/PING.
3. q0,q1,q2:5000publisher/subscriber pairs=10000connections,128B payload,
   MQTT5,10000aggregate publications/s,3s warmup,60s offering,30s drain,
   inflight1/pair, setup concurrency8, connect30s/I/O10s, whole-case420s.
   Publisher i%3/subscriber(i+1)%3: all messages cross nodes on three directed
   ring edges, not all six possible edges. Connections3333/3334/3333.
4. cscale:1000new connections/s, add10000per plateau through100000; each
   plateau holds10s and PINGs all live sockets before advancing.

Stop the entire remaining matrix at the first failed case or plateau. Preserve
partial output and diagnose from existing evidence; no automatic retries, rate
reduction, timeout extension or further pressure after failure. No success from
setup/warmup failure, count of metadata owners or exit0 alone.

Connection success requires all intended arrivals actually started, accepted and
alive after PING; no error/skipped entries, and exact configured rate/plateaus.
Message success requires exact configuration,600000scheduled/attempted/written/
uniquely delivered/completed messages; correct final PUBACK/PUBCOMP counts,
no duplicates/unexpected/invalid payload, missing/incomplete/failed pairs or hidden
retries. Report actual offering-window and through-drain TPS separately. Report
send-to-delivery and schedule-to-delivery latency, scheduling lag, source ACKs and
bounded missing deliveries; do not double ingress+egress or invent latency from
failed setup/warmup. Independently verify raw samples when a message case runs.
128B includes the48B probe header. Inflight1 is bounded offered traffic, not an
unconstrained maximum-TPS search. No additional quiet tail or independent message
ordering assertion exists in this loadgen; zero duplicates is observation-window
scoped. Setup/warmup/drain/reporting may be included in whole-process resources;
separately label any approximate measured-window resource estimate.

Reuse existing Rust probes after SHA verification; no server compilation. Record
timestamps, binary hashes, configured limits, exact commands, source-port range,
Broker/client CPU/RSS and Controller observations. Capacity preflight must cover
all TCP/session/source-port/FD/record limits. Only cscale needs temporary source
ports10240..65535; restore original range in finally. FD250000 as before.
Finite per-case runs, PID-identity checked cleanup, retain earlier run artifacts.
Same source code already passed Phase7o Rust/process gates; testing an unchanged
binary does not require repeating compilation or the full correctness suite.
