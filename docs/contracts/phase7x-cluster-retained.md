# Phase7x — durable-cluster Retained

Supersedes Phase7a/7b/7f/7w refusal of retained PUBLISH and Will-Retain on
durable (Phase7f RAM W2) replicas only. Legacy non-durable cluster still
refuses retain. Shared subscriptions remain refused. Do not claim full cluster
MQTT compatibility.

Standalone retained store, SUBSCRIBE replay, empty-payload clear, expiry and
Retain As Published stay unchanged for non-cluster brokers.

## Frozen capability

Durable cluster admits PUBLISH with RETAIN and CONNECT Will-Retain. MQTT 5
CONNACK advertises `RetainAvailable(1)`. MQTT 5 retain refusal `0x9A` remains
only on legacy cluster.

Each retained topic is one replica record `r:{topic}` (W2 RAM quorum, not
Controller bodies). Snapshot holds the ordinary `ClusterMessage` JSON; empty or
expired payload clears the snapshot. `DurableFilters` carries the topic as a
literal filter (or empty on clear) so every live Broker sees the key. Matching
SUBSCRIBE reads those records; it does not scan client session snapshots.

Live subscribers still receive the publication through the existing ordinary
forward path. Recipient delivery must not apply the retain store (partitions
never own global retain). RAP may keep the RETAIN flag on that live copy.

Will-Retain reuses Phase7w fire-once, then this retain record.

## Visibility and loss

A SUBSCRIBE ACK that follows a successful retain PUBACK must be able to replay
the topic from replica records, including on a Broker that had no subscriber at
publish time. Whole-group RAM loss cannot rebuild retain from control metadata.
A crashed replica is a new identity and must read live `r:` records.

## Ownership

Coordinator owns this contract. Cluster runtime owns `r:` records, AcquireOnce
`r:` keys and matching reads (`mqtt-cluster`). Transport owns PUBLISH/CONNECT
admission, CONNACK `RetainAvailable`, SUBSCRIBE staging and Will-Retain fire.
`mqtt-cluster-core` owns the `commit_retained` / `matching_retained` /
`apply_retained` hooks. Quality owns wire tests. Shared subscriptions remain a
follow-up gate.
