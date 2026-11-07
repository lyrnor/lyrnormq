# Phase7y — durable-cluster Shared Subscriptions

Supersedes Phase7a/7b/7f/7w/7x refusal of `$share` on durable (Phase7f RAM W2)
replicas only. Legacy non-durable cluster still refuses shared subscriptions.
MQTT 3.1.1 still cannot create shared memberships (`SUBACK 0x80`). Do not claim
full cluster MQTT compatibility.

Standalone shared selection, RAP, subscription identifiers, No Local protocol
errors and “no retained replay on shared SUBSCRIBE” stay unchanged for
non-cluster brokers.

## Frozen capability

Durable cluster admits MQTT 5 `SUBSCRIBE` of valid `$share/{name}/{filter}`
keys. MQTT 5 CONNACK advertises `SharedSubscriptionAvailable(1)`. MQTT 5 shared
refusal `0x9E` remains only on legacy cluster.

Groups are keyed by the full shared filter. Membership is the `DurableFilters`
list on `c:{client}` replica owners. The volatile recipient index matches the
underlying filter, not the `$share/` prefix as a Topic Name. A valid shared
key is never an ordinary route. `$share`-looking filters that fail
`split_shared_filter` stay literal Topic Filters.

Each matching group independently selects one member at host-outbox admission.
The selected copy is a `ForwardScope::Shared { group, filter }` intent with
that one recipient. Ordinary matches remain a separate `ForwardScope::Ordinary`
intent. No cross-group or ordinary/shared dedup. Recipient delivery applies
`DeliveryScope::Shared` of the full key so the partition queues the shared copy
only.

## Selection and fairness

Deterministic round-robin by client identifier, using an in-memory cursor on
the forwarding Broker (the host outbox owner). Cursors are runtime-only: they
reset on process restart and are not a cross-node CAS. Fairness across
publishers on different Brokers is not durable. One copy per group per
publication is the correctness invariant.

Online versus persistent-offline preference is applied by the selected
partition (QoS 0 never queues offline). A discarded or unmatched shared copy
completes that intent; it is not reassigned. QoS 2 remains owned by the chosen
session. Shared SUBSCRIBE still ignores Retain Handling for initial replay.

## Visibility and loss

A SUBACK that follows a successful shared `SUBSCRIBE` must be visible to later
publications on any live Broker, including one that did not own the subscriber.
Whole-group RAM loss cannot rebuild membership from control metadata. A crashed
replica is a new identity and must read live `c:` filters.

## Ownership

Coordinator owns this contract. Cluster runtime owns shared matching and
host-time selection (`mqtt-cluster`). Transport owns SUBSCRIBE admission,
CONNACK `SharedSubscriptionAvailable` and `DeliveryScope::Shared` on replica
ingress. `mqtt-cluster-core` owns `ForwardScope::Shared`. Quality owns wire
tests.
