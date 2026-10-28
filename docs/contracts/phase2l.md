# Phase2l — packet lifecycle and property evidence

Continue pinned OASIS5 OS clause matrix; Phase2k corrections remain authoritative.
Coordinator owns docs/matrix/CLI and integration. Isolated Protocol/Session writers
own their crates; isolated Quality owns tests/CI. Do not change other modules.
Preserve schema7/public API/thin lib.rs and existing persistence/outbox/clock.

## Frozen acceptance scope

Add exact property/packet duplicate, range, type and context fixtures, checking
encode/decode independently of production tables. Packet identifiers need allocation,
release, wrap, wrong-stage and reconnect evidence, with peer obligations separated
from broker obligations. ReasonString diagnostics must not drive state transitions.
Wire evidence covers reserved flags, ACK association, role direction, protocol-name/
version refusal, takeover and Unicode byte-preserving Topic equality.

Tests may establish an existing rule without new approval. A confirmed behavior
mismatch requires a failing regression and a new correction subsection here BEFORE
production implementation. Do not manufacture a defect from an optional behavior,
client obligation or unclear interpretation. Direction-sensitive properties/reasons
must be checked at a connection-aware boundary if a symmetric codec cannot decide.
No MUST waiver and no full conformance inference from a nearby test.

Matrix promotions require exact row/context, inspected implementation, actual tests
and commands. Prior observations remain explicit until coordinator reconciles new
integrated evidence; do not blindly promote a feature or whole chapter. Exhaustive
property-table acceptance is not claimed from a finite fixture collection.
