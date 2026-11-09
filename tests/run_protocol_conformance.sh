#!/usr/bin/env bash
# Standalone MQTT protocol / conformance acceptance (not cluster).
# RMQTT_BIN=/abs/rmqtt ./tests/run_protocol_conformance.sh
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
export RMQTT_BIN=${RMQTT_BIN:-"$root/target/debug/rmqtt"}
export PYTHONPATH=$root/tests
cd "$root"

python3 docs/conformance/build_matrix.py --check
python3 tests/check_conformance_matrix.py
python3 docs/conformance/check_authoritative.py
cargo test --locked -p mqtt-protocol

wire=(
  blackbox.py qos_blackbox.py retained_will_blackbox.py auth_acl_blackbox.py
  mqtt5_blackbox.py expiry_will_blackbox.py negotiation_blackbox.py
  subscription_blackbox.py alias_blackbox.py shared_blackbox.py
  enhanced_auth_blackbox.py conformance_blackbox.py clause_boundaries_blackbox.py
  semantic_boundaries_blackbox.py rap_profiles_blackbox.py fault_recovery_blackbox.py
  persistence_blackbox.py tls_blackbox.py websocket_blackbox.py admission_mtls_blackbox.py
  system_client_events_blackbox.py phase3a_wakeup_blackbox.py
)
for suite in "${wire[@]}"; do
  echo "=== $suite ==="
  python3 "tests/$suite" -v
done

python3 tests/ack_reuse_stress.py
python3 tests/phase3_reliability.py
python3 tests/phase3a_wakeup_blackbox.py -v

paho=(
  mqtt5_paho.py retained_will_paho.py auth_acl_paho.py expiry_will_paho.py
  negotiation_paho.py subscription_paho.py alias_paho.py shared_paho.py
  paho_interop.py persistence_paho.py
)
if python3 -c 'import paho.mqtt.client' 2>/dev/null; then
  for suite in "${paho[@]}"; do
    echo "=== $suite ==="
    python3 "tests/$suite" -v
  done
else
  echo "SKIP paho suites (pip install 'paho-mqtt>=2,<3')"
fi

echo "protocol conformance suites completed"
