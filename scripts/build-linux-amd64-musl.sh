#!/bin/sh
# Build the Dashboard on the host, then cross-build the locked static Broker.
set -eu
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_dir"
command -v cargo-zigbuild >/dev/null
command -v zig >/dev/null
: "${CARGO_BUILD_JOBS:=4}"
export CARGO_BUILD_JOBS
npm --prefix dashboard/web ci
npm --prefix dashboard/web run build
cargo zigbuild --locked --release --target x86_64-unknown-linux-musl -p lyrnormq "$@"
python3 scripts/check-static-elf.py target/x86_64-unknown-linux-musl/release/lyrnormq
