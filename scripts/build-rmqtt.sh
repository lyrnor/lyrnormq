#!/bin/sh
# Build the UI on the host, then pass target/profile flags through to Cargo.
set -eu
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_dir/dashboard/web"
npm ci
npm run build
cd "$repo_dir"
profile_set=0
for argument in "$@"; do
  case "$argument" in --release|-r|--profile|--profile=*) profile_set=1 ;; esac
done
if [ "$profile_set" -eq 1 ]; then
  cargo build -p rmqtt "$@"
else
  cargo build --release -p rmqtt "$@"
fi
