# Embedded Dashboard — 2026-10-30

Actual native macOS arm64 build, Rust stable1.98.1, Node24.12.0/npm11.6.2.

- `cargo fmt --all -- --check`, strict workspace/all-target Clippy: pass.
- `cargo test --workspace --offline`:572 passed including doctests (`rust.log`).
- std-only `rustc --edition2024 --test bin/lyrnormq/build.rs` suite:4 pass (missing
  dist diagnostic, allowed/unknown/oversized files, count/total caps, symlinks).
- `cargo build --release -p lyrnormq --offline`: success (`release-build.log`), native
  arm64 executable approximately9.2MiB. No Linux/server deployment claim.
- Copied-release independent acceptance:4/4 (`standalone.log`), empty cwd/PATH
  without Node, HTML-referenced assets/favicon/MIME/CSP, bearer/live MQTT, implicit
  cwd dist ignored, explicit override with no builtin merge or invalid fallback.
- Existing Dashboard live API/MQTT:14/14 using release binary (`api.log`).
- `scripts/build-lyrnormq.sh --profile dev` with offline npm cache: pass
  (`build-wrapper.log`), full React production build then selected Rust profile.
- An additional actual Cargo probe added a unique JS file to dist, rebuilt debug,
  asserted marker in executable, deleted file, rebuilt and asserted marker absent.
  It passed without cargo clean; temporary probe was removed.

The UI source/API/auth/CSP are unchanged. Frontend30 tests and desktop/mobile browser
acceptance from the previous Dashboard phase remain documented separately. Runtime
binary needs token/certificate/storage config as applicable, but no UI dist or Node.
Build host still requires frontend build before Cargo; dist is ignored by Git and
missing inputs deliberately fail compilation. Hosted CI updated, not executed here.
