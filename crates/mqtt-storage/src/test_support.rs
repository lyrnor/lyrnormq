//! File-test isolation shared by both configuration and MQTT durable stores.
//!
//! Crash helpers spawn child processes. During fork/exec, an inherited ownership
//! descriptor can briefly keep another test's lock alive after its store drops.
//! All parent-side file tests use this gate; isolated child workers must not.

pub(crate) static FILE_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());
