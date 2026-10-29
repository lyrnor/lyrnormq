//! Bounded fuzz entry points and deterministic hand-seed corpus.
mod harness;
mod v5_harness;
pub use harness::{MAX_INPUT, codec, seeds, session};
pub use v5_harness::session_v5;

#[cfg(test)]
mod seed_tests;
