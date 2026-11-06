//! Process-wide Rust allocator. C libraries keep the platform malloc.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
