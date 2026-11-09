#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    lyrnormq_fuzz_harness::session(data);
    lyrnormq_fuzz_harness::session_v5(data);
});
