use crate::{codec, seeds, session, session_v5};
#[test]
fn checked_in_corpus_matches_reproducible_hand_seeds() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
    let expected = seeds();
    let actual = std::fs::read_dir(&root).expect("committed corpus").count();
    assert_eq!(actual, expected.len());
    for (index, bytes) in expected.iter().enumerate() {
        assert_eq!(
            std::fs::read(root.join(format!("seed-{index:02}"))).expect("seed bytes"),
            *bytes
        );
    }
}
#[test]
fn replay_all_hand_seeds_against_codec_and_recovery_oracles() {
    for input in seeds() {
        codec(&input);
        session(&input);
        session_v5(&input);
    }
}

#[test]
fn v5_hand_trace_reaches_two_mixed_rap_qos2_deliveries() {
    let trace = [
        2, 0, 2, 2, 1, 137, 69, 6, 1, 21, 6, 0, 36, 0, 0, 28, 0, 0, 32, 0, 0, 36, 0, 0, 28, 1, 0,
        32, 1, 0,
    ];
    assert_eq!(session_v5(&trace), (10, 2));
}
