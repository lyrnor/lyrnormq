use super::*;
#[test]
fn options_are_finite() {
    let mut options = QueueOptions::new(std::path::PathBuf::from("unused"));
    options.max_records = 0;
    assert_eq!(
        Journal::open(options).unwrap_err(),
        QueueError::InvalidOptions
    );
}
