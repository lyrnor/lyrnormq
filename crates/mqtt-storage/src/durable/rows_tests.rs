use super::*;
use crate::durable::SqliteStore;
use crate::durable::tests::{FILE_TESTS, TempDb};
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
};

fn limits() -> RowLimits {
    RowLimits {
        max_row_bytes: 1024,
        max_sessions: 4,
        max_retained: 4,
        max_legacy_bytes: 4096,
    }
}
fn rows(store: &RowStore) -> Rows {
    match store.contents().unwrap() {
        Contents::Rows(rows) => rows,
        Contents::Legacy(_) => panic!("unexpected legacy container"),
    }
}

#[test]
fn fresh_store_applies_batches_and_reopens_rows() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    {
        let mut store = RowStore::open(&path.0, limits()).unwrap();
        assert!(!store.is_legacy());
        assert_eq!(rows(&store), Rows::default());
        store
            .apply(&[
                RowChange::PutSession("b".into(), b"b1".to_vec()),
                RowChange::PutSession("a".into(), b"a1".to_vec()),
                RowChange::PutRetained("t".into(), b"r".to_vec()),
                RowChange::PutSession("b".into(), b"b2".to_vec()),
            ])
            .unwrap();
        store
            .apply(&[RowChange::DeleteRetained("t".into())])
            .unwrap();
        assert!(matches!(
            store.apply(&[RowChange::PutSession("x".into(), vec![0; 2048])]),
            Err(DurableError::SnapshotTooLarge)
        ));
        let synchronous: i64 = store
            .connection()
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(synchronous, 2);
    }
    let store = RowStore::open(&path.0, limits()).unwrap();
    assert_eq!(
        rows(&store),
        Rows {
            sessions: vec![("a".into(), b"a1".to_vec()), ("b".into(), b"b2".to_vec())],
            retained: vec![],
        }
    );
    assert_eq!(
        read_contents(&path.0, limits()).unwrap(),
        Contents::Rows(rows(&store))
    );
}

#[test]
fn container1_is_reported_then_migrated_atomically() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    SqliteStore::open(&path.0, 4096)
        .unwrap()
        .commit(b"legacy")
        .unwrap();
    {
        let mut store = RowStore::open(&path.0, limits()).unwrap();
        assert!(store.is_legacy());
        assert_eq!(
            store.contents().unwrap(),
            Contents::Legacy(Some(b"legacy".to_vec()))
        );
        assert!(
            store
                .apply(&[RowChange::DeleteSession("a".into())])
                .is_err()
        );
        let too_many = Rows {
            sessions: (0..5).map(|i| (format!("c{i}"), vec![1])).collect(),
            retained: vec![],
        };
        assert!(matches!(
            store.migrate(&too_many),
            Err(DurableError::SnapshotTooLarge)
        ));
        assert!(store.is_legacy());
    }
    assert_eq!(
        read_contents(&path.0, limits()).unwrap(),
        Contents::Legacy(Some(b"legacy".to_vec()))
    );
    let split = Rows {
        sessions: vec![("a".into(), b"session".to_vec())],
        retained: vec![("t".into(), b"retained".to_vec())],
    };
    {
        let mut store = RowStore::open(&path.0, limits()).unwrap();
        store.migrate(&split).unwrap();
        assert!(!store.is_legacy());
        assert!(store.migrate(&split).is_err());
    }
    let store = RowStore::open(&path.0, limits()).unwrap();
    assert_eq!(rows(&store), split);
    drop(store);
    assert!(matches!(
        SqliteStore::open(&path.0, 4096),
        Err(DurableError::Schema(_))
    ));
}

#[test]
fn open_limits_and_unknown_schemas_fail_closed() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    {
        let mut store = RowStore::open(&path.0, limits()).unwrap();
        store
            .apply(&[RowChange::PutSession("a".into(), vec![1; 512])])
            .unwrap();
    }
    let small = RowLimits {
        max_row_bytes: 16,
        ..limits()
    };
    assert!(matches!(
        RowStore::open(&path.0, small),
        Err(DurableError::SnapshotTooLarge)
    ));
    assert!(matches!(
        RowStore::open(
            &path.0,
            RowLimits {
                max_sessions: 0,
                ..limits()
            }
        ),
        Err(DurableError::InvalidLimit)
    ));
    Connection::open(&path.0)
        .unwrap()
        .execute("CREATE TABLE extra(value TEXT)", [])
        .unwrap();
    assert!(matches!(
        RowStore::open(&path.0, limits()),
        Err(DurableError::Schema(_))
    ));
    let altered = TempDb::new();
    drop(RowStore::open(&altered.0, limits()).unwrap());
    let connection = Connection::open(&altered.0).unwrap();
    connection
        .execute_batch(
            "DROP TABLE retained; CREATE TABLE retained (topic TEXT PRIMARY KEY, data BLOB)",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        RowStore::open(&altered.0, limits()),
        Err(DurableError::Schema(_))
    ));
    let foreign = TempDb::new();
    Connection::open(&foreign.0)
        .unwrap()
        .execute("CREATE TABLE important(value TEXT)", [])
        .unwrap();
    assert!(matches!(
        RowStore::open(&foreign.0, limits()),
        Err(DurableError::Schema(_))
    ));
    assert!(matches!(
        read_contents(&foreign.0, limits()),
        Err(DurableError::Schema(_))
    ));
}

#[test]
fn row_store_shares_the_exclusive_owner_lock() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    let owner = RowStore::open(&path.0, limits()).unwrap();
    assert!(matches!(
        RowStore::open(&path.0, limits()),
        Err(DurableError::Locked)
    ));
    assert!(matches!(
        SqliteStore::open(&path.0, 8),
        Err(DurableError::Locked)
    ));
    assert_eq!(
        read_contents(&path.0, limits()).unwrap(),
        Contents::Rows(Rows::default())
    );
    drop(owner);
    assert!(RowStore::open(&path.0, limits()).is_ok());
}

#[test]
fn killed_uncommitted_row_batch_keeps_prior_rows() {
    let _serial = FILE_TESTS.lock().unwrap();
    if let Some(path) = std::env::var_os("LYRNORMQ_ROWS_CRASH_CHILD") {
        let store = RowStore::open(Path::new(&path), limits()).unwrap();
        store
            .connection()
            .execute_batch(
                "BEGIN IMMEDIATE; UPDATE sessions SET data = x'00' WHERE client_id = 'a'; DELETE FROM retained",
            )
            .unwrap();
        println!("LYRNORMQ_UNCOMMITTED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
    let path = TempDb::new();
    {
        let mut store = RowStore::open(&path.0, limits()).unwrap();
        store
            .apply(&[
                RowChange::PutSession("a".into(), b"prior".to_vec()),
                RowChange::PutRetained("t".into(), b"kept".to_vec()),
            ])
            .unwrap();
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable::rows::tests::killed_uncommitted_row_batch_keeps_prior_rows",
            "--nocapture",
        ])
        .env("LYRNORMQ_ROWS_CRASH_CHILD", &path.0)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut reader = std::io::BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            reader.read_line(&mut line).unwrap(),
            0,
            "crash child exited early"
        );
        if line.contains("LYRNORMQ_UNCOMMITTED") {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let store = RowStore::open(&path.0, limits()).unwrap();
    assert_eq!(
        rows(&store),
        Rows {
            sessions: vec![("a".into(), b"prior".to_vec())],
            retained: vec![("t".into(), b"kept".to_vec())],
        }
    );
}
