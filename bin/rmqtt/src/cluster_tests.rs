use super::*;

#[test]
fn memory_replication_rejects_local_sqlite_mirror_before_opening_files() {
    let config: mqtt_cluster::Config = serde_json::from_value(serde_json::json!({
        "role":"broker", "cluster_id":"memory-options", "node_id":101,
        "listen":"127.0.0.1:7101", "data_dir":"unused", "token_file":"unused",
        "controllers":[
            {"id":1,"address":"http://127.0.0.1:7001"},
            {"id":2,"address":"http://127.0.0.1:7002"},
            {"id":3,"address":"http://127.0.0.1:7003"}
        ],
        "durable":{"replicas":[
            {"id":101,"address":"http://127.0.0.1:7101"},
            {"id":102,"address":"http://127.0.0.1:7102"},
            {"id":103,"address":"http://127.0.0.1:7103"}
        ]}
    }))
    .unwrap();
    assert!(validate_data_options(Some(&config), Some(Path::new("not-created.sqlite"))).is_err());
    assert!(validate_data_options(Some(&config), None).is_ok());
    assert!(validate_data_options(None, Some(Path::new("standalone.sqlite"))).is_ok());
    let legacy = mqtt_cluster::Config {
        durable: None,
        ..config
    };
    assert!(validate_data_options(Some(&legacy), Some(Path::new("legacy.sqlite"))).is_ok());
}
