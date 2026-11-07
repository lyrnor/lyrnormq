# Phase7z — opt-in RocksDB data replicas

Supersedes only the Phase7f sentence that left RocksDB unimplemented. Phase7f
memory replicas remain the default. Controller bbolt and Raft WAL, rules and
access SQLite, and standalone `sessions.sqlite` stay on their current stores.

## Durability

`cluster.backend = "rocksdb"` opens `ReplicaStore` on `data_dir/broker/rocks`.
The in-memory record map, quotas, fences and repair rules are unchanged. A
write or fence is acknowledged only after that record's RocksDB batch has
synced. Repair staging and the repair cursor sync in the same way. Reads do
not sync.

`disk_id` is created on the first open of an empty database and reused after
restart. A restarted process whose identity matches the Controller anchor and
whose repair generation is current may vote. A blank database, a replaced
disk, or a mismatched identity does not vote and waits for the existing repair
path. Whole-group disk loss still fails closed against live Controller anchors.
`data.sqlite` and `memory.identity` are refused and left untouched. A memory
directory is not imported.

Two acknowledgements mean two synced replicas. This does not make an MQTT
acknowledgement a cross-recipient transaction.

## Mode and upgrade

`DataMode::Rocks` is distinct from `DataMode::Memory`. A cluster cannot mix
them. Register carries `rocks` with serde default false. Older controllers
ignore that field and would store Memory, so every controller is upgraded
before any broker writes `rocks: true`. Brokers check that status reports
`none` or `rocksdb` before joining a RocksDB group.

Admission bounds stay 100,000 records, 8 MiB per record, and 256 MiB of
accounted canonical bytes. Disk IO runs on one `rmqtt-rocks` thread with a
bounded command queue. Async tasks do not fsync and do not hold the map lock
across `.await`.
