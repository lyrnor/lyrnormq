//! Memory replicas may rejoin only after this live process has been repaired.
use super::*;

#[derive(Debug, PartialEq, Eq)]
enum Admission {
    Register,
    WaitForRepair,
}

pub(super) fn storage_matches(config: &Config, metadata: &Metadata) -> bool {
    match (&config.durable, &metadata.data_mode) {
        (None, Some(DataMode::Legacy)) => true,
        (Some(configured), Some(DataMode::Memory(peers))) => {
            configured.replicas.len() == peers.len()
                && configured.replicas.iter().all(|replica| {
                    peers
                        .iter()
                        .any(|(id, address)| *id == replica.id && *address == replica.address)
                })
        }
        _ => false,
    }
}
fn admission(
    config: &Config,
    metadata: &Metadata,
    identity: Option<&str>,
) -> io::Result<Admission> {
    if metadata.data_mode.is_none() {
        return Ok(Admission::Register);
    }
    if !storage_matches(config, metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster storage/membership mismatch; initialize a separate memory cluster without deleting old data",
        ));
    }
    if metadata.excluded.contains(&config.node_id)
        || metadata
            .disk_ids
            .get(&config.node_id)
            .is_some_and(|anchored| Some(anchored.as_str()) != identity)
    {
        return Ok(Admission::WaitForRepair);
    }
    Ok(Admission::Register)
}

impl BrokerRuntime {
    pub(super) async fn register_startup(&self) -> io::Result<()> {
        let rpc = self.rpc().await?;
        let mut deadline = tokio::time::Instant::now() + Duration::from_secs(12);
        let mut replacement = false;
        let mut capability_checked = self.config.durable.is_none();
        loop {
            if !capability_checked && let Ok(status) = rpc.status().await {
                if !matches!(status.data_storage.as_str(), "none" | "memory") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "controller does not advertise memory replica support; use current controllers with separate metadata directories",
                    ));
                }
                capability_checked = true;
            }
            if capability_checked && let Ok(metadata) = rpc.metadata().await {
                let identity = self.journal.as_ref().map(|j| j.disk_id());
                match admission(&self.config, &metadata, identity)? {
                    Admission::WaitForRepair => {
                        if !replacement {
                            replacement = true;
                            deadline = tokio::time::Instant::now() + Duration::from_secs(600);
                            tracing::warn!(
                                event = "memory_replica_waiting_for_repair",
                                "empty memory replica is excluded from service; run cluster-repair against this live normal Broker process"
                            );
                        }
                    }
                    Admission::Register => {
                        if rpc
                            .mutate(Command::Register {
                                node: self.config.node_id,
                                incarnation: self.incarnation.clone(),
                                address: self.config.address(),
                                disk_id: identity.map(str::to_owned),
                                memory: self.config.durable.is_some(),
                                replicas: self.config.durable.as_ref().map(|d| {
                                    d.replicas
                                        .iter()
                                        .map(|p| (p.id, p.address.clone()))
                                        .collect()
                                }),
                                lease_ms: self.config.lease_ms,
                                expires_at: controller::now() + self.config.lease_ms,
                            })
                            .await
                            .is_ok()
                        {
                            // Older controllers may ignore an unknown Register.memory
                            // field. A fresh authoritative read must prove this mode
                            // before the process can start metadata/lease tasks.
                            if let Ok(confirmed) = rpc.metadata().await {
                                let decision = admission(&self.config, &confirmed, identity)?;
                                if decision == Admission::Register
                                    && storage_matches(&self.config, &confirmed)
                                    && confirmed
                                        .nodes
                                        .get(&self.config.node_id)
                                        .is_some_and(|n| n.incarnation == self.incarnation)
                                {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    if replacement {
                        "memory replica catch-up did not complete within 600s; no empty votes or MQTT admissions were allowed"
                    } else {
                        "cluster registration did not complete within 12s"
                    },
                ));
            }
            tokio::time::sleep(Duration::from_millis(if replacement { 250 } else { 100 })).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        serde_json::from_value(serde_json::json!({
            "role":"broker", "cluster_id":"memory-startup", "node_id":101,
            "listen":"127.0.0.1:7101", "data_dir":"unused", "token_file":"unused",
            "controllers":[{"id":1,"address":"http://127.0.0.1:7001"},
                {"id":2,"address":"http://127.0.0.1:7002"},
                {"id":3,"address":"http://127.0.0.1:7003"}],
            "durable":{"replicas":[{"id":101,"address":"http://127.0.0.1:7101"},
                {"id":102,"address":"http://127.0.0.1:7102"},
                {"id":103,"address":"http://127.0.0.1:7103"}]}
        }))
        .expect("config")
    }
    fn members(config: &Config) -> Vec<(u64, String)> {
        config
            .durable
            .as_ref()
            .expect("replicas")
            .replicas
            .iter()
            .map(|p| (p.id, p.address.clone()))
            .collect()
    }
    #[test]
    fn blank_boot_waits_until_exact_repaired_identity_is_anchored() {
        let c = config();
        let mut m = Metadata {
            data_mode: Some(DataMode::Memory(members(&c))),
            ..Default::default()
        };
        m.disk_ids.insert(c.node_id, "old-boot".into());
        assert_eq!(
            admission(&c, &m, Some("new-boot")).unwrap(),
            Admission::WaitForRepair
        );
        m.disk_ids.insert(c.node_id, "new-boot".into());
        m.excluded.insert(c.node_id);
        assert_eq!(
            admission(&c, &m, Some("new-boot")).unwrap(),
            Admission::WaitForRepair
        );
        m.excluded.clear();
        assert_eq!(
            admission(&c, &m, Some("new-boot")).unwrap(),
            Admission::Register
        );
    }
    #[test]
    fn legacy_or_different_membership_is_not_silently_reinterpreted_as_memory() {
        let c = config();
        for mode in [DataMode::Legacy, DataMode::Durable(members(&c))] {
            let m = Metadata {
                data_mode: Some(mode),
                ..Default::default()
            };
            assert_eq!(
                admission(&c, &m, Some("new")).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let mut peers = members(&c);
        peers[0].1 = "http://127.0.0.1:7999".into();
        let m = Metadata {
            data_mode: Some(DataMode::Memory(peers)),
            ..Default::default()
        };
        assert!(admission(&c, &m, Some("new")).is_err());
        assert_eq!(
            admission(&c, &Metadata::default(), Some("new")).unwrap(),
            Admission::Register
        );
    }
}
