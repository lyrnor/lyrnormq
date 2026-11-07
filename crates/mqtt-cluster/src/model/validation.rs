//! Shared syntactic command validation; batches remain internal to the worker.
use super::Command;
pub(crate) fn valid_command(command: &Command) -> bool {
    let text = |s: &str, max: usize| !s.is_empty() && s.len() <= max && !s.contains('\0');
    match command {
        Command::Batch { .. } => false,
        Command::RepairBegin {
            target,
            old_disk,
            new_disk,
            ..
        } => *target > 0 && text(old_disk, 32) && text(new_disk, 32),
        Command::RepairSeal {
            id,
            generation,
            manifest,
        } => text(id, 128) && *generation > 0 && manifest.valid(),
        Command::RepairCommit { id, generation, .. } | Command::RepairAbort { id, generation } => {
            text(id, 128) && *generation > 0
        }
        Command::Acquire {
            key,
            node,
            incarnation,
            ..
        }
        | Command::AcquireOnce {
            key,
            node,
            incarnation,
            ..
        } => {
            *node > 0
                && text(key, 65538)
                && (key.starts_with("c:") || key.starts_with("h:") || key.starts_with("r:"))
                && text(incarnation, 64)
        }
        Command::DurableFilters {
            key,
            node,
            incarnation,
            epoch,
            filters,
            ..
        } => {
            *node > 0
                && *epoch > 0
                && text(key, 65538)
                && text(incarnation, 64)
                && filters.len() <= 256
                && filters
                    .iter()
                    .all(|f| mqtt_router::validate_subscription_filter(f))
        }
        Command::Register {
            node,
            incarnation,
            address,
            ..
        } => *node > 0 && text(incarnation, 64) && crate::config::cluster_peer_address(address),
        Command::Heartbeat {
            node, incarnation, ..
        }
        | Command::Expire {
            node, incarnation, ..
        } => *node > 0 && text(incarnation, 64),
        Command::Claim {
            node,
            incarnation,
            client,
            generation,
        }
        | Command::Release {
            node,
            incarnation,
            client,
            generation,
        } => *node > 0 && *generation > 0 && text(incarnation, 64) && text(client, 65535),
        Command::Filters {
            generation,
            node,
            incarnation,
            client,
            filters,
        } => {
            *node > 0
                && text(incarnation, 64)
                && text(client, 65535)
                && *generation > 0
                && filters.len() <= 256
                && filters
                    .iter()
                    .all(|f| mqtt_router::validate_subscription_filter(f))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::valid_command;
    use crate::model::Command;

    fn register(address: &str) -> Command {
        Command::Register {
            node: 101,
            incarnation: "incarnation".into(),
            address: address.into(),
            replicas: Some(vec![(101, address.into())]),
            memory: true,
            rocks: false,
            disk_id: None,
            expires_at: 100,
            lease_ms: 3000,
        }
    }

    #[test]
    fn private_http_register_is_a_valid_command() {
        assert!(valid_command(&register("http://172.16.240.180:17101")));
        assert!(!valid_command(&register("http://47.84.224.186:17101")));
    }

    #[test]
    fn durable_filters_admit_valid_shared_keys() {
        let command = |filters: Vec<String>| Command::DurableFilters {
            key: "c:device".into(),
            node: 1,
            incarnation: "n1".into(),
            epoch: 1,
            filters,
            logical_generation: 1,
        };
        assert!(valid_command(&command(vec!["sensors/+".into()])));
        assert!(valid_command(&command(vec!["$share/g/sensors/+".into()])));
        assert!(!valid_command(&command(vec!["$share/+/sensors/+".into()])));
        assert!(!valid_command(&command(vec!["$share/g/".into()])));
    }
}
