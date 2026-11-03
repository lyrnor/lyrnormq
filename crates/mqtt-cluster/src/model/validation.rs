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
                && (key.starts_with("c:") || key.starts_with("h:"))
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
                    .all(|f| mqtt_router::validate_topic_filter(f) && !f.starts_with("$share/"))
        }
        Command::Register {
            node,
            incarnation,
            address,
            ..
        } => {
            *node > 0
                && text(incarnation, 64)
                && text(address, 2048)
                && (address.starts_with("http://127.0.0.1:")
                    || address.starts_with("http://[::1]:")
                    || address.starts_with("https://"))
        }
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
                    .all(|f| mqtt_router::validate_topic_filter(f) && !f.starts_with("$share/"))
        }
    }
}
