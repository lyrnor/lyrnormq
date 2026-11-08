//! Directory projections for unified `/api/v1/*` management reads.
use super::*;
use mqtt_cluster_core::{
    ManagementCounts, ManagementPage, ManagementQuery, ManagementRetained, ManagementSession,
    ManagementSubscription,
};

#[derive(Clone)]
struct SessionRow {
    client_id: String,
    online: bool,
    persistent: bool,
    filters: Vec<String>,
    node_id: u64,
}

fn search_hit(search: &str, haystack: &str) -> bool {
    search.is_empty() || haystack.contains(search)
}

fn collect_sessions(metadata: &Metadata) -> Vec<SessionRow> {
    let mut rows = Vec::with_capacity(metadata.clients.len());
    let mut online_ids = BTreeSet::new();
    for (client_id, client) in &metadata.clients {
        online_ids.insert(client_id.clone());
        rows.push(SessionRow {
            client_id: client_id.clone(),
            online: true,
            persistent: metadata
                .durable
                .contains_key(&durable::client_key(client_id)),
            filters: client.filters.clone(),
            node_id: client.node,
        });
    }
    for (key, owner) in &metadata.durable {
        let Some(client_id) = key.strip_prefix("c:") else {
            continue;
        };
        if online_ids.contains(client_id) {
            continue;
        }
        rows.push(SessionRow {
            client_id: client_id.to_owned(),
            online: false,
            persistent: true,
            filters: owner.filters.clone(),
            node_id: owner.node,
        });
    }
    rows.sort_by(|left, right| left.client_id.cmp(&right.client_id));
    rows
}

fn to_session(row: &SessionRow) -> ManagementSession {
    ManagementSession {
        client_id: row.client_id.clone(),
        online: row.online,
        protocol: "unknown".to_owned(),
        principal: String::new(),
        persistent: row.persistent,
        subscriptions: row.filters.len(),
        queued: 0,
        inflight: 0,
        receive_maximum: None,
        session_expiry_interval: 0,
        node_id: row.node_id,
        directory: true,
    }
}

fn filter_sessions(rows: Vec<SessionRow>, query: &ManagementQuery) -> Vec<SessionRow> {
    rows.into_iter()
        .filter(|row| {
            (query.state.is_empty() || row.online == (query.state == "online"))
                && (query.protocol.is_empty() || query.protocol == "unknown")
                && query.node.is_none_or(|node| node == row.node_id)
                && search_hit(&query.search, &row.client_id)
        })
        .collect()
}

fn page_items<T>(mut items: Vec<T>, query: &ManagementQuery) -> ManagementPage<T> {
    let total = items.len();
    let start = query.offset.min(total);
    let end = start.saturating_add(query.limit).min(total);
    items = items.drain(start..end).collect();
    ManagementPage {
        items,
        total,
        offset: query.offset,
        limit: query.limit,
    }
}

fn collect_subscriptions(
    rows: &[SessionRow],
    query: &ManagementQuery,
) -> Vec<ManagementSubscription> {
    let mut items = Vec::new();
    for row in rows {
        if query.node.is_some_and(|node| node != row.node_id) {
            continue;
        }
        for filter in &row.filters {
            if !search_hit(&query.search, filter) && !search_hit(&query.search, &row.client_id) {
                continue;
            }
            items.push(ManagementSubscription {
                client_id: row.client_id.clone(),
                filter: filter.clone(),
                qos: 0,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                identifier: None,
                node_id: row.node_id,
                directory: true,
            });
        }
    }
    items.sort_by(|left, right| {
        left.filter
            .cmp(&right.filter)
            .then_with(|| left.client_id.cmp(&right.client_id))
    });
    items
}

fn collect_retained_topics(metadata: &Metadata, query: &ManagementQuery) -> Vec<(String, u64)> {
    let mut topics = Vec::new();
    for (key, owner) in &metadata.durable {
        if owner.filters.is_empty() {
            continue;
        }
        let Some(topic) = key.strip_prefix("r:") else {
            continue;
        };
        if query.node.is_some_and(|node| node != owner.node) {
            continue;
        }
        if !search_hit(&query.search, topic) {
            continue;
        }
        topics.push((topic.to_owned(), owner.node));
    }
    topics.sort_by(|left, right| left.0.cmp(&right.0));
    topics
}

/// Durable replicas keep live ownership in `durable` rather than Claim `clients`.
/// `LocalBroker::online_client_ids` lists connections currently online on this node.
async fn local_online(runtime: &BrokerRuntime) -> BTreeSet<String> {
    let Ok(local) = runtime.local() else {
        return BTreeSet::new();
    };
    match local.online_client_ids().await {
        Ok(rows) => rows.into_iter().collect(),
        Err(_) => BTreeSet::new(),
    }
}

fn apply_local_online(rows: &mut [SessionRow], online: &BTreeSet<String>) {
    for row in rows {
        if online.contains(&row.client_id) {
            row.online = true;
        }
    }
}

pub(super) async fn management_counts(runtime: &BrokerRuntime) -> ManagementCounts {
    let online = local_online(runtime).await;
    let metadata = runtime
        .metadata
        .read()
        .unwrap_or_else(PoisonError::into_inner);
    let retained_messages = metadata
        .durable
        .iter()
        .filter(|(key, owner)| key.starts_with("r:") && !owner.filters.is_empty())
        .count();
    let mut rows = collect_sessions(&metadata);
    apply_local_online(&mut rows, &online);
    let online_sessions = rows.iter().filter(|row| row.online).count();
    ManagementCounts {
        online_sessions,
        offline_sessions: rows.len().saturating_sub(online_sessions),
        subscriptions: rows.iter().map(|row| row.filters.len()).sum(),
        retained_messages,
    }
}

pub(super) async fn management_sessions(
    runtime: &BrokerRuntime,
    query: ManagementQuery,
) -> io::Result<ManagementPage<ManagementSession>> {
    query.validate()?;
    let online = local_online(runtime).await;
    let metadata = runtime
        .metadata
        .read()
        .unwrap_or_else(PoisonError::into_inner);
    let mut rows = collect_sessions(&metadata);
    apply_local_online(&mut rows, &online);
    let rows = filter_sessions(rows, &query);
    Ok(page_items(rows.iter().map(to_session).collect(), &query))
}

pub(super) async fn management_subscriptions(
    runtime: &BrokerRuntime,
    query: ManagementQuery,
) -> io::Result<ManagementPage<ManagementSubscription>> {
    query.validate()?;
    // Protocol/state filters are session-oriented; directory subscriptions ignore them.
    if !query.protocol.is_empty() && query.protocol != "unknown" {
        return Ok(ManagementPage {
            items: Vec::new(),
            total: 0,
            offset: query.offset,
            limit: query.limit,
        });
    }
    let online = local_online(runtime).await;
    let metadata = runtime
        .metadata
        .read()
        .unwrap_or_else(PoisonError::into_inner);
    let mut rows = collect_sessions(&metadata);
    apply_local_online(&mut rows, &online);
    let rows = filter_sessions(
        rows,
        &ManagementQuery {
            state: query.state.clone(),
            protocol: String::new(),
            search: String::new(),
            node: None,
            ..query.clone()
        },
    );
    Ok(page_items(collect_subscriptions(&rows, &query), &query))
}

pub(super) async fn management_session(
    runtime: &BrokerRuntime,
    client_id: &str,
) -> Option<ManagementSession> {
    let online = local_online(runtime).await;
    let metadata = runtime
        .metadata
        .read()
        .unwrap_or_else(PoisonError::into_inner);
    let mut rows = collect_sessions(&metadata);
    apply_local_online(&mut rows, &online);
    rows.into_iter()
        .find(|row| row.client_id == client_id)
        .map(|row| to_session(&row))
}

pub(super) async fn management_retained(
    runtime: &BrokerRuntime,
    query: ManagementQuery,
) -> io::Result<ManagementPage<ManagementRetained>> {
    query.validate()?;
    let topics = {
        let metadata = runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        collect_retained_topics(&metadata, &query)
    };
    let total = topics.len();
    let start = query.offset.min(total);
    let end = start.saturating_add(query.limit).min(total);
    let page: Vec<(String, u64)> = topics[start..end].to_vec();
    let mut bodies = BTreeMap::new();
    if !page.is_empty()
        && let Ok(messages) = runtime
            .matching_retained(page.iter().map(|(topic, _)| topic.clone()).collect())
            .await
    {
        for message in messages {
            bodies.insert(message.topic.clone(), message);
        }
    }
    let items = page
        .into_iter()
        .map(|(topic, node_id)| match bodies.remove(&topic) {
            Some(message) => ManagementRetained {
                topic,
                qos: message.qos,
                payload_bytes: message.payload.len(),
                expires_at_millis: message.expires_at,
                node_id: Some(node_id),
                directory: false,
            },
            None => ManagementRetained {
                topic,
                qos: 0,
                payload_bytes: 0,
                expires_at_millis: None,
                node_id: Some(node_id),
                directory: true,
            },
        })
        .collect();
    Ok(ManagementPage {
        items,
        total,
        offset: query.offset,
        limit: query.limit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Client, DurableOwner};

    fn metadata_fixture() -> Metadata {
        let mut metadata = Metadata::default();
        metadata.clients.insert(
            "online-a".into(),
            Client {
                node: 1,
                incarnation: "i1".into(),
                generation: 1,
                filters: vec!["sensors/#".into(), "alerts".into()],
            },
        );
        metadata.clients.insert(
            "online-b".into(),
            Client {
                node: 2,
                incarnation: "i2".into(),
                generation: 3,
                filters: vec!["metrics/+".into()],
            },
        );
        metadata.durable.insert(
            durable::client_key("offline-c"),
            DurableOwner {
                node: 2,
                incarnation: "i2".into(),
                generation: 4,
                epoch: 1,
                filters: vec!["archive/#".into()],
                logical_generation: 1,
                acquired_from: None,
            },
        );
        metadata.durable.insert(
            "r:weather/beijing".into(),
            DurableOwner {
                node: 1,
                incarnation: "i1".into(),
                generation: 1,
                epoch: 2,
                filters: vec!["weather/beijing".into()],
                logical_generation: 2,
                acquired_from: None,
            },
        );
        metadata.durable.insert(
            "r:cleared".into(),
            DurableOwner {
                node: 1,
                incarnation: "i1".into(),
                generation: 1,
                epoch: 3,
                filters: Vec::new(),
                logical_generation: 3,
                acquired_from: None,
            },
        );
        metadata
    }

    #[test]
    fn counts_separate_online_offline_and_retained() {
        let metadata = metadata_fixture();
        let sessions = collect_sessions(&metadata);
        let online_sessions = sessions.iter().filter(|row| row.online).count();
        let subscriptions: usize = sessions.iter().map(|row| row.filters.len()).sum();
        let retained_messages = metadata
            .durable
            .iter()
            .filter(|(key, owner)| key.starts_with("r:") && !owner.filters.is_empty())
            .count();
        assert_eq!(online_sessions, 2);
        assert_eq!(sessions.len() - online_sessions, 1);
        assert_eq!(subscriptions, 4);
        assert_eq!(retained_messages, 1);
    }

    #[test]
    fn sessions_filter_by_node_and_state() {
        let rows = filter_sessions(
            collect_sessions(&metadata_fixture()),
            &ManagementQuery {
                state: "offline".into(),
                node: Some(2),
                limit: 25,
                ..ManagementQuery::default()
            },
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].client_id, "offline-c");
    }

    #[test]
    fn subscriptions_expand_filters_in_stable_order() {
        let rows = collect_sessions(&metadata_fixture());
        let items = collect_subscriptions(&rows, &ManagementQuery::default());
        assert_eq!(
            items
                .iter()
                .map(|item| item.filter.as_str())
                .collect::<Vec<_>>(),
            vec!["alerts", "archive/#", "metrics/+", "sensors/#"]
        );
    }

    #[test]
    fn retained_skips_cleared_topics() {
        let topics = collect_retained_topics(&metadata_fixture(), &ManagementQuery::default());
        assert_eq!(topics, vec![("weather/beijing".into(), 1)]);
    }

    #[test]
    fn sessions_search_and_paging_are_stable() {
        let rows = filter_sessions(
            collect_sessions(&metadata_fixture()),
            &ManagementQuery {
                search: "online".into(),
                limit: 1,
                ..ManagementQuery::default()
            },
        );
        assert_eq!(rows.len(), 2);
        let page = page_items(rows.iter().map(to_session).collect::<Vec<_>>(), &ManagementQuery {
            search: "online".into(),
            limit: 1,
            offset: 1,
            ..ManagementQuery::default()
        });
        assert_eq!(page.total, 2);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].client_id, "online-b");
    }

    #[test]
    fn protocol_filter_hides_directory_rows_except_unknown() {
        let rows = filter_sessions(
            collect_sessions(&metadata_fixture()),
            &ManagementQuery {
                protocol: "5.0".into(),
                limit: 25,
                ..ManagementQuery::default()
            },
        );
        assert!(rows.is_empty());
        let unknown = filter_sessions(
            collect_sessions(&metadata_fixture()),
            &ManagementQuery {
                protocol: "unknown".into(),
                limit: 25,
                ..ManagementQuery::default()
            },
        );
        assert_eq!(unknown.len(), 3);
    }

    #[test]
    fn retained_filter_by_node_and_search() {
        let topics = collect_retained_topics(
            &metadata_fixture(),
            &ManagementQuery {
                node: Some(1),
                search: "beijing".into(),
                limit: 25,
                ..ManagementQuery::default()
            },
        );
        assert_eq!(topics, vec![("weather/beijing".into(), 1)]);
        let empty = collect_retained_topics(
            &metadata_fixture(),
            &ManagementQuery {
                node: Some(2),
                limit: 25,
                ..ManagementQuery::default()
            },
        );
        assert!(empty.is_empty());
    }
}
