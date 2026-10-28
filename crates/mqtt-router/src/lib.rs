//! MQTT 3.1.1 subscription routing with an iterative topic-level trie.
//!
//! Callers enforce connection and subscription quotas. Operations are synchronous;
//! an external lock must not be held across asynchronous work. Shared subscriptions
//! and retained message storage remain outside this module.
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn valid_string(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= u16::MAX as usize
        && !value.chars().any(|c| {
            c == '\0' || ('\u{fdd0}'..='\u{fdef}').contains(&c) || (u32::from(c) & 0xffff) >= 0xfffe
        })
}

/// Validate a Topic Filter. Empty levels are valid; `+` occupies one entire
/// level and `#` occupies the final entire level. `$share/` is unsupported.
pub fn validate_filter(filter: &str) -> bool {
    if !valid_string(filter) || filter.starts_with("$share/") {
        return false;
    }
    let mut levels = filter.split('/').peekable();
    while let Some(level) = levels.next() {
        if level.contains('#') && (level != "#" || levels.peek().is_some()) {
            return false;
        }
        if level.contains('+') && level != "+" {
            return false;
        }
    }
    true
}

fn valid_topic(topic: &str) -> bool {
    valid_string(topic) && !topic.contains(['+', '#'])
}

/// Match a valid filter against a valid Topic Name. Root wildcards do not match
/// names starting with `$`; an explicit `$` first level is required.
pub fn matches(filter: &str, topic: &str) -> bool {
    if !validate_filter(filter) || !valid_topic(topic) {
        return false;
    }
    if topic.starts_with('$') && matches!(filter.split('/').next(), Some("+" | "#")) {
        return false;
    }
    let mut topics = topic.split('/');
    for level in filter.split('/') {
        if level == "#" {
            return true;
        }
        match topics.next() {
            Some(actual) if level == "+" || level == actual => {}
            _ => return false,
        }
    }
    topics.next().is_none()
}

fn merge_qos(target: &mut BTreeMap<u64, u8>, source: &BTreeMap<u64, u8>) {
    for (&client, &qos) in source {
        target
            .entry(client)
            .and_modify(|granted| *granted = (*granted).max(qos))
            .or_insert(qos);
    }
}

#[derive(Default)]
struct Node {
    children: BTreeMap<String, usize>,
    clients: BTreeMap<u64, u8>,
    descendants: BTreeMap<u64, u8>,
}
impl Node {
    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.clients.is_empty() && self.descendants.is_empty()
    }
}

/// Topic-level trie with a reverse client index for bounded disconnect cleanup.
/// Flat node ownership avoids recursion even for adversarial, deeply nested topics.
pub struct Router {
    nodes: Vec<Node>,
    free: Vec<usize>,
    subscriptions: HashMap<u64, BTreeSet<String>>,
}
impl Default for Router {
    fn default() -> Self {
        Self {
            nodes: vec![Node::default()],
            free: Vec::new(),
            subscriptions: HashMap::new(),
        }
    }
}
impl Router {
    /// Insert or replace a subscription with QoS 0.
    /// Use [`Self::subscribe_qos`] to grant a different maximum delivery QoS.
    pub fn subscribe(&mut self, client: u64, filter: String) -> bool {
        self.subscribe_qos(client, filter, 0)
    }

    /// Insert or replace the granted maximum QoS of an exact subscription.
    /// Returns false for invalid filters or QoS outside 0..=2, without mutation.
    /// Replacement changes QoS but does not consume another subscription slot.
    pub fn subscribe_qos(&mut self, client: u64, filter: String, qos: u8) -> bool {
        if qos > 2 || !validate_filter(&filter) {
            return false;
        }
        let mut node = 0;
        for level in filter.split('/') {
            if level == "#" {
                self.nodes[node].descendants.insert(client, qos);
                self.subscriptions.entry(client).or_default().insert(filter);
                return true;
            }
            node = match self.nodes[node].children.get(level).copied() {
                Some(index) => index,
                None => {
                    let index = match self.free.pop() {
                        Some(index) => index,
                        None => {
                            self.nodes.push(Node::default());
                            self.nodes.len() - 1
                        }
                    };
                    self.nodes[node].children.insert(level.to_owned(), index);
                    index
                }
            };
        }
        self.nodes[node].clients.insert(client, qos);
        self.subscriptions.entry(client).or_default().insert(filter);
        true
    }

    /// Remove an exact filter. Removing absent filters is idempotent.
    pub fn unsubscribe(&mut self, client: u64, filter: &str) {
        let Some(filters) = self.subscriptions.get_mut(&client) else {
            return;
        };
        if !filters.remove(filter) {
            return;
        }
        if filters.is_empty() {
            self.subscriptions.remove(&client);
        }
        let mut node = 0;
        let mut path = Vec::new();
        let mut hash = false;
        for level in filter.split('/') {
            if level == "#" {
                hash = true;
                break;
            }
            let Some(next) = self.nodes[node].children.get(level).copied() else {
                return;
            };
            path.push((node, level, next));
            node = next;
        }
        if hash {
            self.nodes[node].descendants.remove(&client);
        } else {
            self.nodes[node].clients.remove(&client);
        }
        for (parent, level, child) in path.into_iter().rev() {
            if !self.nodes[child].is_empty() {
                break;
            }
            self.nodes[parent].children.remove(level);
            self.free.push(child);
        }
    }

    /// Remove all subscriptions for a disconnected connection generation.
    pub fn remove_client(&mut self, client: u64) {
        let Some(filters) = self.subscriptions.get(&client).cloned() else {
            return;
        };
        for filter in filters {
            self.unsubscribe(client, &filter);
        }
    }

    /// Return unique matching connection generations in ascending order.
    pub fn matching_clients(&self, topic: &str) -> Vec<u64> {
        self.matching_qos(topic)
            .into_iter()
            .map(|(client, _)| client)
            .collect()
    }

    /// Return unique clients in ascending order with the maximum QoS granted by
    /// any matching subscription. The caller caps this by the published QoS.
    /// Matching visits only applicable trie branches, never the reverse index.
    pub fn matching_qos(&self, topic: &str) -> Vec<(u64, u8)> {
        if !valid_topic(topic) {
            return Vec::new();
        }
        let levels: Vec<_> = topic.split('/').collect();
        let system = topic.starts_with('$');
        let mut pending = vec![(0, 0)];
        let mut found = BTreeMap::new();
        while let Some((node, depth)) = pending.pop() {
            let current = &self.nodes[node];
            if depth != 0 || !system {
                merge_qos(&mut found, &current.descendants);
            }
            if depth == levels.len() {
                merge_qos(&mut found, &current.clients);
                continue;
            }
            if let Some(&child) = current.children.get(levels[depth]) {
                pending.push((child, depth + 1));
            }
            if (depth != 0 || !system)
                && let Some(&child) = current.children.get("+")
            {
                pending.push((child, depth + 1));
            }
        }
        found.into_iter().collect()
    }

    /// Number of distinct filters held by a connection generation.
    pub fn subscription_count(&self, client: u64) -> usize {
        self.subscriptions.get(&client).map_or(0, BTreeSet::len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filter_validation() {
        for valid in ["#", "+", "/", "a/", "/+", "a/#", "a/+/c", "$SYS/#", "a//b"] {
            assert!(validate_filter(valid), "{valid}");
        }
        for invalid in [
            "",
            "a#",
            "a+",
            "a/#/b",
            "##",
            "+a",
            "$share/g/a",
            "a\0b",
            "\u{ffff}",
        ] {
            assert!(!validate_filter(invalid), "{invalid}");
        }
        assert!(!validate_filter(&"a".repeat(65536)));
    }
    #[test]
    fn wildcard_edges() {
        for (filter, topic, expected) in [
            ("sport/#", "sport", true),
            ("sport/#", "sport/", true),
            ("sport/+", "sport", false),
            ("sport/+", "sport/", true),
            ("+", "/", false),
            ("+/+", "/", true),
            ("#", "$SYS/x", false),
            ("+/#", "$SYS/x", false),
            ("$SYS/#", "$SYS", true),
            ("$SYS/+", "$SYS/x", true),
            ("a/+", "a/$b", true),
            ("a", "A", false),
            ("#", "", false),
            ("#", "bad+", false),
            ("a/#", "ab", false),
        ] {
            assert_eq!(matches(filter, topic), expected, "{filter} {topic}");
        }
    }
    #[test]
    fn routing_dedup_and_cleanup() {
        let mut router = Router::default();
        for filter in ["a/#", "a/+", "a/b"] {
            assert!(router.subscribe(1, filter.into()));
        }
        assert!(router.subscribe(2, "a/b".into()));
        assert!(router.subscribe(1, "a/b".into()));
        assert_eq!(router.subscription_count(1), 3);
        assert_eq!(router.matching_clients("a/b"), vec![1, 2]);
        router.unsubscribe(1, "a/#");
        assert_eq!(router.matching_clients("a"), Vec::<u64>::new());
        router.remove_client(1);
        assert_eq!(router.subscription_count(1), 0);
        assert_eq!(router.matching_clients("a/b"), vec![2]);
        router.remove_client(2);
        router.remove_client(2);
        assert!(router.matching_clients("a/b").is_empty());
        assert_eq!(router.nodes[0].children.len(), 0);
        assert!(router.subscribe(3, "x/y".into()));
        assert_eq!(router.matching_clients("x/y"), vec![3]);
    }
    #[test]
    fn trie_agrees_with_independent_matching() {
        let filters = [
            "#", "+", "+/+", "a", "a/#", "a/+", "/#", "$SYS/#", "$SYS/+", "a//+", "a/$x",
        ];
        let mut router = Router::default();
        for (client, filter) in filters.iter().enumerate() {
            assert!(router.subscribe(client as u64, (*filter).into()));
        }
        for topic in [
            "a", "a/", "a/b", "a//b", "/", "//", "$SYS", "$SYS/", "$SYS/a", "a/$x",
        ] {
            let expected: Vec<_> = filters
                .iter()
                .enumerate()
                .filter(|(_, f)| matches(f, topic))
                .map(|(id, _)| id as u64)
                .collect();
            assert_eq!(router.matching_clients(topic), expected, "{topic}");
        }
    }
    #[test]
    fn deep_topic_is_iterative_and_nodes_are_reused() {
        let mut router = Router::default();
        let topic = "/".repeat(65534);
        assert!(router.subscribe(1, topic.clone()));
        assert_eq!(router.matching_clients(&topic), vec![1]);
        router.remove_client(1);
        let count = router.nodes.len();
        assert!(router.subscribe(2, topic.clone()));
        assert_eq!(router.nodes.len(), count);
        router.remove_client(2);
    }
    #[test]
    fn overlapping_qos_uses_maximum_and_replacement_updates_grant() {
        let mut router = Router::default();
        assert!(router.subscribe_qos(1, "a/#".into(), 0));
        assert!(router.subscribe_qos(1, "a/+".into(), 2));
        assert!(router.subscribe_qos(1, "a/b".into(), 1));
        assert!(router.subscribe_qos(2, "a/#".into(), 1));
        assert_eq!(router.matching_qos("a/b"), vec![(1, 2), (2, 1)]);
        assert_eq!(router.matching_clients("a/b"), vec![1, 2]);
        assert_eq!(router.matching_qos("a"), vec![(1, 0), (2, 1)]);
        assert!(router.subscribe_qos(1, "a/+".into(), 0));
        assert_eq!(router.subscription_count(1), 3);
        assert_eq!(router.matching_qos("a/b"), vec![(1, 1), (2, 1)]);
        router.unsubscribe(1, "a/b");
        assert_eq!(router.matching_qos("a/b"), vec![(1, 0), (2, 1)]);
        assert!(router.subscribe_qos(1, "a/#".into(), 2));
        assert_eq!(router.matching_qos("a/b"), vec![(1, 2), (2, 1)]);
        // Compatibility wrapper also replaces an existing grant with QoS0.
        assert!(router.subscribe(1, "a/#".into()));
        assert_eq!(router.matching_qos("a/b"), vec![(1, 0), (2, 1)]);
    }

    #[test]
    fn qos_validation_is_atomic_and_system_topics_are_isolated() {
        let mut router = Router::default();
        assert!(router.subscribe_qos(1, "#".into(), 2));
        assert!(router.subscribe_qos(1, "$SYS/#".into(), 1));
        assert!(!router.subscribe_qos(1, "$SYS/#".into(), 3));
        assert!(!router.subscribe_qos(2, "invalid/#/x".into(), 2));
        assert!(!router.subscribe_qos(2, "valid".into(), 255));
        assert_eq!(router.subscription_count(1), 2);
        assert_eq!(router.subscription_count(2), 0);
        assert_eq!(router.matching_qos("$SYS/a"), vec![(1, 1)]);
        assert_eq!(router.matching_qos("a"), vec![(1, 2)]);
        assert!(router.matching_qos("bad+").is_empty());
        assert!(router.matching_qos("").is_empty());
    }

    #[test]
    fn qos_cleanup_and_node_reuse_leave_no_stale_grants() {
        let mut router = Router::default();
        assert!(router.subscribe_qos(1, "a/b".into(), 2));
        assert!(router.subscribe_qos(1, "a/#".into(), 2));
        assert!(router.subscribe_qos(2, "a/b".into(), 1));
        router.remove_client(1);
        assert_eq!(router.matching_qos("a/b"), vec![(2, 1)]);
        router.remove_client(2);
        let allocated = router.nodes.len();
        for _ in 0..100 {
            assert!(router.subscribe_qos(3, "x/y".into(), 0));
            assert_eq!(router.matching_qos("x/y"), vec![(3, 0)]);
            assert!(router.matching_qos("a/b").is_empty());
            assert_eq!(router.nodes.len(), allocated);
            router.remove_client(3);
            assert!(router.matching_qos("x/y").is_empty());
            assert!(router.subscriptions.is_empty());
            assert!(router.nodes.iter().all(Node::is_empty));
        }
    }

    #[test]
    fn qos_trie_matches_reference_across_replacements_and_removals() {
        let filters = ["#", "+", "a/#", "a/+", "a/b", "/#", "$SYS/#", "a//+"];
        let topics = ["a", "a/", "a/b", "a//b", "/", "$SYS", "$SYS/a"];
        let mut router = Router::default();
        let mut reference = BTreeMap::new();
        for revision in 0..3 {
            for client in 0..3u64 {
                for (position, filter) in filters.iter().enumerate() {
                    let qos = ((position + revision + client as usize) % 3) as u8;
                    assert!(router.subscribe_qos(client, (*filter).into(), qos));
                    reference.insert((client, *filter), qos);
                }
            }
            for topic in topics {
                let mut expected = BTreeMap::<u64, u8>::new();
                for (&(client, filter), &qos) in &reference {
                    if matches(filter, topic) {
                        expected
                            .entry(client)
                            .and_modify(|max| *max = (*max).max(qos))
                            .or_insert(qos);
                    }
                }
                assert_eq!(
                    router.matching_qos(topic),
                    expected.into_iter().collect::<Vec<_>>()
                );
            }
        }
        router.unsubscribe(1, "a/#");
        router.remove_client(2);
        reference.retain(|&(client, filter), _| client != 2 && !(client == 1 && filter == "a/#"));
        for topic in topics {
            let mut expected = BTreeMap::<u64, u8>::new();
            for (&(client, filter), &qos) in &reference {
                if matches(filter, topic) {
                    expected
                        .entry(client)
                        .and_modify(|max| *max = (*max).max(qos))
                        .or_insert(qos);
                }
            }
            assert_eq!(
                router.matching_qos(topic),
                expected.into_iter().collect::<Vec<_>>()
            );
        }
    }
}
