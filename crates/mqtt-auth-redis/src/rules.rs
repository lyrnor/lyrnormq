use crate::Error;
use mqtt_auth::covers_filter;
use mqtt_router::matches_topic_filter;
use serde::Deserialize;

pub(crate) const MAX_FIELD_BYTES: usize = 1024 * 1024 + 64 * 1024;
const MAX_RULES: usize = 256;
const MAX_FILTER: usize = 65535;
const MAX_FILTER_BYTES: usize = 1024 * 1024;

pub(crate) struct Rule {
    pub effect: bool,
    pub filter: String,
    pub priority: i64,
}
#[derive(Deserialize)]
struct RuleRecord {
    effect: String,
    topic_filter: String,
    priority: i64,
}
// Deny rules intersect a requested subscription; containment alone would let a
// broad subscription bypass a narrower denial. Root wildcards exclude $ topics.
fn intersects(a: &str, b: &str) -> bool {
    let mut a = a.split('/');
    let mut b = b.split('/');
    let mut root = true;
    loop {
        match (a.next(), b.next()) {
            (None, None) => return true,
            (Some("#"), other) | (other, Some("#")) => {
                return !(root && other.is_some_and(|s| s.starts_with('$')));
            }
            (Some(x), Some(y)) => {
                if root && ((x == "+" && y.starts_with('$')) || (y == "+" && x.starts_with('$'))) {
                    return false;
                }
                if x != y && x != "+" && y != "+" {
                    return false;
                }
            }
            _ => return false,
        }
        root = false;
    }
}
pub(crate) fn decide(rules: &[Rule], resource: &str, subscription: bool) -> bool {
    rules
        .iter()
        .filter(|rule| {
            if subscription {
                if rule.effect {
                    covers_filter(&rule.filter, resource)
                } else {
                    intersects(&rule.filter, resource)
                }
            } else {
                matches_topic_filter(&rule.filter, resource)
            }
        })
        .max_by_key(|rule| (rule.priority, !rule.effect))
        .is_some_and(|rule| rule.effect)
}
pub(crate) fn parse_rules(
    bytes: &[u8],
    rules: &mut Vec<Rule>,
    total: &mut usize,
) -> Result<(), Error> {
    if bytes.len() > MAX_FIELD_BYTES {
        return Err(Error::InvalidResult);
    }
    let rows: Vec<RuleRecord> = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResult)?;
    for row in rows {
        if rules.len() >= MAX_RULES {
            return Err(Error::InvalidResult);
        }
        *total = total.saturating_add(row.topic_filter.len());
        if row.topic_filter.len() > MAX_FILTER
            || *total > MAX_FILTER_BYTES
            || !mqtt_router::validate_topic_filter(&row.topic_filter)
            || !matches!(row.effect.as_str(), "allow" | "deny")
        {
            return Err(Error::InvalidResult);
        }
        rules.push(Rule {
            effect: row.effect == "allow",
            filter: row.topic_filter,
            priority: row.priority,
        });
    }
    Ok(())
}
