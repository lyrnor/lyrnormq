use mqtt_auth::covers_filter;
use mqtt_router::matches_topic_filter;

pub(crate) struct Rule {
    pub effect: bool,
    pub filter: String,
    pub priority: i64,
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
        .filter(|r| {
            if subscription {
                if r.effect {
                    covers_filter(&r.filter, resource)
                } else {
                    intersects(&r.filter, resource)
                }
            } else {
                matches_topic_filter(&r.filter, resource)
            }
        })
        .max_by_key(|r| (r.priority, !r.effect))
        .is_some_and(|r| r.effect)
}
