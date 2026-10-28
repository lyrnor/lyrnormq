//! Conservative filter-language containment without recursion or topic expansion.
use mqtt_router::validate_filter;

/// Return true only if every topic matched by `requested` is matched by `grant`.
/// Each request must fit one grant: union coverage is intentionally not inferred.
/// Some equivalent forms (e.g. grant `+/#`, request `#`) are conservatively denied.
/// Root wildcards never grant explicit `$` topics. Invalid/shared filters fail closed.
pub fn covers_filter(grant: &str, requested: &str) -> bool {
    if !validate_filter(grant) || !validate_filter(requested) {
        return false;
    }
    let mut grants = grant.split('/');
    let mut requests = requested.split('/');
    let mut root = true;
    loop {
        match (grants.next(), requests.next()) {
            (None, None) => return true,
            (Some("#"), request) => {
                return !(root && request.is_some_and(|level| level.starts_with('$')));
            }
            (Some(g), Some(r)) => {
                if r == "#" || (g != "+" && g != r) {
                    return false;
                }
                if root && g == "+" && r.starts_with('$') {
                    return false;
                }
            }
            _ => return false,
        }
        root = false;
    }
}

#[cfg(test)]
mod tests;
