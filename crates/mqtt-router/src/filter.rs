//! Topic filter validation and standalone matching.
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

pub(super) fn valid_topic(topic: &str) -> bool {
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

#[cfg(test)]
mod tests;
