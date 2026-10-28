//! Immutable JSON policy; hashes and work factors are validated before use.
use crate::{AccessControl, Action, AuthError, ConnectInfo, PolicyError, Principal, covers_filter};
use argon2::{Argon2, Params, PasswordHash, PasswordVerifier};
use mqtt_router::{matches, validate_filter};
use serde::Deserialize;
use std::{collections::HashMap, fmt};

/// Total policy input limit, enforced before deserialization.
pub const MAX_POLICY_BYTES: usize = 1024 * 1024;
const MAX_USERS: usize = 1024;
const MAX_RULES: usize = 128;
const MAX_USERNAME_BYTES: usize = 256;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyRecord {
    allow_anonymous: bool,
    users: Vec<UserRecord>,
    #[serde(default)]
    anonymous: Rules,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserRecord {
    username: String,
    password_hash: String,
    client_id_prefix: String,
    publish: Vec<String>,
    subscribe: Vec<String>,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rules {
    client_id_prefix: String,
    publish: Vec<String>,
    subscribe: Vec<String>,
}
struct User {
    password_hash: String,
    rules: Rules,
}
/// Local default-deny policy using Argon2id hashes and MQTT topic filters.
/// Debug prints only counts, never policy rules, usernames or hashes.
pub struct StaticPolicy {
    allow_anonymous: bool,
    anonymous: Rules,
    users: HashMap<String, User>,
}
impl fmt::Debug for StaticPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticPolicy")
            .field("allow_anonymous", &self.allow_anonymous)
            .field("user_count", &self.users.len())
            .finish_non_exhaustive()
    }
}
impl StaticPolicy {
    /// Reject unknown fields, duplicates, invalid filters and unbounded hash work.
    /// Does no filesystem IO and never formats supplied policy values into errors.
    pub fn from_json(bytes: &[u8]) -> Result<Self, PolicyError> {
        if bytes.len() > MAX_POLICY_BYTES {
            return Err(PolicyError::TooLarge);
        }
        let record: PolicyRecord =
            serde_json::from_slice(bytes).map_err(|_| PolicyError::InvalidJson)?;
        if record.users.len() > MAX_USERS {
            return Err(PolicyError::TooManyUsers);
        }
        validate_rules(&record.anonymous)?;
        let mut users = HashMap::with_capacity(record.users.len());
        for user in record.users {
            if !valid_string(&user.username, MAX_USERNAME_BYTES, false) {
                return Err(PolicyError::InvalidUser);
            }
            if users.contains_key(&user.username) {
                return Err(PolicyError::DuplicateUser);
            }
            let rules = Rules {
                client_id_prefix: user.client_id_prefix,
                publish: user.publish,
                subscribe: user.subscribe,
            };
            validate_rules(&rules)?;
            validate_hash(&user.password_hash)?;
            users.insert(
                user.username,
                User {
                    password_hash: user.password_hash,
                    rules,
                },
            );
        }
        Ok(Self {
            allow_anonymous: record.allow_anonymous,
            anonymous: record.anonymous,
            users,
        })
    }
    fn rules_for(&self, principal: &Principal) -> Option<&Rules> {
        if principal.name == "anonymous" {
            self.allow_anonymous.then_some(&self.anonymous)
        } else {
            principal
                .name
                .strip_prefix("user:")
                .and_then(|username| self.users.get(username))
                .map(|user| &user.rules)
        }
    }
}
impl AccessControl for StaticPolicy {
    fn authenticate(&self, info: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        let principal = match (info.username, info.password) {
            (None, None) => {
                if !self.allow_anonymous {
                    return Err(AuthError::NotAuthorized);
                }
                Principal::anonymous()
            }
            (Some(username), Some(password))
                if password.len() <= u16::MAX as usize
                    && valid_string(username, MAX_USERNAME_BYTES, false) =>
            {
                let user = self.users.get(username).ok_or(AuthError::BadCredentials)?;
                // Parsed and bounded at construction; policy contents cannot change.
                let parsed = PasswordHash::new(&user.password_hash)
                    .map_err(|_| AuthError::BadCredentials)?;
                Argon2::default()
                    .verify_password(password, &parsed)
                    .map_err(|_| AuthError::BadCredentials)?;
                Principal {
                    name: format!("user:{username}"),
                }
            }
            _ => return Err(AuthError::BadCredentials),
        };
        if self.authorize(&principal, Action::Connect, info.client_id) {
            Ok(principal)
        } else {
            Err(AuthError::NotAuthorized)
        }
    }
    fn authorize(&self, principal: &Principal, action: Action, resource: &str) -> bool {
        let Some(rules) = self.rules_for(principal) else {
            return false;
        };
        match action {
            Action::Connect => {
                valid_string(resource, u16::MAX as usize, false)
                    && resource.starts_with(&rules.client_id_prefix)
            }
            Action::Publish => rules.publish.iter().any(|grant| matches(grant, resource)),
            Action::Receive => rules.subscribe.iter().any(|grant| matches(grant, resource)),
            Action::Subscribe => rules
                .subscribe
                .iter()
                .any(|grant| covers_filter(grant, resource)),
        }
    }
}
fn valid_string(value: &str, max: usize, allow_empty: bool) -> bool {
    (allow_empty || !value.is_empty())
        && value.len() <= max
        && !value.chars().any(|c| {
            c == '\0' || ('\u{fdd0}'..='\u{fdef}').contains(&c) || (u32::from(c) & 0xffff) >= 0xfffe
        })
}
fn validate_rules(rules: &Rules) -> Result<(), PolicyError> {
    if !valid_string(&rules.client_id_prefix, u16::MAX as usize, true)
        || rules.publish.len() > MAX_RULES
        || rules.subscribe.len() > MAX_RULES
        || rules
            .publish
            .iter()
            .chain(&rules.subscribe)
            .any(|filter| !validate_filter(filter))
    {
        return Err(PolicyError::InvalidRules);
    }
    Ok(())
}
fn validate_hash(value: &str) -> Result<(), PolicyError> {
    let hash = PasswordHash::new(value).map_err(|_| PolicyError::InvalidHash)?;
    if hash.algorithm.as_str() != "argon2id"
        || hash.version != Some(19)
        || hash.hash.is_none()
        || hash.salt.is_none()
    {
        return Err(PolicyError::InvalidHash);
    }
    // Require explicit m/t/p rather than silently accepting library defaults.
    // Associated-data/keyid extensions are not supported by this local provider.
    if hash.params.iter().count() != 3 {
        return Err(PolicyError::HashParameters);
    }
    let (Some(m), Some(t), Some(p)) = (
        hash.params.get_decimal("m"),
        hash.params.get_decimal("t"),
        hash.params.get_decimal("p"),
    ) else {
        return Err(PolicyError::HashParameters);
    };
    // Bound raw numbers before passing them to the library's parameter builder.
    // This also prevents arithmetic overflow for hostile lane-count values.
    if m > 65536 || t == 0 || t > 10 || p == 0 || p > 4 || m < 8 * p {
        return Err(PolicyError::HashParameters);
    }
    Params::try_from(&hash).map_err(|_| PolicyError::HashParameters)?;
    let mut salt_bytes = [0u8; 64];
    let salt = hash.salt.ok_or(PolicyError::InvalidHash)?;
    if salt
        .decode_b64(&mut salt_bytes)
        .map_err(|_| PolicyError::InvalidHash)?
        .len()
        < 8
    {
        return Err(PolicyError::InvalidHash);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
