pub(crate) const FIELD_SEPARATOR: u8 = 0x1f;

pub(crate) fn auth_key(prefix: &str, username: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 5 + username.len());
    key.extend_from_slice(prefix.as_bytes());
    key.extend_from_slice(b"auth:");
    key.extend_from_slice(username.as_bytes());
    key
}
pub(crate) fn acl_key(prefix: &str, username: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 4 + username.len());
    key.extend_from_slice(prefix.as_bytes());
    key.extend_from_slice(b"acl:");
    key.extend_from_slice(username.as_bytes());
    key
}
pub(crate) fn action_field(action: &str) -> Vec<u8> {
    action.as_bytes().to_vec()
}
/// Rules for one Client ID. The separator is not parsed back out of the field name.
pub(crate) fn client_field(action: &str, client_id: &str) -> Vec<u8> {
    let mut field = Vec::with_capacity(action.len() + 1 + client_id.len());
    field.extend_from_slice(action.as_bytes());
    field.push(FIELD_SEPARATOR);
    field.extend_from_slice(client_id.as_bytes());
    field
}
