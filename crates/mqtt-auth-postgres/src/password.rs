use crate::Error;
use argon2::{Argon2, Params, PasswordHash, PasswordVerifier};

pub(crate) fn verify(hash: &str, password: &[u8]) -> Result<bool, Error> {
    let parsed = PasswordHash::new(hash).map_err(|_| Error::InvalidResult)?;
    if parsed.algorithm.as_str() != "argon2id"
        || parsed.version != Some(19)
        || parsed.hash.is_none()
        || parsed.salt.is_none()
        || parsed.params.iter().count() != 3
    {
        return Err(Error::InvalidResult);
    }
    let (Some(m), Some(t), Some(p)) = (
        parsed.params.get_decimal("m"),
        parsed.params.get_decimal("t"),
        parsed.params.get_decimal("p"),
    ) else {
        return Err(Error::InvalidResult);
    };
    if m > 65536 || !(1..=10).contains(&t) || !(1..=4).contains(&p) || m < 8 * p {
        return Err(Error::InvalidResult);
    }
    Params::try_from(&parsed).map_err(|_| Error::InvalidResult)?;
    let mut salt = [0; 64];
    if parsed
        .salt
        .ok_or(Error::InvalidResult)?
        .decode_b64(&mut salt)
        .map_err(|_| Error::InvalidResult)?
        .len()
        < 8
    {
        return Err(Error::InvalidResult);
    }
    match Argon2::default().verify_password(password, &parsed) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(_) => Err(Error::InvalidResult),
    }
}
