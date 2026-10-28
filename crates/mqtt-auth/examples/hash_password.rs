//! Reads exact binary stdin (no newline trimming), emits only a fresh Argon2id PHC.
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use rand_core::{OsRng, RngCore};
use std::{
    error::Error,
    io::{self, Read},
};
use zeroize::Zeroizing;

fn main() -> Result<(), Box<dyn Error>> {
    let mut password = Zeroizing::new(Vec::new());
    io::stdin()
        .take(u16::MAX as u64 + 1)
        .read_to_end(&mut password)?;
    if password.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "password exceeds MQTT binary length",
        )
        .into());
    }
    let mut salt_bytes = [0u8; 16];
    OsRng.try_fill_bytes(&mut salt_bytes)?;
    let salt = SaltString::encode_b64(&salt_bytes)?;
    let hash = Argon2::default().hash_password(&password, &salt)?;
    println!("{hash}");
    Ok(())
}
