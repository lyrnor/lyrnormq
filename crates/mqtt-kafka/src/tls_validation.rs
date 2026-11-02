//! Offline validation of inline Kafka TLS identity material and bounded key decryption.
use crate::{BuildError, KafkaSecurity};
use pkcs8::{
    EncryptedPrivateKeyInfo,
    der::Document,
    pkcs5::{EncryptionScheme, pbes2::Kdf},
};
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    sign::CertifiedKey,
};
use std::io::Cursor;

const MAX_PEM: usize = 16 * 1024;
const MAX_SECTIONS: usize = 16;
const MAX_PBKDF2_ITERATIONS: u32 = 100_000;

/// Validate only caller-supplied material; this function reads no files or sockets.
/// Password derivation is deliberately capped before any expensive operation.
pub(crate) fn validate(security: &KafkaSecurity) -> Result<(), BuildError> {
    let invalid = BuildError::Invalid;
    if !security.protocol.tls() {
        return if security.ca_pem.is_none()
            && security.certificate_pem.is_none()
            && security.private_key_pem.is_none()
            && security.private_key_password.is_none()
        {
            Ok(())
        } else {
            Err(invalid)
        };
    }
    for pem in [
        &security.ca_pem,
        &security.certificate_pem,
        &security.private_key_pem,
    ]
    .into_iter()
    .flatten()
    {
        if pem.is_empty() || pem.len() > MAX_PEM || pem.contains('\0') || pem == "[redacted]" {
            return Err(invalid);
        }
    }
    if security
        .private_key_password
        .as_ref()
        .is_some_and(|p| p.is_empty() || p.len() > 1024 || p.contains('\0') || p == "[redacted]")
    {
        return Err(invalid);
    }
    if security.certificate_pem.is_some() != security.private_key_pem.is_some()
        || security.private_key_password.is_some() && security.private_key_pem.is_none()
    {
        return Err(invalid);
    }
    if let Some(ca) = &security.ca_pem {
        certificates(ca)?;
    }
    if let (Some(cert), Some(key)) = (&security.certificate_pem, &security.private_key_pem) {
        let chain = certificates(cert)?;
        let key = private_key(key, security.private_key_password.as_deref())?;
        let provider = rustls::crypto::ring::default_provider();
        let certified = CertifiedKey::from_der(chain, key, &provider).map_err(|_| invalid)?;
        certified.keys_match().map_err(|_| invalid)?;
    }
    Ok(())
}

/// A strict PEM envelope scanner: unknown sections and text outside envelopes
/// must not disappear through permissive PEM readers.
fn sections(mut pem: &str) -> Result<Vec<(&str, &str)>, BuildError> {
    let mut sections = Vec::new();
    loop {
        pem = pem.trim_start_matches(|c: char| c.is_ascii_whitespace());
        if pem.is_empty() {
            break;
        }
        if sections.len() >= MAX_SECTIONS {
            return Err(BuildError::Invalid);
        }
        let remainder = pem.strip_prefix("-----BEGIN ").ok_or(BuildError::Invalid)?;
        let label_end = remainder.find("-----").ok_or(BuildError::Invalid)?;
        let label = &remainder[..label_end];
        if label.is_empty()
            || label.len() > 32
            || !label
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b' ')
        {
            return Err(BuildError::Invalid);
        }
        let marker = format!("-----END {label}-----");
        let end = pem.find(&marker).ok_or(BuildError::Invalid)? + marker.len();
        sections.push((label, &pem[..end]));
        pem = &pem[end..];
    }
    if sections.is_empty() {
        return Err(BuildError::Invalid);
    }
    Ok(sections)
}

fn certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>, BuildError> {
    let mut certificates = Vec::new();
    let mut roots = RootCertStore::empty();
    for (label, section) in sections(pem)? {
        if label != "CERTIFICATE" {
            return Err(BuildError::Invalid);
        }
        let Some(rustls_pemfile::Item::X509Certificate(cert)) =
            rustls_pemfile::read_one(&mut Cursor::new(section)).map_err(|_| BuildError::Invalid)?
        else {
            return Err(BuildError::Invalid);
        };
        roots.add(cert.clone()).map_err(|_| BuildError::Invalid)?;
        certificates.push(cert);
    }
    Ok(certificates)
}

fn private_key(pem: &str, password: Option<&str>) -> Result<PrivateKeyDer<'static>, BuildError> {
    let sections = sections(pem)?;
    let [(label, section)] = sections.as_slice() else {
        return Err(BuildError::Invalid);
    };
    if *label == "ENCRYPTED PRIVATE KEY" {
        let (parsed_label, document) =
            Document::from_pem(section).map_err(|_| BuildError::Invalid)?;
        if parsed_label != *label {
            return Err(BuildError::Invalid);
        }
        let info = EncryptedPrivateKeyInfo::try_from(document.as_bytes())
            .map_err(|_| BuildError::Invalid)?;
        bounded_derivation(&info.encryption_algorithm)?;
        let decrypted = info
            .decrypt(password.ok_or(BuildError::Invalid)?)
            .map_err(|_| BuildError::Invalid)?;
        return Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            decrypted.as_bytes().to_vec(),
        )));
    }
    if !matches!(*label, "PRIVATE KEY" | "RSA PRIVATE KEY" | "EC PRIVATE KEY")
        || section.contains("Proc-Type:")
        || section.contains("DEK-Info:")
    {
        return Err(BuildError::Invalid);
    }
    match rustls_pemfile::read_one(&mut Cursor::new(section)).map_err(|_| BuildError::Invalid)? {
        Some(rustls_pemfile::Item::Pkcs1Key(key)) => Ok(PrivateKeyDer::Pkcs1(key)),
        Some(rustls_pemfile::Item::Pkcs8Key(key)) => Ok(PrivateKeyDer::Pkcs8(key)),
        Some(rustls_pemfile::Item::Sec1Key(key)) => Ok(PrivateKeyDer::Sec1(key)),
        _ => Err(BuildError::Invalid),
    }
}

fn bounded_derivation(scheme: &EncryptionScheme<'_>) -> Result<(), BuildError> {
    let EncryptionScheme::Pbes2(parameters) = scheme else {
        return Err(BuildError::Invalid);
    };
    if parameters
        .kdf
        .key_length()
        .is_some_and(|len| len == 0 || len > 64)
    {
        return Err(BuildError::Invalid);
    }
    let bounded = match &parameters.kdf {
        Kdf::Pbkdf2(p) => {
            (1..=MAX_PBKDF2_ITERATIONS).contains(&p.iteration_count)
                && (1..=64).contains(&p.salt.len())
        }
        Kdf::Scrypt(p) => {
            p.cost_parameter.is_power_of_two()
                && (2..=16384).contains(&p.cost_parameter)
                && (1..=8).contains(&p.block_size)
                && p.parallelization == 1
                && (1..=64).contains(&p.salt.len())
        }
        _ => false,
    };
    if bounded {
        Ok(())
    } else {
        Err(BuildError::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SecurityProtocol;
    use pkcs8::{
        EncryptedPrivateKeyInfo,
        der::{EncodePem, SecretDocument, pem::LineEnding},
        pkcs5::pbes2,
    };
    const CERT: &str = include_str!("../../mqtt-transport/src/tls/fixtures/cert.pem");
    const KEY: &str = include_str!("../../mqtt-transport/src/tls/fixtures/key.pem");
    const OTHER_KEY: &str = include_str!("../../mqtt-transport/src/tls/fixtures/other-key.pem");
    fn identity() -> KafkaSecurity {
        KafkaSecurity {
            protocol: SecurityProtocol::Ssl,
            ca_pem: Some(CERT.into()),
            certificate_pem: Some(CERT.into()),
            private_key_pem: Some(KEY.into()),
            ..KafkaSecurity::default()
        }
    }
    fn encrypted(iterations: u32) -> String {
        let (_, key) = SecretDocument::from_pem(KEY).unwrap();
        let parameters = pbes2::Parameters::pbkdf2_sha256_aes256cbc(
            iterations,
            b"0123456789abcdef",
            b"0123456789abcdef",
        )
        .unwrap();
        let ciphertext = parameters
            .encrypt(b"fixture-password", key.as_bytes())
            .unwrap();
        EncryptedPrivateKeyInfo {
            encryption_algorithm: parameters.into(),
            encrypted_data: &ciphertext,
        }
        .to_pem(LineEnding::LF)
        .unwrap()
    }
    #[test]
    fn malformed_ca_and_wrong_pem_sections_are_rejected() {
        for ca in [
            "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----",
            KEY,
            "not a certificate",
        ] {
            let mut security = identity();
            security.ca_pem = Some(ca.into());
            assert_eq!(validate(&security), Err(BuildError::Invalid));
        }
    }
    #[test]
    fn valid_identity_and_certificate_bundle_are_accepted_offline() {
        assert_eq!(validate(&identity()), Ok(()));
        let mut security = identity();
        security.ca_pem = Some(format!("{CERT}\n{CERT}"));
        assert_eq!(validate(&security), Ok(()));
        assert_eq!(
            validate(&KafkaSecurity {
                protocol: SecurityProtocol::Ssl,
                ..KafkaSecurity::default()
            }),
            Ok(())
        );
    }
    #[test]
    fn mismatched_identity_and_multiple_keys_are_rejected() {
        let mut security = identity();
        security.private_key_pem = Some(OTHER_KEY.into());
        assert_eq!(validate(&security), Err(BuildError::Invalid));
        security.private_key_pem = Some(format!("{KEY}\n{KEY}"));
        assert_eq!(validate(&security), Err(BuildError::Invalid));
        security.private_key_pem = Some(CERT.into());
        assert_eq!(validate(&security), Err(BuildError::Invalid));
    }
    #[test]
    fn unknown_sections_and_non_whitespace_outside_pem_are_rejected() {
        for extra in [
            KEY.to_owned(),
            "-----BEGIN UNKNOWN-----\nYWJj\n-----END UNKNOWN-----".into(),
            "unexpected text".into(),
        ] {
            let mut security = identity();
            security.ca_pem = Some(format!("{CERT}\n{extra}"));
            assert_eq!(validate(&security), Err(BuildError::Invalid));
        }
    }
    #[test]
    fn certificate_and_private_key_must_be_paired() {
        let mut security = identity();
        security.private_key_pem = None;
        assert_eq!(validate(&security), Err(BuildError::Invalid));
        security.certificate_pem = None;
        security.private_key_password = Some("unused".into());
        assert_eq!(validate(&security), Err(BuildError::Invalid));
        security = identity();
        security.protocol = SecurityProtocol::Plaintext;
        assert_eq!(validate(&security), Err(BuildError::Invalid));
    }
    #[test]
    fn encrypted_pkcs8_accepts_correct_password_and_rejects_wrong_or_missing_one() {
        let mut security = identity();
        security.private_key_pem = Some(encrypted(2048));
        assert_eq!(validate(&security), Err(BuildError::Invalid));
        security.private_key_password = Some("wrong".into());
        assert_eq!(validate(&security), Err(BuildError::Invalid));
        security.private_key_password = Some("fixture-password".into());
        assert_eq!(validate(&security), Ok(()));
    }
    #[test]
    fn encrypted_key_derivation_cost_is_capped_before_decryption() {
        let valid =
            pbes2::Parameters::pbkdf2_sha256_aes256cbc(2048, b"salt", b"0123456789abcdef").unwrap();
        assert_eq!(bounded_derivation(&valid.clone().into()), Ok(()));
        for rounds in [0, MAX_PBKDF2_ITERATIONS + 1] {
            let mut invalid = valid.clone();
            let Kdf::Pbkdf2(ref mut p) = invalid.kdf else {
                unreachable!()
            };
            p.iteration_count = rounds;
            assert_eq!(
                bounded_derivation(&invalid.into()),
                Err(BuildError::Invalid)
            );
        }
        let mut invalid = valid;
        let Kdf::Pbkdf2(ref mut p) = invalid.kdf else {
            unreachable!()
        };
        p.salt = b"";
        assert_eq!(
            bounded_derivation(&invalid.into()),
            Err(BuildError::Invalid)
        );
        let scrypt = pbes2::ScryptParams {
            salt: b"salt",
            cost_parameter: 16384,
            block_size: 8,
            parallelization: 1,
            key_length: None,
        };
        let mut parameters =
            pbes2::Parameters::pbkdf2_sha256_aes256cbc(2048, b"salt", b"0123456789abcdef").unwrap();
        parameters.kdf = Kdf::Scrypt(scrypt);
        assert_eq!(bounded_derivation(&parameters.clone().into()), Ok(()));
        for invalid in [
            pbes2::ScryptParams {
                cost_parameter: 32768,
                ..scrypt
            },
            pbes2::ScryptParams {
                block_size: 9,
                ..scrypt
            },
            pbes2::ScryptParams {
                parallelization: 2,
                ..scrypt
            },
        ] {
            parameters.kdf = Kdf::Scrypt(invalid);
            assert_eq!(
                bounded_derivation(&parameters.clone().into()),
                Err(BuildError::Invalid)
            );
        }
    }
    #[test]
    fn field_and_pem_section_limits_are_enforced() {
        let mut security = identity();
        security.ca_pem = Some(" ".repeat(16 * 1024 + 1));
        assert_eq!(validate(&security), Err(BuildError::Invalid));
        security = identity();
        security.private_key_password = Some("x".repeat(1025));
        assert_eq!(validate(&security), Err(BuildError::Invalid));
    }
}
