//! Immutable server identity; parsing occurs before a listener starts serving.
use std::{io, sync::Arc};
use tokio_rustls::{TlsAcceptor, rustls};

/// Server-authenticated TLS 1.2/1.3 identity using the explicit ring provider.
#[derive(Clone)]
pub struct TlsConfig {
    pub(crate) acceptor: TlsAcceptor,
}
impl TlsConfig {
    /// Load a certificate chain and exactly one unencrypted private key.
    /// Each input is limited to 1 MiB. Malformed or mismatched identities fail.
    pub fn from_pem(cert_chain: &[u8], private_key: &[u8]) -> io::Result<Self> {
        use rustls::pki_types::PrivateKeyDer;
        let mut certificates = Vec::new();
        for item in pem_blocks(cert_chain)? {
            let rustls_pemfile::Item::X509Certificate(cert) = item else {
                return Err(invalid(
                    "certificate chain contains a non-certificate block",
                ));
            };
            rustls::server::ParsedCertificate::try_from(&cert)
                .map_err(|_| invalid("malformed certificate DER"))?;
            certificates.push(cert);
        }
        if certificates.is_empty() {
            return Err(invalid("certificate chain is empty"));
        }
        let mut keys = pem_blocks(private_key)?;
        if keys.len() != 1 {
            return Err(invalid("identity requires exactly one private key"));
        }
        let key = match keys.remove(0) {
            rustls_pemfile::Item::Pkcs1Key(key) => PrivateKeyDer::Pkcs1(key),
            rustls_pemfile::Item::Pkcs8Key(key) => PrivateKeyDer::Pkcs8(key),
            rustls_pemfile::Item::Sec1Key(key) => PrivateKeyDer::Sec1(key),
            _ => return Err(invalid("unsupported private key block")),
        };
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        // Explicit consistency check fails closed even if a future provider
        // cannot establish the certificate/key public-key relationship.
        rustls::sign::CertifiedKey::from_der(certificates.clone(), key.clone_key(), &provider)
            .and_then(|certified| certified.keys_match())
            .map_err(|_| invalid("invalid or mismatched certificate and private key"))?;
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|_| invalid("unsupported TLS protocol configuration"))?
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(|_| invalid("invalid TLS server identity"))?;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Reject junk, unknown/encrypted blocks and trailing malformed material rather
/// than silently skipping it as permissive PEM iterators may do.
fn pem_blocks(bytes: &[u8]) -> io::Result<Vec<rustls_pemfile::Item>> {
    if bytes.len() > 1024 * 1024 {
        return Err(invalid("PEM input exceeds 1 MiB"));
    }
    let mut text = std::str::from_utf8(bytes).map_err(|_| invalid("PEM is not UTF-8"))?;
    let mut items = Vec::new();
    loop {
        text = text.trim_start_matches(char::is_whitespace);
        if text.is_empty() {
            return Ok(items);
        }
        let Some(after_begin) = text.strip_prefix("-----BEGIN ") else {
            return Err(invalid("unexpected material outside PEM block"));
        };
        let Some(header_end) = after_begin.find("-----") else {
            return Err(invalid("malformed PEM header"));
        };
        let label = &after_begin[..header_end];
        if !matches!(
            label,
            "CERTIFICATE" | "PRIVATE KEY" | "RSA PRIVATE KEY" | "EC PRIVATE KEY"
        ) {
            return Err(invalid("unsupported or encrypted PEM block"));
        }
        let ending = format!("-----END {label}-----");
        let Some(end) = text.find(&ending).map(|at| at + ending.len()) else {
            return Err(invalid("unterminated PEM block"));
        };
        let mut block = &text.as_bytes()[..end];
        let item = rustls_pemfile::read_one(&mut block)
            .map_err(|_| invalid("malformed PEM block"))?
            .ok_or_else(|| invalid("empty PEM block"))?;
        items.push(item);
        text = &text[end..];
    }
}
#[cfg(test)]
#[path = "tls/tests.rs"]
mod tests;
