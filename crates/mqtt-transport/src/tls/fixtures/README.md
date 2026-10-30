These are public, disposable localhost-only TLS test identities, including their
unencrypted private keys. Never use them in a deployment. The second key deliberately
does not match the certificate and exercises fail-closed startup validation.

Generated with OpenSSL (RSA 2048, self-signed localhost SAN, CA:FALSE, 3650 days)
and an independent Ed25519 key. Runtime blackbox tests generate ephemeral identities
separately rather than trusting these fixtures.
