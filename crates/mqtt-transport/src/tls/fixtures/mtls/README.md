These are public, test-only EC P-256 identities; `client-key.pem` is intentionally
committed and must never be used outside tests. The CA private key was discarded.
The server identity remains the existing localhost fixture in the parent folder.

Generated locally with OpenSSL 3.6.3. `ca.pem` is a self-signed CA with critical
CA=true and keyCertSign/cRLSign. `client-key.pem` signs the CSR shared by these
leaf certificates:

- `client.pem`: signed by ca.pem, CA=false, digitalSignature, clientAuth;
  validity 2020-01-01 through 2099-01-01 UTC.
- `expired.pem`: same issuer/key/purpose, validity 2020-01-01 through
  2021-01-01 UTC, isolating certificate expiration from key and trust failures.
- `wrong-eku.pem`: same issuer/key/validity as client.pem, with serverAuth alone.
- `wrong-ca.pem`: self-signed with the same client key and clientAuth, absent
  from the server's configured trust store.

Leaf subjects deliberately do not name an MQTT user. mTLS validates transport
trust only; tests must not infer a principal or ACL from certificate CN/SAN.
