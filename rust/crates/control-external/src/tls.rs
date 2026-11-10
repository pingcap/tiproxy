// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Builds a `rustls::ClientConfig` for the etcd transport from validated TLS
//! material and policy.
//!
//! This is a neutral, in-crate implementation of the frozen `cluster-tls`
//! verification semantics, so `control-external` does not depend on any
//! dataplane crate. The verifier composes a single outer
//! [`ServerCertVerifier`]:
//!
//! | `skip_ca` | common names | server certificate verification |
//! |-----------|--------------|---------------------------------|
//! | false     | empty        | CA chain only; hostname is not checked, matching Go |
//! | false     | non-empty    | CA chain only, then exact leaf Subject-CN pin |
//! | true      | empty        | chain/hostname skipped, TLS `CertificateVerify` signature still checked |
//! | true      | non-empty    | as above, then exact leaf Subject-CN pin |
//!
//! The minimum protocol version is always frozen explicitly (`1.2` and the
//! default select `[TLS1.2, TLS1.3]`; `1.3` selects `[TLS1.3]`), so a rustls
//! upgrade cannot silently change the wire protocol set. Client mTLS material,
//! when present, is parsed here from the generation-bound bytes.

use std::collections::BTreeSet;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};

use crate::etcd::{EtcdConfigError, EtcdTlsPolicy, EtcdTlsVersion};

/// Builds the client TLS configuration for one etcd connection from its CA
/// bundle, optional client identity, and verification policy.
///
/// # Errors
///
/// Returns [`EtcdConfigError::TlsSetup`] when the material cannot form a client
/// configuration (unparseable PEM, a client chain whose key does not match, an
/// empty trust anchor when CA verification is required, or an unusable rustls
/// build).
pub(crate) fn build_client_config(
    ca_certificate_pem: Option<&[u8]>,
    identity: Option<(&[u8], &[u8])>,
    policy: &EtcdTlsPolicy,
) -> Result<Arc<ClientConfig>, EtcdConfigError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // Freeze the protocol set explicitly for every case.
    let versions: &[&rustls::SupportedProtocolVersion] = match policy.minimum_version {
        Some(EtcdTlsVersion::V1_3) => &[&rustls::version::TLS13],
        None | Some(EtcdTlsVersion::V1_2) => &[&rustls::version::TLS12, &rustls::version::TLS13],
    };
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(versions)
        .map_err(|_| EtcdConfigError::TlsSetup)?;

    let verifier = build_server_verifier(&provider, ca_certificate_pem, policy)?;
    let builder = builder
        .dangerous()
        .with_custom_certificate_verifier(verifier);

    let config = match identity {
        Some((certificate_pem, key_pem)) => {
            let chain = parse_certificate_chain(certificate_pem)?;
            let key = parse_private_key(key_pem)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|_| EtcdConfigError::TlsSetup)?
        }
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

/// Composes the single outer server-certificate verifier per the truth table.
fn build_server_verifier(
    provider: &Arc<CryptoProvider>,
    ca_certificate_pem: Option<&[u8]>,
    policy: &EtcdTlsPolicy,
) -> Result<Arc<dyn ServerCertVerifier>, EtcdConfigError> {
    let inner: Arc<dyn ServerCertVerifier> = if policy.skip_ca_verification {
        Arc::new(SkipServerVerification::new(Arc::clone(provider)))
    } else {
        let ca = ca_certificate_pem.ok_or(EtcdConfigError::EmptyCaCertificate)?;
        let mut roots = RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(ca) {
            let certificate = certificate.map_err(|_| EtcdConfigError::TlsSetup)?;
            roots
                .add(certificate)
                .map_err(|_| EtcdConfigError::TlsSetup)?;
        }
        if roots.is_empty() {
            return Err(EtcdConfigError::EmptyCaCertificate);
        }
        Arc::new(CaOnlyServerVerification::new(roots, Arc::clone(provider)))
    };
    if policy.allowed_common_names.is_empty() {
        return Ok(inner);
    }
    Ok(Arc::new(CommonNamePinnedServerVerifier {
        inner,
        allowed_common_names: policy.allowed_common_names.iter().cloned().collect(),
    }))
}

/// Parses a client certificate chain from PEM into owned DER.
fn parse_certificate_chain(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, EtcdConfigError> {
    let mut chain = Vec::new();
    for certificate in CertificateDer::pem_slice_iter(pem) {
        chain.push(
            certificate
                .map_err(|_| EtcdConfigError::TlsSetup)?
                .into_owned(),
        );
    }
    if chain.is_empty() {
        return Err(EtcdConfigError::TlsSetup);
    }
    Ok(chain)
}

/// Parses the first private key from PEM into an owned key.
fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, EtcdConfigError> {
    let key = PrivateKeyDer::pem_slice_iter(pem)
        .next()
        .ok_or(EtcdConfigError::TlsSetup)?
        .map_err(|_| EtcdConfigError::TlsSetup)?;
    Ok(key.clone_key())
}

/// Verifies the server certificate chains to the configured CA and is a TLS
/// server cert, and does not check the dialed hostname.
///
/// Go `CertInfo.verifyCA` leaves `VerifyOptions.DNSName` empty. Operator
/// configs dial PD as `tc-pd:2379` while the serving certificate is issued
/// for the peer DNS name, so a hostname check refuses the connection.
#[derive(Debug)]
struct CaOnlyServerVerification {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
}

impl CaOnlyServerVerification {
    fn new(roots: RootCertStore, provider: Arc<CryptoProvider>) -> Self {
        Self {
            roots: Arc::new(roots),
            provider,
        }
    }
}

impl ServerCertVerifier for CaOnlyServerVerification {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let certificate = rustls::server::ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &certificate,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Skips certificate-chain and hostname trust while still validating the
/// handshake `CertificateVerify` signature against the presented certificate,
/// mirroring Go `skip-ca` exactly (the peer must still hold the key).
#[derive(Debug)]
struct SkipServerVerification {
    provider: Arc<CryptoProvider>,
}

impl SkipServerVerification {
    fn new(provider: Arc<CryptoProvider>) -> Self {
        Self { provider }
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Wraps an inner server-certificate verifier and additionally pins the leaf
/// certificate's Subject common name to an exact allowlist. `skip_ca` does not
/// short-circuit this pin.
#[derive(Debug)]
struct CommonNamePinnedServerVerifier {
    inner: Arc<dyn ServerCertVerifier>,
    allowed_common_names: BTreeSet<String>,
}

impl ServerCertVerifier for CommonNamePinnedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let verified = self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        let (_, certificate) = x509_parser::prelude::parse_x509_certificate(end_entity.as_ref())
            .map_err(|_| rustls::Error::General("server certificate is unparseable".to_owned()))?;
        let allowed = certificate.subject().iter_common_name().any(|name| {
            name.as_str()
                .is_ok_and(|value| self.allowed_common_names.contains(value))
        });
        if !allowed {
            return Err(rustls::Error::General(
                "server certificate common name is not allowed".to_owned(),
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::{CommonNamePinnedServerVerifier, SkipServerVerification, build_client_config};
    use crate::etcd::{EtcdConfigError, EtcdTlsPolicy, EtcdTlsVersion};
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
    use std::sync::Arc;

    /// A self-signed certificate and its private key, both PEM.
    fn certificate_with_common_name(common_name: &str) -> (String, String) {
        let mut params = rcgen::CertificateParams::new(vec!["example.com".to_owned()])
            .unwrap_or_else(|error| unreachable!("params: {error}"));
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap_or_else(|error| unreachable!("key: {error}"));
        let certificate = params
            .self_signed(&key)
            .unwrap_or_else(|error| unreachable!("self-signed: {error}"));
        (certificate.pem(), key.serialize_pem())
    }

    fn policy(skip: bool, common_names: &[&str], version: Option<EtcdTlsVersion>) -> EtcdTlsPolicy {
        EtcdTlsPolicy {
            minimum_version: version,
            allowed_common_names: common_names.iter().map(|name| (*name).to_owned()).collect(),
            skip_ca_verification: skip,
        }
    }

    #[test]
    fn every_verifier_row_and_version_builds() {
        let (ca, _) = certificate_with_common_name("etcd-ca");
        for version in [None, Some(EtcdTlsVersion::V1_2), Some(EtcdTlsVersion::V1_3)] {
            // Row 1 + 2: verify against CA, with and without CN pins.
            assert!(
                build_client_config(Some(ca.as_bytes()), None, &policy(false, &[], version))
                    .is_ok()
            );
            assert!(
                build_client_config(
                    Some(ca.as_bytes()),
                    None,
                    &policy(false, &["etcd"], version)
                )
                .is_ok()
            );
            // Row 3 + 4: skip CA (no CA needed), with and without CN pins.
            assert!(build_client_config(None, None, &policy(true, &[], version)).is_ok());
            assert!(build_client_config(None, None, &policy(true, &["etcd"], version)).is_ok());
        }
    }

    #[test]
    fn ca_verification_requires_a_ca_but_skip_does_not() {
        // skip=false with no CA fails closed; skip=true with no CA is fine.
        assert_eq!(
            build_client_config(None, None, &policy(false, &[], None)).err(),
            Some(EtcdConfigError::EmptyCaCertificate)
        );
        assert!(build_client_config(None, None, &policy(true, &[], None)).is_ok());
    }

    #[test]
    fn client_identity_parsing_is_discriminating() {
        let (ca, _) = certificate_with_common_name("etcd-ca");
        let (client_cert, client_key) = certificate_with_common_name("etcd-client");
        // A matched chain and key builds.
        assert!(
            build_client_config(
                Some(ca.as_bytes()),
                Some((client_cert.as_bytes(), client_key.as_bytes())),
                &policy(false, &[], None),
            )
            .is_ok()
        );
        // Malformed certificate PEM, malformed key PEM, and a mismatched
        // certificate/key pair all fail closed.
        assert!(
            build_client_config(
                Some(ca.as_bytes()),
                Some((b"not-a-cert", client_key.as_bytes())),
                &policy(false, &[], None),
            )
            .is_err()
        );
        assert!(
            build_client_config(
                Some(ca.as_bytes()),
                Some((client_cert.as_bytes(), b"not-a-key")),
                &policy(false, &[], None),
            )
            .is_err()
        );
        let (_, other_key) = certificate_with_common_name("someone-else");
        assert!(
            build_client_config(
                Some(ca.as_bytes()),
                Some((client_cert.as_bytes(), other_key.as_bytes())),
                &policy(false, &[], None),
            )
            .is_err()
        );
    }

    /// An inner verifier that accepts every certificate, so the test isolates
    /// the common-name pin.
    #[derive(Debug)]
    struct AcceptAll;

    impl ServerCertVerifier for AcceptAll {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            Vec::new()
        }
    }

    /// Builds a syntactically valid but cryptographically forged
    /// `DigitallySignedStruct` (the public constructor is crate-private, so this
    /// decodes one from its wire form: scheme, then a `u16`-prefixed signature).
    fn forged_signature() -> DigitallySignedStruct {
        use rustls::internal::msgs::codec::{Codec, Reader};
        let mut wire = Vec::new();
        // SignatureScheme::ECDSA_NISTP256_SHA256 is 0x0403 on the wire.
        wire.extend_from_slice(&0x0403_u16.to_be_bytes());
        let signature = [0_u8; 64];
        let signature_len =
            u16::try_from(signature.len()).unwrap_or_else(|error| unreachable!("sig len: {error}"));
        wire.extend_from_slice(&signature_len.to_be_bytes());
        wire.extend_from_slice(&signature);
        let mut reader = Reader::init(&wire);
        DigitallySignedStruct::read(&mut reader)
            .unwrap_or_else(|error| unreachable!("forged dss: {error:?}"))
    }

    #[test]
    fn common_name_pin_accepts_only_the_allowlisted_cn() {
        let (pinned_pem, _) = certificate_with_common_name("pinned-cn");
        let pinned = CertificateDer::from_pem_slice(pinned_pem.as_bytes())
            .unwrap_or_else(|error| unreachable!("parse: {error}"));
        let server_name = ServerName::try_from("example.com").unwrap_or_else(|_| unreachable!());
        let now = UnixTime::now();

        let allowed = CommonNamePinnedServerVerifier {
            inner: Arc::new(AcceptAll),
            allowed_common_names: ["pinned-cn".to_owned()].into_iter().collect(),
        };
        assert!(
            allowed
                .verify_server_cert(&pinned, &[], &server_name, &[], now)
                .is_ok(),
            "a certificate whose CN is allowlisted passes"
        );

        let rejected = CommonNamePinnedServerVerifier {
            inner: Arc::new(AcceptAll),
            allowed_common_names: ["other-cn".to_owned()].into_iter().collect(),
        };
        assert!(
            rejected
                .verify_server_cert(&pinned, &[], &server_name, &[], now)
                .is_err(),
            "a certificate whose CN is not allowlisted is rejected even though the inner verifier accepted it"
        );
    }

    /// An inner verifier that rejects every certificate, so tests can prove the
    /// common-name pin does not turn a rejecting inner into an acceptance.
    #[derive(Debug)]
    struct RejectAll;

    impl ServerCertVerifier for RejectAll {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Err(rustls::Error::General("inner rejects".to_owned()))
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Err(rustls::Error::General("inner rejects".to_owned()))
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Err(rustls::Error::General("inner rejects".to_owned()))
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            Vec::new()
        }
    }

    /// Builds a self-signed CA and returns its PEM plus a reusable issuer.
    fn make_ca(common_name: &str) -> (String, rcgen::Issuer<'static, rcgen::KeyPair>) {
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new())
            .unwrap_or_else(|error| unreachable!("ca params: {error}"));
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        ca_params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        ca_params.not_after = rcgen::date_time_ymd(2100, 1, 1);
        let ca_key =
            rcgen::KeyPair::generate().unwrap_or_else(|error| unreachable!("ca key: {error}"));
        let ca_cert = ca_params
            .self_signed(&ca_key)
            .unwrap_or_else(|error| unreachable!("ca self-signed: {error}"));
        let ca_pem = ca_cert.pem();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        (ca_pem, issuer)
    }

    /// Signs a leaf for `hostname` with `common_name`, as a server or client
    /// certificate, using the given issuer. Returns the leaf and key PEM.
    fn make_leaf(
        issuer: &rcgen::Issuer<'static, rcgen::KeyPair>,
        common_name: &str,
        hostname: &str,
        server_auth: bool,
    ) -> (String, String) {
        let mut leaf_params = rcgen::CertificateParams::new(vec![hostname.to_owned()])
            .unwrap_or_else(|error| unreachable!("leaf params: {error}"));
        leaf_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        leaf_params.extended_key_usages = vec![if server_auth {
            rcgen::ExtendedKeyUsagePurpose::ServerAuth
        } else {
            rcgen::ExtendedKeyUsagePurpose::ClientAuth
        }];
        leaf_params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        leaf_params.not_after = rcgen::date_time_ymd(2100, 1, 1);
        let leaf_key =
            rcgen::KeyPair::generate().unwrap_or_else(|error| unreachable!("leaf key: {error}"));
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, issuer)
            .unwrap_or_else(|error| unreachable!("leaf signed: {error}"));
        (leaf_cert.pem(), leaf_key.serialize_pem())
    }

    fn parse_chain(pem: &str) -> Vec<CertificateDer<'static>> {
        let mut chain = Vec::new();
        for certificate in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            chain.push(
                certificate
                    .unwrap_or_else(|error| unreachable!("cert: {error}"))
                    .into_owned(),
            );
        }
        chain
    }

    fn parse_key(pem: &str) -> PrivateKeyDer<'static> {
        PrivateKeyDer::from_pem_slice(pem.as_bytes())
            .unwrap_or_else(|error| unreachable!("key: {error}"))
    }

    fn roots(ca_pem: &str) -> Arc<rustls::RootCertStore> {
        let mut store = rustls::RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
            let certificate = certificate.unwrap_or_else(|error| unreachable!("ca cert: {error}"));
            store
                .add(certificate)
                .unwrap_or_else(|error| unreachable!("add root: {error}"));
        }
        Arc::new(store)
    }

    /// Builds a server config for the leaf, optionally requiring client auth
    /// against `client_ca_pem`, offering exactly `versions`.
    fn server_config(
        cert_pem: &str,
        key_pem: &str,
        client_ca_pem: Option<&str>,
        versions: &[&'static rustls::SupportedProtocolVersion],
    ) -> Arc<ServerConfig> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(versions)
            .unwrap_or_else(|error| unreachable!("server versions: {error}"));
        let builder = match client_ca_pem {
            Some(ca) => {
                let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                    roots(ca),
                    provider,
                )
                .build()
                .unwrap_or_else(|error| unreachable!("client verifier: {error}"));
                builder.with_client_cert_verifier(verifier)
            }
            None => builder.with_no_client_auth(),
        };
        Arc::new(
            builder
                .with_single_cert(parse_chain(cert_pem), parse_key(key_pem))
                .unwrap_or_else(|error| unreachable!("server cert: {error}")),
        )
    }

    /// Drives a full in-memory handshake, returning `Err` if either side
    /// rejects.
    fn do_handshake(
        client_config: Arc<ClientConfig>,
        server_conf: Arc<ServerConfig>,
        server_name: &str,
    ) -> Result<(), rustls::Error> {
        let name = ServerName::try_from(server_name.to_owned())
            .unwrap_or_else(|_| unreachable!("server name"));
        let mut client = rustls::ClientConnection::new(client_config, name)
            .unwrap_or_else(|error| unreachable!("client conn: {error}"));
        let mut server = rustls::ServerConnection::new(server_conf)
            .unwrap_or_else(|error| unreachable!("server conn: {error}"));
        for _ in 0..32 {
            let mut to_server = Vec::new();
            while client.wants_write() {
                let Ok(_) = client.write_tls(&mut to_server) else {
                    unreachable!("client write_tls")
                };
            }
            if !to_server.is_empty() {
                let mut cursor = std::io::Cursor::new(&to_server[..]);
                let end = to_server.len() as u64;
                while cursor.position() < end {
                    let Ok(_) = server.read_tls(&mut cursor) else {
                        unreachable!("server read_tls")
                    };
                }
                server.process_new_packets()?;
            }
            let mut to_client = Vec::new();
            while server.wants_write() {
                let Ok(_) = server.write_tls(&mut to_client) else {
                    unreachable!("server write_tls")
                };
            }
            if !to_client.is_empty() {
                let mut cursor = std::io::Cursor::new(&to_client[..]);
                let end = to_client.len() as u64;
                while cursor.position() < end {
                    let Ok(_) = client.read_tls(&mut cursor) else {
                        unreachable!("client read_tls")
                    };
                }
                client.process_new_packets()?;
            }
            if !client.is_handshaking() && !server.is_handshaking() {
                return Ok(());
            }
        }
        Err(rustls::Error::General(
            "handshake did not complete".to_owned(),
        ))
    }

    fn client_config(
        ca_pem: Option<&str>,
        identity: Option<(&str, &str)>,
        policy: &EtcdTlsPolicy,
    ) -> Arc<ClientConfig> {
        let ca_bytes = ca_pem.map(str::as_bytes);
        let identity_bytes = identity.map(|(cert, key)| (cert.as_bytes(), key.as_bytes()));
        build_client_config(ca_bytes, identity_bytes, policy)
            .unwrap_or_else(|error| unreachable!("client config: {error:?}"))
    }

    #[test]
    fn webpki_row_discriminates_ca_and_hostname() {
        let (trusted_ca, trusted_issuer) = make_ca("trusted-ca");
        let (foreign_ca, _foreign_issuer) = make_ca("foreign-ca");
        let (server_cert, server_key) =
            make_leaf(&trusted_issuer, "etcd-server", "localhost", true);
        let policy = policy(false, &[], None);

        // Trusted CA + correct hostname handshakes.
        assert!(
            do_handshake(
                client_config(Some(&trusted_ca), None, &policy),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "localhost",
            )
            .is_ok(),
            "trusted CA and correct hostname must succeed"
        );
        // Correct CA but a different dial name still succeeds. Go does not
        // set VerifyOptions.DNSName for cluster-tls.
        assert!(
            do_handshake(
                client_config(Some(&trusted_ca), None, &policy),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "wrong-host.example",
            )
            .is_ok(),
            "a hostname absent from the SAN must still succeed when the CA matches"
        );
        // Client trusts a different CA than signed the leaf.
        assert!(
            do_handshake(
                client_config(Some(&foreign_ca), None, &policy),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "localhost",
            )
            .is_err(),
            "an untrusted CA must fail"
        );
    }

    #[test]
    fn skip_ca_accepts_untrusted_chain_and_wrong_hostname() {
        let (_foreign_ca, foreign_issuer) = make_ca("foreign-ca");
        let (server_cert, server_key) =
            make_leaf(&foreign_issuer, "etcd-server", "localhost", true);
        assert!(
            do_handshake(
                client_config(None, None, &policy(true, &[], None)),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "arbitrary-host.example",
            )
            .is_ok(),
            "skip_ca ignores an untrusted chain and any hostname"
        );
    }

    #[test]
    fn cn_pin_wraps_a_real_webpki_inner() {
        let (trusted_ca, trusted_issuer) = make_ca("trusted-ca");
        let (server_cert, server_key) =
            make_leaf(&trusted_issuer, "etcd-server", "localhost", true);

        // WebPKI passes and the pinned CN matches: handshake succeeds.
        assert!(
            do_handshake(
                client_config(
                    Some(&trusted_ca),
                    None,
                    &policy(false, &["etcd-server"], None)
                ),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "localhost",
            )
            .is_ok(),
            "matching CN over a passing WebPKI inner must succeed"
        );
        // WebPKI passes but the CN pin rejects: the outer verifier wraps it.
        assert!(
            do_handshake(
                client_config(Some(&trusted_ca), None, &policy(false, &["other-cn"], None)),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "localhost",
            )
            .is_err(),
            "a non-matching CN must fail even though WebPKI passed"
        );

        // A matching CN over a rejecting inner must still fail: the pin does not
        // bypass the inner verifier.
        let (pinned_pem, _key) = make_leaf(&trusted_issuer, "etcd-server", "localhost", true);
        let leaf = CertificateDer::from_pem_slice(pinned_pem.as_bytes())
            .unwrap_or_else(|error| unreachable!("parse: {error}"));
        let verifier = CommonNamePinnedServerVerifier {
            inner: Arc::new(RejectAll),
            allowed_common_names: ["etcd-server".to_owned()].into_iter().collect(),
        };
        let name = ServerName::try_from("localhost").unwrap_or_else(|_| unreachable!());
        assert!(
            verifier
                .verify_server_cert(&leaf, &[], &name, &[], UnixTime::now())
                .is_err(),
            "matching CN plus a rejecting inner must still fail"
        );
    }

    #[test]
    fn skip_ca_does_not_short_circuit_the_cn_pin() {
        let (_foreign_ca, foreign_issuer) = make_ca("foreign-ca");
        let (server_cert, server_key) =
            make_leaf(&foreign_issuer, "etcd-server", "localhost", true);

        // skip_ca ignores the chain, but the CN pin still matches.
        assert!(
            do_handshake(
                client_config(None, None, &policy(true, &["etcd-server"], None)),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "arbitrary-host.example",
            )
            .is_ok(),
            "skip_ca with a matching CN must succeed"
        );
        // skip_ca still enforces the CN pin.
        assert!(
            do_handshake(
                client_config(None, None, &policy(true, &["other-cn"], None)),
                server_config(&server_cert, &server_key, None, rustls::ALL_VERSIONS),
                "arbitrary-host.example",
            )
            .is_err(),
            "skip_ca must not bypass a non-matching CN"
        );
    }

    #[test]
    fn protocol_version_floor_selects_the_wire_set() {
        let (trusted_ca, trusted_issuer) = make_ca("trusted-ca");
        let (server_cert, server_key) =
            make_leaf(&trusted_issuer, "etcd-server", "localhost", true);
        let server_tls12 =
            || server_config(&server_cert, &server_key, None, &[&rustls::version::TLS12]);
        let server_tls13 =
            || server_config(&server_cert, &server_key, None, &[&rustls::version::TLS13]);

        // V1_3 policy maps to [TLS13] only.
        assert!(
            do_handshake(
                client_config(
                    Some(&trusted_ca),
                    None,
                    &policy(false, &[], Some(EtcdTlsVersion::V1_3))
                ),
                server_tls12(),
                "localhost",
            )
            .is_err(),
            "a 1.3 floor cannot share a version with a 1.2-only server"
        );
        assert!(
            do_handshake(
                client_config(
                    Some(&trusted_ca),
                    None,
                    &policy(false, &[], Some(EtcdTlsVersion::V1_3))
                ),
                server_tls13(),
                "localhost",
            )
            .is_ok(),
            "a 1.3 floor handshakes with a 1.3-only server"
        );
        // Default (None) policy maps to [TLS12, TLS13]: accepts both servers.
        assert!(
            do_handshake(
                client_config(Some(&trusted_ca), None, &policy(false, &[], None)),
                server_tls12(),
                "localhost",
            )
            .is_ok(),
            "the default floor accepts a 1.2-only server"
        );
        assert!(
            do_handshake(
                client_config(Some(&trusted_ca), None, &policy(false, &[], None)),
                server_tls13(),
                "localhost",
            )
            .is_ok(),
            "the default floor accepts a 1.3-only server (set is not narrowed to [TLS12])"
        );
        // V1_2 policy also maps to [TLS12, TLS13]: accepts both servers, proving
        // it is not mis-mapped to [TLS13].
        assert!(
            do_handshake(
                client_config(
                    Some(&trusted_ca),
                    None,
                    &policy(false, &[], Some(EtcdTlsVersion::V1_2))
                ),
                server_tls12(),
                "localhost",
            )
            .is_ok(),
            "a 1.2 floor accepts a 1.2-only server"
        );
        assert!(
            do_handshake(
                client_config(
                    Some(&trusted_ca),
                    None,
                    &policy(false, &[], Some(EtcdTlsVersion::V1_2))
                ),
                server_tls13(),
                "localhost",
            )
            .is_ok(),
            "a 1.2 floor accepts a 1.3-only server"
        );
    }

    #[test]
    fn mtls_requires_a_client_certificate_from_the_trusted_ca() {
        let (trusted_ca, trusted_issuer) = make_ca("trusted-ca");
        let (foreign_ca, foreign_issuer) = make_ca("foreign-ca");
        let (server_cert, server_key) =
            make_leaf(&trusted_issuer, "etcd-server", "localhost", true);
        let (client_cert, client_key) =
            make_leaf(&trusted_issuer, "etcd-client", "localhost", false);
        let (foreign_cert, foreign_key) =
            make_leaf(&foreign_issuer, "etcd-client", "localhost", false);
        let policy = policy(false, &[], None);

        // A client identity signed by the trusted CA is accepted.
        assert!(
            do_handshake(
                client_config(
                    Some(&trusted_ca),
                    Some((&client_cert, &client_key)),
                    &policy,
                ),
                server_config(
                    &server_cert,
                    &server_key,
                    Some(&trusted_ca),
                    rustls::ALL_VERSIONS
                ),
                "localhost",
            )
            .is_ok(),
            "a client cert from the trusted CA must be accepted"
        );
        // No client identity is rejected by the client-auth-requiring server.
        assert!(
            do_handshake(
                client_config(Some(&trusted_ca), None, &policy),
                server_config(
                    &server_cert,
                    &server_key,
                    Some(&trusted_ca),
                    rustls::ALL_VERSIONS
                ),
                "localhost",
            )
            .is_err(),
            "a missing client cert must be rejected"
        );
        // A client identity from a foreign CA is rejected.
        assert!(
            do_handshake(
                client_config(
                    Some(&trusted_ca),
                    Some((&foreign_cert, &foreign_key)),
                    &policy,
                ),
                server_config(
                    &server_cert,
                    &server_key,
                    Some(&trusted_ca),
                    rustls::ALL_VERSIONS
                ),
                "localhost",
            )
            .is_err(),
            "a client cert from an untrusted CA must be rejected"
        );
        // Keep the foreign CA PEM live so the intent is explicit.
        assert!(!foreign_ca.is_empty());
    }

    #[test]
    fn skip_verifier_still_checks_certificate_verify_signatures() {
        let (_ca, issuer) = make_ca("trusted-ca");
        let (leaf_pem, _key) = make_leaf(&issuer, "etcd-server", "localhost", true);
        let leaf = CertificateDer::from_pem_slice(leaf_pem.as_bytes())
            .unwrap_or_else(|error| unreachable!("parse: {error}"));
        let verifier =
            SkipServerVerification::new(Arc::new(rustls::crypto::ring::default_provider()));
        let message = b"an arbitrary transcript hash";
        let forged = forged_signature();

        assert!(
            verifier
                .verify_tls12_signature(message, &leaf, &forged)
                .is_err(),
            "a forged TLS1.2 CertificateVerify signature must be rejected"
        );
        assert!(
            verifier
                .verify_tls13_signature(message, &leaf, &forged)
                .is_err(),
            "a forged TLS1.3 CertificateVerify signature must be rejected"
        );

        // The production skip+CN shape wraps the skip verifier in the CN-pin
        // outer. Feed the same forged signature through the outer (with a
        // matching CN) so an unconditional assertion in the outer's signature
        // methods cannot pass unnoticed: the outer must delegate to the inner.
        let outer = CommonNamePinnedServerVerifier {
            inner: Arc::new(SkipServerVerification::new(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))),
            allowed_common_names: ["etcd-server".to_owned()].into_iter().collect(),
        };
        assert!(
            outer
                .verify_tls12_signature(message, &leaf, &forged)
                .is_err(),
            "the CN-pin outer must delegate TLS1.2 signature checks to the skip inner"
        );
        assert!(
            outer
                .verify_tls13_signature(message, &leaf, &forged)
                .is_err(),
            "the CN-pin outer must delegate TLS1.3 signature checks to the skip inner"
        );
    }
}
