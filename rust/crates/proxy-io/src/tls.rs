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

//! Frontend and backend TLS for the Rust dataplane (WIRE-04).
//!
//! The Go reference is `pkg/proxy/net/tls.go` and the TLS decisions in
//! `pkg/proxy/backend/authenticator.go`. The frontend upgrade happens after a
//! plaintext `SSLRequest`, so handshake bytes may already sit in the packet
//! reader's prefetch: [`accept_frontend`] replays that prefix before reading
//! from the socket, matching Go's "handshake must read from the buffered
//! reader" rule. Certificate material comes exclusively from `control-proto`'s
//! [`ValidatedSnapshot`](control_proto::ValidatedSnapshot): sessions capture an
//! immutable `Arc` at establishment, so a failed reload keeps last-good state
//! and a successful reload affects only new sessions.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use control_proto::snapshot::ValidatedTlsPolicy;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

/// Base connection buffer used when a snapshot supplies zero.
pub const DEFAULT_CONN_BUFFER_SIZE: usize = 32 * 1024;

const MIN_TLS_BUFFER: usize = 1024;
const MAX_TLS_READ_BUFFER: usize = 4 * 1024;
const MAX_TLS_WRITE_BUFFER: usize = 16 * 1024;

/// Post-handshake buffer sizes derived from the connection buffer.
///
/// Byte-for-byte Go `tlsBufferSizes` parity: zero normalizes to the 32-KiB
/// default, reads clamp `size/4` into `[1 KiB, 4 KiB]`, and writes clamp
/// `size/2` into `[1 KiB, 16 KiB]`, so a large base buffer never duplicates
/// full-size TLS memory.
#[must_use]
pub const fn tls_buffer_sizes(conn_buffer_size: usize) -> (usize, usize) {
    let normalized = if conn_buffer_size == 0 {
        DEFAULT_CONN_BUFFER_SIZE
    } else {
        conn_buffer_size
    };
    (
        clamp(normalized / 4, MIN_TLS_BUFFER, MAX_TLS_READ_BUFFER),
        clamp(normalized / 2, MIN_TLS_BUFFER, MAX_TLS_WRITE_BUFFER),
    )
}

const fn clamp(value: usize, low: usize, high: usize) -> usize {
    if value < low {
        low
    } else if value > high {
        high
    } else {
        value
    }
}

/// Typed TLS setup failures.
#[derive(Debug, thiserror::Error)]
pub enum TlsSetupError {
    /// The TLS handshake failed.
    #[error("TLS handshake failed: {0}")]
    Handshake(#[source] io::Error),
    /// The handshake did not finish within the configured deadline.
    #[error("TLS handshake timed out after {0:?}")]
    Timeout(Duration),
    /// The backend server name is not a valid SNI/IP name.
    #[error("invalid TLS server name {name:?}")]
    InvalidServerName {
        /// The rejected name.
        name: String,
    },
    /// The snapshot policy cannot produce a client configuration.
    #[error("invalid backend TLS policy: {0}")]
    Policy(String),
}

/// Decoded, bounded handshake facts for routing hooks and logs.
///
/// This is deliberately metadata-only: certificates, keys, and raw TLS bytes
/// never leave the transport layer, matching the control-protocol rule that
/// TLS material must not cross IPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsHandshakeInfo {
    /// Negotiated protocol version, as a stable label.
    pub protocol_version: Option<&'static str>,
    /// Negotiated cipher suite, as a stable label.
    pub cipher_suite: Option<String>,
    /// Whether the peer presented a certificate.
    pub peer_certificate_present: bool,
    /// The SNI requested by a frontend client, when any.
    pub server_name: Option<String>,
}

fn protocol_label(version: rustls::ProtocolVersion) -> &'static str {
    match version {
        rustls::ProtocolVersion::TLSv1_2 => "TLSv1.2",
        rustls::ProtocolVersion::TLSv1_3 => "TLSv1.3",
        _ => "unknown",
    }
}

/// A transport that replays caller-supplied prefix bytes before the inner
/// stream, then passes reads and writes through unchanged.
///
/// The frontend `SSLRequest` upgrade needs this: TLS client-hello bytes may
/// already be buffered above the socket when the upgrade starts. The prefix
/// buffer is zeroed and released as soon as it is fully replayed, and `Debug`
/// never prints its bytes — buffered client-hello material must not reach
/// logs or outlive its use.
pub struct PrefixedIo<S> {
    inner: S,
    prefix: Vec<u8>,
    position: usize,
}

impl<S> std::fmt::Debug for PrefixedIo<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixedIo")
            .field(
                "remaining_prefix_bytes",
                &(self.prefix.len() - self.position),
            )
            .finish_non_exhaustive()
    }
}

impl<S> PrefixedIo<S> {
    /// Wraps `inner`, replaying `prefix` before the first socket read.
    #[must_use]
    pub const fn new(inner: S, prefix: Vec<u8>) -> Self {
        Self {
            inner,
            prefix,
            position: 0,
        }
    }

    /// Returns the bytes not yet replayed.
    #[must_use]
    pub fn remaining_prefix(&self) -> &[u8] {
        &self.prefix[self.position..]
    }

    /// Consumes the wrapper and returns the inner transport.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position < self.prefix.len() {
            let remaining = &self.prefix[self.position..];
            let take = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..take]);
            self.position += take;
            if self.position == self.prefix.len() {
                // Zero and free the replayed client-hello bytes immediately.
                self.prefix.fill(0);
                self.prefix = Vec::new();
                self.position = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(context, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

/// An established frontend TLS session.
pub struct FrontendTls<S> {
    /// The TLS stream; the session layer wraps or splits it as needed.
    pub stream: server::TlsStream<PrefixedIo<S>>,
    /// Decoded handshake facts.
    pub info: TlsHandshakeInfo,
    /// Clamped `(read, write)` buffer sizes for layers above this stream.
    pub buffer_sizes: (usize, usize),
}

impl<S> std::fmt::Debug for FrontendTls<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrontendTls")
            .field("info", &self.info)
            .field("buffer_sizes", &self.buffer_sizes)
            .finish_non_exhaustive()
    }
}

/// An established backend TLS session.
pub struct BackendTls<S> {
    /// The TLS stream; the session layer wraps or splits it as needed.
    pub stream: client::TlsStream<S>,
    /// Decoded handshake facts.
    pub info: TlsHandshakeInfo,
    /// Clamped `(read, write)` buffer sizes for layers above this stream.
    pub buffer_sizes: (usize, usize),
}

impl<S> std::fmt::Debug for BackendTls<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendTls")
            .field("info", &self.info)
            .field("buffer_sizes", &self.buffer_sizes)
            .finish_non_exhaustive()
    }
}

/// Completes a server-side TLS handshake after a plaintext `SSLRequest`.
///
/// `buffered` is whatever the packet layer had already prefetched past the
/// `SSLRequest` packet; it is replayed before the socket is read, so no
/// client-hello byte is lost. The handshake is bounded by `timeout`.
///
/// # Errors
///
/// Returns a typed handshake or timeout error; the plaintext transport is
/// consumed either way, matching Go, where a failed upgrade closes the
/// connection rather than falling back to plaintext.
pub async fn accept_frontend<S>(
    stream: S,
    buffered: Vec<u8>,
    config: Arc<ServerConfig>,
    timeout: Duration,
    conn_buffer_size: usize,
) -> Result<FrontendTls<S>, TlsSetupError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let acceptor = TlsAcceptor::from(config);
    let transport = PrefixedIo::new(stream, buffered);
    let accepted = tokio::time::timeout(timeout, acceptor.accept(transport))
        .await
        .map_err(|_| TlsSetupError::Timeout(timeout))?
        .map_err(TlsSetupError::Handshake)?;
    let (_, connection) = accepted.get_ref();
    let info = TlsHandshakeInfo {
        protocol_version: connection.protocol_version().map(protocol_label),
        cipher_suite: connection
            .negotiated_cipher_suite()
            .map(|suite| format!("{:?}", suite.suite())),
        peer_certificate_present: connection.peer_certificates().is_some(),
        server_name: connection.server_name().map(str::to_owned),
    };
    Ok(FrontendTls {
        stream: accepted,
        info,
        buffer_sizes: tls_buffer_sizes(conn_buffer_size),
    })
}

/// Builds a backend client configuration from the validated snapshot policy.
///
/// Semantics follow Go's backend TLS decisions: the CA roots come from the
/// snapshot, `skip_ca_verification` mirrors `InsecureSkipVerify`, a client
/// certificate/key pair is presented when the policy carries one, and
/// `minimum_version` uses the same `""`/`"1.2"`/`"1.3"` contract that
/// `control-proto` enforces for the frontend.
///
/// # Errors
///
/// Returns [`TlsSetupError::Policy`] when the material cannot form a client
/// configuration (for example a certificate chain with a mismatched key).
pub fn build_backend_config(
    policy: &ValidatedTlsPolicy,
) -> Result<Arc<ClientConfig>, TlsSetupError> {
    // `1.3` is TLS 1.3 only. Go also accepts 1.0 and 1.1; rustls cannot offer
    // them, so every other value uses the TLS 1.2 floor.
    let builder = if policy.minimum_version == "1.3" {
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
    } else {
        ClientConfig::builder()
    };
    let builder = if policy.skip_ca_verification {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification::new()))
    } else {
        let mut roots = RootCertStore::empty();
        for root in &policy.roots.roots {
            roots.roots.push(root.clone());
        }
        builder.with_root_certificates(roots)
    };
    let config = match (policy.certificate_chain.is_empty(), policy.private_key()) {
        (false, Some(key)) => builder
            .with_client_auth_cert(policy.certificate_chain.clone(), key)
            .map_err(|error| TlsSetupError::Policy(error.to_string()))?,
        (false, None) => {
            return Err(TlsSetupError::Policy(
                "client certificate chain without a private key".to_owned(),
            ));
        }
        (true, _) => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

/// Completes a client-side TLS handshake to a backend.
///
/// `server_name` should be the backend's host as selected by routing; Go's
/// rule "use the DNS name as much as possible" is preserved because both DNS
/// names and IP literals parse into a valid rustls server name.
///
/// # Errors
///
/// Returns a typed server-name, handshake, or timeout error.
pub async fn connect_backend<S>(
    stream: S,
    server_name: &str,
    config: Arc<ClientConfig>,
    timeout: Duration,
    conn_buffer_size: usize,
) -> Result<BackendTls<S>, TlsSetupError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let name = ServerName::try_from(server_name.to_owned()).map_err(|_| {
        TlsSetupError::InvalidServerName {
            name: server_name.to_owned(),
        }
    })?;
    let connector = TlsConnector::from(config);
    let connected = tokio::time::timeout(timeout, connector.connect(name, stream))
        .await
        .map_err(|_| TlsSetupError::Timeout(timeout))?
        .map_err(TlsSetupError::Handshake)?;
    let (_, connection) = connected.get_ref();
    let info = TlsHandshakeInfo {
        protocol_version: connection.protocol_version().map(protocol_label),
        cipher_suite: connection
            .negotiated_cipher_suite()
            .map(|suite| format!("{:?}", suite.suite())),
        peer_certificate_present: connection.peer_certificates().is_some(),
        server_name: None,
    };
    Ok(BackendTls {
        stream: connected,
        info,
        buffer_sizes: tls_buffer_sizes(conn_buffer_size),
    })
}

/// Skips certificate-chain and hostname verification, mirroring Go
/// `InsecureSkipVerify` exactly: the handshake's `CertificateVerify`
/// signature is still validated against the presented certificate through the
/// crypto provider, so the peer must actually hold the certificate's private
/// key even when the chain is not trusted.
#[derive(Debug)]
struct SkipServerVerification {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl SkipServerVerification {
    fn new() -> Self {
        Self {
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_sizes_match_go_clamp_semantics() {
        // Zero normalizes to the 32-KiB default: read 8K→4K cap, write 16K.
        assert_eq!(tls_buffer_sizes(0), (4 * 1024, 16 * 1024));
        assert_eq!(tls_buffer_sizes(32 * 1024), (4 * 1024, 16 * 1024));
        // Small buffers clamp up to the 1-KiB minimum.
        assert_eq!(tls_buffer_sizes(1024), (1024, 1024));
        // Mid-size buffers pass through the derivation.
        assert_eq!(tls_buffer_sizes(8 * 1024), (2 * 1024, 4 * 1024));
        // Huge buffers stay capped.
        assert_eq!(tls_buffer_sizes(1024 * 1024), (4 * 1024, 16 * 1024));
    }

    #[test]
    fn invalid_server_name_is_typed() {
        let error = ServerName::try_from("bad name with spaces".to_owned());
        assert!(error.is_err());
    }

    /// The skip-CA verifier must still reject a forged handshake signature:
    /// only chain/hostname checks are skipped, matching Go `InsecureSkipVerify`.
    #[test]
    fn skip_verifier_rejects_bad_signatures() {
        // DigitallySignedStruct's constructor is crate-private; decode one
        // from its wire form (scheme 0x0403 = ecdsa_secp256r1_sha256, then a
        // u16-length-prefixed bogus signature) via the public Codec API.
        use rustls::internal::msgs::codec::{Codec, Reader};

        let verifier = SkipServerVerification::new();
        assert!(!verifier.supported_verify_schemes().is_empty());
        let bogus_cert = CertificateDer::from(vec![0x30, 0x03, 0x02, 0x01, 0x00]);
        let wire = [0x04, 0x03, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef];
        let mut reader = Reader::init(&wire);
        let Ok(dss) = DigitallySignedStruct::read(&mut reader) else {
            unreachable!("fixed wire form must decode")
        };
        assert!(
            verifier
                .verify_tls13_signature(b"handshake transcript", &bogus_cert, &dss)
                .is_err()
        );
        assert!(
            verifier
                .verify_tls12_signature(b"handshake transcript", &bogus_cert, &dss)
                .is_err()
        );
    }

    /// `Debug` must never print buffered client-hello bytes, and the prefix
    /// buffer must be released once fully replayed.
    #[tokio::test]
    async fn prefixed_io_redacts_debug_and_releases_prefix() -> Result<(), io::Error> {
        use tokio::io::AsyncReadExt;

        let (_writer, reader) = tokio::io::duplex(64);
        let mut prefixed = PrefixedIo::new(reader, vec![0xab, 0xcd, 0xef]);
        let rendered = format!("{prefixed:?}");
        assert!(rendered.contains("remaining_prefix_bytes: 3"), "{rendered}");
        assert!(
            !rendered.contains("0xab") && !rendered.contains("171"),
            "{rendered}"
        );

        let mut first = [0_u8; 2];
        prefixed.read_exact(&mut first).await?;
        assert_eq!(first, [0xab, 0xcd]);
        let mut second = [0_u8; 1];
        prefixed.read_exact(&mut second).await?;
        assert_eq!(second, [0xef]);
        assert!(prefixed.remaining_prefix().is_empty());
        assert_eq!(
            prefixed.prefix.capacity(),
            0,
            "prefix allocation must be released"
        );
        let rendered = format!("{prefixed:?}");
        assert!(rendered.contains("remaining_prefix_bytes: 0"), "{rendered}");
        Ok(())
    }
}
