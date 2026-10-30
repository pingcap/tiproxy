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

//! End-to-end session-engine tests (DPL-04): a real listener admits a
//! raw `MySQL` client, the engine routes through the real dispatcher
//! (driven by an observable fake control sender), authenticates
//! against a scripted fake backend, serves commands, and resolves
//! every control command to its exact gate terminal.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use control_proto::control_transport::{
    ClientConfig, ConnectionState, ControlClient, Handler, SessionMeta, TransportError,
};
use control_proto::v1::control_envelope::Body;
use control_proto::v1::{
    CloseCommand, ControlEnvelope, DrainCommand, ErrorCode, HandshakeDecision, Hello,
    RedirectCommand, Role, RouteAssignment,
};
use dataplane::control_dispatch::{
    ControlCommandHandler, DispatchSender, spawn_control_dispatch_parts,
};
use dataplane::metering::MeteringSourceRegistry;
use dataplane::observability::{MetricsRecorder, Observation, QuitSource};
use dataplane::session::SessionLoopConfig;
use dataplane::session_engine::{EngineSessionOwner, send_proxy_owned_query};
use dataplane::{
    BoundSessionHandler, DataplaneServer, DispatchConnectionHandler, SystemMemoryProbe,
};
use mysql_wire::{
    Attribute, CapabilityFlags, ChangeUserParams, HandshakeResponseParams, MAX_PAYLOAD_LEN,
    ResponseHeader, StatusFlags, encode_change_user, encode_eof_packet, encode_error_packet,
    encode_handshake_response, encode_initial_handshake, encode_length_encoded_bytes,
    encode_length_encoded_int, encode_ok_packet, encode_ssl_request, parse_change_user,
    parse_handshake_response, parse_initial_handshake, parse_ssl_request,
};
use proxy_io::compression::{
    CompressedFrameHeader, CompressedIo, CompressionAlgorithm, CompressionError, CompressionLimits,
};
use proxy_io::counted::CountedIo;
use proxy_io::direction::DirectionSync;
use proxy_io::tls::accept_frontend;
use proxy_io::{IoSide, PacketIo, PacketIoError, PacketReader, PacketWriter};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use session_core::handshake::{SUPPORTED_SERVER_CAPABILITIES, build_greeting};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

// ---------------------------------------------------------------------
// Harness pieces
// ---------------------------------------------------------------------

/// Observable dispatch sender (the loop's outbound half).
struct FakeSender {
    next: AtomicU64,
    sent: Mutex<Vec<ControlEnvelope>>,
}

impl FakeSender {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next: AtomicU64::new(1000),
            sent: Mutex::new(Vec::new()),
        })
    }

    fn sent(&self) -> Vec<ControlEnvelope> {
        let Ok(sent) = self.sent.lock() else {
            unreachable!("sent lock poisoned")
        };
        sent.clone()
    }
}

impl DispatchSender for FakeSender {
    fn allocate_request_id(&self) -> Option<u64> {
        Some(self.next.fetch_add(1, Ordering::Relaxed) + 1)
    }

    async fn send_envelope(&self, envelope: ControlEnvelope) -> Result<(), TransportError> {
        let Ok(mut sent) = self.sent.lock() else {
            unreachable!("sent lock poisoned")
        };
        sent.push(envelope);
        Ok(())
    }

    async fn send_session_scoped(
        &self,
        envelope: ControlEnvelope,
        _epoch: u64,
    ) -> Result<(), TransportError> {
        self.send_envelope(envelope).await
    }
}

fn control_client() -> Arc<ControlClient> {
    let hello = Hello {
        role: Role::RustDataplane as i32,
        process_id: "engine-test".to_owned(),
        supported_versions: vec![1],
        capabilities: vec![1, 2, 3],
        max_frame_bytes: 1024 * 1024,
        ..Hello::default()
    };
    let socket = std::env::temp_dir().join(format!("engine-test-{}.sock", std::process::id()));
    let Ok(client) = ControlClient::new(ClientConfig::with_defaults(socket, 0, hello)) else {
        unreachable!("valid client config")
    };
    Arc::new(client)
}

/// A scripted `MySQL` backend: greeting → read response → auth OK → OK
/// for every command until EOF/quit.
/// Every fake session authenticates with a real non-empty native
/// password, so the whole harness exercises the unknown-plugin
/// re-authentication path against the backend's own salt.
const FAKE_BACKEND_PASSWORD: &str = "s3cret-pa55";

/// Minimal SHA-1 (test-only) for the native-password challenge.
#[expect(
    clippy::many_single_char_names,
    reason = "the FIPS-180 reference notation"
)]
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut message = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in message.chunks_exact(64) {
        let mut w = [0_u32; 80];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &word) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(word);
            e = d;
            d = c;
            c = b;
            b = a.rotate_left(30);
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0_u8; 20];
    for (chunk, word) in out.chunks_exact_mut(4).zip(h) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// `mysql_native_password`: SHA1(pwd) XOR SHA1(salt ++ SHA1(SHA1(pwd))).
fn native_scramble(password: &str, salt: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = sha1(password.as_bytes());
    let stage2 = sha1(&stage1);
    let mut salted = salt.to_vec();
    salted.extend_from_slice(&stage2);
    let mask = sha1(&salted);
    stage1
        .iter()
        .zip(mask)
        .map(|(byte, mask)| byte ^ mask)
        .collect()
}

/// Go's authentication failure with the `using password` semantics.
async fn write_access_denied<W: AsyncWrite + Unpin>(
    writer: &mut PacketWriter<W>,
    capabilities: CapabilityFlags,
) -> bool {
    let Ok(denied) = encode_error_packet(
        1045,
        Some(*b"28000"),
        b"Access denied for user 'root' (using password: YES)",
        capabilities,
    ) else {
        return false;
    };
    writer.write_logical(&denied, true).await.is_ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FakeAuth {
    Initial,
    Migration,
}

const WIRE03_LARGE_QUERY: &[u8] = b"\x03SELECT wire03_large_response";
const WIRE03_EXACT_MULTIPLE_QUERY: &[u8] = b"\x03SELECT wire03_exact_multiple_response";
const WIRE03_LOCAL_INFILE_QUERY: &[u8] = b"\x03LOAD DATA LOCAL INFILE 'wire03.csv'";
/// RSP-004: one command, two result sets (see `respond_to_multi_result`).
const MULTI_RESULT_QUERY: &[u8] = b"\x03SELECT 0; SELECT 1";
/// opt#5: a result set whose rows stream continuously with NO terminator, until
/// the client disconnects. Exercises the continuation-packet FSM bypass under a
/// genuinely continuous response so a mid-stream forced close can be verified
/// to still take effect (see `respond_to_stream_rows`).
const STREAM_ROWS_QUERY: &[u8] = b"\x03SELECT stream_rows_until_close";
const WIRE03_LARGE_RESPONSE_LEN: usize = MAX_PAYLOAD_LEN as usize + 257;
const WIRE03_EXACT_MULTIPLE_RESPONSE_LEN: usize = MAX_PAYLOAD_LEN as usize;

// PKT-002 (plain, non-compression): a marker query drives a backend response
// sized to exercise MySQL's 16 MiB physical-packet boundary on the response leg
// through the plaintext production forward path.
const PKT_UNDER_MAX_QUERY: &[u8] = b"\x03SELECT pkt_under_max_response";
const PKT_LARGE_QUERY: &[u8] = b"\x03SELECT pkt_large_response";
const PKT_EXACT_MULTIPLE_QUERY: &[u8] = b"\x03SELECT pkt_exact_multiple_response";
const PKT_UNDER_MAX_RESPONSE_LEN: usize = MAX_PAYLOAD_LEN as usize - 1;
const PKT_LARGE_RESPONSE_LEN: usize = MAX_PAYLOAD_LEN as usize + 33;
const PKT_EXACT_MULTIPLE_RESPONSE_LEN: usize = MAX_PAYLOAD_LEN as usize;

/// The fake backend's connection phase: either the initial Go-oracle
/// unknown-plugin/auth-switch relay or MIG-01's direct session-token login.
async fn write_fake_backend_greeting<W: AsyncWrite + Unpin>(
    writer: &mut PacketWriter<W>,
    broad: CapabilityFlags,
) -> bool {
    let salt = [7_u8; 20];
    let params = build_greeting(
        broad,
        &salt,
        b"8.0.11-TiDB-fake",
        77,
        45,
        StatusFlags::from_bits_retain(0),
    );
    let Ok(greeting) = encode_initial_handshake(params) else {
        return false;
    };
    writer.write_logical(&greeting, true).await.is_ok()
}

/// Starts one backend that sends exactly one caller-supplied greeting. This is
/// enough to exercise the production backend parser and capability verifier:
/// both must reject before any authentication response is forwarded.
async fn spawn_one_greeting_backend(greeting: Vec<u8>) -> (u16, Arc<AtomicU64>) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("one-greeting backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("one-greeting backend address")
    };
    let accepted = Arc::new(AtomicU64::new(0));
    let task_accepted = Arc::clone(&accepted);
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        task_accepted.fetch_add(1, Ordering::Relaxed);
        let mut writer = PacketWriter::new(stream);
        let _ = writer.write_logical(&greeting, true).await;
    });
    (address.port(), accepted)
}

/// Starts a backend that performs a real caching-SHA2 fast-auth exchange:
/// switch → client response → `0x01 0x03` → OK. The OK is sent immediately,
/// without waiting for another client packet, so a proxy that incorrectly
/// changes turns after the fast-success marker deadlocks against the client.
async fn spawn_fast_auth_backend() -> (u16, Arc<AtomicU64>) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("fast-auth backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("fast-auth backend address")
    };
    let accepted = Arc::new(AtomicU64::new(0));
    let task_accepted = Arc::clone(&accepted);
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        task_accepted.fetch_add(1, Ordering::Relaxed);
        let (read, write) = stream.into_split();
        let mut reader = PacketReader::new(read);
        let mut writer = PacketWriter::new(write);
        let broad = fake_backend_capabilities(false);
        if !write_fake_backend_greeting(&mut writer, broad).await {
            return;
        }
        reader.reset_sequence(writer.next_sequence());
        let Ok(response) = reader.read_logical(64 * 1024).await else {
            return;
        };
        let Ok(parsed) = parse_handshake_response(&response.payload) else {
            return;
        };
        if parsed.auth_plugin_name != Some(b"auth_unknown_plugin".as_slice()) {
            return;
        }

        let mut switch = vec![0xFE];
        switch.extend_from_slice(b"caching_sha2_password\0");
        switch.extend_from_slice(&[9_u8; 20]);
        switch.push(0);
        writer.reset_sequence(reader.expected_sequence());
        if writer.write_logical(&switch, true).await.is_err() {
            return;
        }
        reader.reset_sequence(writer.next_sequence());
        let Ok(client_response) = reader.read_logical(64 * 1024).await else {
            return;
        };
        if client_response.payload != b"sha2-response" {
            return;
        }
        writer.reset_sequence(reader.expected_sequence());
        if writer.write_logical(&[0x01, 0x03], true).await.is_err() {
            return;
        }
        let Ok(ok) = encode_ok_packet(
            ResponseHeader::OK,
            0,
            0,
            StatusFlags::from_bits_retain(0x0002),
            0,
            b"",
            broad,
        ) else {
            return;
        };
        let _ = writer.write_logical(&ok, true).await;
    });
    (address.port(), accepted)
}

async fn finish_fake_backend_auth<R, W>(
    reader: &mut PacketReader<R>,
    writer: &mut PacketWriter<W>,
    broad: CapabilityFlags,
    expected_compression: Option<CompressionAlgorithm>,
) -> Option<FakeAuth>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let salt = [7_u8; 20];
    let expected_sequence = reader.expected_sequence();
    match reader.peek_packet().await {
        Ok(preview) if preview.sequence_id == expected_sequence => {}
        _ => return None,
    }
    let Ok(response) = reader.read_logical(64 * 1024).await else {
        return None;
    };
    let Ok(parsed) = parse_handshake_response(&response.payload) else {
        return None;
    };
    match expected_compression {
        Some(CompressionAlgorithm::Zlib)
            if !parsed.capabilities.contains(CapabilityFlags::COMPRESS) =>
        {
            return None;
        }
        Some(CompressionAlgorithm::Zstd { level })
            if !parsed
                .capabilities
                .contains(CapabilityFlags::ZSTD_COMPRESSION_ALGORITHM)
                || parsed.zstd_level != u8::try_from(level).ok() =>
        {
            return None;
        }
        _ => {}
    }
    if parsed.auth_plugin_name == Some(b"tidb_session_token".as_slice()) {
        writer.reset_sequence(reader.expected_sequence());
        if parsed.auth_response != b"signed-token-private"
            || parsed.database != Some(b"snapshot_db".as_slice())
        {
            let _ = write_access_denied(writer, broad).await;
            return None;
        }
        let Ok(auth_ok) = encode_ok_packet(
            ResponseHeader::OK,
            0,
            0,
            StatusFlags::from_bits_retain(0x0002),
            0,
            b"",
            broad,
        ) else {
            return None;
        };
        return writer
            .write_logical(&auth_ok, true)
            .await
            .is_ok()
            .then_some(FakeAuth::Migration);
    }
    if parsed.auth_plugin_name != Some(b"auth_unknown_plugin".as_slice()) {
        writer.reset_sequence(reader.expected_sequence());
        let _ = write_access_denied(writer, broad).await;
        return None;
    }
    let mut switch = vec![0xFE_u8];
    switch.extend_from_slice(b"mysql_native_password\0");
    switch.extend_from_slice(&salt);
    switch.push(0);
    writer.reset_sequence(reader.expected_sequence());
    if writer.write_logical(&switch, true).await.is_err() {
        return None;
    }
    reader.reset_sequence(writer.next_sequence());
    let Ok(rescrambled) = reader.read_logical(64 * 1024).await else {
        return None;
    };
    writer.reset_sequence(reader.expected_sequence());
    if rescrambled.payload != native_scramble(FAKE_BACKEND_PASSWORD, &salt) {
        let _ = write_access_denied(writer, broad).await;
        return None;
    }
    let Ok(auth_ok) = encode_ok_packet(
        ResponseHeader::OK,
        0,
        0,
        StatusFlags::from_bits_retain(0x0002),
        0,
        b"",
        broad,
    ) else {
        return None;
    };
    writer
        .write_logical(&auth_ok, true)
        .await
        .is_ok()
        .then_some(FakeAuth::Initial)
}

/// The fake backend's connection phase: either the initial Go-oracle
/// unknown-plugin/auth-switch relay or MIG-01's direct session-token login.
async fn fake_backend_auth<R, W>(
    reader: &mut PacketReader<R>,
    writer: &mut PacketWriter<W>,
    broad: CapabilityFlags,
) -> Option<FakeAuth>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if !write_fake_backend_greeting(writer, broad).await {
        return None;
    }
    reader.reset_sequence(writer.next_sequence());
    finish_fake_backend_auth(reader, writer, broad, None).await
}

fn result_column(name: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    for field in [b"def".as_slice(), b"db", b"t", b"t", name, name] {
        assert!(encode_length_encoded_bytes(Some(field), &mut payload).is_ok());
    }
    encode_length_encoded_int(12, &mut payload);
    payload.extend_from_slice(&[45, 0, 11, 0, 0, 0, 0x03, 0, 0, 0, 0, 0]);
    payload
}

/// The fixed `COM_STMT_PREPARE`-OK header: `0x00`, statement ID, column and
/// parameter counts, one reserved filler, and the warning count. The wire
/// order is columns-before-parameters even though the parameter definitions
/// are streamed first (Go's `forwardPrepareCmd` reads them from these offsets).
fn encode_prepare_ok(statement_id: u32, columns: u16, parameters: u16, warnings: u16) -> Vec<u8> {
    let mut payload = vec![0x00_u8];
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.extend_from_slice(&columns.to_le_bytes());
    payload.extend_from_slice(&parameters.to_le_bytes());
    payload.push(0x00);
    payload.extend_from_slice(&warnings.to_le_bytes());
    payload
}

/// The `DEPRECATE_EOF` result-set terminator carrying `status`: an OK packet
/// with the `0xFE` header and a >=7-byte body, which the observer recognizes as
/// the resultset OK (not a classic 5-byte EOF and not a data row). Execute
/// surfaces `CURSOR_EXISTS` here; fetch surfaces `LAST_ROW_SENT`.
fn encode_resultset_terminator(status: StatusFlags, capabilities: CapabilityFlags) -> Vec<u8> {
    match encode_ok_packet(
        ResponseHeader::EOF_OR_AUTH_SWITCH,
        0,
        0,
        status,
        0,
        b"",
        capabilities,
    ) {
        Ok(packet) => packet,
        Err(_) => unreachable!("resultset OK terminator encodes"),
    }
}

/// RSP-004: two result sets for one command. The first terminator carries
/// `SERVER_MORE_RESULTS_EXISTS`, so the proxy must keep reading the backend
/// instead of handing the command back to the client; the second clears it and
/// ends the command. A proxy that stops at the first terminator leaves the
/// second result set unread in the backend socket and desynchronises the
/// session, which the follow-up command in the test detects.
async fn respond_to_multi_result<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    capabilities: CapabilityFlags,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    writer.reset_sequence(reader.expected_sequence());
    for (index, more) in [(0u8, true), (1u8, false)] {
        for packet in [vec![0x01], result_column(b"c")] {
            if writer.write_logical(&packet, true).await.is_err() {
                return false;
            }
        }
        // One text row carrying the result-set index, so the two sets are
        // distinguishable on the wire rather than merely counted.
        if writer
            .write_logical(&[0x01, b'0' + index], true)
            .await
            .is_err()
        {
            return false;
        }
        let status = if more {
            StatusFlags::AUTOCOMMIT | StatusFlags::MORE_RESULTS_EXISTS
        } else {
            StatusFlags::AUTOCOMMIT
        };
        if writer
            .write_logical(&encode_resultset_terminator(status, capabilities), true)
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

/// opt#5: streams a single result set's rows forever with NO terminator, until
/// the client (through the proxy) disconnects and the write fails. This is a
/// continuous, unterminated `Continue`-packet response: the column-count and
/// column-definition open the set, then row packets stream until close drives
/// the end (bounded memory — one small packet at a time). Yields periodically
/// so, on a single-threaded runtime shared with the proxy tasks, the forwarding
/// and control tasks still get scheduled. Never writes a result-set terminator,
/// so any clean completion the client observes would be a bug.
async fn respond_to_stream_rows<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    capabilities: CapabilityFlags,
) where
    W: AsyncWrite + Unpin,
{
    let _ = capabilities;
    writer.reset_sequence(reader.expected_sequence());
    for opener in [vec![0x01], result_column(b"c")] {
        if writer.write_logical(&opener, true).await.is_err() {
            return;
        }
    }
    let mut written: u64 = 0;
    loop {
        // One-byte length-encoded text row, same shape as respond_to_multi_result.
        if writer.write_logical(&[0x01, b'r'], true).await.is_err() {
            return;
        }
        written += 1;
        if written % 32 == 0 {
            tokio::task::yield_now().await;
        }
    }
}

/// Emits a `COM_STMT_EXECUTE` response. The execute flags byte (payload[5])
/// selects the shape: `0x80` sentinel = a backend error (e.g. execute after
/// long data), `0x01` (read-only cursor) = a result-set header + column
/// definition + a cursor-open terminator (`CURSOR_EXISTS`, no rows), otherwise
/// a one-row result set with a plain terminator (no cursor).
async fn respond_to_execute<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    payload: &[u8],
    capabilities: CapabilityFlags,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    let flags = payload.get(5).copied().unwrap_or(0);
    writer.reset_sequence(reader.expected_sequence());
    if flags & 0x80 != 0 {
        let Ok(error) =
            encode_error_packet(1064, Some(*b"42000"), b"execute rejected", capabilities)
        else {
            return false;
        };
        return writer.write_logical(&error, true).await.is_ok();
    }
    for packet in [vec![0x01], result_column(b"c")] {
        if writer.write_logical(&packet, true).await.is_err() {
            return false;
        }
    }
    if flags & 0x01 != 0 {
        // Read-only cursor: the terminator opens the cursor; rows come by fetch.
        let terminator = encode_resultset_terminator(
            StatusFlags::AUTOCOMMIT | StatusFlags::CURSOR_EXISTS,
            capabilities,
        );
        return writer.write_logical(&terminator, true).await.is_ok();
    }
    // No cursor: one binary row then a plain terminator.
    if writer.write_logical(&[0x00, 0x00], true).await.is_err() {
        return false;
    }
    let terminator = encode_resultset_terminator(StatusFlags::AUTOCOMMIT, capabilities);
    writer.write_logical(&terminator, true).await.is_ok()
}

/// Emits a `COM_STMT_FETCH` response: one row on the first fetch of a statement
/// (the cursor stays open), then a `LAST_ROW_SENT` terminator on the second.
async fn respond_to_fetch<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    payload: &[u8],
    fetch_counts: &mut HashMap<u32, u32>,
    capabilities: CapabilityFlags,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    let statement_id = u32::from_le_bytes([
        payload.get(1).copied().unwrap_or(0),
        payload.get(2).copied().unwrap_or(0),
        payload.get(3).copied().unwrap_or(0),
        payload.get(4).copied().unwrap_or(0),
    ]);
    let count = fetch_counts.entry(statement_id).or_insert(0);
    *count += 1;
    let last = *count >= 2;
    writer.reset_sequence(reader.expected_sequence());
    if !last {
        if writer.write_logical(&[0x00, 0x00], true).await.is_err() {
            return false;
        }
        let terminator = encode_resultset_terminator(StatusFlags::AUTOCOMMIT, capabilities);
        return writer.write_logical(&terminator, true).await.is_ok();
    }
    let terminator = encode_resultset_terminator(
        StatusFlags::AUTOCOMMIT | StatusFlags::LAST_ROW_SENT,
        capabilities,
    );
    writer.write_logical(&terminator, true).await.is_ok()
}

/// Emits a `COM_STMT_PREPARE` special response for the scripted backend. A
/// fixed statement ID (7) keeps the register/guard-replacement discriminator
/// deterministic; markers in the prepare text pick the branch: `FAIL` → a
/// leading backend error, `NOMETA` → a zero-metadata prepare-OK, otherwise a
/// two-parameter/one-column prepare-OK. `DEPRECATE_EOF` is negotiated on both
/// legs, so the parameter then column definitions carry no classic EOF.
async fn respond_to_prepare<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    prepare_text: &[u8],
    capabilities: CapabilityFlags,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    writer.reset_sequence(reader.expected_sequence());
    if prepare_text.starts_with(b"BADHDR") {
        // A prepare-OK first byte followed by a truncated header (< the
        // canonical 12 bytes): the observer must reject it and the proxy must
        // tear the session down rather than treat it as a complete prepare.
        return writer
            .write_logical(&[0x00, 0xAA, 0xBB], true)
            .await
            .is_ok();
    }
    if prepare_text.windows(4).any(|window| window == b"FAIL") {
        let Ok(err) = encode_error_packet(
            1064,
            Some(*b"42000"),
            b"You have an error in your SQL syntax",
            capabilities,
        ) else {
            return false;
        };
        return writer.write_logical(&err, true).await.is_ok();
    }
    let (parameters, columns): (u16, u16) = if prepare_text.starts_with(b"NOMETA") {
        (0, 0)
    } else {
        (2, 1)
    };
    if writer
        .write_logical(&encode_prepare_ok(7, columns, parameters, 0), true)
        .await
        .is_err()
    {
        return false;
    }
    for index in 0..parameters {
        let name = format!("p{index}");
        if writer
            .write_logical(&result_column(name.as_bytes()), true)
            .await
            .is_err()
        {
            return false;
        }
    }
    for index in 0..columns {
        let name = format!("c{index}");
        if writer
            .write_logical(&result_column(name.as_bytes()), true)
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone, Copy)]
enum SnapshotReply {
    Valid,
    InvalidToken,
    ExpiredToken,
    RestoreError,
    RestoreDisconnect,
    SigningCertificateError,
    NullToken,
    EmptyToken,
    MalformedJson,
    OversizedJson,
    Disconnect,
    /// Snapshot/restore succeed exactly like `Valid`, but the migrated backend
    /// rejects the first replayed `BEGIN` with a `MySQL` ERR (SES-07/MIG-005
    /// regression: a statusless ERR must not leave the engine's transaction
    /// tracker stale after the internal COMMIT closed the transaction).
    ReplayBeginError,
}

async fn write_session_snapshot<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    session_states: &[u8],
    session_token: Option<&[u8]>,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    writer.reset_sequence(reader.expected_sequence());
    let mut row = Vec::new();
    assert!(encode_length_encoded_bytes(Some(session_states), &mut row).is_ok());
    assert!(encode_length_encoded_bytes(session_token, &mut row).is_ok());
    for payload in [
        vec![2],
        result_column(b"Session_states"),
        result_column(b"Session_token"),
        encode_eof_packet(0, StatusFlags::AUTOCOMMIT).to_vec(),
        row,
    ] {
        if writer.write_logical(&payload, false).await.is_err() {
            return false;
        }
    }
    writer
        .write_logical(&encode_eof_packet(0, StatusFlags::AUTOCOMMIT), true)
        .await
        .is_ok()
}

async fn respond_to_snapshot_query<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    snapshot_reply: SnapshotReply,
    capabilities: CapabilityFlags,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    if matches!(snapshot_reply, SnapshotReply::Disconnect) {
        return false;
    }
    if matches!(snapshot_reply, SnapshotReply::SigningCertificateError) {
        writer.reset_sequence(reader.expected_sequence());
        let Ok(error) = encode_error_packet(
            1105,
            Some(*b"HY000"),
            b"session token signing certificate unavailable",
            capabilities,
        ) else {
            return false;
        };
        return writer.write_logical(&error, true).await.is_ok();
    }

    let oversized;
    let (session_states, session_token): (&[u8], Option<&[u8]>) = match snapshot_reply {
        SnapshotReply::Valid
        | SnapshotReply::RestoreError
        | SnapshotReply::RestoreDisconnect
        | SnapshotReply::ReplayBeginError => (
            br#"{"current-db":"snapshot_db","marker":"all-bytes-preserved"}"#,
            Some(b"signed-token-private"),
        ),
        SnapshotReply::InvalidToken => (
            br#"{"current-db":"snapshot_db"}"#,
            Some(b"invalid-token-private"),
        ),
        SnapshotReply::ExpiredToken => (
            br#"{"current-db":"snapshot_db"}"#,
            Some(b"expired-token-private"),
        ),
        SnapshotReply::NullToken => (br#"{"current-db":"snapshot_db"}"#, None),
        SnapshotReply::EmptyToken => (br#"{"current-db":"snapshot_db"}"#, Some(b"")),
        SnapshotReply::MalformedJson => (b"{", Some(b"signed-token-private")),
        SnapshotReply::OversizedJson => {
            oversized = vec![b'x'; 8 * 1024 * 1024 + 1];
            (&oversized, Some(b"signed-token-private"))
        }
        SnapshotReply::SigningCertificateError | SnapshotReply::Disconnect => {
            unreachable!("handled above")
        }
    };
    write_session_snapshot(reader, writer, session_states, session_token).await
}

/// Reads an inbound PROXY v2 header from a raw backend stream, returning the
/// complete on-wire header or `None` on a short/malformed header. Framing
/// mirrors the proxy's own probe (12-byte magic, 4-byte fixed header, sized
/// body).
async fn strip_inbound_proxy_v2(read: &mut tokio::net::tcp::OwnedReadHalf) -> Option<Vec<u8>> {
    use proxy_io::proxy_protocol::{FIXED_HEADER_LEN, MAGIC_V2, ProxyV2Decode, decode_after_magic};
    let mut magic = [0_u8; MAGIC_V2.len()];
    if read.read_exact(&mut magic).await.is_err() || magic != MAGIC_V2 {
        return None;
    }
    let mut wire = vec![0_u8; FIXED_HEADER_LEN];
    if read.read_exact(&mut wire).await.is_err() {
        return None;
    }
    let body_len = match decode_after_magic(&wire) {
        ProxyV2Decode::Incomplete { needed_total } => needed_total.saturating_sub(FIXED_HEADER_LEN),
        ProxyV2Decode::Done { .. } => 0,
    };
    if body_len > 0 {
        wire.resize(FIXED_HEADER_LEN + body_len, 0);
        if read
            .read_exact(&mut wire[FIXED_HEADER_LEN..])
            .await
            .is_err()
        {
            return None;
        }
    }
    let mut header = Vec::with_capacity(MAGIC_V2.len() + wire.len());
    header.extend_from_slice(&magic);
    header.extend_from_slice(&wire);
    Some(header)
}

async fn respond_to_restore_query<R, W>(
    reader: &PacketReader<R>,
    writer: &mut PacketWriter<W>,
    snapshot_reply: SnapshotReply,
    broad: CapabilityFlags,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    if matches!(snapshot_reply, SnapshotReply::RestoreDisconnect) {
        return false;
    }
    writer.reset_sequence(reader.expected_sequence());
    if matches!(snapshot_reply, SnapshotReply::RestoreError) {
        let Ok(error) = encode_error_packet(1105, Some(*b"HY000"), b"restore rejected", broad)
        else {
            return false;
        };
        return writer.write_logical(&error, true).await.is_ok();
    }
    let Ok(ok) = encode_ok_packet(
        ResponseHeader::OK,
        0,
        0,
        StatusFlags::AUTOCOMMIT,
        0,
        b"",
        broad,
    ) else {
        return false;
    };
    writer.write_logical(&ok, true).await.is_ok()
}

fn fake_backend_capabilities(tls: bool) -> CapabilityFlags {
    let mut broad = CapabilityFlags::PROTOCOL_41
        | CapabilityFlags::LONG_PASSWORD
        | CapabilityFlags::SECURE_CONNECTION
        | CapabilityFlags::PLUGIN_AUTH
        | CapabilityFlags::CONNECT_WITH_DB
        | CapabilityFlags::TRANSACTIONS
        | CapabilityFlags::MULTI_STATEMENTS
        | CapabilityFlags::MULTI_RESULTS
        | CapabilityFlags::PS_MULTI_RESULTS
        | CapabilityFlags::CONNECT_ATTRS
        | CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA
        | CapabilityFlags::DEPRECATE_EOF
        | CapabilityFlags::LOCAL_FILES;
    if tls {
        broad |= CapabilityFlags::SSL;
    }
    broad
}

/// Runs the backend side of one strict, multi-round `COM_CHANGE_USER`
/// exchange. The first challenge carries a fresh native-password salt; a
/// second opaque auth-data round proves that the runtime can reverse direction
/// more than once before the terminal packet. Special usernames select the
/// denied and malformed-OK branches used by fail-closed tests.
async fn respond_to_change_user<R, W>(
    reader: &mut PacketReader<R>,
    writer: &mut PacketWriter<W>,
    request: &[u8],
    broad: CapabilityFlags,
) -> bool
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let Ok(parsed) = parse_change_user(request, broad) else {
        return false;
    };
    if parsed.auth_response != b""
        || parsed.auth_plugin_name != Some(b"auth_unknown_plugin".as_slice())
    {
        return false;
    }

    let fresh_salt = [9_u8; 20];
    let mut switch = vec![0xFE_u8];
    switch.extend_from_slice(b"mysql_native_password\0");
    switch.extend_from_slice(&fresh_salt);
    switch.push(0);
    writer.reset_sequence(reader.expected_sequence());
    if writer.write_logical(&switch, true).await.is_err() {
        return false;
    }
    reader.reset_sequence(writer.next_sequence());
    let Ok(first_response) = reader.read_logical(64 * 1024).await else {
        return false;
    };
    if first_response.payload != native_scramble(FAKE_BACKEND_PASSWORD, &fresh_salt) {
        return false;
    }

    // A second backend-owned auth packet forces another backend->client then
    // client->backend turn. The payload is opaque to the proxy by design.
    writer.reset_sequence(reader.expected_sequence());
    if writer.write_logical(&[0x01, 0x04], true).await.is_err() {
        return false;
    }
    reader.reset_sequence(writer.next_sequence());
    let Ok(second_response) = reader.read_logical(64 * 1024).await else {
        return false;
    };
    if second_response.payload != b"second-auth-response" {
        return false;
    }

    writer.reset_sequence(reader.expected_sequence());
    if parsed.username == b"denied_user" {
        let Ok(error) = encode_error_packet(
            1045,
            Some(*b"28000"),
            b"Access denied for change user",
            broad,
        ) else {
            return false;
        };
        return writer.write_logical(&error, true).await.is_ok();
    }
    if parsed.username == b"malformed_ok" {
        // Header-only OK: classification sees OK, but status parsing must fail
        // closed instead of retaining the previous transaction flag.
        return writer.write_logical(&[0x00], true).await.is_ok();
    }
    let Ok(ok) = encode_ok_packet(
        ResponseHeader::OK,
        0,
        0,
        StatusFlags::AUTOCOMMIT,
        0,
        b"",
        broad,
    ) else {
        return false;
    };
    writer.write_logical(&ok, true).await.is_ok()
}

// The scripted backend's command loop dispatches every proxied command type
// (query, prepare, execute, fetch, long-data, change-user, session-states);
// its length tracks that command surface, not incidental complexity.
#[allow(clippy::too_many_lines)]
async fn run_fake_backend_commands<R, W>(
    mut reader: PacketReader<R>,
    mut writer: PacketWriter<W>,
    auth: FakeAuth,
    transcript: Arc<Mutex<Vec<Vec<u8>>>>,
    snapshot_reply: SnapshotReply,
    broad: CapabilityFlags,
    send_idle_byte: bool,
) -> u64
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // WIRE-MTR (idle-liveness probe): optionally push ONE unsolicited raw
    // byte on the backend->proxy direction right after auth, before the
    // command loop's peek. It is not a MySQL packet; the proxy consumes it
    // through the count-aware raw liveness probe.
    if send_idle_byte {
        let _ = writer.get_mut().write_all(&[0xFF]).await;
        let _ = writer.get_mut().flush().await;
    }
    // Command loop: OK for everything until quit/EOF.
    let mut in_transaction = false;
    // Per-statement COM_STMT_FETCH counter: the first fetch of a cursor keeps it
    // open, the second reports SERVER_STATUS_LAST_ROW_SENT.
    let mut fetch_counts: HashMap<u32, u32> = HashMap::new();
    loop {
        reader.reset_sequence(0);
        // Strict wire sequence: every proxied command must restart
        // its exchange at zero, exactly like the Go oracle.
        match reader.peek_packet().await {
            Ok(preview) if preview.sequence_id == 0 => {}
            _ => break,
        }
        let Ok(packet) = reader.read_logical(1024 * 1024).await else {
            break;
        };
        if let Ok(mut commands) = transcript.lock() {
            commands.push(packet.payload.clone());
        }
        if packet.payload.first() == Some(&0x01) {
            break; // COM_QUIT
        }
        if packet.payload.first() == Some(&0x11) {
            if !respond_to_change_user(&mut reader, &mut writer, &packet.payload, broad).await {
                break;
            }
            in_transaction = false;
            continue;
        }
        if packet.payload == MULTI_RESULT_QUERY {
            if !respond_to_multi_result(&reader, &mut writer, broad).await {
                break;
            }
            continue;
        }
        if packet.payload == STREAM_ROWS_QUERY {
            // Streams until the proxy tears the backend down (write fails); the
            // connection is finished once it returns.
            respond_to_stream_rows(&reader, &mut writer, broad).await;
            break;
        }
        if packet.payload == b"\x03SHOW SESSION_STATES" {
            if !respond_to_snapshot_query(&reader, &mut writer, snapshot_reply, broad).await {
                break;
            }
            continue;
        }
        if packet.payload.starts_with(b"\x03SET SESSION_STATES '") {
            if auth != FakeAuth::Migration {
                break;
            }
            if !respond_to_restore_query(&reader, &mut writer, snapshot_reply, broad).await {
                break;
            }
            continue;
        }
        if packet.payload.first() == Some(&0x18) {
            // COM_STMT_SEND_LONG_DATA has no response.
            continue;
        }
        if packet.payload.first() == Some(&0x16) {
            // COM_STMT_PREPARE special response (see `respond_to_prepare`).
            if !respond_to_prepare(&reader, &mut writer, &packet.payload[1..], broad).await {
                break;
            }
            continue;
        }
        if packet.payload.first() == Some(&0x17) {
            // COM_STMT_EXECUTE (see `respond_to_execute`).
            if !respond_to_execute(&reader, &mut writer, &packet.payload, broad).await {
                break;
            }
            continue;
        }
        if packet.payload.first() == Some(&0x1c) {
            // COM_STMT_FETCH (see `respond_to_fetch`).
            if !respond_to_fetch(
                &reader,
                &mut writer,
                &packet.payload,
                &mut fetch_counts,
                broad,
            )
            .await
            {
                break;
            }
            continue;
        }
        // SES-07/MIG-005 regression: the migrated backend rejects the first
        // replayed BEGIN with a MySQL ERR (statusless), so the engine's
        // transaction tracker must already reflect the internal COMMIT that
        // closed the old transaction.
        if auth == FakeAuth::Migration
            && matches!(snapshot_reply, SnapshotReply::ReplayBeginError)
            && packet.payload == b"\x03BEGIN"
        {
            writer.reset_sequence(reader.expected_sequence());
            let Ok(err) =
                encode_error_packet(1105, Some(*b"HY000"), b"replayed begin rejected", broad)
            else {
                break;
            };
            if writer.write_logical(&err, true).await.is_err() {
                break;
            }
            continue;
        }
        if packet.payload == PKT_UNDER_MAX_QUERY
            || packet.payload == PKT_LARGE_QUERY
            || packet.payload == PKT_EXACT_MULTIPLE_QUERY
        {
            writer.reset_sequence(reader.expected_sequence());
            let Ok(mut ok) = encode_ok_packet(
                ResponseHeader::OK,
                1,
                0,
                StatusFlags::AUTOCOMMIT,
                0,
                b"",
                broad,
            ) else {
                break;
            };
            let response_len = if packet.payload == PKT_UNDER_MAX_QUERY {
                PKT_UNDER_MAX_RESPONSE_LEN
            } else if packet.payload == PKT_LARGE_QUERY {
                PKT_LARGE_RESPONSE_LEN
            } else {
                PKT_EXACT_MULTIPLE_RESPONSE_LEN
            };
            ok.resize(response_len, b'L');
            if writer.write_logical(&ok, true).await.is_err() {
                break;
            }
            continue;
        }
        if packet.payload.windows(5).any(|window| window == b"BEGIN") {
            in_transaction = true;
        }
        if packet.payload.windows(6).any(|window| window == b"COMMIT") {
            in_transaction = false;
        }
        if packet.payload.windows(5).any(|window| window == b"SLEEP") {
            // Simulates a long-running statement so a force
            // deadline can preempt an in-flight command.
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        if packet.payload.windows(4).any(|window| window == b"HANG") {
            // A permanently stuck backend: only a force close ever
            // ends this command.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        writer.reset_sequence(reader.expected_sequence());
        let status = 0x0002 | u16::from(in_transaction);
        let Ok(ok) = encode_ok_packet(
            ResponseHeader::OK,
            1,
            0,
            StatusFlags::from_bits_retain(status),
            0,
            b"",
            broad,
        ) else {
            break;
        };
        if writer.write_logical(&ok, true).await.is_err() {
            break;
        }
    }
    writer.out_bytes()
}

fn fake_backend_tls_config() -> Arc<ServerConfig> {
    let Ok(CertifiedKey { cert, signing_key }) =
        generate_simple_self_signed(["127.0.0.1".to_owned()])
    else {
        unreachable!("generate fake backend certificate")
    };
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let Ok(config) = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![CertificateDer::from(cert.der().to_vec())], key)
    else {
        unreachable!("fake backend TLS identity")
    };
    Arc::new(config)
}

async fn run_fake_tls_backend_connection(
    stream: TcpStream,
    tls_config: Arc<ServerConfig>,
    transcript: Arc<Mutex<Vec<Vec<u8>>>>,
    snapshot_reply: SnapshotReply,
    send_idle_byte: bool,
) -> u64 {
    let broad = fake_backend_capabilities(true);
    let counted = CountedIo::new(stream);
    let counters = counted.counters();
    let (read, write) = tokio::io::split(counted);
    let mut reader = PacketReader::new(read);
    let mut writer = PacketWriter::new(write);
    if !write_fake_backend_greeting(&mut writer, broad).await {
        return counters.outbound();
    }
    let Ok(ssl_request) = reader.read_logical(64 * 1024).await else {
        return counters.outbound();
    };
    if parse_ssl_request(&ssl_request.payload).is_err() {
        return counters.outbound();
    }
    let next_sequence = reader.expected_sequence();
    let read = reader.into_inner();
    let write = writer.into_inner();
    let stream = read.unsplit(write);
    let Ok(tls) = accept_frontend(
        stream,
        Vec::new(),
        tls_config,
        Duration::from_secs(5),
        32 * 1024,
    )
    .await
    else {
        return counters.outbound();
    };
    let (read, write) = tokio::io::split(tls.stream);
    let mut reader = PacketReader::new(read);
    reader.reset_sequence(next_sequence);
    let mut writer = PacketWriter::new(write);
    writer.reset_sequence(next_sequence);
    let Some(auth) = finish_fake_backend_auth(&mut reader, &mut writer, broad, None).await else {
        return counters.outbound();
    };
    let _ = run_fake_backend_commands(
        reader,
        writer,
        auth,
        transcript,
        snapshot_reply,
        broad,
        send_idle_byte,
    )
    .await;
    counters.outbound()
}

async fn run_fake_compressed_local_infile(
    io: &mut PacketIo<CompressedTestTransport>,
    transcript: &Arc<Mutex<Vec<Vec<u8>>>>,
    broad: CapabilityFlags,
) -> bool {
    io.reset_write_sequence(io.expected_read_sequence());
    if io.write_logical(b"\xfbwire03.csv", true).await.is_err() {
        return false;
    }
    loop {
        let Ok(chunk) = io.read_logical(1024 * 1024).await else {
            return false;
        };
        if let Ok(mut commands) = transcript.lock() {
            commands.push(chunk.payload.clone());
        }
        if chunk.payload.is_empty() {
            break;
        }
    }
    let Ok(ok) = encode_ok_packet(
        ResponseHeader::OK,
        1,
        0,
        StatusFlags::AUTOCOMMIT,
        0,
        b"",
        broad,
    ) else {
        return false;
    };
    // Do not manually reset the packet sequence here. The compressed
    // transport's direction transition must carry the sequence from the
    // multi-packet upload into this final response.
    io.write_logical(&ok, true).await.is_ok()
}

async fn run_fake_compressed_backend_commands(
    mut io: PacketIo<CompressedTestTransport>,
    auth: FakeAuth,
    transcript: Arc<Mutex<Vec<Vec<u8>>>>,
    snapshot_reply: SnapshotReply,
    broad: CapabilityFlags,
) {
    loop {
        if io.reset_layer_sequence().is_err() {
            break;
        }
        io.reset_read_sequence(0);
        match io.peek_packet().await {
            Ok(preview) if preview.sequence_id == 0 => {}
            _ => break,
        }
        let Ok(packet) = io.read_logical(1024 * 1024).await else {
            break;
        };
        if let Ok(mut commands) = transcript.lock() {
            commands.push(packet.payload.clone());
        }
        if packet.payload.first() == Some(&0x01) {
            break;
        }

        if packet.payload == WIRE03_LOCAL_INFILE_QUERY {
            if !run_fake_compressed_local_infile(&mut io, &transcript, broad).await {
                break;
            }
            continue;
        }

        let response = if packet.payload == WIRE03_LARGE_QUERY
            || packet.payload == WIRE03_EXACT_MULTIPLE_QUERY
        {
            let Ok(mut ok) = encode_ok_packet(
                ResponseHeader::OK,
                1,
                0,
                StatusFlags::AUTOCOMMIT,
                0,
                b"",
                broad,
            ) else {
                break;
            };
            let response_len = if packet.payload == WIRE03_LARGE_QUERY {
                WIRE03_LARGE_RESPONSE_LEN
            } else {
                WIRE03_EXACT_MULTIPLE_RESPONSE_LEN
            };
            ok.resize(response_len, b'L');
            ok
        } else if packet.payload.starts_with(b"\x03SET SESSION_STATES '") {
            if auth != FakeAuth::Migration
                || matches!(snapshot_reply, SnapshotReply::RestoreDisconnect)
            {
                break;
            }
            if matches!(snapshot_reply, SnapshotReply::RestoreError) {
                match encode_error_packet(1105, Some(*b"HY000"), b"restore rejected", broad) {
                    Ok(error) => error,
                    Err(_) => break,
                }
            } else {
                match encode_ok_packet(
                    ResponseHeader::OK,
                    0,
                    0,
                    StatusFlags::AUTOCOMMIT,
                    0,
                    b"",
                    broad,
                ) {
                    Ok(ok) => ok,
                    Err(_) => break,
                }
            }
        } else {
            match encode_ok_packet(
                ResponseHeader::OK,
                1,
                0,
                StatusFlags::AUTOCOMMIT,
                0,
                b"",
                broad,
            ) {
                Ok(ok) => ok,
                Err(_) => break,
            }
        };
        io.reset_write_sequence(io.expected_read_sequence());
        if io.write_logical(&response, true).await.is_err() {
            break;
        }
    }
}

async fn run_fake_compressed_backend_connection(
    stream: TcpStream,
    transcript: Arc<Mutex<Vec<Vec<u8>>>>,
    active_algorithms: Arc<Mutex<Vec<CompressionAlgorithm>>>,
    snapshot_reply: SnapshotReply,
    algorithm: CompressionAlgorithm,
) -> u64 {
    let mut broad = fake_backend_capabilities(false);
    broad |= match algorithm {
        CompressionAlgorithm::Zlib => CapabilityFlags::COMPRESS,
        CompressionAlgorithm::Zstd { .. } => CapabilityFlags::ZSTD_COMPRESSION_ALGORITHM,
    };
    let counted = CountedIo::new(stream);
    let counters = counted.counters();
    let (read, write) = tokio::io::split(counted);
    let mut reader = PacketReader::new(read);
    let mut writer = PacketWriter::new(write);
    if !write_fake_backend_greeting(&mut writer, broad).await {
        return counters.outbound();
    }
    reader.reset_sequence(writer.next_sequence());
    let Some(auth) =
        finish_fake_backend_auth(&mut reader, &mut writer, broad, Some(algorithm)).await
    else {
        return counters.outbound();
    };

    // Authentication terminates in plaintext. Both peers activate the selected
    // codec only after the auth OK, at a clean command boundary.
    let counted = reader.into_inner().unsplit(writer.into_inner());
    let Ok(compressed) = CompressedIo::new(counted, algorithm, CompressionLimits::default()) else {
        return counters.outbound();
    };
    if let Ok(mut active) = active_algorithms.lock() {
        active.push(compressed.codec().algorithm());
    }
    run_fake_compressed_backend_commands(
        PacketIo::new(CompressedTestTransport { inner: compressed }),
        auth,
        transcript,
        snapshot_reply,
        broad,
    )
    .await;
    counters.outbound()
}

async fn run_fake_compressed_backend(
    listener: TcpListener,
    transcript: Arc<Mutex<Vec<Vec<u8>>>>,
    active_algorithms: Arc<Mutex<Vec<CompressionAlgorithm>>>,
    written_bytes: Arc<AtomicU64>,
    snapshot_reply: SnapshotReply,
    algorithm: CompressionAlgorithm,
    proxy_v2: bool,
) {
    while let Ok((stream, _)) = listener.accept().await {
        let stream = if proxy_v2 {
            let (mut read, write) = stream.into_split();
            if strip_inbound_proxy_v2(&mut read).await.is_none() {
                continue;
            }
            let Ok(stream) = read.reunite(write) else {
                continue;
            };
            stream
        } else {
            stream
        };
        let written = run_fake_compressed_backend_connection(
            stream,
            Arc::clone(&transcript),
            Arc::clone(&active_algorithms),
            snapshot_reply,
            algorithm,
        )
        .await;
        written_bytes.fetch_add(written, Ordering::Relaxed);
    }
}

struct FakeBackendConfig {
    snapshot_reply: SnapshotReply,
    proxy_v2: bool,
    proxy_headers: Option<Arc<Mutex<Vec<Vec<u8>>>>>,
    send_idle_byte: bool,
    tls_config: Option<Arc<ServerConfig>>,
    accepted_connections: Option<Arc<AtomicU64>>,
}

async fn run_fake_backend(
    listener: TcpListener,
    transcript: Arc<Mutex<Vec<Vec<u8>>>>,
    written_bytes: Arc<AtomicU64>,
    config: FakeBackendConfig,
) {
    while let Ok((stream, _)) = listener.accept().await {
        if let Some(accepted_connections) = &config.accepted_connections {
            accepted_connections.fetch_add(1, Ordering::Relaxed);
        }
        let stream = if config.proxy_v2 {
            let (mut read, write) = stream.into_split();
            let Some(header) = strip_inbound_proxy_v2(&mut read).await else {
                continue;
            };
            if let Some(proxy_headers) = &config.proxy_headers {
                let Ok(mut captured) = proxy_headers.lock() else {
                    continue;
                };
                captured.push(header);
            }
            let Ok(stream) = read.reunite(write) else {
                continue;
            };
            stream
        } else {
            stream
        };
        let written = if let Some(tls_config) = config.tls_config.clone() {
            run_fake_tls_backend_connection(
                stream,
                tls_config,
                Arc::clone(&transcript),
                config.snapshot_reply,
                config.send_idle_byte,
            )
            .await
        } else {
            let broad = fake_backend_capabilities(false);
            let counted = CountedIo::new(stream);
            let counters = counted.counters();
            let (read, write) = tokio::io::split(counted);
            let mut reader = PacketReader::new(read);
            let mut writer = PacketWriter::new(write);
            let Some(auth) = fake_backend_auth(&mut reader, &mut writer, broad).await else {
                written_bytes.fetch_add(counters.outbound(), Ordering::Relaxed);
                continue;
            };
            let _ = run_fake_backend_commands(
                reader,
                writer,
                auth,
                Arc::clone(&transcript),
                config.snapshot_reply,
                broad,
                config.send_idle_byte,
            )
            .await;
            counters.outbound()
        };
        written_bytes.fetch_add(written, Ordering::Relaxed);
    }
}
async fn spawn_fake_backend_server(
    snapshot_reply: SnapshotReply,
) -> (u16, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicU64>) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let written_bytes = Arc::new(AtomicU64::new(0));
    tokio::spawn(run_fake_backend(
        listener,
        Arc::clone(&transcript),
        Arc::clone(&written_bytes),
        FakeBackendConfig {
            snapshot_reply,
            proxy_v2: false,
            proxy_headers: None,
            send_idle_byte: false,
            tls_config: None,
            accepted_connections: None,
        },
    ));
    (address.port(), transcript, written_bytes)
}

async fn spawn_counting_fake_backend_server(
    snapshot_reply: SnapshotReply,
) -> (
    u16,
    Arc<Mutex<Vec<Vec<u8>>>>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let written_bytes = Arc::new(AtomicU64::new(0));
    let accepted_connections = Arc::new(AtomicU64::new(0));
    tokio::spawn(run_fake_backend(
        listener,
        Arc::clone(&transcript),
        Arc::clone(&written_bytes),
        FakeBackendConfig {
            snapshot_reply,
            proxy_v2: false,
            proxy_headers: None,
            send_idle_byte: false,
            tls_config: None,
            accepted_connections: Some(Arc::clone(&accepted_connections)),
        },
    ));
    (
        address.port(),
        transcript,
        written_bytes,
        accepted_connections,
    )
}

/// A migration target that accepts the private token only when the candidate
/// handshake carries the expected *current* username and attributes. This
/// makes the runtime tests kill mutations that keep reading the immutable
/// initial handshake after a successful change-user, or that overwrite the old
/// identity on a failed change-user.
async fn spawn_identity_validating_backend(
    expected_username: &[u8],
    expected_attributes: &[(Vec<u8>, Vec<u8>)],
) -> (u16, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicU64>) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let written_bytes = Arc::new(AtomicU64::new(0));
    let expected_username = expected_username.to_vec();
    let expected_attributes = expected_attributes.to_vec();
    let server_transcript = Arc::clone(&transcript);
    let server_written = Arc::clone(&written_bytes);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let broad = fake_backend_capabilities(false);
            let counted = CountedIo::new(stream);
            let counters = counted.counters();
            let (read, write) = tokio::io::split(counted);
            let mut reader = PacketReader::new(read);
            let mut writer = PacketWriter::new(write);
            if !write_fake_backend_greeting(&mut writer, broad).await {
                continue;
            }
            reader.reset_sequence(writer.next_sequence());
            let Ok(response) = reader.read_logical(64 * 1024).await else {
                continue;
            };
            let Ok(parsed) = parse_handshake_response(&response.payload) else {
                continue;
            };
            let parsed_attributes = parsed.attributes.map(|attributes| {
                attributes
                    .into_iter()
                    .filter_map(Result::ok)
                    .map(|attribute| (attribute.key.to_vec(), attribute.value.to_vec()))
                    .collect::<Vec<_>>()
            });
            let accepted = parsed.auth_plugin_name == Some(b"tidb_session_token".as_slice())
                && parsed.auth_response == b"signed-token-private"
                && parsed.database == Some(b"snapshot_db".as_slice())
                && parsed.username == expected_username
                && parsed_attributes.as_deref() == Some(expected_attributes.as_slice());
            writer.reset_sequence(reader.expected_sequence());
            if !accepted {
                let _ = write_access_denied(&mut writer, broad).await;
                server_written.fetch_add(counters.outbound(), Ordering::Relaxed);
                continue;
            }
            let Ok(ok) = encode_ok_packet(
                ResponseHeader::OK,
                0,
                0,
                StatusFlags::AUTOCOMMIT,
                0,
                b"",
                broad,
            ) else {
                continue;
            };
            if writer.write_logical(&ok, true).await.is_err() {
                continue;
            }
            let _ = run_fake_backend_commands(
                reader,
                writer,
                FakeAuth::Migration,
                Arc::clone(&server_transcript),
                SnapshotReply::Valid,
                broad,
                false,
            )
            .await;
            server_written.fetch_add(counters.outbound(), Ordering::Relaxed);
        }
    });
    (address.port(), transcript, written_bytes)
}

async fn spawn_fake_tls_backend_server(
    snapshot_reply: SnapshotReply,
) -> (u16, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicU64>) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let written_bytes = Arc::new(AtomicU64::new(0));
    tokio::spawn(run_fake_backend(
        listener,
        Arc::clone(&transcript),
        Arc::clone(&written_bytes),
        FakeBackendConfig {
            snapshot_reply,
            proxy_v2: false,
            proxy_headers: None,
            send_idle_byte: false,
            tls_config: Some(fake_backend_tls_config()),
            accepted_connections: None,
        },
    ));
    (address.port(), transcript, written_bytes)
}

async fn spawn_fake_compressed_backend_server(
    snapshot_reply: SnapshotReply,
    algorithm: CompressionAlgorithm,
    proxy_v2: bool,
) -> (
    u16,
    Arc<Mutex<Vec<Vec<u8>>>>,
    Arc<AtomicU64>,
    Arc<Mutex<Vec<CompressionAlgorithm>>>,
) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let written_bytes = Arc::new(AtomicU64::new(0));
    let active_algorithms = Arc::new(Mutex::new(Vec::new()));
    tokio::spawn(run_fake_compressed_backend(
        listener,
        Arc::clone(&transcript),
        Arc::clone(&active_algorithms),
        Arc::clone(&written_bytes),
        snapshot_reply,
        algorithm,
        proxy_v2,
    ));
    (address.port(), transcript, written_bytes, active_algorithms)
}

/// The whole stack under test.
struct Stack {
    server_handle: dataplane::DataplaneHandle,
    sender: Arc<FakeSender>,
    _state_tx: watch::Sender<ConnectionState>,
    shutdown_tx: watch::Sender<bool>,
    drain_tx: watch::Sender<Option<Duration>>,
    drain_deadline: Duration,
    forwarder: Arc<dataplane::control_dispatch::InboundForwarder>,
    sql_port: u16,
    server_task: tokio::task::JoinHandle<()>,
    dispatch_task: tokio::task::JoinHandle<Result<(), dataplane::control_dispatch::DispatchFatal>>,
    backend_port: u16,
    metrics_rx: mpsc::Receiver<Observation>,
    backend_transcript: Arc<Mutex<Vec<Vec<u8>>>>,
    backend_proxy_headers: Arc<Mutex<Vec<Vec<u8>>>>,
    backend_written_bytes: Arc<AtomicU64>,
    backend_accepted_connections: Arc<AtomicU64>,
}

async fn spawn_stack() -> Stack {
    spawn_stack_with_snapshot(SnapshotReply::Valid).await
}

async fn spawn_stack_with_snapshot(snapshot_reply: SnapshotReply) -> Stack {
    spawn_stack_full(
        snapshot_reply,
        false,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
    )
    .await
}

/// A real frontend-TLS certificate written to disk, plus the CA the test client
/// trusts. `SnapshotStore` builds the served `ServerConfig` from the PEM files at
/// apply time, so the fixture only needs to survive that call.
struct FrontendTlsFixture {
    policy: control_proto::v1::TlsPolicy,
    cert_dir: std::path::PathBuf,
}

async fn spawn_stack_full(
    snapshot_reply: SnapshotReply,
    proxy_v2: bool,
    handshake_deadline: Duration,
    backend_check_interval: Duration,
    send_idle_byte: bool,
    frontend_tls: Option<FrontendTlsFixture>,
) -> Stack {
    spawn_stack_configured(
        snapshot_reply,
        proxy_v2,
        handshake_deadline,
        backend_check_interval,
        send_idle_byte,
        frontend_tls,
        None,
        Duration::from_millis(400),
    )
    .await
}

async fn spawn_tls_stack(snapshot_reply: SnapshotReply) -> Stack {
    spawn_stack_configured(
        snapshot_reply,
        false,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
        Some(fake_backend_tls_config()),
        Duration::from_millis(400),
    )
    .await
}

/// A stack whose graceful-close force deadline is far beyond any test's
/// observation window, so a drain closes only when the session reaches a
/// genuine safe boundary — never because the deadline preempted it.
async fn spawn_stack_long_drain() -> Stack {
    spawn_stack_configured(
        SnapshotReply::Valid,
        false,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
        None,
        Duration::from_secs(30),
    )
    .await
}

async fn spawn_metered_stack(registry: MeteringSourceRegistry) -> Stack {
    spawn_stack_configured_with_metering(
        SnapshotReply::Valid,
        false,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
        None,
        Duration::from_millis(400),
        Some(registry),
        None,
    )
    .await
}

async fn spawn_configured_fake_backend(
    snapshot_reply: SnapshotReply,
    proxy_v2: bool,
    send_idle_byte: bool,
    tls_config: Option<Arc<ServerConfig>>,
) -> (
    u16,
    Arc<Mutex<Vec<Vec<u8>>>>,
    Arc<Mutex<Vec<Vec<u8>>>>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let proxy_headers = Arc::new(Mutex::new(Vec::new()));
    let written_bytes = Arc::new(AtomicU64::new(0));
    let accepted_connections = Arc::new(AtomicU64::new(0));
    tokio::spawn(run_fake_backend(
        listener,
        Arc::clone(&transcript),
        Arc::clone(&written_bytes),
        FakeBackendConfig {
            snapshot_reply,
            proxy_v2,
            proxy_headers: Some(Arc::clone(&proxy_headers)),
            send_idle_byte,
            tls_config,
            accepted_connections: Some(Arc::clone(&accepted_connections)),
        },
    ));
    (
        address.port(),
        transcript,
        proxy_headers,
        written_bytes,
        accepted_connections,
    )
}

// The test-stack constructor mirrors the executable's full composition; its
// knobs are positional test parameters, not a production API surface.
#[allow(clippy::too_many_arguments)]
async fn spawn_stack_configured(
    snapshot_reply: SnapshotReply,
    proxy_v2: bool,
    handshake_deadline: Duration,
    backend_check_interval: Duration,
    send_idle_byte: bool,
    frontend_tls: Option<FrontendTlsFixture>,
    tls_config: Option<Arc<ServerConfig>>,
    drain_deadline: Duration,
) -> Stack {
    spawn_stack_configured_with_metering(
        snapshot_reply,
        proxy_v2,
        handshake_deadline,
        backend_check_interval,
        send_idle_byte,
        frontend_tls,
        tls_config,
        drain_deadline,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn spawn_stack_configured_with_metering(
    snapshot_reply: SnapshotReply,
    proxy_v2: bool,
    handshake_deadline: Duration,
    backend_check_interval: Duration,
    send_idle_byte: bool,
    frontend_tls: Option<FrontendTlsFixture>,
    tls_config: Option<Arc<ServerConfig>>,
    drain_deadline: Duration,
    metering: Option<MeteringSourceRegistry>,
    route_plane: Option<control_router::RoutePlaneHandle>,
) -> Stack {
    let backend =
        spawn_configured_fake_backend(snapshot_reply, proxy_v2, send_idle_byte, tls_config.clone())
            .await;

    // Dispatch loop with an observable sender and a driven state watch.
    let sender = FakeSender::new();
    let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);
    let (snapshot_tx, _snapshot_rx) = mpsc::channel(4);
    let (handle, forwarder, dispatch_task) = spawn_control_dispatch_parts(
        ControlCommandHandler::new(),
        Arc::clone(&sender),
        state_rx,
        snapshot_tx,
        Duration::from_millis(20),
    );
    // The dispatch loop processes inbound frames only while a session is
    // live; publish the matching `Connected` (same lineage as the
    // forwarder resume below) so routed control answers are not deferred.
    state_tx
        .send(ConnectionState::Connected {
            epoch: 1,
            serial: 1,
            capabilities: 0,
            peer_process_id: Arc::from("go-fixture"),
            peer_started_unix_millis: 1_700_000_000_000,
        })
        .ok();
    let Ok(()) = forwarder
        .resume_session(SessionMeta {
            serial: 1,
            epoch: 1,
            peer_process_id: Arc::from("go-fixture"),
            peer_started_unix_millis: 1_700_000_000_000,
        })
        .await
    else {
        unreachable!("the first session's resume sets the frame origin")
    };

    // The engine owner over a real (never-connecting) control client.
    let client = control_client();
    let (metrics, metrics_rx) = MetricsRecorder::channel(128);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (drain_tx, drain_rx) = watch::channel(None::<Duration>);
    let owner = EngineSessionOwner::new(
        Arc::clone(&client),
        "default",
        shutdown_rx,
        drain_rx,
        SessionLoopConfig {
            handshake_deadline,
            drain_deadline,
            backend_check_interval,
            cleanup_deadline: Duration::from_secs(2),
        },
    )
    .with_metrics(metrics);
    let owner = match metering {
        Some(registry) => owner.with_metering(registry),
        None => owner,
    };
    // Production attaches the process-local route plane here; without it the
    // engine falls back to the compatibility bridge acquisition path.
    let owner = match route_plane {
        Some(handle) => owner.with_route_plane(handle),
        None => owner,
    };
    let owner: Arc<dyn BoundSessionHandler> = Arc::new(owner);
    let (connection_handler, installer) = DispatchConnectionHandler::new("default", owner);
    assert!(installer.install(handle));

    // Real SQL listener.
    let Ok(sql_listener) = std::net::TcpListener::bind(("127.0.0.1", 0)) else {
        unreachable!("sql bind probe")
    };
    let Ok(sql_addr) = sql_listener.local_addr() else {
        unreachable!("sql addr")
    };
    drop(sql_listener);
    let snapshot = engine_snapshot(
        sql_addr.port(),
        backend.0,
        proxy_v2,
        frontend_tls.as_ref(),
        tls_config.is_some(),
    );
    let Ok(server) = DataplaneServer::bind(snapshot, Arc::new(SystemMemoryProbe::new())).await
    else {
        unreachable!("dataplane bind")
    };
    // Mirror the executable's composition: forced shutdown holds the
    // abort backstop past the session owners' cleanup bound.
    let server = server.with_force_join_grace(Duration::from_secs(3));
    let server_handle = server.handle();
    let server_task = tokio::spawn(async move {
        let _ = server.run(connection_handler).await;
    });

    Stack {
        server_handle,
        sender,
        _state_tx: state_tx,
        shutdown_tx,
        drain_tx,
        drain_deadline,
        forwarder: Arc::new(forwarder),
        sql_port: sql_addr.port(),
        server_task,
        dispatch_task,
        backend_port: backend.0,
        metrics_rx,
        backend_transcript: backend.1,
        backend_proxy_headers: backend.2,
        backend_written_bytes: backend.3,
        backend_accepted_connections: backend.4,
    }
}

fn engine_snapshot(
    port: u16,
    backend_port: u16,
    proxy_v2: bool,
    frontend_tls: Option<&FrontendTlsFixture>,
    require_backend_tls: bool,
) -> Arc<control_proto::snapshot::ValidatedSnapshot> {
    use control_proto::v1::{
        BackendSnapshot, ConfigSnapshot, KeepalivePolicy, Listener, ProxyProtocolMode,
        StateSnapshot, TlsPolicy,
    };
    let proxy_protocol = if proxy_v2 {
        ProxyProtocolMode::V2
    } else {
        ProxyProtocolMode::Disabled
    };
    // Without a fixture the frontend policy is empty (no cert -> SSL is not
    // advertised); a fixture supplies a real cert/key on disk that the store
    // compiles into a served `ServerConfig`.
    let (frontend_policy, store_dirs) = frontend_tls.map_or_else(
        || (TlsPolicy::default(), Vec::new()),
        |fixture| (fixture.policy.clone(), vec![fixture.cert_dir.clone()]),
    );
    let keepalive = KeepalivePolicy {
        enabled: true,
        idle_millis: 60_000,
        probe_count: 5,
        interval_millis: 3_000,
        user_timeout_millis: 15_000,
    };
    let unhealthy_keepalive = KeepalivePolicy {
        enabled: false,
        ..KeepalivePolicy::default()
    };
    let raw = StateSnapshot {
        config: Some(ConfigSnapshot {
            high_memory_reject_threshold: 0.9,
            connection_buffer_bytes: 32 * 1024,
            frontend_keepalive: Some(keepalive),
            healthy_backend_keepalive: Some(keepalive),
            unhealthy_backend_keepalive: Some(unhealthy_keepalive),
            proxy_protocol: proxy_protocol as i32,
            listeners: vec![Listener {
                address: "127.0.0.1".to_owned(),
                port: u32::from(port),
                name: "sql-0".to_owned(),
            }],
            server_version: "TiProxy-test".to_owned(),
            frontend_tls: Some(frontend_policy),
            backend_tls: Some(TlsPolicy {
                skip_ca_verification: require_backend_tls,
                ..TlsPolicy::default()
            }),
            require_backend_tls,
            ..ConfigSnapshot::default()
        }),
        backends: vec![BackendSnapshot {
            backend_id: "tidb-fake".to_owned(),
            address: format!("127.0.0.1:{backend_port}"),
            cluster_name: String::new(),
            healthy: true,
            local: true,
            ..BackendSnapshot::default()
        }],
        ..StateSnapshot::default()
    };
    let Ok(store) = control_proto::snapshot::SnapshotStore::new(store_dirs) else {
        unreachable!("store")
    };
    let Ok(outcome) = store.apply(
        1,
        raw,
        control_proto::snapshot::UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
        control_proto::snapshot::SnapshotLineage::for_tests("go-fixture"),
    ) else {
        unreachable!("snapshot applies")
    };
    outcome.snapshot
}

fn engine_health_snapshot(
    sql_port: u16,
    backend_port: u16,
    generation: u64,
    healthy: bool,
) -> Arc<control_proto::snapshot::ValidatedSnapshot> {
    let base = engine_snapshot(sql_port, backend_port, false, None, false);
    let mut raw = base.raw().clone();
    raw.backends[0].healthy = healthy;
    let Ok(store) = control_proto::snapshot::SnapshotStore::new([]) else {
        unreachable!("store")
    };
    let Ok(outcome) = store.apply(
        generation,
        raw,
        control_proto::snapshot::UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
        control_proto::snapshot::SnapshotLineage::for_tests("go-fixture"),
    ) else {
        unreachable!("health snapshot applies")
    };
    outcome.snapshot
}

fn engine_redirect_health_snapshot(
    sql_port: u16,
    current_port: u16,
    target_port: u16,
    generation: u64,
) -> Arc<control_proto::snapshot::ValidatedSnapshot> {
    let base = engine_health_snapshot(sql_port, current_port, generation, true);
    let mut raw = base.raw().clone();
    raw.backends.push(control_proto::v1::BackendSnapshot {
        backend_id: "tidb-other".to_owned(),
        address: format!("127.0.0.1:{target_port}"),
        cluster_name: String::new(),
        healthy: false,
        local: true,
        ..control_proto::v1::BackendSnapshot::default()
    });
    let Ok(store) = control_proto::snapshot::SnapshotStore::new([]) else {
        unreachable!("store")
    };
    let Ok(outcome) = store.apply(
        generation,
        raw,
        control_proto::snapshot::UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
        control_proto::snapshot::SnapshotLineage::for_tests("go-fixture"),
    ) else {
        unreachable!("redirect health snapshot applies")
    };
    outcome.snapshot
}

/// A raw `MySQL` client: performs the handshake and returns the packet
/// reader/writer for command exchanges.
struct MysqlClient {
    reader: PacketReader<tokio::net::tcp::OwnedReadHalf>,
    writer: PacketWriter<tokio::net::tcp::OwnedWriteHalf>,
    capabilities: CapabilityFlags,
}

impl MysqlClient {
    async fn connect(port: u16) -> Option<Self> {
        Self::login(port, FAKE_BACKEND_PASSWORD).await.ok()
    }

    async fn connect_with_proxy_header(port: u16, header: &[u8]) -> Option<Self> {
        Self::login_with_proxy_header(port, FAKE_BACKEND_PASSWORD, Some(header))
            .await
            .ok()
    }

    /// Full connection-phase flow with a real challenge/response; the
    /// error branch returns the terminal non-OK payload.
    async fn login(port: u16, password: &str) -> Result<Self, Vec<u8>> {
        Self::login_with_proxy_header(port, password, None).await
    }

    async fn login_with_proxy_header(
        port: u16,
        password: &str,
        proxy_header: Option<&[u8]>,
    ) -> Result<Self, Vec<u8>> {
        let stream = Self::open_stream(port, proxy_header).await?;
        let (read, write) = stream.into_split();
        let mut reader = PacketReader::new(read);
        let mut writer = PacketWriter::new(write);
        // Strict wire sequences throughout the connection phase: the
        // greeting opens at zero and every later packet continues its
        // exchange in lockstep.
        let Ok(preview) = reader.peek_packet().await else {
            return Err(Vec::new());
        };
        assert_eq!(preview.sequence_id, 0);
        let Ok(greeting) = reader.read_logical(64 * 1024).await else {
            return Err(Vec::new());
        };
        let Ok(parsed) = parse_initial_handshake(&greeting.payload) else {
            return Err(Vec::new());
        };
        let mut proxy_salt = parsed.auth_plugin_data_part_1.to_vec();
        proxy_salt.extend_from_slice(parsed.auth_plugin_data_part_2);
        let capabilities = CapabilityFlags::PROTOCOL_41
            | CapabilityFlags::LONG_PASSWORD
            | CapabilityFlags::SECURE_CONNECTION
            | CapabilityFlags::PLUGIN_AUTH
            | CapabilityFlags::CONNECT_ATTRS
            | CapabilityFlags::DEPRECATE_EOF;
        let initial_attributes = [Attribute {
            key: b"program_name",
            value: b"initial-client",
        }];
        // The first scramble answers the proxy's greeting salt.
        let first_scramble = native_scramble(password, &proxy_salt);
        let Ok(response) = encode_handshake_response(HandshakeResponseParams {
            capabilities,
            max_packet_size: 16 * 1024 * 1024,
            collation: 45,
            username: b"root",
            auth_response: &first_scramble,
            database: None,
            auth_plugin_name: Some(b"mysql_native_password"),
            attributes: Some(&initial_attributes),
            zstd_level: None,
        }) else {
            return Err(Vec::new());
        };
        writer.reset_sequence(reader.expected_sequence());
        if writer.write_logical(&response, true).await.is_err() {
            return Err(Vec::new());
        }
        let expected_switch_sequence = writer.next_sequence();
        reader.reset_sequence(expected_switch_sequence);
        let Ok(preview) = reader.peek_packet().await else {
            return Err(Vec::new());
        };
        assert_eq!(preview.sequence_id, expected_switch_sequence);
        let Ok(switch) = reader.read_logical(64 * 1024).await else {
            return Err(Vec::new());
        };
        // The backend always re-requests authentication: the proxy's
        // unknown-plugin rewrite makes it challenge with its own salt.
        if switch.payload.first() != Some(&0xFE) {
            return Err(switch.payload);
        }
        let data = &switch.payload[1..];
        let Some(nul) = data.iter().position(|&byte| byte == 0) else {
            return Err(switch.payload.clone());
        };
        assert_eq!(&data[..nul], b"mysql_native_password");
        let mut backend_salt = &data[nul + 1..];
        if backend_salt.last() == Some(&0) {
            backend_salt = &backend_salt[..backend_salt.len() - 1];
        }
        assert_ne!(
            backend_salt,
            proxy_salt.as_slice(),
            "re-authentication challenges with the backend's own salt"
        );
        writer.reset_sequence(reader.expected_sequence());
        if writer
            .write_logical(&native_scramble(password, backend_salt), true)
            .await
            .is_err()
        {
            return Err(Vec::new());
        }
        let expected_result_sequence = writer.next_sequence();
        reader.reset_sequence(expected_result_sequence);
        let Ok(preview) = reader.peek_packet().await else {
            return Err(Vec::new());
        };
        assert_eq!(preview.sequence_id, expected_result_sequence);
        let Ok(outcome) = reader.read_logical(64 * 1024).await else {
            return Err(Vec::new());
        };
        if outcome.payload.first() != Some(&0x00) {
            return Err(outcome.payload);
        }
        Ok(Self {
            reader,
            writer,
            capabilities,
        })
    }

    async fn open_stream(port: u16, proxy_header: Option<&[u8]>) -> Result<TcpStream, Vec<u8>> {
        let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)).await else {
            return Err(Vec::new());
        };
        if let Some(proxy_header) = proxy_header
            && (stream.write_all(proxy_header).await.is_err() || stream.flush().await.is_err())
        {
            return Err(Vec::new());
        }
        Ok(stream)
    }

    async fn query_ok(&mut self, sql: &str) -> bool {
        let mut payload = vec![0x03_u8];
        payload.extend_from_slice(sql.as_bytes());
        self.writer.reset_sequence(0);
        if self.writer.write_logical(&payload, true).await.is_err() {
            return false;
        }
        self.reader.reset_sequence(1);
        // Strict wire sequence: the response to a fresh command must
        // answer at one, whatever earlier exchanges left behind.
        match self.reader.peek_packet().await {
            Ok(preview) => assert_eq!(preview.sequence_id, 1),
            Err(_) => return false,
        }
        let Ok(response) = self.reader.read_logical(64 * 1024).await else {
            return false;
        };
        let _ = self.capabilities;
        response.payload.first() == Some(&0x00)
    }

    /// RSP-004: sends one command and drains a two-result-set response,
    /// returning the text rows in arrival order. Each set is column-count,
    /// column definition, one row, terminator; the first terminator carries
    /// `SERVER_MORE_RESULTS_EXISTS`. Returning the rows rather than a count
    /// makes a proxy that forwarded only the first set distinguishable from
    /// one that forwarded both.
    async fn query_multi_result(&mut self, sql: &str) -> Option<Vec<u8>> {
        let mut payload = vec![0x03_u8];
        payload.extend_from_slice(sql.as_bytes());
        self.writer.reset_sequence(0);
        self.writer.write_logical(&payload, true).await.ok()?;
        self.reader.reset_sequence(1);
        let mut rows = Vec::new();
        for _ in 0..8 {
            let packet = self.reader.read_logical(64 * 1024).await.ok()?;
            // A text row here is a one-byte length-encoded string; the
            // column-count packet is the bare 0x01 and is one byte shorter.
            if packet.payload.len() == 2 && packet.payload[0] == 0x01 {
                rows.push(packet.payload[1]);
            }
        }
        Some(rows)
    }

    /// Sends one command and reads a large OK-shaped logical response that
    /// crosses `MySQL`'s 16 MiB physical-packet boundary. Returns
    /// `(physical_packets, sequence_mismatches, next_expected_sequence)` so
    /// PKT-001/002 can assert exact physical framing: the split/terminator
    /// packet count, that every physical packet advanced the 8-bit sequence
    /// exactly once (zero mismatches, from the reader's own `ForwardProgress`
    /// rather than a tolerant resync), and the post-read sequence. A bounded
    /// timeout turns a missing zero-length terminator into a fast failure
    /// instead of hanging until the CI job times out.
    async fn query_large_ok(&mut self, sql: &str, expected_len: usize) -> Option<(u64, u64, u8)> {
        let mut payload = vec![0x03_u8];
        payload.extend_from_slice(sql.as_bytes());
        self.writer.reset_sequence(0);
        self.writer.write_logical(&payload, true).await.ok()?;
        self.reader.reset_sequence(1);
        let response = timeout(
            Duration::from_secs(10),
            self.reader.read_logical(expected_len + 64),
        )
        .await
        .ok()?
        .ok()?;
        if response.payload.len() != expected_len
            || response.payload.first() != Some(&0x00)
            || response.payload.last() != Some(&b'L')
        {
            return None;
        }
        Some((
            response.progress.physical_packets(),
            response.progress.sequence_mismatches(),
            self.reader.expected_sequence(),
        ))
    }

    /// Runs one strict multi-round `COM_CHANGE_USER` exchange. Every
    /// non-terminal backend packet receives one client response; the first
    /// auth-switch response is computed from the backend's fresh salt and the
    /// second is an opaque sentinel understood only by the fake backend.
    async fn change_user(
        &mut self,
        username: &[u8],
        database: &[u8],
        original_auth_response: &[u8],
        attributes: &[Attribute<'_>],
    ) -> Result<(), u16> {
        let request = encode_change_user(
            ChangeUserParams {
                username,
                auth_response: original_auth_response,
                database,
                character_set: Some(45),
                auth_plugin_name: Some(b"mysql_native_password"),
                attributes: Some(attributes),
            },
            self.capabilities,
        )
        .map_err(|_| u16::MAX)?;
        self.writer.reset_sequence(0);
        self.writer
            .write_logical(&request, true)
            .await
            .map_err(|_| u16::MAX)?;
        self.reader.reset_sequence(1);

        loop {
            let expected = self.reader.expected_sequence();
            let preview = self.reader.peek_packet().await.map_err(|_| u16::MAX)?;
            assert_eq!(preview.sequence_id, expected);
            let response = self
                .reader
                .read_logical(64 * 1024)
                .await
                .map_err(|_| u16::MAX)?;
            match response.payload.first() {
                Some(0x00) => return Ok(()),
                Some(0xFF) if response.payload.len() >= 3 => {
                    return Err(u16::from_le_bytes([
                        response.payload[1],
                        response.payload[2],
                    ]));
                }
                Some(0xFE) => {
                    let data = &response.payload[1..];
                    let nul = data.iter().position(|&byte| byte == 0).ok_or(u16::MAX)?;
                    assert_eq!(&data[..nul], b"mysql_native_password");
                    let mut salt = &data[nul + 1..];
                    if salt.last() == Some(&0) {
                        salt = &salt[..salt.len() - 1];
                    }
                    self.writer.reset_sequence(self.reader.expected_sequence());
                    self.writer
                        .write_logical(&native_scramble(FAKE_BACKEND_PASSWORD, salt), true)
                        .await
                        .map_err(|_| u16::MAX)?;
                }
                _ => {
                    self.writer.reset_sequence(self.reader.expected_sequence());
                    self.writer
                        .write_logical(b"second-auth-response", true)
                        .await
                        .map_err(|_| u16::MAX)?;
                }
            }
            self.reader.reset_sequence(self.writer.next_sequence());
        }
    }

    /// `COM_STMT_SEND_LONG_DATA`: fire-and-forget, no response.
    async fn send_long_data(&mut self, statement_id: u32) -> bool {
        let mut payload = vec![0x18_u8];
        payload.extend_from_slice(&statement_id.to_le_bytes());
        payload.extend_from_slice(&0_u16.to_le_bytes());
        payload.extend_from_slice(b"chunk");
        self.writer.reset_sequence(0);
        self.writer.write_logical(&payload, true).await.is_ok()
    }

    /// `COM_STMT_RESET`: clears the statement's long-data guard.
    async fn stmt_reset_ok(&mut self, statement_id: u32) -> bool {
        let mut payload = vec![0x1A_u8];
        payload.extend_from_slice(&statement_id.to_le_bytes());
        self.writer.reset_sequence(0);
        if self.writer.write_logical(&payload, true).await.is_err() {
            return false;
        }
        self.reader.reset_sequence(1);
        match self.reader.peek_packet().await {
            Ok(preview) => assert_eq!(preview.sequence_id, 1),
            Err(_) => return false,
        }
        let Ok(response) = self.reader.read_logical(64 * 1024).await else {
            return false;
        };
        response.payload.first() == Some(&0x00)
    }

    /// `COM_STMT_EXECUTE`: sends an execute for `statement_id` with the given
    /// flags byte (`0x01` opens a read-only cursor; `0x80` is the harness
    /// error sentinel) and drains the result set to its terminator.
    async fn stmt_execute(&mut self, statement_id: u32, flags: u8) -> Option<StmtResult> {
        let mut payload = vec![0x17_u8];
        payload.extend_from_slice(&statement_id.to_le_bytes());
        payload.push(flags);
        payload.extend_from_slice(&1_u32.to_le_bytes());
        self.writer.reset_sequence(0);
        self.writer.write_logical(&payload, true).await.ok()?;
        self.reader.reset_sequence(1);
        self.drain_stmt_response().await
    }

    /// `COM_STMT_FETCH`: fetches from `statement_id`'s cursor and drains to the
    /// terminator.
    async fn stmt_fetch(&mut self, statement_id: u32) -> Option<StmtResult> {
        let mut payload = vec![0x1c_u8];
        payload.extend_from_slice(&statement_id.to_le_bytes());
        payload.extend_from_slice(&1_u32.to_le_bytes());
        self.writer.reset_sequence(0);
        self.writer.write_logical(&payload, true).await.ok()?;
        self.reader.reset_sequence(1);
        self.drain_stmt_response().await
    }

    /// `COM_STMT_CLOSE`: fire-and-forget, no response; clears the statement's
    /// guards by removing it from the registry.
    async fn stmt_close(&mut self, statement_id: u32) -> bool {
        let mut payload = vec![0x19_u8];
        payload.extend_from_slice(&statement_id.to_le_bytes());
        self.writer.reset_sequence(0);
        self.writer.write_logical(&payload, true).await.is_ok()
    }

    /// `COM_RESET_CONNECTION`: a generic OK/ERR command that clears ALL prepared
    /// state on success. Returns whether the backend answered OK.
    async fn reset_connection_ok(&mut self) -> bool {
        self.writer.reset_sequence(0);
        if self.writer.write_logical(&[0x1f], true).await.is_err() {
            return false;
        }
        self.reader.reset_sequence(1);
        match self.reader.peek_packet().await {
            Ok(preview) => assert_eq!(preview.sequence_id, 1),
            Err(_) => return false,
        }
        let Ok(response) = self.reader.read_logical(64 * 1024).await else {
            return false;
        };
        response.payload.first() == Some(&0x00)
    }

    /// Reads a binary result set to its terminator: the `0xFE` resultset-OK
    /// (success) or a leading `0xFF` error; headers, column definitions, and
    /// rows are streamed past.
    async fn drain_stmt_response(&mut self) -> Option<StmtResult> {
        loop {
            let packet = self.reader.read_logical(64 * 1024).await.ok()?;
            match packet.payload.first() {
                Some(&0xFF) => {
                    let code =
                        u16::from_le_bytes([*packet.payload.get(1)?, *packet.payload.get(2)?]);
                    return Some(StmtResult::Error { code });
                }
                Some(&0xFE) if packet.payload.len() >= 7 => return Some(StmtResult::Ok),
                _ => {}
            }
        }
    }

    /// `COM_STMT_PREPARE`: sends the prepare and reads the whole special
    /// response — the prepare-OK header, then the parameter and column
    /// definitions with no classic EOF between the groups under the
    /// negotiated `DEPRECATE_EOF` — or a single leading backend error.
    async fn stmt_prepare(&mut self, sql: &str) -> Option<PrepareOutcome> {
        let mut payload = vec![0x16_u8];
        payload.extend_from_slice(sql.as_bytes());
        self.writer.reset_sequence(0);
        if self.writer.write_logical(&payload, true).await.is_err() {
            return None;
        }
        self.reader.reset_sequence(1);
        match self.reader.peek_packet().await {
            Ok(preview) => assert_eq!(preview.sequence_id, 1),
            Err(_) => return None,
        }
        let header = self.reader.read_logical(64 * 1024).await.ok()?;
        match header.payload.first() {
            Some(&0xFF) => {
                let code = u16::from_le_bytes([*header.payload.get(1)?, *header.payload.get(2)?]);
                Some(PrepareOutcome::Error { code })
            }
            Some(&0x00) => {
                let statement_id = u32::from_le_bytes([
                    *header.payload.get(1)?,
                    *header.payload.get(2)?,
                    *header.payload.get(3)?,
                    *header.payload.get(4)?,
                ]);
                let columns =
                    u16::from_le_bytes([*header.payload.get(5)?, *header.payload.get(6)?]);
                let parameters =
                    u16::from_le_bytes([*header.payload.get(7)?, *header.payload.get(8)?]);
                // Drain the parameter then column definitions (no EOFs).
                for _ in 0..(u32::from(parameters) + u32::from(columns)) {
                    self.reader.read_logical(64 * 1024).await.ok()?;
                }
                Some(PrepareOutcome::Ok {
                    statement_id,
                    parameters,
                    columns,
                })
            }
            _ => None,
        }
    }

    async fn quit(mut self) {
        self.writer.reset_sequence(0);
        let _ = self.writer.write_logical(&[0x01], true).await;
    }
}

/// One `COM_STMT_PREPARE` outcome observed on the client wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrepareOutcome {
    /// Prepare-OK with the returned statement ID and metadata counts.
    Ok {
        statement_id: u32,
        parameters: u16,
        columns: u16,
    },
    /// A leading backend error with its code.
    Error { code: u16 },
}

/// One `COM_STMT_EXECUTE` / `COM_STMT_FETCH` outcome observed on the client wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StmtResult {
    /// The result set (or empty result) reached its terminator.
    Ok,
    /// A leading backend error with its code.
    Error { code: u16 },
}

/// Keeps injecting the route assignment until the engine's armed
/// expectation consumes it (the engine's request ids are deterministic:
/// HandshakeResponseEvent=1, RouteRequest=2 on a fresh client, +2 per
/// later session on the same client).
fn spawn_route_answer(stack: &Stack, connection_id: u64, request_id: u64) {
    spawn_route_answer_with_keyspace(stack, connection_id, request_id, "");
}

fn spawn_route_answer_to(stack: &Stack, connection_id: u64, request_id: u64, backend_port: u16) {
    spawn_route_answer_with_keyspace_to(stack, connection_id, request_id, "", backend_port);
}

fn spawn_route_answer_with_keyspace(
    stack: &Stack,
    connection_id: u64,
    request_id: u64,
    keyspace: &str,
) {
    spawn_route_answer_with_keyspace_to(
        stack,
        connection_id,
        request_id,
        keyspace,
        stack.backend_port,
    );
}

fn spawn_route_answer_with_keyspace_to(
    stack: &Stack,
    connection_id: u64,
    request_id: u64,
    keyspace: &str,
    backend_port: u16,
) {
    let forwarder = Arc::clone(&stack.forwarder);
    tokio::spawn(async move {
        // The production adapter always answers the handshake event
        // with a correlated accept decision; the engine consumes it
        // before requesting a route.
        let decision = ControlEnvelope {
            request_id: request_id - 1,
            generation: 1,
            body: Some(Body::HandshakeDecision(HandshakeDecision {
                connection_id,
                accept: true,
                retry: false,
                code: ErrorCode::Ok as i32,
                client_message: String::new(),
                namespace: "default".to_owned(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(decision.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let forwarder = Arc::clone(&stack.forwarder);
    let keyspace = keyspace.to_owned();
    tokio::spawn(async move {
        let assignment = ControlEnvelope {
            request_id,
            generation: 1,
            body: Some(Body::RouteAssignment(RouteAssignment {
                connection_id,
                assignment_id: format!("a-{connection_id}"),
                backend_id: "tidb-fake".to_owned(),
                backend_address: format!("127.0.0.1:{backend_port}"),
                cluster_name: String::new(),
                keyspace,
                healthy: true,
                local: true,
                code: ErrorCode::Ok as i32,
                detail: String::new(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(assignment.clone()).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
}

fn command_envelope(request_id: u64, body: Body) -> ControlEnvelope {
    ControlEnvelope {
        request_id,
        generation: 1,
        body: Some(body),
        ..ControlEnvelope::default()
    }
}

async fn wait_sent<F: Fn(&ControlEnvelope) -> bool>(
    sender: &Arc<FakeSender>,
    predicate: F,
) -> Option<ControlEnvelope> {
    for _ in 0..500 {
        if let Some(envelope) = sender.sent().into_iter().find(|e| predicate(e)) {
            return Some(envelope);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

async fn assert_closed_backend_bytes_include_retired(
    stack: &Stack,
    target_written_bytes: &AtomicU64,
) {
    let closed = wait_sent(
        &stack.sender,
        |envelope| matches!(&envelope.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(ControlEnvelope {
        body: Some(Body::ConnectionEvent(closed)),
        ..
    }) = closed
    else {
        unreachable!("successful migration emits CLOSED totals")
    };
    for _ in 0..100 {
        if stack.backend_written_bytes.load(Ordering::Relaxed) > 0
            && target_written_bytes.load(Ordering::Relaxed) > 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let old_written = stack.backend_written_bytes.load(Ordering::Relaxed);
    let target_written = target_written_bytes.load(Ordering::Relaxed);
    assert!(old_written > 0 && target_written > 0);
    assert_eq!(
        closed.backend_in_bytes,
        old_written.saturating_add(target_written),
        "CLOSED totals retain the retired owner and add the new owner exactly once"
    );
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// The full path: admission → registration → routed dial → auth relay
/// → command OK → quit, over real sockets end to end.
#[tokio::test]
async fn select_one_roundtrip_end_to_end() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("handshake+auth completes end to end")
    };
    assert!(client.query_ok("SELECT 1").await, "query round-trips");
    assert!(
        client.query_ok("SELECT 2").await,
        "second command reuses the session"
    );
    client.quit().await;
    // The CLOSED lifecycle event carries real traffic totals.
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("session close emits the CLOSED event")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    // All four raw-socket byte directions register real wire traffic (WIRE-MTR:
    // these come from the innermost `CountedIo`, not the framing layer).
    assert!(event.client_in_bytes > 0, "totals captured: {event:?}");
    assert!(event.client_out_bytes > 0, "totals captured: {event:?}");
    assert!(event.backend_in_bytes > 0, "totals captured: {event:?}");
    assert!(event.backend_out_bytes > 0, "totals captured: {event:?}");
    stack.dispatch_task.abort();
}

/// Runs one full `SELECT 1` session and returns the raw backend out-byte total
/// from the CLOSED lifecycle event, with or without the outbound PROXY v2 header.
async fn session_backend_out_bytes(proxy_v2: bool) -> u64 {
    let stack = spawn_stack_full(
        SnapshotReply::Valid,
        proxy_v2,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
    )
    .await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("handshake+auth completes end to end")
    };
    assert!(client.query_ok("SELECT 1").await, "query round-trips");
    client.quit().await;
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("session close emits the CLOSED event")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    stack.dispatch_task.abort();
    stack.server_task.abort();
    event.backend_out_bytes
}

/// WIRE-MTR: the outbound PROXY v2 header the proxy writes to the backend is
/// counted exactly once at the raw layer. The two sessions are byte-identical on
/// the wire except for that header (a salt only changes scramble *content*, not
/// length), so the raw backend out-byte delta is precisely the 28-byte IPv4
/// PROXY v2 header — neither zero (missed) nor doubled.
#[tokio::test]
async fn outbound_proxy_v2_header_counts_once_in_raw_backend_out() {
    // 12-byte magic + 4-byte fixed header + 12-byte IPv4 address/port block.
    const PROXY_V2_IPV4_HEADER_LEN: u64 = 28;
    let plain = session_backend_out_bytes(false).await;
    let proxied = session_backend_out_bytes(true).await;
    assert_eq!(
        proxied.saturating_sub(plain),
        PROXY_V2_IPV4_HEADER_LEN,
        "raw backend out must grow by exactly the PROXY header (plain={plain}, proxied={proxied})"
    );
}

/// WIRE-05 / PP2-002: when the client supplied a complete inbound header, the
/// backend hop must receive that same family/address/TLV image with only the
/// command nibble normalized to `PROXY`, matching Go. This real-socket row also
/// proves the header is the backend's first frame: the fake backend strips and
/// captures it before it can emit the `MySQL` greeting.
#[tokio::test]
async fn inbound_proxy_v2_header_and_tlvs_are_forwarded_verbatim() {
    use proxy_io::proxy_protocol::{
        EncodeAddresses, MAGIC_V2, ProxyCommand, ProxyVersion, TransportProtocol, encode_proxy_v2,
    };
    use std::net::{IpAddr, Ipv6Addr};

    let stack = spawn_stack_full(
        SnapshotReply::Valid,
        true,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
    )
    .await;
    let target = spawn_configured_fake_backend(SnapshotReply::Valid, true, false, None).await;
    let target_port = target.0;
    let target_proxy_headers = target.2;
    spawn_route_answer(&stack, 1, 2);

    let Ok(src_ip) = "2001:db8::7133".parse::<Ipv6Addr>() else {
        unreachable!("static IPv6 source parses")
    };
    let Ok(dst_ip) = "2001:db8::4000".parse::<Ipv6Addr>() else {
        unreachable!("static IPv6 destination parses")
    };
    let Ok(inbound) = encode_proxy_v2(
        ProxyVersion::V2,
        ProxyCommand::LOCAL,
        TransportProtocol::STREAM,
        EncodeAddresses::Ip {
            src: (IpAddr::V6(src_ip), 31_234),
            dst: (IpAddr::V6(dst_ip), 4_000),
        },
        &[(0x01, b"ordered-first"), (0xee, b"opaque-second")],
    ) else {
        unreachable!("bounded static PROXY header encodes")
    };
    let mut expected = inbound.clone();
    expected[MAGIC_V2.len()] = (expected[MAGIC_V2.len()] & 0xf0) | ProxyCommand::PROXY.as_nibble();

    let Some(mut client) = timeout(
        Duration::from_secs(5),
        MysqlClient::connect_with_proxy_header(stack.sql_port, &inbound),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("inbound LOCAL+IPv6+TLV session establishes")
    };
    assert!(client.query_ok("SELECT proxy_v2_tlv").await);

    let captured = stack
        .backend_proxy_headers
        .lock()
        .map_or_else(|_| Vec::new(), |headers| headers.clone());
    assert_eq!(captured, vec![expected.clone()]);
    assert_eq!(captured[0].len(), inbound.len());
    assert_eq!(&captured[0][..MAGIC_V2.len()], &inbound[..MAGIC_V2.len()]);
    assert_eq!(
        &captured[0][MAGIC_V2.len() + 1..],
        &inbound[MAGIC_V2.len() + 1..],
        "family, transport, IPv6 address body and ordered TLVs are verbatim"
    );

    let redirect = command_envelope(
        7133,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "proxy-v2-verbatim-redirect".to_owned(),
            backend_id: "tidb-proxy-v2-target".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(
            &envelope.body,
            Some(Body::RedirectResult(result))
                if result.redirect_id == "proxy-v2-verbatim-redirect"
        )
    })
    .await;
    assert!(matches!(
        result.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result))
            if result.succeeded && result.backend_id == "tidb-proxy-v2-target"
    ));
    assert!(client.query_ok("SELECT proxy_v2_tlv_after_redirect").await);
    assert_eq!(
        target_proxy_headers
            .lock()
            .map_or_else(|_| Vec::new(), |headers| headers.clone()),
        vec![expected],
        "redirect candidate receives the same normalized inbound header"
    );

    client.quit().await;
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

/// The real session path emits payload-free metrics with the legacy command
/// and quit-source labels. This is the bridge between the DPL-04 engine and
/// the DPL-05 exporter, not only an aggregator unit test.
#[tokio::test]
async fn session_path_emits_query_traffic_and_exact_quit_source() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);
    client.quit().await;

    let mut saw_handshake = false;
    let mut saw_query = false;
    let mut saw_close = false;
    let received = timeout(Duration::from_secs(5), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            match observation {
                Observation::HandshakeCompleted {
                    backend, traffic, ..
                } => {
                    assert!(backend.ends_with(&stack.backend_port.to_string()));
                    assert!(traffic.inbound_bytes > 0);
                    assert!(traffic.outbound_bytes > 0);
                    saw_handshake = true;
                }
                // opt#23: completions arrive as the session's batch, flushed
                // when the session's recorder drops (possibly after the
                // close is recorded).
                Observation::CommandBatch(batch) if batch.command().name() == "Query" => {
                    assert!(batch.backend().ends_with(&stack.backend_port.to_string()));
                    assert_eq!(batch.count(), 1);
                    assert!(batch.traffic().inbound_bytes > 0);
                    assert!(batch.traffic().outbound_bytes > 0);
                    saw_query = true;
                }
                Observation::SessionClosed { source, .. } => {
                    assert_eq!(source, QuitSource::None);
                    saw_close = true;
                }
                _ => {}
            }
            if saw_handshake && saw_query && saw_close {
                return;
            }
        }
    })
    .await;
    assert!(
        received.is_ok(),
        "session observations reach the bounded queue"
    );
    assert!(saw_handshake && saw_query && saw_close);
    stack.dispatch_task.abort();
}

/// PARITY-RSP-004 at the session engine, not just the observer.
///
/// `ResponseObserver` already models `SERVER_MORE_RESULTS_EXISTS` and the
/// response corpus pins it per packet, but nothing exercised the engine's own
/// `response_rounds` loop, whose `MoreResults` arm is what actually keeps
/// reading the backend. Both halves must hold: the client receives both result
/// sets in order, and -- the part a first-set-only proxy fails -- the session
/// is still synchronised afterwards, since an unread second set would sit in
/// the backend socket and answer the next command at the wrong sequence.
#[tokio::test]
async fn multi_result_command_forwards_every_set_and_keeps_the_session_synchronised() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };

    let rows = timeout(
        Duration::from_secs(5),
        client.query_multi_result("SELECT 0; SELECT 1"),
    )
    .await;
    assert_eq!(
        rows.ok().flatten(),
        Some(vec![b'0', b'1']),
        "both result sets reach the client, in order"
    );

    // The discriminator: a proxy that stopped at the first terminator leaves
    // the second set unread, so this command answers at the wrong sequence.
    assert!(
        timeout(Duration::from_secs(5), client.query_ok("SELECT 1"))
            .await
            .unwrap_or(false),
        "the session is still synchronised after a multi-result command"
    );
    client.quit().await;
    stack.dispatch_task.abort();
}

/// Wire-trace dump for the fast-path on/off A/B (see
/// `scripts/fastpath-trace-ab.sh`). Runs a fixed steady-state command scenario
/// against the real engine + fake backend and prints, between markers, the
/// ordered command-phase wire trace: each command forwarded to the backend
/// (its bytes) and the client-visible OK/err outcome, in order. The handshake
/// (random salt / connection id) is excluded by snapshotting the backend
/// transcript after login, so the trace is deterministic across runs.
///
/// The outer script runs this twice — `TIPROXY_FSM_FASTPATH` unset (slow path)
/// and set (fast path) — and diffs the two traces; byte-identical traces prove
/// the steady-state fast path preserves the observable action sequence. With
/// the flag set, the steady `SELECT`/`UPDATE` commands here commit on the fast
/// path (a quiescent `Ready` `ClientCommand`), the exact case the gate unit
/// tests prove commits; with it unset they take the slow path.
#[tokio::test]
async fn dump_fastpath_wire_trace() {
    use std::fmt::Write as _;
    let fast = std::env::var("TIPROXY_FSM_FASTPATH").unwrap_or_default();
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };

    // Baseline: everything forwarded so far is the handshake/auth relay
    // (nondeterministic salt), which the trace must exclude.
    let forwarded_before = stack
        .backend_transcript
        .lock()
        .map_or(0, |transcript| transcript.len());

    // Fixed steady-state command scenario: each is a quiescent `Ready`
    // `ClientCommand` the fast path is eligible to commit.
    let scenario = ["SELECT 1", "SELECT 2", "UPDATE t SET a = 1", "SELECT 3"];
    let mut outcomes = Vec::new();
    for sql in scenario {
        let ok = timeout(Duration::from_secs(5), client.query_ok(sql))
            .await
            .unwrap_or(false);
        outcomes.push(ok);
    }

    let forwarded: Vec<Vec<u8>> = {
        let transcript = stack
            .backend_transcript
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        transcript[forwarded_before..].to_vec()
    };

    println!("WIRE_TRACE_BEGIN fast_env={fast:?}");
    for (index, bytes) in forwarded.iter().enumerate() {
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            let _ = write!(hex, "{byte:02x}");
        }
        println!("FWD {index} {hex}");
    }
    for (index, ok) in outcomes.iter().enumerate() {
        println!("RSP {index} ok={ok}");
    }
    println!("WIRE_TRACE_END");

    // The scenario itself must have run end to end (every command answered),
    // independent of which path served it.
    assert!(outcomes.iter().all(|ok| *ok), "every command answered OK");
    assert_eq!(
        forwarded.len(),
        scenario.len(),
        "each command forwarded once"
    );

    client.quit().await;
    stack.dispatch_task.abort();
}

/// A complete topology generation re-applies the admission snapshot's
/// health-specific policy to an already authenticated socket, without
/// replacing its protocol config or disturbing the next command boundary.
#[tokio::test]
async fn existing_session_reapplies_keepalive_on_backend_health_flip() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT before_health_flip").await);

    let unhealthy = engine_health_snapshot(stack.sql_port, stack.backend_port, 2, false);
    assert!(stack.server_handle.update_snapshot(unhealthy).is_ok());
    let unhealthy_update = timeout(Duration::from_secs(2), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::BackendKeepaliveUpdated {
                backend,
                healthy: false,
                succeeded,
            } = observation
            {
                return Some((backend, succeeded));
            }
        }
        None
    })
    .await;
    assert!(matches!(
        unhealthy_update,
        Ok(Some((backend, true))) if backend.ends_with(&stack.backend_port.to_string())
    ));
    assert!(client.query_ok("SELECT after_unhealthy_flip").await);

    let healthy = engine_health_snapshot(stack.sql_port, stack.backend_port, 3, true);
    assert!(stack.server_handle.update_snapshot(healthy).is_ok());
    let healthy_update = timeout(Duration::from_secs(2), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::BackendKeepaliveUpdated {
                healthy: true,
                succeeded,
                ..
            } = observation
            {
                return Some(succeeded);
            }
        }
        None
    })
    .await;
    assert_eq!(
        healthy_update,
        Ok(Some(cfg!(target_os = "linux"))),
        "non-Linux reports the configured TCP_USER_TIMEOUT diagnostic"
    );
    assert!(client.query_ok("SELECT after_healthy_flip").await);

    client.quit().await;
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

#[tokio::test]
async fn metering_unknown_initial_keyspace_rejects_before_billable_forwarding() {
    let Ok(registry) = MeteringSourceRegistry::new(7) else {
        unreachable!("metering registry")
    };
    let stack = spawn_metered_stack(registry.clone()).await;
    spawn_route_answer(&stack, 1, 2);
    let client = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten();
    assert!(client.is_none(), "unknown keyspace must reject the session");
    assert_eq!(
        metering_source_count(&registry),
        0,
        "an un-attributed backend never becomes a billable source"
    );
    stack.dispatch_task.abort();
}

#[tokio::test]
async fn metering_redirect_unknown_keeps_old_and_success_starts_new_generation() {
    let Ok(registry) = MeteringSourceRegistry::new(7) else {
        unreachable!("metering registry")
    };
    let stack = spawn_metered_stack(registry.clone()).await;
    let (target_port, _target_transcript, _target_written_bytes) =
        spawn_fake_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer_with_keyspace(&stack, 1, 2, "keyspace-a");
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("metered session established")
    };
    assert_eq!(metering_source_count(&registry), 1);

    let unknown = command_envelope(
        6001,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "meter-unknown".to_owned(),
            backend_id: "tidb-other".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: false,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(unknown).await;
    let Some(unknown) = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "meter-unknown")
    })
    .await
    else {
        unreachable!("unknown redirect terminal")
    };
    let Some(Body::RedirectResult(unknown)) = unknown.body else {
        unreachable!()
    };
    assert!(!unknown.succeeded);
    assert_eq!(metering_source_count(&registry), 1);
    assert!(client.query_ok("SELECT old_survives").await);

    let valid = command_envelope(
        6002,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "meter-valid".to_owned(),
            backend_id: "tidb-other".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: "keyspace-b".to_owned(),
            backend_unhealthy: false,
            backend_local: false,
            deadline_unix_millis: 0,
            command_sequence: 2,
        }),
    );
    let _ = stack.forwarder.handle(valid).await;
    let Some(valid) = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "meter-valid")
    })
    .await
    else {
        unreachable!("valid redirect terminal")
    };
    let Some(Body::RedirectResult(valid)) = valid.body else {
        unreachable!()
    };
    assert!(valid.succeeded);
    assert_eq!(
        metering_source_count(&registry),
        2,
        "old final and new generation coexist until WAL handoff"
    );
    assert!(client.query_ok("SELECT new_generation").await);
    client.quit().await;
    stack.dispatch_task.abort();
}

fn metering_source_count(registry: &MeteringSourceRegistry) -> usize {
    let Ok(count) = registry.active_source_count() else {
        unreachable!("metering source registry is available")
    };
    count
}

/// SES-07/MIG-005 redirect-in-transaction hold: a `BEGIN` issued while a
/// redirect is armed and a transaction is open is held — the proxy commits the
/// old backend internally (its OK never reaches the client), migrates, and
/// replays the `BEGIN` exactly once on the new backend. The old backend sees
/// `COMMIT` then the snapshot query; the new backend sees the restore then the
/// single replayed `BEGIN`, whose OK answers the client.
#[tokio::test]
async fn redirect_in_transaction_holds_commits_and_replays_begin() {
    let stack = spawn_stack().await;
    let (target_port, target_transcript, _target_bytes) =
        spawn_fake_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };

    // Open a transaction on the old backend: the session is now unsafe for a
    // plain redirect, so the redirect can only proceed through the BEGIN hold.
    assert!(
        client.query_ok("BEGIN").await,
        "transaction opens on old backend"
    );

    // Arm the redirect while in-transaction; it is queued, not fired. The
    // short settle lets the admitted redirect reach the engine (set
    // `redirect_target`) before the next command's forward point.
    let redirect = command_envelope(
        7001,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "hold-begin".to_owned(),
            backend_id: "tidb-target".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: false,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A second BEGIN is held: internal COMMIT on the old backend, migrate, then
    // the BEGIN replays on the new backend and its OK answers the client.
    assert!(
        client.query_ok("BEGIN").await,
        "held BEGIN replays and answers the client exactly once"
    );

    let Some(result) = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "hold-begin")
    })
    .await
    else {
        unreachable!("redirect terminal")
    };
    let Some(Body::RedirectResult(result)) = result.body else {
        unreachable!()
    };
    assert!(result.succeeded, "migration succeeds during the hold");

    // The old backend saw the internal COMMIT before the snapshot query, and
    // never received a client-visible duplicate.
    {
        let old = match stack.backend_transcript.lock() {
            Ok(old) => old,
            Err(error) => unreachable!("old transcript: {error}"),
        };
        let commit_at = old
            .iter()
            .position(|packet| packet.as_slice() == b"\x03COMMIT");
        let snapshot_at = old
            .iter()
            .position(|packet| packet.as_slice() == b"\x03SHOW SESSION_STATES");
        assert!(
            commit_at.is_some(),
            "old backend received the internal COMMIT"
        );
        assert!(
            snapshot_at.is_some(),
            "old backend received the migration snapshot"
        );
        assert!(
            commit_at < snapshot_at,
            "COMMIT precedes the snapshot query"
        );
    }

    // The new backend saw the restore then the replayed BEGIN exactly once.
    {
        let target = match target_transcript.lock() {
            Ok(target) => target,
            Err(error) => unreachable!("target transcript: {error}"),
        };
        let restored = target
            .iter()
            .any(|packet| packet.starts_with(b"\x03SET SESSION_STATES '"));
        let replayed_begins = target
            .iter()
            .filter(|packet| packet.as_slice() == b"\x03BEGIN")
            .count();
        assert!(restored, "new backend received the session-state restore");
        assert_eq!(
            replayed_begins, 1,
            "BEGIN replays exactly once on the new backend"
        );
    }

    client.quit().await;
    stack.dispatch_task.abort();
}

/// SES-07/MIG-005 regression (found during exact-head cross-review): after the
/// internal COMMIT closes the transaction and migration succeeds, a replayed
/// BEGIN that fails with a **statusless** `MySQL` ERR must not leave the engine's
/// transaction tracker stale. Otherwise a later statusless `COM_STMT_PREPARE`
/// reads the stale flag in `prepare_session_event`, wrongly reopens the
/// transaction (`BackendResponseTxnOpen`), and blocks the graceful drain
/// forever. The long force deadline means the drain can close only at a genuine
/// safe boundary, so a close is attributable to the tracker being correct.
/// Deleting `self.in_transaction = in_transaction` in `hold_pending_begin`
/// leaves the flag stale-true and this drain never closes.
#[tokio::test]
async fn held_begin_commit_then_replay_error_lets_prepare_and_drain_close() {
    let stack = spawn_stack_long_drain().await;
    let (target_port, _target_transcript, _target_bytes) =
        spawn_fake_backend_server(SnapshotReply::ReplayBeginError).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };

    // Open a transaction on the old backend, then arm the redirect.
    assert!(
        client.query_ok("BEGIN").await,
        "transaction opens on old backend"
    );
    let redirect = command_envelope(
        7101,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "hold-begin-replay-err".to_owned(),
            backend_id: "tidb-target".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: false,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The held BEGIN commits the old backend, migrates, and replays on the new
    // backend — which rejects the replayed BEGIN with a statusless MySQL ERR.
    assert!(
        !client.query_ok("BEGIN").await,
        "the replayed BEGIN is rejected by the migrated backend"
    );
    let Some(result) = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "hold-begin-replay-err")
    })
    .await
    else {
        unreachable!("redirect terminal")
    };
    let Some(Body::RedirectResult(result)) = result.body else {
        unreachable!()
    };
    assert!(
        result.succeeded,
        "migration succeeds even though the replayed BEGIN errors"
    );

    // The internal COMMIT closed the transaction, so this statusless PREPARE
    // must NOT reopen it. Without the tracker sync the prepare reopens the
    // transaction and the drain below never closes.
    assert_eq!(
        client.stmt_prepare("NOMETA").await,
        Some(PrepareOutcome::Ok {
            statement_id: 7,
            parameters: 0,
            columns: 0,
        }),
        "the prepare after the closed transaction succeeds"
    );

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "internal COMMIT closed the transaction; the prepare must not reopen it, so the drain closes"
    );
    stack.dispatch_task.abort();
}

/// A per-connection `CloseCommand` resolves under its exact admitted id
/// with the initiating request id, and the client socket closes.
#[tokio::test]
async fn close_command_terminal_is_exact() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    let close = command_envelope(
        4321,
        Body::CloseCommand(CloseCommand {
            connection_id: 1,
            close_id: "c-exact".to_owned(),
            error_source: 0,
            reason: String::new(),
            force: false,
        }),
    );
    let _ = stack.forwarder.handle(close).await;
    let result = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::CloseResult(result)) if result.close_id == "c-exact"),
    )
    .await;
    let Some(result) = result else {
        unreachable!("the close resolves under its exact admitted id")
    };
    assert_eq!(
        result.request_id, 4321,
        "terminal answers the initiating id"
    );
    // The graceful close lands at the idle boundary: the client's next
    // read observes EOF.
    let gone = timeout(Duration::from_secs(3), async {
        loop {
            if !client.query_ok("SELECT 3").await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(gone.is_ok(), "the session closes at the safe boundary");
    stack.dispatch_task.abort();
}

/// A drain closes the session at the safe boundary and the completion
/// transition proactively produces the terminal `DrainResult`.
#[tokio::test]
async fn drain_closes_idle_session_and_completes() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    let now_ms = 1_000_000_u64;
    let drain = command_envelope(
        5000,
        Body::DrainCommand(DrainCommand {
            drain_id: "d-e2e".to_owned(),
            listener_names: Vec::new(),
            backend_ids: Vec::new(),
            graceful_deadline_unix_millis: now_ms,
            force_deadline_unix_millis: now_ms + 60_000,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(drain).await;
    let terminal = wait_sent(&stack.sender, |e| {
        matches!(&e.body, Some(Body::DrainResult(result))
            if result.drain_id == "d-e2e" && result.complete)
    })
    .await;
    let Some(terminal) = terminal else {
        unreachable!("the last session closing completes the drain proactively")
    };
    assert_eq!(
        terminal.request_id, 5000,
        "the terminal answers the initiating drain request"
    );
    let _ = client;
    stack.dispatch_task.abort();
}

/// The backend sees the proxy-owned placeholder exchange, never client auth.
fn assert_rewritten_change_user(
    commands: &[Vec<u8>],
    capabilities: CapabilityFlags,
    expected_attributes: &[(Vec<u8>, Vec<u8>)],
    original_secret: &[u8],
) {
    let Some(rewritten) = commands
        .iter()
        .find(|payload| payload.first() == Some(&0x11))
    else {
        unreachable!("rewritten change-user reaches the backend: {commands:?}")
    };
    let Ok(parsed) = parse_change_user(rewritten, capabilities) else {
        unreachable!("rewritten request parses")
    };
    assert_eq!(parsed.username, b"new_user");
    assert_eq!(parsed.database, b"new_db");
    assert_eq!(parsed.auth_response, b"");
    assert_eq!(
        parsed.auth_plugin_name,
        Some(b"auth_unknown_plugin".as_slice())
    );
    let parsed_attributes = parsed.attributes.map(|attributes| {
        attributes
            .into_iter()
            .filter_map(Result::ok)
            .map(|attribute| (attribute.key.to_vec(), attribute.value.to_vec()))
            .collect::<Vec<_>>()
    });
    assert_eq!(parsed_attributes.as_deref(), Some(expected_attributes));
    assert!(commands.iter().all(|payload| {
        !payload
            .windows(original_secret.len())
            .any(|window| window == original_secret)
    }));
}

/// A control redirect snapshots the old owner, authenticates the target with
/// the private token, restores the exact escaped state, then atomically swaps
/// under the exact admitted id. The client connection keeps serving.
#[tokio::test]
async fn change_user_success_commits_identity_clears_guard_and_migrates() {
    let stack = spawn_stack().await;
    let new_attributes = vec![(b"program_name".to_vec(), b"changed-client".to_vec())];
    let (target_port, _target_transcript, _target_written_bytes) =
        spawn_identity_validating_backend(b"new_user", &new_attributes).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("BEGIN").await, "old session enters a txn");
    assert!(client.send_long_data(77).await, "prepared guard is pending");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_wire_attributes = [Attribute {
        key: b"program_name",
        value: b"changed-client",
    }];
    let original_secret = b"client-scramble-must-disappear";
    assert_eq!(
        client
            .change_user(
                b"new_user",
                b"new_db",
                original_secret,
                &new_wire_attributes,
            )
            .await,
        Ok(()),
        "fresh-salt multi-round reauthentication succeeds"
    );
    assert!(
        client.query_ok("SELECT after_change_user").await,
        "the next command starts at a clean packet boundary"
    );

    let commands = stack
        .backend_transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert_rewritten_change_user(
        &commands,
        client.capabilities,
        &new_attributes,
        original_secret,
    );
    assert!(
        !format!("{:?}", stack.sender.sent()).contains("client-scramble-must-disappear"),
        "the original PendingCommand auth response never enters control IPC"
    );

    let redirect = command_envelope(
        6800,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-change-user-success".to_owned(),
            backend_id: "tidb-current-identity".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-change-user-success")
    })
    .await;
    assert!(
        matches!(
            result.and_then(|envelope| envelope.body),
            Some(Body::RedirectResult(result)) if result.succeeded
        ),
        "successful change-user clears txn/prepared guards and migration uses its current identity"
    );
    assert!(client.query_ok("SELECT after_identity_migration").await);
    client.quit().await;
    stack.dispatch_task.abort();
}

/// A rejected change-user preserves both the previous identity and the pending
/// prepared guard. The queued redirect must stay blocked until RESET clears the
/// old guard, then authenticate its candidate as the original user/attributes.
#[tokio::test]
async fn change_user_failure_preserves_identity_and_prepared_guard() {
    let stack = spawn_stack().await;
    let old_attributes = vec![(b"program_name".to_vec(), b"initial-client".to_vec())];
    let (target_port, _target_transcript, _target_written_bytes) =
        spawn_identity_validating_backend(b"root", &old_attributes).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.send_long_data(88).await);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let denied_attributes = [Attribute {
        key: b"program_name",
        value: b"must-not-commit",
    }];
    assert_eq!(
        client
            .change_user(
                b"denied_user",
                b"denied_db",
                b"denied-secret",
                &denied_attributes,
            )
            .await,
        Err(1045)
    );
    assert!(
        client.query_ok("SELECT after_denied_change_user").await,
        "ERR completes the command without closing the old owner"
    );

    let redirect = command_envelope(
        6801,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-change-user-failed".to_owned(),
            backend_id: "tidb-old-identity".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        stack.sender.sent().into_iter().all(|envelope| !matches!(
            &envelope.body,
            Some(Body::RedirectResult(result)) if result.redirect_id == "r-change-user-failed"
        )),
        "the failed change-user leaves the old long-data guard pending"
    );

    assert!(client.stmt_reset_ok(88).await);
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-change-user-failed")
    })
    .await;
    assert!(
        matches!(
            result.and_then(|envelope| envelope.body),
            Some(Body::RedirectResult(result)) if result.succeeded
        ),
        "RESET releases the old guard and the candidate sees the old identity"
    );
    assert!(
        client
            .query_ok("SELECT after_failed_identity_migration")
            .await
    );
    client.quit().await;
    stack.dispatch_task.abort();
}

/// The runtime rejects an unparseable request before a single change-user byte
/// reaches the backend.
#[tokio::test]
async fn malformed_change_user_fails_before_backend_write() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    client.writer.reset_sequence(0);
    assert!(client.writer.write_logical(&[0x11], true).await.is_ok());
    client.reader.reset_sequence(1);
    let closed = timeout(
        Duration::from_secs(2),
        client.reader.read_logical(64 * 1024),
    )
    .await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "malformed request closes fail-closed"
    );
    let commands = stack
        .backend_transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert!(
        commands
            .iter()
            .all(|payload| payload.first() != Some(&0x11)),
        "no malformed change-user reaches the backend: {commands:?}"
    );
    stack.dispatch_task.abort();
}

/// PKT-005: a command whose logical payload is empty carries no command byte and
/// is malformed. The production engine fails the session closed before any
/// backend write, matching Go's rejection of a sub-one-byte command packet.
#[tokio::test]
async fn empty_command_payload_fails_closed_before_backend_write() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    client.writer.reset_sequence(0);
    // A zero-length logical command is a single zero-length physical packet with
    // no command byte.
    assert!(client.writer.write_logical(&[], true).await.is_ok());
    client.reader.reset_sequence(1);
    let closed = timeout(
        Duration::from_secs(2),
        client.reader.read_logical(64 * 1024),
    )
    .await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "an empty command payload closes fail-closed"
    );
    let commands = stack
        .backend_transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert!(
        commands.iter().all(|payload| !payload.is_empty()),
        "no empty command reaches the backend: {commands:?}"
    );
    stack.dispatch_task.abort();
}

/// PKT-001/002: a backend response is forwarded through the production engine
/// with exact `MySQL` physical framing. A `MAX-1` logical response is a single
/// physical packet; `MAX+33` splits into two; an exact `MaxPayloadLen` response
/// emits the required zero-length terminator (also two packets). Each response
/// advances the 8-bit sequence exactly once per physical packet (zero
/// mismatches, observed from the reader's `ForwardProgress`, not a tolerant
/// resync), and each subsequent command realigns at a clean sequence boundary.
/// The tuple is `(physical_packets, sequence_mismatches, next_expected_sequence)`.
#[tokio::test]
async fn packet_framing_splits_and_realigns_through_production_engine() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert_eq!(
        client
            .query_large_ok("SELECT pkt_under_max_response", PKT_UNDER_MAX_RESPONSE_LEN)
            .await,
        Some((1, 0, 2)),
        "a MAX-1 response is one physical packet, no sequence mismatch, next seq 2"
    );
    assert!(
        client.query_ok("SELECT after_under_max").await,
        "the next command realigns after the single-packet response"
    );
    assert_eq!(
        client
            .query_large_ok("SELECT pkt_large_response", PKT_LARGE_RESPONSE_LEN)
            .await,
        Some((2, 0, 3)),
        "a MAX+33 response is two physical packets, no sequence mismatch, next seq 3"
    );
    assert!(
        client.query_ok("SELECT after_large").await,
        "the next command realigns after the two-packet response"
    );
    assert_eq!(
        client
            .query_large_ok(
                "SELECT pkt_exact_multiple_response",
                PKT_EXACT_MULTIPLE_RESPONSE_LEN,
            )
            .await,
        Some((2, 0, 3)),
        "exact MaxPayloadLen adds the zero-length terminator (two packets), next seq 3"
    );
    assert!(
        client.query_ok("SELECT after_exact").await,
        "the next command realigns after the exact-multiple terminator"
    );
    stack.dispatch_task.abort();
}

/// A header-only OK is relayed exactly once but cannot commit identity or keep
/// the session alive because it lacks the status field that defines the
/// transaction boundary.
#[tokio::test]
async fn change_user_ok_without_status_fails_closed() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    let attributes = [Attribute {
        key: b"program_name",
        value: b"malformed-ok",
    }];
    assert_eq!(
        client
            .change_user(b"malformed_ok", b"db", b"original-secret", &attributes,)
            .await,
        Ok(()),
        "client receives the already-forwarded terminal byte"
    );
    assert!(
        !client.query_ok("SELECT must_not_run").await,
        "missing OK status poisons the session instead of retaining txn state"
    );
    stack.dispatch_task.abort();
}

fn assert_atomic_redirect_transcripts(stack: &Stack, target_transcript: &Arc<Mutex<Vec<Vec<u8>>>>) {
    let old_commands = stack
        .backend_transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert_eq!(
        old_commands
            .iter()
            .filter(|payload| payload.as_slice() == b"\x03SHOW SESSION_STATES")
            .count(),
        1,
        "MIG-00 captures one snapshot at the redirect safe boundary"
    );
    let target_commands = target_transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert_eq!(
        target_commands
            .iter()
            .filter(|payload| payload.starts_with(b"\x03SET SESSION_STATES '"))
            .count(),
        1,
        "MIG-01 restores the candidate exactly once before swap"
    );
    assert!(
        old_commands.iter().all(|payload| !payload
            .windows(b"signed-token-private".len())
            .any(|window| window == b"signed-token-private")),
        "the signed token is backend-to-proxy only"
    );
    let before = old_commands
        .iter()
        .position(|payload| payload.as_slice() == b"\x03SELECT 1");
    let snapshot = old_commands
        .iter()
        .position(|payload| payload.as_slice() == b"\x03SHOW SESSION_STATES");
    let restore = target_commands
        .iter()
        .position(|payload| payload.starts_with(b"\x03SET SESSION_STATES '"));
    let after = target_commands
        .iter()
        .position(|payload| payload.as_slice() == b"\x03SELECT after_snapshot");
    assert!(
        matches!((before, snapshot, restore, after), (Some(before), Some(snapshot), Some(restore), Some(after)) if before < snapshot && restore < after),
        "snapshot is on the old owner and restore precedes target traffic: old={old_commands:?} target={target_commands:?}"
    );
    assert!(
        old_commands
            .iter()
            .all(|payload| payload.as_slice() != b"\x03SELECT after_snapshot"),
        "the first post-redirect command reaches only the new owner"
    );
}

struct RedirectReplayAndStale<'a> {
    stack: &'a Stack,
    client: &'a mut MysqlClient,
    redirect: ControlEnvelope,
    target_transcript: &'a Arc<Mutex<Vec<Vec<u8>>>>,
    target_connections: &'a AtomicU64,
    stale_port: u16,
    stale_transcript: &'a Arc<Mutex<Vec<Vec<u8>>>>,
    stale_connections: &'a AtomicU64,
}

async fn assert_completed_redirect_replay_is_side_effect_free(
    fixture: &RedirectReplayAndStale<'_>,
) {
    let mut duplicate = fixture.redirect.clone();
    duplicate.request_id = 6001;
    let _ = fixture.stack.forwarder.handle(duplicate).await;
    let replay = wait_sent(&fixture.stack.sender, |envelope| {
        envelope.request_id == 6001
            && matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-e2e")
    })
    .await;
    assert!(matches!(
        replay.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result))
            if result.succeeded
                && result.previous_backend_id == "tidb-fake"
                && result.backend_id == "tidb-other"
    ));
    assert_eq!(
        fixture.target_connections.load(Ordering::Relaxed),
        1,
        "a completed duplicate replays its terminal without a second dial"
    );
    assert_eq!(
        fixture
            .stack
            .backend_transcript
            .lock()
            .map_or(0, |commands| commands
                .iter()
                .filter(|payload| payload.as_slice() == b"\x03SHOW SESSION_STATES")
                .count()),
        1,
        "a completed duplicate does not snapshot the old owner twice"
    );
    assert_eq!(
        fixture
            .target_transcript
            .lock()
            .map_or(0, |commands| commands
                .iter()
                .filter(|payload| payload.starts_with(b"\x03SET SESSION_STATES '"))
                .count()),
        1,
        "a completed duplicate does not restore the target twice"
    );
}

async fn assert_stale_redirect_is_side_effect_free(fixture: &mut RedirectReplayAndStale<'_>) {
    let stale = ControlEnvelope {
        generation: 9,
        ..command_envelope(
            6002,
            Body::RedirectCommand(RedirectCommand {
                connection_id: 1,
                redirect_id: "r-stale-generation".to_owned(),
                backend_id: "tidb-stale-target".to_owned(),
                backend_address: format!("127.0.0.1:{}", fixture.stale_port),
                cluster_name: String::new(),
                keyspace: String::new(),
                backend_unhealthy: false,
                backend_local: true,
                deadline_unix_millis: 0,
                command_sequence: 2,
            }),
        )
    };
    let _ = fixture.stack.forwarder.handle(stale).await;
    let stale_error = wait_sent(&fixture.stack.sender, |envelope| {
        envelope.request_id == 6002
            && matches!(&envelope.body, Some(Body::Error(error)) if error.code == ErrorCode::StaleGeneration as i32)
    })
    .await;
    assert!(stale_error.is_some(), "stale generation fails explicitly");
    assert_eq!(
        fixture.stale_connections.load(Ordering::Relaxed),
        0,
        "a stale generation is rejected before any target socket is touched"
    );
    assert!(
        fixture
            .stale_transcript
            .lock()
            .is_ok_and(|commands| commands.is_empty()),
        "a stale generation cannot authenticate or restore a candidate"
    );
    assert!(
        fixture
            .client
            .query_ok("SELECT after_stale_generation")
            .await
    );
    assert!(
        fixture
            .target_transcript
            .lock()
            .is_ok_and(|commands| commands
                .iter()
                .any(|payload| payload.as_slice() == b"\x03SELECT after_stale_generation")),
        "rejecting stale control leaves the successful target as sole owner"
    );
    assert!(
        fixture
            .stack
            .backend_transcript
            .lock()
            .is_ok_and(|commands| commands
                .iter()
                .all(|payload| payload.as_slice() != b"\x03SELECT after_stale_generation")),
        "the retired owner never receives a later user command"
    );
}

#[tokio::test]
async fn redirect_restores_candidate_and_swaps_atomically() {
    let stack = spawn_stack().await;
    let (target_port, target_transcript, target_written_bytes, target_connections) =
        spawn_counting_fake_backend_server(SnapshotReply::Valid).await;
    let (stale_port, stale_transcript, _, stale_connections) =
        spawn_counting_fake_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    let redirect = command_envelope(
        6000,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-e2e".to_owned(),
            backend_id: "tidb-other".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect.clone()).await;
    let result = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-e2e"),
    )
    .await;
    let Some(result) = result else {
        let transcript = stack
            .backend_transcript
            .lock()
            .map_or_else(|_| Vec::new(), |commands| commands.clone());
        unreachable!("redirect terminal missing; transcript={transcript:?}")
    };
    let Some(Body::RedirectResult(result)) = result.body else {
        unreachable!()
    };
    assert!(result.succeeded, "the restored candidate takes ownership");
    assert_eq!(result.previous_backend_id, "tidb-fake");
    assert_eq!(result.backend_id, "tidb-other");
    assert_eq!(
        target_connections.load(Ordering::Relaxed),
        1,
        "one accepted redirect creates exactly one candidate connection"
    );
    assert!(
        client.query_ok("SELECT after_snapshot").await,
        "the session keeps its backend and keeps serving"
    );
    assert_atomic_redirect_transcripts(&stack, &target_transcript);
    let mut replay_and_stale = RedirectReplayAndStale {
        stack: &stack,
        client: &mut client,
        redirect,
        target_transcript: &target_transcript,
        target_connections: &target_connections,
        stale_port,
        stale_transcript: &stale_transcript,
        stale_connections: &stale_connections,
    };
    assert_completed_redirect_replay_is_side_effect_free(&replay_and_stale).await;
    assert_stale_redirect_is_side_effect_free(&mut replay_and_stale).await;
    client.quit().await;
    assert_closed_backend_bytes_include_retired(&stack, &target_written_bytes).await;
    stack.dispatch_task.abort();
}

#[tokio::test]
async fn redirect_immediately_reconciles_target_keepalive_with_live_topology() {
    let mut stack = spawn_stack().await;
    let (target_port, _target_transcript, _target_written_bytes) =
        spawn_fake_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };

    // The control command was admitted while the target was healthy, but the
    // latest complete topology already marks that exact target unhealthy. The
    // current owner remains healthy, so only the post-swap reconciliation can
    // produce the unhealthy update below.
    let topology =
        engine_redirect_health_snapshot(stack.sql_port, stack.backend_port, target_port, 2);
    assert!(stack.server_handle.update_snapshot(topology).is_ok());
    let redirect = command_envelope(
        6040,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-health-flip".to_owned(),
            backend_id: "tidb-other".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(
            &envelope.body,
            Some(Body::RedirectResult(result)) if result.redirect_id == "r-health-flip"
        )
    })
    .await;
    assert!(matches!(
        result.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result)) if result.succeeded
    ));

    let update = timeout(Duration::from_secs(2), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::BackendKeepaliveUpdated {
                backend,
                healthy: false,
                succeeded: true,
            } = observation
            {
                return Some(backend);
            }
        }
        None
    })
    .await;
    assert!(matches!(
        update,
        Ok(Some(backend)) if backend.ends_with(&target_port.to_string())
    ));
    assert!(client.query_ok("SELECT after_health_flip_redirect").await);

    client.quit().await;
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

/// Backend TLS is renegotiated independently for both the original owner and
/// the migration candidate. The target accepts the token handshake and state
/// restore only after the plaintext `SSLRequest` has upgraded the socket.
#[tokio::test]
async fn redirect_restores_candidate_over_backend_tls() {
    let stack = spawn_tls_stack(SnapshotReply::Valid).await;
    let (target_port, target_transcript, target_written_bytes) =
        spawn_fake_tls_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established through backend TLS")
    };
    assert!(client.query_ok("SELECT before_tls_redirect").await);
    let redirect = command_envelope(
        6149,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-tls".to_owned(),
            backend_id: "tidb-tls-target".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-tls")
    })
    .await;
    assert!(matches!(
        result.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result))
            if result.succeeded && result.backend_id == "tidb-tls-target"
    ));
    assert!(client.query_ok("SELECT after_tls_redirect").await);
    assert!(
        target_transcript.lock().is_ok_and(|commands| commands
            .iter()
            .any(|payload| payload.starts_with(b"\x03SET SESSION_STATES '"))),
        "the state restore is visible only after the target TLS accept and token auth"
    );
    client.quit().await;
    assert_closed_backend_bytes_include_retired(&stack, &target_written_bytes).await;
    stack.dispatch_task.abort();
}

async fn redirect_restores_candidate_over_compression(
    algorithm: CompressionAlgorithm,
    proxy_v2: bool,
) {
    let stack = spawn_stack_full(
        SnapshotReply::Valid,
        proxy_v2,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
    )
    .await;
    let (target_port, target_transcript, target_written_bytes, _) =
        spawn_fake_compressed_backend_server(SnapshotReply::Valid, algorithm, proxy_v2).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        CompressedClient::connect(stack.sql_port, algorithm),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("compressed session established for {algorithm:?}")
    };
    assert!(
        client.query_ok("SELECT before_compressed_redirect").await,
        "the old owner serves the compressed client before migration ({algorithm:?})"
    );

    let redirect_id = format!("r-compressed-{algorithm:?}-{proxy_v2}");
    let redirect = command_envelope(
        6150,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: redirect_id.clone(),
            backend_id: "tidb-compressed-target".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == redirect_id)
    })
    .await;
    assert!(matches!(
        result.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result))
            if result.succeeded && result.backend_id == "tidb-compressed-target"
    ));
    assert!(
        client.query_ok("SELECT after_compressed_redirect").await,
        "the migrated compressed backend stays aligned ({algorithm:?}, proxy_v2={proxy_v2})"
    );

    let target_commands = target_transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    let restore = target_commands
        .iter()
        .position(|payload| payload.starts_with(b"\x03SET SESSION_STATES '"));
    let after = target_commands
        .iter()
        .position(|payload| payload.as_slice() == b"\x03SELECT after_compressed_redirect");
    assert!(
        matches!((restore, after), (Some(restore), Some(after)) if restore < after),
        "candidate auth switches to compression before one restore, then serves user traffic: {target_commands:?}"
    );
    assert_eq!(
        target_commands
            .iter()
            .filter(|payload| payload.starts_with(b"\x03SET SESSION_STATES '"))
            .count(),
        1,
        "the compressed candidate restores exactly once"
    );

    client.quit().await;
    assert_closed_backend_bytes_include_retired(&stack, &target_written_bytes).await;
    stack.dispatch_task.abort();
}

/// MIG-01 activates classic zlib on the candidate only after its plaintext auth
/// OK, restores the state through compressed framing, then atomically swaps.
#[tokio::test]
async fn redirect_restores_candidate_over_zlib() {
    redirect_restores_candidate_over_compression(CompressionAlgorithm::Zlib, false).await;
}

/// The same migration works with independently negotiated zstd while outbound
/// PROXY v2 is written before the target greeting. The target refuses to greet
/// unless that preamble is present, so success also proves the candidate dial
/// uses the configured PROXY path. CLOSED totals compare both retired/current
/// raw sockets even though both client and candidate traffic are compressed.
#[tokio::test]
async fn redirect_restores_candidate_over_zstd_and_proxy_v2() {
    redirect_restores_candidate_over_compression(CompressionAlgorithm::Zstd { level: 3 }, true)
        .await;
}

/// A backend ERR (pre-v9 missing signing certificate), a NULL/empty token,
/// and malformed but bounded JSON are complete internal responses. The
/// redirect fails, but the old backend stays aligned and serves the next user
/// command.
#[tokio::test]
async fn complete_snapshot_validation_failures_preserve_old_backend() {
    for behavior in [
        SnapshotReply::SigningCertificateError,
        SnapshotReply::NullToken,
        SnapshotReply::EmptyToken,
        SnapshotReply::MalformedJson,
    ] {
        let stack = spawn_stack_with_snapshot(behavior).await;
        spawn_route_answer(&stack, 1, 2);
        let Some(mut client) =
            timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
                .await
                .ok()
                .flatten()
        else {
            unreachable!("session established for {behavior:?}")
        };
        assert!(client.query_ok("SELECT before_snapshot_failure").await);

        let redirect = command_envelope(
            6100,
            Body::RedirectCommand(RedirectCommand {
                connection_id: 1,
                redirect_id: format!("r-{behavior:?}"),
                backend_id: "tidb-other".to_owned(),
                backend_address: "127.0.0.1:1".to_owned(),
                cluster_name: String::new(),
                keyspace: String::new(),
                backend_unhealthy: false,
                backend_local: true,
                deadline_unix_millis: 0,
                command_sequence: 1,
            }),
        );
        let _ = stack.forwarder.handle(redirect).await;
        let result = wait_sent(&stack.sender, |envelope| {
            matches!(&envelope.body, Some(Body::RedirectResult(_)))
        })
        .await;
        let Some(ControlEnvelope {
            body: Some(Body::RedirectResult(result)),
            ..
        }) = result
        else {
            unreachable!("redirect terminal for {behavior:?}")
        };
        assert!(!result.succeeded);
        assert_eq!(result.previous_backend_id, "tidb-fake");
        assert_eq!(
            result.backend_id, "tidb-other",
            "a failed terminal names the attempted route target while preserving the old owner"
        );
        assert!(
            client.query_ok("SELECT after_snapshot_failure").await,
            "wire-complete {behavior:?} failure must preserve the old backend"
        );
        let snapshot_queries = stack.backend_transcript.lock().map_or(0, |commands| {
            commands
                .iter()
                .filter(|payload| payload.as_slice() == b"\x03SHOW SESSION_STATES")
                .count()
        });
        assert_eq!(snapshot_queries, 1);
        client.quit().await;
        stack.dispatch_task.abort();
    }
}

/// Candidate-only failures never disturb the aligned old owner: invalid or
/// expired tokens, restore ERR/disconnect, and an unreachable target all
/// resolve the exact redirect as failed while the next user command succeeds.
#[tokio::test]
async fn candidate_failures_preserve_old_backend() {
    for behavior in [
        SnapshotReply::InvalidToken,
        SnapshotReply::ExpiredToken,
        SnapshotReply::RestoreError,
        SnapshotReply::RestoreDisconnect,
    ] {
        let stack = spawn_stack_with_snapshot(behavior).await;
        let (target_port, _, _) = spawn_fake_backend_server(behavior).await;
        spawn_route_answer(&stack, 1, 2);
        let Some(mut client) =
            timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
                .await
                .ok()
                .flatten()
        else {
            unreachable!("session established for {behavior:?}")
        };
        assert!(client.query_ok("SELECT before_candidate_failure").await);
        let redirect = command_envelope(
            6150,
            Body::RedirectCommand(RedirectCommand {
                connection_id: 1,
                redirect_id: format!("r-{behavior:?}"),
                backend_id: "tidb-other".to_owned(),
                backend_address: format!("127.0.0.1:{target_port}"),
                cluster_name: String::new(),
                keyspace: String::new(),
                backend_unhealthy: false,
                backend_local: true,
                deadline_unix_millis: 0,
                command_sequence: 1,
            }),
        );
        let _ = stack.forwarder.handle(redirect).await;
        let result = wait_sent(&stack.sender, |envelope| {
            matches!(&envelope.body, Some(Body::RedirectResult(_)))
        })
        .await;
        let Some(ControlEnvelope {
            body: Some(Body::RedirectResult(result)),
            ..
        }) = result
        else {
            unreachable!("redirect terminal for {behavior:?}")
        };
        assert!(!result.succeeded);
        assert_eq!(result.previous_backend_id, "tidb-fake");
        assert_eq!(
            result.backend_id, "tidb-other",
            "candidate-only failure closes the exact old-to-target route event"
        );
        assert!(
            client.query_ok("SELECT after_candidate_failure").await,
            "candidate-only {behavior:?} failure must preserve the old backend"
        );
        client.quit().await;
        stack.dispatch_task.abort();
    }

    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established for unreachable target")
    };
    let redirect = command_envelope(
        6151,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-unreachable".to_owned(),
            backend_id: "tidb-unreachable".to_owned(),
            backend_address: "127.0.0.1:1".to_owned(),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-unreachable")
    })
    .await;
    assert!(matches!(
        result.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result)) if !result.succeeded
    ));
    assert!(client.query_ok("SELECT after_unreachable_target").await);
    client.quit().await;
    stack.dispatch_task.abort();
}

/// An absolute deadline that has already expired prevents candidate I/O and
/// resolves as one ordinary failed redirect without disturbing the old owner.
#[tokio::test]
async fn expired_redirect_deadline_preserves_old_backend() {
    let stack = spawn_stack().await;
    let (target_port, target_transcript, target_written_bytes) =
        spawn_fake_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established for expired redirect")
    };
    let redirect = command_envelope(
        6152,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-expired".to_owned(),
            backend_id: "tidb-expired".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 1,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-expired")
    })
    .await;
    assert!(matches!(
        result.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result)) if !result.succeeded
    ));
    assert!(
        target_transcript
            .lock()
            .is_ok_and(|commands| commands.is_empty()),
        "an expired deadline cannot send a candidate handshake"
    );
    assert_eq!(
        target_written_bytes.load(Ordering::Relaxed),
        0,
        "the expired attempt never reaches the target socket"
    );
    assert!(client.query_ok("SELECT after_expired_redirect").await);
    client.quit().await;
    stack.dispatch_task.abort();
}

/// The router's current health bit is part of the exact target snapshot. A
/// target already marked unhealthy is rejected before dial and the old owner
/// remains usable.
#[tokio::test]
async fn unhealthy_redirect_target_preserves_old_backend() {
    let stack = spawn_stack().await;
    let (target_port, target_transcript, target_written_bytes) =
        spawn_fake_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established for unhealthy redirect")
    };
    let redirect = command_envelope(
        6153,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-unhealthy".to_owned(),
            backend_id: "tidb-unhealthy".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: true,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-unhealthy")
    })
    .await;
    assert!(matches!(
        result.and_then(|envelope| envelope.body),
        Some(Body::RedirectResult(result)) if !result.succeeded
    ));
    assert!(
        target_transcript
            .lock()
            .is_ok_and(|commands| commands.is_empty())
    );
    assert_eq!(target_written_bytes.load(Ordering::Relaxed), 0);
    assert!(client.query_ok("SELECT after_unhealthy_redirect").await);
    client.quit().await;
    stack.dispatch_task.abort();
}

/// A backend disconnect or an oversized row leaves the internal response
/// incomplete. The engine reports the redirect terminal exactly once and
/// closes instead of reusing a poisoned old command stream.
#[tokio::test]
async fn incomplete_snapshot_failures_close_the_session() {
    for behavior in [SnapshotReply::Disconnect, SnapshotReply::OversizedJson] {
        let stack = spawn_stack_with_snapshot(behavior).await;
        spawn_route_answer(&stack, 1, 2);
        let Some(mut client) =
            timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
                .await
                .ok()
                .flatten()
        else {
            unreachable!("session established for {behavior:?}")
        };
        assert!(client.query_ok("SELECT before_snapshot_disconnect").await);

        let redirect = command_envelope(
            6200,
            Body::RedirectCommand(RedirectCommand {
                connection_id: 1,
                redirect_id: format!("r-{behavior:?}"),
                backend_id: "tidb-other".to_owned(),
                backend_address: "127.0.0.1:1".to_owned(),
                cluster_name: String::new(),
                keyspace: String::new(),
                backend_unhealthy: false,
                backend_local: true,
                deadline_unix_millis: 0,
                command_sequence: 1,
            }),
        );
        let _ = stack.forwarder.handle(redirect).await;
        let result = wait_sent(&stack.sender, |envelope| {
            matches!(&envelope.body, Some(Body::RedirectResult(_)))
        })
        .await;
        let Some(ControlEnvelope {
            body: Some(Body::RedirectResult(result)),
            ..
        }) = result
        else {
            unreachable!("redirect terminal for {behavior:?}")
        };
        assert!(!result.succeeded);
        assert!(
            timeout(Duration::from_secs(3), async {
                loop {
                    if !client.query_ok("SELECT must_not_reuse_poisoned_wire").await {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok(),
            "incomplete {behavior:?} response must close the session"
        );
        stack.dispatch_task.abort();
    }
}

/// Contract #1 cancel-safety: control activity racing a fragmented
/// client command must not drop bytes the engine already consumed from
/// the wire — the exchange completes intact once the frame arrives.
#[tokio::test]
async fn control_activity_during_fragmented_command_keeps_wire_intact() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    // Send only part of the next command's header, then let control
    // traffic win the engine's idle race while the frame is pending.
    let payload = b"\x03SELECT 6";
    let mut frame = vec![u8::try_from(payload.len()).unwrap_or(0), 0, 0, 0];
    frame.extend_from_slice(payload);
    assert!(client.writer.get_mut().write_all(&frame[..2]).await.is_ok());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let redirect = command_envelope(
        6001,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-frag".to_owned(),
            backend_id: "tidb-other".to_owned(),
            backend_address: "127.0.0.1:1".to_owned(),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: true,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let refused = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-frag"),
    )
    .await;
    assert!(refused.is_some(), "control served while the frame is open");

    // Complete the frame: nothing consumed earlier may be lost. A
    // desynced engine never answers, so the read is deadline-bounded.
    assert!(client.writer.get_mut().write_all(&frame[2..]).await.is_ok());
    client.reader.reset_sequence(1);
    match timeout(Duration::from_secs(5), client.reader.peek_packet()).await {
        Ok(Ok(preview)) => assert_eq!(preview.sequence_id, 1),
        other => unreachable!("the response survives the race: {other:?}"),
    }
    let Ok(Ok(response)) = timeout(
        Duration::from_secs(5),
        client.reader.read_logical(64 * 1024),
    )
    .await
    else {
        unreachable!("the fragmented command still round-trips")
    };
    assert_eq!(response.payload.first(), Some(&0x00));
    client.quit().await;
    stack.dispatch_task.abort();
}

/// Go's first-time handshake rewrite end to end: a real non-empty
/// native password authenticates only because the proxy forwards
/// `auth_unknown_plugin` with the original auth data preserved and the
/// backend re-challenges with its own salt (the fake backend rejects
/// any direct plugin, and the client asserts the switch salt differs
/// from the proxy greeting's).
#[tokio::test]
async fn non_empty_password_reauths_against_backend_salt() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Ok(login) = timeout(
        Duration::from_secs(5),
        MysqlClient::login(stack.sql_port, FAKE_BACKEND_PASSWORD),
    )
    .await
    else {
        unreachable!("login completes")
    };
    let Ok(mut client) = login else {
        unreachable!("the re-authentication succeeds end to end")
    };
    assert!(client.query_ok("SELECT 1").await);
    client.quit().await;
    stack.dispatch_task.abort();
}

/// An initial-auth failure keeps Go's `using password` semantics on the
/// client-visible error.
#[tokio::test]
async fn wrong_password_reports_using_password() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Ok(login) = timeout(
        Duration::from_secs(5),
        MysqlClient::login(stack.sql_port, "wrong-pa55"),
    )
    .await
    else {
        unreachable!("login terminates")
    };
    let Err(denied) = login else {
        unreachable!("a wrong password must be denied")
    };
    assert_eq!(
        denied.first(),
        Some(&0xFF),
        "MySQL error packet: {denied:?}"
    );
    assert!(
        String::from_utf8_lossy(&denied).contains("using password"),
        "Go's using-password semantics survive the relay: {denied:?}"
    );
    let closed_source = timeout(Duration::from_secs(5), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::SessionClosed { source, .. } = observation {
                return Some(source);
            }
        }
        None
    })
    .await;
    assert_eq!(closed_source, Ok(Some(QuitSource::ClientAuthFail)));
    stack.dispatch_task.abort();
}

/// The full force ownership chain with a permanently stuck backend:
/// session shutdown and server shutdown back-to-back (the executable's
/// signal path) must return the server — every listener, session owner,
/// loop, and engine joined or aborted — emit the CLOSED terminal exactly
/// once before the control dispatch stops, and fit ONE force budget of
/// wall clock (no stacked cleanup deadlines).
#[tokio::test]
async fn forced_shutdown_with_stuck_backend_joins_everything_in_budget() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    let slow = tokio::spawn(async move { client.query_ok("SELECT HANG").await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    stack.shutdown_tx.send_replace(true);
    stack.server_handle.shutdown();
    assert!(
        timeout(Duration::from_secs(5), stack.server_task)
            .await
            .is_ok_and(|joined| joined.is_ok()),
        "the server returns with every owned task joined"
    );
    let elapsed = started.elapsed();
    // Harness cleanup budget is 2s; one budget plus scheduling margins,
    // never the 4s+ two stacked deadlines would take.
    assert!(
        elapsed < Duration::from_secs(4),
        "one force budget: {elapsed:?}"
    );

    // The terminal work already happened — before the dispatch stops.
    let closed: Vec<_> = stack
        .sender
        .sent()
        .into_iter()
        .filter(|e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3))
        .collect();
    assert_eq!(closed.len(), 1, "CLOSED exactly once: {closed:?}");
    let Some(Body::ConnectionEvent(event)) = &closed[0].body else {
        unreachable!()
    };
    assert_eq!(event.error_source, 6, "shutdown attribution: {event:?}");
    stack.dispatch_task.abort();
    slow.abort();
}

/// PKT-007 production wiring: a transport break while reading the backend
/// greeting is a backend NETWORK break on BOTH observables. Pre-fix, the wire
/// reported `BackendNetwork` while the connection-close log said
/// `BackendHandshake` — this drives the real session engine against a backend
/// that accepts the
/// dial then closes before greeting, and pins the corrected, non-divergent
/// classification (the mutation-killer is `quit_source != BackendHandshake`).
#[tokio::test]
async fn backend_greeting_transport_break_is_backend_network_on_both_observables() {
    let mut stack = spawn_stack().await;
    // A backend that accepts the candidate dial then closes without ever
    // sending a greeting: the proxy's greeting read hits a clean EOF.
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("dead-backend bind")
    };
    let Ok(dead_addr) = listener.local_addr() else {
        unreachable!("dead-backend addr")
    };
    let dead_backend = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });
    spawn_route_answer_to(&stack, 1, 2, dead_addr.port());
    // The client handshake never completes; the connect attempt ends in error.
    let _ = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port)).await;

    // Wire observable: the CLOSED lifecycle event is a backend network break.
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(ControlEnvelope {
        body: Some(Body::ConnectionEvent(event)),
        ..
    }) = closed
    else {
        unreachable!("a handshake-phase failure emits a CLOSED lifecycle event")
    };
    assert_eq!(
        event.error_source, 2,
        "wire observable is BackendNetwork (ERROR_SOURCE_BACKEND_NETWORK=2): {event:?}"
    );

    // Log observable: the connection-close reason is ALSO a backend network
    // break, NOT BackendHandshake — the divergence this fix removes.
    let closed_source = timeout(Duration::from_secs(5), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::SessionClosed { source, .. } = observation {
                return Some(source);
            }
        }
        None
    })
    .await;
    assert_eq!(
        closed_source,
        Ok(Some(QuitSource::BackendNetwork)),
        "greeting transport break classifies as a backend network break on both observables"
    );

    dead_backend.abort();
    stack.dispatch_task.abort();
}

/// PKT-007 production wiring — the `IoSide` inversion, end to end. The client
/// sends a query whose backend response crosses the 16 MiB physical boundary,
/// then hard-closes (RST) WITHOUT reading, so the proxy's `backend -> client`
/// forward breaks on the DESTINATION (client) write mid-response. That
/// destination-write break must attribute to the CLIENT (`ClientNetwork`) on
/// both observables — pre-fix it was hardcoded `BackendNetwork`. This binds the
/// real `forward_packet_to(source=Backend, dest=Client)` call site: reversing
/// the site's source/dest, or reverting it to a hardcoded leg, turns this red.
#[tokio::test]
async fn backend_to_client_destination_write_break_is_client_network_on_both_observables() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    // Send the large-response query but never read the reply.
    let mut query = vec![0x03_u8];
    query.extend_from_slice(b"SELECT pkt_large_response");
    client.writer.reset_sequence(0);
    let Ok(()) = client.writer.write_logical(&query, true).await else {
        unreachable!("the query reaches the proxy")
    };
    // Hard-close: reunite the split halves and set SO_LINGER=0 so the close
    // sends RST, breaking the proxy's in-flight forward-to-client write instead
    // of letting the ~16 MiB response buffer.
    let MysqlClient { reader, writer, .. } = client;
    let Ok(stream) = reader.into_inner().reunite(writer.into_inner()) else {
        unreachable!("the client's split halves reunite")
    };
    let _ = stream.set_zero_linger();
    drop(stream);

    // Log observable: the destination(client)-write break attributes to the
    // CLIENT, not the backend that was the read source.
    let closed_source = timeout(Duration::from_secs(10), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::SessionClosed { source, .. } = observation {
                return Some(source);
            }
        }
        None
    })
    .await;
    assert_eq!(
        closed_source,
        Ok(Some(QuitSource::ClientNetwork)),
        "a backend->client destination-write break is the client's network break"
    );

    // Wire observable agrees (ERROR_SOURCE_CLIENT_NETWORK == 1).
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(ControlEnvelope {
        body: Some(Body::ConnectionEvent(event)),
        ..
    }) = closed
    else {
        unreachable!("the aborted session emits a CLOSED lifecycle event")
    };
    assert_eq!(
        event.error_source, 1,
        "wire observable is ClientNetwork: {event:?}"
    );

    stack.dispatch_task.abort();
}

/// PKT-007 production wiring — a transport break while the proxy reads the
/// backend's AUTH reply is a backend NETWORK break on BOTH observables. This
/// binds `backend_read`'s sole caller (the auth relay): it previously
/// overwrote `quit_source = BackendHandshake` AFTER `backend_read` had already
/// classified the disconnect through `end_source`, re-forking
/// wire=`BackendNetwork` / log=`BackendHandshake`. Reinstating that overwrite
/// turns this red.
#[tokio::test]
async fn backend_auth_read_transport_break_is_backend_network_on_both_observables() {
    let mut stack = spawn_stack().await;
    // A backend that greets and reads the proxy's handshake response, then
    // closes before sending any auth reply — so the proxy's auth-relay
    // `backend_read` hits EOF.
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("auth-drop backend bind")
    };
    let Ok(addr) = listener.local_addr() else {
        unreachable!("auth-drop backend addr")
    };
    let backend = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (read, write) = tokio::io::split(stream);
            let mut reader = PacketReader::new(read);
            let mut writer = PacketWriter::new(write);
            let caps = fake_backend_capabilities(false);
            if !write_fake_backend_greeting(&mut writer, caps).await {
                continue;
            }
            reader.reset_sequence(writer.next_sequence());
            // Consume the proxy's handshake response, then close with no auth
            // reply.
            let _ = reader.read_logical(64 * 1024).await;
            drop((reader, writer));
        }
    });
    spawn_route_answer_to(&stack, 1, 2, addr.port());
    let _ = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port)).await;

    // Log observable: an auth-phase backend read disconnect is a backend network
    // break, NOT BackendHandshake.
    let closed_source = timeout(Duration::from_secs(10), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::SessionClosed { source, .. } = observation {
                return Some(source);
            }
        }
        None
    })
    .await;
    assert_eq!(
        closed_source,
        Ok(Some(QuitSource::BackendNetwork)),
        "an auth-phase backend read disconnect must not be reported as BackendHandshake"
    );

    // Wire observable agrees (ERROR_SOURCE_BACKEND_NETWORK == 2).
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(ControlEnvelope {
        body: Some(Body::ConnectionEvent(event)),
        ..
    }) = closed
    else {
        unreachable!("the failed auth session emits a CLOSED lifecycle event")
    };
    assert_eq!(
        event.error_source, 2,
        "wire observable is BackendNetwork: {event:?}"
    );

    backend.abort();
    stack.dispatch_task.abort();
}

/// Drain honors the transaction safe boundary end to end: an open
/// transaction (BEGIN, backend status `SERVER_STATUS_IN_TRANS`) defers
/// the graceful close; COMMIT clears it and the terminal follows.
#[tokio::test]
async fn drain_waits_for_open_transaction_commit() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("BEGIN").await);

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let closed_early = stack
        .sender
        .sent()
        .into_iter()
        .filter(|e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3))
        .count();
    assert_eq!(
        closed_early, 0,
        "an open transaction defers the drain close"
    );

    assert!(
        client.query_ok("COMMIT").await,
        "the session still serves mid-drain"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(closed.is_some(), "the drain closes at the COMMIT boundary");
    stack.dispatch_task.abort();
}

/// opt#5: a forced close during a continuous, unterminated `Continue`-packet
/// response (forwarded via the FSM-bypass path) still takes effect within the
/// force-drain deadline, and the session cleans up — on a SINGLE-THREADED
/// runtime, where the bypass loop, the FSM/SessionLoop control producer, and
/// the drain timer all share one thread. The client reads on a separate OS
/// thread so it never stops draining the proxy (no downstream backpressure),
/// and triggers the drain only after it has observed a batch of streamed rows
/// (a deterministic "bypass path is active" barrier — no sleep guessing). The
/// assertion is bounded-close within the deadline plus a generous cleanup
/// budget, not a sub-millisecond or "earlier than N rows" threshold; removing
/// the periodic yield is only a local diagnostic (Tokio's own cooperative
/// yielding can also break the tight loop), so it is not asserted here.
#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)] // client-thread + backend-stream harness
async fn continue_bypass_forced_close_bounded_on_continuous_stream() {
    let stack = spawn_stack().await;
    // Routing answer so the proxy attaches the fake backend (connection_id 1,
    // request_id 2), matching the other connection tests.
    spawn_route_answer(&stack, 1, 2);
    let port = stack.sql_port;

    // `barrier` = the deterministic ">=128 rows observed" signal (bypass path is
    // active); `done` = the reader-finished signal carrying (rows, saw_terminator)
    // sent only after the read loop ends (client saw EOF / proxy closed). Both
    // unbounded so the client's sync sends never block its runtime thread.
    let (barrier_tx, mut barrier_rx) = mpsc::unbounded_channel::<()>();
    // Outcome = (rows, saw_terminator, clean_disconnect).
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<(u64, bool, bool)>();
    let client = std::thread::spawn(move || {
        let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            let _ = done_tx.send((0, false, false));
            return;
        };
        let outcome = rt.block_on(async move {
            let Ok(Some(mut client)) =
                timeout(Duration::from_secs(5), MysqlClient::connect(port)).await
            else {
                return (0_u64, false, false);
            };
            client.writer.reset_sequence(0);
            if client
                .writer
                .write_logical(STREAM_ROWS_QUERY, true)
                .await
                .is_err()
            {
                return (0, false, false);
            }
            client.reader.reset_sequence(1);
            let mut rows: u64 = 0;
            let mut signaled = false;
            let mut saw_terminator = false;
            // Reads until the proxy closes the connection; capture the exit error
            // so we can assert it is a genuine source-side connection close (EOF /
            // reset), not a framing/size error that would mean a malformed stream.
            let exit_error = loop {
                match client.reader.read_logical(64 * 1024).await {
                    Ok(packet) => {
                        if packet.payload == [0x01, b'r'] {
                            rows += 1;
                        } else if matches!(packet.payload.first(), Some(&(0x00 | 0xfe))) {
                            // OK/EOF terminator — the streaming backend must never emit one.
                            saw_terminator = true;
                        }
                        if rows >= 128 && !signaled {
                            signaled = true;
                            let _ = barrier_tx.send(());
                        }
                    }
                    Err(error) => break error,
                }
            };
            let clean_disconnect = matches!(
                exit_error,
                PacketIoError::Io {
                    side: IoSide::Source,
                    ..
                }
            );
            (rows, saw_terminator, clean_disconnect)
        });
        // Drop the runtime (its cleanup) BEFORE signalling done, so the main
        // task's later `join()` reaps an already-finished thread.
        drop(rt);
        // Sent AFTER the read loop ends and the runtime is torn down: an
        // async-awaitable "reader finished" event.
        let _ = done_tx.send(outcome);
    });

    // Bounded wait for the bypass path to become active (no infinite hang if the
    // stream never establishes).
    let barrier = timeout(Duration::from_secs(10), barrier_rx.recv()).await;
    assert!(
        matches!(barrier, Ok(Some(()))),
        "client did not stream rows through the bypass path in time"
    );

    // Force a drain while the bypassed stream is in flight.
    stack.drain_tx.send_replace(Some(stack.drain_deadline));

    // The forced close must land within the force deadline plus a generous
    // cleanup budget (not a sub-millisecond or "earlier than N rows" threshold).
    let closed = timeout(
        stack.drain_deadline + Duration::from_secs(10),
        wait_sent(
            &stack.sender,
            |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
        ),
    )
    .await;
    assert!(
        matches!(closed, Ok(Some(_))),
        "forced close did not complete within the deadline + cleanup budget"
    );

    // Confirm the reader finished (saw EOF) with a BOUNDED ASYNC wait: this
    // yields to the runtime so the proxy can finish socket teardown, instead of
    // blocking the single current-thread runtime on `join()` (which could
    // deadlock if the proxy still needs that thread to close the socket).
    let Ok(Some((rows, saw_terminator, clean_disconnect))) = timeout(
        stack.drain_deadline + Duration::from_secs(10),
        done_rx.recv(),
    )
    .await
    else {
        unreachable!("client reader did not finish within the cleanup budget");
    };
    // The thread has finished; join is now instant and proves no leak.
    let Ok(()) = client.join() else {
        unreachable!("client thread joined");
    };
    assert!(
        !saw_terminator,
        "the continuous response must never complete cleanly"
    );
    assert!(
        clean_disconnect,
        "the reader must exit on a source-side connection close, not a framing error"
    );
    assert!(
        rows >= 128,
        "client should have streamed rows before the close"
    );
    stack.dispatch_task.abort();
}

/// Drain honors the prepared long-data guard end to end: an unfinished
/// `COM_STMT_SEND_LONG_DATA` blocks the graceful close until
/// `COM_STMT_RESET` clears the guard.
#[tokio::test]
async fn drain_waits_for_prepared_long_data_guard() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);
    assert!(client.send_long_data(7).await);
    tokio::time::sleep(Duration::from_millis(50)).await;

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let closed_early = stack
        .sender
        .sent()
        .into_iter()
        .filter(|e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3))
        .count();
    assert_eq!(
        closed_early, 0,
        "a pending long-data guard defers the drain close"
    );

    assert!(
        client.stmt_reset_ok(7).await,
        "the session still serves mid-drain"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(closed.is_some(), "the drain closes once the guard clears");
    stack.dispatch_task.abort();
}

/// SES-05: `COM_STMT_PREPARE` is served end to end (no longer the fail-closed
/// refusal). The client receives the backend's statement ID and declared
/// parameter/column counts through the prepare special response, and the
/// session keeps serving afterward. A zero-metadata prepare completes on the
/// header alone.
#[tokio::test]
async fn stmt_prepare_returns_metadata_end_to_end() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert_eq!(
        client.stmt_prepare("SELECT ? + ?").await,
        Some(PrepareOutcome::Ok {
            statement_id: 7,
            parameters: 2,
            columns: 1,
        }),
        "the prepare special response reaches the client verbatim"
    );
    assert!(
        client.query_ok("SELECT 1").await,
        "the session serves normally after a prepare"
    );
    assert_eq!(
        client.stmt_prepare("NOMETA").await,
        Some(PrepareOutcome::Ok {
            statement_id: 7,
            parameters: 0,
            columns: 0,
        }),
        "a zero-metadata prepare completes on the header alone"
    );
    client.quit().await;
    stack.dispatch_task.abort();
}

/// SES-05: a leading backend error ends the prepare immediately, is forwarded
/// verbatim to the client, and leaves the session able to serve the next
/// command (the `CompleteError` branch, never a silent teardown).
#[tokio::test]
async fn stmt_prepare_error_is_forwarded_and_session_continues() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert_eq!(
        client.stmt_prepare("FAIL SELECT").await,
        Some(PrepareOutcome::Error { code: 1064 }),
        "the backend prepare error reaches the client verbatim"
    );
    assert!(
        client.query_ok("SELECT 1").await,
        "the session serves normally after a prepare error"
    );
    client.quit().await;
    stack.dispatch_task.abort();
}

/// SES-05 (mutation-sensitive): a completed `COM_STMT_PREPARE` registers its
/// statement ID, atomically replacing any stale unknown-ID guard with a fresh
/// Idle state, and the registry sync reaches the FSM before the completion
/// boundary. An unfinished `COM_STMT_SEND_LONG_DATA` for that ID blocks the
/// drain; the prepare that returns the SAME ID clears the guard and lets the
/// drain close. Deleting the production `register` (or its sync event) leaves
/// the guard pending and this test never observes the close.
#[tokio::test]
async fn stmt_prepare_register_replaces_stale_guard_and_unblocks_drain() {
    // A long force deadline: the drain can close only at a genuine safe
    // boundary, so the close is attributable to the guard clearing (register),
    // never to the deadline preempting a still-pending guard.
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);
    // An unfinished long-data upload creates an unknown-ID guard for id 7.
    assert!(client.send_long_data(7).await);
    tokio::time::sleep(Duration::from_millis(50)).await;

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let closed_early = stack
        .sender
        .sent()
        .into_iter()
        .filter(|e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3))
        .count();
    assert_eq!(
        closed_early, 0,
        "the pending long-data guard defers the drain close"
    );

    // The prepare returns the SAME statement ID (7): register replaces the
    // stale guard with a fresh Idle state, clearing the migration boundary.
    assert_eq!(
        client.stmt_prepare("SELECT ? + ?").await,
        Some(PrepareOutcome::Ok {
            statement_id: 7,
            parameters: 2,
            columns: 1,
        }),
        "the reused statement ID is served mid-drain"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "registering the reused statement ID clears the guard and the drain closes"
    );
    stack.dispatch_task.abort();
}

/// SES-05 (mutation-sensitive, retained transaction): a prepare carries no
/// server status, so a `COM_STMT_PREPARE` inside an open transaction — whether
/// it succeeds or the backend rejects it — must NOT move the transaction
/// boundary. A drain queued after `BEGIN` stays deferred across the prepare and
/// closes only at `COMMIT`. Forcing the success branch to emit `TxnDone`
/// (`if self.in_transaction` → `if false`) or the ERR branch to stop retaining
/// the transaction closes the drain early and fails this test.
async fn prepare_inside_open_transaction_keeps_boundary(prepare_sql: &str, expect_error: bool) {
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("BEGIN").await, "the transaction opens");

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let closed_after_begin = stack
        .sender
        .sent()
        .into_iter()
        .filter(|e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3))
        .count();
    assert_eq!(
        closed_after_begin, 0,
        "the open transaction defers the drain close"
    );

    // The prepare completes inside the transaction; it must not move the boundary.
    let outcome = client.stmt_prepare(prepare_sql).await;
    if expect_error {
        assert!(
            matches!(outcome, Some(PrepareOutcome::Error { .. })),
            "prepare rejected inside the transaction: {outcome:?}"
        );
    } else {
        assert!(
            matches!(outcome, Some(PrepareOutcome::Ok { .. })),
            "prepare succeeded inside the transaction: {outcome:?}"
        );
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    let closed_after_prepare = stack
        .sender
        .sent()
        .into_iter()
        .filter(|e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3))
        .count();
    assert_eq!(
        closed_after_prepare, 0,
        "the prepare retains the open transaction; the drain stays deferred"
    );

    // Only COMMIT ends the transaction and releases the drain.
    assert!(
        client.query_ok("COMMIT").await,
        "the session still serves mid-drain"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(closed.is_some(), "the drain closes at the COMMIT boundary");
    stack.dispatch_task.abort();
}

#[tokio::test]
async fn prepare_ok_inside_open_transaction_keeps_boundary() {
    prepare_inside_open_transaction_keeps_boundary("SELECT ? + ?", false).await;
}

#[tokio::test]
async fn prepare_error_inside_open_transaction_keeps_boundary() {
    prepare_inside_open_transaction_keeps_boundary("FAIL SELECT", true).await;
}

/// SES-05 (fail-closed): a malformed backend prepare response — a prepare-OK
/// first byte followed by a truncated header — is a protocol violation. The
/// proxy must reject it in `prepare_response_rounds` and tear the session down,
/// never treat it as a complete prepare. The client sees no valid prepare
/// result and the session closes.
#[tokio::test]
async fn stmt_prepare_malformed_backend_framing_tears_down_session() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert_eq!(
        client.stmt_prepare("BADHDR").await,
        None,
        "a malformed prepare response yields no valid prepare result"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "the malformed prepare framing tears the session down"
    );
    stack.dispatch_task.abort();
}

/// Number of session-CLOSED lifecycle events (`ConnectionEvent` kind 3) the
/// control sender has observed so far.
fn closed_event_count(sender: &Arc<FakeSender>) -> usize {
    sender
        .sent()
        .into_iter()
        .filter(|e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3))
        .count()
}

/// SES-05 sub-slice 2 (P0-1, mutation-sensitive): a `COM_STMT_EXECUTE` opening a
/// read-only cursor holds the migration boundary; a fetch WITHOUT
/// `LAST_ROW_SENT` keeps it open, and only the fetch reporting `LAST_ROW_SENT`
/// clears it. Long force deadline so the close is attributable to the guard.
/// Inverting the `!LAST_ROW_SENT` test (or clearing on any fetch) closes the
/// drain one fetch too early and fails this test.
#[tokio::test]
async fn execute_cursor_blocks_drain_until_last_row_fetch() {
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert_eq!(
        client.stmt_execute(7, 0x01).await,
        Some(StmtResult::Ok),
        "the cursor execute completes"
    );

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "an open cursor defers the drain close"
    );

    assert_eq!(
        client.stmt_fetch(7).await,
        Some(StmtResult::Ok),
        "the first fetch does not report the last row"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "a non-final fetch keeps the cursor open and the drain deferred"
    );

    assert_eq!(
        client.stmt_fetch(7).await,
        Some(StmtResult::Ok),
        "the second fetch reports the last row"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "the last-row fetch clears the cursor and the drain closes"
    );
    stack.dispatch_task.abort();
}

/// SES-05 sub-slice 2 (P0-2, mutation-sensitive): cursor guards are tracked per
/// statement ID from the request payload. Two cursors (7 and 8) each block the
/// drain; closing 7 leaves 8's guard intact, and only closing 8 releases the
/// drain. Hardcoding the observed statement ID (e.g. always 7) makes the
/// execute for 8 update 7 instead, so 8 never blocks and the test fails.
#[tokio::test]
async fn execute_cursor_guard_is_statement_specific_across_ids() {
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert_eq!(client.stmt_execute(7, 0x01).await, Some(StmtResult::Ok));
    assert_eq!(client.stmt_execute(8, 0x01).await, Some(StmtResult::Ok));

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "two open cursors defer the drain close"
    );

    assert!(client.stmt_close(7).await, "close statement 7's cursor");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "statement 8's cursor still defers the drain (statement-specific)"
    );

    assert!(client.stmt_close(8).await, "close statement 8's cursor");
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "closing the last open cursor releases the drain"
    );
    stack.dispatch_task.abort();
}

/// SES-05 sub-slice 2 (control): a successful non-cursor `COM_STMT_EXECUTE`
/// CLEARS a pre-existing cursor guard (not merely idle→idle). A prior cursor on
/// 7 blocks the drain; a subsequent no-cursor execute on 7 clears it and the
/// drain closes.
#[tokio::test]
async fn non_cursor_execute_clears_prior_cursor_guard() {
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert_eq!(client.stmt_execute(7, 0x01).await, Some(StmtResult::Ok));
    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "the open cursor defers the drain"
    );

    assert_eq!(
        client.stmt_execute(7, 0x00).await,
        Some(StmtResult::Ok),
        "a non-cursor execute completes"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "the non-cursor execute clears the prior cursor guard and the drain closes"
    );
    stack.dispatch_task.abort();
}

/// SES-05 sub-slice 2 (PS-003, mutation-sensitive): a `COM_STMT_EXECUTE` that
/// the backend rejects AFTER a pending long-data upload must NOT clear the
/// guard — the ERR carries no status and never reaches the cursor path. The
/// pending guard keeps blocking the drain until an explicit RESET clears it.
/// Clearing the guard on an execute error closes the drain too early.
#[tokio::test]
async fn execute_error_after_long_data_keeps_pending_guard() {
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(
        client.send_long_data(7).await,
        "a pending long-data upload guards statement 7"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "the pending long-data guard defers the drain"
    );

    assert_eq!(
        client.stmt_execute(7, 0x80).await,
        Some(StmtResult::Error { code: 1064 }),
        "the execute is rejected by the backend"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "the rejected execute leaves the long-data guard pending (PS-003)"
    );

    assert!(
        client.stmt_reset_ok(7).await,
        "reset clears the statement's guard"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "only the explicit reset releases the drain"
    );
    stack.dispatch_task.abort();
}

/// SES-05 sub-slice 3 (PS-001/CMD-024/CMD-025, mutation-sensitive): pending
/// long-data guards are tracked independently per statement ID, and a
/// `COM_STMT_CLOSE` clears ONLY the referenced statement. Long data on 7 and 8
/// each block the drain; closing 7 leaves 8's guard intact, and only closing 8
/// releases it. Keying the guard mutation to a fixed ID makes closing 7 clear
/// 8 (or 8 never block), failing this test.
#[tokio::test]
async fn long_data_guards_are_statement_specific() {
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(
        client.send_long_data(7).await,
        "long data guards statement 7"
    );
    assert!(
        client.send_long_data(8).await,
        "long data guards statement 8"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "two pending long-data guards defer the drain"
    );

    assert!(client.stmt_close(7).await, "close statement 7");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "statement 8's long-data guard still defers the drain (statement-specific)"
    );

    assert!(client.stmt_close(8).await, "close statement 8");
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "closing the last guarded statement releases the drain"
    );
    stack.dispatch_task.abort();
}

/// SES-05 sub-slice 3 (CMD-031, mutation-sensitive): a successful
/// `COM_RESET_CONNECTION` clears ALL prepared state. A pending long-data guard
/// (7) and an open cursor (8) both defer the drain; the reset-connection OK
/// clears every guard at once and the drain closes. Making the reset's
/// clear-all a no-op leaves the guards pending and fails this test.
#[tokio::test]
async fn reset_connection_clears_all_prepared_state() {
    let stack = spawn_stack_long_drain().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(
        client.send_long_data(7).await,
        "long data guards statement 7"
    );
    assert_eq!(
        client.stmt_execute(8, 0x01).await,
        Some(StmtResult::Ok),
        "statement 8 opens a cursor"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        closed_event_count(&stack.sender),
        0,
        "a long-data guard and an open cursor defer the drain"
    );

    assert!(
        client.reset_connection_ok().await,
        "reset-connection succeeds"
    );
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(
        closed.is_some(),
        "reset-connection clears all prepared guards and the drain closes"
    );
    stack.dispatch_task.abort();
}

/// The accepted decision's namespace is the routing truth: it must
/// reach the wire on the session's CLOSED lifecycle event (and drive
/// the route conversation), not the pre-decision registration seed.
#[tokio::test]
async fn resolved_namespace_reaches_the_closed_event() {
    let stack = spawn_stack().await;
    // Decision resolves a NON-default namespace; route answers follow.
    let forwarder = Arc::clone(&stack.forwarder);
    tokio::spawn(async move {
        let decision = ControlEnvelope {
            request_id: 1,
            generation: 1,
            body: Some(Body::HandshakeDecision(HandshakeDecision {
                connection_id: 1,
                accept: true,
                retry: false,
                code: ErrorCode::Ok as i32,
                client_message: String::new(),
                namespace: "ns-wired".to_owned(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(decision.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let forwarder = Arc::clone(&stack.forwarder);
    let backend_port = stack.backend_port;
    tokio::spawn(async move {
        let assignment = ControlEnvelope {
            request_id: 2,
            generation: 1,
            body: Some(Body::RouteAssignment(RouteAssignment {
                connection_id: 1,
                assignment_id: "a-ns".to_owned(),
                backend_id: "tidb-fake".to_owned(),
                backend_address: format!("127.0.0.1:{backend_port}"),
                cluster_name: String::new(),
                keyspace: String::new(),
                healthy: true,
                local: true,
                code: ErrorCode::Ok as i32,
                detail: String::new(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(assignment.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);
    client.quit().await;

    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("the session reports CLOSED")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    assert_eq!(
        event.namespace, "ns-wired",
        "the CLOSED event carries the decision-resolved namespace"
    );
    stack.dispatch_task.abort();
}

/// The decision namespace is adopted VERBATIM: Go allows names far
/// beyond 255 bytes and multibyte characters, so any local byte-bound
/// truncation would silently rename an identity — and byte 255 of this
/// name is NOT a char boundary, so the old `truncate(255)` would panic
/// the session task outright.
#[tokio::test]
async fn long_multibyte_namespace_is_preserved_verbatim() {
    // 2 + 120×3 = 362 bytes; byte 255 splits a `界` (253 % 3 != 0).
    let long_namespace = format!("ns{}", "界".repeat(120));
    let stack = spawn_stack().await;
    let forwarder = Arc::clone(&stack.forwarder);
    let decision_namespace = long_namespace.clone();
    tokio::spawn(async move {
        let decision = ControlEnvelope {
            request_id: 1,
            generation: 1,
            body: Some(Body::HandshakeDecision(HandshakeDecision {
                connection_id: 1,
                accept: true,
                retry: false,
                code: ErrorCode::Ok as i32,
                client_message: String::new(),
                namespace: decision_namespace.clone(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(decision.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let forwarder = Arc::clone(&stack.forwarder);
    let backend_port = stack.backend_port;
    tokio::spawn(async move {
        let assignment = ControlEnvelope {
            request_id: 2,
            generation: 1,
            body: Some(Body::RouteAssignment(RouteAssignment {
                connection_id: 1,
                assignment_id: "a-long-ns".to_owned(),
                backend_id: "tidb-fake".to_owned(),
                backend_address: format!("127.0.0.1:{backend_port}"),
                cluster_name: String::new(),
                keyspace: String::new(),
                healthy: true,
                local: true,
                code: ErrorCode::Ok as i32,
                detail: String::new(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(assignment.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);
    client.quit().await;

    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("the session reports CLOSED")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    assert_eq!(
        event.namespace, long_namespace,
        "the CLOSED event preserves the full multibyte namespace verbatim"
    );
    stack.dispatch_task.abort();
}

/// Exact `MySQL` ERR packet contract (`CLIENT_PROTOCOL_41`): `0xFF` marker,
/// little-endian error code, `#` sql-state marker, 5-byte SQLSTATE,
/// and the FULL remaining message — approved vocabulary is asserted
/// verbatim, never by substring.
fn assert_exact_err_packet(payload: &[u8], code: u16, sqlstate: [u8; 5], message: &str) {
    assert_eq!(payload.first(), Some(&0xFF), "ERR marker: {payload:?}");
    assert!(payload.len() > 9, "ERR packet too short: {payload:?}");
    assert_eq!(
        u16::from_le_bytes([payload[1], payload[2]]),
        code,
        "ERR code: {payload:?}"
    );
    assert_eq!(payload[3], b'#', "sql-state marker: {payload:?}");
    assert_eq!(payload[4..9], sqlstate, "SQLSTATE: {payload:?}");
    assert_eq!(
        String::from_utf8_lossy(&payload[9..]),
        message,
        "ERR message must equal the approved vocabulary verbatim"
    );
}

/// A REJECTED handshake decision refuses the client on the wire with
/// the decision's approved message (never routes, never silently
/// hangs), and the session reports CLOSED.
#[tokio::test]
async fn rejected_handshake_decision_refuses_the_client() {
    let stack = spawn_stack().await;
    // Inject only a REJECT decision; no route answer exists at all.
    let forwarder = Arc::clone(&stack.forwarder);
    tokio::spawn(async move {
        let decision = ControlEnvelope {
            request_id: 1,
            generation: 1,
            body: Some(Body::HandshakeDecision(HandshakeDecision {
                connection_id: 1,
                accept: false,
                retry: false,
                code: ErrorCode::HandshakeRejected as i32,
                client_message: "failed to find a namespace".to_owned(),
                namespace: String::new(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(decision.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let Ok(refused) = timeout(
        Duration::from_secs(5),
        MysqlClient::login(stack.sql_port, FAKE_BACKEND_PASSWORD),
    )
    .await
    else {
        unreachable!("the refusal terminates the login promptly")
    };
    let Err(denied) = refused else {
        unreachable!("a rejected decision must refuse the client")
    };
    assert_exact_err_packet(&denied, 1105, *b"HY000", "failed to find a namespace");

    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(closed.is_some(), "the refused session reports CLOSED");
    stack.dispatch_task.abort();
}

/// A `NO_BACKEND` route answer refuses the client with Go's approved
/// vocabulary (error-parity row: `ErrProxyNoBackend` reaches the client
/// in Go mode) instead of silently dropping the connection, and the
/// session reports CLOSED.
#[tokio::test]
async fn no_backend_route_answer_refuses_the_client() {
    let mut stack = spawn_stack().await;
    // Accepted decision at request 1; the route request (2) answers a
    // terminal NO_BACKEND assignment.
    let forwarder = Arc::clone(&stack.forwarder);
    tokio::spawn(async move {
        let decision = ControlEnvelope {
            request_id: 1,
            generation: 1,
            body: Some(Body::HandshakeDecision(HandshakeDecision {
                connection_id: 1,
                accept: true,
                retry: false,
                code: ErrorCode::Ok as i32,
                client_message: String::new(),
                namespace: String::new(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(decision.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let forwarder = Arc::clone(&stack.forwarder);
    tokio::spawn(async move {
        let refusal = ControlEnvelope {
            request_id: 2,
            generation: 1,
            body: Some(Body::RouteAssignment(RouteAssignment {
                connection_id: 1,
                assignment_id: String::new(),
                backend_id: String::new(),
                backend_address: String::new(),
                cluster_name: String::new(),
                keyspace: String::new(),
                healthy: false,
                local: false,
                code: ErrorCode::NoBackend as i32,
                detail: "no available backend".to_owned(),
            })),
            ..ControlEnvelope::default()
        };
        for _ in 0..200 {
            let _ = forwarder.handle(refusal.clone()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let Ok(refused) = timeout(
        Duration::from_secs(5),
        MysqlClient::login(stack.sql_port, FAKE_BACKEND_PASSWORD),
    )
    .await
    else {
        unreachable!("the NO_BACKEND refusal terminates the login promptly")
    };
    let Err(denied) = refused else {
        unreachable!("a NO_BACKEND answer must refuse the client")
    };
    assert_exact_err_packet(
        &denied,
        1105,
        *b"HY000",
        "No available TiDB instances, please make sure TiDB is available",
    );

    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(closed.is_some(), "the refused session reports CLOSED");
    let closed_source = timeout(Duration::from_secs(5), async {
        while let Some(observation) = stack.metrics_rx.recv().await {
            if let Observation::SessionClosed { source, .. } = observation {
                return Some(source);
            }
        }
        None
    })
    .await;
    assert_eq!(closed_source, Ok(Some(QuitSource::ProxyNoBackend)));
    stack.dispatch_task.abort();
}

/// Control-plane loss must not tear down a healthy session (control-v1
/// last-good): with the dispatcher gone, established traffic continues.
#[tokio::test]
async fn control_detach_keeps_session_serving() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    stack.dispatch_task.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        client.query_ok("SELECT 5").await,
        "control detach is last-good: the session keeps serving"
    );
    client.quit().await;
}

/// The coordinated local shutdown order: stop-accept first (new
/// connections refused, existing sessions untouched), then the drain
/// signal closes idle sessions at their safe boundary — no force
/// needed for an idle session.
#[tokio::test]
async fn coordinated_shutdown_stops_accept_then_drains() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    // Phase 1: stop-accept. The live session keeps serving.
    stack.server_handle.stop_accepting();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        TcpStream::connect(("127.0.0.1", stack.sql_port))
            .await
            .is_err(),
        "new connections are refused after stop-accept"
    );
    assert!(
        client.query_ok("SELECT 2").await,
        "existing sessions keep serving through stop-accept"
    );

    // Phase 2: graceful drain. The idle session closes at its safe
    // boundary and the CLOSED lifecycle event goes out.
    stack.drain_tx.send_replace(Some(stack.drain_deadline));
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    assert!(closed.is_some(), "the drained session closes and reports");
    stack.dispatch_task.abort();
}

/// A drain whose force deadline lands while a command is in flight
/// (never a safe boundary) force-closes the session: the terminal
/// `DrainResult` counts it as force-closed and the CLOSED lifecycle
/// event carries the proxy-shutdown attribution.
#[tokio::test]
async fn drain_force_deadline_preempts_in_flight_command() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    // Start a long statement: the backend stalls for seconds, so the
    // session sits mid-command with no safe boundary.
    let slow = tokio::spawn(async move { client.query_ok("SELECT SLEEP").await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Force deadline ~now: graceful already passed, force imminent.
    let now_ms = 1_000_000_u64;
    let drain = command_envelope(
        7000,
        Body::DrainCommand(DrainCommand {
            drain_id: "d-force".to_owned(),
            listener_names: Vec::new(),
            backend_ids: Vec::new(),
            graceful_deadline_unix_millis: now_ms.saturating_sub(1_000),
            force_deadline_unix_millis: now_ms.saturating_sub(500),
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(drain).await;

    let terminal = wait_sent(&stack.sender, |e| {
        matches!(&e.body, Some(Body::DrainResult(result))
            if result.drain_id == "d-force" && result.complete)
    })
    .await;
    let Some(terminal) = terminal else {
        unreachable!("the force phase completes the drain")
    };
    let Some(Body::DrainResult(result)) = terminal.body else {
        unreachable!()
    };
    assert_eq!(result.force_closed, 1, "the in-flight session was forced");

    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("the forced session reports CLOSED")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    assert_eq!(
        event.error_source, 6,
        "timeout force-close uses the proxy-shutdown source: {event:?}"
    );
    let _ = slow.await;
    stack.dispatch_task.abort();
}

/// The force phase of a full server shutdown (session shutdown and
/// server shutdown back-to-back, exactly the executable's signal path)
/// must not abort a session owner before its terminal work: the CLOSED
/// notice with shutdown attribution still goes out mid-command.
#[tokio::test]
async fn forced_shutdown_still_emits_session_closed() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(client.query_ok("SELECT 1").await);

    // Mid-command, no safe boundary: the backend stalls for seconds.
    let slow = tokio::spawn(async move { client.query_ok("SELECT SLEEP").await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    stack.shutdown_tx.send_replace(true);
    stack.server_handle.shutdown();

    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("the forced session still reports CLOSED")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    assert_eq!(
        event.error_source, 6,
        "forced shutdown uses the proxy-shutdown source: {event:?}"
    );
    let _ = slow.await;
    stack.dispatch_task.abort();
}

/// WIRE-activation B (stalled partial inbound PROXY v2): with `proxy_protocol =
/// v2`, the client-leg probe runs after the greeting and consumes the remaining
/// absolute handshake budget. A client that opens the connection and sends only
/// a partial magic prefix, then stalls, must be closed fail-closed at the
/// handshake deadline — never routing, never dialing a backend, and never
/// hanging past the budget. This locks the production timeout seam so removing
/// the wrapper regresses.
#[tokio::test]
async fn stalled_partial_proxy_header_fails_closed_at_the_handshake_deadline() {
    let handshake_deadline = Duration::from_millis(300);
    let stack = spawn_stack_full(
        SnapshotReply::Valid,
        true,
        handshake_deadline,
        Duration::from_secs(60),
        false,
        None,
    )
    .await;

    let Ok(mut client) = TcpStream::connect(("127.0.0.1", stack.sql_port)).await else {
        unreachable!("connect to the proxy listener")
    };
    // Send only two of the four magic bytes and then stall: the probe's first
    // raw read can never complete, so it must block until the handshake budget
    // expires.
    let Ok(()) = client
        .write_all(&proxy_io::proxy_protocol::MAGIC_V2[..2])
        .await
    else {
        unreachable!("send a partial PROXY magic")
    };
    client.flush().await.ok();

    // Bounded well above the deadline: the proxy must close the socket (drain
    // the greeting bytes, then observe EOF/reset) within the handshake budget.
    let closed = timeout(handshake_deadline + Duration::from_secs(3), async {
        let mut scratch = [0_u8; 512];
        loop {
            match client.read(&mut scratch).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "a stalled partial PROXY header must close within the handshake budget, not hang"
    );

    // The probe fails before any routing or dial, so no backend was contacted.
    let Ok(transcript) = stack.backend_transcript.lock() else {
        unreachable!("transcript lock")
    };
    assert!(
        transcript.is_empty(),
        "a stalled partial PROXY header must never reach a backend"
    );
    drop(transcript);

    stack.shutdown_tx.send(true).ok();
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

// ---------------------------------------------------------------------
// WIRE-MTR Regression 1: idle-liveness probe byte counts on raw backend-in
// ---------------------------------------------------------------------

/// Runs one idle session (client authenticates but never sends a query) with a
/// short `backend_check_interval` so the liveness probe fires, and returns the
/// raw `backend_in_bytes` from the CLOSED lifecycle event.
///
/// With `send_idle_byte = false` the probe finds `WouldBlock` (alive) and the
/// session is closed from the outside; with `true` the backend pushes one
/// unsolicited raw byte after auth, which the probe consumes (reporting the
/// backend unhealthy) and — the property under test — counts on the raw
/// backend-in counter before the session tears down.
async fn idle_session_backend_in_bytes(send_idle_byte: bool) -> u64 {
    let stack = spawn_stack_full(
        SnapshotReply::Valid,
        false,
        Duration::from_secs(5),
        Duration::from_millis(100),
        send_idle_byte,
        None,
    )
    .await;
    spawn_route_answer(&stack, 1, 2);
    let Some(client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("handshake+auth completes end to end")
    };
    // The client stays idle in a probe-safe (Ready) state; the connect/auth
    // exchange is byte-identical between the two runs, so the non-probe
    // backend-in total is deterministic.
    if !send_idle_byte {
        // Baseline: nothing closes the healthy idle session on its own, so
        // close it deterministically once it has settled.
        stack.shutdown_tx.send(true).ok();
    }
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("session close emits the CLOSED event")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    drop(client);
    stack.dispatch_task.abort();
    stack.server_task.abort();
    event.backend_in_bytes
}

/// WIRE-MTR: the idle-liveness probe reads the raw backend socket beneath
/// `CountedIo` via `try_read`; when it consumes a real byte it records it on the
/// raw backend-in counter (Go parity: Go's liveness `Peek(1)` reads through the
/// counting `basicReadWriter`). A single consumed probe byte must therefore show
/// up as exactly one extra byte in the CLOSED event's `backend_in_bytes` — never
/// zero (missed) nor doubled.
#[tokio::test]
async fn idle_probe_consumed_byte_is_counted_in_raw_backend_in() {
    let baseline = idle_session_backend_in_bytes(false).await;
    let with_probe_byte = idle_session_backend_in_bytes(true).await;
    assert_eq!(
        with_probe_byte,
        baseline + 1,
        "the consumed probe byte is counted exactly once on the raw backend-in \
         counter (baseline={baseline}, probe={with_probe_byte})"
    );
}

// ---------------------------------------------------------------------
// WIRE-MTR Regression 2: production frontend-TLS totals come from the raw seam
// ---------------------------------------------------------------------

/// A CA that signs the frontend leaf and that the test client trusts.
struct TestCa {
    ca_cert_pem: String,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

/// A leaf cert/key PEM pair for the served frontend name.
struct LeafPair {
    cert_pem: String,
    key_pem: String,
}

fn make_ca() -> Option<TestCa> {
    let Ok(ca_key) = rcgen::KeyPair::generate() else {
        return None;
    };
    let Ok(mut params) = rcgen::CertificateParams::new(Vec::new()) else {
        return None;
    };
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let Ok(ca_cert) = params.self_signed(&ca_key) else {
        return None;
    };
    let ca_cert_pem = ca_cert.pem();
    let issuer = rcgen::Issuer::new(params, ca_key);
    Some(TestCa {
        ca_cert_pem,
        issuer,
    })
}

fn make_leaf(ca: &TestCa, name: &str) -> Option<LeafPair> {
    let Ok(key) = rcgen::KeyPair::generate() else {
        return None;
    };
    let Ok(params) = rcgen::CertificateParams::new(vec![name.to_owned()]) else {
        return None;
    };
    let Ok(cert) = params.signed_by(&key, &ca.issuer) else {
        return None;
    };
    Some(LeafPair {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}

/// Writes a real frontend cert/key to a unique temp directory and returns the
/// `spawn_stack_full` fixture plus the CA PEM the test client must trust. The
/// snapshot store compiles the served `ServerConfig` from these files at apply
/// time, so the files only need to exist for that call.
fn write_frontend_tls_fixture() -> Option<(FrontendTlsFixture, String)> {
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);
    let ca = make_ca()?;
    let leaf = make_leaf(&ca, "frontend.local")?;
    let identifier = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "tiproxy-dpl-frontend-tls-{}-{identifier}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).ok()?;
    let certificate_path = dir.join("frontend.crt");
    let private_key_path = dir.join("frontend.key");
    std::fs::write(&certificate_path, &leaf.cert_pem).ok()?;
    std::fs::write(&private_key_path, &leaf.key_pem).ok()?;
    let policy = control_proto::v1::TlsPolicy {
        certificate_path: certificate_path.to_str()?.to_owned(),
        private_key_path: private_key_path.to_str()?.to_owned(),
        minimum_version: "1.2".to_owned(),
        ..Default::default()
    };
    Some((
        FrontendTlsFixture {
            policy,
            cert_dir: dir,
        },
        ca.ca_cert_pem,
    ))
}

/// A rustls client config whose only trust root is the test CA.
fn client_config_trusting(ca_pem: &str) -> Option<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    let Ok(cert) = CertificateDer::from_pem_slice(ca_pem.as_bytes()) else {
        return None;
    };
    if roots.add(cert).is_err() {
        return None;
    }
    Some(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

/// Drives a raw `rustls::ClientConnection` by hand against the proxy listener:
/// reads the greeting, coalesces `[SSLRequest packet || ClientHello]` into ONE
/// `write_all` (deterministically forcing the proxy to prefetch `ClientHello`
/// bytes past the `SSLRequest` packet), then pumps the handshake to completion.
/// Returns whether the TLS handshake finished.
async fn drive_frontend_tls_handshake(port: u16, ca_pem: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)).await else {
        return false;
    };
    // Read the proxy greeting (a MySQL packet: 3-byte LE length + seq +
    // payload). It is written BY the proxy, so it does not count toward the
    // client-in total under test.
    let mut header = [0_u8; 4];
    if stream.read_exact(&mut header).await.is_err() {
        return false;
    }
    let greeting_len =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut greeting = vec![0_u8; greeting_len];
    if stream.read_exact(&mut greeting).await.is_err() {
        return false;
    }

    // Build the `SSLRequest` packet at sequence 1 (the greeting was seq 0).
    let capabilities = CapabilityFlags::PROTOCOL_41
        | CapabilityFlags::SECURE_CONNECTION
        | CapabilityFlags::PLUGIN_AUTH
        | CapabilityFlags::SSL;
    let ssl_request = encode_ssl_request(capabilities, 16 * 1024 * 1024, 45);
    let mut ssl_packet = vec![u8::try_from(ssl_request.len()).unwrap_or(0), 0, 0, 1];
    ssl_packet.extend_from_slice(&ssl_request);

    // Drive rustls by hand: extract the `ClientHello` without any socket I/O.
    let Some(config) = client_config_trusting(ca_pem) else {
        return false;
    };
    let Ok(server_name) = rustls::pki_types::ServerName::try_from("frontend.local".to_owned())
    else {
        return false;
    };
    let Ok(mut conn) = rustls::ClientConnection::new(config, server_name) else {
        return false;
    };
    let mut client_hello = Vec::new();
    while conn.wants_write() {
        if conn.write_tls(&mut client_hello).is_err() {
            return false;
        }
    }

    // Send the `SSLRequest` packet and the `ClientHello` together as the client's
    // opening flight. This does NOT force the proxy to prefetch — the exact-read
    // `SSLRequest` path leaves an empty prefix — it is just one fewer write.
    let mut coalesced = ssl_packet;
    coalesced.extend_from_slice(&client_hello);
    if stream.write_all(&coalesced).await.is_err() {
        return false;
    }
    if stream.flush().await.is_err() {
        return false;
    }

    // Pump the handshake to completion: read the proxy's TLS flight, process it,
    // and write the client's response records back, until rustls reports done.
    let mut scratch = [0_u8; 8192];
    for _ in 0..64 {
        if !conn.is_handshaking() {
            return true;
        }
        let Ok(read) = stream.read(&mut scratch).await else {
            return false;
        };
        if read == 0 {
            break;
        }
        let mut cursor = std::io::Cursor::new(&scratch[..read]);
        if conn.read_tls(&mut cursor).is_err() {
            return false;
        }
        if conn.process_new_packets().is_err() {
            return false;
        }
        let mut out = Vec::new();
        while conn.wants_write() {
            if conn.write_tls(&mut out).is_err() {
                return false;
            }
        }
        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() {
                return false;
            }
            if stream.flush().await.is_err() {
                return false;
            }
        }
    }
    !conn.is_handshaking()
}

/// Opens a real production listener and reads its protocol-10 greeting,
/// returning the still-open raw socket and advertised capability mask.
async fn read_frontend_greeting(port: u16) -> Option<(TcpStream, CapabilityFlags)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await.ok()?;
    assert_eq!(
        header[3], 0,
        "the frontend greeting starts at sequence zero"
    );
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await.ok()?;
    let greeting = parse_initial_handshake(&payload).ok()?;
    Some((stream, greeting.capabilities))
}

/// HS-001/003 production seam: the actual listener emits the selected runtime
/// identity and exact Go capability mask, then rejects an over-1-MiB declared
/// client handshake from its header plus the one byte required by the packet
/// peek. Sending no remaining payload is deliberate: the old generic draining
/// read waited for the body until the handshake deadline, whereas Go's
/// pre-read gate closes from that bounded prefix without allocating the packet
/// or contacting a backend.
#[tokio::test]
async fn frontend_greeting_and_pre_read_handshake_limit_are_production_wired() {
    let stack = spawn_stack().await;
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", stack.sql_port)).await else {
        unreachable!("frontend connect")
    };
    let mut header = [0_u8; 4];
    assert!(stream.read_exact(&mut header).await.is_ok());
    assert_eq!(header[3], 0, "the proxy greeting opens at sequence zero");
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0_u8; length];
    assert!(stream.read_exact(&mut payload).await.is_ok());
    let Ok(greeting) = parse_initial_handshake(&payload) else {
        unreachable!("production greeting parses")
    };
    assert_eq!(greeting.protocol_version, 10);
    assert_eq!(greeting.server_version, b"TiProxy-test");
    assert_eq!(greeting.connection_id, 1);
    assert_eq!(greeting.auth_plugin_data_part_1.len(), 8);
    assert_eq!(greeting.auth_plugin_data_part_2.len(), 12);
    assert!(
        greeting
            .auth_plugin_data_part_1
            .iter()
            .chain(greeting.auth_plugin_data_part_2)
            .any(|byte| *byte != 0),
        "the production salt comes from OS entropy"
    );
    assert_eq!(
        greeting.auth_plugin_name,
        Some(b"mysql_native_password".as_slice())
    );
    assert_eq!(greeting.collation, Some(45));
    assert_eq!(greeting.status, Some(StatusFlags::from_bits_retain(0)));
    assert_eq!(
        greeting.capabilities,
        SUPPORTED_SERVER_CAPABILITIES.without(CapabilityFlags::SSL),
        "the production greeting must use the pinned Go capability mask"
    );

    let declared = mysql_wire::limits::MAX_PRE_HANDSHAKE_PACKET_LEN + 1;
    let declared = u32::try_from(declared).unwrap_or(u32::MAX).to_le_bytes();
    assert!(
        stream
            .write_all(&[declared[0], declared[1], declared[2], 1])
            .await
            .is_ok()
    );
    assert!(stream.write_all(&[0]).await.is_ok());
    assert!(stream.flush().await.is_ok());
    let mut reader = PacketReader::new(stream);
    reader.reset_sequence(1);
    let observed = timeout(Duration::from_secs(1), reader.read_logical(64 * 1024)).await;
    let error = match observed {
        Ok(Ok(error)) => error,
        other => unreachable!("oversized handshake response: {other:?}"),
    };
    assert_exact_err_packet(
        &error.payload,
        1153,
        *b"08S01",
        "Got a packet bigger than 'max_allowed_packet' bytes",
    );
    assert_eq!(
        stack.backend_accepted_connections.load(Ordering::Relaxed),
        0,
        "an oversized frontend handshake must fail before backend routing"
    );

    stack.shutdown_tx.send(true).ok();
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

/// HS-002 production seam: a frontend that omits `PROTOCOL_41` receives the
/// fixed Go error and is rejected before the route/backend path can run.
#[tokio::test]
async fn frontend_missing_protocol41_is_rejected_before_backend_routing() {
    let stack = spawn_stack().await;
    let Some((stream, _)) = read_frontend_greeting(stack.sql_port).await else {
        unreachable!("frontend greeting")
    };
    let capabilities = CapabilityFlags::LONG_PASSWORD
        | CapabilityFlags::SECURE_CONNECTION
        | CapabilityFlags::PLUGIN_AUTH;
    let Ok(response) = encode_handshake_response(HandshakeResponseParams {
        capabilities,
        max_packet_size: 16 * 1024 * 1024,
        collation: 45,
        username: b"legacy-pre-41",
        auth_response: b"",
        database: None,
        auth_plugin_name: Some(b"mysql_native_password"),
        attributes: None,
        zstd_level: None,
    }) else {
        unreachable!("pre-4.1 response encodes")
    };
    let (read, write) = stream.into_split();
    let mut reader = PacketReader::new(read);
    let mut writer = PacketWriter::new(write);
    writer.reset_sequence(1);
    assert!(writer.write_logical(&response, true).await.is_ok());
    reader.reset_sequence(writer.next_sequence());
    let observed = timeout(Duration::from_secs(1), reader.read_logical(64 * 1024)).await;
    let error = match observed {
        Ok(Ok(error)) => error,
        other => unreachable!("missing PROTOCOL_41 response: {other:?}"),
    };
    assert_exact_err_packet(
        &error.payload,
        1251,
        *b"08004",
        "Client does not support authentication protocol requested by server; consider upgrading MySQL client",
    );
    assert_eq!(
        stack.backend_accepted_connections.load(Ordering::Relaxed),
        0,
        "capability rejection must precede backend routing"
    );

    stack.shutdown_tx.send(true).ok();
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

/// HS-002 production seam: Go tolerates legacy drivers that implement plugin
/// auth but omit `CLIENT_PLUGIN_AUTH`. The live engine must force that bit after
/// parsing the legacy layout and still complete the backend auth-switch flow.
#[tokio::test]
async fn frontend_without_plugin_auth_bit_authenticates_end_to_end() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Ok(stream) = TcpStream::connect(("127.0.0.1", stack.sql_port)).await else {
        unreachable!("frontend connect")
    };
    let (read, write) = stream.into_split();
    let mut reader = PacketReader::new(read);
    let mut writer = PacketWriter::new(write);
    let Ok(greeting) = reader.read_logical(64 * 1024).await else {
        unreachable!("frontend greeting")
    };
    let Ok(greeting) = parse_initial_handshake(&greeting.payload) else {
        unreachable!("frontend greeting parses")
    };
    let mut proxy_salt = greeting.auth_plugin_data_part_1.to_vec();
    proxy_salt.extend_from_slice(greeting.auth_plugin_data_part_2);
    let capabilities = CapabilityFlags::PROTOCOL_41
        | CapabilityFlags::LONG_PASSWORD
        | CapabilityFlags::SECURE_CONNECTION
        | CapabilityFlags::CONNECT_ATTRS
        | CapabilityFlags::DEPRECATE_EOF;
    let attributes = [Attribute {
        key: b"program_name",
        value: b"legacy-no-plugin-bit",
    }];
    let scramble = native_scramble(FAKE_BACKEND_PASSWORD, &proxy_salt);
    let Ok(response) = encode_handshake_response(HandshakeResponseParams {
        capabilities,
        max_packet_size: 16 * 1024 * 1024,
        collation: 45,
        username: b"root",
        auth_response: &scramble,
        database: None,
        auth_plugin_name: None,
        attributes: Some(&attributes),
        zstd_level: None,
    }) else {
        unreachable!("legacy response encodes")
    };
    writer.reset_sequence(reader.expected_sequence());
    assert!(writer.write_logical(&response, true).await.is_ok());
    reader.reset_sequence(writer.next_sequence());
    let Ok(Ok(switch)) = timeout(Duration::from_secs(2), reader.read_logical(64 * 1024)).await
    else {
        unreachable!("backend auth switch reaches the legacy client")
    };
    assert_eq!(switch.payload.first(), Some(&0xFE));
    let switch_data = &switch.payload[1..];
    let Some(nul) = switch_data.iter().position(|byte| *byte == 0) else {
        unreachable!("terminated native plugin")
    };
    assert_eq!(&switch_data[..nul], b"mysql_native_password");
    let backend_salt = switch_data[nul + 1..]
        .strip_suffix(&[0])
        .unwrap_or(&switch_data[nul + 1..]);
    writer.reset_sequence(reader.expected_sequence());
    assert!(
        writer
            .write_logical(&native_scramble(FAKE_BACKEND_PASSWORD, backend_salt), true)
            .await
            .is_ok()
    );
    reader.reset_sequence(writer.next_sequence());
    let Ok(Ok(ok)) = timeout(Duration::from_secs(2), reader.read_logical(64 * 1024)).await else {
        unreachable!("legacy client authentication completes")
    };
    assert_eq!(ok.payload.first(), Some(&0x00));

    stack.shutdown_tx.send(true).ok();
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

/// HS-002/011 production seam: the real backend path parses the greeting and
/// rejects both a missing negotiated capability and malformed protocol bytes.
/// Parser unit tests exhaust every truncation prefix; these cases prove the
/// parser/verifier are the code reached after an actual routed dial.
#[tokio::test]
async fn backend_greeting_parser_and_required_capability_gate_are_production_wired() {
    let broad = fake_backend_capabilities(false);
    let salt = [5_u8; 20];
    let missing_required = encode_initial_handshake(build_greeting(
        broad.without(CapabilityFlags::DEPRECATE_EOF),
        &salt,
        b"8.0.11-missing-eof",
        91,
        45,
        StatusFlags::from_bits_retain(0x0002),
    ))
    .unwrap_or_else(|error| unreachable!("valid limited greeting: {error}"));
    for (greeting, expected_message) in [
        (
            missing_required,
            "Verify TiDB capability failed, please upgrade TiDB",
        ),
        (
            vec![9, 0],
            "TiProxy fails to connect to TiDB, please make sure TiDB is available",
        ),
    ] {
        let (backend_port, accepted) = spawn_one_greeting_backend(greeting).await;
        let mut stack = spawn_stack().await;
        spawn_route_answer_to(&stack, 1, 2, backend_port);
        let Ok(Err(denied)) = timeout(
            Duration::from_secs(2),
            MysqlClient::login(stack.sql_port, FAKE_BACKEND_PASSWORD),
        )
        .await
        else {
            unreachable!("an invalid backend greeting must fail closed")
        };
        assert_exact_err_packet(&denied, 1105, *b"HY000", expected_message);
        assert_eq!(accepted.load(Ordering::Relaxed), 1);
        assert_eq!(
            stack.backend_accepted_connections.load(Ordering::Relaxed),
            0,
            "the assignment must use only the deliberately invalid backend"
        );
        let closed_source = timeout(Duration::from_secs(5), async {
            while let Some(observation) = stack.metrics_rx.recv().await {
                if let Observation::SessionClosed { source, .. } = observation {
                    return Some(source);
                }
            }
            None
        })
        .await;
        assert_eq!(closed_source, Ok(Some(QuitSource::BackendHandshake)));
        stack.shutdown_tx.send(true).ok();
        stack.dispatch_task.abort();
        stack.server_task.abort();
    }
}

/// HS-008 production error translation for required backend TLS: a backend
/// that omits SSL receives the exact `ErrBackendNoTLS` response, while one
/// that advertises SSL but cannot complete the upgrade receives Go's
/// `ErrBackendPPV2` response. Both retain the backend-handshake quit label.
#[tokio::test]
async fn backend_tls_failures_use_go_client_errors_and_source_label() {
    let broad = fake_backend_capabilities(false);
    for (capabilities, expected_message) in [
        (
            broad,
            "Require TLS enabled on TiDB when require-backend-tls=true",
        ),
        (
            broad.union(CapabilityFlags::SSL),
            "TiProxy fails to connect to TiDB, please make sure TiDB proxy-protocol is set correctly. If this error still exists, please contact PingCAP",
        ),
    ] {
        let greeting = encode_initial_handshake(build_greeting(
            capabilities,
            &[5_u8; 20],
            b"8.0.11-tls-failure",
            92,
            45,
            StatusFlags::from_bits_retain(0x0002),
        ))
        .unwrap_or_else(|error| unreachable!("valid TLS-case greeting: {error}"));
        let (backend_port, accepted) = spawn_one_greeting_backend(greeting).await;
        let mut stack = spawn_tls_stack(SnapshotReply::Valid).await;
        spawn_route_answer_to(&stack, 1, 2, backend_port);

        let Ok(Err(denied)) = timeout(
            Duration::from_secs(2),
            MysqlClient::login(stack.sql_port, FAKE_BACKEND_PASSWORD),
        )
        .await
        else {
            unreachable!("a required backend TLS failure must refuse the client")
        };
        assert_exact_err_packet(&denied, 1105, *b"HY000", expected_message);
        assert_eq!(accepted.load(Ordering::Relaxed), 1);

        let closed_source = timeout(Duration::from_secs(5), async {
            while let Some(observation) = stack.metrics_rx.recv().await {
                if let Observation::SessionClosed { source, .. } = observation {
                    return Some(source);
                }
            }
            None
        })
        .await;
        assert_eq!(closed_source, Ok(Some(QuitSource::BackendHandshake)));
        stack.shutdown_tx.send(true).ok();
        stack.dispatch_task.abort();
        stack.server_task.abort();
    }
}

/// HS-005 production seam: after a caching-SHA2 fast-success marker, the
/// backend may send OK immediately. The client deliberately sends nothing
/// between those packets; completion therefore proves the engine stayed on the
/// backend turn instead of asking for an erroneous extra client packet.
#[tokio::test]
async fn caching_sha2_fast_auth_needs_no_extra_client_packet() {
    let (backend_port, accepted) = spawn_fast_auth_backend().await;
    let stack = spawn_stack().await;
    spawn_route_answer_to(&stack, 1, 2, backend_port);
    let Ok(stream) = TcpStream::connect(("127.0.0.1", stack.sql_port)).await else {
        unreachable!("frontend connect")
    };
    let (read, write) = stream.into_split();
    let mut reader = PacketReader::new(read);
    let mut writer = PacketWriter::new(write);
    let Ok(greeting) = reader.read_logical(64 * 1024).await else {
        unreachable!("frontend greeting")
    };
    let Ok(greeting) = parse_initial_handshake(&greeting.payload) else {
        unreachable!("frontend greeting parses")
    };
    let mut proxy_salt = greeting.auth_plugin_data_part_1.to_vec();
    proxy_salt.extend_from_slice(greeting.auth_plugin_data_part_2);
    let capabilities = CapabilityFlags::PROTOCOL_41
        | CapabilityFlags::LONG_PASSWORD
        | CapabilityFlags::SECURE_CONNECTION
        | CapabilityFlags::PLUGIN_AUTH
        | CapabilityFlags::DEPRECATE_EOF;
    let scramble = native_scramble(FAKE_BACKEND_PASSWORD, &proxy_salt);
    let Ok(response) = encode_handshake_response(HandshakeResponseParams {
        capabilities,
        max_packet_size: 16 * 1024 * 1024,
        collation: 45,
        username: b"root",
        auth_response: &scramble,
        database: None,
        auth_plugin_name: Some(b"mysql_native_password"),
        attributes: None,
        zstd_level: None,
    }) else {
        unreachable!("frontend response encodes")
    };
    writer.reset_sequence(reader.expected_sequence());
    assert!(writer.write_logical(&response, true).await.is_ok());
    reader.reset_sequence(writer.next_sequence());
    let Ok(Ok(switch)) = timeout(Duration::from_secs(2), reader.read_logical(64 * 1024)).await
    else {
        unreachable!("caching-SHA2 switch reaches the client")
    };
    assert!(switch.payload.starts_with(b"\xfe"));
    assert!(
        switch.payload[1..].starts_with(b"caching_sha2_password\0"),
        "the production relay preserves the backend plugin switch"
    );
    writer.reset_sequence(reader.expected_sequence());
    assert!(writer.write_logical(b"sha2-response", true).await.is_ok());
    reader.reset_sequence(writer.next_sequence());
    let Ok(Ok(fast)) = timeout(Duration::from_secs(2), reader.read_logical(64 * 1024)).await else {
        unreachable!("fast-auth success reaches the client")
    };
    assert_eq!(fast.payload, [0x01, 0x03]);
    let Ok(Ok(ok)) = timeout(Duration::from_secs(1), reader.read_logical(64 * 1024)).await else {
        unreachable!("OK follows without another client packet")
    };
    assert_eq!(ok.payload.first(), Some(&0x00));
    assert_eq!(accepted.load(Ordering::Relaxed), 1);

    stack.shutdown_tx.send(true).ok();
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

/// Writes one sequence-1 packet and reports whether the production listener
/// closes without sending plaintext after rejecting it.
async fn rejected_frontend_packet_closes(mut stream: TcpStream, payload: &[u8]) -> bool {
    let Ok(length) = u32::try_from(payload.len()) else {
        return false;
    };
    let length = length.to_le_bytes();
    let header = [length[0], length[1], length[2], 1];
    if stream.write_all(&header).await.is_err()
        || stream.write_all(payload).await.is_err()
        || stream.flush().await.is_err()
    {
        return false;
    }
    let mut byte = [0_u8; 1];
    matches!(
        timeout(Duration::from_secs(2), stream.read(&mut byte)).await,
        Ok(Ok(0))
    )
}

/// WIRE-04 production seam: the SSL capability follows the live frontend TLS
/// snapshot, and an unsupported or non-exact `SSLRequest` fails closed before
/// routing, backend contact, credentials, or any plaintext fallback.
#[tokio::test]
async fn frontend_tls_capability_and_ssl_request_shape_fail_closed() {
    let requested = CapabilityFlags::PROTOCOL_41
        | CapabilityFlags::SECURE_CONNECTION
        | CapabilityFlags::PLUGIN_AUTH
        | CapabilityFlags::SSL;

    // No compiled frontend TLS policy: SSL is absent from the greeting, and a
    // client that sends an exact SSLRequest anyway is rejected before routing.
    let plain = spawn_stack().await;
    let Some((stream, advertised)) = read_frontend_greeting(plain.sql_port).await else {
        unreachable!("plain frontend greeting")
    };
    assert!(!advertised.contains(CapabilityFlags::SSL));
    let ssl_request = encode_ssl_request(requested, 16 * 1024 * 1024, 45);
    assert!(rejected_frontend_packet_closes(stream, &ssl_request).await);
    assert_eq!(
        plain.backend_accepted_connections.load(Ordering::Relaxed),
        0,
        "an unsupported SSLRequest must not dial a backend"
    );
    plain.shutdown_tx.send(true).ok();
    plain.dispatch_task.abort();
    plain.server_task.abort();

    // A compiled policy advertises SSL, but only the protocol's exact 32-byte
    // SSLRequest shape can cross the TLS upgrade boundary.
    let Some((fixture, _ca_pem)) = write_frontend_tls_fixture() else {
        unreachable!("frontend TLS fixture")
    };
    let tls = spawn_stack_full(
        SnapshotReply::Valid,
        false,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        Some(fixture),
    )
    .await;
    let Some((stream, advertised)) = read_frontend_greeting(tls.sql_port).await else {
        unreachable!("TLS frontend greeting")
    };
    assert!(advertised.contains(CapabilityFlags::SSL));
    let mut malformed = encode_ssl_request(requested, 16 * 1024 * 1024, 45).to_vec();
    malformed.push(0);
    assert_eq!(malformed.len(), 33);
    assert!(rejected_frontend_packet_closes(stream, &malformed).await);
    assert_eq!(
        tls.backend_accepted_connections.load(Ordering::Relaxed),
        0,
        "a malformed SSLRequest must not dial a backend"
    );
    tls.shutdown_tx.send(true).ok();
    tls.dispatch_task.abort();
    tls.server_task.abort();
}

/// WIRE-MTR (production TLS path): under a REAL engine frontend-TLS session,
/// `Engine::totals()` / CLOSED client bytes come from the innermost raw
/// `CountedIo` (reflecting TLS record + handshake bytes), NOT the `PacketIo`
/// framing layer.
///
/// The client sends a plaintext `SSLRequest`, then completes a real TLS
/// handshake against the proxy's served certificate — driving `rustls` by hand
/// so the test needs no `TlsConnector` dependency — and drops; the proxy's next
/// in-TLS read hits EOF and the session closes, emitting CLOSED with the totals.
///
/// This row is discriminating for the totals SOURCE: rewiring client totals back
/// to `PacketIo::in_bytes`, or resetting the raw counter handle across the
/// upgrade, collapses `client_in_bytes` to the tiny framing count (only the
/// ~36-byte `SSLRequest` is framed before the upgrade), so the `> 150` bound
/// fails.
///
/// It does NOT (and cannot) exercise prefix replay: the production `SSLRequest`
/// path reads exactly (4-byte header + 32-byte payload), so `into_upgrade_parts`
/// hands the upgrade an EMPTY prefix regardless of how the client coalesces its
/// bytes on the wire. Prefix-replay exactly-once accounting is proven separately
/// and deterministically by `proxy-io`'s
/// `tls_matrix::raw_counter_is_not_double_counted_by_prefix_replay`.
#[tokio::test]
async fn frontend_tls_session_totals_come_from_the_raw_seam_not_framing() {
    let Some((fixture, ca_pem)) = write_frontend_tls_fixture() else {
        unreachable!("frontend TLS fixture is written to disk")
    };
    let stack = spawn_stack_full(
        SnapshotReply::Valid,
        false,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        Some(fixture),
    )
    .await;

    // The session fails during the TLS handshake phase (before routing), so no
    // route answer or backend contact is needed; the CLOSED event still carries
    // the raw totals captured at engine exit.
    let handshake_ok = timeout(
        Duration::from_secs(10),
        drive_frontend_tls_handshake(stack.sql_port, &ca_pem),
    )
    .await;
    // A completed handshake proves the proxy served real TLS over the counted
    // socket, so the raw counter observed the full TLS flight.
    assert!(
        matches!(handshake_ok, Ok(true)),
        "the frontend TLS handshake completes against the proxy's served cert: \
         {handshake_ok:?}"
    );

    // Drop the client: the proxy's subsequent in-TLS handshake-response read
    // observes EOF and tears the session down, emitting CLOSED with the raw
    // totals.
    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("the frontend-TLS session reports CLOSED")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    // Raw client-in carries the full TLS ClientHello + handshake records
    // (hundreds of bytes). A framing-layer count (`PacketIo::in_bytes`) would
    // have seen only the ~36-byte SSLRequest packet, so this wide lower bound is
    // exactly what fails if client totals are misrouted to the framing layer.
    assert!(
        event.client_in_bytes > 150,
        "raw client-in must reflect the TLS ClientHello + handshake records, not \
         the ~36-byte SSLRequest a framing count would see: {event:?}"
    );
    stack.shutdown_tx.send(true).ok();
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

// ---------------------------------------------------------------------
// WIRE-C: compressed-client end-to-end regression
// ---------------------------------------------------------------------

/// Maps a compression codec error into a transport `io::Error`, mirroring
/// `dataplane::transport`'s private helper so the packet layer's direction hooks
/// can fail closed.
fn compression_io_error(error: CompressionError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

/// Test-only socket transport that reproduces the production compressed client
/// and backend variants' [`DirectionSync`] delegation.
///
/// `PacketIo<T>` requires `T: DirectionSync`, but `CompressedIo`'s inherent
/// `begin_read`/`begin_write` SHADOW (rather than implement) that trait, so a
/// bare `PacketIo<CompressedIo<_>>` cannot compile. The production
/// `dataplane::transport::{ClientTransport, BackendTransport}` — which do the
/// same forwarding — live in a private module, so this integration test cannot
/// name them. This wrapper forwards to the SAME `CompressedIo` codec and
/// `PacketIo` direction hooks the proxy drives, without touching `src/`. It
/// layers compression over the same innermost `CountedIo` production uses.
struct CompressedTestTransport {
    inner: CompressedIo<CountedIo<TcpStream>>,
}

impl AsyncRead for CompressedTestTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(context, buf)
    }
}

impl AsyncWrite for CompressedTestTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(context, data)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

impl DirectionSync for CompressedTestTransport {
    fn begin_read(&mut self) -> std::io::Result<Option<u8>> {
        self.inner.begin_read().map_err(compression_io_error)
    }

    fn begin_write(&mut self) -> std::io::Result<Option<u8>> {
        self.inner.begin_write().map_err(compression_io_error)
    }

    fn reset_layer_sequence(&mut self) -> std::io::Result<()> {
        self.inner.reset_sequence().map_err(compression_io_error)
    }

    fn is_layered(&self) -> bool {
        true
    }
}

/// A compressed `MySQL` client: negotiates `COMPRESS`/`ZSTD` during the plaintext
/// connection phase, then — at the clean auth-OK boundary — reunites the socket
/// and activates `MySQL` compressed framing, exactly the client leg the proxy
/// switches to at that same boundary.
struct CompressedClient {
    io: PacketIo<CompressedTestTransport>,
    capabilities: CapabilityFlags,
}

impl CompressedClient {
    /// Runs the full connection phase (identical to [`MysqlClient::login`] but
    /// advertising the compression capability, plus the zstd level byte for the
    /// zstd case), then wraps the reunited socket in compressed framing matching
    /// the negotiated `algorithm`.
    async fn connect(port: u16, algorithm: CompressionAlgorithm) -> Option<Self> {
        let (compression_capabilities, zstd_level) = match algorithm {
            CompressionAlgorithm::Zlib => (CapabilityFlags::COMPRESS, None),
            CompressionAlgorithm::Zstd { level } => (
                CapabilityFlags::ZSTD_COMPRESSION_ALGORITHM,
                Some(u8::try_from(level).ok()?),
            ),
        };
        Self::connect_advertising(port, algorithm, compression_capabilities, zstd_level).await
    }

    /// Connects with an explicit compression capability mask. This permits the
    /// Go-compatible mixed-leg shape where the client advertises BOTH flags:
    /// zlib wins on the client leg, while a zstd-only backend independently
    /// intersects that mask and activates zstd at `zstd_level`.
    async fn connect_advertising(
        port: u16,
        algorithm: CompressionAlgorithm,
        compression_capabilities: CapabilityFlags,
        zstd_level: Option<u8>,
    ) -> Option<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
        let (read, write) = stream.into_split();
        let mut reader = PacketReader::new(read);
        let mut writer = PacketWriter::new(write);
        // Greeting at sequence zero, then the strict lockstep of the connection
        // phase, exactly like the plaintext client.
        let preview = reader.peek_packet().await.ok()?;
        assert_eq!(preview.sequence_id, 0);
        let greeting = reader.read_logical(64 * 1024).await.ok()?;
        let parsed = parse_initial_handshake(&greeting.payload).ok()?;
        let mut proxy_salt = parsed.auth_plugin_data_part_1.to_vec();
        proxy_salt.extend_from_slice(parsed.auth_plugin_data_part_2);
        // Negotiate compression on top of the base client capabilities; the proxy
        // advertises both COMPRESS and ZSTD, so either bit negotiates.
        let capabilities = CapabilityFlags::PROTOCOL_41
            | CapabilityFlags::LONG_PASSWORD
            | CapabilityFlags::SECURE_CONNECTION
            | CapabilityFlags::PLUGIN_AUTH
            | CapabilityFlags::CONNECT_ATTRS
            | CapabilityFlags::DEPRECATE_EOF
            | compression_capabilities;
        let initial_attributes = [Attribute {
            key: b"program_name",
            value: b"initial-client",
        }];
        let first_scramble = native_scramble(FAKE_BACKEND_PASSWORD, &proxy_salt);
        let response = encode_handshake_response(HandshakeResponseParams {
            capabilities,
            max_packet_size: 16 * 1024 * 1024,
            collation: 45,
            username: b"root",
            auth_response: &first_scramble,
            database: None,
            auth_plugin_name: Some(b"mysql_native_password"),
            attributes: Some(&initial_attributes),
            zstd_level,
        })
        .ok()?;
        writer.reset_sequence(reader.expected_sequence());
        writer.write_logical(&response, true).await.ok()?;
        let expected_switch_sequence = writer.next_sequence();
        reader.reset_sequence(expected_switch_sequence);
        let switch = reader.read_logical(64 * 1024).await.ok()?;
        // The backend re-requests authentication against its own salt.
        if switch.payload.first() != Some(&0xFE) {
            return None;
        }
        let data = &switch.payload[1..];
        let nul = data.iter().position(|&byte| byte == 0)?;
        assert_eq!(&data[..nul], b"mysql_native_password");
        let mut backend_salt = &data[nul + 1..];
        if backend_salt.last() == Some(&0) {
            backend_salt = &backend_salt[..backend_salt.len() - 1];
        }
        writer.reset_sequence(reader.expected_sequence());
        writer
            .write_logical(&native_scramble(FAKE_BACKEND_PASSWORD, backend_salt), true)
            .await
            .ok()?;
        let expected_result_sequence = writer.next_sequence();
        reader.reset_sequence(expected_result_sequence);
        let outcome = reader.read_logical(64 * 1024).await.ok()?;
        if outcome.payload.first() != Some(&0x00) {
            return None;
        }
        // The auth-OK packet is fully read at a clean command boundary — the
        // reader holds no prefetched bytes — so the split halves reunite into a
        // whole socket that we wrap in compressed framing, mirroring the proxy's
        // in-place activation of its client leg at this exact boundary.
        let read_half = reader.into_inner();
        let write_half = writer.into_inner();
        let stream = read_half.reunite(write_half).ok()?;
        let compressed = CompressedIo::new(
            CountedIo::new(stream),
            algorithm,
            CompressionLimits::default(),
        )
        .ok()?;
        Some(Self {
            io: PacketIo::new(CompressedTestTransport { inner: compressed }),
            capabilities,
        })
    }

    /// Runs one compressed `COM_QUERY` and reports whether the response is a
    /// successful (non-error) result, mirroring [`MysqlClient::query_ok`] but
    /// over compressed framing: the compressed sequence resets once per command
    /// and the `DirectionSync` hooks slave the uncompressed sequence to it.
    async fn query_ok(&mut self, sql: &str) -> bool {
        let mut payload = vec![0x03_u8];
        payload.extend_from_slice(sql.as_bytes());
        if self.io.get_mut().reset_layer_sequence().is_err() {
            return false;
        }
        self.io.reset_read_sequence(0);
        if self.io.write_logical(&payload, true).await.is_err() {
            return false;
        }
        match self.io.read_logical(64 * 1024).await {
            Ok(response) => response.payload.first() == Some(&0x00),
            Err(_) => false,
        }
    }

    /// Reads one OK-shaped logical payload that crosses the 16-MiB physical
    /// packet boundary and returns the number of physical packets consumed.
    async fn query_large_ok(&mut self, sql: &str, expected_len: usize) -> Option<(u64, u8)> {
        let mut payload = vec![0x03_u8];
        payload.extend_from_slice(sql.as_bytes());
        self.io.get_mut().reset_layer_sequence().ok()?;
        self.io.reset_read_sequence(0);
        self.io.write_logical(&payload, true).await.ok()?;
        let before = self.io.in_packets();
        let response = self.io.read_logical(expected_len).await.ok()?;
        if response.payload.len() != expected_len
            || response.payload.first() != Some(&0x00)
            || response.payload.last() != Some(&b'L')
        {
            return None;
        }
        Some((
            self.io.in_packets().checked_sub(before)?,
            self.compressed_sequence(),
        ))
    }

    fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.io.get_ref().inner.codec().algorithm()
    }

    fn compressed_sequence(&self) -> u8 {
        self.io.get_ref().inner.codec().sequence()
    }

    /// Runs the full compressed LOCAL INFILE duplex: query, backend request,
    /// multiple client chunks, empty terminator, and final backend OK.
    async fn local_infile(&mut self, sql: &str, chunks: &[&[u8]]) -> bool {
        let mut payload = vec![0x03_u8];
        payload.extend_from_slice(sql.as_bytes());
        if self.io.get_mut().reset_layer_sequence().is_err() {
            return false;
        }
        self.io.reset_read_sequence(0);
        if self.io.write_logical(&payload, true).await.is_err() {
            return false;
        }
        let Ok(request) = self.io.read_logical(64 * 1024).await else {
            return false;
        };
        if request.payload != b"\xfbwire03.csv" {
            return false;
        }
        for chunk in chunks {
            if self.io.write_logical(chunk, false).await.is_err() {
                return false;
            }
        }
        if self.io.write_logical(&[], true).await.is_err() {
            return false;
        }
        match self.io.read_logical(64 * 1024).await {
            Ok(response) => response.payload.first() == Some(&0x00),
            Err(_) => false,
        }
    }

    /// Multi-round change-user over the negotiated compressed transport.
    /// There is one layered reset at command entry only; every later
    /// backend/client direction reversal is governed by the production-equivalent
    /// `DirectionSync` hooks on `PacketIo`.
    async fn change_user(
        &mut self,
        username: &[u8],
        database: &[u8],
        original_auth_response: &[u8],
        attributes: &[Attribute<'_>],
    ) -> Result<(), u16> {
        let request = encode_change_user(
            ChangeUserParams {
                username,
                auth_response: original_auth_response,
                database,
                character_set: Some(45),
                auth_plugin_name: Some(b"mysql_native_password"),
                attributes: Some(attributes),
            },
            self.capabilities,
        )
        .map_err(|_| u16::MAX)?;
        self.io.reset_layer_sequence().map_err(|_| u16::MAX)?;
        self.io.reset_read_sequence(0);
        self.io
            .write_logical(&request, true)
            .await
            .map_err(|_| u16::MAX)?;

        loop {
            let response = self
                .io
                .read_logical(64 * 1024)
                .await
                .map_err(|_| u16::MAX)?;
            match response.payload.first() {
                Some(0x00) => return Ok(()),
                Some(0xFF) if response.payload.len() >= 3 => {
                    return Err(u16::from_le_bytes([
                        response.payload[1],
                        response.payload[2],
                    ]));
                }
                Some(0xFE) => {
                    let data = &response.payload[1..];
                    let nul = data.iter().position(|&byte| byte == 0).ok_or(u16::MAX)?;
                    assert_eq!(&data[..nul], b"mysql_native_password");
                    let mut salt = &data[nul + 1..];
                    if salt.last() == Some(&0) {
                        salt = &salt[..salt.len() - 1];
                    }
                    self.io
                        .write_logical(&native_scramble(FAKE_BACKEND_PASSWORD, salt), true)
                        .await
                        .map_err(|_| u16::MAX)?;
                }
                _ => {
                    self.io
                        .write_logical(b"second-auth-response", true)
                        .await
                        .map_err(|_| u16::MAX)?;
                }
            }
        }
    }

    /// Stages `bytes` as ONE complete compressed frame and flushes, writing raw
    /// bytes straight to the compressed transport (bypassing `write_logical`) so a
    /// single frame can carry only PART of the next `MySQL` command packet — the
    /// deterministic trigger for the proxy's idle `peek_packet` to decode-and-stage
    /// a partial command. `fresh` resets the shared compressed sequence to zero
    /// first (a new command boundary); a continuation frame passes `fresh = false`.
    async fn stage_raw_frame(&mut self, bytes: &[u8], fresh: bool) -> bool {
        let transport = self.io.get_mut();
        if fresh && transport.reset_layer_sequence().is_err() {
            return false;
        }
        if transport.begin_write().is_err() {
            return false;
        }
        if transport.write_all(bytes).await.is_err() {
            return false;
        }
        transport.flush().await.is_ok()
    }

    /// Runs one compressed `COM_STMT_PREPARE` and reads the whole special
    /// response over compressed framing, mirroring [`MysqlClient::stmt_prepare`]:
    /// the compressed sequence resets once for the command and the multi-packet
    /// metadata stream is decoded through the compressed transport.
    async fn stmt_prepare(&mut self, sql: &str) -> Option<PrepareOutcome> {
        let mut payload = vec![0x16_u8];
        payload.extend_from_slice(sql.as_bytes());
        if self.io.get_mut().reset_layer_sequence().is_err() {
            return None;
        }
        self.io.reset_read_sequence(0);
        if self.io.write_logical(&payload, true).await.is_err() {
            return None;
        }
        let header = self.io.read_logical(64 * 1024).await.ok()?;
        match header.payload.first() {
            Some(&0xFF) => {
                let code = u16::from_le_bytes([*header.payload.get(1)?, *header.payload.get(2)?]);
                Some(PrepareOutcome::Error { code })
            }
            Some(&0x00) => {
                let statement_id = u32::from_le_bytes([
                    *header.payload.get(1)?,
                    *header.payload.get(2)?,
                    *header.payload.get(3)?,
                    *header.payload.get(4)?,
                ]);
                let columns =
                    u16::from_le_bytes([*header.payload.get(5)?, *header.payload.get(6)?]);
                let parameters =
                    u16::from_le_bytes([*header.payload.get(7)?, *header.payload.get(8)?]);
                for _ in 0..(u32::from(parameters) + u32::from(columns)) {
                    self.io.read_logical(64 * 1024).await.ok()?;
                }
                Some(PrepareOutcome::Ok {
                    statement_id,
                    parameters,
                    columns,
                })
            }
            _ => None,
        }
    }

    async fn quit(mut self) {
        let _ = self.io.get_mut().reset_layer_sequence();
        self.io.reset_read_sequence(0);
        let _ = self.io.write_logical(&[0x01], true).await;
    }
}

/// Full compressed-client lifecycle over real sockets: negotiate compression,
/// authenticate, run two compressed queries on the same session (proving the
/// per-command compressed-sequence reset), then quit. The fake backend never
/// advertises COMPRESS, so the proxy<->backend leg stays plaintext — a valid
/// mixed scenario — and the CLOSED event still shows real backend traffic.
async fn compressed_client_roundtrips(algorithm: CompressionAlgorithm) {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        CompressedClient::connect(stack.sql_port, algorithm),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("compressed handshake+auth completes end to end for {algorithm:?}")
    };
    assert!(
        client.query_ok("SELECT 1").await,
        "first compressed query round-trips ({algorithm:?})"
    );
    assert!(
        client.query_ok("SELECT 2").await,
        "second compressed query reuses the session and resets the compressed \
         sequence ({algorithm:?})"
    );
    client.quit().await;

    let closed = wait_sent(
        &stack.sender,
        |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
    )
    .await;
    let Some(closed) = closed else {
        unreachable!("the compressed session close emits the CLOSED event ({algorithm:?})")
    };
    let Some(Body::ConnectionEvent(event)) = closed.body else {
        unreachable!()
    };
    // The proxy<->backend leg carried real plaintext bytes in both directions.
    assert!(
        event.backend_in_bytes > 0,
        "the plaintext backend leg moved bytes ({algorithm:?}): {event:?}"
    );
    assert!(
        event.backend_out_bytes > 0,
        "the plaintext backend leg moved bytes ({algorithm:?}): {event:?}"
    );
    stack.dispatch_task.abort();
}

async fn mixed_zlib_client_zstd_backend_stack() -> (
    Stack,
    Arc<Mutex<Vec<Vec<u8>>>>,
    Arc<Mutex<Vec<CompressionAlgorithm>>>,
) {
    let stack = spawn_stack().await;
    let backend_algorithm = CompressionAlgorithm::Zstd { level: 3 };
    let (backend_port, transcript, _, active_algorithms) =
        spawn_fake_compressed_backend_server(SnapshotReply::Valid, backend_algorithm, false).await;
    let topology = engine_snapshot(stack.sql_port, backend_port, false, None, false);
    assert!(stack.server_handle.update_snapshot(topology).is_ok());
    spawn_route_answer_to(&stack, 1, 2, backend_port);
    (stack, transcript, active_algorithms)
}

async fn connect_mixed_zlib_client(port: u16) -> Option<CompressedClient> {
    CompressedClient::connect_advertising(
        port,
        CompressionAlgorithm::Zlib,
        CapabilityFlags::COMPRESS | CapabilityFlags::ZSTD_COMPRESSION_ALGORITHM,
        Some(3),
    )
    .await
}

/// A client that negotiates classic zlib `COMPRESS` round-trips two compressed
/// commands end to end. This exercises the full production sequence bridge
/// (`CompressedIo` codec + `PacketIo` `DirectionSync` hooks) over real sockets.
#[tokio::test]
async fn compressed_client_zlib_roundtrips_end_to_end() {
    compressed_client_roundtrips(CompressionAlgorithm::Zlib).await;
}

/// A client that negotiates `ZSTD` at a concrete level round-trips two
/// compressed commands end to end.
///
/// This is the mixed-leg scenario: the client negotiates ZSTD while the fake
/// backend advertises no compression, so the client<->proxy leg is zstd and the
/// proxy<->backend leg is plaintext. It also guards a fixed regression — the
/// backend handshake-forward must carry `zstd_level` only when the backend leg
/// negotiated ZSTD, otherwise `encode_handshake_response` rejects the packet.
#[tokio::test]
async fn compressed_client_zstd_roundtrips_end_to_end() {
    compressed_client_roundtrips(CompressionAlgorithm::Zstd { level: 3 }).await;
}

/// The client advertises both compression flags, so Go precedence selects zlib
/// on the client leg. The selected backend advertises only zstd, so the backend
/// leg independently intersects the same negotiated mask and selects zstd.
/// Two commands prove both codecs stay live across a command reset.
#[tokio::test]
async fn compressed_mixed_zlib_client_zstd_backend_roundtrips() {
    let (stack, transcript, backend_active_algorithms) =
        mixed_zlib_client_zstd_backend_stack().await;
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        connect_mixed_zlib_client(stack.sql_port),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("mixed zlib-client/zstd-backend session established")
    };
    assert_eq!(client.compression_algorithm(), CompressionAlgorithm::Zlib);
    assert!(client.query_ok("SELECT mixed_one").await);
    assert_eq!(
        backend_active_algorithms
            .lock()
            .map_or_else(|_| Vec::new(), |algorithms| algorithms.clone()),
        [CompressionAlgorithm::Zstd { level: 3 }],
        "the accepted backend connection activated zstd level 3"
    );
    assert_eq!(client.compressed_sequence(), 2);
    assert!(client.query_ok("SELECT mixed_two").await);
    assert_eq!(
        client.compressed_sequence(),
        2,
        "the second command reset the shared read/write sequence to zero"
    );
    client.quit().await;

    let commands = transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert_eq!(
        &commands[..2],
        [
            b"\x03SELECT mixed_one".as_slice(),
            b"\x03SELECT mixed_two".as_slice(),
        ]
    );
    stack.dispatch_task.abort();
}

/// One compressed logical response crosses `MySQL`'s 16-MiB physical-packet
/// boundary. The client must consume exactly two physical packets and reset
/// both layered sequences cleanly for the immediately following command.
#[tokio::test]
async fn compressed_large_logical_response_splits_and_realigns() {
    let (stack, transcript, _) = mixed_zlib_client_zstd_backend_stack().await;
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        connect_mixed_zlib_client(stack.sql_port),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("mixed compressed session established for large response")
    };
    assert_eq!(
        client
            .query_large_ok("SELECT wire03_large_response", WIRE03_LARGE_RESPONSE_LEN)
            .await,
        Some((2, 3)),
        "the >16-MiB logical response is two MySQL packets and two response frames"
    );
    assert_eq!(
        client
            .query_large_ok(
                "SELECT wire03_exact_multiple_response",
                WIRE03_EXACT_MULTIPLE_RESPONSE_LEN,
            )
            .await,
        Some((2, 3)),
        "the exact MaxPayloadLen response adds the zero-length physical terminator"
    );
    assert!(
        client.query_ok("SELECT after_large_response").await,
        "the next compressed command starts at a clean sequence boundary"
    );
    assert_eq!(client.compressed_sequence(), 2);
    client.quit().await;

    let commands = transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert_eq!(commands[0], WIRE03_LARGE_QUERY);
    assert_eq!(commands[1], WIRE03_EXACT_MULTIPLE_QUERY);
    assert_eq!(commands[2], b"\x03SELECT after_large_response");
    stack.dispatch_task.abort();
}

/// LOCAL INFILE reverses direction twice inside one compressed command. Two
/// chunks and the empty terminator share the upload direction; the final OK
/// returns on the same layered sequence, then the next command resets to zero.
#[tokio::test]
async fn compressed_local_infile_multi_turn_realigns() {
    let (stack, transcript, _) = mixed_zlib_client_zstd_backend_stack().await;
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        connect_mixed_zlib_client(stack.sql_port),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("mixed compressed session established for LOCAL INFILE")
    };
    let chunks = [b"1,alpha\n".as_slice(), b"2,beta\n".as_slice()];
    assert!(
        client
            .local_infile("LOAD DATA LOCAL INFILE 'wire03.csv'", &chunks)
            .await
    );
    assert_eq!(
        client.compressed_sequence(),
        4,
        "query, infile request, upload frame, and final OK share one sequence"
    );
    assert!(
        client.query_ok("SELECT after_local_infile").await,
        "the next compressed command starts at a clean sequence boundary"
    );
    assert_eq!(client.compressed_sequence(), 2);
    client.quit().await;

    let commands = transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    assert_eq!(commands[0], WIRE03_LOCAL_INFILE_QUERY);
    assert_eq!(commands[1], chunks[0]);
    assert_eq!(commands[2], chunks[1]);
    assert!(
        commands[3].is_empty(),
        "the upload ends with one empty packet"
    );
    assert_eq!(commands[4], b"\x03SELECT after_local_infile");
    stack.dispatch_task.abort();
}

/// SES-05 over compression: a `COM_STMT_PREPARE` special response rides the
/// client<->proxy compressed leg intact. The proxy reads the multi-packet
/// prepare-OK metadata from the plaintext backend and re-frames it through the
/// compressed transport in `prepare_response_rounds`; the client decodes the
/// statement ID and counts, and the session keeps serving compressed commands
/// (proving the compressed sequence realigned across the prepare's many
/// packets).
async fn compressed_stmt_prepare_roundtrips(algorithm: CompressionAlgorithm) {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        CompressedClient::connect(stack.sql_port, algorithm),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("compressed handshake+auth completes end to end for {algorithm:?}")
    };
    assert_eq!(
        client.stmt_prepare("SELECT ? + ?").await,
        Some(PrepareOutcome::Ok {
            statement_id: 7,
            parameters: 2,
            columns: 1,
        }),
        "the compressed prepare special response round-trips ({algorithm:?})"
    );
    assert!(
        client.query_ok("SELECT 1").await,
        "the compressed session keeps serving after a prepare ({algorithm:?})"
    );
    client.quit().await;
    stack.dispatch_task.abort();
}

#[tokio::test]
async fn compressed_stmt_prepare_zlib_roundtrips_end_to_end() {
    compressed_stmt_prepare_roundtrips(CompressionAlgorithm::Zlib).await;
}

#[tokio::test]
async fn compressed_stmt_prepare_zstd_roundtrips_end_to_end() {
    compressed_stmt_prepare_roundtrips(CompressionAlgorithm::Zstd { level: 3 }).await;
}

/// A zstd frontend performs two backend/client auth direction reversals inside
/// one change-user exchange, consumes the final OK, then starts a fresh query.
/// Any relay-internal compressed reset or bypass of `PacketIo`'s direction
/// hooks makes the strict sequence fail before the next query.
#[tokio::test]
async fn compressed_zstd_change_user_multi_round_stays_aligned() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        CompressedClient::connect(stack.sql_port, CompressionAlgorithm::Zstd { level: 3 }),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("compressed zstd session established")
    };
    let attributes = [Attribute {
        key: b"program_name",
        value: b"zstd-change-user",
    }];
    assert_eq!(
        client
            .change_user(
                b"zstd_user",
                b"zstd_db",
                b"zstd-original-scramble",
                &attributes,
            )
            .await,
        Ok(())
    );
    assert!(
        client.query_ok("SELECT after_zstd_change_user").await,
        "next command resets the compressed exchange exactly once"
    );
    let commands = stack
        .backend_transcript
        .lock()
        .map_or_else(|_| Vec::new(), |commands| commands.clone());
    let Some(rewritten) = commands
        .iter()
        .find(|payload| payload.first() == Some(&0x11))
    else {
        unreachable!("rewritten request is recorded: {commands:?}")
    };
    let Ok(parsed) = parse_change_user(rewritten, client.capabilities) else {
        unreachable!("rewritten compressed request parses")
    };
    assert_eq!(parsed.username, b"zstd_user");
    assert_eq!(parsed.auth_response, b"");
    assert_eq!(
        parsed.auth_plugin_name,
        Some(b"auth_unknown_plugin".as_slice())
    );
    assert!(commands.iter().all(|payload| {
        !payload
            .windows(b"zstd-original-scramble".len())
            .any(|window| window == b"zstd-original-scramble")
    }));
    client.quit().await;
    stack.dispatch_task.abort();
}

/// Contract #1 cancel-safety over COMPRESSION, exercising the real
/// `Engine::command_phase` (the compressed sibling of
/// `control_activity_during_fragmented_command_keeps_wire_intact`): a control
/// command winning the engine's idle select while a compressed frame carrying
/// only PART of the next command is already peeked-and-staged must NOT rewind the
/// shared compressed sequence. The engine's `just_served_control` guard skips the
/// once-per-command compressed reset on control re-entry, so the staged bytes
/// survive and the command still round-trips.
///
/// DETERMINISTIC trigger: the client sends a complete compressed frame carrying
/// only the first 2 bytes of the next command's `MySQL` packet. The proxy's
/// `peek_packet` decodes that frame (compressed sequence -> 1; 2 decoded bytes
/// staged in the packet prefetch) yet returns Pending (it needs the 5-byte
/// header), then the redirect control command wins the idle select.
///
/// DISCRIMINABILITY (verified manually by deleting the guard): without
/// `just_served_control`, the control re-entry calls `PacketIo::reset_layer_sequence`
/// while the 2 staged bytes sit in the read prefetch -> it fails closed -> the
/// session tears down -> the deadline-bounded response read below never completes.
#[tokio::test]
async fn compressed_control_interleave_during_staged_command_keeps_wire_intact() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(
        Duration::from_secs(5),
        CompressedClient::connect(stack.sql_port, CompressionAlgorithm::Zlib),
    )
    .await
    .ok()
    .flatten() else {
        unreachable!("compressed session established")
    };
    // One normal compressed query reaches a clean command boundary.
    assert!(client.query_ok("SELECT 1").await);

    // Build the next command and send ONLY its first 2 bytes as one complete
    // compressed frame: the proxy peeks + stages a partial command (2 decoded
    // bytes in the packet prefetch) yet cannot form the 5-byte header.
    let payload = b"\x03SELECT 6";
    let mut packet = vec![u8::try_from(payload.len()).unwrap_or(0), 0, 0, 0];
    packet.extend_from_slice(payload);
    assert!(
        client.stage_raw_frame(&packet[..2], true).await,
        "the partial-command compressed frame is staged"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A redirect control command wins the engine's idle select while the frame
    // is staged; control is served (RedirectResult emitted) mid-command.
    let redirect = command_envelope(
        6002,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "r-cfrag".to_owned(),
            backend_id: "tidb-other".to_owned(),
            backend_address: "127.0.0.1:1".to_owned(),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: false,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    let refused = wait_sent(&stack.sender, |e| {
        matches!(&e.body, Some(Body::RedirectResult(result)) if result.redirect_id == "r-cfrag")
    })
    .await;
    assert!(
        refused.is_some(),
        "control served while the frame is staged"
    );

    // Complete the command as a second compressed frame: the staged bytes must
    // not have been rewound. A desynced engine never answers, so the response
    // read is deadline-bounded.
    assert!(
        client.stage_raw_frame(&packet[2..], false).await,
        "the command completes as a continuation compressed frame"
    );
    let Ok(Ok(response)) = timeout(Duration::from_secs(5), client.io.read_logical(64 * 1024)).await
    else {
        unreachable!("the staged compressed command still round-trips at the correct sequence")
    };
    assert_eq!(response.payload.first(), Some(&0x00));
    client.quit().await;
    stack.dispatch_task.abort();
}

// ---------------------------------------------------------------------
// WIRE-C: deterministic compressed-backend snapshot sequence regressions
// ---------------------------------------------------------------------
//
// The real-socket zlib/zstd migration rows above prove candidate negotiation,
// restore, swap, PROXY ordering, and raw accounting. These in-memory rows remain
// as narrow mutation-sensitive checks of the lower-level reset seam: the
// backend compressed layer resets before a proxy-owned query, starts a fresh
// compressed seq-0 exchange, stays aligned for the next command, and rejects a
// reset while decoded bytes are staged.

/// In-memory duplex byte transport modeling the backend leg's socket: reads
/// drain `input` (the backend's scripted compressed responses), writes append to
/// `output` (the proxy's compressed requests), over one shared sequence.
struct BackendDuplex {
    input: Vec<u8>,
    input_pos: usize,
    output: Vec<u8>,
}

impl AsyncRead for BackendDuplex {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let available = self.input.len().saturating_sub(self.input_pos);
        let take = available.min(buf.remaining());
        let end = self.input_pos + take;
        let chunk = self.input[self.input_pos..end].to_vec();
        buf.put_slice(&chunk);
        self.input_pos = end;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for BackendDuplex {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.output.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Test-only `DirectionSync` newtype over `CompressedIo<BackendDuplex>`, matching
/// the production `BackendTransport::Compressed` variant's hook delegation.
struct BackendLegTransport {
    inner: CompressedIo<BackendDuplex>,
}

impl AsyncRead for BackendLegTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(context, buf)
    }
}

impl AsyncWrite for BackendLegTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(context, data)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

impl DirectionSync for BackendLegTransport {
    fn begin_read(&mut self) -> std::io::Result<Option<u8>> {
        self.inner.begin_read().map_err(compression_io_error)
    }

    fn begin_write(&mut self) -> std::io::Result<Option<u8>> {
        self.inner.begin_write().map_err(compression_io_error)
    }

    fn reset_layer_sequence(&mut self) -> std::io::Result<()> {
        self.inner.reset_sequence().map_err(compression_io_error)
    }

    fn is_layered(&self) -> bool {
        true
    }
}

/// One `MySQL` physical packet carrying `payload` at physical sequence `seq`.
fn model_mysql_packet(payload: &[u8], seq: u8) -> Vec<u8> {
    let length = u32::try_from(payload.len()).unwrap_or(0).to_le_bytes();
    let mut packet = Vec::with_capacity(4 + payload.len());
    packet.extend_from_slice(&length[..3]);
    packet.push(seq);
    packet.extend_from_slice(payload);
    packet
}

/// A raw (uncompressed-body) compressed frame at compressed `sequence` carrying
/// one `MySQL` physical packet — the backend's scripted response wire.
fn model_raw_frame(packet: &[u8], sequence: u8) -> Vec<u8> {
    let Ok(header) = CompressedFrameHeader::new(packet.len(), sequence, 0) else {
        unreachable!("a small model frame fits the 24-bit compressed field")
    };
    let mut frame = header.encode().to_vec();
    frame.extend_from_slice(packet);
    frame
}

/// Builds a `PacketIo` over a compressed backend leg whose socket replays the
/// given raw response bytes.
fn backend_leg(input: Vec<u8>) -> Option<PacketIo<BackendLegTransport>> {
    let compressed = CompressedIo::new(
        BackendDuplex {
            input,
            input_pos: 0,
            output: Vec::new(),
        },
        CompressionAlgorithm::Zlib,
        CompressionLimits::default(),
    )
    .ok()?;
    Some(PacketIo::new(BackendLegTransport { inner: compressed }))
}

/// The compressed sequence byte of the frame starting at `offset` in the proxy's
/// captured backend-leg output.
fn output_frame_sequence(io: &PacketIo<BackendLegTransport>, offset: usize) -> Option<u8> {
    let output = &io.get_ref().inner.get_ref().output;
    let frame = output.get(offset..)?;
    CompressedFrameHeader::decode(frame)
        .ok()
        .map(CompressedFrameHeader::sequence)
}

/// PacketIo-level model of fix #3 on the BACKEND leg: the proxy-owned `SHOW
/// SESSION_STATES` captured for a redirect resets the backend compressed layer
/// first, so it starts a fresh compressed seq-0 exchange rather than carrying
/// the sequence over from the last user command; the old session then stays
/// aligned (also seq 0) for its next user command. A staged/in-flight backend
/// layer fails that reset closed instead of rewinding over live bytes.
#[tokio::test]
async fn compressed_backend_snapshot_reset_starts_fresh_and_keeps_session_aligned() {
    // OK and snapshot responses both answer at compressed sequence 1 (the shared
    // sequence continues from the request's seq 0 -> read at 1).
    let ok = model_mysql_packet(b"\x00\x00\x00\x02\x00\x00\x00", 1);
    let snapshot = model_mysql_packet(b"\x00row-bytes-preserved", 1);
    let mut input = model_raw_frame(&ok, 1);
    input.extend_from_slice(&model_raw_frame(&snapshot, 1));
    let Some(mut io) = backend_leg(input) else {
        unreachable!("the compressed backend leg builds")
    };

    // --- User command (command_phase parity): reset once, request at seq 0. ---
    let Ok(()) = io.get_mut().reset_layer_sequence() else {
        unreachable!("the clean per-command reset succeeds")
    };
    io.reset_write_sequence(0);
    io.reset_read_sequence(1);
    let user_start = io.get_ref().inner.get_ref().output.len();
    let Ok(()) = io.write_logical(b"\x03SELECT user_cmd", true).await else {
        unreachable!("the user command writes")
    };
    assert_eq!(
        output_frame_sequence(&io, user_start),
        Some(0),
        "the user command opens the exchange at compressed sequence 0"
    );
    let Ok(response) = io.read_logical(64 * 1024).await else {
        unreachable!("the user command's OK response reads back")
    };
    assert_eq!(response.payload.first(), Some(&0x00));

    // --- Snapshot capture (capture_migration_snapshot parity): reset the ---
    // backend layer, then the internal query starts a FRESH seq-0 exchange.
    let Ok(()) = io.get_mut().reset_layer_sequence() else {
        unreachable!("the snapshot boundary is clean, so its reset succeeds")
    };
    io.reset_write_sequence(0);
    io.reset_read_sequence(1);
    let snapshot_start = io.get_ref().inner.get_ref().output.len();
    let Ok(()) = io.write_logical(b"\x03SHOW SESSION_STATES", true).await else {
        unreachable!("the internal snapshot query writes")
    };
    // Without the pre-query reset the shared sequence would still be 2 here; the
    // reset is what makes the proxy-owned query a fresh compressed seq-0 frame.
    assert_eq!(
        output_frame_sequence(&io, snapshot_start),
        Some(0),
        "the proxy-owned SHOW SESSION_STATES opens at a fresh compressed sequence 0"
    );
    let Ok(snapshot_response) = io.read_logical(64 * 1024).await else {
        unreachable!("the snapshot response reads back")
    };
    assert_eq!(snapshot_response.payload.first(), Some(&0x00));

    // --- Old session stays aligned: the next user command resets to seq 0. ---
    let Ok(()) = io.get_mut().reset_layer_sequence() else {
        unreachable!("the post-snapshot per-command reset succeeds")
    };
    io.reset_write_sequence(0);
    io.reset_read_sequence(1);
    let next_start = io.get_ref().inner.get_ref().output.len();
    let Ok(()) = io.write_logical(b"\x03SELECT after_snapshot", true).await else {
        unreachable!("the next user command writes")
    };
    assert_eq!(
        output_frame_sequence(&io, next_start),
        Some(0),
        "the old session's next command stays aligned at compressed sequence 0"
    );
}

/// PacketIo-level model of the fail-closed guard at the snapshot boundary: if the
/// backend compressed layer still holds an in-flight (peeked/staged) frame, the
/// snapshot's `reset_layer_sequence` fails closed rather than silently rewinding
/// the shared sequence over live command bytes.
#[tokio::test]
async fn compressed_backend_snapshot_reset_fails_closed_on_staged_frame() {
    // A next command's frame is present at compressed sequence 0.
    let staged = model_mysql_packet(b"\x03SELECT staged", 0);
    let Some(mut io) = backend_leg(model_raw_frame(&staged, 0)) else {
        unreachable!("the compressed backend leg builds")
    };
    // The idle peek decodes and stages that frame (codec sequence -> 1).
    let Ok(preview) = io.peek_packet().await else {
        unreachable!("the staged frame peeks")
    };
    assert_eq!(preview.first_byte, Some(0x03));
    // The snapshot boundary reset now fails closed on the staged decoded bytes.
    let error = io.get_mut().reset_layer_sequence();
    let Some(error) = error.err() else {
        unreachable!("the reset must fail closed on staged data")
    };
    let wrapped = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<CompressionError>());
    assert!(matches!(
        wrapped,
        Some(CompressionError::ResetWithBufferedData { .. })
    ));
}

/// The production `send_proxy_owned_query` seam resets the compressed layer to
/// sequence zero before sending, so a proxy-owned query starts a FRESH exchange
/// even when the shared compressed sequence carried over non-zero from the last
/// user command. Production `capture_migration_snapshot` calls this exact helper,
/// so this locks the migration snapshot's fresh-exchange invariant.
///
/// DISCRIMINABILITY: deleting the `io.reset_layer_sequence()` line inside
/// `send_proxy_owned_query` would leave the sent frame at the stale non-zero
/// compressed sequence, and the `Some(0)` assertion below would fail.
#[tokio::test]
async fn send_proxy_owned_query_opens_at_fresh_compressed_sequence_zero() {
    // A prior user command's response, so we can run one real exchange first and
    // advance the shared compressed sequence off zero.
    let prior_ok = model_mysql_packet(b"\x00\x00\x00\x02\x00\x00\x00", 1);
    let Some(mut io) = backend_leg(model_raw_frame(&prior_ok, 1)) else {
        unreachable!("the compressed backend leg builds")
    };

    // --- Prior user command: advances the shared compressed sequence off zero.
    let Ok(()) = io.get_mut().reset_layer_sequence() else {
        unreachable!("the clean per-command reset succeeds")
    };
    io.reset_write_sequence(0);
    io.reset_read_sequence(1);
    let Ok(()) = io.write_logical(b"\x03SELECT prior_cmd", true).await else {
        unreachable!("the prior user command writes")
    };
    let Ok(response) = io.read_logical(64 * 1024).await else {
        unreachable!("the prior command's OK response reads back")
    };
    assert_eq!(response.payload.first(), Some(&0x00));
    assert_ne!(
        io.get_ref().inner.codec().sequence(),
        0,
        "the prior exchange left the shared compressed sequence non-zero"
    );

    // --- Proxy-owned query via the production seam: it resets the layered +
    // packet sequence to zero, then sends — mirroring capture_migration_snapshot.
    let owned_start = io.get_ref().inner.get_ref().output.len();
    let Ok(()) = send_proxy_owned_query(&mut io, b"\x03SHOW SESSION_STATES").await else {
        unreachable!("the proxy-owned query resets and sends on a clean boundary")
    };
    assert_eq!(
        output_frame_sequence(&io, owned_start),
        Some(0),
        "send_proxy_owned_query opens the exchange at a fresh compressed sequence 0"
    );
}

/// COMMIT's OK permits the client to send immediately, including while the
/// following redirect handshake is still queued or in flight.
#[tokio::test]
async fn query_immediately_after_commit_survives_redirect() {
    let stack = spawn_stack().await;
    let (target_port, target_transcript, _) = spawn_fake_backend_server(SnapshotReply::Valid).await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = MysqlClient::connect(stack.sql_port).await else {
        unreachable!("session established")
    };
    assert!(client.query_ok("BEGIN").await);
    let redirect = command_envelope(
        7010,
        Body::RedirectCommand(RedirectCommand {
            connection_id: 1,
            redirect_id: "commit-next-command".to_owned(),
            backend_id: "tidb-target".to_owned(),
            backend_address: format!("127.0.0.1:{target_port}"),
            cluster_name: String::new(),
            keyspace: String::new(),
            backend_unhealthy: false,
            backend_local: false,
            deadline_unix_millis: 0,
            command_sequence: 1,
        }),
    );
    let _ = stack.forwarder.handle(redirect).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(client.query_ok("COMMIT").await);
    assert!(
        matches!(
            timeout(
                Duration::from_secs(2),
                client.query_ok("SELECT next_after_commit")
            )
            .await,
            Ok(true)
        ),
        "next query lost its forwarding ACK while redirect was pending"
    );
    let result = wait_sent(&stack.sender, |envelope| {
        matches!(&envelope.body, Some(Body::RedirectResult(result)) if result.redirect_id == "commit-next-command" && result.succeeded)
    }).await;
    assert!(result.is_some());
    assert_eq!(
        target_transcript.lock().map_or(0, |commands| commands
            .iter()
            .filter(|p| p.as_slice() == b"\x03SELECT next_after_commit")
            .count()),
        1
    );
    assert!(stack.backend_transcript.lock().is_ok_and(|commands| {
        commands
            .iter()
            .all(|p| p.as_slice() != b"\x03SELECT next_after_commit")
    }));
    drop(client);
    let _ = stack.shutdown_tx.send(true);
    let _ = timeout(Duration::from_secs(5), stack.server_task).await;
    stack.dispatch_task.abort();
}

/// PKT-003 request payload: larger than `COMMAND_PAYLOAD_LIMIT`, the 64 MiB
/// ceiling the engine is willing to materialize. A proxy that reads the
/// request before forwarding it cannot deliver this at all, which is what
/// makes the property observable from outside.
const STREAMED_QUERY_BYTES: u64 = 64 * 1024 * 1024 + 1024;

/// A backend that drains one oversized command without materializing it
/// either, reporting the exact logical length it received and the head of the
/// payload. Draining into a sink through `forward_packet_to` keeps the test
/// harness from becoming the thing that allocates 64 MiB.
async fn spawn_streaming_fake_backend() -> (u16, Arc<AtomicU64>, Arc<Mutex<Vec<u8>>>) {
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let received = Arc::new(AtomicU64::new(0));
    let head = Arc::new(Mutex::new(Vec::new()));
    let server_received = Arc::clone(&received);
    let server_head = Arc::clone(&head);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let broad = fake_backend_capabilities(false);
            let (read, write) = tokio::io::split(stream);
            let mut reader = PacketReader::new(read);
            let mut writer = PacketWriter::new(write);
            if fake_backend_auth(&mut reader, &mut writer, broad)
                .await
                .is_none()
            {
                continue;
            }
            loop {
                reader.reset_sequence(0);
                match reader.peek_packet().await {
                    Ok(preview) if preview.sequence_id == 0 => {}
                    _ => break,
                }
                // Oversized commands are drained; anything small enough to
                // hold is read normally so the follow-up query still works.
                let first_byte = {
                    let mut sink = PacketWriter::new(tokio::io::sink());
                    let Ok(progress) = reader.forward_packet_to(&mut sink, 64).await else {
                        break;
                    };
                    let logical = progress.logical_payload_bytes();
                    if logical >= STREAMED_QUERY_BYTES {
                        server_received.store(logical, Ordering::Relaxed);
                        if let Ok(mut head) = server_head.lock() {
                            *head = progress.captured_prefix().to_vec();
                        }
                    }
                    progress.captured_prefix().first().copied()
                };
                if first_byte == Some(0x01) {
                    break; // COM_QUIT
                }
                writer.reset_sequence(reader.expected_sequence());
                let Ok(ok) = encode_ok_packet(
                    ResponseHeader::OK,
                    0,
                    0,
                    StatusFlags::AUTOCOMMIT,
                    0,
                    b"",
                    broad,
                ) else {
                    break;
                };
                if writer.write_logical(&ok, true).await.is_err() {
                    break;
                }
            }
        }
    });
    (address.port(), received, head)
}

/// PKT-003: a command whose first physical packet is already maximal is
/// relayed to the backend as it arrives, never held.
///
/// The request is deliberately larger than `COMMAND_PAYLOAD_LIMIT`: under the
/// materializing path `read_logical(COMMAND_PAYLOAD_LIMIT)` refuses it and the
/// session dies, so a passing run is itself the evidence that no copy was
/// made. Both ends stream — the client writes from a generator and the backend
/// drains into a sink — so the only participant that could have allocated 64
/// MiB is the proxy.
#[tokio::test]
async fn oversized_command_streams_to_the_backend_without_being_materialized() {
    let (backend_port, received, head) = spawn_streaming_fake_backend().await;
    let stack = spawn_stack().await;
    spawn_route_answer_to(&stack, 1, 2, backend_port);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        stack.dispatch_task.abort();
        unreachable!("client connect")
    };

    // COM_QUERY followed by filler. `write_logical_from` fragments it into
    // maximal physical packets, so the FIRST one already reaches
    // `MAX_PAYLOAD_LEN` — the exact condition the streaming path keys on.
    let mut source = (&b"\x03"[..]).chain(tokio::io::repeat(b'x').take(STREAMED_QUERY_BYTES - 1));
    client.writer.reset_sequence(0);
    assert!(
        timeout(
            Duration::from_secs(60),
            client
                .writer
                .write_logical_from(&mut source, STREAMED_QUERY_BYTES, true),
        )
        .await
        .is_ok_and(|written| written.is_ok()),
        "the oversized command must reach the proxy"
    );

    client.reader.reset_sequence(1);
    let response = timeout(
        Duration::from_secs(60),
        client.reader.read_logical(64 * 1024),
    )
    .await;
    assert!(
        response
            .is_ok_and(|packet| packet.is_ok_and(|packet| packet.payload.first() == Some(&0x00))),
        "the backend's OK must come back to the client"
    );

    assert_eq!(
        received.load(Ordering::Relaxed),
        STREAMED_QUERY_BYTES,
        "every byte of the logical request must reach the backend"
    );
    let captured = head.lock().map(|head| head.clone()).unwrap_or_default();
    assert_eq!(captured.first(), Some(&0x03), "COM_QUERY head preserved");
    assert!(
        captured[1..].iter().all(|byte| *byte == b'x'),
        "the payload head must arrive unaltered"
    );

    // The session is still in sequence afterwards: a streamed command must
    // leave the wire exactly where an ordinary one does.
    assert!(client.query_ok("SELECT 1").await, "session stays usable");
    client.quit().await;
    stack.dispatch_task.abort();
}

/// A topology client factory for a zero-cluster config: it is asked for the
/// clusters of each published generation, and with `pd-addrs = ""` there are
/// none, so it never produces an etcd client and nothing ever dials one.
struct NoClusterFactory;

impl control_topology::TopologyClientFactory for NoClusterFactory {
    fn build(
        &self,
        snapshot: &control_config::ConfigNamespaceSnapshot,
    ) -> Result<Vec<control_topology::TopologyClusterClient>, String> {
        let topology = snapshot.topology().map_err(|error| format!("{error:?}"))?;
        assert!(
            topology.backend_clusters.is_empty(),
            "the local-route fixture is zero-cluster by construction"
        );
        Ok(Vec::new())
    }
}

/// Discards runtime events; the fixture asserts on behaviour, not on logs.
struct SilentSink;

impl control_plane::EventSink for SilentSink {
    fn record(&self, _event: &control_plane::RuntimeEvent) {}
}

/// The process-local control plane a production session owner consults:
/// a config source with one static namespace, a topology module over it, and
/// a running route plane.
///
/// This is what `spawn_stack*` has never had. Those build their owner without
/// `.with_route_plane(...)`, so every one of them runs the compatibility
/// `BindingRouteChannel` instead — which is why they cannot admit a second
/// session. Here admission resolves the way production resolves it.
struct LocalRoutePlane {
    handle: control_router::RoutePlaneHandle,
    _runtime: control_plane::ControlRuntime,
    topology_task: tokio::task::JoinHandle<Result<(), control_plane::ModuleError>>,
    plane_task: tokio::task::JoinHandle<Result<(), control_plane::ModuleError>>,
}

impl LocalRoutePlane {
    /// Stops both module tasks and waits for them, so a leaked task is never
    /// mistaken for a product defect and the runtime is released before the
    /// test returns.
    async fn shutdown(self) {
        self.topology_task.abort();
        self.plane_task.abort();
        let _ = self.topology_task.await;
        let _ = self.plane_task.await;
    }
}

/// Builds the plane for a namespace whose only backend is `backend_port`,
/// pinned statically so no discovery, health probe or etcd connection runs.
async fn spawn_local_route_plane(user: &str, backend_port: u16) -> LocalRoutePlane {
    use control_config::HealthCheckConfig;
    use control_config::{ConfigNamespaceStore, TopologyRuntimeIdentity};
    use control_plane::ControlModule;
    use control_plane::{ControlConfig, ControlRuntime, LogLevel, MetricsPolicy, TlsPolicy};
    use control_topology::{StaticAdvertiseResolver, TopologyModule};

    // An empty `pd-addrs` yields zero backend clusters, so discovery has
    // nothing to connect to and the factory above returns no clients.
    // `EffectiveConfig`'s fields are private but it derives `Deserialize`,
    // so serde is the supported way to build one that differs from default.
    let Ok(effective) =
        serde_json::from_str::<control_config::EffectiveConfig>(r#"{"proxy":{"pd-addrs":""}}"#)
    else {
        unreachable!("zero-cluster effective config")
    };
    let namespace_json = format!(
        r#"{{"namespace":"default","frontend":{{"user":"{user}"}},"backend":{{"instances":["127.0.0.1:{backend_port}"]}}}}"#
    );
    let Ok(namespace) =
        control_config::source::decode_namespace("default", namespace_json.as_bytes())
    else {
        unreachable!("static namespace decodes")
    };
    let Ok(store) = ConfigNamespaceStore::new(
        effective,
        vec![namespace],
        control_config::SourceRevision::default(),
        &std::env::current_dir().unwrap_or_default(),
    ) else {
        unreachable!("namespace store")
    };

    let registry = control_plane::OwnershipRegistry::new();
    let Ok(control_config) = ControlConfig::new(
        1,
        Duration::from_secs(30),
        0,
        TlsPolicy::default(),
        LogLevel::Info,
        MetricsPolicy::default(),
    ) else {
        unreachable!("control config")
    };
    let Ok(runtime) = ControlRuntime::claim_process(
        &registry,
        "dataplane-local-route-fixture",
        control_config,
        Arc::new(SilentSink),
    ) else {
        unreachable!("claim process")
    };

    let source: Arc<dyn control_config::ConfigNamespaceSource> = Arc::new(store);
    let Ok((topology_module, mut topology_handle)) = TopologyModule::new(
        Arc::clone(&source),
        Box::new(NoClusterFactory),
        Arc::new(StaticAdvertiseResolver::new("127.0.0.1")),
        TopologyRuntimeIdentity {
            version: Arc::from("v-test"),
            git_hash: Arc::from("hash-test"),
            deploy_path: std::path::PathBuf::from("/deploy/test"),
            start_timestamp: 1_700_000_000,
        },
        HealthCheckConfig {
            enabled: false,
            ..HealthCheckConfig::default()
        },
    ) else {
        unreachable!("topology module")
    };

    let (route_plane, mut plane_handle) =
        control_router::RoutePlane::new(Arc::clone(&source), topology_handle.clone(), None);

    let context = runtime.handle().module_context();
    let plane_context = runtime.handle().module_context();
    let Ok(()) = runtime.mark_ready() else {
        unreachable!("runtime ready")
    };
    let topology_task = tokio::spawn(Box::new(topology_module).run(context));
    // Stage 1 of the four-stage localisation: topology must be ready before
    // the plane can bind a namespace incarnation to it.
    assert!(
        timeout(Duration::from_secs(5), topology_handle.wait_ready())
            .await
            .is_ok(),
        "stage topology-ready: the zero-cluster topology module never became ready"
    );
    let plane_task = tokio::spawn(Box::new(route_plane).run(plane_context));
    // Stage 2: the plane builds its router incarnation and publishes it.
    assert!(
        timeout(Duration::from_secs(5), plane_handle.wait_ready())
            .await
            .is_ok(),
        "stage route-ready: the route plane never became ready"
    );

    LocalRoutePlane {
        handle: plane_handle,
        _runtime: runtime,
        topology_task,
        plane_task,
    }
}

/// A stack whose session owner holds a real route plane, so admission runs
/// the production local path instead of the compatibility bridge.
async fn spawn_local_route_stack(user: &str, backend_port: u16) -> (Stack, LocalRoutePlane) {
    let plane = spawn_local_route_plane(user, backend_port).await;
    let stack = spawn_stack_configured_with_metering(
        SnapshotReply::Valid,
        false,
        Duration::from_secs(5),
        Duration::from_secs(60),
        false,
        None,
        None,
        Duration::from_secs(30),
        None,
        Some(plane.handle.clone()),
    )
    .await;
    (stack, plane)
}

/// CFG-001 fixture smoke: one stack admits two client sessions in sequence,
/// each reaching its backend.
///
/// This is the property the compatibility-bridge fixture cannot provide —
/// there `HandshakeResponse`/`RouteRequest` go to a never-running
/// `ControlClient`, so `spawn_route_answer*` has to guess request ids and
/// re-inject them, and a second session never completes. Every reload
/// acceptance test needs a second session by construction, so this smoke is
/// the gate the PROXY-v2 and required-TLS effect tests are built on.
#[tokio::test]
async fn local_route_stack_admits_two_sequential_sessions() {
    let backend = spawn_concurrent_fake_backend(vec![BackendMode::Plain]).await;
    let (stack, plane) = spawn_local_route_stack("root", backend.port).await;
    let accepted = Arc::clone(&backend.accepted);

    // Stage 3 and 4 of the localisation ladder: admit, then backend-accept.
    let Some(mut first) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        stack.dispatch_task.abort();
        plane.shutdown().await;
        backend.shutdown().await;
        unreachable!("stage admit: the first session never completed its handshake")
    };
    assert!(first.query_ok("SELECT 1").await, "first session serves");
    assert_eq!(
        accepted.load(Ordering::Relaxed),
        1,
        "stage backend-accept: exactly one backend connection so far"
    );

    let Some(mut second) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        stack.dispatch_task.abort();
        plane.shutdown().await;
        backend.shutdown().await;
        unreachable!(
            "stage admit: the SECOND session never completed — the fixture still cannot admit one"
        )
    };
    assert!(second.query_ok("SELECT 2").await, "second session serves");
    assert_eq!(
        accepted.load(Ordering::Relaxed),
        2,
        "stage backend-accept: the second session reached the backend on its own connection"
    );

    // Both are independent: the first keeps working after the second exists.
    assert!(
        first.query_ok("SELECT 3").await,
        "the first session is unaffected by the second"
    );

    first.quit().await;
    second.quit().await;
    stack.dispatch_task.abort();
    plane.shutdown().await;
    backend.shutdown().await;
}

/// The transport a task-#119 backend endpoint presents on one accepted
/// connection, fixed by accept index rather than inferred from the wire.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BackendMode {
    /// Plain `MySQL`: greeting first, no PROXY preamble.
    Plain,
    /// Expect a PROXY-v2 preamble before the `MySQL` stream.
    ProxyV2,
    /// Serve the `MySQL` stream inside TLS.
    Tls,
}

/// A backend that serves accepted connections **concurrently**.
///
/// The shared `run_fake_backend` accepts serially — it awaits one connection's
/// whole session before looping back to `accept()` — so a second proxy dial
/// only reaches the kernel backlog while the first session is alive. That is
/// invisible from the outside: the client sees a proxy greeting, admission
/// succeeds, and `accepted_connections` stays at 1 because it is counted after
/// the user-space accept. Any test needing two live sessions therefore needs
/// its own endpoint, which is also what the later PROXY/TLS effect tests want
/// when an old and a new seat must coexist.
struct ConcurrentFakeBackend {
    port: u16,
    accepted: Arc<AtomicU64>,
    proxy_headers: Arc<Mutex<Vec<Vec<u8>>>>,
    stop: watch::Sender<bool>,
    listener_task: tokio::task::JoinHandle<()>,
}

impl ConcurrentFakeBackend {
    /// PROXY-v2 preambles captured per accepted connection, in accept order.
    /// Read by the PROXY effect test; empty for a plain-only endpoint.
    fn proxy_headers(&self) -> Vec<Vec<u8>> {
        self.proxy_headers
            .lock()
            .map(|headers| headers.clone())
            .unwrap_or_default()
    }

    /// Stops accepting and waits until every connection handler has actually
    /// finished.
    ///
    /// The listener is asked to stop rather than aborted from outside, because
    /// dropping a `JoinSet` only calls `abort()` on its children and does not
    /// wait for them — the cancellations would still be in flight, with the
    /// handles gone. Instead the listener leaves its loop, runs
    /// `JoinSet::shutdown().await` (abort *and* join every child), and only
    /// then returns; awaiting the listener here therefore means the whole tree
    /// is finished.
    async fn shutdown(self) {
        let _ = self.stop.send(true);
        let _ = self.listener_task.await;
    }
}

/// Spawns the concurrent endpoint. `modes` is consumed by accept index; past
/// its end the last entry repeats, so a one-element list is a uniform server.
async fn spawn_concurrent_fake_backend(modes: Vec<BackendMode>) -> ConcurrentFakeBackend {
    assert!(!modes.is_empty(), "at least one mode is required");
    let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)).await else {
        unreachable!("backend bind")
    };
    let Ok(address) = listener.local_addr() else {
        unreachable!("backend addr")
    };
    let accepted = Arc::new(AtomicU64::new(0));
    let proxy_headers = Arc::new(Mutex::new(Vec::new()));
    let server_accepted = Arc::clone(&accepted);
    let server_headers = Arc::clone(&proxy_headers);
    let (stop, mut stop_rx) = watch::channel(false);
    let listener_task = tokio::spawn(async move {
        // The handlers are owned here and drained here: `shutdown()` below is
        // the only thing that ends this loop, so the set is never merely
        // dropped.
        let mut handlers = tokio::task::JoinSet::new();
        let mut index = 0_usize;
        loop {
            let accepted_connection = tokio::select! {
                biased;
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                    continue;
                }
                accepted = listener.accept() => accepted,
            };
            let Ok((stream, _)) = accepted_connection else {
                break;
            };
            let mode = modes[index.min(modes.len() - 1)];
            index += 1;
            server_accepted.fetch_add(1, Ordering::Relaxed);
            let headers = Arc::clone(&server_headers);
            // One task per connection: the whole point of this endpoint.
            handlers.spawn(async move {
                let broad = fake_backend_capabilities(false);
                let stream = if mode == BackendMode::ProxyV2 {
                    let (mut read, write) = stream.into_split();
                    let Some(header) = strip_inbound_proxy_v2(&mut read).await else {
                        return;
                    };
                    if let Ok(mut captured) = headers.lock() {
                        captured.push(header);
                    }
                    let Ok(stream) = read.reunite(write) else {
                        return;
                    };
                    stream
                } else {
                    stream
                };
                if mode == BackendMode::Tls {
                    run_fake_tls_backend_connection(
                        stream,
                        fake_backend_tls_config(),
                        Arc::new(Mutex::new(Vec::new())),
                        SnapshotReply::Valid,
                        false,
                    )
                    .await;
                    return;
                }
                let (read, write) = tokio::io::split(stream);
                let mut reader = PacketReader::new(read);
                let mut writer = PacketWriter::new(write);
                let Some(auth) = fake_backend_auth(&mut reader, &mut writer, broad).await else {
                    return;
                };
                let _ = run_fake_backend_commands(
                    reader,
                    writer,
                    auth,
                    Arc::new(Mutex::new(Vec::new())),
                    SnapshotReply::Valid,
                    broad,
                    false,
                )
                .await;
            });
        }
        // Abort every handler AND wait for it, which `Drop` alone does not do.
        handlers.shutdown().await;
    });
    ConcurrentFakeBackend {
        port: address.port(),
        accepted,
        proxy_headers,
        stop,
        listener_task,
    }
}

/// Re-applies the stack's base config at a new generation with `proxy_protocol`
/// or `require_backend_tls` changed, so a reload test states exactly what it
/// varied.
fn reloaded_engine_snapshot(
    sql_port: u16,
    backend_port: u16,
    generation: u64,
    proxy_v2: bool,
    require_backend_tls: bool,
) -> Arc<control_proto::snapshot::ValidatedSnapshot> {
    let base = engine_snapshot(sql_port, backend_port, proxy_v2, None, require_backend_tls);
    let raw = base.raw().clone();
    let Ok(store) = control_proto::snapshot::SnapshotStore::new([]) else {
        unreachable!("store")
    };
    let Ok(outcome) = store.apply(
        generation,
        raw,
        control_proto::snapshot::UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
        control_proto::snapshot::SnapshotLineage::for_tests("go-fixture"),
    ) else {
        unreachable!("reload snapshot applies")
    };
    outcome.snapshot
}

/// CFG-001: reloading `proxy_protocol` to v2 makes the *next* session dial its
/// backend with a PROXY-v2 preamble, while the session admitted before the
/// reload keeps dialling without one.
///
/// Both sessions are alive at the same time, on one backend endpoint whose
/// mode is fixed per accept index — the old seat's connection is plain, the
/// new seat's expects the preamble — so nothing here infers a transport from
/// the wire.
#[tokio::test]
async fn reloaded_proxy_protocol_binds_the_next_session_only() {
    use proxy_io::proxy_protocol::{
        EncodeAddresses, ProxyCommand, ProxyVersion, TransportProtocol, encode_proxy_v2,
    };
    use std::net::{IpAddr, Ipv4Addr};

    let backend =
        spawn_concurrent_fake_backend(vec![BackendMode::Plain, BackendMode::ProxyV2]).await;
    let (stack, plane) = spawn_local_route_stack("root", backend.port).await;

    // Generation 1: PROXY disabled. The first accepted backend connection is
    // the plain one.
    let Some(mut first) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        stack.dispatch_task.abort();
        plane.shutdown().await;
        backend.shutdown().await;
        unreachable!("first session")
    };
    assert!(first.query_ok("SELECT 1").await, "plain dial serves");
    assert!(
        backend.proxy_headers().is_empty(),
        "generation 1 dialled without a PROXY preamble"
    );

    // Generation 2: PROXY v2.
    assert!(
        stack
            .server_handle
            .update_snapshot(reloaded_engine_snapshot(
                stack.sql_port,
                backend.port,
                2,
                true,
                false,
            ))
            .is_ok(),
        "the reload publishes"
    );

    // Enabling v2 arms both legs: the listener now also expects an inbound
    // header from the LB, so the post-reload client presents one.
    let Ok(src_ip) = "203.0.113.7".parse::<Ipv4Addr>() else {
        unreachable!("source parses")
    };
    let Ok(dst_ip) = "203.0.113.9".parse::<Ipv4Addr>() else {
        unreachable!("destination parses")
    };
    let Ok(inbound) = encode_proxy_v2(
        ProxyVersion::V2,
        ProxyCommand::PROXY,
        TransportProtocol::STREAM,
        EncodeAddresses::Ip {
            src: (IpAddr::V4(src_ip), 31_234),
            dst: (IpAddr::V4(dst_ip), 4_000),
        },
        &[],
    ) else {
        unreachable!("header encodes")
    };
    let Some(mut second) = timeout(
        Duration::from_secs(5),
        MysqlClient::connect_with_proxy_header(stack.sql_port, &inbound),
    )
    .await
    .ok()
    .flatten() else {
        stack.dispatch_task.abort();
        plane.shutdown().await;
        backend.shutdown().await;
        unreachable!("second session under the reloaded PROXY config")
    };
    assert!(second.query_ok("SELECT 2").await, "v2 dial serves");
    assert_eq!(
        backend.proxy_headers().len(),
        1,
        "the reload put a PROXY-v2 preamble on the NEW session's dial only"
    );
    assert_eq!(
        backend.accepted.load(Ordering::Relaxed),
        2,
        "both seats hold their own backend connection"
    );

    // The pre-reload seat still runs on its admitted config.
    assert!(
        first.query_ok("SELECT 3").await,
        "the established session is unaffected"
    );
    assert_eq!(
        backend.proxy_headers().len(),
        1,
        "no preamble was retrofitted onto the established session"
    );

    first.quit().await;
    second.quit().await;
    stack.dispatch_task.abort();
    plane.shutdown().await;
    backend.shutdown().await;
}

/// CFG-001: reloading `require_backend_tls` binds the *next* session's dial to
/// TLS while the session admitted before it keeps its plaintext transport.
///
/// One endpoint, two accept-indexed modes: the old seat's connection is
/// plaintext and the new seat's is TLS. Both sessions stay alive, so the test
/// shows the two transports coexisting rather than a global switch.
#[tokio::test]
async fn reloaded_backend_tls_requirement_binds_the_next_session_only() {
    let backend = spawn_concurrent_fake_backend(vec![BackendMode::Plain, BackendMode::Tls]).await;
    let (stack, plane) = spawn_local_route_stack("root", backend.port).await;

    // Generation 1: TLS not required; the dial is plaintext.
    let Some(mut first) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        stack.dispatch_task.abort();
        plane.shutdown().await;
        backend.shutdown().await;
        unreachable!("first session")
    };
    assert!(
        first.query_ok("SELECT 1").await,
        "a plaintext backend serves while TLS is optional"
    );

    // Generation 2: TLS required.
    assert!(
        stack
            .server_handle
            .update_snapshot(reloaded_engine_snapshot(
                stack.sql_port,
                backend.port,
                2,
                false,
                true,
            ))
            .is_ok(),
        "the reload publishes"
    );

    let Some(mut second) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        stack.dispatch_task.abort();
        plane.shutdown().await;
        backend.shutdown().await;
        unreachable!("second session under the reloaded TLS requirement")
    };
    assert!(
        second.query_ok("SELECT 2").await,
        "the TLS backend serves once the reload requires TLS"
    );
    assert_eq!(
        backend.accepted.load(Ordering::Relaxed),
        2,
        "both seats hold their own backend connection"
    );

    // The pre-reload seat keeps the transport it was admitted with.
    assert!(
        first.query_ok("SELECT 3").await,
        "the established plaintext session is unaffected by the reload"
    );

    first.quit().await;
    second.quit().await;
    stack.dispatch_task.abort();
    plane.shutdown().await;
    backend.shutdown().await;
}

// ---------------------------------------------------------------------
// Direct engine event channel (no classifier pump): probes due while
// commands are queued
// ---------------------------------------------------------------------

/// With the loop reading the engine's classified channel directly, the engine
/// can be parked in its one-slot `events` send at the moment the loop's backend
/// probe becomes due. The probe must still be serviced (the engine answers
/// `Probe` while awaiting the command's admission ACK) and the command stream
/// must keep completing: a 5 ms probe interval against a burst of commands
/// exercises probes becoming due around queued commands many times, and any
/// lost ACK or send/probe wait cycle shows up as a hang caught by the timeouts.
/// Scope: liveness smoke only; the deterministic "probe selected before a
/// queued `ClientCommand`" ordering is covered at the loop level in
/// `tests/session_loop.rs`.
#[tokio::test]
async fn probe_due_while_commands_are_queued_keeps_completing() {
    // Fixed count under one total budget (CI-speed independent): the probe
    // interval is far shorter than a command round-trip, so probes become due
    // between and around queued commands throughout the burst.
    const BURST_COMMANDS: u32 = 64;
    let stack = spawn_stack_full(
        SnapshotReply::Valid,
        false,
        Duration::from_secs(5),
        Duration::from_millis(5),
        false,
        None,
    )
    .await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("handshake+auth completes end to end")
    };
    let burst = timeout(Duration::from_secs(30), async {
        for index in 0..BURST_COMMANDS {
            assert!(
                client.query_ok("SELECT 1").await,
                "command {index} completes while probes keep becoming due"
            );
        }
    })
    .await;
    assert!(
        burst.is_ok(),
        "the {BURST_COMMANDS}-command burst finishes within its budget"
    );
    // Let probes run in the probe-safe idle state, then prove the session
    // still serves a command and closes within the budget.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        timeout(Duration::from_secs(5), client.query_ok("SELECT 2"))
            .await
            .unwrap_or(false),
        "a command after idle probes still round-trips"
    );
    stack.shutdown_tx.send(true).ok();
    let closed = timeout(
        Duration::from_secs(5),
        wait_sent(
            &stack.sender,
            |e| matches!(&e.body, Some(Body::ConnectionEvent(event)) if event.kind == 3),
        ),
    )
    .await;
    assert!(
        matches!(closed, Ok(Some(_))),
        "session closes within the budget after the probe/command interleaving"
    );
    drop(client);
    stack.dispatch_task.abort();
    stack.server_task.abort();
}

/// opt#13: a queued `COM_STMT_CLOSE` must not wait for a next command that is
/// only partially here. The client sends the close, then just the header and
/// command byte of a `COM_STMT_PREPARE` and stalls; the backend must still
/// receive the close promptly. Completing the prepare afterwards works.
#[tokio::test]
async fn deferred_close_is_flushed_when_the_next_command_is_incomplete() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(matches!(
        client.stmt_prepare("NOMETA").await,
        Some(PrepareOutcome::Ok { .. })
    ));
    assert!(client.stmt_close(7).await, "the close is written");
    // Header (7-byte payload, sequence 0) plus the COM_STMT_PREPARE byte only.
    let prefix = [7_u8, 0, 0, 0, 0x16];
    assert!(client.writer.get_mut().write_all(&prefix).await.is_ok());
    assert!(client.writer.get_mut().flush().await.is_ok());
    let close_seen = timeout(Duration::from_secs(2), async {
        loop {
            let seen = stack
                .backend_transcript
                .lock()
                .ok()
                .is_some_and(|commands| {
                    commands
                        .iter()
                        .any(|command| command.first() == Some(&0x19))
                });
            if seen {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        close_seen,
        Ok(true),
        "a stalled partial next command does not hold the queued close"
    );
    // Finish the prepare: the rest of its payload, then its OK response.
    assert!(client.writer.get_mut().write_all(b"NOMETA").await.is_ok());
    assert!(client.writer.get_mut().flush().await.is_ok());
    client.reader.reset_sequence(1);
    let Ok(response) = client.reader.read_logical(64 * 1024).await else {
        unreachable!("the completed prepare is answered")
    };
    assert_eq!(response.payload.first(), Some(&0x00));
    client.quit().await;
    stack.dispatch_task.abort();
}

/// opt#13: the bytes of queued `COM_STMT_CLOSE`s are counted exactly once from
/// the raw counter — an idle flush settles them as pure traffic, no observation
/// estimates bytes ahead of the actual write, and the next query carries only
/// its own bytes.
#[tokio::test]
async fn deferred_close_traffic_is_attributed_once() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(matches!(
        client.stmt_prepare("NOMETA").await,
        Some(PrepareOutcome::Ok { .. })
    ));
    // Two closes in a row stay queued together; then idle flushes them
    // without a next command.
    assert!(client.stmt_close(7).await, "the first close is written");
    assert!(client.stmt_close(7).await, "the second close is written");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(client.query_ok("SELECT 1").await);
    client.quit().await;
    // opt#23: the two closes and the query arrive as per-command batches
    // when the session's recorder drops, in no fixed order relative to the
    // settlements and the close record, so read until everything needed is in.
    let mut close_batch = None;
    let mut settled = Vec::new();
    let mut query_after_close = None;
    let mut session_closed = false;
    while let Ok(Some(observation)) = timeout(Duration::from_secs(2), stack.metrics_rx.recv()).await
    {
        match observation {
            Observation::CommandBatch(batch) => match batch.command() {
                session_core::command::Command::StmtClose => {
                    close_batch = Some((batch.count(), batch.traffic()));
                }
                session_core::command::Command::Query => query_after_close = Some(batch.traffic()),
                _ => {}
            },
            Observation::BackendTrafficSettled { traffic, .. } => settled.push(traffic),
            Observation::SessionClosed { .. } => session_closed = true,
            _ => {}
        }
        if close_batch.is_some() && query_after_close.is_some() && session_closed {
            break;
        }
    }
    let Some((close_count, close_traffic)) = close_batch else {
        unreachable!("the closes are observed")
    };
    assert_eq!(close_count, 2, "both closes are observed");
    // Bytes come from the raw counter: nothing was written when the closes
    // were recorded (their packets are framed at queue time).
    assert_eq!(close_traffic.outbound_bytes, 0);
    assert_eq!(close_traffic.outbound_packets, 2);
    // The idle flush(es) settle the two closes (2 x 9 raw bytes) as pure
    // traffic. Whether both closes leave in one flush or one each depends on
    // arrival timing, so only the total is fixed.
    assert!(
        !settled.is_empty(),
        "the idle flush settles the queued closes"
    );
    let settled_bytes: u64 = settled.iter().map(|traffic| traffic.outbound_bytes).sum();
    assert_eq!(settled_bytes, 18);
    for traffic in &settled {
        assert_eq!(traffic.outbound_packets, 0);
    }
    let Some(query_after_close) = query_after_close else {
        unreachable!("the query after the closes is observed")
    };
    // COM_QUERY "SELECT 1" (4 + 9) carries only its own bytes.
    assert_eq!(query_after_close.outbound_bytes, 13);
    assert_eq!(query_after_close.outbound_packets, 1);
    stack.dispatch_task.abort();
}

/// opt#13: a queued `COM_STMT_CLOSE` followed by an abrupt client disconnect
/// (no QUIT, no next command) still has its raw bytes settled at session end.
#[tokio::test]
async fn deferred_close_bytes_settle_when_the_client_disconnects_at_once() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(matches!(
        client.stmt_prepare("NOMETA").await,
        Some(PrepareOutcome::Ok { .. })
    ));
    assert!(client.stmt_close(7).await, "the close is written");
    drop(client);
    let mut settled_bytes = 0;
    let mut close_observed = false;
    let mut session_closed = false;
    // opt#23: the close arrives as the session's batch when its recorder
    // drops, possibly after the close record; settlements are recorded by the
    // engine before either, so once both are in every byte has been seen.
    while let Ok(Some(observation)) = timeout(Duration::from_secs(2), stack.metrics_rx.recv()).await
    {
        match observation {
            Observation::CommandBatch(batch)
                if batch.command() == session_core::command::Command::StmtClose =>
            {
                assert_eq!(batch.count(), 1);
                close_observed = true;
                settled_bytes += batch.traffic().outbound_bytes;
            }
            Observation::BackendTrafficSettled { traffic, .. } => {
                settled_bytes += traffic.outbound_bytes;
            }
            Observation::SessionClosed { .. } => session_closed = true,
            _ => {}
        }
        if close_observed && session_closed {
            break;
        }
    }
    assert!(close_observed, "the close is observed");
    // COM_STMT_CLOSE on the wire: 4-byte header + 5-byte payload, exactly once.
    assert_eq!(settled_bytes, 9);
    stack.dispatch_task.abort();
}

/// A maximal first packet whose command byte is unknown is refused from the
/// header and ends the session after exactly one error: the prefix is never
/// consumed, so continuing would answer the same prefix forever.
#[tokio::test]
async fn oversized_unknown_command_ends_the_session_after_one_error() {
    let stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    // Header: payload length 0xFF_FFFF, sequence 0; then a byte that is not a command.
    let prefix = [0xFF_u8, 0xFF, 0xFF, 0x00, 0x7F];
    assert!(client.writer.get_mut().write_all(&prefix).await.is_ok());
    assert!(client.writer.get_mut().flush().await.is_ok());
    client.reader.reset_sequence(1);
    let Ok(Ok(error)) = timeout(
        Duration::from_secs(2),
        client.reader.read_logical(64 * 1024),
    )
    .await
    else {
        unreachable!("one error packet answers the refused command")
    };
    assert_eq!(error.payload.first(), Some(&0xFF));
    assert_eq!(
        u16::from_le_bytes([error.payload[1], error.payload[2]]),
        1047,
        "unknown command"
    );
    let next = timeout(
        Duration::from_secs(2),
        client.reader.read_logical(64 * 1024),
    )
    .await;
    assert!(
        matches!(next, Ok(Err(_))),
        "the session ends instead of answering the same prefix again: {next:?}"
    );
    stack.dispatch_task.abort();
}

/// opt#13: a queued `COM_STMT_CLOSE` followed by a `COM_STMT_SEND_LONG_DATA`
/// that overflows the bounded queue: the overflow drains the close plus the
/// long-data header inside the long-data command's window and leaves the
/// payload queued for the idle settlement. Whatever the exact split, no byte
/// is counted twice or lost (9 + 4 + 32764 in total).
#[tokio::test]
async fn deferred_partial_drain_counts_every_byte_once() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(mut client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    assert!(matches!(
        client.stmt_prepare("NOMETA").await,
        Some(PrepareOutcome::Ok { .. })
    ));
    assert!(client.stmt_close(7).await, "the close is queued");
    // COM_STMT_SEND_LONG_DATA: command byte, statement id 7, parameter 0, data.
    let mut long_data = vec![0x18_u8, 7, 0, 0, 0, 0, 0];
    long_data.resize(32_764, b'x');
    client.writer.reset_sequence(0);
    assert!(client.writer.write_logical(&long_data, true).await.is_ok());
    // Idle: the queued payload is flushed without a next command.
    tokio::time::sleep(Duration::from_millis(300)).await;
    client.quit().await;
    let mut close_bytes = None;
    let mut long_data_bytes = None;
    let mut settled = Vec::new();
    let mut session_closed = false;
    // opt#23: both commands arrive as batches when the session's recorder
    // drops, possibly after the close record; the settlements precede both.
    while let Ok(Some(observation)) = timeout(Duration::from_secs(2), stack.metrics_rx.recv()).await
    {
        match observation {
            Observation::CommandBatch(batch) => match batch.command() {
                session_core::command::Command::StmtClose => {
                    assert_eq!(batch.count(), 1);
                    close_bytes = Some(batch.traffic().outbound_bytes);
                }
                session_core::command::Command::StmtSendLongData => {
                    assert_eq!(batch.count(), 1);
                    long_data_bytes = Some(batch.traffic().outbound_bytes);
                }
                _ => {}
            },
            Observation::BackendTrafficSettled { traffic, .. } => {
                settled.push(traffic.outbound_bytes);
            }
            Observation::SessionClosed { .. } => session_closed = true,
            _ => {}
        }
        if close_bytes.is_some() && long_data_bytes.is_some() && session_closed {
            break;
        }
    }
    assert_eq!(
        close_bytes,
        Some(0),
        "the close was still queued when recorded"
    );
    // Either the long-data arrived while the close was still queued (the
    // overflow drained the close plus the long-data header: 13 bytes in the
    // long-data window, the payload settles later) or the close was flushed
    // first (9 bytes settled, the long-data queued whole). In both cases the
    // command windows and the settlements together carry every byte once.
    let Some(long_data_bytes) = long_data_bytes else {
        unreachable!("the long-data command is observed")
    };
    assert!(
        long_data_bytes == 13 || long_data_bytes == 0,
        "long-data window carries the drained tail or nothing: {long_data_bytes}"
    );
    let settled_total: u64 = settled.iter().sum();
    assert_eq!(
        long_data_bytes + settled_total,
        9 + 4 + 32_764,
        "close + long-data header + payload, each exactly once"
    );
    stack.dispatch_task.abort();
}

/// opt#13: the handshake observation already carried the handshake bytes, so
/// a client that disconnects right after the handshake (no command) must not
/// have them settled a second time at session end.
#[tokio::test]
async fn handshake_then_disconnect_settles_nothing_twice() {
    let mut stack = spawn_stack().await;
    spawn_route_answer(&stack, 1, 2);
    let Some(client) = timeout(Duration::from_secs(5), MysqlClient::connect(stack.sql_port))
        .await
        .ok()
        .flatten()
    else {
        unreachable!("session established")
    };
    drop(client);
    let mut handshake_outbound = None;
    let mut settled_bytes = 0;
    while let Ok(Some(observation)) = timeout(Duration::from_secs(2), stack.metrics_rx.recv()).await
    {
        match observation {
            Observation::HandshakeCompleted { traffic, .. } => {
                handshake_outbound = Some(traffic.outbound_bytes);
            }
            Observation::BackendTrafficSettled { traffic, .. } => {
                settled_bytes += traffic.outbound_bytes;
            }
            Observation::SessionClosed { .. } => break,
            _ => {}
        }
    }
    assert!(
        handshake_outbound.is_some_and(|bytes| bytes > 0),
        "the handshake observation carried the handshake bytes"
    );
    assert_eq!(settled_bytes, 0, "nothing is settled again at session end");
    stack.dispatch_task.abort();
}
