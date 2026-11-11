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

//! Reload, certificate, and atomic last-good coverage for state snapshots.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use control_proto::snapshot::SnapshotLineage;
use control_proto::snapshot::{SnapshotErrorKind, SnapshotStore};
use control_proto::v1::{
    ConfigSnapshot, ErrorCode, KeepalivePolicy, Listener, ProxyProtocolMode, StateSnapshot,
    TlsPolicy,
};
use rcgen::{CertificateParams, KeyPair, date_time_ymd, generate_simple_self_signed};
use rustls::pki_types::UnixTime;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);
const VALIDATION_TIME_SECONDS: u64 = 1_800_000_000;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Result<Self, Box<dyn Error>> {
        let identifier = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "tiproxy-snapshot-store-{}-{identifier}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn lineage_a() -> SnapshotLineage {
    SnapshotLineage::for_tests("go-lineage-a")
}

#[test]
fn rotation_and_rename_swap_atomically_while_old_sessions_retain_state()
-> Result<(), Box<dyn Error>> {
    let directory = TestDirectory::create()?;
    let first = write_valid_pair(directory.path(), "first")?;
    let store = SnapshotStore::new([directory.path().to_path_buf()])?;
    let generation_one = store.apply(
        1,
        valid_snapshot(first.clone()),
        validation_time(),
        lineage_a(),
    )?;
    let existing_session = Arc::clone(&generation_one.snapshot);
    assert!(generation_one.changed);

    let staging = write_valid_pair(directory.path(), "staging")?;
    let rotated_cert = directory.path().join("rotated.crt");
    let rotated_key = directory.path().join("rotated.key");
    fs::rename(&staging.certificate_path, &rotated_cert)?;
    fs::rename(&staging.private_key_path, &rotated_key)?;
    let rotated = TlsPolicy {
        certificate_path: path_text(&rotated_cert)?,
        private_key_path: path_text(&rotated_key)?,
        minimum_version: "1.3".to_owned(),
        ..Default::default()
    };
    let generation_two_snapshot = valid_snapshot(rotated);
    let generation_two = store.apply(
        2,
        generation_two_snapshot.clone(),
        validation_time(),
        lineage_a(),
    )?;
    assert!(generation_two.changed);
    assert_eq!(existing_session.generation(), 1);
    assert_eq!(generation_two.snapshot.generation(), 2);
    assert_ne!(
        existing_session.frontend_tls.certificate_chain[0],
        generation_two.snapshot.frontend_tls.certificate_chain[0]
    );
    assert!(existing_session.frontend_tls.private_key().is_some());
    assert!(generation_two.snapshot.frontend_tls.private_key().is_some());
    assert!(!Arc::ptr_eq(&existing_session, &generation_two.snapshot));

    let duplicate = store.apply(2, generation_two_snapshot, validation_time(), lineage_a())?;
    assert!(!duplicate.changed);
    assert!(Arc::ptr_eq(&duplicate.snapshot, &generation_two.snapshot));
    assert_eq!(duplicate.to_result().code, ErrorCode::Ok as i32);
    Ok(())
}

#[test]
fn invalid_conflicting_and_stale_generations_keep_last_good() -> Result<(), Box<dyn Error>> {
    let directory = TestDirectory::create()?;
    let valid = write_valid_pair(directory.path(), "valid")?;
    let mismatch = write_valid_pair(directory.path(), "mismatch")?;
    let store = SnapshotStore::new([directory.path().to_path_buf()])?;
    let applied = store.apply(
        7,
        valid_snapshot(valid.clone()),
        validation_time(),
        lineage_a(),
    )?;

    let mismatched = TlsPolicy {
        certificate_path: valid.certificate_path.clone(),
        private_key_path: mismatch.private_key_path,
        ..Default::default()
    };
    let error = store
        .apply(
            8,
            valid_snapshot(mismatched),
            validation_time(),
            lineage_a(),
        )
        .err()
        .ok_or("mismatched key unexpectedly applied")?;
    assert_eq!(error.kind(), SnapshotErrorKind::Invalid);
    assert!(error.detail().contains("do not match"));
    let result = error.to_result(7);
    assert_eq!(result.applied_generation, 7);
    assert_eq!(result.code, ErrorCode::InvalidSnapshot as i32);
    assert!(!result.detail.is_empty());
    assert_eq!(store.current()?.ok_or("missing last-good")?.generation(), 7);

    let stale = store
        .apply(
            6,
            valid_snapshot(valid.clone()),
            validation_time(),
            lineage_a(),
        )
        .err()
        .ok_or("stale generation unexpectedly applied")?;
    assert_eq!(stale.kind(), SnapshotErrorKind::Stale);

    let conflicting = store
        .apply(
            7,
            valid_snapshot(TlsPolicy::default()),
            validation_time(),
            lineage_a(),
        )
        .err()
        .ok_or("conflicting generation unexpectedly applied")?;
    assert_eq!(conflicting.kind(), SnapshotErrorKind::Invalid);
    assert!(Arc::ptr_eq(
        &applied.snapshot,
        &store.current()?.ok_or("missing last-good")?
    ));
    Ok(())
}

#[test]
fn expiry_ca_tls_policy_and_unsupported_configuration_are_rejected_atomically()
-> Result<(), Box<dyn Error>> {
    let directory = TestDirectory::create()?;
    let valid = write_valid_pair(directory.path(), "valid")?;
    let expired = write_expired_pair(directory.path(), "expired")?;
    let store = SnapshotStore::new([directory.path().to_path_buf()])?;
    store.apply(
        1,
        valid_snapshot(valid.clone()),
        validation_time(),
        lineage_a(),
    )?;

    let expired_error = store
        .apply(2, valid_snapshot(expired), validation_time(), lineage_a())
        .err()
        .ok_or("expired certificate unexpectedly applied")?;
    assert_eq!(expired_error.kind(), SnapshotErrorKind::Invalid);
    assert!(expired_error.detail().contains("expired or not yet valid"));

    let mut missing_ca = valid_snapshot(valid.clone());
    let config = missing_ca.config.as_mut().ok_or("missing config")?;
    config.require_backend_tls = true;
    let missing_ca_error = store
        .apply(2, missing_ca, validation_time(), lineage_a())
        .err()
        .ok_or("backend TLS without CA unexpectedly applied")?;
    assert!(missing_ca_error.detail().contains("backend CA"));

    let ca = write_valid_pair(directory.path(), "ca")?;
    let mut with_ca = valid_snapshot(valid.clone());
    let config = with_ca.config.as_mut().ok_or("missing config")?;
    config.require_backend_tls = true;
    config.backend_tls = Some(TlsPolicy {
        ca_path: ca.certificate_path,
        minimum_version: "1.3".to_owned(),
        allowed_common_names: vec!["backend.test".to_owned()],
        ..Default::default()
    });
    let applied = store.apply(2, with_ca, validation_time(), lineage_a())?;
    assert_eq!(applied.snapshot.backend_tls.roots.len(), 1);
    assert_eq!(applied.snapshot.backend_tls.minimum_version, "1.3");
    assert_eq!(
        applied.snapshot.backend_tls.allowed_common_names,
        ["backend.test"]
    );

    let mut skip_ca = valid_snapshot(valid.clone());
    let config = skip_ca.config.as_mut().ok_or("missing config")?;
    config.require_backend_tls = true;
    config.backend_tls = Some(TlsPolicy {
        skip_ca_verification: true,
        ..Default::default()
    });
    assert!(
        store
            .apply(3, skip_ca, validation_time(), lineage_a())
            .is_ok()
    );

    let mut replay = valid_snapshot(valid);
    replay
        .config
        .as_mut()
        .ok_or("missing config")?
        .traffic_replay_enabled = true;
    let applied = store.apply(4, replay, validation_time(), lineage_a())?;
    assert!(
        applied
            .snapshot
            .raw()
            .config
            .as_ref()
            .is_some_and(|config| config.traffic_replay_enabled),
        "the Go default traffic-replay flag is retained and does not reject serving"
    );
    assert_eq!(store.current()?.ok_or("missing last-good")?.generation(), 4);
    Ok(())
}

#[test]
fn tls_paths_must_remain_beneath_the_allowlist() -> Result<(), Box<dyn Error>> {
    let allowed = TestDirectory::create()?;
    let outside = TestDirectory::create()?;
    let pair = write_valid_pair(outside.path(), "outside")?;
    let store = SnapshotStore::new([allowed.path().to_path_buf()])?;
    let error = store
        .apply(1, valid_snapshot(pair), validation_time(), lineage_a())
        .err()
        .ok_or("outside TLS path unexpectedly applied")?;
    assert_eq!(error.kind(), SnapshotErrorKind::Invalid);
    assert!(error.detail().contains("outside configured TLS roots"));
    assert!(store.current()?.is_none());
    Ok(())
}

#[test]
fn detached_server_tls_is_prepared_without_publishing() -> Result<(), Box<dyn Error>> {
    let directory = TestDirectory::create()?;
    let pair = write_valid_pair(directory.path(), "metric-owner")?;
    let store = SnapshotStore::new([directory.path().to_path_buf()])?;
    let prepared = store.prepare_server_tls("server_http_tls", &pair, validation_time())?;
    assert!(prepared.is_some(), "a complete server identity is retained");
    assert!(
        store.current()?.is_none(),
        "detached preparation never publishes snapshot state"
    );
    assert!(
        store
            .prepare_server_tls("server_http_tls", &TlsPolicy::default(), validation_time(),)?
            .is_none(),
        "an accepted empty policy selects plaintext"
    );
    Ok(())
}

fn valid_snapshot(frontend_tls: TlsPolicy) -> StateSnapshot {
    let keepalive = KeepalivePolicy {
        enabled: true,
        idle_millis: 60_000,
        probe_count: 5,
        interval_millis: 3_000,
        user_timeout_millis: 15_000,
    };
    StateSnapshot {
        config: Some(ConfigSnapshot {
            high_memory_reject_threshold: 0.9,
            connection_buffer_bytes: 32 * 1024,
            frontend_keepalive: Some(keepalive),
            healthy_backend_keepalive: Some(keepalive),
            unhealthy_backend_keepalive: Some(keepalive),
            proxy_protocol: ProxyProtocolMode::Disabled as i32,
            listeners: vec![Listener {
                address: "127.0.0.1".to_owned(),
                port: 6000,
                name: "sql-0".to_owned(),
            }],
            server_version: "TiProxy-test".to_owned(),
            frontend_tls: Some(frontend_tls),
            backend_tls: Some(TlsPolicy::default()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn write_valid_pair(directory: &Path, name: &str) -> Result<TlsPolicy, Box<dyn Error>> {
    let generated = generate_simple_self_signed(["localhost".to_owned()])?;
    write_pair(
        directory,
        name,
        generated.cert.pem(),
        generated.signing_key.serialize_pem(),
    )
}

fn write_expired_pair(directory: &Path, name: &str) -> Result<TlsPolicy, Box<dyn Error>> {
    let key = KeyPair::generate()?;
    let mut parameters = CertificateParams::new(["localhost".to_owned()])?;
    parameters.not_before = date_time_ymd(2000, 1, 1);
    parameters.not_after = date_time_ymd(2001, 1, 1);
    let certificate = parameters.self_signed(&key)?;
    write_pair(directory, name, certificate.pem(), key.serialize_pem())
}

fn write_pair(
    directory: &Path,
    name: &str,
    certificate: String,
    private_key: String,
) -> Result<TlsPolicy, Box<dyn Error>> {
    let certificate_path = directory.join(format!("{name}.crt"));
    let private_key_path = directory.join(format!("{name}.key"));
    fs::write(&certificate_path, certificate)?;
    fs::write(&private_key_path, private_key)?;
    Ok(TlsPolicy {
        certificate_path: path_text(&certificate_path)?,
        private_key_path: path_text(&private_key_path)?,
        minimum_version: "1.2".to_owned(),
        ..Default::default()
    })
}

fn path_text(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(path.to_str().ok_or("test path is not UTF-8")?.to_owned())
}

fn validation_time() -> UnixTime {
    UnixTime::since_unix_epoch(Duration::from_secs(VALIDATION_TIME_SECONDS))
}

/// DPL-07 cross-language contract: the Go projection's honest edge
/// shapes — an unscoped namespace (no unambiguous cluster: the
/// boot-time default before backends report, or a mixed-cluster
/// namespace) and a clusterless legacy static backend — must apply
/// through the full store path, not merely parse.
#[test]
fn unscoped_topology_applies_through_the_store() -> Result<(), Box<dyn Error>> {
    use control_proto::v1::{BackendSnapshot, NamespaceSnapshot};

    let directory = TestDirectory::create()?;
    let store = SnapshotStore::new([directory.path().to_path_buf()])?;
    let mut snapshot = valid_snapshot(TlsPolicy::default());
    snapshot.backends = vec![
        BackendSnapshot {
            backend_id: "alpha/tidb-1:4000".to_owned(),
            address: "tidb-1:4000".to_owned(),
            cluster_name: "alpha".to_owned(),
            keyspace: "ks-a".to_owned(),
            healthy: true,
            ..Default::default()
        },
        BackendSnapshot {
            backend_id: "legacy-tidb:4000".to_owned(),
            address: "legacy-tidb:4000".to_owned(),
            cluster_name: String::new(),
            healthy: true,
            ..Default::default()
        },
    ];
    snapshot.namespaces = vec![
        NamespaceSnapshot {
            name: "default".to_owned(),
            users: Vec::new(),
            backend_cluster: String::new(),
        },
        NamespaceSnapshot {
            name: "ns-alpha".to_owned(),
            users: vec!["alice".to_owned()],
            backend_cluster: "alpha".to_owned(),
        },
    ];
    let applied = store.apply(11, snapshot, validation_time(), lineage_a())?;
    assert!(applied.changed);
    assert_eq!(applied.snapshot.generation(), 11);
    assert_eq!(applied.snapshot.raw().namespaces.len(), 2);
    assert_eq!(applied.snapshot.raw().backends.len(), 2);
    Ok(())
}

fn lineage_b() -> SnapshotLineage {
    SnapshotLineage::for_tests("go-lineage-b")
}

/// Fix-2 lineage rollover triad: (1) generation monotonicity keeps
/// rejecting same-lineage lower generations; (2) a restarted Go — a
/// NEW lineage — starts a fresh generation sequence at 1 without
/// tripping the stale/duplicate rules; (3) an invalid new-lineage
/// candidate is rejected atomically and must NOT advance the store's
/// lineage: the old last-good and the old lineage's sequence rules
/// stay in force, because lineage advances only at COMMIT.
#[test]
fn lineage_rollover_is_fresh_and_only_commit_advances_lineage() -> Result<(), Box<dyn Error>> {
    let directory = TestDirectory::create()?;
    let valid = write_valid_pair(directory.path(), "valid")?;
    let mismatch = write_valid_pair(directory.path(), "mismatch")?;
    let store = SnapshotStore::new([directory.path().to_path_buf()])?;

    // Lineage A serves generation 5.
    let applied = store.apply(
        5,
        valid_snapshot(valid.clone()),
        validation_time(),
        lineage_a(),
    )?;
    assert_eq!(applied.snapshot.generation(), 5);

    // (1) Same lineage, lower generation: stale.
    let stale = store
        .apply(
            4,
            valid_snapshot(valid.clone()),
            validation_time(),
            lineage_a(),
        )
        .err()
        .ok_or("same-lineage lower generation unexpectedly applied")?;
    assert_eq!(stale.kind(), SnapshotErrorKind::Stale);

    // (3) An INVALID candidate from new lineage B is rejected
    // atomically…
    let broken = TlsPolicy {
        certificate_path: valid.certificate_path.clone(),
        private_key_path: mismatch.private_key_path.clone(),
        ..Default::default()
    };
    let invalid = store
        .apply(1, valid_snapshot(broken), validation_time(), lineage_b())
        .err()
        .ok_or("invalid new-lineage candidate unexpectedly applied")?;
    assert_eq!(invalid.kind(), SnapshotErrorKind::Invalid);
    // …the old last-good still serves…
    assert!(Arc::ptr_eq(
        &applied.snapshot,
        &store.current()?.ok_or("missing last-good")?
    ));
    // …and the lineage did NOT advance: lineage A's sequence rules
    // still bind after the failed rollover.
    let still_stale = store
        .apply(
            4,
            valid_snapshot(valid.clone()),
            validation_time(),
            lineage_a(),
        )
        .err()
        .ok_or("post-failure same-lineage lower generation unexpectedly applied")?;
    assert_eq!(still_stale.kind(), SnapshotErrorKind::Stale);
    let advanced = store.apply(
        6,
        valid_snapshot(TlsPolicy::default()),
        validation_time(),
        lineage_a(),
    )?;
    assert_eq!(advanced.snapshot.generation(), 6);

    // (2) A VALID new lineage B starts a fresh sequence: generation 1
    // — far below the committed 6 — applies as changed content.
    let rolled = store.apply(
        1,
        valid_snapshot(valid.clone()),
        validation_time(),
        lineage_b(),
    )?;
    assert!(rolled.changed);
    assert_eq!(rolled.snapshot.generation(), 1);
    assert_eq!(store.current()?.ok_or("missing last-good")?.generation(), 1);

    // The rollover COMMITTED lineage B: within B the same-generation
    // different-content replay now conflicts…
    let conflict = store
        .apply(
            1,
            valid_snapshot(TlsPolicy::default()),
            validation_time(),
            lineage_b(),
        )
        .err()
        .ok_or("same-lineage B conflict unexpectedly applied")?;
    assert_eq!(conflict.kind(), SnapshotErrorKind::Invalid);
    // …while A — itself a different lineage from the committed one —
    // rolls over fresh again: restart ping-pong stays consistent.
    let back = store.apply(
        1,
        valid_snapshot(TlsPolicy::default()),
        validation_time(),
        lineage_a(),
    )?;
    assert!(back.changed);
    assert_eq!(back.snapshot.generation(), 1);
    Ok(())
}

/// One CFG-002 case: a name, a single-field mutation, and the substring the
/// rejection detail must name.
type ConfigBoundCase = (&'static str, Box<dyn Fn(&mut ConfigSnapshot)>, &'static str);

/// Numeric and enum bounds: threshold, buffer size, PROXY mode.
fn numeric_config_bound_cases() -> Vec<ConfigBoundCase> {
    vec![
        (
            "threshold above one",
            Box::new(|config: &mut ConfigSnapshot| config.high_memory_reject_threshold = 1.5),
            "high_memory_reject_threshold",
        ),
        (
            // Go rejects the whole 0 < t < 0.5 band, not just out-of-range
            // values: a threshold that low would reject nearly every
            // connection on a healthy process.
            "threshold inside the forbidden band",
            Box::new(|config: &mut ConfigSnapshot| config.high_memory_reject_threshold = 0.25),
            "high_memory_reject_threshold",
        ),
        (
            "threshold not a number",
            Box::new(|config: &mut ConfigSnapshot| {
                config.high_memory_reject_threshold = f64::NAN;
            }),
            "high_memory_reject_threshold",
        ),
        (
            "buffer below one KiB",
            Box::new(|config: &mut ConfigSnapshot| config.connection_buffer_bytes = 512),
            "connection_buffer_bytes",
        ),
        (
            "buffer above sixteen MiB",
            Box::new(|config: &mut ConfigSnapshot| {
                config.connection_buffer_bytes = 32 * 1024 * 1024;
            }),
            "connection_buffer_bytes",
        ),
        (
            // The wire enum has no v1 member at all, so an unsupported
            // version cannot be expressed; what CAN arrive is the proto3
            // zero default, and defaulting THAT to either real mode would
            // silently pick a PROXY behaviour nobody configured.
            "an unspecified PROXY mode",
            Box::new(|config: &mut ConfigSnapshot| config.proxy_protocol = 0),
            "proxy_protocol",
        ),
        (
            "a PROXY value outside the enum",
            Box::new(|config: &mut ConfigSnapshot| config.proxy_protocol = 99),
            "proxy_protocol",
        ),
    ]
}

/// Listener shape, keepalive presence, and the remaining identity fields.
fn listener_config_bound_cases() -> Vec<ConfigBoundCase> {
    vec![
        (
            "no listeners at all",
            Box::new(|config: &mut ConfigSnapshot| config.listeners.clear()),
            "listeners must contain",
        ),
        (
            "a listener port beyond u16",
            Box::new(|config: &mut ConfigSnapshot| config.listeners[0].port = 70_000),
            "listener port",
        ),
        (
            "an unnamed listener",
            Box::new(|config: &mut ConfigSnapshot| config.listeners[0].name.clear()),
            "listener name",
        ),
        (
            // The expanded range is what arrives here, so a duplicated port
            // in the projection must not silently bind once.
            "a duplicated listener address",
            Box::new(|config: &mut ConfigSnapshot| {
                let duplicate = config.listeners[0].clone();
                config.listeners.push(Listener {
                    name: "sql-1".to_owned(),
                    ..duplicate
                });
            }),
            "duplicate",
        ),
        (
            "a duplicated listener name",
            Box::new(|config: &mut ConfigSnapshot| {
                let duplicate = config.listeners[0].clone();
                config.listeners.push(Listener {
                    port: duplicate.port + 1,
                    ..duplicate
                });
            }),
            "duplicate",
        ),
        (
            "a missing frontend keepalive policy",
            Box::new(|config: &mut ConfigSnapshot| config.frontend_keepalive = None),
            "frontend_keepalive",
        ),
        (
            "a missing healthy backend keepalive policy",
            Box::new(|config: &mut ConfigSnapshot| config.healthy_backend_keepalive = None),
            "healthy_backend_keepalive",
        ),
        (
            "a missing unhealthy backend keepalive policy",
            Box::new(|config: &mut ConfigSnapshot| config.unhealthy_backend_keepalive = None),
            "unhealthy_backend_keepalive",
        ),
        (
            "an empty server version",
            Box::new(|config: &mut ConfigSnapshot| config.server_version.clear()),
            "server_version",
        ),
        (
            "a malformed public CIDR",
            Box::new(|config: &mut ConfigSnapshot| {
                config.public_cidrs = vec!["10.0.0.0/99".to_owned()];
            }),
            "CIDR",
        ),
    ]
}

/// CFG-002: every config bound the Rust side owns rejects its own way, and a
/// rejection never disturbs the retained last-good generation.
///
/// Go's `ProxyServer.Check` is the primary gate, so these arms should be
/// unreachable in production — which is exactly why they had no test. The
/// store is the last line before a bad generation becomes the one every new
/// session reads, so each arm is driven here by mutating ONE field of an
/// otherwise-valid snapshot. The mutations are one-field-at-a-time on purpose:
/// a snapshot broken in two ways would pass even if only one check survived.
#[test]
fn config_bounds_are_rejected_without_disturbing_last_good() -> Result<(), Box<dyn Error>> {
    let directory = TestDirectory::create()?;
    let valid = write_valid_pair(directory.path(), "valid")?;
    let store = SnapshotStore::new([directory.path().to_path_buf()])?;
    store.apply(
        1,
        valid_snapshot(valid.clone()),
        validation_time(),
        lineage_a(),
    )?;

    let cases = numeric_config_bound_cases()
        .into_iter()
        .chain(listener_config_bound_cases());
    for (name, mutate, expected) in cases {
        let mut snapshot = valid_snapshot(valid.clone());
        let config = snapshot.config.as_mut().ok_or("missing config")?;
        mutate(config);
        let error = store
            .apply(2, snapshot, validation_time(), lineage_a())
            .err()
            .ok_or_else(|| format!("{name} was applied"))?;
        assert_eq!(error.kind(), SnapshotErrorKind::Invalid, "{name}");
        assert!(
            error.detail().contains(expected),
            "{name}: detail {:?} does not name {expected}",
            error.detail()
        );
        let current = store.current()?.ok_or("missing last-good")?;
        assert_eq!(current.generation(), 1, "{name} disturbed the last good");
    }

    // The same store still accepts a good generation afterwards: none of the
    // rejections left it wedged.
    store.apply(
        2,
        valid_snapshot(valid.clone()),
        validation_time(),
        lineage_a(),
    )?;
    assert_eq!(store.current()?.ok_or("missing last-good")?.generation(), 2);
    // Port 0 is Go's empty SQL address: the listener binds an ephemeral port.
    let mut ephemeral = valid_snapshot(valid);
    ephemeral.config.as_mut().ok_or("missing config")?.listeners[0].port = 0;
    store.apply(3, ephemeral, validation_time(), lineage_a())?;
    assert_eq!(store.current()?.ok_or("missing last-good")?.generation(), 3);
    Ok(())
}
