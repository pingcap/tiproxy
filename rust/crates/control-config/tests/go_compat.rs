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

//! Acceptance timing shared with Go `Config.Check` and the SQL server.

use std::path::Path;
use std::sync::Arc;

use control_config::{
    ConfigError, ConfigNamespaceSource, ConfigNamespaceStore, FieldEffect, ReloadClass,
    ServingConfig, StoreError, field_contract,
};

fn load(toml: &str) -> Result<ServingConfig, StoreError> {
    Ok(
        ConfigNamespaceStore::from_toml(toml.as_bytes(), None, Path::new("/tmp"))?
            .current()
            .effective()
            .serving()?,
    )
}

#[test]
fn empty_document_serves_with_the_go_defaults() {
    let serving = load("").unwrap_or_else(|error| unreachable!("empty document: {error}"));
    assert!(serving.traffic_replay_enabled);
    assert_eq!(serving.listeners.len(), 1);
    assert_eq!(serving.listeners[0].address.as_ref(), "0.0.0.0");
    assert_eq!(serving.listeners[0].port, 6000);
}

#[test]
fn tls_versions_relative_paths_and_partial_pairs_follow_go_check() {
    let cases = [
        ("1.0", "1.0"),
        ("1.1", "1.1"),
        ("1.2", "1.2"),
        ("1.3", "1.3"),
        ("tls1.3", "1.3"),
        ("nope", "1.2"),
        ("", "1.2"),
    ];
    for (input, effective) in cases {
        let serving = load(&format!(
            "[security.server-tls]\nmin-tls-version = \"{input}\"\ncert = \"relative.pem\"\n"
        ))
        .unwrap_or_else(|error| unreachable!("{input}: {error}"));
        assert_eq!(serving.frontend_tls.minimum_version.as_ref(), effective);
        assert_eq!(
            serving
                .frontend_tls
                .certificate_path
                .as_deref()
                .and_then(|path| path.to_str()),
            Some("relative.pem")
        );
        assert!(serving.frontend_tls.private_key_path.is_none());
    }
}

#[test]
fn public_endpoints_keep_go_parse_cidr_list_order() {
    let serving = load(
        r#"
[proxy]
public-endpoints = ["2001:db8::1", "10.0.0.9", "not-an-ip", "10.0.0.0/24", "10.0.0.0/24", "10.1.2.3/24"]
"#,
    )
    .unwrap_or_else(|error| unreachable!("public endpoints: {error}"));
    let cidrs = serving
        .public_cidrs
        .iter()
        .map(Arc::as_ref)
        .collect::<Vec<_>>();
    assert_eq!(
        cidrs,
        [
            "2001:db8::/32",
            "10.0.0.9/32",
            "10.0.0.0/24",
            "10.0.0.0/24",
            "10.1.2.0/24",
        ]
    );
}

#[test]
fn keepalive_out_of_range_values_project_as_zero() {
    let serving = load(
        r#"
[proxy.frontend-keepalive]
idle = "999us"
cnt = -3
intvl = "-1s"
"#,
    )
    .unwrap_or_else(|error| unreachable!("keepalive: {error}"));
    assert_eq!(serving.frontend_keepalive.idle_millis, 0);
    assert_eq!(serving.frontend_keepalive.probe_count, 0);
    assert_eq!(serving.frontend_keepalive.interval_millis, 0);
}

#[test]
fn empty_sql_address_is_an_ephemeral_listener() {
    let serving =
        load("[proxy]\naddr = \"\"\n").unwrap_or_else(|error| unreachable!("empty addr: {error}"));
    assert_eq!(serving.listeners.len(), 1);
    assert_eq!(serving.listeners[0].name.as_ref(), "sql-0");
    assert_eq!(serving.listeners[0].address.as_ref(), "");
    assert_eq!(serving.listeners[0].port, 0);
    assert!(matches!(
        load("[proxy]\naddr = \"\"\nport-range = [6000, 6000]\n"),
        Err(StoreError::Config(ConfigError::InvalidField {
            field: "proxy.addr",
            class: "empty_with_port_range",
        }))
    ));
}

#[test]
fn routing_rule_reloads_and_restart_fields_match_the_contract() {
    let store = ConfigNamespaceStore::from_toml(b"", None, Path::new("/tmp"))
        .unwrap_or_else(|error| unreachable!("defaults: {error}"));
    store
        .apply_toml(
            b"[balance]\nrouting-rule = \"port\"\n",
            None,
            2,
            Path::new("/tmp"),
        )
        .unwrap_or_else(|error| unreachable!("routing-rule reload: {error}"));
    assert!(matches!(
        store.apply_toml(
            b"[proxy]\naddr = \"127.0.0.1:1\"\n",
            None,
            3,
            Path::new("/tmp"),
        ),
        Err(StoreError::Config(ConfigError::RestartRequired {
            field: "proxy.addr",
        }))
    ));
    for name in [
        "workdir",
        "proxy.addr",
        "proxy.advertise-addr",
        "proxy.pd-addrs",
        "proxy.port-range",
        "api",
        "log.encoder",
        "log.simple",
        "ha",
        "metering",
        "rust-dataplane",
    ] {
        let entry = field_contract()
            .iter()
            .find(|entry| entry.field == name)
            .unwrap_or_else(|| unreachable!("{name} is missing from the field contract"));
        assert_eq!(entry.reload, ReloadClass::Restart, "{name}");
    }
    let replay = field_contract()
        .iter()
        .find(|entry| entry.field == "enable-traffic-replay")
        .unwrap_or_else(|| unreachable!("traffic replay contract"));
    assert_eq!(
        (replay.reload, replay.effect),
        (ReloadClass::Dynamic, FieldEffect::Inert)
    );
}
