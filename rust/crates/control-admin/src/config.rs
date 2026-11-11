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

//! `/api/admin/namespace/*` and `/api/admin/config/` (CP-ADMIN slice 2).
//!
//! The handlers keep the Go status codes and bounded bodies
//! (`pkg/server/api/namespace.go`, `config.go`) while the storage behind
//! them is the [`ConfigAdmin`] seam: the executable binds it to the
//! owner-fenced `ConfigModuleHandle`, so namespaces persist below `/config`
//! in etcd instead of Go's in-process B-tree, and a configuration `PUT` is
//! Go's `SetTOMLConfig` through the config owner.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use control_config::{ConfigMutationError, NamespaceConfig};
use serde_json::Value;

/// Boxed future returned by the storage seam.
pub type AdminFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a namespace commit failed, mapped to the Go handler's bodies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitError {
    /// A named namespace does not exist (`500 "failed to get namespace"`).
    Missing,
    /// The serving view did not pick up the namespaces in time
    /// (`500 "failed to reload namespaces"`).
    Reload,
}

/// Storage behind the namespace and configuration endpoints.
pub trait ConfigAdmin: Send + Sync {
    /// Name-sorted namespaces from the current accepted generation.
    fn list_namespaces(&self) -> Vec<NamespaceConfig>;
    /// One namespace by exact name.
    fn get_namespace(&self, name: &str) -> Option<NamespaceConfig>;
    /// Persists one namespace under its name.
    fn set_namespace(
        &self,
        value: NamespaceConfig,
    ) -> AdminFuture<'_, Result<(), ConfigMutationError>>;
    /// Deletes one namespace; deleting an absent name succeeds like Go's
    /// B-tree delete.
    fn delete_namespace(&self, name: String) -> AdminFuture<'_, Result<(), ConfigMutationError>>;
    /// Makes the named namespaces (all when empty) serving.
    fn commit_namespaces(&self, names: Vec<String>) -> AdminFuture<'_, Result<(), CommitError>>;
    /// Current effective configuration as TOML.
    fn config_toml(&self) -> Option<String>;
    /// Current effective configuration as JSON.
    fn config_json(&self) -> Option<String>;
    /// Go `SetTOMLConfig`: partial document merged onto the current
    /// configuration, validated as a whole.
    fn put_config_toml(&self, data: Vec<u8>) -> AdminFuture<'_, Result<(), ConfigMutationError>>;
    /// Whether the accepted configuration asks for traffic capture/replay.
    ///
    /// The native process does not capture. A true value makes the traffic
    /// routes answer `unsupported`; false keeps Go's `disabled` answer.
    fn traffic_replay_enabled(&self) -> bool;
}

/// In-memory [`ConfigAdmin`] with Go's per-process semantics, for tests and
/// the differential replay.
#[derive(Debug, Default)]
pub struct MemoryConfigAdmin {
    state: Mutex<MemoryState>,
}

#[derive(Debug, Default)]
struct MemoryState {
    namespaces: std::collections::BTreeMap<String, NamespaceConfig>,
    config: control_config::EffectiveConfig,
    current_dir: std::path::PathBuf,
    committed: Vec<String>,
}

impl MemoryConfigAdmin {
    /// Starts from `config` validated against `current_dir`.
    #[must_use]
    pub fn new(config: control_config::EffectiveConfig, current_dir: std::path::PathBuf) -> Self {
        Self {
            state: Mutex::new(MemoryState {
                namespaces: std::collections::BTreeMap::new(),
                config,
                current_dir,
                committed: Vec::new(),
            }),
        }
    }

    /// CRC32 of the current configuration, as Go reports it in the health body.
    #[must_use]
    pub fn go_checksum(&self) -> u32 {
        self.state
            .lock()
            .map(|state| state.config.go_checksum())
            .unwrap_or_default()
    }

    /// Names passed to the last successful commit.
    #[must_use]
    pub fn committed(&self) -> Vec<String> {
        self.state
            .lock()
            .map(|state| state.committed.clone())
            .unwrap_or_default()
    }
}

impl ConfigAdmin for MemoryConfigAdmin {
    fn list_namespaces(&self) -> Vec<NamespaceConfig> {
        self.state
            .lock()
            .map(|state| state.namespaces.values().cloned().collect())
            .unwrap_or_default()
    }

    fn get_namespace(&self, name: &str) -> Option<NamespaceConfig> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.namespaces.get(name).cloned())
    }

    fn set_namespace(
        &self,
        value: NamespaceConfig,
    ) -> AdminFuture<'_, Result<(), ConfigMutationError>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ConfigMutationError::Stopped)?;
            state.namespaces.insert(value.namespace.clone(), value);
            Ok(())
        })
    }

    fn delete_namespace(&self, name: String) -> AdminFuture<'_, Result<(), ConfigMutationError>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ConfigMutationError::Stopped)?;
            state.namespaces.remove(&name);
            Ok(())
        })
    }

    fn commit_namespaces(&self, names: Vec<String>) -> AdminFuture<'_, Result<(), CommitError>> {
        Box::pin(async move {
            let mut state = self.state.lock().map_err(|_| CommitError::Reload)?;
            if names
                .iter()
                .any(|name| !state.namespaces.contains_key(name))
            {
                return Err(CommitError::Missing);
            }
            state.committed = names;
            Ok(())
        })
    }

    fn config_toml(&self) -> Option<String> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.config.to_toml_string().ok())
    }

    fn config_json(&self) -> Option<String> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.config.to_json_string().ok())
    }

    fn put_config_toml(&self, data: Vec<u8>) -> AdminFuture<'_, Result<(), ConfigMutationError>> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ConfigMutationError::Stopped)?;
            let current_dir = state.current_dir.clone();
            let candidate = state
                .config
                .patched_with_toml(&data, &current_dir)
                .map_err(|_| ConfigMutationError::Invalid)?;
            candidate
                .check_reload_from(&state.config)
                .map_err(|_| ConfigMutationError::RestartRequired)?;
            state.config = candidate;
            Ok(())
        })
    }

    fn traffic_replay_enabled(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.config.traffic_replay_enabled())
            .unwrap_or(false)
    }
}

/// Field kinds of the Go structs a body decodes into, so decoding can apply
/// `encoding/json` rules per member without a generic normalization pass.
#[derive(Clone, Copy)]
pub enum Schema {
    /// Go `string`.
    Str,
    /// Go `bool`.
    Bool,
    /// Go integer.
    Int,
    /// Go `[]string`.
    StrList,
    /// Go struct with these lowercase JSON tags.
    Object(&'static [(&'static str, Schema)]),
}

/// Go `config.TLSConfig` (all tags lowercase, `omitempty`).
pub const TLS_SCHEMA: Schema = Schema::Object(&[
    ("cert", Schema::Str),
    ("key", Schema::Str),
    ("ca", Schema::Str),
    ("min-tls-version", Schema::Str),
    ("cert-allowed-cn", Schema::StrList),
    ("auto-certs", Schema::Bool),
    ("rsa-key-size", Schema::Int),
    ("autocert-expire-duration", Schema::Str),
    ("skip-ca", Schema::Bool),
]);

/// Go `config.Namespace`.
pub const NAMESPACE_SCHEMA: Schema = Schema::Object(&[
    ("namespace", Schema::Str),
    (
        "frontend",
        Schema::Object(&[("user", Schema::Str), ("security", TLS_SCHEMA)]),
    ),
    (
        "backend",
        Schema::Object(&[("instances", Schema::StrList), ("security", TLS_SCHEMA)]),
    ),
]);

/// Decodes a request body the way gin's `ShouldBindJSON` (`json.Decoder`)
/// does for a Go struct described by `schema`: one JSON value only
/// (trailing bytes ignored); `null` leaves the target unchanged; object
/// members match a field exactly or case-insensitively; unknown members
/// are skipped whatever their value; every occurrence of a known member is
/// decoded in order (a wrong type anywhere is an error, a `null` leaves the
/// field unchanged, objects merge field by field, arrays and scalars are
/// replaced by the later value).
///
/// The result is a JSON object with lowercase keys that `serde(default)`
/// types accept, or `None` for a top-level `null`.
///
/// # Errors
///
/// Returns the decode error for malformed JSON, a non-object top level, or
/// a known member of the wrong kind.
pub fn go_json_body(bytes: &[u8], schema: Schema) -> Result<Option<Value>, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let tree: Tree = serde::Deserialize::deserialize(&mut deserializer)?;
    match tree {
        Tree::Null => Ok(None),
        Tree::Object(members) => {
            let mut target = Value::Object(serde_json::Map::new());
            apply_object(&mut target, members, schema)?;
            Ok(Some(target))
        }
        other => Err(serde::de::Error::custom(format!(
            "cannot unmarshal {} into a struct",
            kind(&other)
        ))),
    }
}

fn apply_object(
    target: &mut Value,
    members: Vec<(String, Tree)>,
    schema: Schema,
) -> Result<(), serde_json::Error> {
    let Schema::Object(fields) = schema else {
        return Err(serde::de::Error::custom("object into a non-struct field"));
    };
    let Value::Object(object) = target else {
        return Err(serde::de::Error::custom("target is not an object"));
    };
    for (key, value) in members {
        // Go prefers an exact tag match, then a case-insensitive one.
        let Some((name, field_schema)) = fields
            .iter()
            .find(|(name, _)| *name == key)
            .or_else(|| {
                fields
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&key))
            })
            .copied()
        else {
            continue;
        };
        if matches!(value, Tree::Null) {
            // Go: null unmarshals into a slice by setting it to nil; into a
            // string/bool/int/struct it leaves the value unchanged.
            if matches!(field_schema, Schema::StrList) {
                object.insert(name.to_owned(), Value::Array(Vec::new()));
            }
            continue;
        }
        let slot = object.entry(name.to_owned()).or_insert(Value::Null);
        apply_value(slot, value, field_schema, name)?;
    }
    Ok(())
}

fn apply_value(
    slot: &mut Value,
    value: Tree,
    schema: Schema,
    name: &str,
) -> Result<(), serde_json::Error> {
    let mismatch = |found: &Tree| {
        serde::de::Error::custom(format!(
            "cannot unmarshal {} into field {name}",
            kind(found)
        ))
    };
    match (schema, value) {
        (Schema::Str, Tree::String(text)) => *slot = Value::String(text),
        (Schema::Bool, Tree::Bool(flag)) => *slot = Value::Bool(flag),
        (Schema::Int, Tree::Number(number)) if number.is_i64() || number.is_u64() => {
            *slot = Value::Number(number);
        }
        (Schema::StrList, Tree::Array(items)) => {
            let mut list = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Tree::String(text) => list.push(Value::String(text)),
                    Tree::Null => list.push(Value::String(String::new())),
                    other => return Err(mismatch(&other)),
                }
            }
            *slot = Value::Array(list);
        }
        (Schema::Object(_), Tree::Object(members)) => {
            if !slot.is_object() {
                *slot = Value::Object(serde_json::Map::new());
            }
            apply_object(slot, members, schema)?;
        }
        (_, other) => return Err(mismatch(&other)),
    }
    Ok(())
}

/// JSON tree that keeps duplicate object members in order.
enum Tree {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Tree>),
    Object(Vec<(String, Tree)>),
}

impl<'de> serde::Deserialize<'de> for Tree {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TreeVisitor;
        impl<'de> serde::de::Visitor<'de> for TreeVisitor {
            type Value = Tree;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("any JSON value")
            }
            fn visit_unit<E>(self) -> Result<Tree, E> {
                Ok(Tree::Null)
            }
            fn visit_bool<E>(self, value: bool) -> Result<Tree, E> {
                Ok(Tree::Bool(value))
            }
            fn visit_i64<E>(self, value: i64) -> Result<Tree, E> {
                Ok(Tree::Number(value.into()))
            }
            fn visit_u64<E>(self, value: u64) -> Result<Tree, E> {
                Ok(Tree::Number(value.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Tree, E> {
                serde_json::Number::from_f64(value)
                    .map(Tree::Number)
                    .ok_or_else(|| E::custom("non-finite number"))
            }
            fn visit_str<E>(self, value: &str) -> Result<Tree, E> {
                Ok(Tree::String(value.to_owned()))
            }
            fn visit_string<E>(self, value: String) -> Result<Tree, E> {
                Ok(Tree::String(value))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Tree, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Tree::Array(items))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Tree, A::Error> {
                let mut members = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, Tree>()? {
                    members.push((key, value));
                }
                Ok(Tree::Object(members))
            }
        }
        deserializer.deserialize_any(TreeVisitor)
    }
}

fn kind(tree: &Tree) -> &'static str {
    match tree {
        Tree::Null => "null",
        Tree::Bool(_) => "bool",
        Tree::Number(_) => "number",
        Tree::String(_) => "string",
        Tree::Array(_) => "array",
        Tree::Object(_) => "object",
    }
}

/// Shared handle type the router stores.
pub type SharedConfigAdmin = Arc<dyn ConfigAdmin>;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn body_decoder_follows_go_rules() {
        let decode = |text: &str| go_json_body(text.as_bytes(), NAMESPACE_SCHEMA);
        assert_eq!(decode("null").unwrap(), None);
        assert_eq!(
            decode("{\"Namespace\":\"a\"} trailing").unwrap(),
            Some(serde_json::json!({"namespace": "a"}))
        );
        // Duplicate objects merge field by field like sequential decoding.
        assert_eq!(
            decode("{\"frontend\":{\"user\":\"a\"},\"frontend\":{\"security\":{\"ca\":\"x\"}}}")
                .unwrap(),
            Some(serde_json::json!({"frontend": {"user": "a", "security": {"ca": "x"}}}))
        );
        // Unknown members are ignored whatever their shapes.
        assert_eq!(
            decode("{\"extra\":1,\"extra\":{\"x\":[]},\"namespace\":\"n\"}").unwrap(),
            Some(serde_json::json!({"namespace": "n"}))
        );
        // Null members leave fields unchanged; later scalars win.
        assert_eq!(
            decode("{\"namespace\":\"a\",\"namespace\":null,\"NAMESPACE\":\"b\"}").unwrap(),
            Some(serde_json::json!({"namespace": "b"}))
        );
        assert_eq!(
            decode("{\"backend\":{\"Instances\":[\"a\"],\"security\":null}}").unwrap(),
            Some(serde_json::json!({"backend": {"instances": ["a"]}}))
        );
        // A null slice member clears an earlier array (Go sets the slice to nil).
        assert_eq!(
            decode("{\"backend\":{\"instances\":[\"a\"],\"instances\":null}}").unwrap(),
            Some(serde_json::json!({"backend": {"instances": []}}))
        );
        for bad in [
            "",
            "{",
            "[]",
            "\"x\"",
            "{\"frontend\":5}",
            "{\"frontend\":{\"user\":1,\"user\":\"x\"}}",
            "{\"backend\":{\"instances\":[1]}}",
            "{\"namespace\":true}",
        ] {
            assert!(decode(bad).is_err(), "{bad}");
        }
    }
}
