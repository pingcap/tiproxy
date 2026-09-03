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

//! Credential-redacted `OpenDAL` input ownership.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use aes::Aes256;
use ctr::cipher::{KeyIvInit, StreamCipher};
use flate2::read::GzDecoder;
use futures_util::TryStreamExt;
use opendal::{ErrorKind, Operator, Scheme};
use serde::{Deserialize, Serialize};
use time::PrimitiveDateTime;
use url::Url;
use zeroize::Zeroizing;

use crate::{ReplayError, TrafficFormat};

const MAX_STORED_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_EXPANDED_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_METADATA_BYTES: u64 = 64 * 1024;
const STORAGE_READ_CHUNK_BYTES: usize = 1024 * 1024;

type Aes256Ctr = ctr::Ctr128BE<Aes256>;

/// Native capture metadata.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CaptureMetadata {
    /// Format version.
    pub version: String,
    /// Capture duration in nanoseconds.
    pub duration: i64,
    /// Number of captured commands.
    pub cmds: u64,
    /// Number of capture-side filtered commands.
    #[serde(default)]
    pub filtered_cmds: u64,
    /// Optional encryption method.
    #[serde(default)]
    pub encrypt_method: String,
}

/// One replay input file returned in deterministic order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputEntry {
    /// Operator-relative object path.
    pub path: String,
    /// Credential-redacted logical path.
    pub safe_path: String,
    /// Stored content length.
    pub content_length: u64,
}

/// OpenDAL-backed read/list/stat owner for one input root.
#[derive(Clone)]
pub struct InputRoot {
    operator: Operator,
    safe_root: String,
}

impl std::fmt::Debug for InputRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InputRoot")
            .field("safe_root", &self.safe_root)
            .finish_non_exhaustive()
    }
}

impl InputRoot {
    /// Parses one local or supported object-store root. Query credentials are
    /// consumed into provider configuration and never retained in diagnostics.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid roots, unsupported providers, or provider
    /// configuration failures.
    pub fn open(input: &str) -> Result<Self, ReplayError> {
        if !input.contains("://") {
            return Self::open_local(Path::new(input));
        }
        let parsed = Url::parse(input)
            .map_err(|_| ReplayError::Config("input URL is invalid".to_owned()))?;
        if parsed.scheme() == "file" {
            let path = parsed
                .to_file_path()
                .map_err(|()| ReplayError::Config("file URL is invalid".to_owned()))?;
            return Self::open_local(&path);
        }
        let scheme = match parsed.scheme() {
            "s3" => Scheme::S3,
            "gcs" | "gs" => Scheme::Gcs,
            "azblob" | "azure" => Scheme::Azblob,
            "oss" => Scheme::Oss,
            "cos" => Scheme::Cos,
            other => {
                return Err(ReplayError::Config(format!(
                    "unsupported input scheme {other}"
                )));
            }
        };
        let authority = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| ReplayError::Config("object-store URL has no bucket".to_owned()))?;
        let mut config = HashMap::new();
        match scheme {
            Scheme::Azblob => {
                config.insert("container".to_owned(), authority.to_owned());
            }
            _ => {
                config.insert("bucket".to_owned(), authority.to_owned());
            }
        }
        let root = parsed.path().trim_matches('/');
        config.insert("root".to_owned(), format!("/{root}"));
        for (key, value) in parsed.query_pairs() {
            if let Some((normalized, value)) = provider_query_option(scheme, &key, &value)? {
                config.insert(normalized, value);
            }
        }
        let operator = Operator::via_iter(scheme, config)
            .map_err(|error| ReplayError::storage("configure", safe_uri(&parsed), &error))?;
        Ok(Self {
            operator,
            safe_root: safe_uri(&parsed),
        })
    }

    fn open_local(path: &Path) -> Result<Self, ReplayError> {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|source| ReplayError::Io {
                    operation: "resolve input root",
                    path: path.to_path_buf(),
                    source,
                })?
                .join(path)
        };
        let root = path.canonicalize().map_err(|source| ReplayError::Io {
            operation: "canonicalize input root",
            path: path.clone(),
            source,
        })?;
        let root_text = root
            .to_str()
            .ok_or_else(|| ReplayError::Config("input root is not valid UTF-8".to_owned()))?
            .to_owned();
        let operator = Operator::via_iter(Scheme::Fs, [("root".to_owned(), root_text.clone())])
            .map_err(|error| ReplayError::storage("configure", &root_text, &error))?;
        Ok(Self {
            operator,
            safe_root: root_text,
        })
    }

    /// Reads optional native metadata. Missing metadata is compatible with Go.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata storage or JSON decoding fails.
    pub async fn metadata(&self) -> Result<Option<CaptureMetadata>, ReplayError> {
        match self.operator.stat("meta").await {
            Ok(metadata) => {
                if metadata.content_length() > MAX_METADATA_BYTES {
                    return Err(ReplayError::decode(
                        self.safe_path("meta"),
                        0,
                        format!("metadata exceeds {MAX_METADATA_BYTES} bytes"),
                    ));
                }
                let buffer = self
                    .read_object_bounded("meta", &self.safe_path("meta"), MAX_METADATA_BYTES)
                    .await?;
                let metadata: CaptureMetadata =
                    serde_json::from_slice(buffer.as_slice()).map_err(|error| {
                        ReplayError::decode(
                            self.safe_path("meta"),
                            0,
                            format!("invalid meta JSON: {error}"),
                        )
                    })?;
                if metadata.version != "v1" {
                    return Err(ReplayError::decode(
                        self.safe_path("meta"),
                        0,
                        format!("unsupported native capture version {}", metadata.version),
                    ));
                }
                Ok(Some(metadata))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ReplayError::storage("stat", self.safe_path("meta"), &error)),
        }
    }

    /// Lists replay inputs recursively in timestamp/path order.
    ///
    /// # Errors
    ///
    /// Returns an error when listing or metadata lookup fails, or when an
    /// input exceeds the stored-file bound.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    pub async fn list(&self, format: TrafficFormat) -> Result<Vec<InputEntry>, ReplayError> {
        let prefix = if format.is_audit() {
            "tidb-audit-"
        } else {
            "traffic-"
        };
        let entries = self
            .operator
            .list_with("")
            .recursive(true)
            .await
            .map_err(|error| ReplayError::storage("list", &self.safe_root, &error))?;
        let mut output = Vec::new();
        for entry in entries {
            let path = entry.path();
            if capture_timestamp(path, prefix).is_none() {
                continue;
            }
            let metadata = self
                .operator
                .stat(path)
                .await
                .map_err(|error| ReplayError::storage("stat", self.safe_path(path), &error))?;
            if metadata.is_file() && metadata.content_length() <= MAX_STORED_FILE_BYTES {
                output.push(InputEntry {
                    path: path.to_owned(),
                    safe_path: self.safe_path(path),
                    content_length: metadata.content_length(),
                });
            } else if metadata.is_file() {
                return Err(ReplayError::decode(
                    self.safe_path(path),
                    0,
                    format!("stored file exceeds {MAX_STORED_FILE_BYTES} bytes"),
                ));
            }
        }
        output.sort_by(|left, right| {
            file_timestamp_key(&left.path)
                .cmp(file_timestamp_key(&right.path))
                .then_with(|| left.path.cmp(&right.path))
        });
        Ok(output)
    }

    /// Reads, decompresses, and decrypts one bounded input file.
    ///
    /// # Errors
    ///
    /// Returns an error when the object cannot be read, expanded within its
    /// bound, or decrypted with the configured metadata and key.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    pub async fn read_decoded(
        &self,
        entry: &InputEntry,
        metadata: Option<&CaptureMetadata>,
        key_file: Option<&Path>,
    ) -> Result<Vec<u8>, ReplayError> {
        let stored = self
            .read_object_bounded(&entry.path, &entry.safe_path, MAX_STORED_FILE_BYTES)
            .await?;
        let expanded = if entry.path.ends_with(".gz") {
            let mut decoder = GzDecoder::new(stored.as_slice());
            read_bounded(&mut decoder, MAX_EXPANDED_FILE_BYTES, &entry.safe_path)?
        } else {
            stored
        };
        decrypt(expanded, metadata, key_file, &entry.safe_path)
    }

    /// Credential-redacted root identifier.
    #[must_use]
    pub fn safe_root(&self) -> &str {
        &self.safe_root
    }

    fn safe_path(&self, path: &str) -> String {
        if path.is_empty() {
            self.safe_root.clone()
        } else {
            format!("{}/{path}", self.safe_root.trim_end_matches('/'))
        }
    }

    async fn read_object_bounded(
        &self,
        path: &str,
        safe_path: &str,
        limit: u64,
    ) -> Result<Vec<u8>, ReplayError> {
        let reader = self
            .operator
            .reader_with(path)
            .chunk(STORAGE_READ_CHUNK_BYTES)
            .await
            .map_err(|error| ReplayError::storage("read", safe_path, &error))?;
        let mut stream = reader
            .into_stream(..)
            .await
            .map_err(|error| ReplayError::storage("read", safe_path, &error))?;
        let mut output = Vec::new();
        while let Some(buffer) = stream
            .try_next()
            .await
            .map_err(|error| ReplayError::storage("read", safe_path, &error))?
        {
            let next_len = output
                .len()
                .checked_add(buffer.len())
                .ok_or_else(|| ReplayError::decode(safe_path, 0, "stored size overflow"))?;
            reject_oversize(next_len, limit, safe_path, "stored object")?;
            for bytes in buffer {
                output.extend_from_slice(&bytes);
            }
        }
        Ok(output)
    }
}

fn reject_oversize(actual: usize, limit: u64, path: &str, kind: &str) -> Result<(), ReplayError> {
    if u64::try_from(actual).unwrap_or(u64::MAX) > limit {
        return Err(ReplayError::decode(
            path,
            0,
            format!("{kind} exceeds {limit} bytes"),
        ));
    }
    Ok(())
}

fn read_bounded(reader: &mut impl Read, limit: u64, path: &str) -> Result<Vec<u8>, ReplayError> {
    let mut output = Vec::new();
    reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut output)
        .map_err(|source| ReplayError::Io {
            operation: "decompress input",
            path: PathBuf::from(path),
            source,
        })?;
    if u64::try_from(output.len()).unwrap_or(u64::MAX) > limit {
        return Err(ReplayError::decode(
            path,
            0,
            format!("expanded file exceeds {limit} bytes"),
        ));
    }
    Ok(output)
}

fn decrypt(
    mut input: Vec<u8>,
    metadata: Option<&CaptureMetadata>,
    key_file: Option<&Path>,
    path: &str,
) -> Result<Vec<u8>, ReplayError> {
    let method = metadata.map_or("", |metadata| metadata.encrypt_method.as_str());
    match method.to_ascii_lowercase().as_str() {
        "" | "plaintext" => Ok(input),
        "aes256-ctr" => {
            let key_path = key_file.ok_or_else(|| {
                ReplayError::Config("security.encryption-key-path is not set".to_owned())
            })?;
            let key =
                Zeroizing::new(std::fs::read(key_path).map_err(|source| ReplayError::Io {
                    operation: "read encryption key",
                    path: key_path.to_path_buf(),
                    source,
                })?);
            if key.len() < 32 {
                return Err(ReplayError::Config(format!(
                    "invalid AES-256 key length {}, expecting at least 32",
                    key.len()
                )));
            }
            if input.len() < 16 {
                return Err(ReplayError::decode(
                    path,
                    0,
                    "encrypted input has a short IV",
                ));
            }
            let (iv, body) = input.split_at_mut(16);
            let mut cipher = Aes256Ctr::new_from_slices(&key[..32], iv)
                .map_err(|_| ReplayError::Config("invalid AES-256 key or IV".to_owned()))?;
            cipher.apply_keystream(body);
            Ok(body.to_vec())
        }
        other => Err(ReplayError::Config(format!(
            "unsupported encrypt method {other}"
        ))),
    }
}

fn provider_query_option(
    scheme: Scheme,
    key: &str,
    value: &str,
) -> Result<Option<(String, String)>, ReplayError> {
    let canonical = key.replace('-', "_");
    if canonical == "provider" {
        return Ok(None);
    }
    if scheme == Scheme::S3 && canonical == "force_path_style" {
        let force_path_style = value.parse::<bool>().map_err(|_| {
            ReplayError::Config("force-path-style must be true or false".to_owned())
        })?;
        return Ok(Some((
            "enable_virtual_host_style".to_owned(),
            (!force_path_style).to_string(),
        )));
    }
    let normalized = match (scheme, canonical.as_str()) {
        (Scheme::S3 | Scheme::Oss, "access_key" | "access_key_id") => "access_key_id",
        (Scheme::Cos, "access_key" | "access_key_id" | "secret_id") => "secret_id",
        (Scheme::S3, "secret_access_key") => "secret_access_key",
        (Scheme::Oss, "secret_access_key" | "access_key_secret") => "access_key_secret",
        (Scheme::Cos, "secret_access_key" | "secret_key") => "secret_key",
        (Scheme::Gcs, "credentials_file") => "credential_path",
        _ => canonical.as_str(),
    };
    Ok(Some((normalized.to_owned(), value.to_owned())))
}

fn safe_uri(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    format!("{}://{}{}", url.scheme(), host, url.path())
}

fn file_timestamp_key(path: &str) -> &str {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.strip_prefix("traffic-")
        .or_else(|| name.strip_prefix("tidb-audit-"))
        .and_then(|value| value.strip_suffix(".gz").or(Some(value)))
        .and_then(|value| value.strip_suffix(".log"))
        .unwrap_or(name)
}

fn capture_timestamp<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    let name = name.strip_prefix(prefix)?;
    let name = name.strip_suffix(".gz").unwrap_or(name);
    let timestamp = name.strip_suffix(".log")?;
    if let Some((_, fraction)) = timestamp.split_once('.')
        && (fraction.is_empty()
            || fraction.len() > 3
            || !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    let description = if timestamp.contains('.') {
        "[year]-[month]-[day]T[hour]-[minute]-[second].[subsecond]"
    } else {
        "[year]-[month]-[day]T[hour]-[minute]-[second]"
    };
    let format = time::format_description::parse_borrowed::<2>(description).ok()?;
    PrimitiveDateTime::parse(timestamp, &format).ok()?;
    Some(timestamp)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_list_is_timestamp_then_path_ordered() {
        let directory =
            std::env::temp_dir().join(format!("tiproxy-replayer-storage-{}", std::process::id()));
        std::fs::create_dir_all(directory.join("nested")).expect("create directory");
        std::fs::write(directory.join("traffic-2026-01-01T00-00-01.000.log"), b"a")
            .expect("write file");
        std::fs::write(
            directory
                .join("nested")
                .join("traffic-2026-01-01T00-00-00.000.log"),
            b"ignored nested input",
        )
        .expect("write file");
        std::fs::write(directory.join("traffic-2026-01-01T00-00-00.000.log"), b"b")
            .expect("write file");
        std::fs::write(directory.join("traffic-2026-01-01T00-00-02.log"), b"c")
            .expect("write file");
        std::fs::write(directory.join("traffic-not-a-time.log"), b"bad")
            .expect("write invalid file");
        std::fs::write(directory.join("traffic-2026-99-01T00-00-03.log"), b"bad")
            .expect("write invalid date");
        std::fs::write(directory.join("ignore.txt"), b"x").expect("write ignored file");
        let root = InputRoot::open(directory.to_str().expect("UTF-8 path")).expect("open root");
        let files = root.list(TrafficFormat::Native).await.expect("list files");
        assert_eq!(files.len(), 3);
        assert!(files[0].path.contains("00-00-00.000"));
        assert!(files[1].path.contains("00-00-01.000"));
        assert!(files[2].path.contains("00-00-02.log"));
        std::fs::remove_dir_all(directory).expect("remove directory");
    }

    #[tokio::test]
    async fn native_metadata_rejects_unknown_version() {
        let directory =
            std::env::temp_dir().join(format!("tiproxy-replayer-meta-{}", std::process::id()));
        if directory.exists() {
            std::fs::remove_dir_all(&directory).expect("remove stale directory");
        }
        std::fs::create_dir_all(&directory).expect("create directory");
        std::fs::write(
            directory.join("meta"),
            br#"{"Version":"v2","Duration":0,"Cmds":0}"#,
        )
        .expect("write metadata");
        let root = InputRoot::open(directory.to_str().expect("UTF-8 path")).expect("open root");
        let error = root.metadata().await.expect_err("unknown version");
        assert!(
            error
                .to_string()
                .contains("unsupported native capture version v2")
        );
        std::fs::remove_dir_all(directory).expect("remove directory");
    }

    #[test]
    fn provider_options_map_to_native_opendal_fields() {
        assert_eq!(
            provider_query_option(Scheme::S3, "force-path-style", "true").expect("option"),
            Some(("enable_virtual_host_style".to_owned(), "false".to_owned()))
        );
        assert_eq!(
            provider_query_option(Scheme::Oss, "secret-access-key", "secret").expect("option"),
            Some(("access_key_secret".to_owned(), "secret".to_owned()))
        );
        assert_eq!(
            provider_query_option(Scheme::Cos, "access-key", "id").expect("option"),
            Some(("secret_id".to_owned(), "id".to_owned()))
        );
        assert_eq!(
            provider_query_option(Scheme::Gcs, "credentials-file", "/tmp/key").expect("option"),
            Some(("credential_path".to_owned(), "/tmp/key".to_owned()))
        );
        assert_eq!(
            provider_query_option(Scheme::S3, "provider", "minio").expect("option"),
            None
        );
        assert!(provider_query_option(Scheme::S3, "force-path-style", "maybe").is_err());
    }

    #[test]
    fn uri_diagnostics_drop_credentials() {
        let parsed =
            Url::parse("s3://bucket/prefix?access-key=secret-one&secret-access-key=secret-two")
                .expect("URL");
        let safe = safe_uri(&parsed);
        assert_eq!(safe, "s3://bucket/prefix");
        assert!(!safe.contains("secret"));
    }

    #[test]
    fn post_read_bounds_reject_object_growth() {
        assert!(reject_oversize(65, 64, "traffic.log", "stored file").is_err());
        assert!(reject_oversize(64, 64, "traffic.log", "stored file").is_ok());
    }
}
