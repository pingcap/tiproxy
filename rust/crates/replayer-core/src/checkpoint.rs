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

//! Versioned, atomic local replay checkpoints.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};

use crate::ReplayError;

const CHECKPOINT_SCHEMA_VERSION: u32 = 1;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Digest binding a checkpoint to the exact replay input and filters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InputIdentity {
    /// Lowercase SHA-256 hexadecimal digest.
    pub sha256: String,
}

impl InputIdentity {
    /// Hashes a canonical byte representation.
    #[must_use]
    pub fn from_canonical_bytes(bytes: &[u8]) -> Self {
        let value = digest(&SHA256, bytes);
        let mut sha256 = String::with_capacity(value.as_ref().len() * 2);
        for byte in value.as_ref() {
            use std::fmt::Write as _;
            let _ = write!(sha256, "{byte:02x}");
        }
        Self { sha256 }
    }
}

/// Durable replay frontier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Checkpoint {
    schema_version: u32,
    /// Exact replay identity.
    pub input_identity: InputIdentity,
    /// Last committed command start time in Unix nanoseconds.
    pub command_start_unix_nanos: i128,
    /// Last committed command end time in Unix nanoseconds.
    pub command_end_unix_nanos: i128,
    /// Credential-redacted logical source path.
    pub source: String,
    /// Replay-local logical connection identifier.
    pub connection_id: u64,
    /// Stable input-source ordinal used to break timestamp ties.
    pub source_ordinal: u64,
    /// Stable source-local record ordinal.
    pub record_ordinal: u64,
    /// Number of commands committed before and including this frontier.
    pub committed_commands: u64,
    /// Number of read-only-filtered commands before and including this frontier.
    pub filtered_commands: u64,
}

impl Checkpoint {
    /// Creates a versioned checkpoint.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        input_identity: InputIdentity,
        command_start_unix_nanos: i128,
        command_end_unix_nanos: i128,
        source: String,
        connection_id: u64,
        source_ordinal: u64,
        record_ordinal: u64,
        committed_commands: u64,
        filtered_commands: u64,
    ) -> Self {
        Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            input_identity,
            command_start_unix_nanos,
            command_end_unix_nanos,
            source,
            connection_id,
            source_ordinal,
            record_ordinal,
            committed_commands,
            filtered_commands,
        }
    }

    /// Loads and validates a checkpoint identity. Empty and missing files are
    /// treated as no checkpoint, matching the Go command.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, is malformed, has an
    /// unsupported schema, or belongs to a different replay input.
    pub fn load(path: &Path, expected: &InputIdentity) -> Result<Option<Self>, ReplayError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(ReplayError::Io {
                    operation: "read checkpoint",
                    path: path.to_path_buf(),
                    source,
                });
            }
        };
        if bytes.is_empty() {
            return Ok(None);
        }
        let checkpoint: Self = serde_json::from_slice(&bytes)
            .map_err(|error| ReplayError::Checkpoint(format!("invalid JSON: {error}")))?;
        if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION {
            return Err(ReplayError::Checkpoint(format!(
                "unsupported schema version {}",
                checkpoint.schema_version
            )));
        }
        if &checkpoint.input_identity != expected {
            return Err(ReplayError::Checkpoint(
                "input identity does not match this replay".to_owned(),
            ));
        }
        Ok(Some(checkpoint))
    }

    /// Persists with same-directory create, file sync, rename, and directory
    /// sync. No remote path is accepted by this API.
    ///
    /// # Errors
    ///
    /// Returns an error when the checkpoint cannot be encoded or durably
    /// written to the requested local path.
    pub fn save_atomic(&self, path: &Path) -> Result<(), ReplayError> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                ReplayError::Checkpoint("checkpoint path has no file name".to_owned())
            })?;
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            sequence
        ));
        let result = self.write_temporary(&temporary).and_then(|()| {
            std::fs::rename(&temporary, path).map_err(|source| ReplayError::Io {
                operation: "rename checkpoint",
                path: path.to_path_buf(),
                source,
            })?;
            let directory = File::open(parent).map_err(|source| ReplayError::Io {
                operation: "open checkpoint directory",
                path: parent.to_path_buf(),
                source,
            })?;
            directory.sync_all().map_err(|source| ReplayError::Io {
                operation: "sync checkpoint directory",
                path: parent.to_path_buf(),
                source,
            })
        });
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result
    }

    fn write_temporary(&self, path: &Path) -> Result<(), ReplayError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| ReplayError::Io {
                operation: "create checkpoint temporary",
                path: path.to_path_buf(),
                source,
            })?;
        serde_json::to_writer(&mut file, self)
            .map_err(|error| ReplayError::Checkpoint(format!("encode JSON: {error}")))?;
        file.write_all(b"\n").map_err(|source| ReplayError::Io {
            operation: "write checkpoint temporary",
            path: path.to_path_buf(),
            source,
        })?;
        file.sync_all().map_err(|source| ReplayError::Io {
            operation: "sync checkpoint temporary",
            path: path.to_path_buf(),
            source,
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn atomic_round_trip_and_identity_fence() {
        let directory = std::env::temp_dir().join(format!(
            "tiproxy-replayer-checkpoint-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("create temp directory");
        let path = directory.join("checkpoint.json");
        let identity = InputIdentity::from_canonical_bytes(b"input-a");
        let checkpoint =
            Checkpoint::new(identity.clone(), 10, 11, "input".to_owned(), 7, 1, 2, 3, 1);
        checkpoint.save_atomic(&path).expect("save checkpoint");
        assert_eq!(
            Checkpoint::load(&path, &identity).expect("load"),
            Some(checkpoint)
        );
        let other = InputIdentity::from_canonical_bytes(b"input-b");
        assert!(Checkpoint::load(&path, &other).is_err());
        std::fs::remove_dir_all(directory).expect("remove temp directory");
    }
}
