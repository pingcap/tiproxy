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

//! Public, credential-redacted replay errors.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

/// Error returned by replay configuration, storage, decoding, or persistence.
#[derive(Debug, Error)]
pub enum ReplayError {
    /// Invalid user configuration.
    #[error("invalid replay configuration: {0}")]
    Config(String),
    /// A storage operation failed. The path is already credential-redacted.
    #[error("storage {operation} failed for {path}: {message}")]
    Storage {
        /// Operation name.
        operation: &'static str,
        /// Safe logical path.
        path: String,
        /// Provider classification without endpoint credentials.
        message: String,
    },
    /// An input record was malformed.
    #[error("decode failed at {path}:{offset}: {message}")]
    Decode {
        /// Safe logical input path.
        path: String,
        /// Zero-based byte offset.
        offset: u64,
        /// Failure detail.
        message: String,
    },
    /// A local filesystem operation failed.
    #[error("local {operation} failed for {path:?}: {source}")]
    Io {
        /// Operation name.
        operation: &'static str,
        /// Local path.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: io::Error,
    },
    /// A checkpoint cannot be used for this replay configuration.
    #[error("checkpoint rejected: {0}")]
    Checkpoint(String),
}

impl ReplayError {
    /// Builds a decode error with stable path and offset context.
    #[must_use]
    pub fn decode(path: impl Into<String>, offset: usize, message: impl Into<String>) -> Self {
        Self::Decode {
            path: path.into(),
            offset: u64::try_from(offset).unwrap_or(u64::MAX),
            message: message.into(),
        }
    }

    /// Builds a redacted storage error from an `OpenDAL` failure.
    #[must_use]
    pub fn storage(
        operation: &'static str,
        path: impl Into<String>,
        source: &opendal::Error,
    ) -> Self {
        Self::Storage {
            operation,
            path: path.into(),
            message: source.kind().to_string(),
        }
    }
}
