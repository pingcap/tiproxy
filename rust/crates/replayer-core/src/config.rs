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

//! Replay configuration and compatibility validation.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{PreparedCloseStrategy, ReplayError, TrafficFormat};

/// Maximum native payload and audit-log line size.
pub const DEFAULT_RECORD_LIMIT: usize = 64 * 1024 * 1024;

/// Validated replay job configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ReplayConfig {
    /// Comma-separated input roots for audit logs, or one native root.
    pub input: String,
    /// Replay speed multiplier.
    pub speed: f64,
    /// Backend username.
    pub username: String,
    /// Backend address.
    pub address: String,
    /// Input format.
    pub format: TrafficFormat,
    /// Replay only commands classified as read-only.
    pub read_only: bool,
    /// Absolute replay start time.
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: OffsetDateTime,
    /// Ignore recoverable command failures.
    pub ignore_errors: bool,
    /// Audit reorder buffer size.
    pub reorder_buffer: usize,
    /// Prepared-statement close policy.
    pub prepared_close: PreparedCloseStrategy,
    /// Perform the entire input path without connecting to `TiDB`.
    pub dry_run: bool,
    /// Optional local checkpoint path.
    pub checkpoint_path: Option<PathBuf>,
    /// Watch for new traffic directories.
    pub dynamic_input: bool,
    /// Number of dynamic-input replayer owners.
    pub replayer_count: u64,
    /// This process's dynamic-input owner index.
    pub replayer_index: u64,
    /// Optional local SQL output path.
    pub output_path: Option<PathBuf>,
    /// Filter audit commands marked as retries.
    pub filter_command_with_retry: bool,
    /// Case-insensitive audit-log user allowlist.
    pub user_allowlist: Vec<String>,
    /// Continue watching after the current input reaches EOF.
    pub wait_on_eof: bool,
    /// Optional command start-time frontier.
    #[serde(with = "time::serde::rfc3339::option")]
    pub command_start_time: Option<OffsetDateTime>,
    /// Optional command end-time frontier.
    #[serde(with = "time::serde::rfc3339::option")]
    pub command_end_time: Option<OffsetDateTime>,
    /// Optional AES-256 key path.
    pub key_file: Option<PathBuf>,
    /// Per-record allocation limit.
    pub record_limit: usize,
}

impl ReplayConfig {
    /// Validates the public Go-compatible replay configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when fields violate the Go CLI compatibility rules or
    /// the local safety bounds.
    pub fn validate(&mut self, now: OffsetDateTime) -> Result<(), ReplayError> {
        if self.input.trim().is_empty() {
            return Err(ReplayError::Config("input is required".to_owned()));
        }
        let input_count = self.input.split(',').count();
        if input_count > 1 && !self.format.is_audit() {
            return Err(ReplayError::Config(
                "only `audit_log_plugin` format supports multiple input files".to_owned(),
            ));
        }
        if !self.dry_run && self.username.is_empty() {
            return Err(ReplayError::Config("username is required".to_owned()));
        }
        if self.speed == 0.0 {
            self.speed = 1.0;
        } else if !(0.1..=10.0).contains(&self.speed) || !self.speed.is_finite() {
            return Err(ReplayError::Config(
                "speed should be between 0.1 and 10".to_owned(),
            ));
        }
        let latest_allowed_now = self
            .start_time
            .checked_add(time::Duration::seconds(60))
            .ok_or_else(|| ReplayError::Config("start time grace period overflows".to_owned()))?;
        if latest_allowed_now < now {
            return Err(ReplayError::Config(
                "start time should not be in the past".to_owned(),
            ));
        }
        if self.format == TrafficFormat::Native
            && self.prepared_close != PreparedCloseStrategy::Directed
        {
            return Err(ReplayError::Config(
                "only `directed` prepared statement close strategy is supported for `native` format"
                    .to_owned(),
            ));
        }
        if !self.format.is_audit() && self.command_end_time.is_some() {
            return Err(ReplayError::Config(
                "command end time is only supported for audit formats".to_owned(),
            ));
        }
        if self.format == TrafficFormat::AuditLogExtension {
            if self.prepared_close == PreparedCloseStrategy::Directed {
                return Err(ReplayError::Config(
                    "prepared statement directed close strategy is not supported for audit log plugin v2 format"
                        .to_owned(),
                ));
            }
            if self.filter_command_with_retry {
                return Err(ReplayError::Config(
                    "filtering commands with retry is not supported for audit log plugin v2 format"
                        .to_owned(),
                ));
            }
        }
        if self.dynamic_input {
            if input_count != 1 {
                return Err(ReplayError::Config(
                    "dynamic input cannot be enabled with more than one input".to_owned(),
                ));
            }
            if self.replayer_count == 0 || self.replayer_index >= self.replayer_count {
                return Err(ReplayError::Config(
                    "dynamic input requires a valid replayer count and index".to_owned(),
                ));
            }
        }
        if self.record_limit == 0 || self.record_limit > DEFAULT_RECORD_LIMIT {
            return Err(ReplayError::Config(format!(
                "record limit must be between 1 and {DEFAULT_RECORD_LIMIT}"
            )));
        }
        Ok(())
    }

    /// Returns normalized input roots without accepting empty members.
    ///
    /// # Errors
    ///
    /// Returns an error when any comma-separated input root is empty.
    pub fn inputs(&self) -> Result<Vec<&str>, ReplayError> {
        self.input
            .split(',')
            .map(str::trim)
            .map(|input| {
                if input.is_empty() {
                    Err(ReplayError::Config(
                        "input contains an empty root".to_owned(),
                    ))
                } else {
                    Ok(input)
                }
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn config(now: OffsetDateTime) -> ReplayConfig {
        ReplayConfig {
            input: "capture".to_owned(),
            speed: 1.0,
            username: "root".to_owned(),
            address: "127.0.0.1:4000".to_owned(),
            format: TrafficFormat::Native,
            read_only: false,
            start_time: now,
            ignore_errors: false,
            reorder_buffer: 100_000,
            prepared_close: PreparedCloseStrategy::Directed,
            dry_run: true,
            checkpoint_path: None,
            dynamic_input: false,
            replayer_count: 1,
            replayer_index: 0,
            output_path: None,
            filter_command_with_retry: false,
            user_allowlist: Vec::new(),
            wait_on_eof: false,
            command_start_time: None,
            command_end_time: None,
            key_file: None,
            record_limit: DEFAULT_RECORD_LIMIT,
        }
    }

    #[test]
    fn matches_go_validation_edges() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let mut valid = config(now);
        valid.speed = 0.0;
        assert!(valid.validate(now).is_ok());
        assert!((valid.speed - 1.0).abs() < f64::EPSILON);

        let mut multiple_native = config(now);
        multiple_native.input = "one,two".to_owned();
        assert!(multiple_native.validate(now).is_err());

        let mut bad_dynamic = config(now);
        bad_dynamic.dynamic_input = true;
        bad_dynamic.replayer_count = 1;
        bad_dynamic.replayer_index = 1;
        assert!(bad_dynamic.validate(now).is_err());

        let mut extension = config(now);
        extension.format = TrafficFormat::AuditLogExtension;
        assert!(extension.validate(now).is_err());
    }

    #[test]
    fn start_time_grace_period_overflow_fails_closed() {
        let maximum = time::Date::MAX
            .with_hms(23, 59, 59)
            .expect("valid maximum time")
            .assume_utc();
        let mut overflow = config(maximum);
        overflow.start_time = maximum;
        let error = overflow
            .validate(maximum)
            .expect_err("overflow must fail closed");
        assert!(error.to_string().contains("grace period overflows"));
    }
}
