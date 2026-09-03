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

//! Deterministic dry-run replay path.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::checkpoint::{Checkpoint, InputIdentity};
use crate::config::ReplayConfig;
use crate::decode::{AuditDecoder, NativeDecoder};
use crate::storage::InputRoot;
use crate::{Command, CommandCode, ReplayError, TrafficFormat};

/// Semantic output of a complete dry run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DryRunSummary {
    /// Number of decoded commands before replay-side filtering.
    pub decoded_commands: u64,
    /// Number of commands accepted by replay-side filtering.
    pub replayed_commands: u64,
    /// Number of replay-side filtered commands.
    pub filtered_commands: u64,
    /// Number of deterministic input files consumed.
    pub input_files: u64,
    /// Last committed command timestamp in RFC3339.
    pub last_command_time: Option<String>,
    /// Exact input identity used by the checkpoint.
    pub input_identity: InputIdentity,
}

/// Runs the full local/OpenDAL read, decrypt, decompress, decode, ordering,
/// filtering, and atomic-checkpoint path without a backend connection.
///
/// # Errors
///
/// Returns an error when configuration, storage, decoding, filtering, or
/// checkpoint persistence fails closed.
#[allow(clippy::too_many_lines)]
pub async fn dry_run(config: &ReplayConfig) -> Result<DryRunSummary, ReplayError> {
    if !config.dry_run {
        return Err(ReplayError::Config(
            "dry_run requires dry-run configuration".to_owned(),
        ));
    }
    let inputs = config.inputs()?;
    let mut commands = Vec::new();
    let mut identity_parts = Vec::new();
    let mut input_files = 0_u64;
    for (source_index, input) in inputs.iter().enumerate() {
        let source_ordinal = u64::try_from(source_index)
            .map_err(|_| ReplayError::Config("too many input roots".to_owned()))?;
        let root = InputRoot::open(input)?;
        let metadata = if config.format == TrafficFormat::Native {
            root.metadata().await?
        } else {
            None
        };
        let entries = root.list(config.format).await?;
        identity_parts.extend_from_slice(root.safe_root().as_bytes());
        identity_parts.push(0);
        if let Some(metadata) = metadata.as_ref() {
            identity_parts.extend_from_slice(
                &serde_json::to_vec(metadata).map_err(|error| {
                    ReplayError::Checkpoint(format!("encode metadata: {error}"))
                })?,
            );
        }
        let mut audit = if config.format.is_audit() {
            Some(AuditDecoder::new(
                config.format,
                root.safe_root(),
                config.record_limit,
                config.prepared_close,
                config.command_start_time,
                config.command_end_time,
                config.filter_command_with_retry,
                &config.user_allowlist,
                source_ordinal,
            )?)
        } else {
            None
        };
        for entry in entries {
            input_files = input_files.saturating_add(1);
            identity_parts.extend_from_slice(entry.path.as_bytes());
            identity_parts.extend_from_slice(&entry.content_length.to_le_bytes());
            identity_parts.push(0);
            let bytes = root
                .read_decoded(&entry, metadata.as_ref(), config.key_file.as_deref())
                .await?;
            let content_identity = InputIdentity::from_canonical_bytes(&bytes);
            identity_parts.extend_from_slice(content_identity.sha256.as_bytes());
            identity_parts.push(0);
            match config.format {
                TrafficFormat::Native => {
                    let mut decoder = NativeDecoder::new(
                        &bytes,
                        &entry.safe_path,
                        config.record_limit,
                        config.command_start_time,
                    );
                    while let Some(mut command) = decoder.next_command()? {
                        command.source_ordinal = source_ordinal;
                        commands.push(command);
                    }
                }
                TrafficFormat::AuditLogPlugin | TrafficFormat::AuditLogExtension => {
                    let decoder = audit.as_mut().ok_or_else(|| {
                        ReplayError::Config("audit decoder was not constructed".to_owned())
                    })?;
                    commands.extend(decoder.decode_all(&bytes)?);
                }
            }
        }
    }
    identity_parts.extend_from_slice(
        &serde_json::to_vec(&IdentityConfig::from(config))
            .map_err(|error| ReplayError::Checkpoint(format!("encode identity: {error}")))?,
    );
    let input_identity = InputIdentity::from_canonical_bytes(&identity_parts);
    let checkpoint = match config.checkpoint_path.as_deref() {
        Some(path) => Checkpoint::load(path, &input_identity)?,
        None => None,
    };
    commands.sort_by(|left, right| {
        left.start_time
            .cmp(&right.start_time)
            .then_with(|| left.connection_id.cmp(&right.connection_id))
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
            .then_with(|| left.record_ordinal.cmp(&right.record_ordinal))
    });
    if let Some(frontier) = checkpoint.as_ref() {
        commands.retain(|command| {
            (
                command.start_time.unix_timestamp_nanos(),
                command.connection_id,
                command.source_ordinal,
                command.record_ordinal,
            ) > (
                frontier.command_start_unix_nanos,
                frontier.connection_id,
                frontier.source_ordinal,
                frontier.record_ordinal,
            )
        });
    }
    let decoded_commands = u64::try_from(commands.len()).unwrap_or(u64::MAX);
    let mut filtered_commands = 0_u64;
    let mut read_only_statements = HashMap::new();
    let mut last = None;
    for command in &commands {
        let accepted = !config.read_only
            || command_is_read_only(command, &mut read_only_statements, config.record_limit)?;
        if !accepted {
            filtered_commands = filtered_commands.saturating_add(1);
        }
        last = Some(command);
    }
    let replayed_commands = decoded_commands.saturating_sub(filtered_commands);
    if let (Some(path), Some(command)) = (config.checkpoint_path.as_deref(), last) {
        let checkpoint = Checkpoint::new(
            input_identity.clone(),
            command.start_time.unix_timestamp_nanos(),
            command
                .end_time
                .unwrap_or(command.start_time)
                .unix_timestamp_nanos(),
            command.source.clone(),
            command.connection_id,
            command.source_ordinal,
            command.record_ordinal,
            checkpoint
                .as_ref()
                .map_or(0, |value| value.committed_commands)
                .saturating_add(replayed_commands),
            checkpoint
                .as_ref()
                .map_or(0, |value| value.filtered_commands)
                .saturating_add(filtered_commands),
        );
        checkpoint.save_atomic(path)?;
    }
    Ok(DryRunSummary {
        decoded_commands,
        replayed_commands,
        filtered_commands,
        input_files,
        last_command_time: last.and_then(|command| {
            command
                .start_time
                .format(&time::format_description::well_known::Rfc3339)
                .ok()
        }),
        input_identity,
    })
}

#[derive(Serialize)]
struct IdentityConfig<'a> {
    format: TrafficFormat,
    read_only: bool,
    prepared_close: crate::PreparedCloseStrategy,
    filter_command_with_retry: bool,
    user_allowlist: &'a [String],
    command_start_time: Option<i128>,
    command_end_time: Option<i128>,
    dynamic_input: bool,
    replayer_count: u64,
    replayer_index: u64,
}

impl<'a> From<&'a ReplayConfig> for IdentityConfig<'a> {
    fn from(config: &'a ReplayConfig) -> Self {
        Self {
            format: config.format,
            read_only: config.read_only,
            prepared_close: config.prepared_close,
            filter_command_with_retry: config.filter_command_with_retry,
            user_allowlist: &config.user_allowlist,
            command_start_time: config
                .command_start_time
                .map(time::OffsetDateTime::unix_timestamp_nanos),
            command_end_time: config
                .command_end_time
                .map(time::OffsetDateTime::unix_timestamp_nanos),
            dynamic_input: config.dynamic_input,
            replayer_count: config.replayer_count,
            replayer_index: config.replayer_index,
        }
    }
}

fn command_is_read_only(
    command: &Command,
    prepared: &mut HashMap<u64, HashSet<u32>>,
    limit: usize,
) -> Result<bool, ReplayError> {
    match command.command {
        CommandCode::QUERY | CommandCode::STMT_PREPARE => {
            let sql = std::str::from_utf8(command.payload.get(1..).unwrap_or_default()).map_err(
                |_| ReplayError::decode(&command.source, 0, "SQL payload is not valid UTF-8"),
            )?;
            let read_only = sql_is_read_only(sql, limit)?;
            if command.command == CommandCode::STMT_PREPARE
                && read_only
                && let Some(statement_id) = command.captured_statement_id
            {
                prepared
                    .entry(command.connection_id)
                    .or_default()
                    .insert(statement_id);
            }
            Ok(read_only)
        }
        CommandCode::STMT_CLOSE => Ok(command.captured_statement_id.is_some_and(|statement_id| {
            prepared
                .get_mut(&command.connection_id)
                .is_some_and(|statements| statements.remove(&statement_id))
        })),
        CommandCode::STMT_EXECUTE
        | CommandCode::STMT_SEND_LONG_DATA
        | CommandCode::STMT_RESET
        | CommandCode::STMT_FETCH => {
            Ok(command.captured_statement_id.is_some_and(|statement_id| {
                prepared
                    .get(&command.connection_id)
                    .is_some_and(|statements| statements.contains(&statement_id))
            }))
        }
        CommandCode::CREATE_DB | CommandCode::DROP_DB | CommandCode::DELAYED_INSERT => Ok(false),
        CommandCode::QUIT => {
            prepared.remove(&command.connection_id);
            Ok(true)
        }
        _ => Ok(true),
    }
}

fn sql_is_read_only(sql: &str, limit: usize) -> Result<bool, ReplayError> {
    let tokens = tokenize(sql, limit)?;
    let first = tokens.first().map(String::as_str).unwrap_or_default();
    match first {
        "SELECT" => Ok(!tokens
            .windows(2)
            .any(|pair| pair[0] == "FOR" && pair[1] == "UPDATE")),
        "SHOW" | "WITH" | "USE" | "DESC" | "DESCRIBE" | "TABLE" | "DO" | "BEGIN" | "COMMIT"
        | "ROLLBACK" => Ok(true),
        "START" => Ok(tokens.get(1).map(String::as_str) == Some("TRANSACTION")),
        "SET" => {
            let second = tokens.get(1).map(String::as_str).unwrap_or_default();
            Ok(matches!(
                second,
                "SESSION_STATES"
                    | "SESSION"
                    | "NAMES"
                    | "CHAR"
                    | "CHARSET"
                    | "CHARACTER"
                    | "TRANSACTION"
            ) || second.starts_with('@') && !second.starts_with("@@GLOBAL"))
        }
        _ => Ok(false),
    }
}

fn tokenize(sql: &str, limit: usize) -> Result<Vec<String>, ReplayError> {
    if sql.len() > limit {
        return Err(ReplayError::Config("SQL exceeds record limit".to_owned()));
    }
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0_usize;
    while index < bytes.len() {
        match bytes[index] {
            byte if byte.is_ascii_whitespace() || matches!(byte, b'(' | b')' | b',' | b';') => {
                index += 1;
            }
            b'#' => {
                index = skip_line_comment(bytes, index + 1);
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index = skip_line_comment(bytes, index + 2);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_block_comment(bytes, index + 2)?;
            }
            quote @ (b'\'' | b'"' | b'`') => {
                index = skip_quoted(bytes, index + 1, quote)?;
            }
            _ => {
                let start = index;
                while index < bytes.len()
                    && !bytes[index].is_ascii_whitespace()
                    && !matches!(
                        bytes[index],
                        b'(' | b')' | b',' | b';' | b'\'' | b'"' | b'`'
                    )
                {
                    index += 1;
                }
                let token = sql.get(start..index).ok_or_else(|| {
                    ReplayError::Config("SQL token is not on UTF-8 boundaries".to_owned())
                })?;
                tokens.push(token.to_ascii_uppercase());
                if tokens.len() > 1_000_000 {
                    return Err(ReplayError::Config("SQL token limit exceeded".to_owned()));
                }
            }
        }
    }
    Ok(tokens)
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> Result<usize, ReplayError> {
    while index + 1 < bytes.len() {
        if bytes[index] == b'*' && bytes[index + 1] == b'/' {
            return Ok(index + 2);
        }
        index += 1;
    }
    Err(ReplayError::Config(
        "unterminated SQL block comment".to_owned(),
    ))
}

fn skip_quoted(bytes: &[u8], mut index: usize, quote: u8) -> Result<usize, ReplayError> {
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index = index.saturating_add(2);
            continue;
        }
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            return Ok(index + 1);
        }
        index += 1;
    }
    Err(ReplayError::Config(
        "unterminated SQL quoted construct".to_owned(),
    ))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    #[test]
    fn readonly_filter_ignores_literals_and_comments() {
        assert!(sql_is_read_only("/* FOR UPDATE */ SELECT 'for update'", 1024).expect("classify"));
        assert!(!sql_is_read_only("SELECT 1 FOR UPDATE", 1024).expect("classify"));
        assert!(sql_is_read_only("SET @@session.foo=1", 1024).expect("classify"));
        assert!(!sql_is_read_only("SET @@global.foo=1", 1024).expect("classify"));
    }

    #[test]
    fn malformed_sql_fails_closed() {
        assert!(sql_is_read_only("SELECT 'oops", 1024).is_err());
        assert!(sql_is_read_only("/* oops", 1024).is_err());
    }

    #[test]
    fn prepared_readonly_state_is_scoped_to_connection() {
        let timestamp = OffsetDateTime::UNIX_EPOCH;
        let mut prepared = HashMap::new();
        let mut prepare = Command::new(b"\x16SELECT ?".to_vec(), timestamp, 1, "fixture", 1)
            .expect("prepare command");
        prepare.captured_statement_id = Some(7);
        assert!(command_is_read_only(&prepare, &mut prepared, 1024).expect("classify prepare"));

        let mut other_connection_execute =
            Command::new(vec![0x17, 7, 0, 0, 0], timestamp, 2, "fixture", 2)
                .expect("execute command");
        other_connection_execute.captured_statement_id = Some(7);
        assert!(
            !command_is_read_only(&other_connection_execute, &mut prepared, 1024)
                .expect("classify execute")
        );

        let mut same_connection_execute =
            Command::new(vec![0x17, 7, 0, 0, 0], timestamp, 1, "fixture", 3)
                .expect("execute command");
        same_connection_execute.captured_statement_id = Some(7);
        assert!(
            command_is_read_only(&same_connection_execute, &mut prepared, 1024)
                .expect("classify execute")
        );
    }
}
