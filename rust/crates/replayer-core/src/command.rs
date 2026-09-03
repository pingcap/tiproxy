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

//! Replay command model shared by all input decoders.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::ReplayError;

/// Traffic input format accepted by the replayer.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficFormat {
    /// `TiProxy`'s binary-preserving native capture format.
    #[default]
    Native,
    /// `TiDB` audit-log plugin format.
    AuditLogPlugin,
    /// `TiDB` audit-log extension format.
    AuditLogExtension,
}

impl TrafficFormat {
    /// Whether the format is one of `TiDB`'s audit formats.
    #[must_use]
    pub const fn is_audit(self) -> bool {
        matches!(self, Self::AuditLogPlugin | Self::AuditLogExtension)
    }
}

impl fmt::Display for TrafficFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Native => "native",
            Self::AuditLogPlugin => "audit_log_plugin",
            Self::AuditLogExtension => "audit_log_extension",
        })
    }
}

impl FromStr for TrafficFormat {
    type Err = ReplayError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "" | "native" => Ok(Self::Native),
            "audit_log_plugin" => Ok(Self::AuditLogPlugin),
            "audit_log_extension" => Ok(Self::AuditLogExtension),
            other => Err(ReplayError::Config(format!(
                "invalid traffic file format {other}"
            ))),
        }
    }
}

/// Prepared-statement close policy.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedCloseStrategy {
    /// Replay only captured close commands.
    #[default]
    Directed,
    /// Close immediately after each execution.
    Always,
    /// Reuse each distinct statement for the life of the connection.
    Never,
}

impl fmt::Display for PreparedCloseStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Directed => "directed",
            Self::Always => "always",
            Self::Never => "never",
        })
    }
}

impl FromStr for PreparedCloseStrategy {
    type Err = ReplayError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "directed" => Ok(Self::Directed),
            "always" => Ok(Self::Always),
            "never" => Ok(Self::Never),
            other => Err(ReplayError::Config(format!(
                "invalid prepared statement close strategy {other}"
            ))),
        }
    }
}

/// One-byte `MySQL` command code.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct CommandCode(u8);

impl CommandCode {
    /// `COM_QUIT`.
    pub const QUIT: Self = Self(0x01);
    /// `COM_INIT_DB`.
    pub const INIT_DB: Self = Self(0x02);
    /// `COM_QUERY`.
    pub const QUERY: Self = Self(0x03);
    /// `COM_CREATE_DB`.
    pub const CREATE_DB: Self = Self(0x05);
    /// `COM_DROP_DB`.
    pub const DROP_DB: Self = Self(0x06);
    /// `COM_DELAYED_INSERT`.
    pub const DELAYED_INSERT: Self = Self(0x10);
    /// `COM_STMT_PREPARE`.
    pub const STMT_PREPARE: Self = Self(0x16);
    /// `COM_STMT_EXECUTE`.
    pub const STMT_EXECUTE: Self = Self(0x17);
    /// `COM_STMT_SEND_LONG_DATA`.
    pub const STMT_SEND_LONG_DATA: Self = Self(0x18);
    /// `COM_STMT_CLOSE`.
    pub const STMT_CLOSE: Self = Self(0x19);
    /// `COM_STMT_RESET`.
    pub const STMT_RESET: Self = Self(0x1a);
    /// `COM_STMT_FETCH`.
    pub const STMT_FETCH: Self = Self(0x1c);

    /// Returns the wire byte.
    #[must_use]
    pub const fn byte(self) -> u8 {
        self.0
    }

    /// Parses the names emitted by the Go native encoder.
    ///
    /// # Errors
    ///
    /// Returns an error for a command name outside the Go command table.
    pub fn from_go_name(value: &str) -> Result<Self, String> {
        const NAMES: [&str; 32] = [
            "Sleep",
            "Quit",
            "InitDB",
            "Query",
            "FieldList",
            "CreateDB",
            "DropDB",
            "Refresh",
            "(DEPRECATED)Shutdown",
            "Statistics",
            "ProcessInfo",
            "Connect",
            "ProcessKill",
            "Debug",
            "Ping",
            "Time",
            "DelayedInsert",
            "ChangeUser",
            "BinlogDump",
            "TableDump",
            "ConnectOut",
            "RegisterSlave",
            "StmtPrepare",
            "StmtExecute",
            "StmtSendLongData",
            "StmtClose",
            "StmtReset",
            "SetOption",
            "StmtFetch",
            "Daemon",
            "BinlogDumpGtid",
            "ResetConnect",
        ];
        NAMES
            .iter()
            .position(|name| *name == value)
            .and_then(|index| u8::try_from(index).ok())
            .map(Self)
            .ok_or_else(|| format!("unknown MySQL command {value}"))
    }
}

/// One decoded replay command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Command {
    /// Command payload including the one-byte command code.
    pub payload: Vec<u8>,
    /// Captured command start time.
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: OffsetDateTime,
    /// Captured command end time, when the input provides it.
    #[serde(with = "time::serde::rfc3339::option")]
    pub end_time: Option<OffsetDateTime>,
    /// Replay-local logical connection identifier.
    pub connection_id: u64,
    /// Original `TiDB` connection identifier.
    pub upstream_connection_id: u64,
    /// `MySQL` command code.
    pub command: CommandCode,
    /// Current database that must be restored after reconnect.
    pub current_database: String,
    /// Captured prepared-statement identifier.
    pub captured_statement_id: Option<u32>,
    /// Prepared SQL text when known.
    pub prepared_statement: Option<String>,
    /// Audit statement type when known.
    pub statement_type: Option<String>,
    /// Whether the captured command succeeded.
    pub succeeded: bool,
    /// Credential-redacted logical source path.
    pub source: String,
    /// One-based source line for diagnostics.
    pub line: u64,
    /// Stable input-source ordinal used to break timestamp ties.
    pub source_ordinal: u64,
    /// Stable record ordinal within one source.
    pub record_ordinal: u64,
    /// Stable command ordinal within a record that expands to multiple commands.
    #[serde(default)]
    pub command_ordinal: u32,
}

impl Command {
    /// Constructs a minimal command while preserving the required invariants.
    #[must_use]
    pub fn new(
        payload: Vec<u8>,
        start_time: OffsetDateTime,
        connection_id: u64,
        source: impl Into<String>,
        line: u64,
    ) -> Option<Self> {
        let command = payload.first().copied().map(CommandCode)?;
        Some(Self {
            payload,
            start_time,
            end_time: None,
            connection_id,
            upstream_connection_id: connection_id,
            command,
            current_database: String::new(),
            captured_statement_id: None,
            prepared_statement: None,
            statement_type: None,
            succeeded: true,
            source: source.into(),
            line,
            source_ordinal: 0,
            record_ordinal: 0,
            command_ordinal: 0,
        })
    }
}
