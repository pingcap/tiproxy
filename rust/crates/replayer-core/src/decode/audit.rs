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

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use time::OffsetDateTime;

use crate::decode::parse_go_quoted;
use crate::{Command, CommandCode, PreparedCloseStrategy, ReplayError, TrafficFormat};

const AUDIT_TIME_FORMAT: &str = concat!(
    "[year]/[month]/[day] [hour]:[minute]:[second].",
    "[subsecond digits:3] [offset_hour sign:mandatory]:[offset_minute]"
);

#[derive(Clone, Debug)]
enum Parameter {
    Null,
    Signed(i64),
    Unsigned(u64),
    Float(f32),
    Double(f64),
    String(Vec<u8>),
}

#[derive(Clone, Debug, Default)]
struct ConnectionState {
    replay_id: u64,
    last_statement_id: u32,
    prepared_ids: HashSet<u32>,
    prepared_by_sql: HashMap<String, u32>,
    last_write: Option<WriteSignature>,
}

#[derive(Clone, Debug)]
struct WriteSignature {
    command: String,
    sql: String,
    params: String,
    statement_type: String,
    start: OffsetDateTime,
    end: OffsetDateTime,
}

/// Stateful decoder for `TiDB` audit plugin and audit extension records.
pub struct AuditDecoder {
    format: TrafficFormat,
    path: String,
    record_limit: usize,
    strategy: PreparedCloseStrategy,
    command_start_time: Option<OffsetDateTime>,
    command_end_time: Option<OffsetDateTime>,
    filter_retries: bool,
    user_allowlist: HashSet<String>,
    source_ordinal: u64,
    next_connection_id: u64,
    record_ordinal: u64,
    connections: HashMap<u64, ConnectionState>,
    pending: VecDeque<Command>,
}

impl AuditDecoder {
    /// Creates an audit decoder. The source ordinal must fit the Go allocator's
    /// ten-bit decoder partition.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-audit format or an exhausted source-ordinal
    /// partition.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        format: TrafficFormat,
        path: impl Into<String>,
        record_limit: usize,
        strategy: PreparedCloseStrategy,
        command_start_time: Option<OffsetDateTime>,
        command_end_time: Option<OffsetDateTime>,
        filter_retries: bool,
        user_allowlist: &[String],
        source_ordinal: u64,
    ) -> Result<Self, ReplayError> {
        if !format.is_audit() {
            return Err(ReplayError::Config(
                "audit decoder requires an audit format".to_owned(),
            ));
        }
        if source_ordinal >= 1024 {
            return Err(ReplayError::Config(format!(
                "decoder source ordinal {source_ordinal} is too large"
            )));
        }
        Ok(Self {
            format,
            path: path.into(),
            record_limit,
            strategy,
            command_start_time,
            command_end_time,
            filter_retries,
            user_allowlist: user_allowlist
                .iter()
                .map(|user| user.trim().to_ascii_lowercase())
                .filter(|user| !user.is_empty())
                .collect(),
            source_ordinal,
            next_connection_id: source_ordinal << 54,
            record_ordinal: 0,
            connections: HashMap::new(),
            pending: VecDeque::new(),
        })
    }

    /// Decodes every bounded line in one decrypted input file.
    ///
    /// # Errors
    ///
    /// Returns an error when a line exceeds the configured bound or violates
    /// the selected audit grammar.
    pub fn decode_all(&mut self, input: &[u8]) -> Result<Vec<Command>, ReplayError> {
        let mut output = Vec::new();
        let mut offset = 0_usize;
        let mut line = 0_u64;
        while offset < input.len() {
            let relative_end = input[offset..]
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap_or(input.len() - offset);
            if relative_end > self.record_limit {
                return Err(ReplayError::decode(
                    &self.path,
                    offset,
                    "audit line exceeds record limit",
                ));
            }
            let end = offset + relative_end;
            line = line.saturating_add(1);
            let text = std::str::from_utf8(&input[offset..end]).map_err(|_| {
                ReplayError::decode(&self.path, offset, "audit line is not valid UTF-8")
            })?;
            self.record_ordinal = self.record_ordinal.saturating_add(1);
            let commands = self.decode_line(text, offset, line)?;
            output.extend(commands);
            offset = if end < input.len() { end + 1 } else { end };
        }
        while let Some(command) = self.pending.pop_front() {
            output.push(command);
        }
        Ok(output)
    }

    fn decode_line(
        &mut self,
        line_text: &str,
        offset: usize,
        line: u64,
    ) -> Result<Vec<Command>, ReplayError> {
        let fields = parse_bracket_fields(line_text)
            .map_err(|message| ReplayError::decode(&self.path, offset, message))?;
        let upstream_connection_id = required(&fields, "CONNECTION_ID", &self.path, offset)?
            .parse::<u64>()
            .map_err(|_| ReplayError::decode(&self.path, offset, "invalid CONNECTION_ID"))?;
        match self.format {
            TrafficFormat::AuditLogPlugin => {
                self.decode_plugin(&fields, upstream_connection_id, offset, line)
            }
            TrafficFormat::AuditLogExtension => {
                self.decode_extension(&fields, upstream_connection_id, offset, line)
            }
            TrafficFormat::Native => Err(ReplayError::decode(
                &self.path,
                offset,
                "native input routed to audit decoder",
            )),
        }
    }

    fn decode_plugin(
        &mut self,
        fields: &HashMap<String, String>,
        upstream_connection_id: u64,
        offset: usize,
        line: u64,
    ) -> Result<Vec<Command>, ReplayError> {
        let end_time = parse_audit_time(required(fields, "TIMESTAMP", &self.path, offset)?)
            .map_err(|message| ReplayError::decode(&self.path, offset, message))?;
        let start_time = match fields.get("COST_TIME").filter(|value| !value.is_empty()) {
            Some(cost) => {
                let micros = cost
                    .parse::<f32>()
                    .map_err(|_| ReplayError::decode(&self.path, offset, "invalid COST_TIME"))?;
                end_time - duration_from_cost_micros(f64::from(micros), &self.path, offset)?
            }
            None => end_time,
        };
        if self
            .command_start_time
            .is_some_and(|frontier| start_time < frontier)
            || self
                .command_end_time
                .is_some_and(|frontier| end_time < frontier)
        {
            return Ok(Vec::new());
        }
        if !self.user_allowed(fields) {
            return Ok(Vec::new());
        }
        let replay_id = self.ensure_connection(upstream_connection_id)?;
        let class = required(fields, "EVENT_CLASS", &self.path, offset)?;
        if class == "CONNECTION" {
            if fields.get("EVENT_SUBCLASS").map(String::as_str) == Some("Disconnect") {
                self.connections.remove(&upstream_connection_id);
                return Ok(vec![build_command(
                    vec![CommandCode::QUIT.byte()],
                    start_time,
                    end_time,
                    replay_id,
                    upstream_connection_id,
                    fields,
                    &self.path,
                    line,
                    self.source_ordinal,
                    self.record_ordinal,
                )?]);
            }
            return Ok(Vec::new());
        }
        if class != "GENERAL" && class != "TABLE_ACCESS" {
            return Err(ReplayError::decode(
                &self.path,
                offset,
                format!("unknown EVENT_CLASS {class}"),
            ));
        }
        if fields.get("EVENT").map(String::as_str) != Some("COMPLETED") {
            return Ok(Vec::new());
        }
        let encoded_command = fields.get("COMMAND").map_or("", String::as_str);
        let command_name = if encoded_command.starts_with('"') {
            parse_go_quoted(encoded_command)
                .map_err(|message| ReplayError::decode(&self.path, offset, message))?
        } else {
            encoded_command.to_owned()
        };
        let sql = match command_name.as_str() {
            "Query" | "Execute" => Some(
                unquote_if_needed(required(fields, "SQL_TEXT", &self.path, offset)?)
                    .map_err(|message| ReplayError::decode(&self.path, offset, message))?,
            ),
            _ => None,
        };
        if matches!(command_name.as_str(), "Query" | "Execute")
            && self.filter_retries
            && fields.get("RETRY").map(String::as_str) == Some("true")
        {
            return Ok(Vec::new());
        }
        if let Some(sql) = sql.as_deref()
            && !self.filter_retries
            && (command_name != "Execute" || fields.contains_key("EXECUTE_PARAMS"))
            && self.is_duplicate_write(
                upstream_connection_id,
                fields,
                &command_name,
                sql,
                start_time,
                end_time,
            )?
        {
            return Ok(Vec::new());
        }
        self.commands_for_event(
            fields,
            upstream_connection_id,
            start_time,
            end_time,
            line,
            &command_name,
            sql.as_deref(),
            false,
            offset,
        )
    }

    fn decode_extension(
        &mut self,
        fields: &HashMap<String, String>,
        upstream_connection_id: u64,
        offset: usize,
        line: u64,
    ) -> Result<Vec<Command>, ReplayError> {
        let end_time = parse_audit_time(required(fields, "_LOG_TIME", &self.path, offset)?)
            .map_err(|message| ReplayError::decode(&self.path, offset, message))?;
        if self
            .command_end_time
            .is_some_and(|frontier| end_time < frontier)
        {
            return Ok(Vec::new());
        }
        if !self.user_allowed(fields) {
            return Ok(Vec::new());
        }
        let replay_id = self.ensure_connection(upstream_connection_id)?;
        let encoded_event = required(fields, "EVENT", &self.path, offset)?;
        let event = parse_go_quoted(encoded_event)
            .map_err(|message| ReplayError::decode(&self.path, offset, message))?;
        let event = event
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .ok_or_else(|| ReplayError::decode(&self.path, offset, "invalid EVENT tuple"))?;
        let events: Vec<&str> = event.split(',').collect();
        match events.first().copied() {
            Some("CONNECTION") if events.get(1).copied() == Some("DISCONNECT") => {
                self.connections.remove(&upstream_connection_id);
                Ok(vec![build_command(
                    vec![CommandCode::QUIT.byte()],
                    end_time,
                    end_time,
                    replay_id,
                    upstream_connection_id,
                    fields,
                    &self.path,
                    line,
                    self.source_ordinal,
                    self.record_ordinal,
                )?])
            }
            Some("QUERY") => {
                let sql = match fields.get("SQL_TEXT") {
                    Some(value) if !value.is_empty() => unquote_if_needed(value)
                        .map_err(|message| ReplayError::decode(&self.path, offset, message))?,
                    _ => String::new(),
                };
                let command_name = if events.get(1).copied() == Some("EXECUTE") {
                    "Execute"
                } else {
                    "Query"
                };
                self.commands_for_event(
                    fields,
                    upstream_connection_id,
                    end_time,
                    end_time,
                    line,
                    command_name,
                    Some(&sql),
                    true,
                    offset,
                )
            }
            _ => Ok(Vec::new()),
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn commands_for_event(
        &mut self,
        fields: &HashMap<String, String>,
        upstream_connection_id: u64,
        start_time: OffsetDateTime,
        end_time: OffsetDateTime,
        line: u64,
        command_name: &str,
        sql: Option<&str>,
        extension: bool,
        offset: usize,
    ) -> Result<Vec<Command>, ReplayError> {
        let mut state = self
            .connections
            .remove(&upstream_connection_id)
            .ok_or_else(|| ReplayError::Config("audit connection state is missing".to_owned()))?;
        let mut commands = Vec::with_capacity(3);
        match command_name {
            "Query" => {
                let sql = sql.unwrap_or_default();
                let mut payload = vec![CommandCode::QUERY.byte()];
                payload.extend_from_slice(sql.as_bytes());
                commands.push(build_command(
                    payload,
                    start_time,
                    end_time,
                    state.replay_id,
                    upstream_connection_id,
                    fields,
                    &self.path,
                    line,
                    self.source_ordinal,
                    self.record_ordinal,
                )?);
            }
            "Close stmt" => {
                if self.strategy == PreparedCloseStrategy::Directed {
                    let statement_id = parse_statement_id(fields, &self.path, offset)?;
                    if state.prepared_ids.remove(&statement_id) {
                        commands.push(prepared_command(
                            close_payload(statement_id),
                            start_time,
                            end_time,
                            state.replay_id,
                            upstream_connection_id,
                            fields,
                            &self.path,
                            line,
                            self.source_ordinal,
                            self.record_ordinal,
                            statement_id,
                            None,
                        )?);
                    }
                }
            }
            "Execute" => {
                let Some(encoded_parameters) = fields.get("EXECUTE_PARAMS") else {
                    self.connections.insert(upstream_connection_id, state);
                    return Ok(Vec::new());
                };
                let parameters = if extension {
                    parse_extension_parameters(encoded_parameters)
                } else {
                    parse_plugin_parameters(encoded_parameters)
                }
                .map_err(|message| ReplayError::decode(&self.path, offset, message))?;
                let sql = sql.unwrap_or_default();
                let (statement_id, should_prepare) = match self.strategy {
                    PreparedCloseStrategy::Always => {
                        state.last_statement_id = state.last_statement_id.saturating_add(1);
                        (state.last_statement_id, true)
                    }
                    PreparedCloseStrategy::Directed => {
                        let statement_id = parse_statement_id(fields, &self.path, offset)?;
                        (statement_id, state.prepared_ids.insert(statement_id))
                    }
                    PreparedCloseStrategy::Never => {
                        if let Some(statement_id) = state.prepared_by_sql.get(sql) {
                            (*statement_id, false)
                        } else {
                            state.last_statement_id = state.last_statement_id.saturating_add(1);
                            state
                                .prepared_by_sql
                                .insert(sql.to_owned(), state.last_statement_id);
                            (state.last_statement_id, true)
                        }
                    }
                };
                if should_prepare {
                    let mut payload = vec![CommandCode::STMT_PREPARE.byte()];
                    payload.extend_from_slice(sql.as_bytes());
                    commands.push(prepared_command(
                        payload,
                        start_time,
                        end_time,
                        state.replay_id,
                        upstream_connection_id,
                        fields,
                        &self.path,
                        line,
                        self.source_ordinal,
                        self.record_ordinal,
                        statement_id,
                        Some(sql),
                    )?);
                }
                commands.push(prepared_command(
                    execute_payload(statement_id, &parameters)?,
                    start_time,
                    end_time,
                    state.replay_id,
                    upstream_connection_id,
                    fields,
                    &self.path,
                    line,
                    self.source_ordinal,
                    self.record_ordinal,
                    statement_id,
                    Some(sql),
                )?);
                if self.strategy == PreparedCloseStrategy::Always {
                    commands.push(prepared_command(
                        close_payload(statement_id),
                        start_time,
                        end_time,
                        state.replay_id,
                        upstream_connection_id,
                        fields,
                        &self.path,
                        line,
                        self.source_ordinal,
                        self.record_ordinal,
                        statement_id,
                        Some(sql),
                    )?);
                }
            }
            _ => {}
        }
        self.connections.insert(upstream_connection_id, state);
        Ok(commands)
    }

    fn ensure_connection(&mut self, upstream: u64) -> Result<u64, ReplayError> {
        if let Some(state) = self.connections.get(&upstream) {
            return Ok(state.replay_id);
        }
        self.next_connection_id = self.next_connection_id.checked_add(1).ok_or_else(|| {
            ReplayError::Config("audit connection identifier space exhausted".to_owned())
        })?;
        self.connections.insert(
            upstream,
            ConnectionState {
                replay_id: self.next_connection_id,
                ..ConnectionState::default()
            },
        );
        Ok(self.next_connection_id)
    }

    fn user_allowed(&self, fields: &HashMap<String, String>) -> bool {
        self.user_allowlist.is_empty()
            || self.user_allowlist.contains(
                &fields
                    .get("USER")
                    .map_or("", String::as_str)
                    .trim()
                    .to_ascii_lowercase(),
            )
    }

    fn is_duplicate_write(
        &mut self,
        upstream: u64,
        fields: &HashMap<String, String>,
        command: &str,
        sql: &str,
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Result<bool, ReplayError> {
        let params = fields.get("EXECUTE_PARAMS").cloned().unwrap_or_default();
        let statement_type = fields.get("SQL_STATEMENTS").cloned().unwrap_or_default();
        let is_write = matches!(
            statement_type.as_str(),
            "Insert" | "Update" | "Delete" | "Replace"
        ) || (statement_type == "Select"
            && sql.to_ascii_lowercase().contains("for update"));
        let state = self
            .connections
            .get_mut(&upstream)
            .ok_or_else(|| ReplayError::Config("audit connection state is missing".to_owned()))?;
        if !is_write {
            state.last_write = None;
            return Ok(false);
        }
        let duplicate = state.last_write.as_ref().is_some_and(|last| {
            last.command == command
                && last.sql == sql
                && last.params == params
                && last.statement_type == statement_type
                && last.end >= start
                && start - last.start <= Duration::from_millis(1)
        });
        if !duplicate {
            state.last_write = Some(WriteSignature {
                command: command.to_owned(),
                sql: sql.to_owned(),
                params,
                statement_type,
                start,
                end,
            });
        }
        Ok(duplicate)
    }
}

/// Parses the `TiDB` audit logger's bounded bracketed field grammar.
///
/// # Errors
///
/// Returns an error for duplicate fields, unbalanced delimiters, unterminated
/// quoted values, or invalid UTF-8 boundaries.
pub fn parse_bracket_fields(line: &str) -> Result<HashMap<String, String>, String> {
    let bytes = line.as_bytes();
    let mut fields = HashMap::new();
    let mut index = 0_usize;
    let mut first = true;
    while index < bytes.len() {
        if bytes[index] != b'[' {
            index += 1;
            continue;
        }
        let field_start = index + 1;
        let mut cursor = field_start;
        let mut separator = None;
        let mut quote = None;
        while cursor < bytes.len() {
            let byte = bytes[cursor];
            if let Some(delimiter) = quote {
                if byte == b'\\' {
                    cursor = cursor
                        .checked_add(2)
                        .ok_or_else(|| "quoted field offset overflow".to_owned())?;
                    continue;
                }
                if byte == delimiter {
                    quote = None;
                }
            } else {
                match byte {
                    b'"' | b'\'' => quote = Some(byte),
                    b'=' if separator.is_none() => separator = Some(cursor),
                    b']' => break,
                    _ => {}
                }
            }
            cursor += 1;
        }
        if quote.is_some() {
            return Err("unterminated quote in audit field".to_owned());
        }
        if cursor >= bytes.len() {
            return Err("unterminated bracket in audit line".to_owned());
        }
        let value_start = separator.map_or(field_start, |position| position + 1);
        let value = line
            .get(value_start..cursor)
            .ok_or_else(|| "audit field is not on UTF-8 boundaries".to_owned())?;
        if first {
            fields.insert("_LOG_TIME".to_owned(), value.to_owned());
            first = false;
        } else if let Some(position) = separator {
            if position == field_start {
                return Err("empty audit field key".to_owned());
            }
            let key = line
                .get(field_start..position)
                .ok_or_else(|| "audit key is not on UTF-8 boundaries".to_owned())?;
            if fields.insert(key.to_owned(), value.to_owned()).is_some() {
                return Err(format!("duplicate audit field {key}"));
            }
        }
        index = cursor + 1;
    }
    Ok(fields)
}

fn parse_audit_time(value: &str) -> Result<OffsetDateTime, String> {
    let format = time::format_description::parse_borrowed::<2>(AUDIT_TIME_FORMAT)
        .map_err(|_| "internal audit timestamp format is invalid".to_owned())?;
    OffsetDateTime::parse(value, &format).map_err(|_| format!("invalid audit timestamp {value}"))
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn duration_from_cost_micros(
    micros: f64,
    path: &str,
    offset: usize,
) -> Result<Duration, ReplayError> {
    const MAX_DURATION_NANOS: f64 = 9_223_372_036_854_775_807.0;
    let nanos = micros * 1_000.0;
    if !nanos.is_finite() || !(0.0..=MAX_DURATION_NANOS).contains(&nanos) {
        return Err(ReplayError::decode(
            path,
            offset,
            "COST_TIME is outside the supported range",
        ));
    }
    // Go converts COST_TIME * time.Microsecond to an integer duration by
    // truncating toward zero. The bounds above make this cast explicit.
    Ok(Duration::from_nanos(nanos as u64))
}

fn required<'a>(
    fields: &'a HashMap<String, String>,
    key: &str,
    path: &str,
    offset: usize,
) -> Result<&'a str, ReplayError> {
    fields
        .get(key)
        .filter(|value| !value.is_empty())
        .map(String::as_str)
        .ok_or_else(|| ReplayError::decode(path, offset, format!("missing {key}")))
}

fn unquote_if_needed(value: &str) -> Result<String, String> {
    if value.starts_with('"') {
        parse_go_quoted(value)
    } else if value.is_empty() {
        Err("empty SQL or command value".to_owned())
    } else {
        Ok(value.to_owned())
    }
}

fn parse_statement_id(
    fields: &HashMap<String, String>,
    path: &str,
    offset: usize,
) -> Result<u32, ReplayError> {
    required(fields, "PREPARED_STMT_ID", path, offset)?
        .parse::<u32>()
        .map_err(|_| ReplayError::decode(path, offset, "invalid PREPARED_STMT_ID"))
}

#[allow(clippy::too_many_arguments)]
fn build_command(
    payload: Vec<u8>,
    start: OffsetDateTime,
    end: OffsetDateTime,
    replay_connection_id: u64,
    upstream_connection_id: u64,
    fields: &HashMap<String, String>,
    path: &str,
    line: u64,
    source_ordinal: u64,
    record_ordinal: u64,
) -> Result<Command, ReplayError> {
    let mut command = Command::new(payload, start, replay_connection_id, path.to_owned(), line)
        .ok_or_else(|| ReplayError::decode(path, 0, "empty generated command"))?;
    command.end_time = Some(end);
    command.upstream_connection_id = upstream_connection_id;
    command.current_database = fields.get("CURRENT_DB").cloned().unwrap_or_default();
    command.statement_type = fields.get("SQL_STATEMENTS").cloned();
    command.source_ordinal = source_ordinal;
    command.record_ordinal = record_ordinal;
    Ok(command)
}

#[allow(clippy::too_many_arguments)]
fn prepared_command(
    payload: Vec<u8>,
    start: OffsetDateTime,
    end: OffsetDateTime,
    replay_connection_id: u64,
    upstream_connection_id: u64,
    fields: &HashMap<String, String>,
    path: &str,
    line: u64,
    source_ordinal: u64,
    record_ordinal: u64,
    statement_id: u32,
    sql: Option<&str>,
) -> Result<Command, ReplayError> {
    let mut command = build_command(
        payload,
        start,
        end,
        replay_connection_id,
        upstream_connection_id,
        fields,
        path,
        line,
        source_ordinal,
        record_ordinal,
    )?;
    command.captured_statement_id = Some(statement_id);
    command.prepared_statement = sql.map(str::to_owned);
    Ok(command)
}

fn parse_plugin_parameters(input: &str) -> Result<Vec<Parameter>, String> {
    let decoded = parse_go_quoted(input).map_err(|error| format!("invalid params: {error}"))?;
    let body = decoded
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| "params have no surrounding brackets".to_owned())?;
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let values = split_quoted_list(body)?;
    values
        .into_iter()
        .map(|value| {
            let decoded = parse_go_quoted(value)?;
            let (kind, value) = decoded
                .split_once(' ')
                .ok_or_else(|| format!("no space in param {decoded}"))?;
            match kind {
                "KindNull" => Ok(Parameter::Null),
                "KindInt64" => value
                    .parse::<i64>()
                    .map(Parameter::Signed)
                    .map_err(|_| format!("invalid signed param {value}")),
                "KindUint64" => value
                    .parse::<u64>()
                    .map(Parameter::Unsigned)
                    .map_err(|_| format!("invalid unsigned param {value}")),
                "KindFloat32" => value
                    .parse::<f32>()
                    .map(Parameter::Float)
                    .map_err(|_| format!("invalid float param {value}")),
                "KindFloat64" | "KindMysqlDecimal" => value
                    .parse::<f64>()
                    .map(Parameter::Double)
                    .map_err(|_| format!("invalid double param {value}")),
                "KindString" => parse_go_quoted(&format!("\"{value}\""))
                    .map(|value| Parameter::String(value.into_bytes())),
                "KindBytes" => parse_go_quoted(&format!("\"{value}\""))
                    .map(|value| Parameter::String(value.into_bytes())),
                "KindBinaryLiteral" | "KindMysqlBit" | "KindMysqlSet" | "KindMysqlTime"
                | "KindMysqlJSON" => Ok(Parameter::String(value.as_bytes().to_vec())),
                "KindMysqlDuration" | "KindMysqlEnum" | "KindInterface" | "KindMinNotNull"
                | "KindMaxValue" | "KindRaw" => Err(format!("unsupported param type {kind}")),
                _ => Err(format!("unknown param type {kind}")),
            }
        })
        .collect()
}

fn parse_extension_parameters(input: &str) -> Result<Vec<Parameter>, String> {
    let decoded = parse_go_quoted(input).map_err(|error| format!("invalid params: {error}"))?;
    let body = decoded
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| "params have no surrounding brackets".to_owned())?;
    if body.is_empty() {
        return Ok(Vec::new());
    }
    split_extension_list(body).map(|values| {
        values
            .into_iter()
            .map(|value| Parameter::String(value.into_bytes()))
            .collect()
    })
}

fn split_quoted_list(input: &str) -> Result<Vec<&str>, String> {
    let bytes = input.as_bytes();
    let mut values = Vec::new();
    let mut index = 0_usize;
    while index < bytes.len() {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace)
            || bytes.get(index) == Some(&b',')
        {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        if bytes[index] != b'"' {
            return Err("expected quoted audit parameter".to_owned());
        }
        let start = index;
        index += 1;
        let mut closed = false;
        while index < bytes.len() {
            match bytes[index] {
                b'\\' => index += 2,
                b'"' => {
                    index += 1;
                    values.push(
                        input
                            .get(start..index)
                            .ok_or_else(|| "parameter is not on UTF-8 boundaries".to_owned())?,
                    );
                    closed = true;
                    break;
                }
                _ => index += 1,
            }
        }
        if !closed {
            return Err("unterminated quoted audit parameter".to_owned());
        }
    }
    Ok(values)
}

fn split_extension_list(input: &str) -> Result<Vec<String>, String> {
    let bytes = input.as_bytes();
    let mut values = Vec::new();
    let mut index = 0_usize;
    while index < bytes.len() {
        while bytes
            .get(index)
            .is_some_and(|byte| *byte == b',' || byte.is_ascii_whitespace())
        {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        if bytes[index] == b'"' {
            let start = index;
            index += 1;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' => index += 2,
                    b'"' => {
                        index += 1;
                        let quoted = input
                            .get(start..index)
                            .ok_or_else(|| "parameter is not on UTF-8 boundaries".to_owned())?;
                        values.push(parse_go_quoted(quoted)?);
                        break;
                    }
                    _ => index += 1,
                }
            }
            if index > bytes.len() || bytes.get(index.saturating_sub(1)) != Some(&b'"') {
                return Err("unterminated extension parameter".to_owned());
            }
        } else {
            let start = index;
            while index < bytes.len() && bytes[index] != b',' {
                index += 1;
            }
            let value = input
                .get(start..index)
                .ok_or_else(|| "parameter is not on UTF-8 boundaries".to_owned())?
                .trim();
            values.push(value.to_owned());
        }
    }
    Ok(values)
}

fn execute_payload(statement_id: u32, parameters: &[Parameter]) -> Result<Vec<u8>, ReplayError> {
    let bitmap_length = parameters.len().div_ceil(8);
    let capacity = 10_usize
        .checked_add(bitmap_length)
        .and_then(|base| base.checked_add(usize::from(!parameters.is_empty())))
        .and_then(|base| base.checked_add(parameters.len().saturating_mul(10)))
        .ok_or_else(|| ReplayError::Config("execute payload size overflow".to_owned()))?;
    let mut output = Vec::with_capacity(capacity);
    output.push(CommandCode::STMT_EXECUTE.byte());
    output.extend_from_slice(&statement_id.to_le_bytes());
    output.push(0);
    output.extend_from_slice(&1_u32.to_le_bytes());
    if parameters.is_empty() {
        return Ok(output);
    }
    let bitmap_offset = output.len();
    output.resize(bitmap_offset + bitmap_length, 0);
    output.push(1);
    for (index, parameter) in parameters.iter().enumerate() {
        let (field_type, unsigned) = match parameter {
            Parameter::Null => {
                output[bitmap_offset + index / 8] |= 1 << (index % 8);
                (6_u8, 0_u8)
            }
            Parameter::Signed(_) => (8, 0),
            Parameter::Unsigned(_) => (8, 0x80),
            Parameter::Float(_) => (4, 0),
            Parameter::Double(_) => (5, 0),
            Parameter::String(_) => (0xfe, 0),
        };
        output.extend_from_slice(&[field_type, unsigned]);
    }
    for parameter in parameters {
        match parameter {
            Parameter::Null => {}
            Parameter::Signed(value) => output.extend_from_slice(&value.to_le_bytes()),
            Parameter::Unsigned(value) => output.extend_from_slice(&value.to_le_bytes()),
            Parameter::Float(value) => output.extend_from_slice(&value.to_le_bytes()),
            Parameter::Double(value) => output.extend_from_slice(&value.to_le_bytes()),
            Parameter::String(value) => append_length_encoded(&mut output, value)?,
        }
    }
    Ok(output)
}

fn append_length_encoded(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ReplayError> {
    let length = u64::try_from(value.len())
        .map_err(|_| ReplayError::Config("parameter length does not fit u64".to_owned()))?;
    match length {
        0..=250 => output.push(
            u8::try_from(length)
                .map_err(|_| ReplayError::Config("parameter length exceeds u8".to_owned()))?,
        ),
        251..=65_535 => {
            output.push(0xfc);
            output.extend_from_slice(
                &u16::try_from(length)
                    .map_err(|_| ReplayError::Config("parameter length exceeds u16".to_owned()))?
                    .to_le_bytes(),
            );
        }
        65_536..=16_777_215 => {
            output.push(0xfd);
            let bytes = length.to_le_bytes();
            output.extend_from_slice(&bytes[..3]);
        }
        _ => {
            output.push(0xfe);
            output.extend_from_slice(&length.to_le_bytes());
        }
    }
    output.extend_from_slice(value);
    Ok(())
}

fn close_payload(statement_id: u32) -> Vec<u8> {
    let mut output = vec![CommandCode::STMT_CLOSE.byte()];
    output.extend_from_slice(&statement_id.to_le_bytes());
    output
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_brackets_and_quotes() {
        let fields = parse_bracket_fields(
            r#"[2025/09/06 17:03:53.720 +08:00] [SQL_TEXT="select \"[=]\""] [CONNECTION_ID=9]"#,
        )
        .expect("valid fields");
        assert_eq!(
            fields.get("_LOG_TIME").map(String::as_str),
            Some("2025/09/06 17:03:53.720 +08:00")
        );
        assert_eq!(
            fields.get("SQL_TEXT").map(String::as_str),
            Some(r#""select \"[=]\"""#)
        );
        assert_eq!(fields.get("CONNECTION_ID").map(String::as_str), Some("9"));
    }

    #[test]
    fn rejects_unterminated_and_duplicate_fields() {
        assert!(parse_bracket_fields("[a=b").is_err());
        assert!(parse_bracket_fields("[time] [a=b] [a=c]").is_err());
    }

    #[test]
    fn plugin_query_and_retry_filter_match_contract() {
        let line = concat!(
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=SELECT 1] [SQL_STATEMENTS=Select] [CONNECTION_ID=9] ",
            "[USER=root] [CURRENT_DB=test] [RETRY=false]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogPlugin,
            "audit.log",
            4096,
            PreparedCloseStrategy::Directed,
            None,
            None,
            true,
            &["ROOT".to_owned()],
            3,
        )
        .expect("decoder");
        let commands = decoder.decode_all(line.as_bytes()).expect("decode");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].payload, b"\x03SELECT 1");
        assert_eq!(commands[0].connection_id, (3_u64 << 54) + 1);
        assert_eq!(commands[0].current_database, "test");
    }

    #[test]
    fn extension_always_emits_prepare_execute_close() {
        let line = concat!(
            "[2026/01/08 19:44:11.114 +08:00] ",
            "[EVENT=\"[QUERY,EXECUTE,SELECT]\"] [USER=root] [CONNECTION_ID=7] ",
            "[CURRENT_DB=test] [SQL_TEXT=\"SELECT ?\"] [EXECUTE_PARAMS=\"[1]\"]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogExtension,
            "audit.log",
            4096,
            PreparedCloseStrategy::Always,
            None,
            None,
            false,
            &[],
            0,
        )
        .expect("decoder");
        let commands = decoder.decode_all(line.as_bytes()).expect("decode");
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].command, CommandCode::STMT_PREPARE);
        assert_eq!(commands[1].command, CommandCode::STMT_EXECUTE);
        assert_eq!(commands[2].command, CommandCode::STMT_CLOSE);
        assert_eq!(
            commands[1].payload,
            vec![0x17, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0xfe, 0, 1, b'1']
        );
    }

    #[test]
    fn ignored_plugin_events_still_advance_connection_allocator() {
        let lines = concat!(
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=STARTING] [COMMAND=Query] ",
            "[SQL_TEXT=SELECT 1] [SQL_STATEMENTS=Select] [CONNECTION_ID=40]\n",
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Connected] [CONNECTION_ID=41]\n",
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=SELECT 2] [SQL_STATEMENTS=Select] [CONNECTION_ID=42]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogPlugin,
            "audit.log",
            4096,
            PreparedCloseStrategy::Directed,
            None,
            None,
            false,
            &[],
            2,
        )
        .expect("decoder");
        let commands = decoder.decode_all(lines.as_bytes()).expect("decode");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].connection_id, (2_u64 << 54) + 3);
    }

    #[test]
    fn disconnect_uses_existing_replay_connection_id() {
        let lines = concat!(
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=SELECT 1] [SQL_STATEMENTS=Select] [CONNECTION_ID=9]\n",
            "[2025/09/06 17:03:53.721 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.721 +08:00] ",
            "[EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Disconnect] [CONNECTION_ID=9]\n",
            "[2025/09/06 17:03:53.722 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.722 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=SELECT 2] [SQL_STATEMENTS=Select] [CONNECTION_ID=9]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogPlugin,
            "audit.log",
            4096,
            PreparedCloseStrategy::Directed,
            None,
            None,
            false,
            &[],
            0,
        )
        .expect("decoder");
        let commands = decoder.decode_all(lines.as_bytes()).expect("decode");
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].connection_id, commands[1].connection_id);
        assert_eq!(commands[1].command, CommandCode::QUIT);
        assert_ne!(commands[1].connection_id, commands[2].connection_id);
    }

    #[test]
    fn intervening_read_prevents_non_adjacent_write_deduplication() {
        let lines = concat!(
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=INSERT INTO t VALUES (1)] [SQL_STATEMENTS=Insert] [CONNECTION_ID=9]\n",
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=SELECT 1] [SQL_STATEMENTS=Select] [CONNECTION_ID=9]\n",
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=INSERT INTO t VALUES (1)] [SQL_STATEMENTS=Insert] [CONNECTION_ID=9]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogPlugin,
            "audit.log",
            4096,
            PreparedCloseStrategy::Directed,
            None,
            None,
            false,
            &[],
            0,
        )
        .expect("decoder");
        let commands = decoder.decode_all(lines.as_bytes()).expect("decode");
        assert_eq!(commands.len(), 3);
    }

    #[test]
    fn retry_filter_does_not_suppress_prepared_close() {
        let lines = concat!(
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Execute] ",
            "[SQL_TEXT=SELECT ?] [SQL_STATEMENTS=Select] [CONNECTION_ID=9] ",
            "[PREPARED_STMT_ID=7] [EXECUTE_PARAMS=\"[]\"] [RETRY=false]\n",
            "[2025/09/06 17:03:53.721 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.721 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=\"Close stmt\"] ",
            "[CONNECTION_ID=9] [PREPARED_STMT_ID=7] [RETRY=true]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogPlugin,
            "audit.log",
            4096,
            PreparedCloseStrategy::Directed,
            None,
            None,
            true,
            &[],
            0,
        )
        .expect("decoder");
        let commands = decoder.decode_all(lines.as_bytes()).expect("decode");
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[1].payload, vec![0x17, 7, 0, 0, 0, 0, 1, 0, 0, 0]);
        assert_eq!(commands[2].command, CommandCode::STMT_CLOSE);
    }

    #[test]
    fn plugin_validates_timestamp_before_user_filter() {
        let line = concat!(
            "[not-a-time] [TIMESTAMP=not-a-time] [EVENT_CLASS=GENERAL] ",
            "[EVENT=COMPLETED] [COMMAND=Query] [SQL_TEXT=SELECT 1] ",
            "[SQL_STATEMENTS=Select] [CONNECTION_ID=9] [USER=someone-else]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogPlugin,
            "audit.log",
            4096,
            PreparedCloseStrategy::Directed,
            None,
            None,
            false,
            &["root".to_owned()],
            0,
        )
        .expect("decoder");
        assert!(decoder.decode_all(line.as_bytes()).is_err());
    }

    #[test]
    fn empty_plugin_command_is_ignored_after_allocating_connection() {
        let lines = concat!(
            "[2025/09/06 17:03:53.720 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.720 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [CONNECTION_ID=8]\n",
            "[2025/09/06 17:03:53.721 +08:00] ",
            "[TIMESTAMP=2025/09/06 17:03:53.721 +08:00] ",
            "[EVENT_CLASS=GENERAL] [EVENT=COMPLETED] [COMMAND=Query] ",
            "[SQL_TEXT=SELECT 1] [SQL_STATEMENTS=Select] [CONNECTION_ID=9]\n"
        );
        let mut decoder = AuditDecoder::new(
            TrafficFormat::AuditLogPlugin,
            "audit.log",
            4096,
            PreparedCloseStrategy::Directed,
            None,
            None,
            false,
            &[],
            0,
        )
        .expect("decoder");
        let commands = decoder.decode_all(lines.as_bytes()).expect("decode");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].connection_id, 2);
    }
}
