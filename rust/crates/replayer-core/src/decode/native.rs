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

use std::collections::HashSet;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::decode::parse_go_quoted;
use crate::{Command, CommandCode, ReplayError};

/// Stateful decoder for `TiProxy` native capture records.
pub struct NativeDecoder<'a> {
    input: &'a [u8],
    path: String,
    offset: usize,
    line: u64,
    record_limit: usize,
    command_start_time: Option<OffsetDateTime>,
    record_ordinal: u64,
}

impl<'a> NativeDecoder<'a> {
    /// Creates a bounded decoder over one uncompressed, decrypted file.
    #[must_use]
    pub fn new(
        input: &'a [u8],
        path: impl Into<String>,
        record_limit: usize,
        command_start_time: Option<OffsetDateTime>,
    ) -> Self {
        Self {
            input,
            path: path.into(),
            offset: 0,
            line: 0,
            record_limit,
            command_start_time,
            record_ordinal: 0,
        }
    }

    /// Decodes the next record, or `None` at a clean file boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the next record violates the bounded native
    /// capture grammar.
    pub fn next_command(&mut self) -> Result<Option<Command>, ReplayError> {
        loop {
            if self.offset == self.input.len() {
                return Ok(None);
            }
            let command = self.decode_one()?;
            if self
                .command_start_time
                .is_some_and(|frontier| command.start_time <= frontier)
            {
                continue;
            }
            return Ok(Some(command));
        }
    }

    #[allow(clippy::too_many_lines)]
    fn decode_one(&mut self) -> Result<Command, ReplayError> {
        let record_offset = self.offset;
        let mut seen = HashSet::new();
        let mut start_time = None;
        let mut connection_id = None;
        let mut command = CommandCode::QUERY;
        let mut succeeded = true;
        let mut captured_statement_id = None;
        let mut prepared_statement = None;

        loop {
            let (line_offset, line) = self.read_header_line()?;
            let line = std::str::from_utf8(line).map_err(|_| {
                ReplayError::decode(&self.path, line_offset, "header is not valid UTF-8")
            })?;
            let body = line.strip_prefix("# ").ok_or_else(|| {
                ReplayError::decode(&self.path, line_offset, "line does not start with '# '")
            })?;
            let (key, value) = body.split_once(": ").ok_or_else(|| {
                ReplayError::decode(&self.path, line_offset, "header does not contain ': '")
            })?;
            if value.is_empty() {
                return Err(ReplayError::decode(
                    &self.path,
                    line_offset,
                    format!("{key} value is empty"),
                ));
            }
            let known = matches!(
                key,
                "Time"
                    | "Conn_ID"
                    | "Cmd_type"
                    | "Success"
                    | "Captured_ps_id"
                    | "Prepared_stmt"
                    | "Payload_len"
            );
            if known && !seen.insert(key.to_owned()) {
                return Err(ReplayError::decode(
                    &self.path,
                    line_offset,
                    format!("duplicate {key} header"),
                ));
            }
            match key {
                "Time" => {
                    start_time = Some(OffsetDateTime::parse(value, &Rfc3339).map_err(|_| {
                        ReplayError::decode(&self.path, line_offset, "invalid RFC3339 Time")
                    })?);
                }
                "Conn_ID" => {
                    let value = value.parse::<u64>().map_err(|_| {
                        ReplayError::decode(&self.path, line_offset, "invalid Conn_ID")
                    })?;
                    if value == 0 {
                        return Err(ReplayError::decode(
                            &self.path,
                            line_offset,
                            "Conn_ID must be positive",
                        ));
                    }
                    connection_id = Some(value);
                }
                "Cmd_type" => {
                    command = CommandCode::from_go_name(value)
                        .map_err(|message| ReplayError::decode(&self.path, line_offset, message))?;
                }
                "Success" => succeeded = value == "true",
                "Captured_ps_id" => {
                    captured_statement_id = Some(value.parse::<u32>().map_err(|_| {
                        ReplayError::decode(&self.path, line_offset, "invalid Captured_ps_id")
                    })?);
                }
                "Prepared_stmt" => {
                    prepared_statement = Some(parse_go_quoted(value).map_err(|message| {
                        ReplayError::decode(&self.path, line_offset, message)
                    })?);
                }
                "Payload_len" => {
                    let length = value.parse::<usize>().map_err(|_| {
                        ReplayError::decode(&self.path, line_offset, "invalid Payload_len")
                    })?;
                    if length > self.record_limit {
                        return Err(ReplayError::decode(
                            &self.path,
                            line_offset,
                            format!("Payload_len {length} exceeds limit {}", self.record_limit),
                        ));
                    }
                    let payload_offset = self.offset;
                    let payload_end = payload_offset.checked_add(length).ok_or_else(|| {
                        ReplayError::decode(&self.path, payload_offset, "Payload_len overflow")
                    })?;
                    if payload_end >= self.input.len() {
                        return Err(ReplayError::decode(
                            &self.path,
                            payload_offset,
                            "truncated payload or missing final newline",
                        ));
                    }
                    if self.input[payload_end] != b'\n' {
                        return Err(ReplayError::decode(
                            &self.path,
                            payload_end,
                            "payload is not followed by newline",
                        ));
                    }
                    let mut payload = Vec::with_capacity(length + 1);
                    payload.push(command.byte());
                    payload.extend_from_slice(&self.input[payload_offset..payload_end]);
                    self.offset = payload_end + 1;
                    self.line = self.line.saturating_add(1);
                    self.record_ordinal = self.record_ordinal.saturating_add(1);
                    let start_time = start_time.ok_or_else(|| {
                        ReplayError::decode(&self.path, record_offset, "missing Time header")
                    })?;
                    let connection_id = connection_id.ok_or_else(|| {
                        ReplayError::decode(&self.path, record_offset, "missing Conn_ID header")
                    })?;
                    let mut decoded = Command::new(
                        payload,
                        start_time,
                        connection_id,
                        self.path.clone(),
                        self.line,
                    )
                    .ok_or_else(|| {
                        ReplayError::decode(&self.path, payload_offset, "empty payload")
                    })?;
                    decoded.command = command;
                    decoded.succeeded = succeeded;
                    decoded.captured_statement_id = captured_statement_id;
                    decoded.prepared_statement = prepared_statement;
                    decoded.record_ordinal = self.record_ordinal;
                    return Ok(decoded);
                }
                _ => {}
            }
        }
    }

    fn read_header_line(&mut self) -> Result<(usize, &'a [u8]), ReplayError> {
        let start = self.offset;
        let relative_end = self.input[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or_else(|| ReplayError::decode(&self.path, start, "unterminated header line"))?;
        if relative_end > self.record_limit {
            return Err(ReplayError::decode(
                &self.path,
                start,
                "header line exceeds record limit",
            ));
        }
        let end = start + relative_end;
        self.offset = end + 1;
        self.line = self.line.saturating_add(1);
        Ok((start, &self.input[start..end]))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn decodes_binary_native_record_and_ties() {
        let input = concat!(
            "# Time: 2026-01-08T19:44:11.099+08:00\n",
            "# Conn_ID: 42\n",
            "# Cmd_type: StmtExecute\n",
            "# Captured_ps_id: 7\n",
            r#"# Prepared_stmt: "select \n?""#,
            "\n",
            "# Payload_len: 5\n",
        );
        let mut bytes = input.as_bytes().to_vec();
        bytes.extend_from_slice(&[7, 0, 0, 0, 0]);
        bytes.push(b'\n');
        let mut decoder = NativeDecoder::new(&bytes, "traffic.log", 64, None);
        let command = decoder
            .next_command()
            .expect("decode succeeds")
            .expect("record");
        assert_eq!(command.command, CommandCode::STMT_EXECUTE);
        assert_eq!(command.payload, vec![0x17, 7, 0, 0, 0, 0]);
        assert_eq!(command.captured_statement_id, Some(7));
        assert_eq!(command.prepared_statement.as_deref(), Some("select \n?"));
        assert!(decoder.next_command().expect("EOF succeeds").is_none());
    }

    #[test]
    fn rejects_oversized_before_allocation() {
        let bytes = b"# Time: 2026-01-08T19:44:11Z\n# Conn_ID: 1\n# Payload_len: 65\n";
        let mut decoder = NativeDecoder::new(bytes, "traffic.log", 64, None);
        let error = decoder.next_command().expect_err("oversize rejected");
        assert!(error.to_string().contains("exceeds limit 64"));
    }

    #[test]
    fn rejects_duplicate_and_missing_newline() {
        let duplicate = b"# Time: 2026-01-08T19:44:11Z\n# Time: 2026-01-08T19:44:12Z\n";
        let mut decoder = NativeDecoder::new(duplicate, "traffic.log", 64, None);
        assert!(decoder.next_command().is_err());

        let truncated = b"# Time: 2026-01-08T19:44:11Z\n# Conn_ID: 1\n# Payload_len: 1\nx";
        let mut decoder = NativeDecoder::new(truncated, "traffic.log", 64, None);
        assert!(decoder.next_command().is_err());
    }

    #[test]
    fn ignores_duplicate_unknown_headers_for_forward_compatibility() {
        let input = concat!(
            "# Future: one\n",
            "# Future: two\n",
            "# Time: 2026-01-08T19:44:11Z\n",
            "# Conn_ID: 1\n",
            "# Payload_len: 0\n",
            "\n"
        );
        let mut decoder = NativeDecoder::new(input.as_bytes(), "traffic.log", 64, None);
        let command = decoder
            .next_command()
            .expect("decode succeeds")
            .expect("record");
        assert_eq!(command.payload, vec![CommandCode::QUERY.byte()]);
    }
}
