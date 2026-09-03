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

//! Emits canonical Rust decoder observations for the checked-in Go/Rust corpus.

use std::error::Error;
use std::fs::File;
use std::io::{self, Write};

use replayer_core::decode::{AuditDecoder, NativeDecoder};
use replayer_core::{Command, PreparedCloseStrategy, TrafficFormat};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    format: TrafficFormat,
    prepared_close: PreparedCloseStrategy,
    filter_retries: bool,
    user_allowlist: Vec<String>,
    source_ordinal: u64,
    records: Vec<NativeRecord>,
    lines: Vec<String>,
}

#[derive(Deserialize)]
struct NativeRecord {
    headers: Vec<(String, String)>,
    payload_hex: String,
}

#[derive(Serialize)]
struct Observation {
    case_id: String,
    index: u64,
    payload_hex: String,
    start_unix_nanos: i64,
    end_unix_nanos: i64,
    connection_id: u64,
    upstream_connection_id: u64,
    command: u8,
    current_database: String,
    captured_statement_id: u32,
    prepared_statement: String,
    statement_type: String,
    succeeded: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let corpus_path = std::env::var("CP_REPLAYER_CORPUS")?;
    let corpus: Corpus = serde_json::from_reader(File::open(corpus_path)?)?;
    let mut observations = Vec::new();
    for case in corpus.cases {
        let commands = decode_case(&case)?;
        for (index, command) in commands.into_iter().enumerate() {
            observations.push(observe(&case.id, index, command)?);
        }
    }
    if std::env::var_os("CP_REPLAYER_MUTATE_CONNECTION_ID").is_some()
        && let Some(first) = observations.first_mut()
    {
        first.connection_id = first.connection_id.saturating_add(1);
    }
    serde_json::to_writer(io::stdout().lock(), &observations)?;
    io::stdout().lock().write_all(b"\n")?;
    Ok(())
}

fn decode_case(case: &Case) -> Result<Vec<Command>, Box<dyn Error>> {
    let input = build_input(case)?;
    match case.format {
        TrafficFormat::Native => {
            let mut decoder = NativeDecoder::new(&input, &case.id, 64 * 1024 * 1024, None);
            let mut commands = Vec::new();
            while let Some(mut command) = decoder.next_command()? {
                command.source_ordinal = case.source_ordinal;
                commands.push(command);
            }
            Ok(commands)
        }
        TrafficFormat::AuditLogPlugin | TrafficFormat::AuditLogExtension => {
            let mut decoder = AuditDecoder::new(
                case.format,
                &case.id,
                64 * 1024 * 1024,
                case.prepared_close,
                None,
                None,
                case.filter_retries,
                &case.user_allowlist,
                case.source_ordinal,
            )?;
            Ok(decoder.decode_all(&input)?)
        }
    }
}

fn build_input(case: &Case) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut input = Vec::new();
    if case.format == TrafficFormat::Native {
        for record in &case.records {
            for (key, value) in &record.headers {
                writeln!(input, "# {key}: {value}")?;
            }
            let payload = decode_hex(&record.payload_hex)?;
            writeln!(input, "# Payload_len: {}", payload.len())?;
            input.extend_from_slice(&payload);
            input.push(b'\n');
        }
    } else {
        for line in &case.lines {
            input.extend_from_slice(line.as_bytes());
            input.push(b'\n');
        }
    }
    Ok(input)
}

fn observe(case_id: &str, index: usize, command: Command) -> Result<Observation, Box<dyn Error>> {
    Ok(Observation {
        case_id: case_id.to_owned(),
        index: u64::try_from(index)?,
        payload_hex: encode_hex(&command.payload),
        start_unix_nanos: i64::try_from(command.start_time.unix_timestamp_nanos())?,
        end_unix_nanos: command
            .end_time
            .map_or(Ok(0), |value| i64::try_from(value.unix_timestamp_nanos()))?,
        connection_id: command.connection_id,
        upstream_connection_id: command.upstream_connection_id,
        command: command.command.byte(),
        current_database: command.current_database,
        captured_statement_id: command.captured_statement_id.unwrap_or_default(),
        prepared_statement: command.prepared_statement.unwrap_or_default(),
        statement_type: command.statement_type.unwrap_or_default(),
        succeeded: command.succeeded,
    })
}

fn decode_hex(value: &str) -> Result<Vec<u8>, io::Error> {
    if !value.len().is_multiple_of(2) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "hex payload has odd length",
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            u8::from_str_radix(text, 16)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        })
        .collect()
}

fn encode_hex(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}
