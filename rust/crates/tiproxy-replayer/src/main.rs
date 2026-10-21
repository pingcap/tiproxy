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

//! Standalone composition root for the Rust offline replayer.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use replayer_core::config::DEFAULT_RECORD_LIMIT;
use replayer_core::{PreparedCloseStrategy, ReplayConfig, TrafficFormat, dry_run};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use zeroize::Zeroizing;

/// Standalone `TiProxy` traffic replayer.
#[derive(Parser)]
#[command(name = "tiproxy-replayer", version, about)]
#[allow(clippy::struct_excessive_bools)]
struct Options {
    /// Downstream `TiDB` address.
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,
    /// Directory or object-store root containing traffic files.
    #[arg(long, default_value = "")]
    input: String,
    /// Replay speed multiplier.
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// `TiDB` username.
    #[arg(long, default_value = "root")]
    username: String,
    /// `TiDB` password. Never included in debug or output.
    #[arg(long, default_value = "")]
    password: String,
    /// Replay only read-only statements.
    #[arg(long)]
    read_only: bool,
    /// Input format: `native`, `audit_log_plugin`, or `audit_log_extension`.
    #[arg(long, default_value = "")]
    format: TrafficFormat,
    /// Log output path.
    #[arg(long, default_value = "")]
    log_file: String,
    /// Skip commands at or before this RFC3339 start time.
    #[arg(long, value_parser = parse_timestamp)]
    command_start_time: Option<OffsetDateTime>,
    /// Skip audit commands ending before this RFC3339 time.
    #[arg(long, value_parser = parse_timestamp)]
    command_end_time: Option<OffsetDateTime>,
    /// Ignore recoverable replay command errors.
    #[arg(long)]
    ignore_errs: bool,
    /// Audit reorder buffer size; zero disables reordering.
    #[arg(long, default_value_t = 100_000)]
    bufsize: usize,
    /// Optional pprof/API listen address retained for CLI compatibility.
    #[arg(long, default_value = "")]
    pprof_addr: String,
    /// Prepared close strategy: directed, always, or never.
    #[arg(long, default_value = "directed")]
    ps_close: PreparedCloseStrategy,
    /// Decode and filter without connecting to `TiDB`.
    #[arg(long)]
    dry_run: bool,
    /// Local atomic checkpoint path.
    #[arg(long)]
    checkpoint_path: Option<PathBuf>,
    /// Watch for newly published traffic directories.
    #[arg(long)]
    dynamic_input: bool,
    /// Total dynamic-input replayer count.
    #[arg(long, default_value_t = 1)]
    replayer_count: u64,
    /// This dynamic-input replayer index.
    #[arg(long, default_value_t = 0)]
    replayer_index: u64,
    /// Optional local replayed-SQL output path.
    #[arg(long)]
    output_path: Option<PathBuf>,
    /// Run the replay-only HTTP service.
    #[arg(long)]
    service_mode: bool,
    /// Log level retained for CLI compatibility.
    #[arg(long, default_value = "info")]
    log_level: String,
    /// Absolute RFC3339 replay start time; defaults to process start.
    #[arg(long, value_parser = parse_timestamp)]
    start_time: Option<OffsetDateTime>,
    /// Filter audit commands marked as retries.
    #[arg(long)]
    filter_command_with_retry: bool,
    /// Case-insensitive audit user allowlist.
    #[arg(long, value_delimiter = ',')]
    user_allowlist: Vec<String>,
    /// Wait for new files after EOF.
    #[arg(long)]
    wait_on_eof: bool,
    /// AES-256 key file for encrypted native captures.
    #[arg(long)]
    key_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let options = Options::parse();
    match run(options).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tiproxy-replayer stopped: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run(options: Options) -> Result<(), String> {
    let now = OffsetDateTime::now_utc();
    drop(Zeroizing::new(options.password));
    drop((options.log_file, options.pprof_addr, options.log_level));
    if options.service_mode {
        return Err("service mode is not enabled in the current implementation slice".to_owned());
    }
    if !options.dry_run {
        return Err(
            "backend execution is not enabled in the current implementation slice; use --dry-run"
                .to_owned(),
        );
    }
    if options.dynamic_input || options.wait_on_eof {
        return Err("dynamic input is not enabled in the current implementation slice".to_owned());
    }
    if options.output_path.is_some() {
        return Err("SQL output is not enabled in the current implementation slice".to_owned());
    }
    let mut config = ReplayConfig {
        input: options.input,
        speed: options.speed,
        username: options.username,
        address: options.addr,
        format: options.format,
        read_only: options.read_only,
        start_time: options.start_time.unwrap_or(now),
        ignore_errors: options.ignore_errs,
        reorder_buffer: options.bufsize,
        prepared_close: options.ps_close,
        dry_run: options.dry_run,
        checkpoint_path: options.checkpoint_path,
        dynamic_input: options.dynamic_input,
        replayer_count: options.replayer_count,
        replayer_index: options.replayer_index,
        output_path: options.output_path,
        filter_command_with_retry: options.filter_command_with_retry,
        user_allowlist: options.user_allowlist,
        wait_on_eof: options.wait_on_eof,
        command_start_time: options.command_start_time,
        command_end_time: options.command_end_time,
        key_file: options.key_file,
        record_limit: DEFAULT_RECORD_LIMIT,
    };
    config.validate(now).map_err(|error| error.to_string())?;
    let summary = dry_run(&config).await.map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn parse_timestamp(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())
}
