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

//! End-to-end dry-run storage and checkpoint contract tests.

use std::error::Error;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use aes::Aes256;
use ctr::cipher::{KeyIvInit, StreamCipher};
use flate2::Compression;
use flate2::write::GzEncoder;
use replayer_core::config::DEFAULT_RECORD_LIMIT;
use replayer_core::{Checkpoint, PreparedCloseStrategy, ReplayConfig, TrafficFormat, dry_run};
use time::OffsetDateTime;

type TestResult = Result<(), Box<dyn Error>>;
type Aes256Ctr = ctr::Ctr128BE<Aes256>;

static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn temporary_directory(name: &str) -> Result<PathBuf, std::io::Error> {
    let sequence = DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "tiproxy-replayer-{name}-{}-{sequence}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

fn native_record(timestamp: &str, connection_id: u64, sql: &str) -> Vec<u8> {
    format!(
        "# Time: {timestamp}\n# Conn_ID: {connection_id}\n# Payload_len: {}\n{sql}\n",
        sql.len()
    )
    .into_bytes()
}

fn dry_run_config(input: &Path, checkpoint: Option<PathBuf>) -> ReplayConfig {
    ReplayConfig {
        input: input.to_string_lossy().into_owned(),
        speed: 1.0,
        username: "root".to_owned(),
        address: "127.0.0.1:4000".to_owned(),
        format: TrafficFormat::Native,
        read_only: false,
        start_time: OffsetDateTime::now_utc(),
        ignore_errors: false,
        reorder_buffer: 100_000,
        prepared_close: PreparedCloseStrategy::Directed,
        dry_run: true,
        checkpoint_path: checkpoint,
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

#[tokio::test]
async fn checkpoint_uses_full_tie_frontier_and_content_identity() -> TestResult {
    let directory = temporary_directory("checkpoint")?;
    let traffic = directory.join("traffic-2026-01-08T19-44-11.099.log");
    let checkpoint_path = directory.join("checkpoint.json");
    let timestamp = "2026-01-08T19:44:11.099+08:00";
    let mut input = native_record(timestamp, 2, "SELECT 1");
    input.extend(native_record(timestamp, 1, "SELECT 1"));
    std::fs::write(&traffic, &input)?;

    let config = dry_run_config(&directory, Some(checkpoint_path.clone()));
    let first = dry_run(&config).await?;
    assert_eq!(first.decoded_commands, 2);
    assert_eq!(first.replayed_commands, 2);
    let checkpoint = Checkpoint::load(&checkpoint_path, &first.input_identity)?
        .ok_or("checkpoint was not persisted")?;
    assert_eq!(checkpoint.connection_id, 2);
    assert_eq!(checkpoint.source_ordinal, 0);
    assert_eq!(checkpoint.record_ordinal, 1);

    let resumed = dry_run(&config).await?;
    assert_eq!(resumed.decoded_commands, 0);
    assert_eq!(resumed.replayed_commands, 0);

    let mut changed = native_record(timestamp, 2, "SELECT 2");
    changed.extend(native_record(timestamp, 1, "SELECT 1"));
    assert_eq!(changed.len(), input.len());
    std::fs::write(&traffic, changed)?;
    let mismatch = dry_run(&config).await;
    assert!(mismatch.is_err());
    assert!(
        mismatch
            .err()
            .is_some_and(|error| error.to_string().contains("input identity"))
    );

    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[tokio::test]
async fn gzip_then_aes_ctr_matches_native_storage_layers() -> TestResult {
    let directory = temporary_directory("encrypted")?;
    let key_path = directory.join("key");
    let key = [0x5a_u8; 32];
    std::fs::write(&key_path, key)?;
    std::fs::write(
        directory.join("meta"),
        br#"{"Version":"v1","Duration":1,"Cmds":1,"EncryptMethod":"aes256-ctr"}"#,
    )?;

    let plaintext = native_record("2026-01-08T19:44:11.099+08:00", 1, "SELECT 1");
    let iv = [0x24_u8; 16];
    let mut encrypted = plaintext;
    let mut cipher = Aes256Ctr::new_from_slices(&key, &iv)?;
    cipher.apply_keystream(&mut encrypted);
    let mut stored = iv.to_vec();
    stored.extend(encrypted);
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&stored)?;
    std::fs::write(
        directory.join("traffic-2026-01-08T19-44-11.099.log.gz"),
        encoder.finish()?,
    )?;

    let mut config = dry_run_config(&directory, None);
    config.key_file = Some(key_path);
    let summary = dry_run(&config).await?;
    assert_eq!(summary.input_files, 1);
    assert_eq!(summary.decoded_commands, 1);
    assert_eq!(summary.replayed_commands, 1);

    std::fs::remove_dir_all(directory)?;
    Ok(())
}
