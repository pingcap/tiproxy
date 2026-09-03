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

//! Bounded input, decoding, and checkpoint primitives for `TiProxy`'s offline
//! traffic replayer.

#![forbid(unsafe_code)]

pub mod checkpoint;
pub mod command;
pub mod config;
pub mod decode;
pub mod engine;
pub mod error;
pub mod storage;

pub use checkpoint::{Checkpoint, InputIdentity};
pub use command::{Command, CommandCode, PreparedCloseStrategy, TrafficFormat};
pub use config::ReplayConfig;
pub use engine::{DryRunSummary, dry_run};
pub use error::ReplayError;
