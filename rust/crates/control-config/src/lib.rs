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

//! Rust-native `TiProxy` configuration and namespace domain.

#![forbid(unsafe_code)]

pub mod model;
pub mod module;
pub mod source;

pub use model::{
    AwsMeteringConfig, AzureMeteringConfig, BackendClusterConfig, ClientTlsConfig,
    CloudMeteringConfig, ConfigError, ConfigPersistence, EffectiveConfig, HealthCheckConfig,
    LocalFsMeteringConfig, LogOnlineConfig, MeteringConfig, NamespaceConfig, ProxyOnlineConfig,
    RoutingBalancePolicy, RoutingConfig, RoutingConnectionFactorConfig, RoutingFactorConfig,
    RoutingNamespace, RoutingRule, RoutingSelectionPolicy, ServingConfig, ServingKeepalive,
    ServingListener, ServingNamespace, ServingTlsConfig, SourceRevision, TopologyConfig,
    TopologyRuntimeIdentity, VipConfig,
};
pub use module::{
    ConfigModule, ConfigModuleHandle, ConfigModuleOptions, ConfigMutationError,
    PersistenceClientFactory,
};
pub use source::{
    CONFIG_PREFIX, CandidateValidator, ConfigNamespaceSnapshot, ConfigNamespaceSource,
    ConfigNamespaceStore, LOG_CONFIG_KEY, NAMESPACE_CONFIG_PREFIX, NamespaceIncarnation,
    PROXY_CONFIG_KEY, PersistentConfigSnapshot, PreparedArtifact, ResourceIncarnation, StoreError,
    decode_persistent_entries, encode_canonical_config,
};
