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

//! Dedicated, process-local VIP election with an awaited pre-close boundary.

use std::sync::Arc;
use std::time::Duration;

use control_config::VipConfig;
use control_etcd::{ElectionConfig, ElectionSession, ElectionState};
use control_external::EtcdClientConfig;
use control_plane::logging::{self, Level};
use control_plane::{ControlModule, LifecyclePhase, ModuleContext, ModuleError, ModuleFuture};
use control_topology::ElectionOwnerHistory;
use tokio::sync::watch;

use crate::vip_network::{NetworkError, NetworkOperation};

const TTL_SECONDS: i64 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);
const PRE_CLOSE_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) struct VipModule {
    network: Arc<dyn NetworkOperation>,
    etcd: EtcdClientConfig,
    election: ElectionConfig,
    election_name: String,
    refresh_count: u64,
    stop: watch::Receiver<bool>,
    closed: watch::Sender<Option<Result<(), NetworkError>>>,
    history: Arc<ElectionOwnerHistory>,
}

pub(super) struct VipHandle {
    network: Arc<dyn NetworkOperation>,
    stop: watch::Sender<bool>,
    closed: watch::Receiver<Option<Result<(), NetworkError>>>,
}

impl VipModule {
    pub fn new(
        config: &VipConfig,
        member_id: String,
        process_id: &str,
        etcd: EtcdClientConfig,
        network: Arc<dyn NetworkOperation>,
        history: Arc<ElectionOwnerHistory>,
    ) -> Result<(Self, VipHandle), String> {
        let election_name = config.election_name();
        let session_key = format!("/tiproxy/vip/{}/session/{process_id}", config.ip);
        let election =
            ElectionConfig::new(election_name.clone(), member_id, session_key, TTL_SECONDS)
                .map_err(|error| format!("validate VIP election: {error}"))?;
        let (stop_tx, stop) = watch::channel(false);
        let (closed, closed_rx) = watch::channel(None);
        Ok((
            Self {
                network: Arc::clone(&network),
                etcd,
                election,
                election_name,
                refresh_count: config.garp_refresh_count,
                stop,
                closed,
                history,
            },
            VipHandle {
                network,
                stop: stop_tx,
                closed: closed_rx,
            },
        ))
    }

    async fn serve(
        mut self,
        mut lifecycle: watch::Receiver<control_plane::LifecycleSnapshot>,
        owner: control_plane::OwnerToken,
    ) -> Result<(), ModuleError> {
        // Go removes a stale local VIP before entering the election. Failing
        // here is safer than campaigning with an address this process cannot
        // remove on retirement.
        self.network.delete_ip().await.map_err(network_failure)?;
        // The module is spawned during guarded startup. A winner must not
        // advertise this node before SQL serving has crossed the ready gate.
        while lifecycle.borrow().phase != LifecyclePhase::Ready {
            if *self.stop.borrow() || shutting_down(&lifecycle) {
                return Ok(());
            }
            tokio::select! {
                _ = self.stop.changed() => return Ok(()),
                changed = lifecycle.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                }
            }
        }
        loop {
            if *self.stop.borrow() || shutting_down(&lifecycle) {
                return Ok(());
            }
            let campaign = tokio::select! {
                biased;
                _ = self.stop.changed() => return Ok(()),
                _ = lifecycle.changed() => {
                    if shutting_down(&lifecycle) {
                        return Ok(());
                    }
                    continue;
                },
                result = ElectionSession::campaign(owner.clone(), self.etcd.clone(), self.election.clone()) => result,
            };
            let mut session = match campaign {
                Ok(session) => session,
                Err(error) => {
                    logging::emit(Level::Warn, &format!("VIP campaign retry: {error}"));
                    tokio::select! {
                        _ = self.stop.changed() => return Ok(()),
                        _ = lifecycle.changed() => continue,
                        () = tokio::time::sleep(RETRY_DELAY) => continue,
                    }
                }
            };
            let _owned = OwnedElection::new(Arc::clone(&self.history), self.election_name.clone());
            // If pre-close races a Linux command, dropping its future kills the
            // child; deletion below covers a command that already completed.
            let add = tokio::select! {
                biased;
                _ = self.stop.changed() => None,
                _ = lifecycle.changed() => None,
                result = self.network.add_ip() => Some(result),
            };
            if let Some(Err(error)) = add {
                self.network.delete_ip().await.map_err(network_failure)?;
                let _ = session.shutdown().await;
                return Err(network_failure(error));
            }
            if add.is_some() {
                let first_arp = tokio::select! {
                    biased;
                    _ = self.stop.changed() => None,
                    _ = lifecycle.changed() => None,
                    result = self.network.send_arp() => Some(result),
                };
                if let Some(Err(error)) = first_arp {
                    logging::emit(
                        Level::Warn,
                        &format!(
                            "VIP first GARP burst failed: {} / {}",
                            error.operation, error.class
                        ),
                    );
                }
                logging::emit(Level::Info, "VIP election won and address bound");
                if first_arp.is_some() {
                    Box::pin(self.maintain(&mut session, &mut lifecycle)).await;
                }
            }
            // This awaited delete is the retirement boundary. Never resign the
            // lease while our local interface still has the VIP.
            self.network.delete_ip().await.map_err(network_failure)?;
            let _ = session.shutdown().await;
            logging::emit(Level::Info, "VIP retired and address removed");
            if *self.stop.borrow() || shutting_down(&lifecycle) {
                return Ok(());
            }
            tokio::select! {
                _ = self.stop.changed() => return Ok(()),
                _ = lifecycle.changed() => {},
                () = tokio::time::sleep(RETRY_DELAY) => {}
            }
        }
    }

    async fn maintain(
        &mut self,
        session: &mut ElectionSession,
        lifecycle: &mut watch::Receiver<control_plane::LifecycleSnapshot>,
    ) {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh = tokio::time::interval(Duration::from_secs(1));
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        refresh.tick().await; // the first burst was sent synchronously above
        let mut remaining = self.refresh_count;
        while !*self.stop.borrow() && !shutting_down(lifecycle) {
            enum Event {
                Stop,
                Heartbeat,
                Watch,
                Refresh,
            }
            let event = tokio::select! {
                biased;
                _ = self.stop.changed() => Event::Stop,
                _ = lifecycle.changed() => Event::Stop,
                _ = heartbeat.tick() => Event::Heartbeat,
                result = session.watch_once() => {
                    if result.is_err() { Event::Stop } else { Event::Watch }
                },
                _ = refresh.tick(), if remaining > 0 => Event::Refresh,
            };
            match event {
                Event::Stop => break,
                Event::Heartbeat => {
                    if session.keep_alive().await.is_err() {
                        // Uncertain ownership cannot leave an address bound
                        // until a remote lease may expire and a peer takes it.
                        break;
                    }
                }
                Event::Watch => {}
                Event::Refresh => {
                    remaining -= 1;
                    let send = tokio::select! {
                        biased;
                        _ = self.stop.changed() => break,
                        _ = lifecycle.changed() => break,
                        result = self.network.send_arp() => result,
                    };
                    if let Err(error) = send {
                        logging::emit(
                            Level::Warn,
                            &format!(
                                "VIP GARP refresh stopped: {} / {}",
                                error.operation, error.class
                            ),
                        );
                        remaining = 0;
                    }
                }
            }
            if matches!(
                session.snapshot().state,
                ElectionState::Uncertain | ElectionState::Retired | ElectionState::Stopped
            ) {
                break;
            }
        }
    }
}

impl ControlModule for VipModule {
    fn name(&self) -> &'static str {
        "vip_owner"
    }

    fn run(self: Box<Self>, context: ModuleContext) -> ModuleFuture {
        Box::pin(async move {
            let network = Arc::clone(&self.network);
            let closed = self.closed.clone();
            let result = Box::pin(self.serve(context.lifecycle(), context.owner().clone())).await;
            // A final delete covers an interrupted add and gives pre-close a
            // direct result even if the election loop failed before it won.
            let cleanup = network.delete_ip().await;
            closed.send_replace(Some(cleanup));
            match (result, cleanup) {
                (Err(error), _) => Err(error),
                (Ok(()), Err(error)) => Err(network_failure(error)),
                (Ok(()), Ok(())) => Ok(()),
            }
        })
    }
}

impl VipHandle {
    /// Removes the local VIP and waits for the actor to retire before the SQL
    /// listener enters drain. A timed-out actor is never reported as closed:
    /// it may still have an in-flight add after a direct deletion.
    pub async fn pre_close(&mut self) -> Result<(), NetworkError> {
        self.stop.send_replace(true);
        let wait = async {
            loop {
                if let Some(result) = *self.closed.borrow() {
                    return result;
                }
                if self.closed.changed().await.is_err() {
                    return self.network.delete_ip().await;
                }
            }
        };
        if let Ok(result) = tokio::time::timeout(PRE_CLOSE_TIMEOUT, wait).await {
            match result {
                Ok(()) => Ok(()),
                Err(_) => self.network.delete_ip().await,
            }
        } else {
            self.network.delete_ip().await?;
            Err(NetworkError {
                operation: "pre_close",
                class: "actor_timeout",
            })
        }
    }
}

struct OwnedElection {
    history: Arc<ElectionOwnerHistory>,
    key: String,
}

impl OwnedElection {
    fn new(history: Arc<ElectionOwnerHistory>, key: String) -> Self {
        history.won(&key);
        Self { history, key }
    }
}

impl Drop for OwnedElection {
    fn drop(&mut self) {
        self.history.retired(&self.key);
    }
}

fn shutting_down(lifecycle: &watch::Receiver<control_plane::LifecycleSnapshot>) -> bool {
    matches!(
        lifecycle.borrow().phase,
        LifecyclePhase::Quiescing
            | LifecyclePhase::Draining
            | LifecyclePhase::Stopping
            | LifecyclePhase::Stopped
            | LifecyclePhase::Failed
    )
}

fn network_failure(error: NetworkError) -> ModuleError {
    logging::emit(
        Level::Error,
        &format!(
            "VIP network operation failed: {} / {}",
            error.operation, error.class
        ),
    );
    ModuleError {
        module: "vip_owner",
        error_class: "network_failed",
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::watch;

    use super::VipHandle;
    use crate::vip_network::{NetworkError, NetworkOperation};

    type NetworkFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

    #[derive(Default)]
    struct FakeNetwork {
        deletes: AtomicUsize,
    }

    impl NetworkOperation for FakeNetwork {
        fn has_ip(&self) -> NetworkFuture<'_, bool> {
            Box::pin(async { Ok(false) })
        }

        fn add_ip(&self) -> NetworkFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn delete_ip(&self) -> NetworkFuture<'_, ()> {
            Box::pin(async {
                self.deletes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn send_arp(&self) -> NetworkFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn pre_close_waits_for_actor_retirement_before_reporting_success() {
        let network = Arc::new(FakeNetwork::default());
        let (stop, mut stop_rx) = watch::channel(false);
        let (closed, closed_rx) = watch::channel(None);
        let mut handle = VipHandle {
            network: network.clone(),
            stop,
            closed: closed_rx,
        };
        let close = tokio::spawn(async move { handle.pre_close().await });
        assert!(stop_rx.changed().await.is_ok());
        assert!(*stop_rx.borrow());
        assert!(!close.is_finished(), "SQL drain must await VIP retirement");
        closed.send_replace(Some(Ok(())));
        assert!(matches!(close.await, Ok(Ok(()))));
        assert_eq!(network.deletes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn pre_close_deletes_after_actor_exits_without_an_ack() {
        let network = Arc::new(FakeNetwork::default());
        let (stop, _stop_rx) = watch::channel(false);
        let (closed, closed_rx) = watch::channel(None);
        drop(closed);
        let mut handle = VipHandle {
            network: network.clone(),
            stop,
            closed: closed_rx,
        };
        assert!(handle.pre_close().await.is_ok());
        assert_eq!(network.deletes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pre_close_retries_delete_after_actor_cleanup_failure() {
        let network = Arc::new(FakeNetwork::default());
        let (stop, _stop_rx) = watch::channel(false);
        let (closed, closed_rx) = watch::channel(Some(Err(NetworkError {
            operation: "del",
            class: "ip_failed",
        })));
        let mut handle = VipHandle {
            network: network.clone(),
            stop,
            closed: closed_rx,
        };
        assert!(handle.pre_close().await.is_ok());
        assert_eq!(network.deletes.load(Ordering::SeqCst), 1);
        drop(closed);
    }
}
