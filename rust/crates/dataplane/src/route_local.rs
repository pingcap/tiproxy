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

//! Process-local initial-route adapter.
//!
//! This is deliberately a narrow adapter between the existing dataplane
//! acquisition loop and one exact [`control_router::RouteAdmission`]. It owns
//! the reservation authority for the whole SQL session; dropping it closes the
//! selector ledger through `RouteAdmission` without involving the Go bridge.

use control_router::{
    Reservation, RouteAdmission, RouteCommandReceiver, RouteCommandRegistration, RouteError,
    Settlement,
};
use control_routing::group::ClientInfo;
use control_routing::{RouteAssignment, RouteCode, RouteResult};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, timeout_at};

use crate::{RouteChannel, RouteChannelError};

pub(crate) const LOCAL_ROUTE_TRANSIENT_WAIT: Duration = Duration::from_secs(1);

trait LocalRouteAuthority: Send + Sync {
    fn namespace(&self) -> &str;
    fn has_pending(&self) -> bool;
    fn next(
        &mut self,
        client: ClientInfo<'_>,
        listener_port: &str,
    ) -> Result<RouteAssignment, RouteError>;
    fn finish(&mut self, assignment_id: &str, connected: bool) -> Option<Settlement>;
}

struct PlaneRouteAuthority {
    // Field order is intentional: unregister and drain exact command guards
    // before dropping the admission/selector that closes the ledger session.
    _commands: RouteCommandRegistration,
    admission: RouteAdmission,
    pending: Option<Reservation>,
}

impl LocalRouteAuthority for PlaneRouteAuthority {
    fn namespace(&self) -> &str {
        self.admission.namespace()
    }

    fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    fn next(
        &mut self,
        client: ClientInfo<'_>,
        listener_port: &str,
    ) -> Result<RouteAssignment, RouteError> {
        if self.pending.is_some() {
            return Err(RouteError::AlreadyActive);
        }
        let reservation = self.admission.selector_mut().next(client, listener_port)?;
        let assignment = reservation.assignment().clone();
        self.pending = Some(reservation);
        Ok(assignment)
    }

    fn finish(&mut self, assignment_id: &str, connected: bool) -> Option<Settlement> {
        let reservation = self.pending.take()?;
        if assignment_id != reservation.assignment().assignment_id {
            self.pending = Some(reservation);
            return None;
        }
        Some(self.admission.selector().finish(&reservation, connected))
    }
}

/// A session-long local route lease implementing the dataplane route channel.
pub struct LocalRouteChannel {
    authority: Box<dyn LocalRouteAuthority>,
    updates: Option<watch::Receiver<u64>>,
    connection_id: u64,
    client_address: String,
    proxy_address: String,
    listener_port: String,
    requested: bool,
}

/// Releases the orderly session's exact local route authority. Aborted engine
/// tasks retain the field-level drop fallback; the normal end path calls this
/// seam so tests can observe the accounting edge explicitly.
pub(crate) fn release_local_route_lease(lease: &mut Option<LocalRouteChannel>) {
    drop(lease.take());
    debug_assert!(lease.is_none());
}

impl LocalRouteChannel {
    /// Captures the immutable connection metadata used by every retry.
    ///
    /// # Errors
    ///
    /// Returns an error if this exact route session already has a command
    /// registration.
    pub fn new(
        admission: RouteAdmission,
        connection_id: u64,
        client_address: impl Into<String>,
        proxy_address: impl Into<String>,
        listener_port: impl Into<String>,
    ) -> Result<(Self, RouteCommandReceiver), RouteError> {
        const COMMAND_CAPACITY: usize = 8;

        let updates = admission.subscribe_updates();
        let (commands, receiver) = admission.register_commands(connection_id, COMMAND_CAPACITY)?;
        Ok((
            Self::with_authority(
                Box::new(PlaneRouteAuthority {
                    _commands: commands,
                    admission,
                    pending: None,
                }),
                Some(updates),
                connection_id,
                client_address.into(),
                proxy_address.into(),
                listener_port.into(),
            ),
            receiver,
        ))
    }

    fn with_authority(
        authority: Box<dyn LocalRouteAuthority>,
        updates: Option<watch::Receiver<u64>>,
        connection_id: u64,
        client_address: String,
        proxy_address: String,
        listener_port: String,
    ) -> Self {
        Self {
            authority,
            updates,
            connection_id,
            client_address,
            proxy_address,
            listener_port,
            requested: false,
        }
    }

    /// Exact namespace/router incarnation retained by this lease.
    #[must_use]
    pub fn namespace(&self) -> &str {
        self.authority.namespace()
    }

    fn terminal(&self, error: RouteError) -> RouteAssignment {
        let category = error.category();
        #[cfg(debug_assertions)]
        if std::env::var_os("TIPROXY_ROUTE_DIAGNOSTIC").is_some() {
            eprintln!("local route terminal category={category}");
        }
        let (code, detail) = match error {
            RouteError::NoBackend | RouteError::WrappedNoBackend => (
                RouteCode::NoBackend,
                "No available TiDB instances, please make sure TiDB is available",
            ),
            RouteError::Observer(_)
            | RouteError::ControlUnavailable
            | RouteError::StaleCandidate
            | RouteError::NamespaceMissing
            | RouteError::NamespaceReplaced => (RouteCode::ControlUnavailable, category),
            RouteError::PortConflict | RouteError::InvalidConfig => {
                (RouteCode::InvalidSnapshot, category)
            }
            RouteError::Unsupported(_) => (RouteCode::UnsupportedConfiguration, category),
            RouteError::Capacity => (RouteCode::QueueFull, category),
            RouteError::WorkerRunning
            | RouteError::InvalidSession
            | RouteError::AlreadyActive
            | RouteError::Exhausted
            | RouteError::NotActive
            | RouteError::RedirectPending
            | RouteError::CoolingDown
            | RouteError::ForceClosing
            | RouteError::SameBackend
            | RouteError::CrossKeyspace => (RouteCode::Internal, category),
        };
        RouteAssignment {
            connection_id: self.connection_id,
            code,
            detail: detail.to_owned(),
            ..RouteAssignment::default()
        }
    }
}

impl RouteChannel for LocalRouteChannel {
    async fn request_route(
        &mut self,
        excluded_backend_ids: Vec<String>,
    ) -> Result<(), RouteChannelError> {
        // Selector::next owns the only exclusion cycle. Accepting bridge-era
        // exclusions here would apply a second policy and could choose a
        // different backend from the process-local router.
        if self.requested || !excluded_backend_ids.is_empty() {
            return Err(RouteChannelError::Rejected);
        }
        self.requested = true;
        Ok(())
    }

    async fn next_assignment(&mut self) -> Result<RouteAssignment, RouteChannelError> {
        if !self.requested || self.authority.has_pending() {
            return Err(RouteChannelError::Rejected);
        }
        let deadline = Instant::now() + LOCAL_ROUTE_TRANSIENT_WAIT;
        let mut assignment = loop {
            let client = ClientInfo {
                client_address: Some(&self.client_address),
                proxy_address: Some(&self.proxy_address),
            };
            match self.authority.next(client, &self.listener_port) {
                Ok(assignment) => break assignment,
                Err(error @ (RouteError::ControlUnavailable | RouteError::StaleCandidate)) => {
                    let Some(updates) = &mut self.updates else {
                        return Ok(self.terminal(error));
                    };
                    if !matches!(timeout_at(deadline, updates.changed()).await, Ok(Ok(()))) {
                        return Ok(self.terminal(error));
                    }
                }
                Err(error) => return Ok(self.terminal(error)),
            }
        };
        assignment.connection_id = self.connection_id;
        Ok(assignment)
    }

    async fn report_result(&mut self, result: RouteResult) -> Result<(), RouteChannelError> {
        if result.connection_id != self.connection_id {
            return Err(RouteChannelError::Rejected);
        }
        if self
            .authority
            .finish(&result.assignment_id, result.connected)
            != Some(Settlement::Applied)
        {
            return Err(RouteChannelError::Rejected);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, PoisonError};

    use control_routing::{RouteAssignment, RouteCode, RouteErrorSource, RouteResult};
    use tokio::sync::watch;

    use super::{
        ClientInfo, LOCAL_ROUTE_TRANSIENT_WAIT, LocalRouteAuthority, LocalRouteChannel,
        RouteChannel, RouteChannelError, RouteError, Settlement,
    };

    #[derive(Default)]
    struct Observed {
        calls: Vec<(String, String, String)>,
        finishes: Vec<(String, bool)>,
        drops: usize,
        active: usize,
    }

    struct FakeAuthority {
        namespace: String,
        observed: Arc<Mutex<Observed>>,
        answers: VecDeque<Result<RouteAssignment, RouteError>>,
        update_on_error: Option<watch::Sender<u64>>,
        notify_on_error: bool,
        pending: Option<String>,
        active: bool,
    }

    impl Drop for FakeAuthority {
        fn drop(&mut self) {
            let mut observed = self.observed.lock().unwrap_or_else(PoisonError::into_inner);
            observed.drops += 1;
            if self.active {
                observed.active -= 1;
            }
        }
    }

    impl LocalRouteAuthority for FakeAuthority {
        fn namespace(&self) -> &str {
            &self.namespace
        }

        fn has_pending(&self) -> bool {
            self.pending.is_some()
        }

        fn next(
            &mut self,
            client: ClientInfo<'_>,
            listener_port: &str,
        ) -> Result<RouteAssignment, RouteError> {
            self.observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .calls
                .push((
                    client.client_address.unwrap_or_default().to_owned(),
                    client.proxy_address.unwrap_or_default().to_owned(),
                    listener_port.to_owned(),
                ));
            let answer = self
                .answers
                .pop_front()
                .unwrap_or(Err(RouteError::NoBackend));
            if answer.is_err()
                && self.notify_on_error
                && let Some(updates) = &self.update_on_error
            {
                updates.send_modify(|revision| *revision = revision.saturating_add(1));
            }
            let answer = answer?;
            self.pending = Some(answer.assignment_id.clone());
            Ok(answer)
        }

        fn finish(&mut self, assignment_id: &str, connected: bool) -> Option<Settlement> {
            if self.pending.as_deref() != Some(assignment_id) {
                return None;
            }
            self.pending = None;
            let mut observed = self.observed.lock().unwrap_or_else(PoisonError::into_inner);
            observed
                .finishes
                .push((assignment_id.to_owned(), connected));
            if connected {
                assert!(!self.active, "a fake session can become active only once");
                self.active = true;
                observed.active += 1;
            }
            Some(Settlement::Applied)
        }
    }

    fn assignment(id: &str) -> RouteAssignment {
        RouteAssignment {
            assignment_id: id.to_owned(),
            backend_id: "backend-a".to_owned(),
            backend_address: "127.0.0.1:4000".to_owned(),
            code: RouteCode::Ok,
            ..RouteAssignment::default()
        }
    }

    fn channel(
        answers: impl IntoIterator<Item = Result<RouteAssignment, RouteError>>,
    ) -> (LocalRouteChannel, Arc<Mutex<Observed>>) {
        let observed = Arc::new(Mutex::new(Observed::default()));
        let authority = FakeAuthority {
            namespace: "tenant-a".to_owned(),
            observed: Arc::clone(&observed),
            answers: answers.into_iter().collect(),
            update_on_error: None,
            notify_on_error: false,
            pending: None,
            active: false,
        };
        (
            LocalRouteChannel::with_authority(
                Box::new(authority),
                None,
                17,
                "203.0.113.7:5000".to_owned(),
                "192.0.2.8:6000".to_owned(),
                "4000".to_owned(),
            ),
            observed,
        )
    }

    fn channel_with_updates(
        answers: impl IntoIterator<Item = Result<RouteAssignment, RouteError>>,
    ) -> (LocalRouteChannel, Arc<Mutex<Observed>>) {
        channel_with_update_behavior(answers, true)
    }

    fn channel_with_silent_updates(
        answers: impl IntoIterator<Item = Result<RouteAssignment, RouteError>>,
    ) -> (LocalRouteChannel, Arc<Mutex<Observed>>) {
        channel_with_update_behavior(answers, false)
    }

    fn channel_with_update_behavior(
        answers: impl IntoIterator<Item = Result<RouteAssignment, RouteError>>,
        notify_on_error: bool,
    ) -> (LocalRouteChannel, Arc<Mutex<Observed>>) {
        let observed = Arc::new(Mutex::new(Observed::default()));
        let (updates, updates_rx) = watch::channel(0u64);
        let authority = FakeAuthority {
            namespace: "tenant-a".to_owned(),
            observed: Arc::clone(&observed),
            answers: answers.into_iter().collect(),
            update_on_error: Some(updates),
            notify_on_error,
            pending: None,
            active: false,
        };
        (
            LocalRouteChannel::with_authority(
                Box::new(authority),
                Some(updates_rx),
                17,
                "203.0.113.7:5000".to_owned(),
                "192.0.2.8:6000".to_owned(),
                "4000".to_owned(),
            ),
            observed,
        )
    }

    #[tokio::test]
    async fn local_channel_owns_one_retry_cycle_and_exact_settlement() {
        let (mut channel, observed) = channel([Ok(assignment("attempt-1"))]);
        assert_eq!(channel.namespace(), "tenant-a");
        assert_eq!(
            channel.request_route(vec!["external".to_owned()]).await,
            Err(RouteChannelError::Rejected)
        );
        assert_eq!(channel.request_route(Vec::new()).await, Ok(()));
        assert_eq!(
            channel.request_route(Vec::new()).await,
            Err(RouteChannelError::Rejected)
        );

        let assignment = channel.next_assignment().await;
        assert!(assignment.is_ok(), "expected an assignment: {assignment:?}");
        let Ok(assignment) = assignment else {
            return;
        };
        assert_eq!(assignment.connection_id, 17);
        assert_eq!(assignment.assignment_id, "attempt-1");
        assert_eq!(
            channel.next_assignment().await,
            Err(RouteChannelError::Rejected),
            "an unsettled exact reservation cannot be replaced"
        );
        assert_eq!(
            channel
                .report_result(RouteResult {
                    connection_id: 18,
                    assignment_id: "attempt-1".to_owned(),
                    connected: false,
                    error_source: RouteErrorSource::BackendNetwork,
                    code: RouteCode::BackendDialFailed,
                    detail: String::new(),
                })
                .await,
            Err(RouteChannelError::Rejected)
        );
        assert_eq!(
            channel
                .report_result(RouteResult {
                    connection_id: 17,
                    assignment_id: "wrong".to_owned(),
                    connected: false,
                    error_source: RouteErrorSource::BackendNetwork,
                    code: RouteCode::BackendDialFailed,
                    detail: String::new(),
                })
                .await,
            Err(RouteChannelError::Rejected)
        );
        assert_eq!(
            channel
                .report_result(RouteResult {
                    connection_id: 17,
                    assignment_id: "attempt-1".to_owned(),
                    connected: false,
                    error_source: RouteErrorSource::BackendNetwork,
                    code: RouteCode::BackendDialFailed,
                    detail: String::new(),
                })
                .await,
            Ok(())
        );

        let observed = observed.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(
            observed.calls,
            [(
                "203.0.113.7:5000".to_owned(),
                "192.0.2.8:6000".to_owned(),
                "4000".to_owned()
            )]
        );
        assert_eq!(observed.finishes, [("attempt-1".to_owned(), false)]);
    }

    #[tokio::test]
    async fn local_channel_preserves_closed_source_error_categories() {
        let (mut channel, _) = channel([Err(RouteError::Observer(
            control_topology::ObserverError::TopologyUnavailable,
        ))]);
        assert_eq!(channel.request_route(Vec::new()).await, Ok(()));
        let terminal = channel.next_assignment().await;
        assert!(
            terminal.is_ok(),
            "expected a terminal assignment: {terminal:?}"
        );
        let Ok(terminal) = terminal else {
            return;
        };
        assert_eq!(terminal.connection_id, 17);
        assert_eq!(terminal.code, RouteCode::ControlUnavailable);
        assert_eq!(terminal.detail, "topology_unavailable");
    }

    #[tokio::test]
    async fn local_channel_retries_transient_source_drift_after_reconcile() {
        let (mut channel, observed) = channel_with_updates([
            Err(RouteError::StaleCandidate),
            Err(RouteError::ControlUnavailable),
            Ok(assignment("current")),
        ]);
        assert_eq!(channel.request_route(Vec::new()).await, Ok(()));
        let selected = channel.next_assignment().await;
        assert!(
            selected.is_ok(),
            "expected current assignment: {selected:?}"
        );
        let Ok(selected) = selected else {
            return;
        };
        assert_eq!(selected.assignment_id, "current");
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .calls
                .len(),
            3
        );
    }

    #[tokio::test(start_paused = true)]
    async fn local_channel_does_not_wait_or_retry_semantic_errors() {
        let (mut channel, observed) =
            channel_with_silent_updates([Err(RouteError::NamespaceMissing)]);
        assert_eq!(channel.request_route(Vec::new()).await, Ok(()));
        let started = tokio::time::Instant::now();
        let terminal = channel.next_assignment().await;
        assert_eq!(tokio::time::Instant::now(), started);
        assert!(
            terminal.is_ok(),
            "expected terminal assignment: {terminal:?}"
        );
        let Ok(terminal) = terminal else {
            return;
        };
        assert_eq!(terminal.code, RouteCode::ControlUnavailable);
        assert_eq!(terminal.detail, "namespace_missing");
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .calls
                .len(),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn local_channel_returns_last_transient_when_update_deadline_expires() {
        let (mut channel, observed) =
            channel_with_silent_updates([Err(RouteError::StaleCandidate)]);
        assert_eq!(channel.request_route(Vec::new()).await, Ok(()));
        let started = tokio::time::Instant::now();
        let terminal = channel.next_assignment().await;
        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            LOCAL_ROUTE_TRANSIENT_WAIT
        );
        assert!(
            terminal.is_ok(),
            "expected terminal assignment: {terminal:?}"
        );
        let Ok(terminal) = terminal else {
            return;
        };
        assert_eq!(terminal.code, RouteCode::ControlUnavailable);
        assert_eq!(terminal.detail, "stale_candidate");
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .calls
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn successful_local_channel_remains_the_session_lease_until_drop() {
        let (mut channel, observed) = channel([Ok(assignment("active"))]);
        assert_eq!(channel.request_route(Vec::new()).await, Ok(()));
        let assignment = channel.next_assignment().await;
        assert!(assignment.is_ok(), "expected assignment: {assignment:?}");
        assert_eq!(
            channel
                .report_result(RouteResult {
                    connection_id: 17,
                    assignment_id: "active".to_owned(),
                    connected: true,
                    error_source: RouteErrorSource::Unspecified,
                    code: RouteCode::Ok,
                    detail: String::new(),
                })
                .await,
            Ok(())
        );
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .drops,
            0
        );
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .finishes,
            [("active".to_owned(), true)]
        );
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .active,
            1,
            "a successful settlement remains charged for the live session"
        );
        let mut lease = Some(channel);
        super::release_local_route_lease(&mut lease);
        let observed = observed.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(observed.drops, 1, "the session authority is released once");
        assert_eq!(
            observed.active, 0,
            "the explicit orderly-end seam returns active accounting to baseline"
        );
    }
}
