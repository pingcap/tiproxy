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

//! Tick budgets and bounded, payload-free migration commands.
use crate::{ForceClose, Redirect, Router, Session, Settlement};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
pub(crate) const TICK: Duration = Duration::from_millis(10);
const PRODUCTION_REDIRECT_TTL: Duration = Duration::from_secs(15);

/// One exact migration operation.  The token contains no SQL payload and is
/// meaningful only to the router incarnation that minted it.
#[derive(Clone, Debug)]
pub enum MigrationCommand {
    /// Score transferred; physical completion is a later exact terminal.
    Redirect(Redirect),
    /// Closing admitted; accounting stays until an observed close.
    ForceClose(ForceClose),
}
impl From<Redirect> for MigrationCommand {
    fn from(value: Redirect) -> Self {
        Self::Redirect(value)
    }
}
impl From<ForceClose> for MigrationCommand {
    fn from(value: ForceClose) -> Self {
        Self::ForceClose(value)
    }
}

impl MigrationCommand {
    fn same_operation(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Redirect(left), Self::Redirect(right)) => left.same_operation(right),
            (Self::ForceClose(left), Self::ForceClose(right)) => left.same_operation(right),
            _ => false,
        }
    }
}

struct TerminalGuard {
    router: Arc<Router>,
    command: Option<MigrationCommand>,
}

impl TerminalGuard {
    fn disarm(&mut self) {
        self.command = None;
    }

    fn finish_redirect(&mut self, success: bool) -> Settlement {
        let Some(MigrationCommand::Redirect(redirect)) = self.command.take() else {
            return Settlement::Ignored;
        };
        self.router
            .finish_redirect(&redirect, success, Instant::now())
    }

    fn observe_close(&mut self) -> Settlement {
        let Some(MigrationCommand::ForceClose(close)) = self.command.take() else {
            return Settlement::Ignored;
        };
        self.router.observe_close(&close)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let Some(command) = self.command.take() else {
            return;
        };
        match command {
            MigrationCommand::Redirect(redirect) => {
                let _ = self
                    .router
                    .finish_redirect(&redirect, false, Instant::now());
            }
            MigrationCommand::ForceClose(close) => {
                let _ = self.router.observe_close(&close);
            }
        }
    }
}

/// One production FIFO entry together with its exact terminal backstop.
///
/// Dropping an unsettled redirect records failure; dropping an unsettled close
/// observes the physical session disappearance.  Every path that can drop an
/// envelope first moves it outside registry/FIFO locks.
pub struct RouteCommandEnvelope {
    command: MigrationCommand,
    guard: TerminalGuard,
    public_connection_id: u64,
    redirect_deadline: Option<Instant>,
}

impl fmt::Debug for RouteCommandEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RouteCommandEnvelope")
            .field("command", &self.command)
            .field("public_connection_id", &self.public_connection_id)
            .field("redirect_deadline", &self.redirect_deadline)
            .finish_non_exhaustive()
    }
}

impl RouteCommandEnvelope {
    fn new(
        router: Arc<Router>,
        command: MigrationCommand,
        public_connection_id: u64,
        redirect_ttl: Duration,
    ) -> Self {
        let redirect_deadline =
            matches!(command, MigrationCommand::Redirect(_)).then(|| Instant::now() + redirect_ttl);
        Self {
            guard: TerminalGuard {
                router,
                command: Some(command.clone()),
            },
            command,
            public_connection_id,
            redirect_deadline,
        }
    }

    /// The exact command token and immutable target metadata.
    #[must_use]
    pub const fn command(&self) -> &MigrationCommand {
        &self.command
    }

    /// External connection id retained only for diagnostics.
    #[must_use]
    pub const fn public_connection_id(&self) -> u64 {
        self.public_connection_id
    }

    /// Remaining time a redirect may sit in the command queue before the
    /// session accepts it. Force-close has no expiry and returns `None`.
    ///
    /// Time after acceptance, including a transaction that has not reached
    /// a safe boundary, is not part of this budget. Go `tryRedirect` keeps
    /// the signal until the next finished command and only then dials.
    #[must_use]
    pub fn redirect_budget(&self) -> Option<Duration> {
        self.redirect_deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// Whether another delivered envelope names this same exact router token.
    #[must_use]
    pub fn same_operation(&self, other: &Self) -> bool {
        self.command.same_operation(&other.command)
    }

    /// Settles an exact redirect terminal once. Wrong-kind, duplicate and late
    /// terminals return [`Settlement::Ignored`].
    #[must_use]
    pub fn finish_redirect(mut self, success: bool) -> Settlement {
        self.guard.finish_redirect(success)
    }

    /// Observes an exact force-close terminal once.
    #[must_use]
    pub fn observe_close(mut self) -> Settlement {
        self.guard.observe_close()
    }

    /// Disarms a fault-injected duplicate envelope without settling the real
    /// operation a second time.
    pub fn ignore_duplicate(mut self) {
        self.guard.disarm();
    }

    fn disarm(&mut self) {
        self.guard.disarm();
    }
}

#[derive(Default)]
struct FifoState {
    closed: bool,
    entries: VecDeque<Box<RouteCommandEnvelope>>,
}

struct RouteCommandFifo {
    capacity: usize,
    state: Mutex<FifoState>,
    notify: Notify,
}

impl RouteCommandFifo {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(FifoState::default()),
            notify: Notify::new(),
        }
    }

    fn try_send(&self, envelope: RouteCommandEnvelope) -> Result<(), Box<RouteCommandEnvelope>> {
        let mut envelope = Some(Box::new(envelope));
        let accepted = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.closed || state.entries.len() >= self.capacity {
                false
            } else {
                state.entries.push_back(
                    envelope
                        .take()
                        .unwrap_or_else(|| unreachable!("owned envelope")),
                );
                true
            }
        };
        if accepted {
            self.notify.notify_one();
            Ok(())
        } else {
            Err(envelope.unwrap_or_else(|| unreachable!("rejected envelope")))
        }
    }

    fn take(&self) -> Option<RouteCommandEnvelope> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .pop_front()
            .map(|envelope| *envelope)
    }

    fn closed_and_empty(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.closed && state.entries.is_empty()
    }

    fn close_and_drain(&self) -> VecDeque<Box<RouteCommandEnvelope>> {
        let drained = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.closed = true;
            std::mem::take(&mut state.entries)
        };
        // There is exactly one receiver. `notify_one` retains a permit when
        // close races between its state check and polling `notified()`, while
        // `notify_waiters` would lose that wake when no waiter is registered.
        self.notify.notify_one();
        drained
    }
}

/// The receiving half of one exact router-session command FIFO.
pub struct RouteCommandReceiver {
    fifo: Arc<RouteCommandFifo>,
}

impl fmt::Debug for RouteCommandReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RouteCommandReceiver { .. }")
    }
}

impl RouteCommandReceiver {
    /// Waits for the next exact command. `None` means the route lease or this
    /// receiver closed; any previously queued guards were already drained
    /// outside the FIFO lock.
    pub async fn recv(&mut self) -> Option<RouteCommandEnvelope> {
        loop {
            let notified = self.fifo.notify.notified();
            if let Some(envelope) = self.fifo.take() {
                return Some(envelope);
            }
            if self.fifo.closed_and_empty() {
                return None;
            }
            notified.await;
        }
    }
}

impl Drop for RouteCommandReceiver {
    fn drop(&mut self) {
        // Move first; guard destructors run only after the FIFO lock is gone.
        let drained = self.fifo.close_and_drain();
        drop(drained);
    }
}

/// Unique registration owned by a session-long local route lease.
pub struct RouteCommandRegistration {
    dispatcher: Arc<RouteCommandDispatcher>,
    session_sequence: u64,
    fifo: Arc<RouteCommandFifo>,
}

impl Drop for RouteCommandRegistration {
    fn drop(&mut self) {
        self.dispatcher
            .unregister(self.session_sequence, &self.fifo);
        // Unregister before closing/draining.  Guards leave the FIFO lock before
        // they may call back into the router, and the lease closes afterwards.
        let drained = self.fifo.close_and_drain();
        drop(drained);
    }
}

/// Production effect boundary for one exact router incarnation.
///
/// The per-incarnation dispatcher plus the router-private session sequence form
/// the unforgeable registry key. Public connection ids are never looked up.
pub struct RouteCommandDispatcher {
    router: Arc<Router>,
    redirect_ttl: Duration,
    sessions: Mutex<BTreeMap<u64, (u64, Arc<RouteCommandFifo>)>>,
}

impl RouteCommandDispatcher {
    pub(crate) fn new(router: Arc<Router>) -> Arc<Self> {
        Self::with_redirect_ttl(router, PRODUCTION_REDIRECT_TTL)
    }

    pub(crate) fn with_redirect_ttl(router: Arc<Router>, redirect_ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            router,
            redirect_ttl,
            sessions: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        session: &Session,
        public_connection_id: u64,
        capacity: usize,
    ) -> Result<(RouteCommandRegistration, RouteCommandReceiver), crate::RouteError> {
        let fifo = Arc::new(RouteCommandFifo::new(capacity));
        {
            let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
            if sessions.contains_key(&session.sequence()) {
                return Err(crate::RouteError::AlreadyActive);
            }
            sessions.insert(
                session.sequence(),
                (public_connection_id, Arc::clone(&fifo)),
            );
        }
        Ok((
            RouteCommandRegistration {
                dispatcher: Arc::clone(self),
                session_sequence: session.sequence(),
                fifo: Arc::clone(&fifo),
            },
            RouteCommandReceiver { fifo },
        ))
    }

    fn unregister(&self, session_sequence: u64, fifo: &Arc<RouteCommandFifo>) {
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        if sessions
            .get(&session_sequence)
            .is_some_and(|(_, registered)| Arc::ptr_eq(registered, fifo))
        {
            sessions.remove(&session_sequence);
        }
    }
}

pub(crate) struct RejectedCommand {
    production: Option<Box<RouteCommandEnvelope>>,
    _unarmed: Option<Box<MigrationCommand>>,
}

impl RejectedCommand {
    fn production(envelope: Box<RouteCommandEnvelope>) -> Self {
        Self {
            production: Some(envelope),
            _unarmed: None,
        }
    }

    fn unarmed(command: MigrationCommand) -> Self {
        Self {
            production: None,
            _unarmed: Some(Box::new(command)),
        }
    }

    pub(crate) fn disarm(&mut self) {
        if let Some(envelope) = &mut self.production {
            envelope.disarm();
        }
    }
}

pub(crate) trait MigrationCommandSink: Send + Sync {
    fn try_send(&self, command: MigrationCommand) -> Result<(), RejectedCommand>;
}

impl MigrationCommandSink for RouteCommandDispatcher {
    fn try_send(&self, command: MigrationCommand) -> Result<(), RejectedCommand> {
        let session_sequence = match &command {
            MigrationCommand::Redirect(redirect) => redirect.session().sequence(),
            MigrationCommand::ForceClose(close) => close.session().sequence(),
        };
        // Clone only the FIFO under the registry lock.  Envelope construction,
        // FIFO admission and every possible guard destructor happen after it.
        let target = self
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&session_sequence)
            .cloned();
        let Some((public_connection_id, fifo)) = target else {
            // The session-long lease has already unregistered. Reject before
            // allocating an envelope/guard or touching any FIFO; the router
            // still commits its bounded refusal cooldown around this verdict.
            return Err(RejectedCommand::unarmed(command));
        };
        let envelope = RouteCommandEnvelope::new(
            Arc::clone(&self.router),
            command,
            public_connection_id,
            self.redirect_ttl,
        );
        fifo.try_send(envelope).map_err(RejectedCommand::production)
    }
}

#[cfg(test)]
type CommandSink = Box<dyn Fn(&MigrationCommand) -> bool + Send + Sync>;
pub(crate) struct CommandQueue {
    capacity: usize,
    #[cfg(test)]
    api_sink: Option<CommandSink>,
    entries: Mutex<VecDeque<MigrationCommand>>,
}
impl CommandQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            #[cfg(test)]
            api_sink: None,
            entries: Mutex::new(VecDeque::new()),
        }
    }
    // Test-client acceptance at the command boundary, never internal selection.
    #[cfg(test)]
    pub(crate) fn with_api_sink(capacity: usize, sink: CommandSink) -> Self {
        Self {
            capacity,
            entries: Mutex::new(VecDeque::new()),
            api_sink: Some(sink),
        }
    }
    fn try_send_inner(&self, command: MigrationCommand) -> Result<(), RejectedCommand> {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= self.capacity {
            return Err(RejectedCommand::unarmed(command));
        }
        #[cfg(test)]
        if self.api_sink.as_ref().is_some_and(|sink| !sink(&command)) {
            return Err(RejectedCommand::unarmed(command));
        }
        entries.push_back(command);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn observation(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|c| match c {
                MigrationCommand::Redirect(r) => format!("r{}", r.to().connection_id),
                MigrationCommand::ForceClose(c) => format!("c{}", c.assignment().connection_id),
            })
            .collect()
    }
    pub(crate) fn take(&self) -> Option<MigrationCommand> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }
    #[cfg(test)]
    pub(crate) fn take_redirect(&self) -> Option<Redirect> {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if !matches!(entries.front(), Some(MigrationCommand::Redirect(_))) {
            return None;
        }
        match entries.pop_front() {
            Some(MigrationCommand::Redirect(redirect)) => Some(redirect),
            _ => unreachable!("checked queue front under lock"),
        }
    }
}

impl MigrationCommandSink for CommandQueue {
    fn try_send(&self, command: MigrationCommand) -> Result<(), RejectedCommand> {
        self.try_send_inner(command)
    }
}
/// Bounded per-group observations; these counters grant no issuance authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MigrationProgress {
    /// Accepted redirects, not attempts or results.
    pub redirects: u64,
    /// Accepted close commands, not observed closed sessions.
    pub closes: u64,
    /// Pair and issuance-backstop keyspace refusals.
    pub keyspace_refusals: u64,
    /// Refusal evidence records limited to once per group per ten seconds.
    pub keyspace_records: u64,
}
/// The latest rate-limited keyspace refusal record for a retained group.
/// Its bounded diagnostic fields never authorize a redirect or expose payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyspaceRefusal {
    /// Current physical source backend identifier.
    pub from: String,
    /// Refused destination backend identifier.
    pub to: String,
    /// Source's current routing keyspace.
    pub from_keyspace: String,
    /// Destination's current routing keyspace.
    pub to_keyspace: String,
    /// Pair factor, absent for the manual issuance backstop.
    pub reason: Option<crate::Factor>,
    /// Source population represented by this refusal.
    pub physical_connections: u64,
    /// Accumulated refused pairs/backstop calls when the record was emitted.
    pub refusals: u64,
}

#[derive(Default)]
pub(crate) struct GroupSchedule {
    pub(crate) last_accepted: Option<Instant>,
    last_keyspace_record: Option<Instant>,
    pub(crate) last_refusal: Option<KeyspaceRefusal>,
    pub(crate) progress: MigrationProgress,
}
impl GroupSchedule {
    // Explicit extreme-rate difference: clamp zero to 1ns and bound each round
    // by source physical population; ordinary rates truncate like Go.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub(crate) fn budget(&self, rate: f64, now: Instant, physical: usize) -> usize {
        if rate.is_nan() || rate <= 0.0 {
            return 0;
        }
        let interval = Duration::from_nanos(((1_000_000_000.0 / rate) as u64).max(1));
        if interval < TICK * 2 {
            usize::try_from((TICK.as_nanos() - 1) / interval.as_nanos() + 1)
                .unwrap_or(usize::MAX)
                .min(physical)
        } else if self
            .last_accepted
            .is_none_or(|last| now.saturating_duration_since(last) >= interval)
        {
            physical.min(1)
        } else {
            0
        }
    }
    pub(crate) fn accepted(&mut self, now: Instant) {
        self.last_accepted = Some(now);
        self.progress.redirects = self.progress.redirects.saturating_add(1);
    }
    pub(crate) fn refuse_keyspace(&mut self, now: Instant, mut record: KeyspaceRefusal) {
        self.progress.keyspace_refusals = self.progress.keyspace_refusals.saturating_add(1);
        if self
            .last_keyspace_record
            .is_none_or(|last| now.saturating_duration_since(last) >= Duration::from_secs(10))
        {
            self.last_keyspace_record = Some(now);
            record.refusals = self.progress.keyspace_refusals;
            self.last_refusal = Some(record);
            self.progress.keyspace_records = self.progress.keyspace_records.saturating_add(1);
        }
    }
}

// Production reads a fresh monotonic clock for each group and again for close.
// Deterministic injection is compiled only into unit/evidence tests.
#[derive(Default)]
pub(crate) struct RoundClock {
    #[cfg(test)]
    pub(crate) fixed: Option<(Instant, Instant, i64)>,
}
// The receiver carries the test-only clock values; production reads fresh time.
#[cfg_attr(not(test), allow(clippy::unused_self))]
impl RoundClock {
    pub(crate) fn balance_now(&self) -> Instant {
        #[cfg(test)]
        if let Some((balance, _, _)) = self.fixed {
            return balance;
        }
        tokio::time::Instant::now().into_std()
    }
    pub(crate) fn close_now(&self) -> Instant {
        #[cfg(test)]
        if let Some((_, close, _)) = self.fixed {
            return close;
        }
        tokio::time::Instant::now().into_std()
    }
    pub(crate) fn wall(&self) -> Result<i64, crate::RouteError> {
        #[cfg(test)]
        if let Some((_, _, wall)) = self.fixed {
            return Ok(wall);
        }
        crate::selector::now_nanos()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn worker_extreme_rates_bound_scan_and_reject_invalid_rates() {
        let schedule = GroupSchedule::default();
        let now = Instant::now();
        for rate in [1e20, f64::INFINITY] {
            assert_eq!(schedule.budget(rate, now, 6), 6, "WORKER_EXTREME_BOUND");
        }
        for rate in [0.0, -1.0, f64::NAN] {
            assert_eq!(schedule.budget(rate, now, 6), 0);
        }
        assert_eq!(schedule.budget(1e20, now, 0), 0);
    }
}
