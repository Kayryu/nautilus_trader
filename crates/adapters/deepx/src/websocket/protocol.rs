// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Request ownership and shared lifecycle state for DeepX WebSocket sessions.

use std::collections::HashMap;

use nautilus_core::UUID4;
use nautilus_network::websocket::{AuthTracker, SubscriptionState, auth::AuthResultReceiver};
use serde_json::Value;
use tokio::sync::oneshot;

use super::{DeepXWsError, DeepXWsFrame};

/// Numeric correlator carried by one DeepX WebSocket request and response.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DeepXWsRequestId(u64);

impl DeepXWsRequestId {
    /// Creates an ID decoded from a WebSocket response frame.
    #[must_use]
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire-level numeric request ID.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Connection-owned request registration created before a send is exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXWsRequest {
    owner_id: UUID4,
    id: DeepXWsRequestId,
    send_token: u64,
    connection_epoch: u64,
}

/// Connection-owned authentication attempt registered before credentials are sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXWsAuthenticationAttempt {
    owner_id: UUID4,
    token: u64,
    connection_epoch: u64,
}

/// Proof that one authentication attempt completed for a specific transport connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXWsAuthenticatedSession {
    owner_id: UUID4,
    token: u64,
    connection_epoch: u64,
}

impl DeepXWsAuthenticatedSession {
    /// Returns the transport connection epoch which owns this authenticated session.
    #[must_use]
    pub const fn connection_epoch(self) -> u64 {
        self.connection_epoch
    }
}

impl DeepXWsAuthenticationAttempt {
    /// Returns the transport connection epoch which owns this attempt.
    #[must_use]
    pub const fn connection_epoch(self) -> u64 {
        self.connection_epoch
    }
}

impl DeepXWsRequest {
    /// Returns the request correlator.
    #[must_use]
    pub const fn id(self) -> DeepXWsRequestId {
        self.id
    }

    /// Returns the transport connection epoch which owns this request.
    #[must_use]
    pub const fn connection_epoch(self) -> u64 {
        self.connection_epoch
    }
}

#[derive(Debug)]
struct PendingRequest {
    send_token: u64,
    connection_epoch: u64,
    response_tx: oneshot::Sender<Result<Value, DeepXWsError>>,
}

/// Single-owner protocol state shared by a future DeepX WebSocket handler.
#[derive(Debug)]
pub struct DeepXWsProtocolCore {
    owner_id: UUID4,
    next_request_id: u64,
    next_send_token: u64,
    next_authentication_token: u64,
    connection_epoch: u64,
    pending_authentication: Option<DeepXWsAuthenticationAttempt>,
    authenticated_session: Option<DeepXWsAuthenticatedSession>,
    pending: HashMap<DeepXWsRequestId, PendingRequest>,
    auth_tracker: AuthTracker,
    subscriptions: SubscriptionState,
}

impl DeepXWsProtocolCore {
    /// Creates protocol state with an explicit topic delimiter.
    #[must_use]
    pub fn new(topic_delimiter: char) -> Self {
        Self {
            owner_id: UUID4::new(),
            next_request_id: 1,
            next_send_token: 1,
            next_authentication_token: 1,
            connection_epoch: 0,
            pending_authentication: None,
            authenticated_session: None,
            pending: HashMap::new(),
            auth_tracker: AuthTracker::new(),
            subscriptions: SubscriptionState::new(topic_delimiter),
        }
    }

    /// Registers a pending request before its send is exposed to another task.
    ///
    /// # Errors
    ///
    /// Returns [`DeepXWsError::RequestIdExhausted`] rather than wrapping request IDs.
    pub fn register_request(
        &mut self,
    ) -> Result<
        (
            DeepXWsRequest,
            oneshot::Receiver<Result<Value, DeepXWsError>>,
        ),
        DeepXWsError,
    > {
        let next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(DeepXWsError::RequestIdExhausted)?;
        let next_send_token = self
            .next_send_token
            .checked_add(1)
            .ok_or(DeepXWsError::SendTokenExhausted)?;
        let request_id = DeepXWsRequestId(self.next_request_id);
        let send_token = self.next_send_token;
        self.next_request_id = next_request_id;
        self.next_send_token = next_send_token;
        let (response_tx, response_rx) = oneshot::channel();
        self.pending.insert(
            request_id,
            PendingRequest {
                send_token,
                connection_epoch: self.connection_epoch,
                response_tx,
            },
        );

        Ok((
            DeepXWsRequest {
                owner_id: self.owner_id,
                id: request_id,
                send_token,
                connection_epoch: self.connection_epoch,
            },
            response_rx,
        ))
    }

    /// Resolves and removes a request by response ID.
    ///
    /// Returns `false` for an unknown or already completed response ID.
    pub fn complete_request(
        &mut self,
        id: DeepXWsRequestId,
        connection_epoch: u64,
        response: Value,
    ) -> bool {
        if self
            .pending
            .get(&id)
            .is_none_or(|pending| pending.connection_epoch != connection_epoch)
        {
            return false;
        }
        let Some(pending) = self.pending.remove(&id) else {
            return false;
        };
        let _ = pending.response_tx.send(Ok(response));
        true
    }

    /// Completes a registered request from an already decoded inbound frame.
    ///
    /// Unknown frames, unknown IDs, and responses from stale connection epochs are not consumed.
    pub fn complete_frame(&mut self, connection_epoch: u64, frame: &DeepXWsFrame) -> bool {
        let DeepXWsFrame::Correlated { id, value } = frame else {
            return false;
        };
        self.complete_request(*id, connection_epoch, value.clone())
    }

    /// Removes a request only when the send failure belongs to the same registration.
    pub fn fail_send(&mut self, request: DeepXWsRequest, reason: impl Into<String>) -> bool {
        self.cancel_request(request, reason)
    }

    /// Removes a request only when cancellation belongs to the same registration.
    pub fn cancel_request(&mut self, request: DeepXWsRequest, reason: impl Into<String>) -> bool {
        if request.owner_id != self.owner_id {
            return false;
        }
        if self
            .pending
            .get(&request.id)
            .is_none_or(|pending| pending.send_token != request.send_token)
        {
            return false;
        }
        let Some(pending) = self.pending.remove(&request.id) else {
            return false;
        };
        let _ = pending
            .response_tx
            .send(Err(DeepXWsError::RequestCanceled(reason.into())));
        true
    }

    /// Cancels all requests owned by the replaced or closed connection.
    pub fn drain_pending(&mut self, reason: impl Into<String>) {
        let error = DeepXWsError::RequestCanceled(reason.into());
        for (_, pending) in self.pending.drain() {
            let _ = pending.response_tx.send(Err(error.clone()));
        }
    }

    /// Cancels all request and authentication waiters owned by the current connection.
    pub fn drain_connection_owned(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        self.drain_pending(reason.clone());
        self.pending_authentication = None;
        self.authenticated_session = None;
        self.auth_tracker.cancel_pending(reason);
        self.auth_tracker.invalidate();
    }

    /// Returns the current number of pending request waiters.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Returns the current transport connection epoch.
    #[must_use]
    pub const fn connection_epoch(&self) -> u64 {
        self.connection_epoch
    }

    /// Begins an authentication attempt owned by the current connection epoch.
    ///
    /// The returned receiver resolves when the matching attempt succeeds or a later attempt
    /// supersedes it.
    pub fn begin_authentication(
        &mut self,
    ) -> Result<(DeepXWsAuthenticationAttempt, AuthResultReceiver), DeepXWsError> {
        let next_authentication_token = self
            .next_authentication_token
            .checked_add(1)
            .ok_or(DeepXWsError::AuthenticationAttemptTokenExhausted)?;
        let attempt = DeepXWsAuthenticationAttempt {
            owner_id: self.owner_id,
            token: self.next_authentication_token,
            connection_epoch: self.connection_epoch,
        };
        self.next_authentication_token = next_authentication_token;
        self.pending_authentication = Some(attempt);
        self.authenticated_session = None;
        Ok((attempt, self.auth_tracker.begin()))
    }

    /// Marks the active authentication attempt successful for its connection epoch.
    ///
    /// Returns `false` without changing authentication state when no matching attempt is active.
    pub fn complete_authentication(&mut self, attempt: DeepXWsAuthenticationAttempt) -> bool {
        if attempt.connection_epoch != self.connection_epoch
            || self.pending_authentication != Some(attempt)
        {
            return false;
        }
        self.pending_authentication = None;
        self.authenticated_session = Some(DeepXWsAuthenticatedSession {
            owner_id: self.owner_id,
            token: attempt.token,
            connection_epoch: attempt.connection_epoch,
        });
        self.auth_tracker.succeed();
        true
    }

    /// Marks the active authentication attempt failed for its connection epoch.
    ///
    /// Returns `false` without changing authentication state when no matching attempt is active.
    pub fn fail_authentication(
        &mut self,
        attempt: DeepXWsAuthenticationAttempt,
        reason: impl Into<String>,
    ) -> bool {
        if attempt.connection_epoch != self.connection_epoch
            || self.pending_authentication != Some(attempt)
        {
            return false;
        }
        self.pending_authentication = None;
        self.authenticated_session = None;
        self.auth_tracker.fail(reason);
        true
    }

    /// Cancels the active authentication attempt without making retry terminal.
    ///
    /// Returns `false` without changing authentication state when no matching attempt is active.
    pub fn cancel_authentication(
        &mut self,
        attempt: DeepXWsAuthenticationAttempt,
        reason: impl Into<String>,
    ) -> bool {
        if attempt.connection_epoch != self.connection_epoch
            || self.pending_authentication != Some(attempt)
        {
            return false;
        }
        self.pending_authentication = None;
        self.authenticated_session = None;
        self.auth_tracker.cancel_pending(reason);
        self.auth_tracker.invalidate();
        true
    }

    /// Returns proof of authentication for the current connection, when available.
    #[must_use]
    pub fn authenticated_session(&self) -> Option<DeepXWsAuthenticatedSession> {
        self.auth_tracker
            .is_authenticated()
            .then_some(self.authenticated_session)
            .flatten()
    }

    /// Returns whether the supplied authenticated session is still current.
    #[must_use]
    pub fn is_authenticated_session(&self, session: DeepXWsAuthenticatedSession) -> bool {
        self.authenticated_session() == Some(session)
    }

    /// Returns whether the current connection epoch has authenticated successfully.
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.authenticated_session()
            .is_some_and(|session| session.connection_epoch == self.connection_epoch)
    }

    /// Records desired subscription intent for the current connection.
    ///
    /// Returns `true` when a transport subscribe request should be sent.
    pub(crate) fn subscribe(&self, topic: &str) -> bool {
        self.subscriptions.try_mark_subscribe(topic)
    }

    /// Confirms a subscription only for the current connection epoch.
    pub(crate) fn confirm_subscription(&self, connection_epoch: u64, topic: &str) -> bool {
        if connection_epoch != self.connection_epoch
            || !self
                .subscriptions
                .pending_subscribe_topics()
                .iter()
                .any(|pending| pending == topic)
        {
            return false;
        }
        self.subscriptions.confirm_subscribe(topic);
        true
    }

    /// Returns a failed subscription to pending state only for the current connection epoch.
    pub(crate) fn fail_subscription(&self, connection_epoch: u64, topic: &str) -> bool {
        if connection_epoch != self.connection_epoch
            || !self
                .subscriptions
                .all_topics()
                .iter()
                .any(|active| active == topic)
        {
            return false;
        }
        self.subscriptions.mark_failure(topic);
        true
    }

    /// Records desired unsubscription intent for the current connection.
    ///
    /// Returns `true` when a transport unsubscribe request should be sent.
    pub(crate) fn unsubscribe(&self, topic: &str) -> bool {
        if !self
            .subscriptions
            .all_topics()
            .iter()
            .any(|active| active == topic)
        {
            return false;
        }
        self.subscriptions.mark_unsubscribe(topic);
        true
    }

    /// Confirms an unsubscription only for the current connection epoch.
    pub(crate) fn confirm_unsubscription(&self, connection_epoch: u64, topic: &str) -> bool {
        if connection_epoch != self.connection_epoch
            || !self
                .subscriptions
                .pending_unsubscribe_topics()
                .iter()
                .any(|pending| pending == topic)
        {
            return false;
        }
        self.subscriptions.confirm_unsubscribe(topic);
        true
    }

    /// Resets connection-owned state while preserving desired subscription intent.
    pub fn reset_after_reconnect(
        &mut self,
        connection_epoch: u64,
        reason: impl Into<String>,
    ) -> Result<Vec<String>, DeepXWsError> {
        if connection_epoch <= self.connection_epoch {
            return Err(DeepXWsError::NonIncreasingConnectionEpoch {
                current: self.connection_epoch,
                received: connection_epoch,
            });
        }
        self.drain_connection_owned(reason);
        self.connection_epoch = connection_epoch;
        Ok(self.subscriptions.reset_after_reconnect())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn registers_before_response_and_completes_once() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (request, response_rx) = core.register_request().unwrap();

        assert_eq!(core.pending_len(), 1);
        assert!(core.complete_request(
            request.id(),
            request.connection_epoch(),
            json!({ "ok": true }),
        ));
        assert!(!core.complete_request(
            request.id(),
            request.connection_epoch(),
            json!({ "ok": false }),
        ));
        assert_eq!(response_rx.await.unwrap().unwrap(), json!({ "ok": true }));
        assert_eq!(core.pending_len(), 0);
    }

    #[tokio::test]
    async fn send_failure_resolves_only_matching_registration() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (request, response_rx) = core.register_request().unwrap();
        let stale = DeepXWsRequest {
            owner_id: request.owner_id,
            id: request.id,
            send_token: request.send_token.wrapping_add(1),
            connection_epoch: request.connection_epoch,
        };

        assert!(!core.fail_send(stale, "stale failure"));
        assert_eq!(core.pending_len(), 1);
        assert!(core.complete_request(
            request.id(),
            request.connection_epoch(),
            json!({ "ok": true }),
        ));
        assert_eq!(response_rx.await.unwrap().unwrap(), json!({ "ok": true }));
    }

    #[tokio::test]
    async fn cancellation_resolves_only_matching_registration() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (request, response_rx) = core.register_request().unwrap();

        assert!(core.cancel_request(request, "caller stopped waiting"));
        assert!(!core.cancel_request(request, "duplicate cancellation"));
        assert!(matches!(
            response_rx.await.unwrap(),
            Err(DeepXWsError::RequestCanceled(reason)) if reason == "caller stopped waiting"
        ));
        assert_eq!(core.pending_len(), 0);
    }

    #[tokio::test]
    async fn ignores_response_from_stale_connection_epoch() {
        let mut core = DeepXWsProtocolCore::new(':');
        core.reset_after_reconnect(4, "initial connection").unwrap();
        let (request, response_rx) = core.register_request().unwrap();

        assert!(!core.complete_request(request.id(), 3, json!({ "stale": true })));
        assert_eq!(core.pending_len(), 1);
        assert!(core.complete_request(request.id(), 4, json!({ "ok": true })));
        assert_eq!(response_rx.await.unwrap().unwrap(), json!({ "ok": true }));
    }

    #[tokio::test]
    async fn completes_request_from_single_decoded_frame() {
        let mut core = DeepXWsProtocolCore::new(':');
        core.reset_after_reconnect(9, "connected").unwrap();
        let (request, response_rx) = core.register_request().unwrap();
        let frame = DeepXWsFrame::parse(&format!(
            r#"{{"id":{},"result":{{"ok":true}}}}"#,
            request.id().as_u64(),
        ))
        .unwrap();

        assert!(core.complete_frame(9, &frame));
        assert_eq!(
            response_rx.await.unwrap().unwrap(),
            json!({"id": 1, "result": {"ok": true}}),
        );
    }

    #[rstest]
    fn unknown_frame_does_not_change_request_registry() {
        let mut core = DeepXWsProtocolCore::new(':');
        let _ = core.register_request().unwrap();
        let frame = DeepXWsFrame::parse(r#"{"channel":"unproven"}"#).unwrap();

        assert!(!core.complete_frame(0, &frame));
        assert_eq!(core.pending_len(), 1);
    }

    #[tokio::test]
    async fn drains_all_connection_owned_requests() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (_, first_rx) = core.register_request().unwrap();
        let (_, second_rx) = core.register_request().unwrap();

        core.drain_pending("connection replaced");

        assert_eq!(core.pending_len(), 0);
        for response_rx in [first_rx, second_rx] {
            assert!(matches!(
                response_rx.await.unwrap(),
                Err(DeepXWsError::RequestCanceled(reason)) if reason == "connection replaced"
            ));
        }
    }

    #[rstest]
    fn request_ids_are_monotonic() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (first, _) = core.register_request().unwrap();
        let (second, _) = core.register_request().unwrap();

        assert_eq!(first.id().as_u64(), 1);
        assert_eq!(second.id().as_u64(), 2);
    }

    #[rstest]
    fn allocation_exhaustion_does_not_partially_register_request() {
        let mut core = DeepXWsProtocolCore::new(':');
        core.next_send_token = u64::MAX;

        assert!(matches!(
            core.register_request(),
            Err(DeepXWsError::SendTokenExhausted),
        ));
        assert_eq!(core.next_request_id, 1);
        assert_eq!(core.pending_len(), 0);
    }

    #[rstest]
    fn reconnect_invalidates_auth_and_preserves_subscription_intent() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (attempt, _) = core.begin_authentication().unwrap();
        assert!(core.complete_authentication(attempt));
        let session = core.authenticated_session().unwrap();
        assert!(core.is_authenticated());
        assert!(core.is_authenticated_session(session));
        assert!(core.subscribe("trades:ETH-USDC"));
        assert!(core.confirm_subscription(0, "trades:ETH-USDC"));

        let replay = core
            .reset_after_reconnect(1, "connection replaced")
            .unwrap();

        assert!(!core.is_authenticated());
        assert!(!core.is_authenticated_session(session));
        assert_eq!(replay, vec!["trades:ETH-USDC".to_string()]);
        assert_eq!(core.subscriptions.delimiter(), ':');
        assert_eq!(
            core.subscriptions.pending_subscribe_topics(),
            vec!["trades:ETH-USDC".to_string()],
        );
    }

    #[rstest]
    fn stale_connection_cannot_complete_current_authentication() {
        let mut core = DeepXWsProtocolCore::new(':');
        core.reset_after_reconnect(4, "initial connection").unwrap();
        let (attempt, _) = core.begin_authentication().unwrap();
        core.reset_after_reconnect(5, "connection replaced")
            .unwrap();

        assert!(!core.complete_authentication(attempt));
        assert!(!core.is_authenticated());
        assert_eq!(core.connection_epoch(), 5);
    }

    #[tokio::test]
    async fn stale_reconnect_epoch_preserves_current_connection_state() {
        let mut core = DeepXWsProtocolCore::new(':');
        core.reset_after_reconnect(4, "initial connection").unwrap();
        let (request, response_rx) = core.register_request().unwrap();
        let (attempt, authentication_rx) = core.begin_authentication().unwrap();
        assert!(core.subscribe("trades:ETH-USDC"));

        assert_eq!(
            core.reset_after_reconnect(4, "stale reconnect"),
            Err(DeepXWsError::NonIncreasingConnectionEpoch {
                current: 4,
                received: 4,
            }),
        );
        assert_eq!(core.connection_epoch(), 4);
        assert_eq!(core.pending_len(), 1);
        assert_eq!(
            core.subscriptions.pending_subscribe_topics(),
            vec!["trades:ETH-USDC".to_string()],
        );
        assert!(core.complete_request(request.id(), 4, json!({ "ok": true })));
        assert_eq!(response_rx.await.unwrap().unwrap(), json!({ "ok": true }));
        assert!(core.complete_authentication(attempt));
        assert_eq!(authentication_rx.await.unwrap(), Ok(()));
    }

    #[rstest]
    fn stale_connection_cannot_acknowledge_replayed_subscription() {
        let mut core = DeepXWsProtocolCore::new(':');
        assert!(core.subscribe("trades:ETH-USDC"));
        core.reset_after_reconnect(1, "connection replaced")
            .unwrap();

        assert!(!core.confirm_subscription(0, "trades:ETH-USDC"));
        assert_eq!(
            core.subscriptions.pending_subscribe_topics(),
            vec!["trades:ETH-USDC".to_string()],
        );
        assert!(core.confirm_subscription(1, "trades:ETH-USDC"));
        assert!(core.subscriptions.pending_subscribe_topics().is_empty());
        assert_eq!(core.subscriptions.len(), 1);
    }

    #[rstest]
    fn unknown_and_duplicate_subscription_results_are_rejected() {
        let core = DeepXWsProtocolCore::new(':');

        assert!(!core.confirm_subscription(0, "trades:ETH-USDC"));
        assert!(!core.fail_subscription(0, "trades:ETH-USDC"));
        assert!(!core.unsubscribe("trades:ETH-USDC"));
        assert!(!core.confirm_unsubscription(0, "trades:ETH-USDC"));

        assert!(core.subscribe("trades:ETH-USDC"));
        assert!(core.confirm_subscription(0, "trades:ETH-USDC"));
        assert!(!core.confirm_subscription(0, "trades:ETH-USDC"));
        assert!(core.unsubscribe("trades:ETH-USDC"));
        assert!(!core.unsubscribe("trades:ETH-USDC"));
        assert!(core.confirm_unsubscription(0, "trades:ETH-USDC"));
        assert!(!core.confirm_unsubscription(0, "trades:ETH-USDC"));
    }

    #[rstest]
    fn authentication_success_requires_an_active_attempt() {
        let mut core = DeepXWsProtocolCore::new(':');
        let attempt = DeepXWsAuthenticationAttempt {
            owner_id: core.owner_id,
            token: 1,
            connection_epoch: 0,
        };

        assert!(!core.complete_authentication(attempt));
        assert!(!core.is_authenticated());
    }

    #[rstest]
    fn request_capability_cannot_cancel_another_protocol_owner() {
        let mut first = DeepXWsProtocolCore::new(':');
        let mut second = DeepXWsProtocolCore::new(':');
        let (first_request, _) = first.register_request().unwrap();
        let (_, second_rx) = second.register_request().unwrap();

        assert!(!second.cancel_request(first_request, "foreign timeout"));
        assert!(second.complete_request(
            DeepXWsRequestId::new(1),
            second.connection_epoch(),
            serde_json::json!({"ok": true}),
        ));
        assert_eq!(second_rx.blocking_recv().unwrap().unwrap()["ok"], true);
    }

    #[rstest]
    fn authentication_capabilities_are_bound_to_protocol_owner() {
        let mut first = DeepXWsProtocolCore::new(':');
        let mut second = DeepXWsProtocolCore::new(':');
        let (first_attempt, _) = first.begin_authentication().unwrap();
        let (second_attempt, _) = second.begin_authentication().unwrap();

        assert!(!second.complete_authentication(first_attempt));
        assert!(second.complete_authentication(second_attempt));
        let second_session = second.authenticated_session().unwrap();
        assert!(!first.is_authenticated_session(second_session));

        assert!(first.complete_authentication(first_attempt));
        let first_session = first.authenticated_session().unwrap();
        assert!(!second.is_authenticated_session(first_session));
        assert!(second.is_authenticated_session(second_session));
    }

    #[tokio::test]
    async fn superseded_attempt_cannot_complete_new_authentication() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (first, first_rx) = core.begin_authentication().unwrap();
        let (second, _) = core.begin_authentication().unwrap();

        assert!(first_rx.await.unwrap().is_err());
        assert!(!core.complete_authentication(first));
        assert!(!core.is_authenticated());
        assert!(core.complete_authentication(second));
        assert!(core.is_authenticated());
        assert_eq!(
            core.authenticated_session().unwrap().connection_epoch(),
            second.connection_epoch(),
        );
    }

    #[tokio::test]
    async fn authentication_failure_only_applies_to_the_active_attempt() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (stale, stale_rx) = core.begin_authentication().unwrap();
        let (active, active_rx) = core.begin_authentication().unwrap();

        assert!(stale_rx.await.unwrap().is_err());
        assert!(!core.fail_authentication(stale, "stale rejection"));
        assert!(core.fail_authentication(active, "invalid credentials"));
        assert_eq!(
            active_rx.await.unwrap(),
            Err("invalid credentials".to_string()),
        );
        assert!(!core.is_authenticated());
        assert!(core.authenticated_session().is_none());

        let (retry, retry_rx) = core.begin_authentication().unwrap();
        assert!(core.complete_authentication(retry));
        assert_eq!(retry_rx.await.unwrap(), Ok(()));
        let session = core.authenticated_session().unwrap();
        assert!(!core.fail_authentication(active, "late rejection"));
        assert!(core.is_authenticated());
        assert!(core.is_authenticated_session(session));
    }

    #[tokio::test]
    async fn authentication_cancellation_only_applies_to_the_active_attempt() {
        let mut core = DeepXWsProtocolCore::new(':');
        let (stale, stale_rx) = core.begin_authentication().unwrap();
        let (active, active_rx) = core.begin_authentication().unwrap();

        assert!(stale_rx.await.unwrap().is_err());
        assert!(!core.cancel_authentication(stale, "stale timeout"));
        assert!(core.cancel_authentication(active, "authentication timed out"));
        assert_eq!(
            active_rx.await.unwrap(),
            Err("authentication timed out".to_string()),
        );
        assert!(!core.complete_authentication(active));
        assert!(!core.is_authenticated());

        let (retry, retry_rx) = core.begin_authentication().unwrap();
        assert!(core.complete_authentication(retry));
        assert_eq!(retry_rx.await.unwrap(), Ok(()));
        assert!(core.is_authenticated());
    }
}
