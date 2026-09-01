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

use nautilus_network::websocket::{AuthTracker, SubscriptionState};
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
    id: DeepXWsRequestId,
    send_token: u64,
    connection_epoch: u64,
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
    next_request_id: u64,
    next_send_token: u64,
    connection_epoch: u64,
    pending: HashMap<DeepXWsRequestId, PendingRequest>,
    auth_tracker: AuthTracker,
    subscriptions: SubscriptionState,
}

impl DeepXWsProtocolCore {
    /// Creates protocol state with an explicit topic delimiter.
    #[must_use]
    pub fn new(topic_delimiter: char) -> Self {
        Self {
            next_request_id: 1,
            next_send_token: 1,
            connection_epoch: 0,
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

    /// Returns the current number of pending request waiters.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Returns the shared authentication tracker.
    #[must_use]
    pub const fn auth_tracker(&self) -> &AuthTracker {
        &self.auth_tracker
    }

    /// Returns the shared desired-versus-confirmed subscription state.
    #[must_use]
    pub const fn subscriptions(&self) -> &SubscriptionState {
        &self.subscriptions
    }

    /// Resets connection-owned state while preserving desired subscription intent.
    pub fn reset_after_reconnect(
        &mut self,
        connection_epoch: u64,
        reason: impl Into<String>,
    ) -> Vec<String> {
        self.drain_pending(reason);
        self.connection_epoch = connection_epoch;
        self.auth_tracker.invalidate();
        self.subscriptions.reset_after_reconnect()
    }
}

#[cfg(test)]
mod tests {
    use nautilus_network::websocket::auth::AuthState;
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
        core.reset_after_reconnect(4, "initial connection");
        let (request, response_rx) = core.register_request().unwrap();

        assert!(!core.complete_request(request.id(), 3, json!({ "stale": true })));
        assert_eq!(core.pending_len(), 1);
        assert!(core.complete_request(request.id(), 4, json!({ "ok": true })));
        assert_eq!(response_rx.await.unwrap().unwrap(), json!({ "ok": true }));
    }

    #[tokio::test]
    async fn completes_request_from_single_decoded_frame() {
        let mut core = DeepXWsProtocolCore::new(':');
        core.reset_after_reconnect(9, "connected");
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
        core.auth_tracker().succeed();
        assert!(core.subscriptions().try_mark_subscribe("trades:ETH-USDC"));
        core.subscriptions().confirm_subscribe("trades:ETH-USDC");

        let replay = core.reset_after_reconnect(1, "connection replaced");

        assert_eq!(core.auth_tracker().auth_state(), AuthState::Unauthenticated);
        assert_eq!(replay, vec!["trades:ETH-USDC".to_string()]);
        assert_eq!(core.subscriptions().delimiter(), ':');
        assert_eq!(
            core.subscriptions().pending_subscribe_topics(),
            vec!["trades:ETH-USDC".to_string()],
        );
    }
}
