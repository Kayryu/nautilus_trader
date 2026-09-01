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

//! Single-owner command loop for transport-neutral DeepX WebSocket protocol state.

use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{DeepXWsError, DeepXWsFrame, DeepXWsProtocolCore, DeepXWsRequest};

const DEEPX_WS_COMMAND_CAPACITY: usize = 1024;

/// Request registration and its correlated response waiter.
pub type DeepXWsRegisteredRequest = (
    DeepXWsRequest,
    oneshot::Receiver<Result<Value, DeepXWsError>>,
);

#[derive(Debug)]
enum DeepXWsHandlerCommand {
    Register {
        response_tx: oneshot::Sender<Result<DeepXWsRegisteredRequest, DeepXWsError>>,
    },
    FailSend {
        request: DeepXWsRequest,
        reason: String,
        response_tx: oneshot::Sender<bool>,
    },
    CancelRequest {
        request: DeepXWsRequest,
        reason: String,
        response_tx: oneshot::Sender<bool>,
    },
    IngestText {
        connection_epoch: u64,
        text: String,
        response_tx: oneshot::Sender<Result<bool, DeepXWsError>>,
    },
    ResetAfterReconnect {
        connection_epoch: u64,
        reason: String,
        response_tx: oneshot::Sender<Vec<String>>,
    },
}

/// Command handle for one single-owner DeepX WebSocket protocol task.
#[derive(Clone, Debug)]
pub struct DeepXWsProtocolHandle {
    command_tx: mpsc::Sender<DeepXWsHandlerCommand>,
}

impl DeepXWsProtocolHandle {
    /// Registers a request before a future transport send is exposed.
    ///
    /// # Errors
    ///
    /// Returns a typed error when ID allocation fails or the handler is no longer running.
    pub async fn register_request(&self) -> Result<DeepXWsRegisteredRequest, DeepXWsError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DeepXWsHandlerCommand::Register { response_tx })
            .await
            .map_err(|_| handler_stopped())?;
        response_rx.await.map_err(|_| handler_stopped())?
    }

    /// Cancels a registration when its matching transport send fails.
    ///
    /// Returns `false` when the registration is stale or already completed.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the handler is no longer running.
    pub async fn fail_send(
        &self,
        request: DeepXWsRequest,
        reason: impl Into<String>,
    ) -> Result<bool, DeepXWsError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DeepXWsHandlerCommand::FailSend {
                request,
                reason: reason.into(),
                response_tx,
            })
            .await
            .map_err(|_| handler_stopped())?;
        response_rx.await.map_err(|_| handler_stopped())
    }

    /// Cancels a matching request registration which the caller no longer needs.
    ///
    /// Returns `false` when the registration is stale or already completed.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the handler is no longer running.
    pub async fn cancel_request(
        &self,
        request: DeepXWsRequest,
        reason: impl Into<String>,
    ) -> Result<bool, DeepXWsError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DeepXWsHandlerCommand::CancelRequest {
                request,
                reason: reason.into(),
                response_tx,
            })
            .await
            .map_err(|_| handler_stopped())?;
        response_rx.await.map_err(|_| handler_stopped())
    }

    /// Waits for a correlated response and removes the registration on timeout.
    ///
    /// # Errors
    ///
    /// Returns the correlated protocol error, a typed timeout, or a handler-stopped error.
    pub async fn wait_for_response(
        &self,
        request: DeepXWsRequest,
        mut response_rx: oneshot::Receiver<Result<Value, DeepXWsError>>,
        timeout: Duration,
    ) -> Result<Value, DeepXWsError> {
        match tokio::time::timeout(timeout, &mut response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(handler_stopped()),
            Err(_) => {
                if self
                    .cancel_request(request, "request response timed out")
                    .await?
                {
                    return Err(DeepXWsError::RequestTimeout {
                        request_id: request.id().as_u64(),
                    });
                }

                response_rx.await.map_err(|_| handler_stopped())?
            }
        }
    }

    /// Parses one text frame once and offers correlated responses to the protocol registry.
    ///
    /// Returns `false` for valid unknown frames and unknown or stale response IDs.
    ///
    /// # Errors
    ///
    /// Returns a typed error for malformed JSON or when the handler is no longer running.
    pub async fn ingest_text(
        &self,
        connection_epoch: u64,
        text: impl Into<String>,
    ) -> Result<bool, DeepXWsError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DeepXWsHandlerCommand::IngestText {
                connection_epoch,
                text: text.into(),
                response_tx,
            })
            .await
            .map_err(|_| handler_stopped())?;
        response_rx.await.map_err(|_| handler_stopped())?
    }

    /// Replaces connection-owned state and returns desired topics requiring replay.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the handler is no longer running.
    pub async fn reset_after_reconnect(
        &self,
        connection_epoch: u64,
        reason: impl Into<String>,
    ) -> Result<Vec<String>, DeepXWsError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DeepXWsHandlerCommand::ResetAfterReconnect {
                connection_epoch,
                reason: reason.into(),
                response_tx,
            })
            .await
            .map_err(|_| handler_stopped())?;
        response_rx.await.map_err(|_| handler_stopped())
    }
}

/// Single-owner DeepX WebSocket protocol command loop.
#[derive(Debug)]
pub struct DeepXWsProtocolHandler {
    core: DeepXWsProtocolCore,
    command_rx: mpsc::Receiver<DeepXWsHandlerCommand>,
}

impl DeepXWsProtocolHandler {
    /// Runs until owner cancellation or closure of every command handle.
    pub async fn run(mut self, cancellation: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    self.core.drain_pending("protocol handler canceled");
                    return;
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        self.core.drain_pending("protocol command channel closed");
                        return;
                    };
                    handle_command(&mut self.core, command);
                }
            }
        }
    }
}

/// Creates a command handle and its single-owner protocol handler.
#[must_use]
pub fn deepx_ws_protocol_handler(
    topic_delimiter: char,
) -> (DeepXWsProtocolHandle, DeepXWsProtocolHandler) {
    deepx_ws_protocol_handler_with_capacity(topic_delimiter, DEEPX_WS_COMMAND_CAPACITY)
}

fn deepx_ws_protocol_handler_with_capacity(
    topic_delimiter: char,
    command_capacity: usize,
) -> (DeepXWsProtocolHandle, DeepXWsProtocolHandler) {
    let (command_tx, command_rx) = mpsc::channel(command_capacity);
    let handle = DeepXWsProtocolHandle { command_tx };
    let handler = DeepXWsProtocolHandler {
        core: DeepXWsProtocolCore::new(topic_delimiter),
        command_rx,
    };
    (handle, handler)
}

fn handle_command(core: &mut DeepXWsProtocolCore, command: DeepXWsHandlerCommand) {
    match command {
        DeepXWsHandlerCommand::Register { response_tx } => {
            let _ = response_tx.send(core.register_request());
        }
        DeepXWsHandlerCommand::FailSend {
            request,
            reason,
            response_tx,
        } => {
            let _ = response_tx.send(core.fail_send(request, reason));
        }
        DeepXWsHandlerCommand::CancelRequest {
            request,
            reason,
            response_tx,
        } => {
            let _ = response_tx.send(core.cancel_request(request, reason));
        }
        DeepXWsHandlerCommand::IngestText {
            connection_epoch,
            text,
            response_tx,
        } => {
            let result = DeepXWsFrame::parse(&text)
                .map(|frame| core.complete_frame(connection_epoch, &frame));
            let _ = response_tx.send(result);
        }
        DeepXWsHandlerCommand::ResetAfterReconnect {
            connection_epoch,
            reason,
            response_tx,
        } => {
            let replay = core.reset_after_reconnect(connection_epoch, reason);
            let _ = response_tx.send(replay);
        }
    }
}

fn handler_stopped() -> DeepXWsError {
    DeepXWsError::RequestCanceled("protocol handler stopped".to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::websocket::{DeepXWsTaskHandles, DeepXWsTaskOutcome};

    #[tokio::test]
    async fn correlates_frame_through_single_owner_loop() {
        let mut tasks = DeepXWsTaskHandles::new();
        let (handle, handler) = deepx_ws_protocol_handler(':');
        tasks
            .spawn(|cancellation| handler.run(cancellation))
            .unwrap();
        let (request, response_rx) = handle.register_request().await.unwrap();

        let consumed = handle
            .ingest_text(
                request.connection_epoch(),
                format!(
                    r#"{{"id":{},"result":{{"ok":true}}}}"#,
                    request.id().as_u64()
                ),
            )
            .await
            .unwrap();

        assert!(consumed);
        assert_eq!(
            response_rx.await.unwrap().unwrap(),
            json!({"id": 1, "result": {"ok": true}}),
        );
        drop(handle);
        assert_eq!(
            tasks.shutdown(Duration::from_secs(1)).await.unwrap(),
            DeepXWsTaskOutcome::Completed,
        );
    }

    #[tokio::test]
    async fn malformed_and_unknown_frames_are_non_destructive() {
        let (handle, handler) = deepx_ws_protocol_handler(':');
        let handler_task = tokio::spawn(handler.run(CancellationToken::new()));
        let (request, response_rx) = handle.register_request().await.unwrap();

        assert!(matches!(
            handle.ingest_text(0, "{").await,
            Err(DeepXWsError::InvalidJsonFrame(_)),
        ));
        assert!(
            !handle
                .ingest_text(0, r#"{"channel":"unproven"}"#)
                .await
                .unwrap()
        );
        assert!(
            handle
                .ingest_text(
                    0,
                    format!(r#"{{"id":{},"ok":true}}"#, request.id().as_u64()),
                )
                .await
                .unwrap()
        );
        assert_eq!(response_rx.await.unwrap().unwrap()["ok"], true);

        drop(handle);
        handler_task.await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_cancels_old_waiter_and_rejects_stale_epoch() {
        let (handle, handler) = deepx_ws_protocol_handler(':');
        let handler_task = tokio::spawn(handler.run(CancellationToken::new()));
        let (_, old_response_rx) = handle.register_request().await.unwrap();

        assert!(
            handle
                .reset_after_reconnect(4, "connection replaced")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            old_response_rx.await.unwrap(),
            Err(DeepXWsError::RequestCanceled(reason)) if reason == "connection replaced"
        ));

        let (request, response_rx) = handle.register_request().await.unwrap();
        let frame = format!(r#"{{"id":{},"ok":true}}"#, request.id().as_u64());
        assert!(!handle.ingest_text(3, &frame).await.unwrap());
        assert!(handle.ingest_text(4, frame).await.unwrap());
        assert_eq!(response_rx.await.unwrap().unwrap()["ok"], true);

        drop(handle);
        handler_task.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_drains_pending_and_stops_handle() {
        let cancellation = CancellationToken::new();
        let (handle, handler) = deepx_ws_protocol_handler(':');
        let handler_task = tokio::spawn(handler.run(cancellation.clone()));
        let (_, response_rx) = handle.register_request().await.unwrap();

        cancellation.cancel();
        handler_task.await.unwrap();

        assert!(matches!(
            response_rx.await.unwrap(),
            Err(DeepXWsError::RequestCanceled(reason)) if reason == "protocol handler canceled"
        ));
        assert!(matches!(
            handle.register_request().await,
            Err(DeepXWsError::RequestCanceled(reason)) if reason == "protocol handler stopped"
        ));
    }

    #[tokio::test]
    async fn matching_send_failure_cancels_registered_waiter() {
        let (handle, handler) = deepx_ws_protocol_handler(':');
        let handler_task = tokio::spawn(handler.run(CancellationToken::new()));
        let (request, response_rx) = handle.register_request().await.unwrap();

        assert!(
            handle
                .fail_send(request, "transport send failed")
                .await
                .unwrap()
        );
        assert!(matches!(
            response_rx.await.unwrap(),
            Err(DeepXWsError::RequestCanceled(reason)) if reason == "transport send failed"
        ));
        assert!(!handle.fail_send(request, "stale failure").await.unwrap());

        drop(handle);
        handler_task.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_wait_returns_correlated_response() {
        let (handle, handler) = deepx_ws_protocol_handler(':');
        let handler_task = tokio::spawn(handler.run(CancellationToken::new()));
        let (request, response_rx) = handle.register_request().await.unwrap();
        let wait_handle = handle.clone();
        let waiter = tokio::spawn(async move {
            wait_handle
                .wait_for_response(request, response_rx, Duration::from_secs(30))
                .await
        });

        assert!(
            handle
                .ingest_text(
                    request.connection_epoch(),
                    format!(r#"{{"id":{},"ok":true}}"#, request.id().as_u64()),
                )
                .await
                .unwrap()
        );
        assert_eq!(waiter.await.unwrap().unwrap()["ok"], true);

        drop(handle);
        handler_task.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_wait_cancels_registration_on_timeout() {
        let (handle, handler) = deepx_ws_protocol_handler(':');
        let handler_task = tokio::spawn(handler.run(CancellationToken::new()));
        let (request, response_rx) = handle.register_request().await.unwrap();
        let wait_handle = handle.clone();
        let waiter = tokio::spawn(async move {
            wait_handle
                .wait_for_response(request, response_rx, Duration::from_millis(1))
                .await
        });

        assert!(matches!(
            waiter.await.unwrap(),
            Err(DeepXWsError::RequestTimeout { request_id }) if request_id == request.id().as_u64()
        ));
        assert!(
            !handle
                .ingest_text(
                    request.connection_epoch(),
                    format!(r#"{{"id":{},"late":true}}"#, request.id().as_u64()),
                )
                .await
                .unwrap()
        );

        drop(handle);
        handler_task.await.unwrap();
    }

    #[tokio::test]
    async fn completed_response_wins_at_timeout_boundary() {
        let (handle, handler) = deepx_ws_protocol_handler(':');
        let handler_task = tokio::spawn(handler.run(CancellationToken::new()));
        let (request, response_rx) = handle.register_request().await.unwrap();

        assert!(
            handle
                .ingest_text(
                    request.connection_epoch(),
                    format!(r#"{{"id":{},"ok":true}}"#, request.id().as_u64()),
                )
                .await
                .unwrap()
        );
        assert_eq!(
            handle
                .wait_for_response(request, response_rx, Duration::ZERO)
                .await
                .unwrap()["ok"],
            true,
        );

        drop(handle);
        handler_task.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_command_queue_applies_backpressure_until_handler_drains() {
        let (handle, handler) = deepx_ws_protocol_handler_with_capacity(':', 1);
        let (queued_response_tx, _queued_response_rx) = oneshot::channel();
        handle
            .command_tx
            .try_send(DeepXWsHandlerCommand::Register {
                response_tx: queued_response_tx,
            })
            .unwrap();
        assert_eq!(handle.command_tx.capacity(), 0);

        let blocked_handle = handle.clone();
        let blocked_registration =
            tokio::spawn(async move { blocked_handle.register_request().await });
        tokio::task::yield_now().await;
        assert!(!blocked_registration.is_finished());

        let handler_task = tokio::spawn(handler.run(CancellationToken::new()));
        let (_, response_rx) = blocked_registration.await.unwrap().unwrap();

        drop(handle);
        handler_task.await.unwrap();
        assert!(matches!(
            response_rx.await.unwrap(),
            Err(DeepXWsError::RequestCanceled(reason))
                if reason == "protocol command channel closed"
        ));
    }
}
