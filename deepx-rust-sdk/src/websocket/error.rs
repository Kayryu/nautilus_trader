//! Errors for DeepX WebSocket frames and protocol state.

use thiserror::Error;

/// Errors emitted before DeepX WebSocket business schemas are enabled.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DeepXWsError {
    /// No transport frame arrived within a bounded read.
    #[error("DeepX WebSocket receive timed out")]
    ReceiveTimeout,
    /// An inbound text frame was not valid JSON.
    #[error("invalid DeepX WebSocket JSON frame: {0}")]
    InvalidJsonFrame(String),
    /// A task generation was started while the previous generation was still owned.
    #[error("DeepX WebSocket task is already running")]
    TaskAlreadyRunning,
    /// A WebSocket task terminated without completing or being owner-aborted.
    #[error("DeepX WebSocket task join failed: {0}")]
    TaskJoin(String),
    /// No more numeric request identifiers can be allocated safely.
    #[error("DeepX WebSocket request ID space exhausted")]
    RequestIdExhausted,
    /// No more send ownership tokens can be allocated safely.
    #[error("DeepX WebSocket send token space exhausted")]
    SendTokenExhausted,
    /// No more authentication attempt tokens can be allocated safely.
    #[error("DeepX WebSocket authentication attempt token space exhausted")]
    AuthenticationAttemptTokenExhausted,
    /// A reconnect attempted to reuse or move backward from the current connection epoch.
    #[error(
        "DeepX WebSocket connection epoch must increase: current {current}, received {received}"
    )]
    NonIncreasingConnectionEpoch {
        /// Current protocol-owner connection epoch.
        current: u64,
        /// Reconnect epoch supplied by the caller.
        received: u64,
    },
    /// An authentication attempt completed with an explicit failure.
    #[error("DeepX WebSocket authentication failed: {0}")]
    AuthenticationFailed(String),
    /// No authentication result arrived within the caller's bounded wait.
    #[error("DeepX WebSocket authentication timed out")]
    AuthenticationTimeout,
    /// A connection-owned request ended before a correlated response arrived.
    #[error("DeepX WebSocket request canceled: {0}")]
    RequestCanceled(String),
    /// No correlated response arrived within the caller's bounded wait.
    #[error("DeepX WebSocket request {request_id} timed out")]
    RequestTimeout {
        /// Wire-level request identifier which timed out.
        request_id: u64,
    },
}
