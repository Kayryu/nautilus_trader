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

//! Owned task lifecycle for a future DeepX WebSocket handler.

use std::{future::Future, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::DeepXWsError;

/// Terminal outcome of an owner-requested WebSocket task shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeepXWsTaskOutcome {
    /// The task observed cancellation and exited within the grace period.
    Completed,
    /// The task exceeded the grace period and was forcibly aborted and joined.
    Aborted,
}

/// Owns one handler-task generation and its cancellation token.
#[derive(Debug)]
pub struct DeepXWsTaskHandles {
    cancellation: CancellationToken,
    handler: Option<JoinHandle<()>>,
}

impl Default for DeepXWsTaskHandles {
    fn default() -> Self {
        Self::new()
    }
}

impl DeepXWsTaskHandles {
    /// Creates an empty task owner.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            handler: None,
        }
    }

    /// Spawns and owns one handler generation.
    ///
    /// The supplied factory receives the generation-specific cancellation token.
    ///
    /// # Errors
    ///
    /// Returns [`DeepXWsError::TaskAlreadyRunning`] while another handle is owned.
    pub fn spawn<F, Fut>(&mut self, task: F) -> Result<(), DeepXWsError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.handler.is_some() {
            return Err(DeepXWsError::TaskAlreadyRunning);
        }

        self.cancellation = CancellationToken::new();
        let cancellation = self.cancellation.clone();
        self.handler = Some(tokio::spawn(task(cancellation)));
        Ok(())
    }

    /// Returns the cancellation token for the currently owned generation.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Returns whether a handler task remains owned.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.handler
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Cancels and joins the owned task, aborting it after `grace_period` elapses.
    ///
    /// # Errors
    ///
    /// Returns [`DeepXWsError::TaskJoin`] if the task panicked or otherwise failed to join.
    pub async fn shutdown(
        &mut self,
        grace_period: Duration,
    ) -> Result<DeepXWsTaskOutcome, DeepXWsError> {
        let Some(mut handler) = self.handler.take() else {
            return Ok(DeepXWsTaskOutcome::Completed);
        };

        self.cancellation.cancel();
        match tokio::time::timeout(grace_period, &mut handler).await {
            Ok(result) => result
                .map(|()| DeepXWsTaskOutcome::Completed)
                .map_err(|error| DeepXWsError::TaskJoin(error.to_string())),
            Err(_) => {
                handler.abort();
                match handler.await {
                    Err(error) if error.is_cancelled() => Ok(DeepXWsTaskOutcome::Aborted),
                    Err(error) => Err(DeepXWsError::TaskJoin(error.to_string())),
                    Ok(()) => Ok(DeepXWsTaskOutcome::Aborted),
                }
            }
        }
    }
}

impl Drop for DeepXWsTaskHandles {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(handler) = self.handler.as_ref() {
            handler.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use rstest::rstest;

    use super::*;

    #[tokio::test]
    async fn cooperative_task_is_cancelled_and_joined() {
        let stopped = Arc::new(AtomicBool::new(false));
        let task_stopped = Arc::clone(&stopped);
        let mut tasks = DeepXWsTaskHandles::new();
        tasks
            .spawn(move |cancellation| async move {
                cancellation.cancelled().await;
                task_stopped.store(true, Ordering::Release);
            })
            .unwrap();

        let outcome = tasks.shutdown(Duration::from_secs(1)).await.unwrap();

        assert_eq!(outcome, DeepXWsTaskOutcome::Completed);
        assert!(stopped.load(Ordering::Acquire));
        assert!(!tasks.is_running());
    }

    #[tokio::test]
    async fn uncooperative_task_is_aborted_and_joined() {
        let mut tasks = DeepXWsTaskHandles::new();
        tasks
            .spawn(|_| async move { std::future::pending::<()>().await })
            .unwrap();

        let outcome = tasks.shutdown(Duration::ZERO).await.unwrap();

        assert_eq!(outcome, DeepXWsTaskOutcome::Aborted);
        assert!(!tasks.is_running());
    }

    #[tokio::test]
    async fn rejects_second_task_while_handle_is_owned() {
        let mut tasks = DeepXWsTaskHandles::new();
        tasks
            .spawn(|cancellation| async move { cancellation.cancelled().await })
            .unwrap();

        assert!(matches!(
            tasks.spawn(|_| async {}),
            Err(DeepXWsError::TaskAlreadyRunning),
        ));
        tasks.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn task_panic_is_reported_as_join_failure() {
        let mut tasks = DeepXWsTaskHandles::new();
        tasks
            .spawn(|_| async move { panic!("handler failed") })
            .unwrap();

        assert!(matches!(
            tasks.shutdown(Duration::from_secs(1)).await,
            Err(DeepXWsError::TaskJoin(_)),
        ));
        assert!(!tasks.is_running());
    }

    #[tokio::test]
    async fn starts_fresh_generation_after_shutdown() {
        let mut tasks = DeepXWsTaskHandles::new();
        tasks
            .spawn(|cancellation| async move { cancellation.cancelled().await })
            .unwrap();
        let first_token = tasks.cancellation_token();
        tasks.shutdown(Duration::from_secs(1)).await.unwrap();

        tasks
            .spawn(|cancellation| async move { cancellation.cancelled().await })
            .unwrap();
        let second_token = tasks.cancellation_token();

        assert!(first_token.is_cancelled());
        assert!(!second_token.is_cancelled());
        tasks.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[rstest]
    fn drop_cancels_generation_token() {
        let token = {
            let tasks = DeepXWsTaskHandles::new();
            let token = tasks.cancellation_token();
            drop(tasks);
            token
        };

        assert!(token.is_cancelled());
    }
}
