// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Caller-owned WebSocket reads and typed read-only commands, with no credentials or trading sends.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use nautilus_network::{
    transport::{BoxedWsTransport, Message},
    websocket::{MessageReader, TransportBackend, WebSocketClientInner},
};

use crate::config::DeepXNetworkConfig;

/// Owns the socket halves directly; dropping it leaves no reconnect or reader task behind.
pub struct DeepXWsReadConnection {
    writer: SplitSink<BoxedWsTransport, Message>,
    reader: MessageReader,
    timeout: Duration,
    closed: bool,
}

impl std::fmt::Debug for DeepXWsReadConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeepXWsReadConnection")
            .field("timeout", &self.timeout)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl DeepXWsReadConnection {
    /// Sends one documented public subscription or heartbeat command without automatic retry.
    ///
    /// A send error marks the connection terminal; it does not prove the peer received nothing.
    /// No command can submit a transaction or access a private account.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request, terminal connection or bounded send failure.
    pub async fn send_public_request(
        &mut self,
        request: &super::public::DeepXWsPublicRequest,
    ) -> anyhow::Result<()> {
        let text = request.to_text()?;
        self.send_text(text, "public").await
    }

    /// Sends one documented address-scoped account subscription without automatic retry.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid request, terminal connection or bounded send failure.
    pub async fn send_account_request(
        &mut self,
        request: &super::account::DeepXWsUserBalancesRequest,
    ) -> anyhow::Result<()> {
        let text = request.to_text()?;
        self.send_text(text, "account").await
    }

    async fn send_text(&mut self, text: String, role: &str) -> anyhow::Result<()> {
        anyhow::ensure!(!self.closed, "DeepX WebSocket connection is closed");
        if !tokio::time::timeout(self.timeout, self.writer.send(Message::text(text)))
            .await
            .is_ok_and(|result| result.is_ok())
        {
            self.closed = true;
            anyhow::bail!("DeepX {role} WebSocket send failed or timed out");
        }
        Ok(())
    }

    /// Opens the documented endpoint once without credentials, retries or automatic reconnect.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, failed upgrade or bounded connect timeout.
    pub async fn connect(
        network: &DeepXNetworkConfig,
        proxy_url: Option<&str>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !timeout.is_zero(),
            "DeepX WebSocket timeout must be positive"
        );
        let url = network.ws_connection_url()?;
        nautilus_cryptography::providers::install_cryptographic_provider();
        let (writer, reader) = tokio::time::timeout(
            timeout,
            WebSocketClientInner::connect_with_server(
                &url,
                vec![],
                TransportBackend::Tungstenite,
                proxy_url,
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("DeepX WebSocket upgrade timed out"))?
        .map_err(|_| anyhow::anyhow!("DeepX WebSocket upgrade failed"))?;
        Ok(Self {
            writer,
            reader,
            timeout,
            closed: false,
        })
    }

    /// Receives one raw frame, answering protocol Ping frames without assuming a JSON schema.
    ///
    /// Timeout leaves the socket owned and permits another bounded receive. EOF or transport
    /// failure makes subsequent receives terminal. Reads send only protocol control responses.
    ///
    /// # Errors
    ///
    /// Returns an error for receive timeout, failed transport or failed control-frame response.
    pub async fn receive(&mut self) -> anyhow::Result<Option<Message>> {
        if self.closed {
            return Ok(None);
        }
        let message = tokio::time::timeout(self.timeout, self.reader.next())
            .await
            .map_err(|_| super::DeepXWsError::ReceiveTimeout)?;
        let message = match message {
            Some(Ok(message)) => message,
            Some(Err(_)) => {
                self.closed = true;
                anyhow::bail!("DeepX WebSocket receive failed");
            }
            None => {
                self.closed = true;
                return Ok(None);
            }
        };
        match &message {
            Message::Ping(payload) => {
                if !tokio::time::timeout(
                    self.timeout,
                    self.writer.send(Message::Pong(payload.clone())),
                )
                .await
                .is_ok_and(|result| result.is_ok())
                {
                    self.closed = true;
                    anyhow::bail!("DeepX WebSocket Pong failed");
                }
            }
            Message::Close(_) => {
                self.closed = true;
                tokio::time::timeout(self.timeout, self.writer.flush())
                    .await
                    .map_err(|_| anyhow::anyhow!("DeepX WebSocket close response timed out"))?
                    .map_err(|_| anyhow::anyhow!("DeepX WebSocket close response failed"))?;
            }
            _ => {}
        }
        Ok(Some(message))
    }

    /// Sends a bounded close handshake and marks the connection terminal.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot close within the operation timeout.
    pub async fn close(&mut self) -> anyhow::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        tokio::time::timeout(self.timeout, self.writer.close())
            .await
            .map_err(|_| anyhow::anyhow!("DeepX WebSocket close timed out"))?
            .map_err(|_| anyhow::anyhow!("DeepX WebSocket close failed"))
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        extract::ws::{Message as ServerMessage, WebSocketUpgrade},
        routing::get,
    };
    use tokio::sync::oneshot;

    use super::*;

    struct Server(tokio::task::JoinHandle<()>);

    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    async fn server(
        scenario: u8,
    ) -> (DeepXNetworkConfig, oneshot::Receiver<ServerMessage>, Server) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel();
        let tx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx)));
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| {
                let tx = tx.clone();
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        match scenario {
                            0 => {
                                socket
                                    .send(ServerMessage::Text("{\"unknown\":true}".into()))
                                    .await
                                    .unwrap();
                                socket
                                    .send(ServerMessage::Binary(vec![0, 1, 255].into()))
                                    .await
                                    .unwrap();
                                socket
                                    .send(ServerMessage::Ping(vec![7, 8].into()))
                                    .await
                                    .unwrap();
                            }
                            1 => {
                                socket.send(ServerMessage::Close(None)).await.unwrap();
                            }
                            _ => {
                                tokio::time::sleep(Duration::from_millis(80)).await;
                                socket
                                    .send(ServerMessage::Text("late".into()))
                                    .await
                                    .unwrap();
                            }
                        }
                        if let Ok(Some(Ok(message))) =
                            tokio::time::timeout(Duration::from_secs(2), socket.recv()).await
                            && let Some(tx) = tx.lock().await.take()
                        {
                            let _ = tx.send(message);
                        }
                    })
                }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            DeepXNetworkConfig {
                base_url_ws: Some(format!("ws://{address}")),
                ..Default::default()
            },
            rx,
            Server(task),
        )
    }

    #[tokio::test]
    async fn raw_connection_preserves_frames_and_answers_protocol_ping() {
        let (network, pong, _server) = server(0).await;
        let mut connection = DeepXWsReadConnection::connect(&network, None, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            connection.receive().await.unwrap(),
            Some(Message::text("{\"unknown\":true}"))
        );
        assert_eq!(
            connection.receive().await.unwrap(),
            Some(Message::binary(vec![0, 1, 255]))
        );
        assert_eq!(
            connection.receive().await.unwrap(),
            Some(Message::ping(vec![7, 8]))
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pong)
                .await
                .unwrap()
                .unwrap(),
            ServerMessage::Pong(vec![7, 8].into())
        );
        connection.close().await.unwrap();
        assert!(connection.receive().await.unwrap().is_none());
        connection.close().await.unwrap();
    }

    #[tokio::test]
    async fn remote_close_is_terminal_and_acknowledged() {
        let (network, close, _server) = server(1).await;
        let mut connection = DeepXWsReadConnection::connect(&network, None, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            connection.receive().await.unwrap(),
            Some(Message::Close(None))
        );
        assert!(connection.receive().await.unwrap().is_none());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), close)
                .await
                .unwrap()
                .unwrap(),
            ServerMessage::Close(None)
        );
    }

    #[tokio::test]
    async fn receive_timeout_does_not_discard_owned_connection() {
        let (network, _close, _server) = server(2).await;
        let mut connection = DeepXWsReadConnection::connect(&network, None, Duration::from_secs(1))
            .await
            .unwrap();
        connection.timeout = Duration::from_millis(10);
        assert!(connection.receive().await.is_err());
        connection.timeout = Duration::from_secs(1);
        assert_eq!(
            connection.receive().await.unwrap(),
            Some(Message::text("late"))
        );
        connection.close().await.unwrap();
    }

    #[tokio::test]
    async fn read_connection_rejects_zero_timeout_and_failed_upgrade() {
        assert!(
            DeepXWsReadConnection::connect(&DeepXNetworkConfig::default(), None, Duration::ZERO)
                .await
                .is_err()
        );
        let (mut network, _close, _server) = server(0).await;
        network.base_url_ws.as_mut().unwrap().push_str("/missing");
        assert!(
            DeepXWsReadConnection::connect(&network, None, Duration::from_secs(1))
                .await
                .is_err()
        );
    }
}
