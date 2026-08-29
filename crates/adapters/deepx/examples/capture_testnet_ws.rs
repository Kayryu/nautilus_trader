// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Captures redacted DeepX testnet WebSocket frames for protocol analysis.

use std::{
    env, fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use nautilus_deepx::{
    common::{enums::DeepXEnvironment, urls},
    websocket::requests::{DeepXWsParams, DeepXWsRequest},
};
use nautilus_network::{
    Message,
    websocket::{MessageHandler, TransportBackend, WebSocketClient, WebSocketConfig},
};
use serde::Serialize;
use serde_json::{Value, json};

const OUTPUT_ENV: &str = "DEEPX_WS_CAPTURE_PATH";
const REQUESTS_ENV: &str = "DEEPX_WS_CAPTURE_REQUESTS";
const SYMBOL_ENV: &str = "DEEPX_WS_CAPTURE_SYMBOL";
const DURATION_ENV: &str = "DEEPX_WS_CAPTURE_SECS";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Capture {
    captured_at_unix_ms: u64,
    environment: DeepXEnvironment,
    url: String,
    requests: Vec<Value>,
    frames: Vec<CapturedFrame>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CapturedFrame {
    elapsed_ms: u64,
    frame_type: &'static str,
    payload: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let output_path = PathBuf::from(
        env::var(OUTPUT_ENV).with_context(|| format!("set {OUTPUT_ENV} to an output JSON path"))?,
    );
    let duration_secs = env::var(DURATION_ENV)
        .map_or(Ok(30), |value| value.parse::<u64>())
        .with_context(|| format!("{DURATION_ENV} must be an unsigned integer"))?;
    anyhow::ensure!(
        duration_secs > 0,
        "{DURATION_ENV} must be greater than zero"
    );

    let requests = capture_requests()?;
    let url = urls::ws_url(DeepXEnvironment::Testnet).to_string();
    let config = WebSocketConfig {
        url: url.clone(),
        headers: vec![],
        heartbeat_interval_secs: None,
        heartbeat_payload: None,
        connect_timeout_ms: Some(10_000),
        reconnect_delay_initial_ms: None,
        reconnect_delay_max_ms: None,
        reconnect_backoff_factor: None,
        reconnect_jitter_ms: None,
        reconnect_max_attempts: Some(0),
        heartbeat_timeout_secs: None,
        idle_timeout_ms: None,
        backend: TransportBackend::default(),
        proxy_url: None,
    };
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let handler: MessageHandler = Arc::new(move |message| {
        let _ = sender.send(message);
    });
    let client = WebSocketClient::builder()
        .config(config)
        .message_handler(handler)
        .connect()
        .await
        .context("failed to connect to the DeepX testnet WebSocket")?;

    for request in &requests {
        client
            .send_text(serde_json::to_string(request)?, None)
            .await
            .context("failed to send a DeepX testnet capture request")?;
    }

    let started = Instant::now();
    let deadline = started + Duration::from_secs(duration_secs);
    let mut frames = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, receiver.recv()).await {
            Ok(Some(message)) => frames.push(capture_frame(message, started)),
            Ok(None) | Err(_) => break,
        }
    }
    client.send_close_message().await.ok();

    let captured_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis()
        .try_into()
        .context("capture timestamp exceeds u64")?;
    let capture = Capture {
        captured_at_unix_ms,
        environment: DeepXEnvironment::Testnet,
        url,
        requests: requests.into_iter().map(redact_json).collect(),
        frames,
    };
    fs::write(&output_path, serde_json::to_vec_pretty(&capture)?)
        .with_context(|| format!("failed to write capture to {}", output_path.display()))?;
    println!(
        "Wrote {} redacted DeepX WebSocket frames to {}",
        capture.frames.len(),
        output_path.display()
    );
    Ok(())
}

fn capture_requests() -> Result<Vec<Value>> {
    if let Ok(raw) = env::var(REQUESTS_ENV) {
        let requests: Vec<Value> = serde_json::from_str(&raw)
            .with_context(|| format!("{REQUESTS_ENV} must be a JSON array of request objects"))?;
        anyhow::ensure!(!requests.is_empty(), "{REQUESTS_ENV} must not be empty");
        anyhow::ensure!(
            requests.iter().all(Value::is_object),
            "{REQUESTS_ENV} must contain only request objects"
        );
        return Ok(requests);
    }

    let symbol = env::var(SYMBOL_ENV).unwrap_or_else(|_| "ETH-USDC".to_string());
    Ok(vec![
        serde_json::to_value(DeepXWsRequest::subscribe(
            1,
            DeepXWsParams::perpetual_order_book(&symbol),
        ))?,
        serde_json::to_value(DeepXWsRequest::subscribe(
            2,
            DeepXWsParams::perpetual_trades(symbol),
        ))?,
    ])
}

fn capture_frame(message: Message, started: Instant) -> CapturedFrame {
    let elapsed_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    let (frame_type, payload) = match message {
        Message::Text(bytes) => ("text", redact_payload(&bytes)),
        Message::Binary(bytes) => ("binary", json!({"length": bytes.len()})),
        Message::Ping(bytes) => ("ping", redact_payload(&bytes)),
        Message::Pong(bytes) => ("pong", redact_payload(&bytes)),
        Message::Close(frame) => ("close", json!({"frame": format!("{frame:?}")})),
    };
    CapturedFrame {
        elapsed_ms,
        frame_type,
        payload,
    }
}

fn redact_payload(bytes: &[u8]) -> Value {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return json!({"length": bytes.len()});
    };
    serde_json::from_str(text)
        .map(redact_json)
        .unwrap_or_else(|_| Value::String(redact_text(text)))
}

fn redact_json(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| {
                    let value = if is_sensitive_key(&key) {
                        Value::String("<redacted>".to_string())
                    } else {
                        redact_json(value)
                    };
                    (key, value)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(redact_json).collect()),
        Value::String(value) => Value::String(redact_text(&value)),
        value => value,
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    [
        "address",
        "apikey",
        "authorization",
        "privatekey",
        "signature",
        "subaccount",
        "token",
        "wallet",
    ]
    .iter()
    .any(|candidate| normalized.contains(candidate))
}

fn redact_text(text: &str) -> String {
    text.split_whitespace()
        .map(|word| {
            if word.starts_with("0x") && word.len() >= 20 {
                "<redacted>"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_nested_sensitive_json_fields() {
        let value = json!({
            "params": {"subaccount": "0x1234", "symbol": "ETH-USDC"},
            "signature": "secret",
        });

        assert_eq!(
            redact_json(value),
            json!({
                "params": {"subaccount": "<redacted>", "symbol": "ETH-USDC"},
                "signature": "<redacted>",
            })
        );
    }

    #[test]
    fn redacts_address_like_plain_text_tokens() {
        assert_eq!(
            redact_text("account 0x1234567890abcdef1234 connected"),
            "account <redacted> connected"
        );
    }
}
