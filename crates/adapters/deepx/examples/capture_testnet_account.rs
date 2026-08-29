// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Captures redacted, schema-neutral DeepX testnet account responses for protocol analysis.

use std::{
    env, fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use nautilus_deepx::{
    common::{credential::DEEPX_TESTNET_SUBACCOUNT_ADDRESS, enums::DeepXEnvironment, urls},
    http::{
        client::DeepXRawHttpClient,
        models::DeepXRawAccountCapture,
        query::{DeepXHistoryQuery, DeepXSymbolQuery},
    },
};

const OUTPUT_ENV: &str = "DEEPX_ACCOUNT_CAPTURE_PATH";
const CAPTURE_ENDPOINTS: [&str; 6] = [
    "GET /v1/account/subaccounts/{address}/balances",
    "GET /v1/account/subaccounts/{address}/portfolio",
    "GET /v1/account/subaccounts/{address}/perp/positions",
    "GET /v1/account/subaccounts/{address}/perp/orders/open",
    "GET /v1/account/subaccounts/{address}/perp/orders",
    "GET /v1/account/subaccounts/{address}/perp/trades",
];

#[tokio::main]
async fn main() -> Result<()> {
    let address = env::var(DEEPX_TESTNET_SUBACCOUNT_ADDRESS).with_context(|| {
        format!("set {DEEPX_TESTNET_SUBACCOUNT_ADDRESS} to a DeepX testnet subaccount")
    })?;
    let output_path = PathBuf::from(
        env::var(OUTPUT_ENV).with_context(|| format!("set {OUTPUT_ENV} to an output JSON path"))?,
    );
    let rest_base_url = urls::rest_url(DeepXEnvironment::Testnet).to_string();
    let client = DeepXRawHttpClient::new(Some(rest_base_url.clone()), 30, None)?;
    let snapshot = client
        .get_raw_account_snapshot(
            &address,
            &DeepXSymbolQuery::default(),
            &DeepXHistoryQuery {
                limit: Some(500),
                ..Default::default()
            },
        )
        .await?;
    let captured_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis()
        .try_into()
        .context("capture timestamp exceeds u64")?;
    let capture = DeepXRawAccountCapture::new(
        captured_at_unix_ms,
        DeepXEnvironment::Testnet,
        rest_base_url,
        CAPTURE_ENDPOINTS.iter().map(ToString::to_string).collect(),
        &snapshot,
    );
    let json = serde_json::to_vec_pretty(&capture)?;

    fs::write(&output_path, json)
        .with_context(|| format!("failed to write capture to {}", output_path.display()))?;
    println!(
        "Wrote redacted DeepX account capture to {}",
        output_path.display()
    );
    Ok(())
}
