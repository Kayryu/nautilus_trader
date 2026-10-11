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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of initial account summary WebSocket values.

use std::time::Duration;

use anyhow::{Context, ensure};
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::DeepXHttpClient,
    websocket::{DeepXWsAccountMetricsRequest, observe_initial_account_metrics},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let wallet = args
        .next()
        .context("usage: deepx-verify-ws-account-metrics <wallet-account-id20>")?;
    ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let directory = client.get_wallet_subaccounts(&wallet).await?;
    ensure!(
        !directory.addresses.is_empty(),
        "wallet subaccount directory is empty"
    );
    for subaccount in &directory.addresses {
        let request = DeepXWsAccountMetricsRequest::new(subaccount)?;
        let metrics = observe_initial_account_metrics(
            &config.network,
            config.proxy_url.as_deref(),
            Duration::from_secs(35),
            &request,
        )
        .await?;
        for metric in metrics {
            println!(
                "DeepX account summary subaccount={} channel={:?} raw_data={} timestamp_ms={}",
                request.subaccount(),
                metric.channel(),
                metric.raw_data().get(),
                metric.timestamp_ms(),
            );
        }
    }
    println!(
        "DeepX account summaries verified wallet={} subaccounts={}; mutable read-only observations, no credentials or transactions sent",
        directory.wallet(),
        directory.addresses.len(),
    );
    Ok(())
}
