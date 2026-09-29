// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of DeepX Spot ADL candidate discovery.

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXDataClientConfig,
    http::{DeepXHttpClient, DeepXSpotAdlCandidatesRequest},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let target_subaccount = args.next().context(
        "usage: deepx-verify-rest-spot-adl-candidates <target-subaccount> <asset> <liability> <min-cover-raw-integer> [limit]",
    )?;
    let asset = args.next().context("missing asset symbol")?;
    let liability = args.next().context("missing liability symbol")?;
    let min_cover_amount = args.next().context("missing raw minimum cover amount")?;
    let limit = args
        .next()
        .map(|value| {
            value
                .parse::<u32>()
                .context("limit must be an unsigned integer")
        })
        .transpose()?;
    anyhow::ensure!(args.next().is_none(), "unexpected extra argument");

    let config = DeepXDataClientConfig::default();
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )?;
    let response = client
        .get_spot_adl_candidates(&DeepXSpotAdlCandidatesRequest {
            target_subaccount,
            asset,
            liability,
            min_cover_amount,
            limit,
        })
        .await?;

    println!(
        "DeepX Spot ADL candidates verified target={} asset={} liability={} min_cover_raw={} covered_raw={} candidates={}",
        response.target_subaccount,
        response.asset,
        response.liability,
        response.min_cover_amount,
        response.covered_amount,
        response.candidates.len(),
    );
    for (index, candidate) in response.candidates.iter().enumerate() {
        println!(
            "candidate index={} subaccount={} available_raw={} borrow_raw={}",
            index, candidate.subaccount, candidate.available_amount, candidate.borrow_amount,
        );
    }
    println!(
        "Amounts retain endpoint raw integer units; candidate order and eligibility are not inferred. No credentials or transactions were used"
    );
    Ok(())
}
