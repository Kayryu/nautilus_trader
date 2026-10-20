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

//! Live read-only verification of testnet signing metadata discovery.

use nautilus_core::hex;
use nautilus_deepx::{
    config::DeepXNetworkConfig, observe_and_validate_rpc_endpoint_identities,
    observe_current_runtime_snapshot_for_endpoints,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = DeepXNetworkConfig::default();
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config).await?;
    let snapshot =
        observe_current_runtime_snapshot_for_endpoints(&config.environment, &endpoints).await?;
    let identity = snapshot.identity();
    println!(
        "genesis=0x{} spec={} transaction={} metadata_sha256=0x{}",
        hex::encode(identity.genesis_hash),
        identity.spec_version,
        identity.transaction_version,
        hex::encode(identity.metadata_sha256),
    );
    Ok(())
}
