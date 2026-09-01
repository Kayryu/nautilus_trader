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

//! Capture public DeepX testnet runtime identity fixtures.
//!
//! Run with:
//! `cargo run -p nautilus-deepx --bin deepx-capture-runtime-fixtures`

use std::{fs, path::PathBuf};

use anyhow::{Context, ensure};
use aws_lc_rs::digest;
use jiff::{Timestamp, tz::Offset};
use nautilus_deepx::common::signed_extension_identifiers;
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const DEFAULT_RPC_URL: &str = "https://rpc-testnet.deepx.fi";
const EXPECTED_GENESIS_HASH: &str =
    "0x86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b";

#[derive(Debug, Deserialize, Serialize)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    id: u64,
    result: T,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeVersion {
    spec_name: String,
    impl_name: String,
    authoring_version: u32,
    spec_version: u32,
    impl_version: u32,
    apis: Vec<(String, u32)>,
    transaction_version: u32,
    state_version: u8,
}

#[derive(Debug, Serialize)]
struct FixtureIdentity {
    genesis_hash: String,
    metadata_sha256: String,
    spec_version: u32,
    transaction_version: u32,
}

#[derive(Debug, Serialize)]
struct FixtureRecord {
    method: String,
    params: Value,
    payload_path: String,
    bytes: usize,
}

#[derive(Debug, Serialize)]
struct FixtureManifest {
    captured_at: String,
    deployment: String,
    endpoint_role: String,
    rpc_url: String,
    block_reference: String,
    block_hash: String,
    identity: FixtureIdentity,
    metadata_bytes: usize,
    signed_extensions: Vec<String>,
    fixtures: Vec<FixtureRecord>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let genesis = rpc::<String>(&client, "chain_getBlockHash", json!([0])).await?;
    ensure!(
        genesis.result == EXPECTED_GENESIS_HASH,
        "DeepX genesis hash mismatch: expected {EXPECTED_GENESIS_HASH}, received {}",
        genesis.result,
    );

    let finalized_head = rpc::<String>(&client, "chain_getFinalizedHead", json!([])).await?;
    let block_params = json!([finalized_head.result]);
    let runtime =
        rpc::<RuntimeVersion>(&client, "state_getRuntimeVersion", block_params.clone()).await?;
    let metadata = rpc::<String>(&client, "state_getMetadata", block_params.clone()).await?;
    let metadata_bytes = nautilus_core::hex::decode(metadata.result.trim_start_matches("0x"))
        .context("DeepX runtime metadata was not valid hex")?;
    let metadata_sha256 =
        nautilus_core::hex::encode(digest::digest(&digest::SHA256, &metadata_bytes).as_ref());
    let signed_extensions = signed_extension_identifiers(&metadata_bytes)
        .context("Failed to extract DeepX signed-extension order")?;
    let identity = FixtureIdentity {
        genesis_hash: genesis.result.clone(),
        metadata_sha256,
        spec_version: runtime.result.spec_version,
        transaction_version: runtime.result.transaction_version,
    };
    let fixture_dir = fixture_dir(&identity, &finalized_head.result);
    ensure!(
        !fixture_dir.exists(),
        "DeepX fixture set already exists and is immutable: {}",
        fixture_dir.display(),
    );
    fs::create_dir_all(&fixture_dir)?;

    let fixtures = vec![
        write_fixture(
            &fixture_dir,
            "genesis_hash.json",
            "chain_getBlockHash",
            json!([0]),
            &genesis,
        )?,
        write_fixture(
            &fixture_dir,
            "finalized_head.json",
            "chain_getFinalizedHead",
            json!([]),
            &finalized_head,
        )?,
        write_fixture(
            &fixture_dir,
            "runtime_version.json",
            "state_getRuntimeVersion",
            block_params.clone(),
            &runtime,
        )?,
        write_fixture(
            &fixture_dir,
            "metadata.json",
            "state_getMetadata",
            block_params,
            &metadata,
        )?,
    ];
    let manifest = FixtureManifest {
        captured_at: Timestamp::now()
            .display_with_offset(Offset::UTC)
            .to_string(),
        deployment: "testnet".to_string(),
        endpoint_role: "runtime_identity".to_string(),
        rpc_url: DEFAULT_RPC_URL.to_string(),
        block_reference: "finalized".to_string(),
        block_hash: finalized_head.result,
        identity,
        metadata_bytes: metadata_bytes.len(),
        signed_extensions,
        fixtures,
    };
    write_json(fixture_dir.join("manifest.json"), &manifest)?;

    println!(
        "Captured DeepX runtime fixtures under {}",
        fixture_dir.display()
    );
    Ok(())
}

async fn rpc<T>(client: &Client, method: &str, params: Value) -> anyhow::Result<JsonRpcResponse<T>>
where
    T: DeserializeOwned,
{
    client
        .post(DEFAULT_RPC_URL)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("Failed to decode DeepX RPC response for {method}"))
}

fn fixture_dir(identity: &FixtureIdentity, block_hash: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_data/runtime/testnet")
        .join(format!(
            "genesis-{}_metadata-{}_spec-{}_tx-{}_finalized-{}",
            &identity.genesis_hash[2..10],
            &identity.metadata_sha256[..8],
            identity.spec_version,
            identity.transaction_version,
            block_hash
                .trim_start_matches("0x")
                .get(..8)
                .unwrap_or(block_hash),
        ))
}

fn write_fixture<T>(
    fixture_dir: &std::path::Path,
    file_name: &str,
    method: &str,
    params: Value,
    value: &T,
) -> anyhow::Result<FixtureRecord>
where
    T: Serialize,
{
    let bytes = write_json(fixture_dir.join(file_name), value)?;
    Ok(FixtureRecord {
        method: method.to_string(),
        params,
        payload_path: file_name.to_string(),
        bytes,
    })
}

fn write_json(path: PathBuf, value: &impl Serialize) -> anyhow::Result<usize> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(path, &bytes)?;
    Ok(bytes.len())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn fixture_identity() -> FixtureIdentity {
        FixtureIdentity {
            genesis_hash: "0x86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b"
                .to_string(),
            metadata_sha256: "e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37"
                .to_string(),
            spec_version: 366,
            transaction_version: 1,
        }
    }

    #[rstest]
    fn fixture_directory_includes_runtime_and_finalized_block_identity() {
        let path = fixture_dir(
            &fixture_identity(),
            "0xcfb45de9dc182734a6ce745ef75e8510cd796628861b17a286ea2f077de68315",
        );

        assert!(
            path.ends_with("genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-cfb45de9",)
        );
    }

    #[rstest]
    fn different_finalized_blocks_produce_different_directories() {
        let identity = fixture_identity();
        let first = fixture_dir(&identity, "0x1111111122222222");
        let second = fixture_dir(&identity, "0x3333333344444444");

        assert_ne!(first, second);
    }
}
