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

use std::{
    collections::BTreeSet,
    env, fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, ensure};
use aws_lc_rs::digest;
use jiff::{Timestamp, tz::Offset};
use nautilus_deepx::common::signed_extension_identifiers;
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const DEFAULT_RPC_URL: &str = "https://rpc-testnet.deepx.fi";
const RPC_URL_ENV: &str = "DEEPX_TESTNET_RPC_URL";
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockHeader {
    parent_hash: String,
    number: String,
    state_root: String,
    extrinsics_root: String,
    digest: Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct FixtureIdentity {
    genesis_hash: String,
    metadata_sha256: String,
    spec_version: u32,
    transaction_version: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct FixtureRecord {
    method: String,
    params: Value,
    payload_path: String,
    bytes: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct FixtureManifest {
    captured_at: String,
    deployment: String,
    endpoint_role: String,
    rpc_url: String,
    block_reference: String,
    block_hash: String,
    block_number: u64,
    identity: FixtureIdentity,
    metadata_bytes: usize,
    signed_extensions: Vec<String>,
    fixtures: Vec<FixtureRecord>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let rpc_url = env::var(RPC_URL_ENV).unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
    let genesis = rpc::<String>(&client, &rpc_url, "chain_getBlockHash", json!([0])).await?;
    ensure!(
        genesis.result == EXPECTED_GENESIS_HASH,
        "DeepX genesis hash mismatch: expected {EXPECTED_GENESIS_HASH}, received {}",
        genesis.result,
    );

    let finalized_head =
        rpc::<String>(&client, &rpc_url, "chain_getFinalizedHead", json!([])).await?;
    let block_params = json!([finalized_head.result]);
    let finalized_header =
        rpc::<BlockHeader>(&client, &rpc_url, "chain_getHeader", block_params.clone()).await?;
    let block_number = parse_block_number(&finalized_header.result.number)?;
    let finalized_block_hash = rpc::<String>(
        &client,
        &rpc_url,
        "chain_getBlockHash",
        json!([block_number]),
    )
    .await?;
    ensure!(
        finalized_block_hash.result == finalized_head.result,
        "DeepX finalized header is not canonical at block {block_number}",
    );
    let runtime = rpc::<RuntimeVersion>(
        &client,
        &rpc_url,
        "state_getRuntimeVersion",
        block_params.clone(),
    )
    .await?;
    let metadata =
        rpc::<String>(&client, &rpc_url, "state_getMetadata", block_params.clone()).await?;
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
    let staging_dir = fixture_dir.with_extension(format!("partial-{}", std::process::id()));
    ensure!(
        !staging_dir.exists(),
        "DeepX fixture staging directory already exists: {}",
        staging_dir.display(),
    );
    fs::create_dir_all(&staging_dir)?;

    let write_result = (|| -> anyhow::Result<()> {
        let fixtures = vec![
            write_fixture(
                &staging_dir,
                "genesis_hash.json",
                "chain_getBlockHash",
                json!([0]),
                &genesis,
            )?,
            write_fixture(
                &staging_dir,
                "finalized_head.json",
                "chain_getFinalizedHead",
                json!([]),
                &finalized_head,
            )?,
            write_fixture(
                &staging_dir,
                "finalized_header.json",
                "chain_getHeader",
                block_params.clone(),
                &finalized_header,
            )?,
            write_fixture(
                &staging_dir,
                "finalized_block_hash.json",
                "chain_getBlockHash",
                json!([block_number]),
                &finalized_block_hash,
            )?,
            write_fixture(
                &staging_dir,
                "runtime_version.json",
                "state_getRuntimeVersion",
                block_params.clone(),
                &runtime,
            )?,
            write_fixture(
                &staging_dir,
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
            rpc_url,
            block_reference: "finalized".to_string(),
            block_hash: finalized_head.result,
            block_number,
            identity,
            metadata_bytes: metadata_bytes.len(),
            signed_extensions,
            fixtures,
        };
        write_json(staging_dir.join("manifest.json"), &manifest)?;
        Ok(())
    })();
    if let Err(e) = write_result {
        return Err(clean_staging_after_error(&staging_dir, e));
    }
    publish_fixture_set(&staging_dir, &fixture_dir)?;

    println!(
        "Captured DeepX runtime fixtures under {}",
        fixture_dir.display()
    );
    Ok(())
}

fn publish_fixture_set(staging_dir: &Path, fixture_dir: &Path) -> anyhow::Result<()> {
    let result = validate_written_fixture_set(staging_dir).and_then(|()| {
        fs::rename(staging_dir, fixture_dir).with_context(|| {
            format!(
                "Failed to publish DeepX fixture set from {} to {}",
                staging_dir.display(),
                fixture_dir.display(),
            )
        })
    });
    result.map_err(|e| clean_staging_after_error(staging_dir, e))
}

fn clean_staging_after_error(staging_dir: &Path, error: anyhow::Error) -> anyhow::Error {
    match fs::remove_dir_all(staging_dir) {
        Ok(()) => error,
        Err(cleanup_error) => error.context(format!(
            "Failed to clean incomplete DeepX fixture staging directory {}: {cleanup_error}",
            staging_dir.display(),
        )),
    }
}

async fn rpc<T>(
    client: &Client,
    rpc_url: &str,
    method: &str,
    params: Value,
) -> anyhow::Result<JsonRpcResponse<T>>
where
    T: DeserializeOwned,
{
    let response: JsonRpcResponse<T> = client
        .post(rpc_url)
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
        .with_context(|| format!("Failed to decode DeepX RPC response for {method}"))?;
    ensure!(
        response.jsonrpc == "2.0",
        "DeepX RPC response for {method} has unexpected JSON-RPC version: {}",
        response.jsonrpc,
    );
    ensure!(
        response.id == 1,
        "DeepX RPC response for {method} has unexpected request ID: {}",
        response.id,
    );
    Ok(response)
}

fn parse_block_number(value: &str) -> anyhow::Result<u64> {
    let encoded = value
        .strip_prefix("0x")
        .context("DeepX finalized block number must start with 0x")?;
    ensure!(
        !encoded.is_empty(),
        "DeepX finalized block number must not be empty",
    );
    u64::from_str_radix(encoded, 16)
        .with_context(|| format!("Invalid DeepX finalized block number: {value}"))
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

fn validate_written_fixture_set(fixture_dir: &Path) -> anyhow::Result<()> {
    let manifest_path = fixture_dir.join("manifest.json");
    let manifest: FixtureManifest = read_json(&manifest_path)?;
    let expected_fixtures = [
        "genesis_hash.json",
        "finalized_head.json",
        "finalized_header.json",
        "finalized_block_hash.json",
        "runtime_version.json",
        "metadata.json",
    ];
    ensure!(
        manifest.fixtures.len() == expected_fixtures.len(),
        "DeepX fixture manifest must contain exactly six payload records",
    );
    let mut payload_paths = BTreeSet::new();

    for fixture in &manifest.fixtures {
        let payload_path = Path::new(&fixture.payload_path);
        ensure!(
            matches!(
                payload_path.components().collect::<Vec<_>>().as_slice(),
                [Component::Normal(_)]
            ),
            "DeepX fixture payload path must be a file name: {}",
            fixture.payload_path,
        );
        ensure!(
            payload_paths.insert(&fixture.payload_path),
            "DeepX fixture manifest contains duplicate payload path {}",
            fixture.payload_path,
        );
        let payload_metadata =
            fs::symlink_metadata(fixture_dir.join(payload_path)).with_context(|| {
                format!(
                    "Failed to inspect DeepX fixture payload {}",
                    fixture.payload_path
                )
            })?;
        ensure!(
            payload_metadata.file_type().is_file(),
            "DeepX fixture payload must be a regular file: {}",
            fixture.payload_path,
        );
        let actual_bytes = payload_metadata.len();
        ensure!(
            actual_bytes == fixture.bytes as u64,
            "DeepX fixture byte count mismatch for {}: expected {}, received {actual_bytes}",
            fixture.payload_path,
            fixture.bytes,
        );
    }
    for payload_path in expected_fixtures {
        fixture_record(&manifest, payload_path)?;
    }

    let genesis_record = fixture_record(&manifest, "genesis_hash.json")?;
    ensure!(
        genesis_record.method == "chain_getBlockHash",
        "DeepX genesis fixture has unexpected RPC method",
    );
    ensure!(
        genesis_record.params == json!([0]),
        "DeepX genesis fixture must request block zero",
    );
    let genesis = read_fixture::<String>(fixture_dir, genesis_record)?;
    ensure!(
        genesis.result == manifest.identity.genesis_hash,
        "DeepX fixture genesis hash does not match manifest identity",
    );

    let finalized_head_record = fixture_record(&manifest, "finalized_head.json")?;
    ensure!(
        finalized_head_record.method == "chain_getFinalizedHead",
        "DeepX finalized-head fixture has unexpected RPC method",
    );
    ensure!(
        finalized_head_record.params == json!([]),
        "DeepX finalized-head fixture must not have parameters",
    );
    let finalized_head = read_fixture::<String>(fixture_dir, finalized_head_record)?;
    ensure!(
        finalized_head.result == manifest.block_hash,
        "DeepX finalized head does not match manifest block hash",
    );

    let block_params = json!([manifest.block_hash]);
    let header_record = fixture_record(&manifest, "finalized_header.json")?;
    ensure!(
        header_record.method == "chain_getHeader",
        "DeepX finalized-header fixture has unexpected RPC method",
    );
    ensure!(
        header_record.params == block_params,
        "DeepX finalized-header fixture is not bound to the manifest block hash",
    );
    let header = read_fixture::<BlockHeader>(fixture_dir, header_record)?;
    ensure!(
        parse_block_number(&header.result.number)? == manifest.block_number,
        "DeepX finalized header number does not match manifest block number",
    );

    let block_hash_record = fixture_record(&manifest, "finalized_block_hash.json")?;
    ensure!(
        block_hash_record.method == "chain_getBlockHash",
        "DeepX finalized block-hash fixture has unexpected RPC method",
    );
    ensure!(
        block_hash_record.params == json!([manifest.block_number]),
        "DeepX finalized block-hash fixture does not request the manifest block number",
    );
    let block_hash = read_fixture::<String>(fixture_dir, block_hash_record)?;
    ensure!(
        block_hash.result == manifest.block_hash,
        "DeepX finalized header hash does not match manifest block hash",
    );

    let runtime_record = fixture_record(&manifest, "runtime_version.json")?;
    ensure!(
        runtime_record.method == "state_getRuntimeVersion",
        "DeepX runtime-version fixture has unexpected RPC method",
    );
    ensure!(
        runtime_record.params == block_params,
        "DeepX runtime-version fixture is not bound to the manifest block hash",
    );
    let runtime = read_fixture::<RuntimeVersion>(fixture_dir, runtime_record)?;
    ensure!(
        runtime.result.spec_version == manifest.identity.spec_version,
        "DeepX runtime spec version does not match manifest identity",
    );
    ensure!(
        runtime.result.transaction_version == manifest.identity.transaction_version,
        "DeepX runtime transaction version does not match manifest identity",
    );

    let metadata_record = fixture_record(&manifest, "metadata.json")?;
    ensure!(
        metadata_record.method == "state_getMetadata",
        "DeepX metadata fixture has unexpected RPC method",
    );
    ensure!(
        metadata_record.params == block_params,
        "DeepX metadata fixture is not bound to the manifest block hash",
    );
    let metadata = read_fixture::<String>(fixture_dir, metadata_record)?;
    let metadata_bytes = nautilus_core::hex::decode(metadata.result.trim_start_matches("0x"))
        .context("DeepX fixture metadata was not valid hex")?;
    ensure!(
        metadata_bytes.len() == manifest.metadata_bytes,
        "DeepX metadata length does not match manifest",
    );
    let metadata_sha256 =
        nautilus_core::hex::encode(digest::digest(&digest::SHA256, &metadata_bytes).as_ref());
    ensure!(
        metadata_sha256 == manifest.identity.metadata_sha256,
        "DeepX metadata SHA-256 does not match manifest identity",
    );
    ensure!(
        signed_extension_identifiers(&metadata_bytes)? == manifest.signed_extensions,
        "DeepX signed-extension order does not match runtime metadata",
    );
    Ok(())
}

fn fixture_record<'a>(
    manifest: &'a FixtureManifest,
    payload_path: &str,
) -> anyhow::Result<&'a FixtureRecord> {
    let mut matching = manifest
        .fixtures
        .iter()
        .filter(|fixture| fixture.payload_path == payload_path);
    let fixture = matching
        .next()
        .with_context(|| format!("DeepX fixture manifest is missing {payload_path}"))?;
    ensure!(
        matching.next().is_none(),
        "DeepX fixture manifest contains duplicate {payload_path} records",
    );
    Ok(fixture)
}

fn read_fixture<T>(
    fixture_dir: &Path,
    fixture: &FixtureRecord,
) -> anyhow::Result<JsonRpcResponse<T>>
where
    T: DeserializeOwned,
{
    let path = fixture_dir.join(&fixture.payload_path);
    let response: JsonRpcResponse<T> = read_json(&path)?;
    ensure!(
        response.jsonrpc == "2.0",
        "DeepX fixture response for {} has unexpected JSON-RPC version: {}",
        fixture.method,
        response.jsonrpc,
    );
    ensure!(
        response.id == 1,
        "DeepX fixture response for {} has unexpected request ID: {}",
        fixture.method,
        response.id,
    );
    Ok(response)
}

fn read_json<T>(path: &Path) -> anyhow::Result<T>
where
    T: DeserializeOwned,
{
    let bytes = fs::read(path)
        .with_context(|| format!("Failed to read DeepX fixture {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("Failed to decode DeepX fixture {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{Json, Router, routing::post};
    use rstest::rstest;
    use tokio::net::TcpListener;

    use super::*;

    const FINALIZED_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/runtime/testnet/",
        "genesis-86604388_metadata-e6b8b68e_spec-366_tx-1_finalized-03e29c08/metadata.json",
    ));
    const FINALIZED_HASH: &str =
        "0x03e29c08d90b26697535dacbcfa940c8d2ae08653e4b4760ac1dd4a281ced7c6";

    static TEMP_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempFixtureDir(PathBuf);

    impl TempFixtureDir {
        fn new() -> Self {
            let sequence = TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = env::temp_dir().join(format!(
                "deepx-runtime-fixture-{}-{timestamp}-{sequence}",
                std::process::id(),
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempFixtureDir {
        fn drop(&mut self) {
            if self.0.exists() {
                fs::remove_dir_all(&self.0).unwrap();
            }
        }
    }

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

    fn write_valid_fixture_set() -> TempFixtureDir {
        let fixture_dir = TempFixtureDir::new();
        let metadata: JsonRpcResponse<String> = serde_json::from_str(FINALIZED_METADATA).unwrap();
        let metadata_bytes =
            nautilus_core::hex::decode(metadata.result.trim_start_matches("0x")).unwrap();
        let responses = [
            (
                "genesis_hash.json",
                "chain_getBlockHash",
                json!([0]),
                json!({ "jsonrpc": "2.0", "id": 1, "result": EXPECTED_GENESIS_HASH }),
            ),
            (
                "finalized_head.json",
                "chain_getFinalizedHead",
                json!([]),
                json!({ "jsonrpc": "2.0", "id": 1, "result": FINALIZED_HASH }),
            ),
            (
                "finalized_header.json",
                "chain_getHeader",
                json!([FINALIZED_HASH]),
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "parentHash": "0x01",
                        "number": "0x2a",
                        "stateRoot": "0x02",
                        "extrinsicsRoot": "0x03",
                        "digest": { "logs": [] }
                    }
                }),
            ),
            (
                "finalized_block_hash.json",
                "chain_getBlockHash",
                json!([42]),
                json!({ "jsonrpc": "2.0", "id": 1, "result": FINALIZED_HASH }),
            ),
            (
                "runtime_version.json",
                "state_getRuntimeVersion",
                json!([FINALIZED_HASH]),
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "specName": "deepx",
                        "implName": "deepx",
                        "authoringVersion": 1,
                        "specVersion": 366,
                        "implVersion": 1,
                        "apis": [],
                        "transactionVersion": 1,
                        "stateVersion": 1
                    }
                }),
            ),
            (
                "metadata.json",
                "state_getMetadata",
                json!([FINALIZED_HASH]),
                serde_json::to_value(&metadata).unwrap(),
            ),
        ];
        let fixtures = responses
            .into_iter()
            .map(|(file_name, method, params, response)| {
                write_fixture(&fixture_dir.0, file_name, method, params, &response).unwrap()
            })
            .collect();
        let manifest = FixtureManifest {
            captured_at: "2026-09-10T00:00:00Z".to_string(),
            deployment: "testnet".to_string(),
            endpoint_role: "runtime_identity".to_string(),
            rpc_url: DEFAULT_RPC_URL.to_string(),
            block_reference: "finalized".to_string(),
            block_hash: FINALIZED_HASH.to_string(),
            block_number: 42,
            identity: FixtureIdentity {
                genesis_hash: EXPECTED_GENESIS_HASH.to_string(),
                metadata_sha256: nautilus_core::hex::encode(
                    digest::digest(&digest::SHA256, &metadata_bytes).as_ref(),
                ),
                spec_version: 366,
                transaction_version: 1,
            },
            metadata_bytes: metadata_bytes.len(),
            signed_extensions: signed_extension_identifiers(&metadata_bytes).unwrap(),
            fixtures,
        };
        write_json(fixture_dir.0.join("manifest.json"), &manifest).unwrap();
        fixture_dir
    }

    #[rstest]
    fn validates_complete_written_fixture_set() {
        let fixture_dir = write_valid_fixture_set();

        validate_written_fixture_set(&fixture_dir.0).unwrap();
    }

    #[rstest]
    fn rejects_tampered_signed_extension_order() {
        let fixture_dir = write_valid_fixture_set();
        let manifest_path = fixture_dir.0.join("manifest.json");
        let mut manifest: FixtureManifest = read_json(&manifest_path).unwrap();
        manifest.signed_extensions.swap(0, 1);
        write_json(manifest_path, &manifest).unwrap();

        let error = validate_written_fixture_set(&fixture_dir.0).unwrap_err();

        assert!(error.to_string().contains("signed-extension order"));
    }

    #[rstest]
    fn rejects_header_for_another_block_number() {
        let fixture_dir = write_valid_fixture_set();
        let header_path = fixture_dir.0.join("finalized_header.json");
        let mut header: JsonRpcResponse<BlockHeader> = read_json(&header_path).unwrap();
        header.result.number = "0x2b".to_string();
        let bytes = write_json(header_path, &header).unwrap();
        let manifest_path = fixture_dir.0.join("manifest.json");
        let mut manifest: FixtureManifest = read_json(&manifest_path).unwrap();
        fixture_record_mut(&mut manifest, "finalized_header.json").bytes = bytes;
        write_json(manifest_path, &manifest).unwrap();

        let error = validate_written_fixture_set(&fixture_dir.0).unwrap_err();

        assert!(error.to_string().contains("header number"));
    }

    #[rstest]
    fn rejects_another_block_hash_at_header_height() {
        let fixture_dir = write_valid_fixture_set();
        let block_hash_path = fixture_dir.0.join("finalized_block_hash.json");
        let mut block_hash: JsonRpcResponse<String> = read_json(&block_hash_path).unwrap();
        block_hash.result = format!("0x{}", "ff".repeat(32));
        let bytes = write_json(block_hash_path, &block_hash).unwrap();
        let manifest_path = fixture_dir.0.join("manifest.json");
        let mut manifest: FixtureManifest = read_json(&manifest_path).unwrap();
        fixture_record_mut(&mut manifest, "finalized_block_hash.json").bytes = bytes;
        write_json(manifest_path, &manifest).unwrap();

        let error = validate_written_fixture_set(&fixture_dir.0).unwrap_err();

        assert!(error.to_string().contains("header hash"));
    }

    #[rstest]
    fn rejects_tampered_fixture_response_envelope() {
        let fixture_dir = write_valid_fixture_set();
        let runtime_path = fixture_dir.0.join("runtime_version.json");
        let mut runtime: JsonRpcResponse<RuntimeVersion> = read_json(&runtime_path).unwrap();
        runtime.id = 2;
        let bytes = write_json(runtime_path, &runtime).unwrap();
        let manifest_path = fixture_dir.0.join("manifest.json");
        let mut manifest: FixtureManifest = read_json(&manifest_path).unwrap();
        fixture_record_mut(&mut manifest, "runtime_version.json").bytes = bytes;
        write_json(manifest_path, &manifest).unwrap();

        let error = validate_written_fixture_set(&fixture_dir.0).unwrap_err();

        assert!(error.to_string().contains("unexpected request ID"));
    }

    #[rstest]
    fn publishes_valid_fixture_set_atomically() {
        let staging_dir = write_valid_fixture_set();
        let fixture_dir = staging_dir.0.with_extension("published");

        publish_fixture_set(&staging_dir.0, &fixture_dir).unwrap();

        assert!(!staging_dir.0.exists());
        assert!(fixture_dir.join("manifest.json").is_file());
        fs::remove_dir_all(fixture_dir).unwrap();
    }

    #[rstest]
    fn cleans_staging_directory_when_validation_fails() {
        let staging_dir = write_valid_fixture_set();
        fs::remove_file(staging_dir.0.join("metadata.json")).unwrap();
        let fixture_dir = staging_dir.0.with_extension("published");

        let error = publish_fixture_set(&staging_dir.0, &fixture_dir).unwrap_err();

        assert!(error.to_string().contains("metadata.json"));
        assert!(!staging_dir.0.exists());
        assert!(!fixture_dir.exists());
    }

    fn fixture_record_mut<'a>(
        manifest: &'a mut FixtureManifest,
        payload_path: &str,
    ) -> &'a mut FixtureRecord {
        manifest
            .fixtures
            .iter_mut()
            .find(|fixture| fixture.payload_path == payload_path)
            .unwrap()
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

    #[rstest]
    #[case("0x0", 0)]
    #[case("0x2a", 42)]
    #[case("0xffffffffffffffff", u64::MAX)]
    fn parses_hex_finalized_block_number(#[case] value: &str, #[case] expected: u64) {
        assert_eq!(parse_block_number(value).unwrap(), expected);
    }

    #[rstest]
    #[case("")]
    #[case("42")]
    #[case("0x")]
    #[case("0xgg")]
    #[case("0x10000000000000000")]
    fn rejects_invalid_finalized_block_number(#[case] value: &str) {
        assert!(parse_block_number(value).is_err());
    }

    #[tokio::test]
    async fn finalized_header_request_is_bound_to_captured_hash() {
        async fn handler(Json(payload): Json<Value>) -> Json<Value> {
            assert_eq!(payload["method"], "chain_getHeader");
            assert_eq!(payload["params"], json!([FINALIZED_HASH]));
            Json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "parentHash": "0x01",
                    "number": "0x2a",
                    "stateRoot": "0x02",
                    "extrinsicsRoot": "0x03",
                    "digest": { "logs": [] }
                }
            }))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/", post(handler)))
                .await
                .unwrap();
        });

        let response = rpc::<BlockHeader>(
            &Client::new(),
            &format!("http://{address}"),
            "chain_getHeader",
            json!([FINALIZED_HASH]),
        )
        .await
        .unwrap();

        assert_eq!(parse_block_number(&response.result.number).unwrap(), 42);
    }

    #[rstest]
    #[case("1.0", 1, "unexpected JSON-RPC version: 1.0")]
    #[case("2.0", 2, "unexpected request ID: 2")]
    #[tokio::test]
    async fn rejects_invalid_json_rpc_envelope(
        #[case] jsonrpc: &'static str,
        #[case] id: u64,
        #[case] expected_error: &str,
    ) {
        let router = Router::new().route(
            "/",
            post(move || async move {
                Json(json!({
                    "jsonrpc": jsonrpc,
                    "id": id,
                    "result": "valid-result"
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let error = rpc::<String>(
            &Client::new(),
            &format!("http://{address}"),
            "test_method",
            json!([]),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains(expected_error));
    }
}
