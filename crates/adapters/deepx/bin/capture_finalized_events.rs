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

//! Capture complete event storage from a finalized DeepX testnet spec-376 block.
//!
//! Run with:
//! `cargo run -p nautilus-deepx --bin deepx-capture-finalized-events -- --block-number <number>`

use std::{env, fs, path::PathBuf};

use anyhow::{Context, ensure};
use aws_lc_rs::digest;
use jiff::{Timestamp, tz::Offset};
use parity_scale_codec::{Compact, Decode, Encode};
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const DEFAULT_RPC_URL: &str = "https://rpc-testnet.deepx.fi";
const RPC_URL_ENV: &str = "DEEPX_TESTNET_RPC_URL";
const EXPECTED_GENESIS_HASH: &str =
    "0x87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7";
const EXPECTED_METADATA_SHA256: &str =
    "57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447";
const EXPECTED_SPEC_VERSION: u32 = 376;
const EXPECTED_TRANSACTION_VERSION: u32 = 1;
const SYSTEM_EVENTS_STORAGE_KEY: &str = concat!(
    "0x26aa394eea5630e07c48ae0c9558cef7",
    "80d41e5e16056765bc8461851072c9d7",
);

#[derive(Debug, Deserialize, Serialize)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    id: u64,
    result: T,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeVersion {
    spec_version: u32,
    transaction_version: u32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockHeader {
    number: String,
}

#[derive(Debug, Serialize)]
struct RuntimeIdentity {
    genesis_hash: &'static str,
    metadata_sha256: &'static str,
    spec_version: u32,
    transaction_version: u32,
}

#[derive(Debug, Serialize)]
struct StorageManifest {
    layout: &'static str,
    standard_events_key: &'static str,
    threads_key: Option<String>,
    events_map_keys: Vec<String>,
    event_count: u32,
    reassembled_bytes: usize,
    reassembled_sha256: String,
}

#[derive(Debug, Serialize)]
struct FixtureManifest {
    captured_at: String,
    deployment: &'static str,
    endpoint_role: &'static str,
    rpc_url: String,
    block_hash: String,
    block_number: u64,
    finalized_head_hash_at_capture: String,
    finalized_head_number_at_capture: u64,
    runtime_identity: RuntimeIdentity,
    storage: StorageManifest,
    rpc_payloads: Vec<String>,
    reassembled_payload_path: &'static str,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (block_number, output_root) = capture_options(env::args().skip(1))?;
    let rpc_url = env::var(RPC_URL_ENV).unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
    let client = Client::new();

    let genesis = rpc::<String>(&client, &rpc_url, "chain_getBlockHash", json!([0])).await?;
    ensure!(
        genesis.result == EXPECTED_GENESIS_HASH,
        "DeepX genesis hash mismatch"
    );
    let finalized_head =
        rpc::<String>(&client, &rpc_url, "chain_getFinalizedHead", json!([])).await?;
    let finalized_header = rpc::<BlockHeader>(
        &client,
        &rpc_url,
        "chain_getHeader",
        json!([finalized_head.result]),
    )
    .await?;
    let finalized_head_number = parse_block_number(&finalized_header.result.number)?;
    ensure!(
        block_number <= finalized_head_number,
        "DeepX block {block_number} is above finalized head {finalized_head_number}",
    );
    let block_hash = rpc::<String>(
        &client,
        &rpc_url,
        "chain_getBlockHash",
        json!([block_number]),
    )
    .await?;
    let header = rpc::<BlockHeader>(
        &client,
        &rpc_url,
        "chain_getHeader",
        json!([block_hash.result]),
    )
    .await?;
    ensure!(
        parse_block_number(&header.result.number)? == block_number,
        "DeepX canonical header number mismatch",
    );
    let runtime = rpc::<RuntimeVersion>(
        &client,
        &rpc_url,
        "state_getRuntimeVersion",
        json!([block_hash.result]),
    )
    .await?;
    ensure!(
        runtime.result.spec_version == EXPECTED_SPEC_VERSION
            && runtime.result.transaction_version == EXPECTED_TRANSACTION_VERSION,
        "DeepX block does not use the approved read-only runtime identity",
    );
    let metadata = rpc::<String>(
        &client,
        &rpc_url,
        "state_getMetadata",
        json!([block_hash.result]),
    )
    .await?;
    let metadata_bytes = decode_hex(&metadata.result)?;
    ensure!(
        sha256(&metadata_bytes) == EXPECTED_METADATA_SHA256,
        "DeepX metadata hash mismatch",
    );

    let fixture_dir = output_root.join(format!("finalized-events-{block_number}-spec376"));
    ensure!(
        !fixture_dir.exists(),
        "DeepX event fixture set already exists and is immutable: {}",
        fixture_dir.display(),
    );
    let staging_dir = fixture_dir.with_extension(format!("partial-{}", std::process::id()));
    ensure!(
        !staging_dir.exists(),
        "DeepX event staging directory already exists"
    );
    fs::create_dir_all(&staging_dir)?;

    let capture = capture_event_storage(&client, &rpc_url, block_number, &block_hash.result).await;
    let (events, storage, payloads) = match capture {
        Ok(capture) => capture,
        Err(error) => {
            fs::remove_dir_all(&staging_dir)?;
            return Err(error);
        }
    };
    for (name, payload) in &payloads {
        write_json(staging_dir.join(name), payload)?;
    }
    fs::write(
        staging_dir.join("reassembled_events.hex"),
        format!("0x{}\n", nautilus_core::hex::encode(&events)),
    )?;
    let manifest = FixtureManifest {
        captured_at: Timestamp::now()
            .display_with_offset(Offset::UTC)
            .to_string(),
        deployment: "testnet",
        endpoint_role: "finalized_event_storage",
        rpc_url,
        block_hash: block_hash.result,
        block_number,
        finalized_head_hash_at_capture: finalized_head.result,
        finalized_head_number_at_capture: finalized_head_number,
        runtime_identity: RuntimeIdentity {
            genesis_hash: EXPECTED_GENESIS_HASH,
            metadata_sha256: EXPECTED_METADATA_SHA256,
            spec_version: EXPECTED_SPEC_VERSION,
            transaction_version: EXPECTED_TRANSACTION_VERSION,
        },
        storage,
        rpc_payloads: payloads.into_iter().map(|(name, _)| name).collect(),
        reassembled_payload_path: "reassembled_events.hex",
    };
    write_json(staging_dir.join("manifest.json"), &manifest)?;
    fs::rename(&staging_dir, &fixture_dir)?;
    println!(
        "Captured DeepX finalized events under {}",
        fixture_dir.display()
    );
    Ok(())
}

type Capture = (Vec<u8>, StorageManifest, Vec<(String, Value)>);

async fn capture_event_storage(
    client: &Client,
    rpc_url: &str,
    block_number: u64,
    block_hash: &str,
) -> anyhow::Result<Capture> {
    let standard = storage_rpc(client, rpc_url, SYSTEM_EVENTS_STORAGE_KEY, block_hash).await?;
    let mut payloads = vec![(
        "system_events.json".to_string(),
        serde_json::to_value(&standard)?,
    )];
    if let Some(encoded) = standard.result {
        let events = decode_hex(&encoded)?;
        let (repeated, query) = verify_query_storage(
            client,
            rpc_url,
            SYSTEM_EVENTS_STORAGE_KEY,
            block_hash,
            &encoded,
        )
        .await?;
        payloads.extend([
            ("system_events_repeated.json".to_string(), repeated),
            ("system_events_query_storage_at.json".to_string(), query),
        ]);
        let event_count = event_count(&events)?;
        let storage = storage_manifest("System.Events", None, vec![], event_count, &events);
        return Ok((events, storage, payloads));
    }

    let threads_key = system_threads_storage_key(block_number);
    let threads = storage_rpc(client, rpc_url, &threads_key, block_hash).await?;
    let encoded_threads = threads
        .result
        .as_deref()
        .context("DeepX System.Threads is unavailable")?;
    let (repeated_threads, query_threads) =
        verify_query_storage(client, rpc_url, &threads_key, block_hash, encoded_threads).await?;
    let thread_bytes = decode_hex(encoded_threads)?;
    let [last_thread] = thread_bytes.as_slice() else {
        anyhow::bail!("DeepX System.Threads value is malformed");
    };
    payloads.push((
        "system_threads.json".to_string(),
        serde_json::to_value(&threads)?,
    ));
    payloads.extend([
        ("system_threads_repeated.json".to_string(), repeated_threads),
        (
            "system_threads_query_storage_at.json".to_string(),
            query_threads,
        ),
    ]);

    let mut batches = Vec::with_capacity(usize::from(*last_thread) + 1);
    let mut events_map_keys = Vec::with_capacity(batches.capacity());
    for thread in 0..=*last_thread {
        let key = system_events_map_storage_key(block_number, thread);
        let response = storage_rpc(client, rpc_url, &key, block_hash).await?;
        let encoded = response
            .result
            .as_deref()
            .context("DeepX System.EventsMap is unavailable")?;
        let (repeated, query) =
            verify_query_storage(client, rpc_url, &key, block_hash, encoded).await?;
        batches.push(decode_hex(encoded)?);
        events_map_keys.push(key);
        payloads.push((
            format!("events_map_thread_{thread}.json"),
            serde_json::to_value(&response)?,
        ));
        payloads.extend([
            (
                format!("events_map_thread_{thread}_repeated.json"),
                repeated,
            ),
            (
                format!("events_map_thread_{thread}_query_storage_at.json"),
                query,
            ),
        ]);
    }
    let events = combine_events_map_batches(&batches)?;
    let event_count = event_count(&events)?;
    let storage = storage_manifest(
        "System.Threads+System.EventsMap",
        Some(threads_key),
        events_map_keys,
        event_count,
        &events,
    );
    Ok((events, storage, payloads))
}

async fn storage_rpc(
    client: &Client,
    rpc_url: &str,
    key: &str,
    block_hash: &str,
) -> anyhow::Result<JsonRpcResponse<Option<String>>> {
    rpc(
        client,
        rpc_url,
        "state_getStorage",
        json!([key, block_hash]),
    )
    .await
}

async fn verify_query_storage(
    client: &Client,
    rpc_url: &str,
    key: &str,
    block_hash: &str,
    expected: &str,
) -> anyhow::Result<(Value, Value)> {
    let repeated = storage_rpc(client, rpc_url, key, block_hash).await?;
    ensure!(
        repeated.result.as_deref() == Some(expected),
        "repeated storage read disagreed"
    );
    let query = rpc::<Value>(
        client,
        rpc_url,
        "state_queryStorageAt",
        json!([[key], block_hash]),
    )
    .await?;
    ensure!(
        query.result[0]["block"] == block_hash
            && query.result[0]["changes"][0][0] == key
            && query.result[0]["changes"][0][1] == expected,
        "state_queryStorageAt disagreed with state_getStorage",
    );
    Ok((
        serde_json::to_value(repeated)?,
        serde_json::to_value(query)?,
    ))
}

fn storage_manifest(
    layout: &'static str,
    threads_key: Option<String>,
    events_map_keys: Vec<String>,
    event_count: u32,
    events: &[u8],
) -> StorageManifest {
    StorageManifest {
        layout,
        standard_events_key: SYSTEM_EVENTS_STORAGE_KEY,
        threads_key,
        events_map_keys,
        event_count,
        reassembled_bytes: events.len(),
        reassembled_sha256: sha256(events),
    }
}

fn system_threads_storage_key(block_number: u64) -> String {
    system_map_storage_key("Threads", &block_number.to_le_bytes())
}

fn system_map_storage_key(item: &str, key: &[u8]) -> String {
    let mut encoded = Vec::with_capacity(32 + key.len());
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(b"System"));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(item.as_bytes()));
    encoded.extend_from_slice(key);
    format!("0x{}", nautilus_core::hex::encode(encoded))
}

fn system_events_map_storage_key(block_number: u64, thread: u8) -> String {
    let mut encoded = Vec::with_capacity(64);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(b"System"));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(b"EventsMap"));
    encoded.extend_from_slice(&sp_crypto_hashing::blake2_128(&block_number.to_le_bytes()));
    encoded.extend_from_slice(&sp_crypto_hashing::blake2_128(&[thread]));
    format!("0x{}", nautilus_core::hex::encode(encoded))
}

fn combine_events_map_batches(batches: &[Vec<u8>]) -> anyhow::Result<Vec<u8>> {
    let mut records = Vec::new();
    let mut total = 0_u32;
    for batch in batches {
        let mut input = batch.as_slice();
        let count = Compact::<u32>::decode(&mut input)
            .context("invalid event batch count")?
            .0;
        ensure!(
            !input.is_empty() || count == 0,
            "event batch count has no records"
        );
        ensure!(
            input.is_empty() || count != 0,
            "nonempty event batch has zero count"
        );
        total = total.checked_add(count).context("event count overflow")?;
        records.extend_from_slice(input);
    }
    let mut combined = Compact(total).encode();
    combined.extend_from_slice(&records);
    Ok(combined)
}

fn event_count(events: &[u8]) -> anyhow::Result<u32> {
    let mut input = events;
    Ok(Compact::<u32>::decode(&mut input)
        .context("invalid complete event count")?
        .0)
}

fn capture_options(args: impl Iterator<Item = String>) -> anyhow::Result<(u64, PathBuf)> {
    let mut args = args;
    let mut block_number = None;
    let mut output_root = None;
    while let Some(option) = args.next() {
        match option.as_str() {
            "--block-number" if block_number.is_none() => {
                block_number = Some(
                    args.next()
                        .context("--block-number requires a value")?
                        .parse()?,
                );
            }
            "--output-root" if output_root.is_none() => {
                let path = PathBuf::from(args.next().context("--output-root requires a path")?);
                ensure!(
                    path.is_absolute(),
                    "--output-root requires an absolute path"
                );
                output_root = Some(path);
            }
            _ => anyhow::bail!("unknown or repeated DeepX event capture option: {option}"),
        }
    }
    Ok((
        block_number.context("--block-number is required")?,
        output_root.unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/runtime/testnet")
        }),
    ))
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
        .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("Failed to decode DeepX RPC response for {method}"))?;
    ensure!(
        response.jsonrpc == "2.0" && response.id == 1,
        "invalid JSON-RPC envelope"
    );
    Ok(response)
}

fn decode_hex(value: &str) -> anyhow::Result<Vec<u8>> {
    nautilus_core::hex::decode(
        value
            .strip_prefix("0x")
            .context("hex value must start with 0x")?,
    )
    .context("invalid hex value")
}

fn sha256(bytes: &[u8]) -> String {
    nautilus_core::hex::encode(digest::digest(&digest::SHA256, bytes).as_ref())
}

fn parse_block_number(value: &str) -> anyhow::Result<u64> {
    u64::from_str_radix(
        value
            .strip_prefix("0x")
            .context("block number must start with 0x")?,
        16,
    )
    .context("invalid block number")
}

fn write_json(path: PathBuf, value: &impl Serialize) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(path, bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn combines_split_event_batches() {
        let first = [Compact(1_u32).encode(), vec![10]].concat();
        let second = [Compact(2_u32).encode(), vec![20, 30]].concat();

        assert_eq!(
            combine_events_map_batches(&[first, second]).unwrap(),
            [Compact(3_u32).encode(), vec![10, 20, 30]].concat(),
        );
    }

    #[rstest]
    fn derives_known_split_storage_keys() {
        assert_eq!(
            system_threads_storage_key(187_071_212),
            "0x26aa394eea5630e07c48ae0c9558cef79cb5c9f400ab41399c6b8b51f8d18819ec7a260b00000000",
        );
        assert_eq!(
            system_events_map_storage_key(187_071_212, 0),
            "0x26aa394eea5630e07c48ae0c9558cef731d080e4d67db2640c561142dcdc206531de4920507d6e7f25adf620bb0e67f37025e075d5e2f6cde3cc051a31f07660",
        );
    }

    #[rstest]
    fn requires_explicit_block_number() {
        assert!(capture_options(std::iter::empty()).is_err());
        let (block, root) = capture_options(
            [
                "--block-number",
                "19592700",
                "--output-root",
                "/tmp/deepx-events",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert_eq!(block, 19_592_700);
        assert_eq!(root, PathBuf::from("/tmp/deepx-events"));
    }
}
