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

//! Exact EVM ABI calls used by the DeepX MultiTokenBridge example.

use std::time::{Duration, Instant};

use alloy::{
    primitives::{Address, B256, Bytes, FixedBytes, U256},
    sol,
    sol_types::{SolCall, SolValue},
};
use anyhow::{Context, ensure};
use serde::Deserialize;
use serde_json::json;

use super::DeepXRpcClient;
use crate::signing::DeepXOfflineEvmTransaction;

const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);

sol! {
    function getBridgeFee(
        uint256 tokenId,
        uint32 dstChainId,
        uint256 amount,
        bytes32 dstRecipient,
        bytes customData
    ) external view returns (uint256);

    function tokenIdToInfo(uint256 tokenId) external view returns (
        bool isNativeAsset,
        bool isGasToken,
        address token,
        uint8 commonDecimal,
        uint8 localDecimal,
        bool isConfigFixed
    );

    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);

    function bridgeOut(
        uint32 dstChainId,
        uint256 tokenId,
        uint256 amount,
        bytes32 dstRecipient,
        address refundAddress,
        bytes32 salt,
        bytes customData,
        bytes signature
    ) external;
}

/// Token registration and decimal metadata returned by a bridge contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXBridgeTokenInfo {
    /// Whether the registered asset is native to its source chain.
    pub is_native_asset: bool,
    /// Whether the asset pays the bridge amount through transaction value.
    pub is_gas_token: bool,
    /// Source-chain token address; zero means the ID is unregistered.
    pub token: Address,
    /// Shared bridge decimal precision.
    pub common_decimal: u8,
    /// Local token decimal precision.
    pub local_decimal: u8,
    /// Whether the token configuration is fixed.
    pub is_config_fixed: bool,
}

/// Exact arguments for one `bridgeOut` contract call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBridgeOutParams {
    /// Destination EVM chain ID encoded as a contract `uint32`.
    pub destination_chain_id: u32,
    /// Registered bridge token ID.
    pub token_id: U256,
    /// Source token amount in its smallest units.
    pub amount: U256,
    /// Destination recipient, left-padded to `bytes32` for an EVM address.
    pub destination_recipient: FixedBytes<32>,
    /// Source address to receive a refund if bridging fails.
    pub refund_address: Address,
    /// Unique authorization salt; the caller must generate a fresh value.
    pub salt: FixedBytes<32>,
    /// Optional relayer payload.
    pub custom_data: Bytes,
    /// Signature returned by the bridge authorizer for these exact terms.
    pub signature: Bytes,
}

/// Exact terms used for one bridge fee quotation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBridgeFeeParams {
    /// Registered token ID.
    pub token_id: U256,
    /// Destination EVM chain ID.
    pub destination_chain_id: u32,
    /// Source amount in raw units.
    pub amount: U256,
    /// Destination recipient encoded as `bytes32`.
    pub destination_recipient: FixedBytes<32>,
    /// Optional relayer payload.
    pub custom_data: Bytes,
}

/// Destination event terms used to identify a bridge arrival.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBridgeArrivalFilter {
    /// Destination bridge contract.
    pub bridge: Address,
    /// Destination EVM recipient.
    pub recipient: Address,
    /// Earliest destination block to inspect.
    pub from_block: u64,
    /// Registered token ID.
    pub token_id: U256,
    /// Delivered amount in raw event units.
    pub amount: U256,
    /// Optional bridge protocol ID linking the source and destination.
    pub expected_bridge_transaction_id: Option<B256>,
    /// Number of canonical destination blocks required after inclusion.
    pub confirmations: u64,
}

/// Confirmed source-chain receipt for one exact bridge or approval transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBridgeEvmReceipt {
    /// Exact EVM transaction hash.
    pub tx_hash: B256,
    /// Canonical source block hash at confirmation time.
    pub block_hash: B256,
    /// Source block height.
    pub block_number: u64,
}

/// One destination bridge arrival emitted for an exact recipient.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXBridgeInEvent {
    /// Destination transaction hash carrying the event.
    pub transaction_hash: B256,
    /// Destination block hash carrying the event.
    pub block_hash: B256,
    /// Destination block height.
    pub block_number: u64,
    /// Bridge protocol identifier, used for source-to-destination reconciliation.
    pub bridge_transaction_id: B256,
    /// Registered token ID.
    pub token_id: U256,
    /// Delivered amount in the event's raw units.
    pub amount: U256,
}

/// Encodes the bridge's exact fee quotation call.
#[must_use]
pub fn encode_bridge_fee_call(
    token_id: U256,
    destination_chain_id: u32,
    amount: U256,
    destination_recipient: FixedBytes<32>,
    custom_data: Bytes,
) -> Vec<u8> {
    getBridgeFeeCall {
        tokenId: token_id,
        dstChainId: destination_chain_id,
        amount,
        dstRecipient: destination_recipient,
        customData: custom_data,
    }
    .abi_encode()
}

/// Encodes one exact ERC-20 approval for the bridge as spender.
#[must_use]
pub fn encode_bridge_approve_call(bridge: Address, amount: U256) -> Vec<u8> {
    approveCall {
        spender: bridge,
        amount,
    }
    .abi_encode()
}

/// Encodes one bridge-out call using exact authorizer-approved terms.
#[must_use]
pub fn encode_bridge_out_call(params: &DeepXBridgeOutParams) -> Vec<u8> {
    bridgeOutCall {
        dstChainId: params.destination_chain_id,
        tokenId: params.token_id,
        amount: params.amount,
        dstRecipient: params.destination_recipient,
        refundAddress: params.refund_address,
        salt: params.salt,
        customData: params.custom_data.clone(),
        signature: params.signature.clone(),
    }
    .abi_encode()
}

/// Reads the exact bridge fee at a selected source-chain EVM block.
///
/// # Errors
///
/// Returns an error for RPC failure or malformed contract response.
pub async fn get_bridge_fee_at_block(
    rpc: &DeepXRpcClient,
    bridge: Address,
    params: &DeepXBridgeFeeParams,
    block: &str,
) -> anyhow::Result<U256> {
    let call = encode_bridge_fee_call(
        params.token_id,
        params.destination_chain_id,
        params.amount,
        params.destination_recipient,
        params.custom_data.clone(),
    );
    let response = read_call(rpc, bridge, &call, block).await?;
    let fee = getBridgeFeeCall::abi_decode_returns(&response)
        .context("invalid bridge fee ABI response")?;
    ensure!(
        fee.abi_encode() == response,
        "noncanonical bridge fee response"
    );
    Ok(fee)
}

/// Reads a token registration at a selected source-chain EVM block.
///
/// # Errors
///
/// Returns an error for RPC failure or malformed contract response.
pub async fn get_bridge_token_info_at_block(
    rpc: &DeepXRpcClient,
    bridge: Address,
    token_id: U256,
    block: &str,
) -> anyhow::Result<DeepXBridgeTokenInfo> {
    let call = tokenIdToInfoCall { tokenId: token_id }.abi_encode();
    let response = read_call(rpc, bridge, &call, block).await?;
    let info = tokenIdToInfoCall::abi_decode_returns(&response)
        .context("invalid bridge token ABI response")?;
    ensure!(
        tokenIdToInfoCall::abi_encode_returns(&info) == response,
        "noncanonical bridge token response"
    );
    Ok(DeepXBridgeTokenInfo {
        is_native_asset: info.isNativeAsset,
        is_gas_token: info.isGasToken,
        token: info.token,
        common_decimal: info.commonDecimal,
        local_decimal: info.localDecimal,
        is_config_fixed: info.isConfigFixed,
    })
}

/// Reads the owner's ERC-20 allowance for the selected bridge at one EVM block.
///
/// # Errors
///
/// Returns an error for RPC failure or malformed token response.
pub async fn get_bridge_allowance_at_block(
    rpc: &DeepXRpcClient,
    token: Address,
    owner: Address,
    bridge: Address,
    block: &str,
) -> anyhow::Result<U256> {
    let call = allowanceCall {
        owner,
        spender: bridge,
    }
    .abi_encode();
    let response = read_call(rpc, token, &call, block).await?;
    let allowance = allowanceCall::abi_decode_returns(&response)
        .context("invalid bridge allowance ABI response")?;
    ensure!(
        allowance.abi_encode() == response,
        "noncanonical bridge allowance response"
    );
    Ok(allowance)
}

/// Reads the source or destination EVM chain ID from its RPC endpoint.
///
/// # Errors
///
/// Returns an error for RPC failure or a malformed chain ID.
pub async fn get_evm_chain_id(rpc: &DeepXRpcClient) -> anyhow::Result<u64> {
    let value: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": [],
        }))
        .await?;
    parse_hex_u64(&value).context("invalid EVM chain ID")
}

/// Reads the current best EVM block number.
///
/// # Errors
///
/// Returns an error for RPC failure or a malformed block number.
pub async fn get_evm_block_number(rpc: &DeepXRpcClient) -> anyhow::Result<u64> {
    let value: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": [],
        }))
        .await?;
    parse_hex_u64(&value).context("invalid EVM block number")
}

/// Reads an EVM account's next pending sequential nonce.
///
/// # Errors
///
/// Returns an error for RPC failure or a malformed account nonce.
pub async fn get_pending_evm_nonce(rpc: &DeepXRpcClient, account: Address) -> anyhow::Result<u64> {
    let value: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionCount",
            "params": [account.to_string(), "pending"],
        }))
        .await?;
    parse_hex_u64(&value).context("invalid pending EVM nonce")
}

/// Submits exact raw EVM bytes once and verifies the RPC returned their Keccak hash.
///
/// A transport error after sending is uncertain; reconcile by the returned signed hash before
/// preparing a replacement or advancing the nonce.
///
/// # Errors
///
/// Returns an error for RPC failure or a mismatched submission hash.
pub async fn submit_bridge_evm_transaction(
    rpc: &DeepXRpcClient,
    signed: &DeepXOfflineEvmTransaction,
) -> anyhow::Result<B256> {
    let hash = B256::from(signed.tx_hash);
    let returned: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction",
            "params": [format!("0x{}", hex::encode(&signed.bytes))],
        }))
        .await
        .with_context(|| format!("EVM submission uncertain; reconcile hash={hash}"))?;
    let returned = returned
        .parse::<B256>()
        .context("invalid EVM submission hash")?;
    ensure!(
        returned == hash,
        "EVM submission hash differs from signed bytes"
    );
    Ok(hash)
}

/// Waits for a successful receipt and a canonical block with the requested confirmations.
///
/// A timeout leaves the transaction outcome uncertain. The caller reconciles by hash and nonce.
///
/// # Errors
///
/// Returns an error for timeout, RPC failure, failed execution, reorg, or mismatched identities.
pub async fn wait_bridge_evm_receipt(
    rpc: &DeepXRpcClient,
    tx_hash: B256,
    sender: Address,
    contract: Address,
    confirmations: u64,
    timeout: Duration,
) -> anyhow::Result<DeepXBridgeEvmReceipt> {
    ensure!(confirmations > 0, "EVM confirmations must be positive");
    let deadline = Instant::now()
        .checked_add(timeout)
        .context("EVM receipt timeout overflow")?;
    loop {
        let receipt: Option<EvmReceiptWire> = rpc
            .execute_optional_rpc_call(json!({
                "jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt",
                "params": [tx_hash.to_string()],
            }))
            .await?;

        if let Some(receipt) = receipt {
            ensure!(
                receipt.transaction_hash == tx_hash,
                "EVM receipt hash mismatch"
            );
            ensure!(receipt.from == sender, "EVM receipt sender mismatch");
            ensure!(
                receipt.to == Some(contract),
                "EVM receipt contract mismatch"
            );
            ensure!(
                parse_hex_u64(&receipt.status) == Some(1),
                "EVM transaction failed"
            );
            let height =
                parse_hex_u64(&receipt.block_number).context("invalid EVM receipt block number")?;
            let best: String = rpc
                .execute_rpc_call(json!({
                    "jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": [],
                }))
                .await?;
            let best = parse_hex_u64(&best).context("invalid EVM best block number")?;
            let confirmed_at = height
                .checked_add(confirmations - 1)
                .context("EVM confirmation height overflow")?;

            if best >= confirmed_at {
                let canonical: Option<EvmBlockWire> = rpc
                    .execute_optional_rpc_call(json!({
                        "jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber",
                        "params": [format!("0x{height:x}"), false],
                    }))
                    .await?;
                let canonical = canonical.context("EVM receipt block disappeared")?;
                ensure!(
                    canonical.hash == receipt.block_hash,
                    "EVM receipt block was reorged; reconcile hash={tx_hash}"
                );
                return Ok(DeepXBridgeEvmReceipt {
                    tx_hash,
                    block_hash: receipt.block_hash,
                    block_number: height,
                });
            }
        }
        ensure!(
            Instant::now() < deadline,
            "EVM receipt timed out; reconcile hash={tx_hash}"
        );
        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
}

/// Reads destination `BridgeIn` logs for one bridge contract and recipient.
///
/// The caller selects `from_block` before source submission to avoid missing an arrival.
///
/// # Errors
///
/// Returns an error for RPC failure or an unexpected event layout.
pub async fn get_bridge_in_logs(
    rpc: &DeepXRpcClient,
    bridge: Address,
    recipient: Address,
    from_block: u64,
) -> anyhow::Result<Vec<DeepXBridgeInEvent>> {
    let topic = alloy::primitives::keccak256("BridgeIn(bytes32,uint256,uint256,address)");
    let mut recipient_topic = [0_u8; 32];
    recipient_topic[12..].copy_from_slice(recipient.as_slice());
    let recipient_topic = B256::from(recipient_topic);
    let logs: Vec<BridgeInLogWire> = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
            "params": [{
                "address": bridge,
                "topics": [topic, recipient_topic],
                "fromBlock": format!("0x{from_block:x}"),
                "toBlock": "latest",
            }],
        }))
        .await?;
    logs.into_iter()
        .map(|log| decode_bridge_in_log(&log, bridge, topic, recipient_topic))
        .collect()
}

/// Returns the first matching arrival after `from_block`, optionally requiring its bridge ID.
///
/// Without an expected bridge ID, matching recipient, token, and amount alone does not prove
/// that this arrival belongs to the submitted source transaction.
///
/// # Errors
///
/// Returns an error for timeout, RPC failure, or malformed matching events.
pub async fn wait_bridge_in(
    rpc: &DeepXRpcClient,
    filter: &DeepXBridgeArrivalFilter,
    timeout: Duration,
) -> anyhow::Result<DeepXBridgeInEvent> {
    ensure!(
        filter.confirmations > 0,
        "bridge confirmations must be positive"
    );
    let deadline = Instant::now()
        .checked_add(timeout)
        .context("bridge arrival timeout overflow")?;

    loop {
        let logs =
            get_bridge_in_logs(rpc, filter.bridge, filter.recipient, filter.from_block).await?;
        if let Some(event) = logs.into_iter().find(|event| {
            event.token_id == filter.token_id
                && event.amount == filter.amount
                && filter
                    .expected_bridge_transaction_id
                    .is_none_or(|expected| event.bridge_transaction_id == expected)
        }) {
            let best = get_evm_block_number(rpc).await?;
            let confirmed_at = event
                .block_number
                .checked_add(filter.confirmations - 1)
                .context("bridge confirmation height overflow")?;

            if best >= confirmed_at {
                let canonical: Option<EvmBlockWire> = rpc
                    .execute_optional_rpc_call(json!({
                        "jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber",
                        "params": [format!("0x{:x}", event.block_number), false],
                    }))
                    .await?;
                let canonical = canonical.context("BridgeIn block disappeared")?;
                ensure!(
                    canonical.hash == event.block_hash,
                    "BridgeIn block was reorged; reconcile destination transaction"
                );
                return Ok(event);
            }
        }
        ensure!(Instant::now() < deadline, "destination BridgeIn timed out");
        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BridgeInLogWire {
    address: Address,
    topics: Vec<B256>,
    data: String,
    transaction_hash: B256,
    block_hash: B256,
    block_number: String,
    #[serde(default)]
    removed: bool,
}

fn decode_bridge_in_log(
    log: &BridgeInLogWire,
    bridge: Address,
    topic: B256,
    recipient_topic: B256,
) -> anyhow::Result<DeepXBridgeInEvent> {
    ensure!(log.address == bridge, "BridgeIn contract address mismatch");
    ensure!(
        log.topics.as_slice() == [topic, recipient_topic],
        "BridgeIn event topics mismatch"
    );
    ensure!(!log.removed, "BridgeIn event was removed from the chain");
    let bytes = log
        .data
        .strip_prefix("0x")
        .and_then(|data| hex::decode(data).ok())
        .context("invalid BridgeIn event data")?;
    let values: (B256, U256, U256) =
        SolValue::abi_decode(&bytes).context("invalid BridgeIn ABI data")?;
    ensure!(
        values.abi_encode() == bytes,
        "noncanonical BridgeIn ABI data"
    );
    Ok(DeepXBridgeInEvent {
        transaction_hash: log.transaction_hash,
        block_hash: log.block_hash,
        block_number: parse_hex_u64(&log.block_number).context("invalid BridgeIn block number")?,
        bridge_transaction_id: values.0,
        token_id: values.1,
        amount: values.2,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvmReceiptWire {
    transaction_hash: B256,
    block_hash: B256,
    block_number: String,
    status: String,
    from: Address,
    to: Option<Address>,
}

#[derive(Deserialize)]
struct EvmBlockWire {
    hash: B256,
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    let value = value.strip_prefix("0x")?;
    (!value.is_empty() && value.len() <= 16)
        .then(|| u64::from_str_radix(value, 16).ok())
        .flatten()
}

async fn read_call(
    rpc: &DeepXRpcClient,
    contract: Address,
    call: &[u8],
    block: &str,
) -> anyhow::Result<Vec<u8>> {
    let encoded = rpc.eth_call(&contract.to_string(), call, block).await?;
    let response = encoded
        .strip_prefix("0x")
        .context("invalid EVM call hex prefix")?;
    hex::decode(response).context("invalid EVM call hex response")
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use axum::{Json, Router, extract::State, routing::post};
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::{
        credential::{DeepXKeyScheme, DeepXPrivateKey},
        signing::{DeepXBridgeEvmTransactionParams, sign_bridge_evm_transaction},
    };

    #[derive(Clone)]
    struct MockEvm {
        signed_hex: String,
        hash: B256,
        signer: Address,
        contract: Address,
        failed: Arc<AtomicBool>,
        wrong_hash: Arc<AtomicBool>,
        reorged: Arc<AtomicBool>,
    }

    async fn evm_rpc(State(state): State<MockEvm>, Json(request): Json<Value>) -> Json<Value> {
        let result = match request["method"].as_str().unwrap() {
            "eth_chainId" => json!("0x12f8"),
            "eth_getTransactionCount" => json!("0x7"),
            "eth_sendRawTransaction" => {
                assert_eq!(request["params"][0], state.signed_hex);
                if state.wrong_hash.load(Ordering::SeqCst) {
                    json!(B256::from([0x99; 32]))
                } else {
                    json!(state.hash)
                }
            }
            "eth_getTransactionReceipt" => json!({
                "transactionHash": state.hash,
                "blockHash": B256::from([0x22; 32]),
                "blockNumber": "0x64",
                "status": if state.failed.load(Ordering::SeqCst) { "0x0" } else { "0x1" },
                "from": state.signer,
                "to": state.contract,
            }),
            "eth_blockNumber" => json!("0x71"),
            "eth_getBlockByNumber" => {
                if state.reorged.load(Ordering::SeqCst) {
                    json!({"hash": B256::from([0x99; 32])})
                } else if request["params"][0] == "0x70" {
                    json!({"hash": B256::from([0x77; 32])})
                } else {
                    json!({"hash": B256::from([0x22; 32])})
                }
            }
            "eth_getLogs" => {
                let topic =
                    alloy::primitives::keccak256("BridgeIn(bytes32,uint256,uint256,address)");
                let mut recipient_topic = [0_u8; 32];
                recipient_topic[12..].copy_from_slice(state.signer.as_slice());
                assert_eq!(request["params"][0]["topics"][0], json!(topic));
                assert_eq!(
                    request["params"][0]["topics"][1],
                    json!(B256::from(recipient_topic))
                );
                json!([{
                    "address": state.contract,
                    "topics": [topic, B256::from(recipient_topic)],
                    "data": format!("0x{}", hex::encode((B256::from([0x55; 32]), U256::from(3), U256::from(1_000_000)).abi_encode())),
                    "transactionHash": B256::from([0x66; 32]),
                    "blockHash": B256::from([0x77; 32]),
                    "blockNumber": "0x70",
                    "removed": false,
                }])
            }
            method => panic!("unexpected EVM RPC method: {method}"),
        };
        Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
    }

    #[tokio::test]
    async fn bridge_rpc_verifies_hash_receipt_and_chain_identity() {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let contract = Address::from([0x11; 20]);
        let signed = sign_bridge_evm_transaction(
            &key,
            &DeepXBridgeEvmTransactionParams {
                chain_id: 4856,
                nonce: 7,
                gas_limit: 200_000,
                max_fee_per_gas: 20_000_000_000,
                max_priority_fee_per_gas: 1_000_000_000,
                contract_address: contract.into_array(),
                value: U256::ZERO,
                calldata: vec![0x12, 0x34],
            },
        )
        .unwrap();
        let failed = Arc::new(AtomicBool::new(false));
        let wrong_hash = Arc::new(AtomicBool::new(false));
        let reorged = Arc::new(AtomicBool::new(false));
        let state = MockEvm {
            signed_hex: format!("0x{}", hex::encode(&signed.bytes)),
            hash: B256::from(signed.tx_hash),
            signer: Address::from(signed.signer),
            contract,
            failed: failed.clone(),
            wrong_hash: wrong_hash.clone(),
            reorged: reorged.clone(),
        };
        let app = Router::new().route("/", post(evm_rpc)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let rpc = DeepXRpcClient::new(format!("http://{address}"));

        assert_eq!(get_evm_chain_id(&rpc).await.unwrap(), 4856);
        assert_eq!(
            get_pending_evm_nonce(&rpc, Address::from(signed.signer))
                .await
                .unwrap(),
            7
        );
        assert_eq!(
            submit_bridge_evm_transaction(&rpc, &signed).await.unwrap(),
            B256::from(signed.tx_hash)
        );
        let receipt = wait_bridge_evm_receipt(
            &rpc,
            B256::from(signed.tx_hash),
            Address::from(signed.signer),
            contract,
            2,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(receipt.block_number, 100);
        let arrival = wait_bridge_in(
            &rpc,
            &DeepXBridgeArrivalFilter {
                bridge: contract,
                recipient: Address::from(signed.signer),
                from_block: 99,
                token_id: U256::from(3),
                amount: U256::from(1_000_000),
                expected_bridge_transaction_id: Some(B256::from([0x55; 32])),
                confirmations: 2,
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(arrival.block_number, 112);
        assert_eq!(arrival.bridge_transaction_id, B256::from([0x55; 32]));
        reorged.store(true, Ordering::SeqCst);
        assert!(
            wait_bridge_evm_receipt(
                &rpc,
                B256::from(signed.tx_hash),
                Address::from(signed.signer),
                contract,
                2,
                Duration::from_secs(1),
            )
            .await
            .is_err()
        );
        assert!(
            wait_bridge_in(
                &rpc,
                &DeepXBridgeArrivalFilter {
                    bridge: contract,
                    recipient: Address::from(signed.signer),
                    from_block: 99,
                    token_id: U256::from(3),
                    amount: U256::from(1_000_000),
                    expected_bridge_transaction_id: Some(B256::from([0x55; 32])),
                    confirmations: 2,
                },
                Duration::from_secs(1),
            )
            .await
            .is_err()
        );
        reorged.store(false, Ordering::SeqCst);
        wrong_hash.store(true, Ordering::SeqCst);
        assert!(submit_bridge_evm_transaction(&rpc, &signed).await.is_err());
        failed.store(true, Ordering::SeqCst);
        assert!(
            wait_bridge_evm_receipt(
                &rpc,
                B256::from(signed.tx_hash),
                Address::from(signed.signer),
                contract,
                1,
                Duration::from_secs(1),
            )
            .await
            .is_err()
        );
        server.abort();
    }

    #[rstest]
    fn bridge_calls_encode_exact_contract_abi() {
        assert_eq!(
            alloy::primitives::keccak256("BridgeIn(bytes32,uint256,uint256,address)").to_string(),
            "0xd27dacda04d3d9b76b4b91db5d3aae546abc65744db0169e7d13ac4891b0bbd6"
        );
        let recipient = FixedBytes::<32>::from([0x11; 32]);
        let fee = encode_bridge_fee_call(
            U256::from(3),
            11_155_111,
            U256::from(1_000_000),
            recipient,
            Bytes::new(),
        );
        assert_eq!(
            &fee[..4],
            &alloy::primitives::keccak256("getBridgeFee(uint256,uint32,uint256,bytes32,bytes)")
                [..4]
        );
        let decoded = getBridgeFeeCall::abi_decode(&fee).unwrap();
        assert_eq!(decoded.tokenId, U256::from(3));
        assert_eq!(decoded.amount, U256::from(1_000_000));

        let approve = encode_bridge_approve_call(Address::from([0x22; 20]), U256::from(1_000_000));
        let decoded = approveCall::abi_decode(&approve).unwrap();
        assert_eq!(decoded.spender, Address::from([0x22; 20]));
        assert_eq!(decoded.amount, U256::from(1_000_000));

        let params = DeepXBridgeOutParams {
            destination_chain_id: 11_155_111,
            token_id: U256::from(3),
            amount: U256::from(1_000_000),
            destination_recipient: recipient,
            refund_address: Address::from([0x33; 20]),
            salt: FixedBytes::<32>::from([0x44; 32]),
            custom_data: Bytes::new(),
            signature: Bytes::from(vec![0x55; 65]),
        };
        let encoded = encode_bridge_out_call(&params);
        let decoded = bridgeOutCall::abi_decode(&encoded).unwrap();
        assert_eq!(decoded.dstChainId, params.destination_chain_id);
        assert_eq!(decoded.signature, params.signature);
        assert_eq!(decoded.salt, params.salt);
    }

    #[rstest]
    fn bridge_token_tuple_is_canonical() {
        let info = tokenIdToInfoReturn {
            isNativeAsset: false,
            isGasToken: false,
            token: Address::from([0x11; 20]),
            commonDecimal: 6,
            localDecimal: 6,
            isConfigFixed: true,
        };
        let bytes = tokenIdToInfoCall::abi_encode_returns(&info);
        let decoded = tokenIdToInfoCall::abi_decode_returns(&bytes).unwrap();
        assert_eq!(decoded.token, info.token);
        assert_eq!(decoded.localDecimal, info.localDecimal);
        assert_eq!(tokenIdToInfoCall::abi_encode_returns(&decoded), bytes);
    }
}
