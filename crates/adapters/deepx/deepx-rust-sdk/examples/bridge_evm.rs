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

//! DeepX testnet USDC bridge-out and destination `BridgeIn` observation.

use std::{str::FromStr, time::Duration};

use alloy::{
    primitives::{Address, B256, Bytes, FixedBytes, U256},
    signers::local::PrivateKeySigner,
};
use anyhow::{Context, ensure};
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    enums::DeepXEnvironment,
    rest::bridge_authorizer::{DeepXBridgeAuthorizationTerms, request_bridge_authorization},
    rpc::{
        DeepXRpcClient,
        bridge::{
            DeepXBridgeArrivalFilter, DeepXBridgeFeeParams, DeepXBridgeOutParams,
            encode_bridge_approve_call, encode_bridge_out_call, get_bridge_allowance_at_block,
            get_bridge_fee_at_block, get_bridge_token_info_at_block, get_evm_block_number,
            get_evm_chain_id, get_pending_evm_nonce, submit_bridge_evm_transaction,
            wait_bridge_evm_receipt, wait_bridge_in,
        },
    },
    signing::{DeepXBridgeEvmTransactionParams, sign_bridge_evm_transaction},
};
use serde_json::json;

const SOURCE_RPC: &str = "https://rpc-testnet.deepx.fi";
const DESTINATION_RPC: &str = "https://ethereum-sepolia-rpc.publicnode.com";
const SOURCE_CHAIN_ID: u64 = 4_856;
const DESTINATION_CHAIN_ID: u64 = 11_155_111;
const USDC_TOKEN_ID: u64 = 3;
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(180);
const ARRIVAL_TIMEOUT: Duration = Duration::from_secs(1_800);

struct BridgeConfig {
    source_rpc: String,
    destination_rpc: String,
    source_bridge: Address,
    destination_bridge: Address,
    sign_api_base: String,
    amount: U256,
    submit: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = BridgeConfig::from_env()?;
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let source = DeepXRpcClient::new_with_proxy(config.source_rpc, proxy.as_deref())?;
    let destination = DeepXRpcClient::new_with_proxy(config.destination_rpc, proxy.as_deref())?;
    ensure!(
        get_evm_chain_id(&source).await? == SOURCE_CHAIN_ID,
        "source RPC chain ID differs from the pinned Python bridge example"
    );
    ensure!(
        get_evm_chain_id(&destination).await? == DESTINATION_CHAIN_ID,
        "destination RPC is not Ethereum Sepolia"
    );
    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    let sender = PrivateKeySigner::from_slice(key.as_bytes())?.address();
    let recipient = std::env::var("BRIDGE_RECIPIENT")
        .ok()
        .map(|value| Address::from_str(&value))
        .transpose()
        .context("invalid BRIDGE_RECIPIENT")?
        .unwrap_or(sender);
    let mut recipient32 = [0_u8; 32];
    recipient32[12..].copy_from_slice(recipient.as_slice());
    let recipient32 = FixedBytes::from(recipient32);
    let watch_from = get_evm_block_number(&destination).await?.saturating_sub(20);
    let token = get_bridge_token_info_at_block(
        &source,
        config.source_bridge,
        U256::from(USDC_TOKEN_ID),
        "latest",
    )
    .await?;
    ensure!(
        token.token != Address::ZERO,
        "USDC is not registered on the source bridge"
    );
    ensure!(
        !token.is_gas_token,
        "registered USDC is unexpectedly a gas token"
    );
    ensure!(
        token.local_decimal == 6,
        "source USDC does not have six decimals"
    );
    let destination_token = get_bridge_token_info_at_block(
        &destination,
        config.destination_bridge,
        U256::from(USDC_TOKEN_ID),
        "latest",
    )
    .await?;
    ensure!(
        destination_token.token != Address::ZERO,
        "USDC is not registered on the destination bridge"
    );
    ensure!(
        destination_token.local_decimal == 6,
        "destination USDC does not have six decimals"
    );
    let fee = get_bridge_fee_at_block(
        &source,
        config.source_bridge,
        &DeepXBridgeFeeParams {
            token_id: U256::from(USDC_TOKEN_ID),
            destination_chain_id: u32::try_from(DESTINATION_CHAIN_ID)?,
            amount: config.amount,
            destination_recipient: recipient32,
            custom_data: Bytes::new(),
        },
        "latest",
    )
    .await?;
    let allowance =
        get_bridge_allowance_at_block(&source, token.token, sender, config.source_bridge, "latest")
            .await?;
    println!(
        "source USDC={} amount_raw={} fee_wei={} allowance_raw={} recipient={recipient}",
        token.token, config.amount, fee, allowance
    );
    let mut salt = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut salt)
        .map_err(|_| anyhow::anyhow!("unable to generate bridge salt"))?;
    let salt = FixedBytes::from(salt);
    let terms = DeepXBridgeAuthorizationTerms {
        source_chain_id: SOURCE_CHAIN_ID,
        bridge_contract: config.source_bridge,
        destination_chain_id: u32::try_from(DESTINATION_CHAIN_ID)?,
        token_id: U256::from(USDC_TOKEN_ID),
        amount: config.amount,
        sender,
        destination_recipient: recipient32,
        refund_address: sender,
        salt,
        custom_data: Bytes::new(),
    };
    let expected_authorizer = std::env::var("BRIDGE_EXPECTED_AUTHORIZER")
        .ok()
        .map(|value| Address::from_str(&value))
        .transpose()
        .context("invalid BRIDGE_EXPECTED_AUTHORIZER")?;
    let expected_id = std::env::var("BRIDGE_EXPECTED_TX_ID")
        .ok()
        .map(|value| B256::from_str(&value))
        .transpose()
        .context("invalid BRIDGE_EXPECTED_TX_ID")?;
    let authorization = request_bridge_authorization(
        &reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        &config.sign_api_base,
        &terms,
        expected_authorizer,
    )
    .await?;
    let bridge_call = encode_bridge_out_call(&DeepXBridgeOutParams {
        destination_chain_id: terms.destination_chain_id,
        token_id: terms.token_id,
        amount: terms.amount,
        destination_recipient: terms.destination_recipient,
        refund_address: terms.refund_address,
        salt: terms.salt,
        custom_data: terms.custom_data.clone(),
        signature: authorization.signature,
    });
    println!(
        "bridge authorization received: salt=0x{} calldata_bytes={}",
        hex::encode(salt),
        bridge_call.len()
    );
    if !config.submit {
        println!("read-only preview; pass --submit to send approval and bridge transactions");
        return Ok(());
    }

    let max_fee_per_gas = required_u128("BRIDGE_MAX_FEE_PER_GAS_WEI")?;
    let max_priority_fee_per_gas = required_u128("BRIDGE_PRIORITY_FEE_PER_GAS_WEI")?;
    let approval_gas_limit = required_u64("BRIDGE_APPROVE_GAS_LIMIT")?;
    let bridge_gas_limit = required_u64("BRIDGE_OUT_GAS_LIMIT")?;
    let mut nonce = get_pending_evm_nonce(&source, sender).await?;
    if allowance < config.amount {
        let approve = sign_bridge_evm_transaction(
            &key,
            &DeepXBridgeEvmTransactionParams {
                chain_id: SOURCE_CHAIN_ID,
                nonce,
                gas_limit: approval_gas_limit,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                contract_address: token.token.into_array(),
                value: U256::ZERO,
                calldata: encode_bridge_approve_call(config.source_bridge, config.amount),
            },
        )?;
        println!(
            "approval signed: nonce={nonce} hash=0x{}",
            hex::encode(approve.tx_hash)
        );
        let approve_hash = submit_bridge_evm_transaction(&source, &approve).await?;
        wait_bridge_evm_receipt(
            &source,
            approve_hash,
            sender,
            token.token,
            2,
            RECEIPT_TIMEOUT,
        )
        .await?;
        let after = get_bridge_allowance_at_block(
            &source,
            token.token,
            sender,
            config.source_bridge,
            "latest",
        )
        .await?;
        ensure!(
            after >= config.amount,
            "approval finalized without sufficient allowance"
        );
        nonce = nonce.checked_add(1).context("EVM nonce overflow")?;
        ensure!(
            get_pending_evm_nonce(&source, sender).await? == nonce,
            "pending nonce changed after approval; reconcile before bridge-out"
        );
    }
    let simulation: String = source
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{
                "from": sender,
                "to": config.source_bridge,
                "data": format!("0x{}", hex::encode(&bridge_call)),
                "value": format!("0x{fee:x}"),
            }, "latest"],
        }))
        .await?;
    ensure!(
        simulation == "0x",
        "bridge-out simulation returned unexpected data"
    );
    let bridge = sign_bridge_evm_transaction(
        &key,
        &DeepXBridgeEvmTransactionParams {
            chain_id: SOURCE_CHAIN_ID,
            nonce,
            gas_limit: bridge_gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            contract_address: config.source_bridge.into_array(),
            value: fee,
            calldata: bridge_call,
        },
    )?;
    println!(
        "bridge-out signed: nonce={nonce} hash=0x{}",
        hex::encode(bridge.tx_hash)
    );
    let source_hash = submit_bridge_evm_transaction(&source, &bridge).await?;
    let receipt = wait_bridge_evm_receipt(
        &source,
        source_hash,
        sender,
        config.source_bridge,
        2,
        RECEIPT_TIMEOUT,
    )
    .await?;
    println!(
        "source bridge-out confirmed: block={}",
        receipt.block_number
    );
    let event = wait_bridge_in(
        &destination,
        &DeepXBridgeArrivalFilter {
            bridge: config.destination_bridge,
            recipient,
            from_block: watch_from,
            token_id: U256::from(USDC_TOKEN_ID),
            amount: config.amount,
            expected_bridge_transaction_id: expected_id,
            confirmations: 2,
        },
        ARRIVAL_TIMEOUT,
    )
    .await?;
    println!(
        "destination BridgeIn observed: tx={} block={} bridge_id={} source_linked={}",
        event.transaction_hash,
        event.block_number,
        event.bridge_transaction_id,
        expected_id.is_some()
    );
    Ok(())
}

impl BridgeConfig {
    fn from_env() -> anyhow::Result<Self> {
        let mut args = std::env::args();
        let submit = match args.nth(1).as_deref() {
            None => false,
            Some("--submit") => true,
            Some(_) => anyhow::bail!("usage: deepx-sdk-bridge-evm [--submit]"),
        };
        ensure!(args.next().is_none(), "unexpected bridge argument");
        let source_bridge = required_env("BRIDGE_SRC")?
            .parse()
            .context("invalid BRIDGE_SRC")?;
        let destination_bridge = required_env("BRIDGE_DST")?
            .parse()
            .context("invalid BRIDGE_DST")?;
        let amount = U256::from(required_u128("BRIDGE_AMOUNT_RAW")?);
        ensure!(amount > U256::ZERO, "bridge amount must be positive");
        Ok(Self {
            source_rpc: std::env::var("BRIDGE_SRC_RPC").unwrap_or_else(|_| SOURCE_RPC.to_string()),
            destination_rpc: std::env::var("BRIDGE_DST_RPC")
                .unwrap_or_else(|_| DESTINATION_RPC.to_string()),
            source_bridge,
            destination_bridge,
            sign_api_base: std::env::var("SIGN_API_BASE")
                .unwrap_or_else(|_| "https://rest-api-testnet.deepx.fi".to_string()),
            amount,
            submit,
        })
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).with_context(|| format!("missing {name}"))
}

fn required_u64(name: &str) -> anyhow::Result<u64> {
    required_env(name)?
        .parse()
        .with_context(|| format!("invalid {name}"))
}

fn required_u128(name: &str) -> anyhow::Result<u128> {
    required_env(name)?
        .parse()
        .with_context(|| format!("invalid {name}"))
}
