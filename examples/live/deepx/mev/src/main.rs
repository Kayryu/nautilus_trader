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

//! Observes large pending DeepX orders as inputs to MEV execution planning.

use std::{collections::HashSet, env, time::Duration};

use anyhow::Context;
use deepx_rust_sdk::{
    rpc::{DeepXRpcClient, chain_decode::decode_runtime_metadata, get_metadata},
    signing::DeepXRuntimeConfig,
};
use serde_json::json;
use subxt_core::{
    Metadata,
    blocks::Extrinsics,
    ext::scale_value::{At, Composite, Value, ValueDef},
};

mod execution;
mod journal;
mod ordering;
mod spot;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();

    if args.len() < 5 || args[0] == "--help" {
        println!(
            "Usage: deepx-mev RPC_URL EXPECTED_GENESIS_HASH MARKET MINIMUM_RAW_SIZE POLL_MS\n\
             [--once] [--inspect-wallet 0xAccountId20] [--ordering-account 0xAccountId20]\n\
             [--front-size RAW_BASE --minimum-profit RAW_QUOTE --cost-budget RAW_QUOTE]\n\
             [--execute|--check-account --subaccount 0xAccountId20 --journal PATH\n\
              --maximum-loss RAW_QUOTE --exit-after-blocks N [--wallet 0xAccountId20]]\n\
             MARKET: perp:<u16>, spot:<pair hash>, or spot-quote:<pair hash>\n\
             Output: JSON lines on stdout; diagnostics on stderr (RUST_LOG, default: info)."
        );
        anyhow::ensure!(
            args.first().is_some_and(|arg| arg == "--help"),
            "invalid arguments"
        );
        return Ok(());
    }

    let mut once = false;
    let mut inspect_wallet = None;
    let mut ordering_account = None;
    let mut maximum_front_size = None;
    let mut minimum_profit = 1_u128;
    let mut cost_budget = 0_u128;
    let mut execute = false;
    let mut check_account = false;
    let mut execution_subaccount = None;
    let mut execution_wallet = None;
    let mut journal_path = None;
    let mut maximum_loss = None;
    let mut exit_after_blocks = None;
    let mut options = args[5..].iter();
    while let Some(option) = options.next() {
        match option.as_str() {
            "--check-account" => {
                anyhow::ensure!(!check_account, "duplicate account check");
                check_account = true;
            }
            "--wallet" => {
                anyhow::ensure!(execution_wallet.is_none(), "duplicate execution wallet");
                execution_wallet = Some(execution::parse_account(
                    options.next().context("missing execution wallet")?,
                )?);
            }
            "--inspect-wallet" => {
                anyhow::ensure!(inspect_wallet.is_none(), "duplicate wallet inspection");
                inspect_wallet = Some(execution::parse_account(
                    options.next().context("missing wallet")?,
                )?);
            }
            "--execute" => {
                anyhow::ensure!(!execute, "duplicate --execute option");
                execute = true;
            }
            "--subaccount" => {
                anyhow::ensure!(execution_subaccount.is_none(), "duplicate subaccount");
                execution_subaccount = Some(execution::parse_account(
                    options.next().context("missing subaccount")?,
                )?);
            }
            "--journal" => {
                anyhow::ensure!(journal_path.is_none(), "duplicate journal");
                journal_path = Some(std::path::PathBuf::from(
                    options.next().context("missing journal path")?,
                ));
            }
            "--maximum-loss" => {
                anyhow::ensure!(maximum_loss.is_none(), "duplicate maximum loss");
                maximum_loss = Some(
                    options
                        .next()
                        .context("missing maximum loss")?
                        .parse::<u128>()?,
                );
            }
            "--exit-after-blocks" => {
                anyhow::ensure!(exit_after_blocks.is_none(), "duplicate exit deadline");
                exit_after_blocks = Some(
                    options
                        .next()
                        .context("missing exit deadline")?
                        .parse::<u64>()?,
                );
            }
            "--once" => {
                anyhow::ensure!(!once, "duplicate --once option");
                once = true;
            }
            "--ordering-account" => {
                anyhow::ensure!(ordering_account.is_none(), "duplicate ordering account");
                let encoded = options.next().context("missing ordering account")?;
                let bytes = hex::decode(
                    encoded
                        .strip_prefix("0x")
                        .context("account must start with 0x")?,
                )?;
                ordering_account = Some(
                    <[u8; 20]>::try_from(bytes)
                        .map_err(|_| anyhow::anyhow!("account must contain exactly 20 bytes"))?,
                );
            }
            "--front-size" => {
                anyhow::ensure!(maximum_front_size.is_none(), "duplicate front size");
                let size: u128 = options.next().context("missing front size")?.parse()?;
                anyhow::ensure!(size > 0, "front size must be positive");
                maximum_front_size = Some(size);
            }
            "--minimum-profit" => {
                minimum_profit = options.next().context("missing minimum profit")?.parse()?;
                anyhow::ensure!(minimum_profit > 0, "minimum profit must be positive");
            }
            "--cost-budget" => {
                cost_budget = options.next().context("missing cost budget")?.parse()?;
            }
            _ => anyhow::bail!("unknown option: {option}"),
        }
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let expected_genesis = parse_hash(&args[1])?;
    let market = Market::parse(&args[2])?;
    anyhow::ensure!(
        maximum_front_size.is_none() || !matches!(market, Market::Perp(_)),
        "front-size analysis currently supports Spot markets"
    );
    let spot_settings = maximum_front_size.map(|maximum_front_size| spot::Settings {
        maximum_front_size,
        minimum_profit,
        cost_budget,
    });
    anyhow::ensure!(
        inspect_wallet.is_none() || (!execute && !check_account),
        "wallet inspection is a separate read-only mode"
    );
    anyhow::ensure!(
        execute
            || check_account
            || (execution_subaccount.is_none()
                && execution_wallet.is_none()
                && journal_path.is_none()
                && maximum_loss.is_none()
                && exit_after_blocks.is_none()),
        "execution settings require --execute"
    );
    anyhow::ensure!(
        !execute || !check_account,
        "account check cannot execute trades"
    );
    let rpc = DeepXRpcClient::new_with_proxy(args[0].clone(), None)?;
    let genesis = rpc.get_block_hash_required_once(0).await?;
    anyhow::ensure!(
        parse_hash(&genesis)? == expected_genesis,
        "chain genesis mismatch"
    );
    log::info!("{}", json!({"event": "chain_verified", "genesis": genesis}));
    let execution = if execute || check_account {
        anyhow::ensure!(
            !execute || spot_settings.is_some(),
            "execution requires --front-size"
        );
        let pair = match market {
            Market::Spot(pair) | Market::SpotQuote(pair) => pair,
            Market::Perp(_) => anyhow::bail!("execution currently supports Spot markets"),
        };
        let execution = execution::Execution::open(
            &rpc,
            execution::Settings {
                journal: journal_path.context("execution requires --journal")?,
                subaccount: execution_subaccount.context("execution requires --subaccount")?,
                wallet: execution_wallet,
                maximum_loss: maximum_loss.context("execution requires --maximum-loss")?,
                exit_after_blocks: exit_after_blocks
                    .context("execution requires --exit-after-blocks")?,
            },
            expected_genesis,
            pair,
        )
        .await?;
        anyhow::ensure!(
            ordering_account.is_none_or(|account| account == execution.wallet()),
            "ordering account must match execution wallet"
        );
        ordering_account = Some(execution.wallet());
        Some(execution)
    } else {
        None
    };
    let minimum: u128 = args[3].parse().context("invalid minimum raw size")?;
    let poll_ms: u64 = args[4].parse().context("invalid poll interval")?;
    anyhow::ensure!(
        minimum > 0 && poll_ms > 0,
        "size and poll interval must be positive"
    );
    log::info!(
        "{}",
        json!({
            "event": "connecting",
            "execute": execute,
            "minimum_raw_size": minimum.to_string(),
            "poll_ms": poll_ms,
            "maximum_front_size": maximum_front_size.map(|size| size.to_string())
        })
    );
    if let Some(wallet) = inspect_wallet {
        execution::inspect_wallet(&rpc, wallet).await?;
        return Ok(());
    }
    if check_account {
        execution
            .as_ref()
            .context("missing account checker")?
            .check_account(&rpc)
            .await?;
        return Ok(());
    }
    if let Some(execution) = &execution
        && execution.resume(&rpc, poll_ms).await?
    {
        return Ok(());
    }
    let mut previous = HashSet::new();
    let mut cached: Option<CachedRuntime> = None;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = &mut shutdown => {
                result?;
                log::info!("{}", json!({"event": "observer_stopped"}));
                return Ok(());
            }
            result = poll(&rpc, &market, minimum, &mut previous, &mut cached, ordering_account, spot_settings.as_ref()) => {
                let result = result.inspect_err(|e| {
                    log::error!("{}", json!({"event": "poll_failed", "error": format!("{e:#}")}));
                })?;
                if let Some(orders) = result {
                    let count = orders.len();
                    for order in orders {
                        if let (Some(execution), Some(settings)) = (&execution, &spot_settings)
                            && execution.consider(&rpc, &order, settings, poll_ms).await? {
                            return Ok(());
                        }
                    }
                    if once {
                        log::info!("{}", json!({"event": "pool_observation_completed", "matching_orders": count}));
                        return Ok(());
                    }
                }
            }
        }

        tokio::select! {
            result = &mut shutdown => {
                result?;
                log::info!("{}", json!({"event": "observer_stopped"}));
                return Ok(());
            }
            () = tokio::time::sleep(Duration::from_millis(poll_ms)) => {}
        }
    }
}

struct CachedRuntime {
    metadata: Metadata,
}

async fn runtime_code_hash(rpc: &DeepXRpcClient, head: &str) -> anyhow::Result<[u8; 32]> {
    let encoded: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorageHash",
            "params": ["0x3a636f6465", head],
        }))
        .await?;
    parse_hash(&encoded).context("invalid runtime code hash")
}

async fn poll(
    rpc: &DeepXRpcClient,
    market: &Market,
    minimum: u128,
    previous: &mut HashSet<String>,
    cached: &mut Option<CachedRuntime>,
    ordering_account: Option<[u8; 20]>,
    spot_settings: Option<&spot::Settings>,
) -> anyhow::Result<Option<Vec<serde_json::Value>>> {
    for attempt in 1..=3 {
        match poll_once(
            rpc,
            market,
            minimum,
            previous,
            cached,
            ordering_account,
            spot_settings,
        )
        .await
        {
            Err(e) if is_unknown_block(&e) && attempt < 3 => {
                // Restart the whole observation rather than mixing state from different heads
                log::warn!(
                    "{}",
                    json!({
                        "event": "poll_discarded",
                        "reason": "state_block_unavailable",
                        "attempt": attempt,
                        "error": format!("{e:#}")
                    })
                );
            }
            result => return result,
        }
    }
    unreachable!("the final polling attempt always returns")
}

fn is_unknown_block(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        let message = cause.to_string();
        message.starts_with("RPC error -32000:") && message.contains("UnknownBlock:")
    })
}

async fn poll_once(
    rpc: &DeepXRpcClient,
    market: &Market,
    minimum: u128,
    previous: &mut HashSet<String>,
    cached: &mut Option<CachedRuntime>,
    ordering_account: Option<[u8; 20]>,
    spot_settings: Option<&spot::Settings>,
) -> anyhow::Result<Option<Vec<serde_json::Value>>> {
    let head = rpc.get_best_block_hash_once().await?;

    if cached.is_none() {
        log::info!(
            "{}",
            json!({"event": "runtime_metadata_loading", "observed_head": head})
        );
        let encoded: String = rpc
            .execute_rpc_call(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "state_getMetadata",
                "params": [head],
            }))
            .await?;
        let bytes = decode_runtime_metadata(&encoded).context("invalid runtime metadata hex")?;
        *cached = Some(CachedRuntime {
            metadata: subxt_core::metadata::decode_from(&bytes[..])?,
        });
    } else {
        log::debug!(
            "{}",
            json!({"event": "runtime_metadata_cached", "observed_head": head})
        );
    }

    let pending = rpc.get_pending_extrinsics().await?;
    let pending_count = pending.len();

    let metadata = &cached
        .as_ref()
        .context("missing runtime metadata")?
        .metadata;
    anyhow::ensure!(
        spot_settings.is_none() || matches!(market, Market::Spot(_) | Market::SpotQuote(_)),
        "Spot analysis requires a Spot market"
    );
    let mut current = HashSet::new();
    let mut output = Vec::new();
    let mut previously_seen = 0_usize;

    for (index, encoded) in pending.iter().enumerate() {
        let bytes = hex::decode(
            encoded
                .strip_prefix("0x")
                .context("invalid extrinsic prefix")?,
        )?;
        let normalized = hex::encode(&bytes);
        anyhow::ensure!(
            current.insert(normalized.clone()),
            "duplicate pending extrinsic"
        );

        if previous.contains(&normalized) {
            previously_seen += 1;
            log::debug!(
                "{}",
                json!({"event": "pending_extrinsic_previously_seen", "index": index})
            );
            continue;
        }

        let decoded = decode_order(metadata, bytes, market, minimum).inspect_err(|e| {
            log::error!(
                "{}",
                json!({
                    "event": "pending_order_decode_failed",
                    "index": index,
                    "error": format!("{e:#}")
                })
            );
        })?;

        if let Some(order) = decoded {
            output.push(order);
        } else {
            log::debug!(
                "{}",
                json!({"event": "pending_extrinsic_filtered", "index": index})
            );
        }
    }

    if !output.is_empty() {
        let nonce_evidence = if let Some(account) = ordering_account {
            Some(ordering::observe(rpc, metadata, &head, account, &pending).await?)
        } else {
            None
        };
        let spot_snapshot = match (spot_settings, market) {
            (Some(_), Market::Spot(pair) | Market::SpotQuote(pair)) => {
                Some(spot::observe(rpc, metadata, &head, *pair).await?)
            }
            _ => None,
        };
        for order in &mut output {
            if let Some(evidence) = &nonce_evidence {
                let target = order["nonce"]
                    .as_str()
                    .context("missing order nonce")?
                    .parse()?;
                order["nonce_plan"] = match evidence.allocate(target) {
                    Ok(pair) => json!({
                        "front": pair.front.to_string(),
                        "target": target.to_string(),
                        "back": pair.back.to_string(),
                        "execution_ready": false,
                        "atomic": false,
                    }),
                    Err(e) => json!({"execution_ready": false, "reason": e.to_string()}),
                };
            }
            if let (Some(snapshot), Some(settings)) = (&spot_snapshot, spot_settings) {
                order["spot_plan"] = snapshot.analyze(order, settings);
            }
        }
    }

    for order in &mut output {
        order["observed_head"] = json!(head);
    }

    log::info!(
        "{}",
        json!({
            "event": "pool_observed",
            "observed_head": head,
            "pending": pending_count,
            "previously_seen": previously_seen,
            "new_matching_orders": output.len(),
            "new_nonmatching_orders": pending_count - previously_seen - output.len()
        })
    );
    *previous = current;
    Ok(Some(output))
}

#[derive(Debug)]
enum Market {
    Spot([u8; 32]),
    SpotQuote([u8; 32]),
    Perp(u16),
}

impl Market {
    fn parse(value: &str) -> anyhow::Result<Self> {
        if let Some(pair) = value.strip_prefix("spot-quote:") {
            return Ok(Self::SpotQuote(parse_hash(pair)?));
        }

        if let Some(pair) = value.strip_prefix("spot:") {
            return Ok(Self::Spot(parse_hash(pair)?));
        }

        if let Some(id) = value.strip_prefix("perp:") {
            return Ok(Self::Perp(
                id.parse().context("invalid perpetual market ID")?,
            ));
        }

        anyhow::bail!(
            "market must be spot:<pair hash>, spot-quote:<pair hash>, or perp:<market ID>"
        )
    }
}

fn parse_hash(value: &str) -> anyhow::Result<[u8; 32]> {
    hex::decode(
        value
            .strip_prefix("0x")
            .context("hash must start with 0x")?,
    )?
    .try_into()
    .map_err(|_| anyhow::anyhow!("hash must contain exactly 32 bytes"))
}

fn decode_order(
    metadata: &Metadata,
    bytes: Vec<u8>,
    market: &Market,
    minimum: u128,
) -> anyhow::Result<Option<serde_json::Value>> {
    let extrinsics = Extrinsics::<DeepXRuntimeConfig>::decode_from(vec![bytes], metadata.clone())?;
    let details = extrinsics.iter().next().context("missing extrinsic")?;
    let pallet = details.pallet_name()?;

    if details.variant_name()? != "place_order"
        || !matches!(
            (market, pallet),
            (Market::Spot(_) | Market::SpotQuote(_), "SpotMarket")
                | (Market::Perp(_), "PerpMarket")
        )
    {
        log::debug!(target:"order", "skipping non-place_order extrinsic or mismatched variant {}, pallet: {}, market: {:?}", details.variant_name()?, pallet, market);
        return Ok(None);
    }

    let fields = details.field_values()?;
    let params = fields.at("params").context("missing order params")?;
    let (base_size, quote_amount, side, market_name) = match market {
        Market::Spot(pair) | Market::SpotQuote(pair) => {
            let actual = fixed_bytes::<32>(params.at("pair").context("missing pair")?)?;

            if actual != *pair {
                log::debug!(target:"order", "skipping order due to mismatched pair: expected 0x{}, got 0x{}", hex::encode(pair), hex::encode(actual));
                return Ok(None);
            }

            let size = spot_size(params.at("base_amount").context("missing base amount")?)?;
            let quote = spot_size(params.at("quote_amount").context("missing quote amount")?)?;
            let side = params
                .at("is_buy")
                .and_then(Value::as_bool)
                .context("invalid side")?;
            (
                size,
                Some(quote),
                side,
                format!("spot:0x{}", hex::encode(pair)),
            )
        }
        Market::Perp(id) => {
            if params.at("market_id").and_then(Value::as_u128) != Some(u128::from(*id)) {
                return Ok(None);
            }

            let size = params
                .at("size")
                .and_then(Value::as_u128)
                .context("invalid size")?;
            let side = params
                .at("is_long")
                .and_then(Value::as_bool)
                .context("invalid side")?;
            (size, None, side, format!("perp:{id}"))
        }
    };

    let quote_sized = matches!(market, Market::SpotQuote(_));
    let threshold_size = if quote_sized {
        quote_amount.context("missing quote threshold quantity")?
    } else {
        base_size
    };

    if threshold_size < minimum {
        // Skip orders that do not meet the minimum threshold size
        log::warn!("order below minimum threshold size: {}", threshold_size);
        return Ok(None);
    }

    let signer = details
        .address_bytes()
        .context("order has no signed sender")?;
    anyhow::ensure!(signer.len() == 20, "expected AccountId20 signer");
    let nonce = details
        .transaction_extensions()
        .and_then(|extensions| extensions.nonce())
        .context("missing signed order nonce")?;
    let order_type = params.at("order_type").context("missing order type")?;
    let ValueDef::Variant(variant) = &order_type.value else {
        anyhow::bail!("invalid order type");
    };
    let time_in_force = if variant.name == "Limit" {
        match variant.values.values().next().map(|value| &value.value) {
            Some(ValueDef::Variant(time_in_force)) => Some(time_in_force.name.as_str()),
            _ => anyhow::bail!("invalid limit order time in force"),
        }
    } else {
        None
    };
    let post_only = params.at("post_only").context("missing post-only policy")?;
    let ValueDef::Variant(post_only) = &post_only.value else {
        anyhow::bail!("invalid post-only policy");
    };
    let reduce_only = params
        .at("reduce_only")
        .and_then(Value::as_bool)
        .context("invalid reduce-only policy")?;
    Ok(Some(json!({
        "event": "large_pending_order",
        "transaction_hash": format!("0x{}", hex::encode(details.hash())),
        "market": market_name,
        "signer": format!("0x{}", hex::encode(signer)),
        "subaccount": format!("0x{}", hex::encode(fixed_bytes::<20>(params.at("subaccount").context("missing subaccount")?)?)),
        "nonce": nonce.to_string(),
        "side": if side { "buy" } else { "sell" },
        "raw_base_size": base_size.to_string(),
        "raw_quote_amount": quote_amount.map(|amount| amount.to_string()),
        "threshold_asset": if quote_sized { "quote" } else { "base" },
        "order_type": variant.name,
        "time_in_force": time_in_force,
        "post_only": post_only.name,
        "reduce_only": reduce_only,
    })))
}

fn fixed_bytes<const N: usize>(value: &Value<u32>) -> anyhow::Result<[u8; N]> {
    let ValueDef::Composite(composite) = &value.value else {
        anyhow::bail!("expected byte array");
    };

    // H160/H256 have one tuple field wrapping the actual byte array
    if composite.len() == 1 {
        return fixed_bytes(composite.values().next().context("missing array field")?);
    }

    let bytes: Vec<u8> = composite
        .values()
        .map(|value| {
            value
                .as_u128()
                .and_then(|n| u8::try_from(n).ok())
                .context("invalid byte")
        })
        .collect::<anyhow::Result<_>>()?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid byte array length"))
}

fn spot_size(value: &Value<u32>) -> anyhow::Result<u128> {
    // primitive_types::U256 is a tuple struct containing four little-endian u64 limbs
    let limbs = value.at(0).context("missing U256 limbs")?;
    let ValueDef::Composite(Composite::Unnamed(limbs)) = &limbs.value else {
        anyhow::bail!("invalid U256 limbs");
    };
    anyhow::ensure!(limbs.len() == 4, "invalid U256 limb count");
    let limbs = limbs
        .iter()
        .map(|value| {
            value
                .as_u128()
                .and_then(|n| u64::try_from(n).ok())
                .context("invalid U256 limb")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        limbs[2] == 0 && limbs[3] == 0,
        "spot size exceeds supported u128 range"
    );
    Ok(u128::from(limbs[0]) | (u128::from(limbs[1]) << 64))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use axum::{Json, Router, extract::State, routing::post};
    use deepx_rust_sdk::{
        credential::{DeepXKeyScheme, DeepXPrivateKey},
        deployment::DEEPX_TESTNET_GENESIS_HASH,
        enums::DeepXEnvironment,
        rpc::{submission::decode_signed_extrinsic_identity, timestamp::timestamp_now_storage_key},
        signing::{
            DeepXOfflinePalletCall, DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPostOnlyParam,
            DeepXSpotOrderType, DeepXSpotPlaceParams, DeepXValidatedRuntime,
            sign_validated_pallet_call, validate_testnet_runtime,
        },
    };
    use rstest::rstest;
    use subxt_core::{dynamic, ext::scale_value::scale::encode_as_type, storage};

    use super::*;

    fn encoded_metadata() -> String {
        let response: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../deepx-rust-sdk/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        response["result"].as_str().unwrap().to_string()
    }

    fn runtime() -> DeepXValidatedRuntime {
        let bytes = decode_runtime_metadata(&encoded_metadata()).unwrap();
        validate_testnet_runtime(
            &DeepXEnvironment::Testnet,
            parse_hash(DEEPX_TESTNET_GENESIS_HASH).unwrap(),
            376,
            1,
            &bytes,
        )
        .unwrap()
    }

    fn signed(runtime: &DeepXValidatedRuntime, call: DeepXOfflinePalletCall) -> Vec<u8> {
        // Public deterministic test key; never fund this account
        let key = DeepXPrivateKey::new(
            &format!("0x{}", "01".repeat(32)),
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        sign_validated_pallet_call(runtime, &key, call, 1_725_000_000_124)
            .unwrap()
            .bytes()
            .to_vec()
    }

    fn perp(is_long: bool, size: u128) -> DeepXOfflinePalletCall {
        DeepXOfflinePalletCall::PerpPlaceOrder(DeepXPerpPlaceParams {
            subaccount: [2; 20],
            market_id: 7,
            is_long,
            size,
            price: 3_000_000_000,
            order_type: DeepXPerpOrderType::Market(Some(50)),
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXPostOnlyParam::None,
        })
    }

    #[rstest]
    #[case(true, "buy")]
    #[case(false, "sell")]
    fn decodes_signed_perpetual_without_precision_loss(#[case] is_long: bool, #[case] side: &str) {
        let runtime = runtime();
        let bytes = signed(&runtime, perp(is_long, u128::MAX));
        let order = decode_order(runtime.metadata(), bytes, &Market::Perp(7), 100)
            .unwrap()
            .unwrap();
        assert_eq!(order["raw_base_size"], u128::MAX.to_string());
        assert_eq!(order["side"], side);
        assert_eq!(order["nonce"], "1725000000124");
        assert_eq!(order["order_type"], "Market");
    }

    #[rstest]
    fn filters_other_markets_small_orders_and_non_order_calls() {
        let runtime = runtime();
        let bytes = signed(&runtime, perp(true, 100));
        assert!(
            decode_order(runtime.metadata(), bytes.clone(), &Market::Perp(8), 100)
                .unwrap()
                .is_none()
        );
        assert!(
            decode_order(runtime.metadata(), bytes.clone(), &Market::Perp(7), 101)
                .unwrap()
                .is_none()
        );
        assert!(
            decode_order(runtime.metadata(), bytes, &Market::Perp(7), 100)
                .unwrap()
                .is_some()
        );
        let bytes = signed(&runtime, DeepXOfflinePalletCall::NoOp);
        assert!(
            decode_order(runtime.metadata(), bytes, &Market::Perp(7), 100)
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn decodes_spot_u256_amount_and_pair(#[case] is_buy: bool) {
        let runtime = runtime();
        let mut amount = [0; 32];
        amount[..16].copy_from_slice(&u128::MAX.to_le_bytes());
        let call = DeepXOfflinePalletCall::SpotPlaceOrder(DeepXSpotPlaceParams {
            subaccount: [2; 20],
            pair: [3; 32],
            is_buy,
            quote_amount: [0; 32],
            base_amount: amount,
            order_type: DeepXSpotOrderType::Market(Some(50)),
            post_only: DeepXPostOnlyParam::None,
            reduce_only: false,
        });
        let bytes = signed(&runtime, call);
        let order = decode_order(
            runtime.metadata(),
            bytes.clone(),
            &Market::Spot([3; 32]),
            100,
        )
        .unwrap()
        .unwrap();
        assert_eq!(order["raw_base_size"], u128::MAX.to_string());
        assert_eq!(order["side"], if is_buy { "buy" } else { "sell" });
        assert!(
            decode_order(runtime.metadata(), bytes, &Market::Spot([4; 32]), 100)
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    fn rejects_truncated_extrinsics_and_amount_overflow() {
        let runtime = runtime();
        let mut bytes = signed(&runtime, perp(true, 100));
        bytes.pop();
        assert!(decode_order(runtime.metadata(), bytes, &Market::Perp(7), 100).is_err());
        let value = Value::unnamed_composite([Value::unnamed_composite([
            Value::u128(0),
            Value::u128(0),
            Value::u128(1),
            Value::u128(0),
        ])])
        .map_context(|()| 0);
        assert!(spot_size(&value).is_err());
    }

    #[rstest]
    fn detects_quote_sized_spot_buy_without_base_quantity() {
        let runtime = runtime();
        let mut quote_amount = [0; 32];
        quote_amount[..16].copy_from_slice(&1_000_000_000_000u128.to_le_bytes());
        let bytes = signed(
            &runtime,
            DeepXOfflinePalletCall::SpotPlaceOrder(DeepXSpotPlaceParams {
                subaccount: [2; 20],
                pair: [3; 32],
                is_buy: true,
                quote_amount,
                base_amount: [0; 32],
                order_type: DeepXSpotOrderType::Market(Some(50)),
                post_only: DeepXPostOnlyParam::None,
                reduce_only: false,
            }),
        );
        let order = decode_order(
            runtime.metadata(),
            bytes.clone(),
            &Market::SpotQuote([3; 32]),
            1_000_000_000_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(order["raw_quote_amount"], "1000000000000");
        assert_eq!(order["raw_base_size"], "0");
        assert_eq!(order["threshold_asset"], "quote");
        assert!(
            decode_order(
                runtime.metadata(),
                bytes,
                &Market::SpotQuote([3; 32]),
                1_000_000_000_001
            )
            .unwrap()
            .is_none()
        );
    }

    async fn mock_rpc(
        script: Vec<(&'static str, serde_json::Value)>,
    ) -> (DeepXRpcClient, tokio::task::JoinHandle<()>) {
        mock_rpc_responses(
            script
                .into_iter()
                .map(|(method, value)| (method, Ok(value)))
                .collect(),
        )
        .await
    }

    async fn mock_rpc_responses(
        script: Vec<(&'static str, Result<serde_json::Value, serde_json::Value>)>,
    ) -> (DeepXRpcClient, tokio::task::JoinHandle<()>) {
        type Script =
            Arc<Mutex<VecDeque<(&'static str, Result<serde_json::Value, serde_json::Value>)>>>;
        async fn respond(
            State(script): State<Script>,
            Json(request): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            let (method, result) = script
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected RPC call");
            assert_eq!(request["method"], method);
            Json(match result {
                Ok(result) => json!({"jsonrpc": "2.0", "id": request["id"], "result": result}),
                Err(e) => json!({"jsonrpc": "2.0", "id": request["id"], "error": e}),
            })
        }
        let app = Router::new()
            .route("/", post(respond))
            .with_state(Arc::new(Mutex::new(VecDeque::from(script))));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (DeepXRpcClient::new_with_proxy(url, None).unwrap(), server)
    }

    fn unknown_block() -> serde_json::Value {
        json!({
            "code": -32000,
            "message": "Client error: UnknownBlock: Can not pin MVCC state for block `old_head`. Block not found."
        })
    }

    #[tokio::test]
    async fn unavailable_metadata_restarts_observation() {
        let runtime = runtime();
        let mut script = vec![
            ("chain_getBlockHash", Ok(json!("old_head"))),
            ("state_getMetadata", Err(unknown_block())),
        ];
        let encoded = format!("0x{}", hex::encode(signed(&runtime, perp(true, 100))));
        script.extend([
            ("chain_getBlockHash", Ok(json!("fresh_head"))),
            ("state_getMetadata", Ok(json!(encoded_metadata()))),
            ("author_pendingExtrinsics", Ok(json!([encoded]))),
            ("chain_getBlockHash", Ok(json!("fresh_head"))),
        ]);
        let (rpc, server) = mock_rpc_responses(script).await;
        let mut previous = HashSet::from(["previous_pool_entry".to_string()]);
        let mut cached = None;
        let orders = poll(
            &rpc,
            &Market::Perp(7),
            100,
            &mut previous,
            &mut cached,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0]["observed_head"], "fresh_head");
        assert!(cached.is_some());
        assert_eq!(previous, HashSet::from([encoded[2..].to_string()]));
        server.abort();
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn polling_retries_only_unknown_blocks_and_stops_after_three_attempts(
        #[case] unavailable: bool,
    ) {
        let attempts = if unavailable { 3 } else { 1 };
        let error = if unavailable {
            unknown_block()
        } else {
            json!({"code": -32000, "message": "Client error: invalid storage"})
        };
        let mut script = Vec::new();
        for _ in 0..attempts {
            script.extend([
                ("chain_getBlockHash", Ok(json!("head"))),
                ("state_getMetadata", Err(error.clone())),
            ]);
        }
        let (rpc, server) = mock_rpc_responses(script).await;
        let mut previous = HashSet::from(["previous_pool_entry".to_string()]);
        let mut cached = None;
        let e = poll(
            &rpc,
            &Market::Perp(7),
            100,
            &mut previous,
            &mut cached,
            None,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(is_unknown_block(&e), unavailable);
        assert!(cached.is_none());
        assert_eq!(previous, HashSet::from(["previous_pool_entry".to_string()]));
        server.abort();
    }

    #[tokio::test]
    async fn pool_observations_deduplicate_and_report_reappearance() {
        let runtime = runtime();
        let encoded = format!("0x{}", hex::encode(signed(&runtime, perp(true, 100))));
        let mut script = Vec::new();
        for pool in [
            json!([encoded]),
            json!([encoded]),
            json!([]),
            json!([encoded]),
        ] {
            script.push(("chain_getBlockHash", json!("head")));
            script.push(("author_pendingExtrinsics", pool));
        }
        let (rpc, server) = mock_rpc(script).await;
        let mut cached = Some(CachedRuntime {
            metadata: runtime.metadata().clone(),
        });
        let mut previous = HashSet::new();
        for expected in [1, 0, 0, 1] {
            let orders = poll(
                &rpc,
                &Market::Perp(7),
                100,
                &mut previous,
                &mut cached,
                None,
                None,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(orders.len(), expected);
            if let Some(order) = orders.first() {
                assert_eq!(order["observed_head"], "head");
            }
        }
        server.abort();
    }

    #[rstest]
    #[case("empty")]
    #[case("filtered")]
    #[case("seen")]
    #[tokio::test]
    async fn no_new_matching_orders_skip_nonce_and_book_reads(#[case] pool_kind: &str) {
        let runtime = runtime();
        let encoded = format!("0x{}", hex::encode(signed(&runtime, perp(true, 100))));
        let pool = if pool_kind == "empty" {
            json!([])
        } else {
            json!([encoded])
        };
        let (rpc, server) = mock_rpc(vec![
            ("chain_getBlockHash", json!("head")),
            ("author_pendingExtrinsics", pool),
            ("chain_getBlockHash", json!("head")),
        ])
        .await;
        let mut cached = Some(CachedRuntime {
            metadata: runtime.metadata().clone(),
        });
        let mut previous = if pool_kind == "seen" {
            HashSet::from([encoded.strip_prefix("0x").unwrap().to_string()])
        } else {
            HashSet::new()
        };
        let settings = spot::Settings {
            maximum_front_size: 100,
            minimum_profit: 0,
            cost_budget: 0,
        };
        let orders = poll(
            &rpc,
            &Market::Spot([3; 32]),
            100,
            &mut previous,
            &mut cached,
            Some([1; 20]),
            Some(&settings),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(orders.is_empty());
        assert_eq!(previous.len(), usize::from(pool_kind != "empty"));
        server.abort();
    }

    #[tokio::test]
    async fn failed_candidate_evidence_does_not_update_seen_pool() {
        let runtime = runtime();
        let bytes = signed(
            &runtime,
            DeepXOfflinePalletCall::SpotPlaceOrder(DeepXSpotPlaceParams {
                subaccount: [2; 20],
                pair: [3; 32],
                is_buy: true,
                quote_amount: [0; 32],
                base_amount: {
                    let mut amount = [0; 32];
                    amount[0] = 100;
                    amount
                },
                order_type: DeepXSpotOrderType::Market(Some(50)),
                post_only: DeepXPostOnlyParam::None,
                reduce_only: false,
            }),
        );
        let head = format!("0x{}", "11".repeat(32));
        let (rpc, server) = mock_rpc(vec![
            ("chain_getBlockHash", json!(head)),
            (
                "author_pendingExtrinsics",
                json!([format!("0x{}", hex::encode(bytes))]),
            ),
            ("chain_getBlockHash", json!(head)),
            ("state_queryStorageAt", json!([])),
        ])
        .await;
        let mut cached = Some(CachedRuntime {
            metadata: runtime.metadata().clone(),
        });
        let mut previous = HashSet::new();
        assert!(
            poll(
                &rpc,
                &Market::Spot([3; 32]),
                100,
                &mut previous,
                &mut cached,
                Some([1; 20]),
                None,
            )
            .await
            .is_err()
        );
        assert!(previous.is_empty());
        server.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn metadata_is_loaded_only_for_empty_cache(#[case] already_cached: bool) {
        let runtime = runtime();
        let encoded = format!("0x{}", hex::encode(signed(&runtime, perp(true, 100))));
        let script = if already_cached {
            vec![
                ("chain_getBlockHash", json!("head")),
                ("author_pendingExtrinsics", json!([encoded])),
                ("chain_getBlockHash", json!("head")),
            ]
        } else {
            vec![
                ("chain_getBlockHash", json!("head")),
                ("state_getMetadata", json!(encoded_metadata())),
                ("author_pendingExtrinsics", json!([encoded])),
                ("chain_getBlockHash", json!("head")),
            ]
        };
        let (rpc, server) = mock_rpc(script).await;
        let mut cached = already_cached.then(|| CachedRuntime {
            metadata: runtime.metadata().clone(),
        });
        let mut previous = HashSet::new();
        let orders = poll(
            &rpc,
            &Market::Perp(7),
            100,
            &mut previous,
            &mut cached,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(orders.len(), 1);
        assert!(cached.is_some());
        server.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn pool_observation_continues_when_head_changes(#[case] invalid_pending: bool) {
        let runtime = runtime();
        let encoded = format!("0x{}", hex::encode(signed(&runtime, perp(true, 100))));
        let (rpc, server) = mock_rpc(vec![
            ("chain_getBlockHash", json!("head")),
            (
                "author_pendingExtrinsics",
                json!([if invalid_pending { "invalid" } else { &encoded }]),
            ),
        ])
        .await;
        let mut cached = Some(CachedRuntime {
            metadata: runtime.metadata().clone(),
        });
        let mut previous = HashSet::from(["previous_pool_entry".to_string()]);
        let result = poll(
            &rpc,
            &Market::Perp(7),
            100,
            &mut previous,
            &mut cached,
            None,
            None,
        )
        .await;
        if invalid_pending {
            assert!(result.is_err());
            assert_eq!(previous, HashSet::from(["previous_pool_entry".to_string()]));
        } else {
            let orders = result.unwrap().unwrap();
            assert_eq!(orders.len(), 1);
            assert_eq!(orders[0]["observed_head"], "head");
            assert_eq!(previous.len(), 1);
        }
        assert!(cached.is_some());
        server.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn invalid_pool_does_not_publish_partial_results(#[case] duplicate: bool) {
        let runtime = runtime();
        let encoded = format!("0x{}", hex::encode(signed(&runtime, perp(true, 100))));
        let invalid = if duplicate {
            encoded.clone()
        } else {
            "0x100102".to_string()
        };
        let (rpc, server) = mock_rpc(vec![
            ("chain_getBlockHash", json!("head")),
            ("author_pendingExtrinsics", json!([encoded, invalid])),
            ("chain_getBlockHash", json!("head")),
        ])
        .await;
        let mut cached = Some(CachedRuntime {
            metadata: runtime.metadata().clone(),
        });
        let mut previous = HashSet::new();
        assert!(
            poll(
                &rpc,
                &Market::Perp(7),
                100,
                &mut previous,
                &mut cached,
                None,
                None,
            )
            .await
            .is_err()
        );
        assert!(previous.is_empty());
        server.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn nonce_evidence_decodes_account_storage_and_pending_operations(#[case] trailing: bool) {
        let runtime = runtime();
        let metadata = runtime.metadata();
        let pending = signed(&runtime, DeepXOfflinePalletCall::NoOp);
        let identity = decode_signed_extrinsic_identity(metadata, &pending).unwrap();
        let now = identity.nonce;
        let address = dynamic::storage(
            "System",
            "Account",
            vec![Value::from_bytes(identity.signer)],
        );
        let mut account = storage::default_value(&address, metadata)
            .unwrap()
            .to_value()
            .unwrap();
        let ValueDef::Composite(Composite::Named(fields)) = &mut account.value else {
            panic!("expected named account fields");
        };
        for (name, value) in fields {
            if name == "quota" {
                *value = Value::u128(100).map_context(|()| value.context);
            } else if name == "time_nonce" {
                *value = Value::unnamed_composite([Value::unnamed_composite([
                    Value::u128(u128::from(now - 1)),
                    Value::u128(u128::from(now + 2)),
                ])])
                .map_context(|()| value.context);
            }
        }
        let mut account_bytes = Vec::new();
        encode_as_type(
            &account,
            account.context,
            metadata.types(),
            &mut account_bytes,
        )
        .unwrap();
        if trailing {
            account_bytes.push(0);
        }
        let head = format!("0x{}", "33".repeat(32));
        let (rpc, server) = mock_rpc(vec![
            ("state_queryStorageAt", json!([{
                "block": head,
                "changes": [[timestamp_now_storage_key(), format!("0x{}", hex::encode(now.to_le_bytes()))]],
            }])),
            ("state_getStorage", json!(format!("0x{}", hex::encode(account_bytes)))),
        ]).await;
        let result = ordering::observe(
            &rpc,
            metadata,
            &head,
            identity.signer,
            &[format!("0x{}", hex::encode(pending))],
        )
        .await;
        if trailing {
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("trailing account storage bytes")
            );
        } else {
            let pair = result.unwrap().allocate(now + 1).unwrap();
            assert_eq!(pair.front, now - 2);
            assert_eq!(pair.back, now + 3);
        }
        server.abort();
    }
}
