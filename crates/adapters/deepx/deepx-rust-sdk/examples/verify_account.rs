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

//! Read-only account REST checks using the standalone DeepX SDK.

use std::{
    collections::HashSet,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    credential::{DeepXKeyScheme, DeepXPrivateKey},
    deployment::{DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL},
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    query::{
        DeepXAccountSortOrder, DeepXAllSubaccountsRequest, DeepXBalanceChangesRequest,
        DeepXHourlyUnsettledFundingRequest, DeepXLiquidationExecutionRecordsRequest,
        DeepXLiquidationRecordsRequest, DeepXPerpFundingFeeRequest,
        DeepXPerpLiquidationPriceRequest, DeepXPerpOpenOrdersRequest, DeepXPerpWalletOrdersRequest,
        DeepXPerpWalletTradesRequest, DeepXQuotaHistoryRequest, DeepXSpotAccountTradesRequest,
        DeepXSpotAdlCandidatesRequest, DeepXSpotHistoryOrdersRequest, DeepXSpotOpenOrdersRequest,
        DeepXSpotOrderByIdRequest, DeepXSpotOrderSide, DeepXSpotWalletOrdersRequest,
        DeepXSpotWalletTradesRequest, DeepXWalletFundingFeeRequest,
    },
    rest::{
        DeepXLiquidationExecutionRoute, DeepXRestTransport,
        ownership::{
            verify_account_ownership_evidence, verify_delegated_account_ownership_evidence,
        },
    },
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcBlockResponse, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        endpoint_identity::{
            DEEPX_RECOVERY_RPC_METHODS, DEEPX_SUBMISSION_RPC_METHODS, DEEPX_WATCH_RPC_METHODS,
            DeepXObservedRpcEndpoint, DeepXRpcRole, validate_rpc_endpoint_identities,
        },
        event_decode::{
            DeepXPerpCancelExpected, DeepXPerpCancelInclusionOutcome, decode_perp_cancel_inclusion,
        },
        event_storage::get_system_events,
        get_finalized_head, get_metadata, get_runtime_version,
        perp_place::{
            DeepXPerpPlaceExpected, DeepXPerpPlaceInclusionOutcome, decode_perp_place_inclusion,
        },
        timestamp::{decode_timestamp_set_inherent, get_timestamp_now},
    },
    signing::{
        DeepXPermittedPalletExtrinsic, DeepXPerpCancelParams, DeepXPerpOrderType,
        DeepXPerpPlaceParams, DeepXPostOnlyParam, DeepXRuntimeSnapshotService, DeepXTimeInForce,
        RuntimeSnapshot, derive_signer_account_id, sign_perp_cancel, sign_perp_place_order,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Deserialize;
use serde_json::json;

const BASE_URL: &str = DEEPX_TESTNET_REST_URL;
const PAGE_SIZE: u32 = 100;
const MAX_PAGES: usize = 100;
const CAPTURE_START_MS: u64 = 1_789_488_862_441;
const CAPTURE_END_MS: u64 = 1_789_521_262_677;
const DELEGATE_MARKET_ID: u16 = 3;
const MAX_CLOCK_DRIFT_MS: u64 = 10_000;
const FINALITY_TIMEOUT: Duration = Duration::from_secs(120);
const FINALITY_POLL_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Clone, Copy)]
enum DelegateOrderOperation {
    Place {
        params: DeepXPerpPlaceParams,
        order_id: u64,
    },
    Cancel {
        subaccount: [u8; 20],
        order_id: u64,
    },
}

impl DelegateOrderOperation {
    fn name(self) -> &'static str {
        match self {
            Self::Place { .. } => "place",
            Self::Cancel { .. } => "cancel",
        }
    }
}

#[derive(Deserialize)]
struct DelegateRpcMethods {
    methods: Vec<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().context(
        "usage: deepx-sdk-verify-account <snapshot|directory|ownership|delegate-ownership|delegate-order|quota|quota-history|invitation-codes|balance-changes|funding-fees|hourly-funding|liquidations|liquidation-execution|liquidation-keeper|spot-adl|perp-open-orders|perp-order|perp-order-tx|wallet-orders|wallet-trades|spot-wallet|spot-orders|spot-order|spot-trades> [arguments]",
    )?;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    match mode.as_str() {
        "snapshot" => {
            let wallet = args.next().context("snapshot requires a wallet address")?;
            let market_id = args.next().map(|value| value.parse::<u64>()).transpose()?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_snapshot(&transport, &wallet, market_id).await?;
        }
        "directory" => {
            ensure!(args.next().is_none(), "unexpected argument");
            verify_directory(&transport).await?;
        }
        "ownership" => {
            let signer = args.next().context("missing signer address")?;
            let subaccount = args.next().context("missing subaccount address")?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_ownership(&transport, &signer, &subaccount, false).await?;
        }
        "delegate-ownership" => {
            let subaccount = args.next().context("missing subaccount address")?;
            ensure!(args.next().is_none(), "unexpected argument");
            let key =
                DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
            let signer = format!("0x{}", hex::encode(derive_signer_account_id(&key)?));
            verify_ownership(&transport, &signer, &subaccount, true).await?;
        }
        "delegate-order" => {
            let wallet = args.next().context("missing wallet address")?;
            let subaccount = args.next().context("missing subaccount address")?;
            let (submit, recovery) = match args.next().as_deref() {
                None => (false, None),
                Some("--submit") => (true, None),
                Some("--cancel-existing") => (
                    false,
                    Some((
                        args.next().context("missing existing order ID")?,
                        args.next().context("missing existing extrinsic hash")?,
                    )),
                ),
                Some(_) => anyhow::bail!("expected --submit or --cancel-existing"),
            };
            ensure!(args.next().is_none(), "unexpected argument");
            verify_delegate_order(&transport, &wallet, &subaccount, submit, recovery.as_ref())
                .await?;
        }
        "quota" => {
            let wallet = args.next().context("quota requires a wallet address")?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_quota(&transport, &wallet).await?;
        }
        "quota-history" => {
            let wallet = args
                .next()
                .context("quota-history requires a wallet address")?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_quota_history(&transport, &wallet).await?;
        }
        "invitation-codes" => {
            let wallet = args
                .next()
                .context("invitation-codes requires a wallet address")?;
            ensure!(args.next().is_none(), "unexpected argument");
            let codes = transport.get_invitation_codes(BASE_URL, &wallet).await?;
            println!(
                "invitation codes observed: wallet={wallet} count={}",
                codes.len()
            );
            for code in codes {
                println!("code={code}");
            }
        }
        "balance-changes" | "liquidations" => {
            let scope = args.next().context("missing wallet or subaccount scope")?;
            let address = args.next().context("missing account address")?;
            ensure!(args.next().is_none(), "unexpected argument");
            let (subaccount, wallet) = account_scope(&scope, &address)?;
            if mode == "balance-changes" {
                verify_balance_changes(&transport, subaccount, wallet).await?;
            } else {
                verify_liquidations(&transport, subaccount, wallet).await?;
            }
        }
        "funding-fees" => {
            let scope = args.next().context("missing wallet or subaccount scope")?;
            let address = args.next().context("missing account address")?;
            let market_id = args.next().map(|value| value.parse::<u64>()).transpose()?;
            ensure!(args.next().is_none(), "unexpected argument");
            account_scope(&scope, &address)?;
            verify_funding_fees(&transport, &scope, &address, market_id).await?;
        }
        "hourly-funding" => {
            let wallet = args
                .next()
                .context("hourly-funding requires a wallet address")?;
            let market_id = args.next().context("missing market ID")?.parse::<u64>()?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_hourly_funding(&transport, &wallet, market_id).await?;
        }
        "liquidation-execution" => {
            let route = match args.next().as_deref() {
                Some("liquidation") => DeepXLiquidationExecutionRoute::Ordinary,
                Some("cranker") => DeepXLiquidationExecutionRoute::Cranker,
                Some("tp-sl") => DeepXLiquidationExecutionRoute::TpSl,
                _ => anyhow::bail!("kind must be 'liquidation', 'cranker', or 'tp-sl'"),
            };
            let scope = args.next().context("missing wallet or subaccount scope")?;
            let address = args.next().context("missing account address")?;
            ensure!(args.next().is_none(), "unexpected argument");
            let (subaccount, wallet) = account_scope(&scope, &address)?;
            verify_liquidation_execution(&transport, route, subaccount, wallet).await?;
        }
        "liquidation-keeper" => {
            let subaccount = args.next().context("missing subaccount")?;
            let chain_id = args.next();
            ensure!(args.next().is_none(), "unexpected argument");
            let state = transport
                .get_liquidation_keeper_state_raw(BASE_URL, &subaccount, chain_id.as_deref())
                .await?;
            println!("liquidation keeper state observed: subaccount={subaccount} data={state}");
        }
        "spot-adl" => {
            let target_subaccount = args.next().context("missing target subaccount")?;
            let asset = args.next().context("missing asset symbol")?;
            let liability = args.next().context("missing liability symbol")?;
            let min_cover_amount = args.next().context("missing raw minimum cover amount")?;
            let limit = args.next().map(|value| value.parse::<u32>()).transpose()?;
            ensure!(args.next().is_none(), "unexpected argument");
            let response = transport
                .get_spot_adl_candidates(
                    BASE_URL,
                    &DeepXSpotAdlCandidatesRequest {
                        target_subaccount,
                        asset,
                        liability,
                        min_cover_amount,
                        limit,
                    },
                )
                .await?;
            println!(
                "Spot ADL candidates verified: target={} asset={} liability={} min_cover_raw={} covered_raw={} candidates={}",
                response.target_subaccount,
                response.asset,
                response.liability,
                response.min_cover_amount,
                response.covered_amount,
                response.candidates.len()
            );
            for (index, candidate) in response.candidates.iter().enumerate() {
                println!(
                    "candidate index={} subaccount={} available_raw={} borrow_raw={}",
                    index,
                    candidate.subaccount,
                    candidate.available_amount,
                    candidate.borrow_amount
                );
            }
        }
        "perp-open-orders" => {
            let first = args.next().context("missing wallet or --subaccount")?;
            let subaccount_mode = first == "--subaccount";
            let address = if subaccount_mode {
                args.next().context("missing subaccount address")?
            } else {
                first
            };
            let market_id = args.next().context("missing market ID")?.parse::<u64>()?;
            ensure!(
                market_id > 0 && args.next().is_none(),
                "invalid market ID or extra argument"
            );
            if subaccount_mode {
                let pages = transport
                    .get_perp_open_order_pages(
                        BASE_URL,
                        &DeepXPerpOpenOrdersRequest {
                            subaccount: address.clone(),
                            market_id: Some(market_id),
                            is_long: None,
                            cursor: None,
                            page_size: Some(100),
                            sort: DeepXAccountSortOrder::Descending,
                        },
                        100,
                    )
                    .await?;
                let orders = pages.iter().map(|page| page.items.len()).sum::<usize>();
                println!(
                    "subaccount Perp open orders verified: address={address} market_id={market_id} pages={} orders={orders}",
                    pages.len()
                );
            } else {
                verify_perp_open_orders(&transport, &address, market_id).await?;
            }
        }
        "perp-order" => {
            let subaccount = args.next().context("missing subaccount")?;
            let market_id = args.next().context("missing market ID")?.parse::<u64>()?;
            let order_id = args.next().context("missing order ID")?;
            ensure!(args.next().is_none(), "unexpected argument");
            let order = transport
                .get_perp_order_by_id(BASE_URL, &subaccount, market_id, &order_id)
                .await?;
            println!(
                "perp order verified: owner={} market={} order_id={} status={} size={} filled={}",
                order.owner,
                order.market_id,
                order.order_id,
                order.status,
                order.size,
                order.size_filled
            );
        }
        "perp-order-tx" => {
            let tx_hash = args.next().context("missing transaction hash")?;
            ensure!(args.next().is_none(), "unexpected argument");
            let order = transport.get_perp_order_by_tx(BASE_URL, &tx_hash).await?;
            println!(
                "perp order verified: owner={} market={} order_id={} tx_hash={}",
                order.owner, order.market_id, order.order_id, order.tx_hash
            );
        }
        "wallet-orders" => {
            let wallet = args.next().context("missing wallet address")?;
            let market_id = args.next().map(|value| value.parse::<u64>()).transpose()?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_wallet_orders(&transport, &wallet, market_id).await?;
        }
        "wallet-trades" => {
            let wallet = args.next().context("missing wallet address")?;
            let market_id = args.next().map(|value| value.parse::<u64>()).transpose()?;
            let subaccount = args.next();
            let max_pages = args
                .next()
                .map(|value| value.parse::<usize>())
                .transpose()?;
            ensure!(args.next().is_none(), "unexpected argument");
            ensure!(
                subaccount.is_some() == max_pages.is_some(),
                "subaccount and max-pages must be supplied together"
            );
            ensure!(
                subaccount.is_none() || market_id.is_some(),
                "subaccount pagination requires a market ID"
            );
            verify_wallet_trades(&transport, &wallet, market_id, subaccount, max_pages).await?;
        }
        "spot-wallet" => {
            let wallet = args
                .next()
                .context("spot-wallet requires a wallet address")?;
            let market_name = args.next().context("spot-wallet requires a market name")?;
            ensure!(args.next().is_none(), "unexpected argument");
            verify_spot_wallet(&transport, &wallet, &market_name).await?;
        }
        "spot-orders" | "spot-trades" => {
            let subaccount = args.next().context("missing subaccount")?;
            let market_name = args.next().context("missing market name")?;
            ensure!(args.next().is_none(), "unexpected argument");
            if mode == "spot-orders" {
                verify_spot_orders(&transport, &subaccount, &market_name).await?;
            } else {
                verify_spot_account_trades(&transport, &subaccount, &market_name).await?;
            }
        }
        "spot-order" => {
            let subaccount = args.next().context("missing subaccount")?;
            let name = args.next().context("missing market name")?;
            let order_id = args.next().context("missing order ID")?;
            let order_side = match args.next().as_deref() {
                Some("Buy") => DeepXSpotOrderSide::Buy,
                Some("Sell") => DeepXSpotOrderSide::Sell,
                _ => anyhow::bail!("order side must be Buy or Sell"),
            };
            let tx_hash = args.next().context("missing transaction hash")?;
            ensure!(args.next().is_none(), "unexpected argument");
            let by_id = transport
                .get_spot_order_by_id(
                    BASE_URL,
                    &DeepXSpotOrderByIdRequest {
                        subaccount,
                        name: Some(name),
                        pair: None,
                        order_id,
                        order_side,
                    },
                )
                .await?;
            let by_tx = transport.get_spot_order_by_tx(BASE_URL, &tx_hash).await?;
            ensure!(by_id == by_tx, "ID and transaction lookups disagree");
            println!(
                "Spot order verified: maker={} order_id={} status={} tx_hash={}",
                by_id.maker, by_id.order_id, by_id.status, by_id.tx_hash
            );
        }
        _ => anyhow::bail!("unknown account verification mode: {mode}"),
    }
    Ok(())
}

async fn verify_snapshot(
    transport: &DeepXRestTransport,
    wallet: &str,
    market_id: Option<u64>,
) -> anyhow::Result<()> {
    let snapshot = transport
        .get_wallet_account_snapshot(BASE_URL, wallet)
        .await?;
    let invitation = transport.get_invitation_status(BASE_URL, wallet).await?;
    let quota = transport.get_quota_summary(BASE_URL, wallet).await?;
    let delegates = transport.get_delegate_accounts(BASE_URL, wallet).await?;
    ensure!(
        quota.subaccount_count == snapshot.subaccounts.len() as u64,
        "quota summary subaccount count does not match the wallet directory"
    );
    for delegate in &delegates.accounts {
        let delegators = transport
            .get_delegator_accounts(BASE_URL, &delegate.delegate_address)
            .await?;
        ensure!(
            delegators
                .wallets
                .iter()
                .any(|address| address.eq_ignore_ascii_case(wallet)),
            "reverse delegator directory does not contain the queried wallet"
        );
    }
    for subaccount in &snapshot.subaccounts {
        ensure!(
            subaccount.margin_ratio.exact_margin.is_some(),
            "validated margin ratio lacks exact values"
        );
        println!(
            "subaccount={} assets={} equity_usd={}",
            subaccount.profile.address,
            subaccount.balances.assets.len(),
            subaccount.equity.equity_usd
        );
        if let Some(market_id) = market_id {
            let price = transport
                .get_perp_liquidation_price(
                    BASE_URL,
                    &DeepXPerpLiquidationPriceRequest {
                        subaccount: subaccount.profile.address.clone(),
                        market_name: None,
                        market_id: Some(market_id),
                    },
                )
                .await?;
            println!(
                "liquidation price market={} price={:?}",
                market_id, price.liquidate_price
            );
        }
    }
    println!(
        "wallet snapshot verified: {} subaccounts={} invitation_bound={} delegates={}",
        snapshot.directory.wallet(),
        snapshot.subaccounts.len(),
        invitation.is_bound,
        delegates.accounts.len()
    );
    Ok(())
}

async fn verify_directory(transport: &DeepXRestTransport) -> anyhow::Result<()> {
    let page = transport
        .get_all_subaccounts(
            BASE_URL,
            &DeepXAllSubaccountsRequest {
                cursor: None,
                page_size: Some(5),
            },
        )
        .await?;
    ensure!(
        !page.items.is_empty(),
        "global subaccount directory is empty"
    );
    println!(
        "subaccount directory verified: records={} has_next={}",
        page.items.len(),
        page.has_next
    );
    for record in &page.items {
        println!(
            "subaccount={} owner={} status={:?}",
            record.subaccount, record.owner, record.status
        );
    }
    Ok(())
}

async fn verify_ownership(
    transport: &DeepXRestTransport,
    signer: &str,
    subaccount: &str,
    delegated: bool,
) -> anyhow::Result<()> {
    let signer_id = decode_account_id20(signer).context("invalid signer address")?;
    let profile = transport.get_subaccount_info(BASE_URL, subaccount).await?;
    let directory = transport
        .get_wallet_subaccounts(BASE_URL, &profile.authority)
        .await?;
    let proof = if delegated {
        let delegates = transport
            .get_delegate_accounts(BASE_URL, &profile.authority)
            .await?;
        let delegators = transport.get_delegator_accounts(BASE_URL, signer).await?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis();
        verify_delegated_account_ownership_evidence(
            signer_id,
            subaccount,
            &directory,
            &profile,
            &delegates,
            &delegators,
            u64::try_from(now_ms).context("current timestamp exceeds u64")?,
        )?
    } else {
        verify_account_ownership_evidence(signer_id, subaccount, &directory, &profile)?
    };
    println!(
        "ownership verified: signer=0x{} wallet=0x{} subaccount=0x{} profile_height={} delegated={delegated}",
        hex::encode(proof.signer()),
        hex::encode(proof.wallet()),
        hex::encode(proof.subaccount()),
        proof.profile_height()
    );
    Ok(())
}

async fn verify_delegate_order(
    transport: &DeepXRestTransport,
    wallet: &str,
    subaccount: &str,
    submit: bool,
    recovery: Option<&(String, String)>,
) -> anyhow::Result<()> {
    decode_account_id20(wallet).context("invalid wallet AccountId20")?;
    let subaccount_id =
        decode_account_id20(subaccount).context("invalid subaccount AccountId20")?;
    let key = DeepXPrivateKey::from_env(&DeepXEnvironment::Testnet, &DeepXKeyScheme::Secp256k1)?;
    let signer = format!("0x{}", hex::encode(derive_signer_account_id(&key)?));
    verify_ownership(transport, &signer, subaccount, true).await?;
    let profile = transport.get_subaccount_info(BASE_URL, subaccount).await?;
    ensure!(
        profile.authority.eq_ignore_ascii_case(wallet),
        "delegated signer does not control a subaccount of the requested wallet"
    );
    if let Some((order_id, expected_hash)) = recovery {
        return cancel_existing_delegate_order(
            transport,
            &key,
            subaccount,
            subaccount_id,
            order_id,
            expected_hash,
        )
        .await;
    }

    let market = transport
        .get_perp_markets(BASE_URL)
        .await?
        .into_iter()
        .find(|market| market.id == u64::from(DELEGATE_MARKET_ID))
        .context("ETH-USDC perpetual market is unavailable")?;
    ensure!(
        market.name == "ETH-USDC" && !market.is_paused,
        "market is unavailable"
    );
    ensure!(
        market.base_decimal <= 18 && market.quote_decimal == 6,
        "market scale changed"
    );
    let pages = transport
        .get_perp_open_order_pages(
            BASE_URL,
            &DeepXPerpOpenOrdersRequest {
                subaccount: subaccount.to_string(),
                market_id: Some(u64::from(DELEGATE_MARKET_ID)),
                is_long: None,
                cursor: None,
                page_size: Some(100),
                sort: DeepXAccountSortOrder::Descending,
            },
            2,
        )
        .await?;
    ensure!(
        pages.iter().all(|page| page.items.is_empty()),
        "subaccount already has open orders in the test market"
    );
    let quantity = market.order_spec_min_qty;
    ensure!(
        quantity > Decimal::ZERO && market.order_spec_step_size > Decimal::ZERO,
        "market minimum quantity is invalid"
    );
    ensure!(
        quantity % market.order_spec_step_size == Decimal::ZERO,
        "market minimum quantity is not step aligned"
    );
    let tick = market.order_spec_tick_size;
    ensure!(
        tick > Decimal::ZERO && market.mark_price > Decimal::ZERO,
        "market price is invalid"
    );
    let price = market
        .mark_price
        .checked_mul(Decimal::new(9, 1))
        .and_then(|value| value.checked_div(tick))
        .map(|value| value.floor())
        .and_then(|value| value.checked_mul(tick))
        .context("post-only test price exceeds Decimal range")?;
    let notional = price
        .checked_mul(quantity)
        .context("post-only test notional exceeds Decimal range")?;
    let guard_price = Decimal::ONE
        .checked_sub(market.limit_order_guard_limit_long)
        .and_then(|limit| market.oracle_price.checked_mul(limit))
        .context("long-side guard price exceeds Decimal range")?;
    ensure!(
        price > Decimal::ZERO
            && price < market.mark_price
            && notional >= market.order_spec_min_notional
            && price >= guard_price,
        "post-only test price violates current market constraints"
    );
    let scaled_size = quantity
        .checked_mul(Decimal::from(10_u64.pow(u32::from(market.base_decimal))))
        .context("minimum quantity exceeds Decimal range")?;
    ensure!(
        scaled_size.fract() == Decimal::ZERO,
        "minimum quantity is not an exact runtime integer"
    );
    let size_raw = scaled_size
        .to_u128()
        .context("minimum quantity cannot be represented in runtime units")?;
    let scaled_price = price
        .checked_mul(Decimal::from(1_000_000_u64))
        .context("test price exceeds Decimal range")?;
    ensure!(
        scaled_price.fract() == Decimal::ZERO,
        "test price is not an exact runtime integer"
    );
    let price_raw = scaled_price
        .to_u128()
        .context("test price cannot be represented in runtime units")?;

    let rpc = DeepXRpcClient::new_with_proxy(
        DEEPX_TESTNET_RPC_URL.to_string(),
        std::env::var("DEEPX_PROXY_URL").ok().as_deref(),
    )?;
    verify_delegate_rpc_endpoints(&rpc).await?;
    let (nonce, block_hash) = observe_delegate_nonce(&rpc, 0).await?;
    let snapshot = observe_delegate_runtime(&rpc, &block_hash).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot.clone());
    let permit = service.acquire()?;
    let place_params = DeepXPerpPlaceParams {
        subaccount: subaccount_id,
        market_id: DELEGATE_MARKET_ID,
        is_long: true,
        size: size_raw,
        price: price_raw,
        order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
        take_profit: None,
        stop_loss: None,
        reduce_only: false,
        post_only: DeepXPostOnlyParam::MustPostOnly,
    };
    let signed = sign_perp_place_order(&permit, &key, place_params, nonce)?;
    ensure!(
        dry_run_delegate_order(&rpc, &signed, &block_hash).await? == "accepted",
        "delegated place dry run did not prove validity"
    );
    println!(
        "delegated post-only place dry_run=accepted subaccount={subaccount} market={} quantity={quantity} price={price} order_id={nonce} hash=0x{}",
        DELEGATE_MARKET_ID,
        hex::encode(signed.extrinsic_hash())
    );
    if !submit {
        return Ok(());
    }
    let current_hash = rpc.get_best_block_hash_once().await?;
    let current_runtime = observe_delegate_runtime(&rpc, &current_hash).await?;
    ensure!(
        current_runtime.identity() == signed.runtime(),
        "runtime changed after place signing"
    );
    submit_and_verify_delegate_order(
        &rpc,
        &signed,
        DelegateOrderOperation::Place {
            params: place_params,
            order_id: nonce,
        },
    )
    .await?;
    match transport
        .get_perp_order_by_id(
            BASE_URL,
            subaccount,
            u64::from(DELEGATE_MARKET_ID),
            &nonce.to_string(),
        )
        .await
    {
        Ok(order) => {
            if order.size != quantity || order.price != price {
                println!(
                    "delegated order REST size or price disagrees with signed terms; continuing to cancellation"
                );
            }
            println!(
                "delegated order observed: order_id={} status={} filled={}",
                order.order_id, order.status, order.size_filled
            );
        }
        Err(error) => println!(
            "delegated order REST lookup unavailable after finality: {error}; continuing to cancellation"
        ),
    }
    let (cancel_nonce, cancel_block_hash) =
        observe_delegate_nonce(&rpc, nonce.checked_add(1).context("place nonce overflow")?).await?;
    let cancel = sign_perp_cancel(
        &permit,
        &key,
        DeepXPerpCancelParams {
            subaccount: subaccount_id,
            order_id: nonce,
            market_id: DELEGATE_MARKET_ID,
            fast_cancel: false,
        },
        cancel_nonce,
    )?;
    let cancel_dry_run = dry_run_delegate_order(&rpc, &cancel, &cancel_block_hash).await?;
    println!(
        "delegated cancel dry_run={cancel_dry_run} order_id={nonce} hash=0x{}",
        hex::encode(cancel.extrinsic_hash())
    );
    let current_hash = rpc.get_best_block_hash_once().await?;
    let current_runtime = observe_delegate_runtime(&rpc, &current_hash).await?;
    ensure!(
        current_runtime.identity() == cancel.runtime(),
        "runtime changed after cancel signing"
    );
    submit_and_verify_delegate_order(
        &rpc,
        &cancel,
        DelegateOrderOperation::Cancel {
            subaccount: subaccount_id,
            order_id: nonce,
        },
    )
    .await?;
    let order = transport
        .get_perp_order_by_id(
            BASE_URL,
            subaccount,
            u64::from(DELEGATE_MARKET_ID),
            &nonce.to_string(),
        )
        .await?;
    ensure!(
        order.status == "Canceled" && order.size_filled == Decimal::ZERO,
        "delegated order was not canceled without fills: status={} filled={}",
        order.status,
        order.size_filled
    );
    println!("delegated post-only order placed and canceled without fills: order_id={nonce}");
    Ok(())
}

async fn cancel_existing_delegate_order(
    transport: &DeepXRestTransport,
    key: &DeepXPrivateKey,
    subaccount: &str,
    subaccount_id: [u8; 20],
    order_id: &str,
    expected_hash: &str,
) -> anyhow::Result<()> {
    decode_prefixed_hash(expected_hash).context("invalid existing extrinsic hash")?;
    let order = transport
        .get_perp_order_by_id(
            BASE_URL,
            subaccount,
            u64::from(DELEGATE_MARKET_ID),
            order_id,
        )
        .await?;
    ensure!(
        order.owner.eq_ignore_ascii_case(subaccount)
            && order.status == "Open"
            && order.size_filled == Decimal::ZERO,
        "existing order is not owned, open, and unfilled"
    );
    ensure!(
        order.tx_hash_type == "EXTRINSIC_HASH"
            && order.post_only == "MustPostOnly"
            && order.order_type == "Limit",
        "existing order does not match delegated post-only placement"
    );
    ensure!(
        order.tx_hash.eq_ignore_ascii_case(expected_hash),
        "existing order transaction hash differs from supplied hash"
    );
    let order_id = order_id
        .parse::<u64>()
        .context("invalid existing order ID")?;
    let rpc = DeepXRpcClient::new_with_proxy(
        DEEPX_TESTNET_RPC_URL.to_string(),
        std::env::var("DEEPX_PROXY_URL").ok().as_deref(),
    )?;
    verify_delegate_rpc_endpoints(&rpc).await?;
    let (nonce, block_hash) = observe_delegate_nonce(
        &rpc,
        order_id
            .checked_add(1)
            .context("existing order ID overflow")?,
    )
    .await?;
    let snapshot = observe_delegate_runtime(&rpc, &block_hash).await?;
    let service = DeepXRuntimeSnapshotService::new(snapshot);
    let permit = service.acquire()?;
    let signed = sign_perp_cancel(
        &permit,
        key,
        DeepXPerpCancelParams {
            subaccount: subaccount_id,
            order_id,
            market_id: DELEGATE_MARKET_ID,
            fast_cancel: false,
        },
        nonce,
    )?;
    ensure!(
        dry_run_delegate_order(&rpc, &signed, &block_hash).await? == "accepted",
        "existing order cancellation dry run did not prove validity"
    );
    let current_hash = rpc.get_best_block_hash_once().await?;
    let current_runtime = observe_delegate_runtime(&rpc, &current_hash).await?;
    ensure!(
        current_runtime.identity() == signed.runtime(),
        "runtime changed after cancellation signing"
    );
    println!(
        "existing order cancel dry run accepted: order_id={order_id} hash=0x{}",
        hex::encode(signed.extrinsic_hash())
    );
    submit_and_verify_delegate_order(
        &rpc,
        &signed,
        DelegateOrderOperation::Cancel {
            subaccount: subaccount_id,
            order_id,
        },
    )
    .await?;
    let order = transport
        .get_perp_order_by_id(
            BASE_URL,
            subaccount,
            u64::from(DELEGATE_MARKET_ID),
            &order_id.to_string(),
        )
        .await?;
    ensure!(
        order.status == "Canceled" && order.size_filled == Decimal::ZERO,
        "existing order cancellation did not complete: status={} filled={}",
        order.status,
        order.size_filled
    );
    println!("existing delegated order canceled without fills: order_id={order_id}");
    Ok(())
}

async fn observe_delegate_nonce(
    rpc: &DeepXRpcClient,
    minimum: u64,
) -> anyhow::Result<(u64, String)> {
    let block_hash = rpc.get_best_block_hash_once().await?;
    let block: DeepXRpcBlockResponse = rpc.get_block(&block_hash).await?;
    let block_number = decode_block_number_u64(&block.block.header.number)
        .context("invalid current block number")?;
    let block_id = decode_prefixed_hash(&block_hash).context("invalid current block hash")?;
    let storage_ms = get_timestamp_now(rpc, block_id).await?;
    ensure!(
        decode_timestamp_set_inherent(&block.block.extrinsics) == Some(storage_ms),
        "current timestamp storage and block inherent disagree"
    );
    let canonical = rpc.get_block_hash_required_once(block_number).await?;
    ensure!(
        canonical.eq_ignore_ascii_case(&block_hash),
        "current block is no longer canonical"
    );
    let local_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    ensure!(
        local_ms.abs_diff(storage_ms) <= MAX_CLOCK_DRIFT_MS,
        "local clock differs too far from the DeepX chain"
    );
    Ok((local_ms.max(storage_ms).max(minimum), block_hash))
}

async fn verify_delegate_rpc_endpoints(watch: &DeepXRpcClient) -> anyhow::Result<()> {
    let url = DEEPX_TESTNET_RPC_URL.to_string();
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let submission = DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?;
    let recovery = DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?;
    let (submission_hash, watch_hash, recovery_hash) = tokio::try_join!(
        submission.get_block_hash_required_once(0),
        watch.get_block_hash_required_once(0),
        recovery.get_block_hash_required_once(0),
    )?;
    let observations = [
        (DeepXRpcRole::Submission, submission_hash),
        (DeepXRpcRole::Watch, watch_hash),
        (DeepXRpcRole::Recovery, recovery_hash),
    ]
    .into_iter()
    .map(|(role, encoded)| {
        let hash = decode_prefixed_hash(&encoded).context("invalid RPC role genesis hash")?;
        anyhow::Ok(DeepXObservedRpcEndpoint::new(role, url.clone(), hash))
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
    validate_rpc_endpoint_identities(&url, &url, &url, observations)?;
    verify_delegate_rpc_methods(&submission, DeepXRpcRole::Submission).await?;
    verify_delegate_rpc_methods(watch, DeepXRpcRole::Watch).await?;
    verify_delegate_rpc_methods(&recovery, DeepXRpcRole::Recovery).await?;
    Ok(())
}

async fn verify_delegate_rpc_methods(
    rpc: &DeepXRpcClient,
    role: DeepXRpcRole,
) -> anyhow::Result<()> {
    let required = match role {
        DeepXRpcRole::Submission => DEEPX_SUBMISSION_RPC_METHODS,
        DeepXRpcRole::Watch => DEEPX_WATCH_RPC_METHODS,
        DeepXRpcRole::Recovery => DEEPX_RECOVERY_RPC_METHODS,
    };
    let observed: DelegateRpcMethods = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "rpc_methods", "params": [],
        }))
        .await?;
    let methods = observed
        .methods
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    ensure!(
        required.iter().all(|method| methods.contains(method)),
        "RPC endpoint for {role:?} lacks required methods"
    );
    if role == DeepXRpcRole::Watch {
        ensure!(
            [
                "state_getMetadata",
                "state_getRuntimeVersion",
                "system_dryRunAt"
            ]
            .iter()
            .all(|method| methods.contains(method)),
            "RPC watch endpoint lacks methods required for delegated order verification"
        );
    }
    Ok(())
}

async fn observe_delegate_runtime(
    rpc: &DeepXRpcClient,
    block_hash: &str,
) -> anyhow::Result<RuntimeSnapshot> {
    let version: DeepXRpcRuntimeVersion = get_runtime_version(rpc, block_hash).await?;
    let metadata = get_metadata(rpc, block_hash).await?;
    let metadata = hex::decode(
        metadata
            .strip_prefix("0x")
            .context("invalid metadata hex")?,
    )?;
    let genesis = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
        .context("invalid approved genesis hash")?;
    RuntimeSnapshot::approved_testnet(
        &DeepXEnvironment::Testnet,
        genesis,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )
    .map_err(Into::into)
}

async fn dry_run_delegate_order(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    block_hash: &str,
) -> anyhow::Result<&'static str> {
    let result: String = rpc
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "system_dryRunAt",
            "params": [format!("0x{}", hex::encode(signed.bytes())), block_hash],
        }))
        .await?;
    ensure!(
        result == "0x0000" || result == "0x01000b",
        "delegated order dry run failed: {result}"
    );
    Ok(if result == "0x0000" {
        "accepted"
    } else {
        "inconclusive"
    })
}

async fn submit_and_verify_delegate_order(
    rpc: &DeepXRpcClient,
    signed: &DeepXPermittedPalletExtrinsic,
    operation: DelegateOrderOperation,
) -> anyhow::Result<()> {
    let name = operation.name();
    let encoded = format!("0x{}", hex::encode(signed.bytes()));
    let pending = rpc.get_pending_extrinsics().await?;
    ensure!(
        !pending
            .iter()
            .any(|item| item.eq_ignore_ascii_case(&encoded)),
        "delegated {name} extrinsic is already pending"
    );
    let finalized_before = get_finalized_head(rpc).await?;
    let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_before).await?;
    let mut next_height = decode_block_number_u64(&header.number)
        .context("invalid finalized block number")?
        .checked_add(1)
        .context("finalized block number overflow")?;
    let submitted_hash = rpc.submit_encoded_extrinsic(&encoded).await?;
    ensure!(
        submitted_hash.eq_ignore_ascii_case(&format!("0x{}", hex::encode(signed.extrinsic_hash()))),
        "submission endpoint returned another extrinsic hash"
    );
    println!("delegated {name} submitted: hash={submitted_hash}; awaiting finality");
    let deadline = Instant::now() + FINALITY_TIMEOUT;
    loop {
        let finalized_hash = get_finalized_head(rpc).await?;
        let header: DeepXRpcBlockHeader = rpc.get_header_once(&finalized_hash).await?;
        let finalized_height =
            decode_block_number_u64(&header.number).context("invalid finalized block number")?;
        for height in next_height..=finalized_height {
            let block_hash = rpc.get_block_hash_required_once(height).await?;
            let block: DeepXRpcBlockResponse = rpc.get_block(&block_hash).await?;
            ensure!(
                decode_block_number_u64(&block.block.header.number) == Some(height),
                "finalized block height mismatch"
            );
            let indices = block
                .block
                .extrinsics
                .iter()
                .enumerate()
                .filter(|(_, candidate)| candidate.eq_ignore_ascii_case(&encoded))
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if indices.is_empty() {
                continue;
            }
            ensure!(indices.len() == 1, "duplicate extrinsic in finalized block");
            let index = u32::try_from(indices[0]).context("extrinsic index exceeds u32")?;
            let events = get_system_events(rpc, &block_hash, height).await?;
            let repeated = get_system_events(rpc, &block_hash, height).await?;
            ensure!(events == repeated, "finalized event storage changed");
            let runtime = observe_delegate_runtime(rpc, &block_hash).await?;
            let success = match operation {
                DelegateOrderOperation::Place { params, order_id } => {
                    decode_perp_place_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXPerpPlaceExpected {
                            params,
                            order_id,
                            block_number: height,
                            extrinsic_index: index,
                        },
                        &events,
                    )?
                    .outcome
                        == DeepXPerpPlaceInclusionOutcome::Success
                }
                DelegateOrderOperation::Cancel {
                    subaccount,
                    order_id,
                } => {
                    decode_perp_cancel_inclusion(
                        runtime.interfaces(),
                        runtime.metadata(),
                        DeepXPerpCancelExpected {
                            subaccount,
                            order_id,
                            extrinsic_index: index,
                        },
                        false,
                        &events,
                    )?
                    .outcome
                        == DeepXPerpCancelInclusionOutcome::Success
                }
            };
            let canonical = rpc.get_block_hash_required_once(height).await?;
            ensure!(
                canonical.eq_ignore_ascii_case(&block_hash),
                "finalized block hash changed during dispatch verification"
            );
            ensure!(
                success,
                "delegated {name} finalized business outcome failed"
            );
            println!(
                "delegated {name} finalized: block={height} index={index} hash={submitted_hash} outcome=success"
            );
            return Ok(());
        }
        next_height = finalized_height
            .checked_add(1)
            .context("finalized block number overflow")?;
        ensure!(
            Instant::now() < deadline,
            "delegated {name} finality was not observed within the bounded wait; reconcile hash={submitted_hash}"
        );
        tokio::time::sleep(FINALITY_POLL_INTERVAL).await;
    }
}

async fn verify_quota(transport: &DeepXRestTransport, wallet: &str) -> anyhow::Result<()> {
    let summary = transport.get_quota_summary(BASE_URL, wallet).await?;
    println!(
        "quota verified: owner={} subaccounts={} spot_volume_usd={} perp_volume_usd={} total_volume_usd={} earned={} granted={} reserved={} pending={}",
        summary.owner,
        summary.subaccount_count,
        summary.spot_volume_usd,
        summary.perp_volume_usd,
        summary.total_volume_usd,
        summary.quota_earned,
        summary.quota_granted,
        summary.quota_reserved,
        summary.quota_pending,
    );
    println!(
        "first_trade_ts_ms={:?} last_trade_ts_ms={:?} first_trade_at={:?} last_trade_at={:?} updated_at={:?}",
        summary.first_trade_ts_ms,
        summary.last_trade_ts_ms,
        summary.first_trade_at,
        summary.last_trade_at,
        summary.updated_at,
    );
    Ok(())
}

fn account_scope(scope: &str, address: &str) -> anyhow::Result<(Option<String>, Option<String>)> {
    match scope {
        "wallet" => Ok((None, Some(address.to_string()))),
        "subaccount" => Ok((Some(address.to_string()), None)),
        _ => anyhow::bail!("scope must be 'wallet' or 'subaccount'"),
    }
}

async fn verify_quota_history(transport: &DeepXRestTransport, wallet: &str) -> anyhow::Result<()> {
    let page = transport
        .get_quota_history(
            BASE_URL,
            &DeepXQuotaHistoryRequest {
                wallet: wallet.to_string(),
                buyer_address: None,
                history_type: None,
                cursor: None,
                limit: Some(5),
            },
        )
        .await?;
    ensure!(!page.items.is_empty(), "wallet quota history is empty");
    println!(
        "quota history verified: wallet={wallet} records={} has_next={}",
        page.items.len(),
        page.has_next
    );
    for record in page.items {
        println!(
            "id={} type={:?} quota={} block={} event={} created_at={}",
            record.id,
            record.history_type,
            record.quota,
            record.block_number,
            record.event_index,
            record.created_at
        );
    }
    Ok(())
}

async fn verify_balance_changes(
    transport: &DeepXRestTransport,
    subaccount: Option<String>,
    wallet: Option<String>,
) -> anyhow::Result<()> {
    let pages = transport
        .get_balance_change_pages(
            BASE_URL,
            &DeepXBalanceChangesRequest {
                subaccount,
                wallet,
                start_ms: None,
                end_ms: None,
                change_types: Vec::new(),
                cursor: None,
                page_size: Some(PAGE_SIZE),
            },
            MAX_PAGES,
        )
        .await?;
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "balance changes verified: pages={} records={records}",
        pages.len()
    );
    for change in pages.into_iter().flat_map(|page| page.items) {
        println!(
            "id={} type={:?} asset={} amount={} time={} height={} event_idx={} position_owner={}",
            change.id,
            change.change_type,
            change.asset,
            change.balance_change,
            change.time,
            change.height,
            change.event_idx,
            change
                .position
                .as_ref()
                .map_or("none", |position| position.owner.as_str())
        );
    }
    Ok(())
}

async fn verify_funding_fees(
    transport: &DeepXRestTransport,
    scope: &str,
    address: &str,
    market_id: Option<u64>,
) -> anyhow::Result<()> {
    let pages = if scope == "wallet" {
        transport
            .get_wallet_funding_fee_pages(
                BASE_URL,
                &DeepXWalletFundingFeeRequest {
                    wallet: address.to_string(),
                    market_name: None,
                    market_id,
                    start_ms: None,
                    end_ms: None,
                    cursor: None,
                    page_size: Some(PAGE_SIZE),
                },
                MAX_PAGES,
            )
            .await?
    } else {
        transport
            .get_perp_funding_fee_pages(
                BASE_URL,
                &DeepXPerpFundingFeeRequest {
                    subaccount: address.to_string(),
                    market_id,
                    start_ms: None,
                    end_ms: None,
                    cursor: None,
                    page_size: Some(PAGE_SIZE),
                },
                MAX_PAGES,
            )
            .await?
    };
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "funding fees verified: scope={scope} address={address} pages={} records={records}",
        pages.len()
    );
    for fee in pages.into_iter().flat_map(|page| page.items) {
        println!(
            "height={} event_idx={} market={} side={} position_size={} fee={} rate={} settled={} created_at={}",
            fee.height,
            fee.event_idx,
            fee.market,
            if fee.is_long { "long" } else { "short" },
            fee.position_size,
            fee.fee,
            fee.fee_rate,
            fee.is_settled,
            fee.created_at
        );
    }
    Ok(())
}

async fn verify_hourly_funding(
    transport: &DeepXRestTransport,
    wallet: &str,
    market_id: u64,
) -> anyhow::Result<()> {
    let pages = transport
        .get_hourly_unsettled_funding_pages(
            BASE_URL,
            &DeepXHourlyUnsettledFundingRequest {
                subaccount: None,
                wallet: Some(wallet.to_string()),
                market_id: Some(market_id),
                start_ms: Some(CAPTURE_START_MS),
                end_ms: Some(CAPTURE_END_MS),
                cursor: None,
                page_size: Some(5),
                sort: DeepXAccountSortOrder::Descending,
            },
            3,
        )
        .await?;
    let records = pages.iter().map(Vec::len).sum::<usize>();
    ensure!(records > 0, "bounded verification returned no records");
    println!(
        "hourly funding verified: wallet={wallet} market={market_id} pages={} records={records}",
        pages.len()
    );
    for record in pages.into_iter().flatten() {
        println!(
            "subaccount={} market={} position_version={} signed_position_size_raw={} baseline_index_raw={} cumulative_index_raw={} delta_index_raw={} mark_price_raw={} payment_raw={} boundary_timestamp_ms={} boundary_block={} boundary_event_index={} boundary_event_id={}",
            record.subaccount,
            record.market_id,
            record.position_version,
            record.signed_position_size_raw,
            record.baseline_index_raw,
            record.cumulative_index_raw,
            record.delta_index_raw,
            record.mark_price_raw,
            record.payment_raw,
            record.boundary_timestamp_ms,
            record.boundary_block,
            record.boundary_event_index,
            record.boundary_event_id
        );
    }
    Ok(())
}

async fn verify_liquidations(
    transport: &DeepXRestTransport,
    subaccount: Option<String>,
    wallet: Option<String>,
) -> anyhow::Result<()> {
    let pages = transport
        .get_liquidation_record_pages(
            BASE_URL,
            &DeepXLiquidationRecordsRequest {
                subaccount,
                wallet,
                liquidation_types: Vec::new(),
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(PAGE_SIZE),
            },
            MAX_PAGES,
        )
        .await?;
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "liquidations verified: pages={} records={records}",
        pages.len()
    );
    for record in pages.into_iter().flat_map(|page| page.items) {
        println!(
            "id={} liquidation_id={} type={:?} target={} liquidator={} market={:?} margin_shortage={} margin_freed={} liquidator_fee={:?} if_fee={} liquidate_base_amount={:?} oracle_price={:?} liquidator_order_id={:?} target_order_id={:?} borrow_amount={:?} bankrupt={} height={} event_idx={} created_at={} tx_hash={}",
            record.id,
            record.liquidation_id,
            record.liquidation_type,
            record.target_account,
            record.liquidator,
            record.market_index,
            record.margin_shortage,
            record.margin_freed,
            record.liquidator_fee,
            record.if_fee,
            record.liquidate_base_amount,
            record.oracle_price,
            record.liquidator_order_id,
            record.target_account_order_id,
            record.borrow_amount,
            record.bankrupt,
            record.height,
            record.event_idx,
            record.created_at,
            record.tx_hash.as_deref().unwrap_or("none")
        );
    }
    Ok(())
}

async fn verify_liquidation_execution(
    transport: &DeepXRestTransport,
    route: DeepXLiquidationExecutionRoute,
    subaccount: Option<String>,
    wallet: Option<String>,
) -> anyhow::Result<()> {
    let pages = transport
        .get_liquidation_execution_record_pages_raw(
            BASE_URL,
            route,
            &DeepXLiquidationExecutionRecordsRequest {
                subaccount,
                wallet,
                status: None,
                reason: None,
                tx_hash: None,
                extrinsic_hash: None,
                trigger_block_from: None,
                trigger_block_to: None,
                start_ms: None,
                end_ms: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(PAGE_SIZE),
            },
            MAX_PAGES,
        )
        .await?;
    let records = pages.iter().map(|page| page.items.len()).sum::<usize>();
    println!(
        "liquidation execution observed: route={route:?} pages={} records={records}",
        pages.len()
    );
    for item in pages.into_iter().flat_map(|page| page.items) {
        println!("record={item}");
    }
    Ok(())
}

async fn verify_perp_open_orders(
    transport: &DeepXRestTransport,
    wallet: &str,
    market_id: u64,
) -> anyhow::Result<()> {
    let directory = transport.get_wallet_subaccounts(BASE_URL, wallet).await?;
    let mut total_orders = 0;
    for subaccount in &directory.addresses {
        let pages = transport
            .get_perp_open_order_pages(
                BASE_URL,
                &DeepXPerpOpenOrdersRequest {
                    subaccount: subaccount.clone(),
                    market_id: Some(market_id),
                    is_long: None,
                    cursor: None,
                    page_size: Some(100),
                    sort: DeepXAccountSortOrder::Descending,
                },
                20,
            )
            .await
            .with_context(|| format!("open-order query failed for {subaccount}"))?;
        let orders = pages.iter().map(|page| page.items.len()).sum::<usize>();
        total_orders += orders;
        println!(
            "subaccount={subaccount} market_id={market_id} pages={} open_orders={orders}",
            pages.len()
        );
    }
    let final_directory = transport.get_wallet_subaccounts(BASE_URL, wallet).await?;
    let addresses = |values: &[String]| {
        values
            .iter()
            .map(|address| address.to_ascii_lowercase())
            .collect::<HashSet<_>>()
    };
    ensure!(
        addresses(&directory.addresses) == addresses(&final_directory.addresses),
        "wallet subaccount directory changed during open-order query"
    );
    println!(
        "wallet Perp open orders verified: wallet={} subaccounts={} orders={total_orders}",
        directory.wallet(),
        directory.addresses.len()
    );
    Ok(())
}

async fn verify_spot_wallet(
    transport: &DeepXRestTransport,
    wallet: &str,
    market_name: &str,
) -> anyhow::Result<()> {
    let orders = transport
        .get_spot_wallet_orders(
            BASE_URL,
            &DeepXSpotWalletOrdersRequest {
                wallet: Some(wallet.to_string()),
                name: Some(market_name.to_string()),
                pair: None,
                order_side: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                start_ms: None,
                end_ms: None,
                page_size: Some(5),
            },
        )
        .await?;
    let trades = transport
        .get_spot_wallet_trades(
            BASE_URL,
            &DeepXSpotWalletTradesRequest {
                wallet: wallet.to_string(),
                name: Some(market_name.to_string()),
                pair: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                start_ms: None,
                end_ms: None,
                page_size: Some(5),
            },
        )
        .await?;
    let order_count = orders
        .markets
        .iter()
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.orders.items.len())
        .sum::<usize>();
    let trade_count = trades
        .markets
        .iter()
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.trades.items.len())
        .sum::<usize>();
    ensure!(
        order_count > 0 || !orders.has_next,
        "empty Spot wallet order page advertises a continuation"
    );
    ensure!(
        trade_count > 0 || !trades.has_next,
        "empty Spot wallet trade page advertises a continuation"
    );
    println!(
        "Spot wallet verified: wallet={wallet} market={market_name} orders={order_count} trades={trade_count}"
    );
    Ok(())
}

async fn verify_spot_orders(
    transport: &DeepXRestTransport,
    subaccount: &str,
    market_name: &str,
) -> anyhow::Result<()> {
    let open = transport
        .get_spot_open_orders(
            BASE_URL,
            &DeepXSpotOpenOrdersRequest {
                subaccount: subaccount.to_string(),
                name: Some(market_name.to_string()),
                pair: None,
                order_side: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(5),
            },
        )
        .await?;
    let history = transport
        .get_spot_history_orders(
            BASE_URL,
            &DeepXSpotHistoryOrdersRequest {
                subaccount: subaccount.to_string(),
                name: Some(market_name.to_string()),
                pair: None,
                order_side: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                page_size: Some(5),
            },
        )
        .await?;
    ensure!(
        !open.items.is_empty() || !history.items.is_empty(),
        "both Spot order pages are empty for the requested subaccount and market"
    );
    println!(
        "Spot orders verified: subaccount={subaccount} market={market_name} open={} history={}",
        open.items.len(),
        history.items.len()
    );
    Ok(())
}

async fn verify_wallet_orders(
    transport: &DeepXRestTransport,
    wallet: &str,
    market_id: Option<u64>,
) -> anyhow::Result<()> {
    let pages = transport
        .get_perp_wallet_order_pages(
            BASE_URL,
            &DeepXPerpWalletOrdersRequest {
                wallet: wallet.to_string(),
                market_name: None,
                market_id,
                is_long: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                start_ms: None,
                end_ms: None,
                page_size: Some(5),
            },
            MAX_PAGES,
        )
        .await?;
    let records = pages
        .iter()
        .flat_map(|page| &page.markets)
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.orders.items.len())
        .sum::<usize>();
    ensure!(records > 0, "wallet order history is empty");
    let markets = pages
        .iter()
        .flat_map(|page| &page.markets)
        .map(|market| market.market_id)
        .collect::<HashSet<_>>();
    let subaccounts = pages
        .iter()
        .flat_map(|page| &page.markets)
        .flat_map(|market| &market.subaccounts)
        .map(|subaccount| subaccount.subaccount.as_str())
        .collect::<HashSet<_>>();
    println!(
        "wallet orders verified: wallet={wallet} pages={} records={records} markets={} subaccounts={}",
        pages.len(),
        markets.len(),
        subaccounts.len()
    );
    Ok(())
}

async fn verify_wallet_trades(
    transport: &DeepXRestTransport,
    wallet: &str,
    market_id: Option<u64>,
    subaccount: Option<String>,
    max_pages: Option<usize>,
) -> anyhow::Result<()> {
    let request = DeepXPerpWalletTradesRequest {
        wallet: wallet.to_string(),
        market_name: None,
        market_id,
        cursor: None,
        sort: DeepXAccountSortOrder::Descending,
        start_ms: None,
        end_ms: None,
        page_size: Some(5),
    };
    let groups = transport.get_perp_wallet_trades(BASE_URL, &request).await?;
    ensure!(!groups.is_empty(), "wallet trade snapshot is empty");
    let trades = groups
        .iter()
        .flat_map(|market| &market.subaccounts)
        .map(|group| group.trades.items.len())
        .sum::<usize>();
    let subaccount_groups = groups
        .iter()
        .map(|market| market.subaccounts.len())
        .sum::<usize>();
    let subaccounts = groups
        .iter()
        .flat_map(|market| &market.subaccounts)
        .map(|group| group.subaccount.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let zero_size = groups
        .iter()
        .flat_map(|market| &market.subaccounts)
        .flat_map(|group| &group.trades.items)
        .filter(|trade| trade.size == Decimal::ZERO)
        .count();
    let continuation_groups = groups
        .iter()
        .flat_map(|market| &market.subaccounts)
        .filter(|group| group.trades.has_next)
        .count();
    let distinct_cursors = groups
        .iter()
        .flat_map(|market| &market.subaccounts)
        .filter(|group| group.trades.has_next)
        .filter_map(|group| group.trades.next_cursor.as_deref())
        .collect::<HashSet<_>>()
        .len();
    println!(
        "wallet trades verified: wallet={wallet} markets={} subaccounts={} subaccount_groups={subaccount_groups} trades={trades} zero_size={zero_size} continuation_groups={continuation_groups} distinct_cursors={distinct_cursors}",
        groups.len(),
        subaccounts.len()
    );
    if let (Some(subaccount), Some(max_pages)) = (subaccount, max_pages) {
        let expected_market_name = groups
            .iter()
            .find(|group| Some(group.market_id) == market_id)
            .context("selected wallet trade market is absent")?
            .market_name
            .as_str();
        let mut pagination_request = request;
        pagination_request.page_size = Some(100);
        let pages = transport
            .get_perp_wallet_trade_pages_for_subaccount(
                BASE_URL,
                &pagination_request,
                &subaccount,
                expected_market_name,
                max_pages,
            )
            .await?;
        let count = pages.iter().map(|page| page.items.len()).sum::<usize>();
        println!(
            "wallet trade continuation verified: subaccount={subaccount} pages={} trades={count} terminal={}",
            pages.len(),
            pages.last().is_some_and(|page| !page.has_next)
        );
    }
    Ok(())
}

async fn verify_spot_account_trades(
    transport: &DeepXRestTransport,
    subaccount: &str,
    market_name: &str,
) -> anyhow::Result<()> {
    let page = transport
        .get_spot_account_trades(
            BASE_URL,
            &DeepXSpotAccountTradesRequest {
                subaccount: subaccount.to_string(),
                order_id: None,
                order_side: None,
                name: Some(market_name.to_string()),
                pair: None,
                cursor: None,
                sort: DeepXAccountSortOrder::Descending,
                start_ms: None,
                end_ms: None,
                page_size: Some(5),
            },
        )
        .await?;
    ensure!(
        !page.items.is_empty(),
        "Spot account-trade page is empty for the requested subaccount and market"
    );
    let latest = &page.items[0];
    println!(
        "Spot trades verified: subaccount={subaccount} market={market_name} count={} latest_id={} order_id={} price={} amount={}",
        page.items.len(),
        latest.id,
        latest.order_id,
        latest.price,
        latest.base_amount
    );
    Ok(())
}
