// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License. You may obtain a copy of the
//  License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only verification of finalized DeepX active perpetual-order storage.

use anyhow::Context;
use nautilus_core::hex;
use nautilus_deepx::{
    DeepXStoredOrderStatus, DeepXStoredPerpOrderType, DeepXStoredPostOnlyParam,
    config::DeepXDataClientConfig,
    http::{DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpHistoryOrdersRequest},
    observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
    observe_finalized_perp_orders_by_id_evidence, observe_finalized_perp_orders_evidence,
};
use rust_decimal::Decimal;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let encoded_subaccount = args
        .next()
        .context("usage: deepx-verify-finalized-perp-orders <subaccount-account-id20> [--history <market-id>]")?;
    let subaccount = hex::decode_array::<20>(
        encoded_subaccount
            .strip_prefix("0x")
            .context("subaccount must be 0x-prefixed")?,
    )
    .context("subaccount must contain exactly 20 bytes of hex")?;

    let config = DeepXDataClientConfig::default();
    let endpoints = observe_and_validate_rpc_endpoint_identities(&config.network).await?;
    let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints).await?;
    if let Some(mode) = args.next() {
        anyhow::ensure!(mode == "--history", "unsupported mode {mode}");
        let market_id = args
            .next()
            .context("--history requires a market ID")?
            .parse::<u64>()
            .context("market ID must be a decimal u64")?;
        anyhow::ensure!(args.next().is_none(), "unexpected extra argument");
        return verify_history(
            &config,
            &endpoints,
            &capabilities,
            encoded_subaccount,
            subaccount,
            market_id,
        )
        .await;
    }
    let evidence = observe_finalized_perp_orders_evidence(
        &config.network.environment,
        &endpoints,
        &capabilities,
        subaccount,
    )
    .await?;
    anyhow::ensure!(
        evidence.subaccount() == subaccount,
        "finalized order evidence belongs to another subaccount"
    );

    let checkpoint = evidence.checkpoint();
    println!(
        "DeepX finalized active perpetual orders verified subaccount=0x{} block={} hash=0x{} spec_version={} transaction_version={} orders={}",
        hex::encode(evidence.subaccount()),
        checkpoint.block_number(),
        hex::encode(checkpoint.block_hash()),
        evidence.runtime_identity().spec_version,
        evidence.runtime_identity().transaction_version,
        evidence.orders().len(),
    );
    for order in evidence.orders() {
        println!(
            "market={} order_id={} owner=0x{} is_long={} size={} price={} order_type={:?} create_time={} leverage={} status={:?} size_filled={} size_remain={} take_profit={:?} stop_loss={:?} reduce_only={} post_only={:?}",
            order.market_id(),
            order.order_id(),
            hex::encode(order.owner()),
            order.is_long(),
            order.size(),
            order.price(),
            order.order_type(),
            order.create_time(),
            order.leverage(),
            order.status(),
            order.size_filled(),
            order.size_remain(),
            order.take_profit(),
            order.stop_loss(),
            order.reduce_only(),
            order.post_only(),
        );
    }
    println!(
        "Values are raw finalized runtime storage evidence; no REST lifecycle, framework report, credentials, signing, or transactions were used"
    );
    Ok(())
}

async fn verify_history(
    config: &DeepXDataClientConfig,
    endpoints: &nautilus_deepx::config::DeepXValidatedRpcEndpoints,
    capabilities: &nautilus_deepx::rpc::DeepXValidatedRpcMethodCapabilities,
    encoded_subaccount: String,
    subaccount: [u8; 20],
    market_id: u64,
) -> anyhow::Result<()> {
    let http = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url.clone(),
    )?;
    let market = http
        .get_perp_markets()
        .await?
        .into_iter()
        .find(|market| market.id == market_id)
        .context("Perpetual market is absent from the REST directory")?;
    let pages = http
        .get_perp_history_order_pages(
            &DeepXPerpHistoryOrdersRequest {
                subaccount: encoded_subaccount,
                market_id: Some(market_id),
                cursor: None,
                page_size: Some(100),
                sort: DeepXAccountSortOrder::Ascending,
            },
            100,
        )
        .await?;
    let records = pages
        .into_iter()
        .flat_map(|page| page.items)
        .collect::<Vec<_>>();
    let order_ids = records
        .iter()
        .map(|record| {
            record
                .order_id
                .parse::<u64>()
                .context("Perpetual order ID is invalid")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let evidence = observe_finalized_perp_orders_by_id_evidence(
        &config.network.environment,
        endpoints,
        capabilities,
        subaccount,
        &order_ids,
    )
    .await?;
    anyhow::ensure!(evidence.subaccount() == subaccount);
    println!(
        "DeepX finalized Perpetual history verified subaccount=0x{} market_id={} finalized_block={} rest_records={} chain_records={}",
        hex::encode(subaccount),
        market_id,
        evidence.checkpoint().block_number(),
        records.len(),
        evidence.orders().len(),
    );
    anyhow::ensure!(evidence.orders().len() == records.len());
    for record in &records {
        let order_id = record.order_id.parse::<u64>()?;
        let order = evidence
            .orders()
            .iter()
            .find(|order| order.order_id() == order_id)
            .context("REST history order is absent from finalized storage")?;
        anyhow::ensure!(
            order.owner() == subaccount
                && u64::from(order.market_id()) == record.market_id
                && order.create_time() == record.height,
            "REST and finalized order identity or creation height differ"
        );
        let runtime_decimal = |value: u128, scale: u32| -> anyhow::Result<Decimal> {
            Ok(Decimal::from_i128_with_scale(i128::try_from(value)?, scale))
        };
        anyhow::ensure!(
            record.size == runtime_decimal(order.size(), u32::from(market.base_decimal))?
                && record.size_filled
                    == runtime_decimal(order.size_filled(), u32::from(market.base_decimal))?
                && record.size_remain
                    == runtime_decimal(order.size_remain(), u32::from(market.base_decimal))?
                && record.reduce_only == order.reduce_only()
                && record.is_long == order.is_long(),
            "REST and finalized order quantity or flags differ for order {order_id}"
        );
        let expected_leverage = Decimal::from_i128_with_scale(i128::from(order.leverage()), 3);
        anyhow::ensure!(
            record.leverage == expected_leverage,
            "REST and finalized leverage differ for order {order_id}"
        );
        let (expected_type, expected_slippage) = match order.order_type() {
            DeepXStoredPerpOrderType::Limit(_) => ("Limit", Decimal::ZERO),
            DeepXStoredPerpOrderType::Market(slippage) => {
                ("Market", Decimal::from(slippage.unwrap_or_default()))
            }
            DeepXStoredPerpOrderType::Stop => ("Stop", Decimal::ZERO),
        };
        anyhow::ensure!(
            record.order_type == expected_type && record.slippage == expected_slippage,
            "REST and finalized order type or slippage differ for order {order_id}"
        );
        let expected_status = match order.status() {
            DeepXStoredOrderStatus::Open => "Open",
            DeepXStoredOrderStatus::PartiallyFilled => "PartiallyFilled",
            DeepXStoredOrderStatus::Filled => "Filled",
            DeepXStoredOrderStatus::Canceled => "Canceled",
            DeepXStoredOrderStatus::Rejected => "Rejected",
            DeepXStoredOrderStatus::Expired => "Expired",
        };
        let expected_post_only = match order.post_only() {
            DeepXStoredPostOnlyParam::None => "None",
            DeepXStoredPostOnlyParam::MustPostOnly => "MustPostOnly",
            DeepXStoredPostOnlyParam::Adaptive => "Adaptive",
        };
        anyhow::ensure!(
            record.status == expected_status && record.post_only == expected_post_only,
            "REST and finalized lifecycle fields differ for order {order_id}"
        );
        let expected_take_profit = order
            .take_profit()
            .map(|value| runtime_decimal(value, 6))
            .transpose()?;
        let expected_stop_loss = order
            .stop_loss()
            .map(|value| runtime_decimal(value, 6))
            .transpose()?;
        anyhow::ensure!(
            record.take_profit == expected_take_profit && record.stop_loss == expected_stop_loss,
            "REST and finalized trigger fields differ for order {order_id}"
        );
        if let DeepXStoredPerpOrderType::Limit(_) = order.order_type() {
            anyhow::ensure!(
                record.price == runtime_decimal(order.price(), 6)?,
                "REST and finalized limit prices differ for order {order_id}"
            );
        }
        println!(
            "order_id={} type={} status={} height={} leverage={} slippage={} size={} filled={} remaining={} price={} take_profit={:?} stop_loss={:?}",
            record.order_id,
            expected_type,
            expected_status,
            record.height,
            record.leverage,
            record.slippage,
            record.size,
            record.size_filled,
            record.size_remain,
            record.price,
            record.take_profit,
            record.stop_loss,
        );
    }
    println!(
        "REST lifecycle fields matched the corresponding same-checkpoint finalized OrderInfo evidence; no signing or transactions were used"
    );
    Ok(())
}
