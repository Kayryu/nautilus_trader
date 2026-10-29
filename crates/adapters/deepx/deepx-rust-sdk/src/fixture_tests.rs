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

use std::{fs, path::Path};

use rstest::rstest;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    query::{
        DeepXBalanceChangeType, DeepXLiquidationType, DeepXQuotaBuyerType, DeepXQuotaHistoryType,
    },
    rest::{
        DeepXAccountPage, DeepXApiCode, DeepXApiResponse,
        models::{
            DeepXBalanceChangeRecord, DeepXDelegateAccount, DeepXDelegateMode,
            DeepXHourlyUnsettledFundingRecord, DeepXLendingAsset, DeepXLendingInterestRateHistory,
            DeepXLendingInterestRateParams, DeepXLendingStatusHistory, DeepXLiquidationRecord,
            DeepXPerpAccountTradeRecord, DeepXPerpFundingFeeRecord, DeepXPerpLiquidationPrice,
            DeepXPerpMarketLookup, DeepXPerpOrderBook, DeepXPerpOrderRecord,
            DeepXPerpPositionRecord, DeepXPerpVolume, DeepXPerpWalletOrderMarket,
            DeepXPerpWalletTradeMarket, DeepXQuotaHistoryRecord, DeepXQuotaSummary,
            DeepXSpotAccountTradeRecord, DeepXSpotCandlesPage, DeepXSpotLastPrice, DeepXSpotMarket,
            DeepXSpotOrderBook, DeepXSpotOrderRecord, DeepXSpotTradesPage, DeepXSpotVolume,
            DeepXSpotWalletOrderMarket, DeepXSpotWalletTradeMarket, DeepXSubaccountBalances,
            DeepXSubaccountDirectoryRecord, DeepXSubaccountEquity, DeepXSubaccountMarginRatio,
            DeepXSubaccountProfile, DeepXUserStats,
        },
    },
};

const PERP_VOLUME_1H_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_volume_1h.json");
const PERP_VOLUME_1H_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_volume_1h.manifest.json");
const SPOT_TRADES_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_trades_eth_usdc_page_1.json");
const SPOT_TRADES_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_trades_eth_usdc_page_1.manifest.json");
const SPOT_CANDLES_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_candles_eth_usdc_1m.json");
const SPOT_CANDLES_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_candles_eth_usdc_1m.manifest.json");
const SPOT_LAST_PRICE_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_last_price_eth_usdc.json");
const SPOT_LAST_PRICE_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_last_price_eth_usdc.manifest.json");
const SPOT_VOLUME_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_volume_eth_usdc_1h.json");
const SPOT_VOLUME_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_volume_eth_usdc_1h.manifest.json");
const SPOT_ORDER_BOOK_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_order_book_eth_usdc_tick_001.json");
const SPOT_ORDER_BOOK_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_order_book_eth_usdc_tick_001.manifest.json");
const PERP_ORDER_BOOK_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_order_book_eth_usdc_tick_001.json");
const PERP_ORDER_BOOK_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_order_book_eth_usdc_tick_001.manifest.json");
const SPOT_MARKETS_SPEC369_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_markets_spec369.json");
const SPOT_MARKETS_SPEC369_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_markets_spec369.manifest.json");
const SPOT_MARKET_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_market_eth_usdc_by_pair.json");
const SPOT_MARKET_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_market_eth_usdc_by_pair.manifest.json");
const PERP_MARKET_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_market_eth_usdc_by_id.json");
const PERP_MARKET_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_market_eth_usdc_by_id.manifest.json");
const SUBACCOUNT_MARGIN_RATIO_EXACT_RESPONSE: &str =
    include_str!("../test_data/http/testnet/subaccount_margin_ratio_exact.json");
const SUBACCOUNT_MARGIN_RATIO_EXACT_MANIFEST: &str =
    include_str!("../test_data/http/testnet/subaccount_margin_ratio_exact.manifest.json");
const LENDING_ASSETS_RESPONSE: &str = include_str!("../test_data/http/testnet/lending_assets.json");
const LENDING_ASSETS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/lending_assets.manifest.json");
const LENDING_INTEREST_RATE_RESPONSE: &str =
    include_str!("../test_data/http/testnet/lending_interest_rate_usdc.json");
const LENDING_INTEREST_RATE_MANIFEST: &str =
    include_str!("../test_data/http/testnet/lending_interest_rate_usdc.manifest.json");
const LENDING_INTEREST_RATE_HISTORY_RESPONSE: &str =
    include_str!("../test_data/http/testnet/lending_interest_rate_history_usdc_1h.json");
const LENDING_INTEREST_RATE_HISTORY_MANIFEST: &str =
    include_str!("../test_data/http/testnet/lending_interest_rate_history_usdc_1h.manifest.json");
const LENDING_STATUS_HISTORY_RESPONSE: &str =
    include_str!("../test_data/http/testnet/lending_status_history_usdc_1h.json");
const LENDING_STATUS_HISTORY_MANIFEST: &str =
    include_str!("../test_data/http/testnet/lending_status_history_usdc_1h.manifest.json");
const PERP_WALLET_TRADES_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_wallet_trades_page.json");
const PERP_WALLET_TRADES_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_wallet_trades_page.manifest.json");
const PERP_WALLET_ORDERS_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_wallet_orders_page_1.json");
const PERP_WALLET_ORDERS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_wallet_orders_page_1.manifest.json");
const SPOT_ORDER_BY_ID_CANCELED_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_order_by_id_canceled.json");
const SPOT_ORDER_BY_ID_CANCELED_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_order_by_id_canceled.manifest.json");
const PERP_ORDER_BY_TX_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_order_by_tx.json");
const PERP_ORDER_BY_TX_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_order_by_tx.manifest.json");
const WALLET_FUNDING_FEES_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_funding_fees_page_2.json");
const WALLET_FUNDING_FEES_MANIFEST: &str =
    include_str!("../test_data/http/testnet/wallet_funding_fees_page_2.manifest.json");
const SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json");
const SPOT_HISTORY_ORDERS_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_history_orders_eth_usdc_page_1.manifest.json");
const SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_account_trades_eth_usdc_page_1.json");
const SPOT_ACCOUNT_TRADES_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_account_trades_eth_usdc_page_1.manifest.json");
const SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json");
const SPOT_WALLET_ORDERS_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.manifest.json");
const SPOT_WALLET_TRADES_ETH_USDC_RESPONSE: &str =
    include_str!("../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json");
const SPOT_WALLET_TRADES_ETH_USDC_MANIFEST: &str =
    include_str!("../test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.manifest.json");
const WALLET_USER_STATS_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_user_stats.json");
const WALLET_USER_STATS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/wallet_user_stats.manifest.json");
const PERP_LIQUIDATION_PRICE_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_liquidation_price_account_market_3.json");
const PERP_LIQUIDATION_PRICE_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_liquidation_price_account_market_3.manifest.json");
const ALL_SUBACCOUNTS_RESPONSE: &str =
    include_str!("../test_data/http/testnet/all_subaccounts_page_1.json");
const ALL_SUBACCOUNTS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/all_subaccounts_page_1.manifest.json");
const QUOTA_HISTORY_RESPONSE: &str =
    include_str!("../test_data/http/testnet/quota_history_purchase_page.json");
const QUOTA_HISTORY_MANIFEST: &str =
    include_str!("../test_data/http/testnet/quota_history_purchase_page.manifest.json");
const WALLET_QUOTA_SUMMARY_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_quota_summary.json");
const WALLET_QUOTA_SUMMARY_MANIFEST: &str =
    include_str!("../test_data/http/testnet/wallet_quota_summary.manifest.json");
const WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.json");
const WALLET_HOURLY_FUNDING_PAGE_1_MANIFEST: &str = include_str!(
    "../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_1.manifest.json"
);
const WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_2.json");
const WALLET_HOURLY_FUNDING_PAGE_2_MANIFEST: &str = include_str!(
    "../test_data/http/testnet/wallet_hourly_unsettled_funding_market_3_page_2.manifest.json"
);
const WALLET_DELEGATE_ACCOUNTS_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_delegate_accounts.json");
const WALLET_DELEGATE_ACCOUNTS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/wallet_delegate_accounts.manifest.json");
const DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE: &str =
    include_str!("../test_data/http/testnet/delegate_delegator_accounts.json");
const DELEGATE_DELEGATOR_ACCOUNTS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/delegate_delegator_accounts.manifest.json");
const WALLET_LIQUIDATION_RECORDS_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_liquidation_records_page_1.json");
const WALLET_LIQUIDATION_RECORDS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/wallet_liquidation_records_page_1.manifest.json");
const PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_history_orders_account_market_3.json");
const PERP_HISTORY_ORDERS_ACCOUNT_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_history_orders_account_market_3.manifest.json");
const PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_account_trades_account_market_3.json");
const PERP_ACCOUNT_TRADES_ACCOUNT_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_account_trades_account_market_3.manifest.json");
const PERP_FUNDING_FEES_ACCOUNT_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_funding_fees_account_market_3.json");
const PERP_FUNDING_FEES_ACCOUNT_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_funding_fees_account_market_3.manifest.json");
const PERP_POSITIONS_ACCOUNT_RESPONSE: &str =
    include_str!("../test_data/http/testnet/perp_positions_account_market_3.json");
const PERP_POSITIONS_ACCOUNT_MANIFEST: &str =
    include_str!("../test_data/http/testnet/perp_positions_account_market_3.manifest.json");
const WALLET_SUBACCOUNTS_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_subaccounts.json");
const WALLET_SUBACCOUNTS_MANIFEST: &str =
    include_str!("../test_data/http/testnet/wallet_subaccounts.manifest.json");
const SUBACCOUNT_INFO_RESPONSE: &str =
    include_str!("../test_data/http/testnet/subaccount_info.json");
const SUBACCOUNT_INFO_MANIFEST: &str =
    include_str!("../test_data/http/testnet/subaccount_info.manifest.json");
const SUBACCOUNT_BALANCES_RESPONSE: &str =
    include_str!("../test_data/http/testnet/subaccount_balances.json");
const SUBACCOUNT_BALANCES_MANIFEST: &str =
    include_str!("../test_data/http/testnet/subaccount_balances.manifest.json");
const SUBACCOUNT_EQUITY_RESPONSE: &str =
    include_str!("../test_data/http/testnet/subaccount_equity.json");
const SUBACCOUNT_EQUITY_MANIFEST: &str =
    include_str!("../test_data/http/testnet/subaccount_equity.manifest.json");
const SUBACCOUNT_MARGIN_RATIO_RESPONSE: &str =
    include_str!("../test_data/http/testnet/subaccount_margin_ratio.json");
const SUBACCOUNT_MARGIN_RATIO_MANIFEST: &str =
    include_str!("../test_data/http/testnet/subaccount_margin_ratio.manifest.json");
const WALLET_BALANCE_CHANGES_RESPONSE: &str =
    include_str!("../test_data/http/testnet/wallet_balance_changes_page_2.json");
const WALLET_BALANCE_CHANGES_MANIFEST: &str =
    include_str!("../test_data/http/testnet/wallet_balance_changes_page_2.manifest.json");

fn assert_fixture_digest(manifest: &serde_json::Value, payload: &str) {
    assert_eq!(
        hex::encode(sp_crypto_hashing::sha2_256(payload.as_bytes())),
        manifest["response"]["payload_sha256"],
    );
}

#[rstest]
fn decodes_spot_trade_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(SPOT_TRADES_ETH_USDC_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXSpotTradesPage> =
        serde_json::from_str(SPOT_TRADES_ETH_USDC_RESPONSE).unwrap();

    assert_eq!(manifest["captured_at"], "2026-09-17T03:27:31Z");
    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["endpoint_role"], "public_rest_market_data");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/market/spot/trades"
    );
    assert_eq!(manifest["request"]["query"]["name"], "ETH/USDC");
    assert_eq!(manifest["request"]["query"]["pageSize"], 2);
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, SPOT_TRADES_ETH_USDC_RESPONSE);
    assert_eq!(response.data.items.len(), 2);
    assert!(response.data.has_next);
    assert!(response.data.next_cursor.is_some());
    assert_eq!(response.data.total, 11_437_005);
    assert_eq!(response.data.items[0].id, 188_405_950_000_036);
    assert_eq!(response.data.items[0].price, Decimal::new(243_436, 2));
    assert_eq!(response.data.items[0].base_amount, Decimal::new(7_039, 4));
    assert_eq!(
        response.data.items[0].quote_amount,
        Decimal::new(1_713_546_004, 6)
    );
    assert_eq!(response.data.items[0].maker_fee, Decimal::new(7_039, 8));
    assert_eq!(response.data.items[1].maker_fee, Decimal::new(955, 8));
}

#[rstest]
fn decodes_spot_market_observation_fixtures_exactly() {
    let candles_manifest: serde_json::Value =
        serde_json::from_str(SPOT_CANDLES_ETH_USDC_MANIFEST).unwrap();
    let candles: DeepXApiResponse<DeepXSpotCandlesPage> =
        serde_json::from_str(SPOT_CANDLES_ETH_USDC_RESPONSE).unwrap();
    assert_eq!(candles_manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&candles_manifest, SPOT_CANDLES_ETH_USDC_RESPONSE);
    assert_eq!(candles.data.pair, "ETH/USDC");
    assert_eq!(candles.data.details.len(), 3);
    assert_eq!(
        candles.data.details[0].volume,
        "5.6965012112303635".parse::<Decimal>().unwrap()
    );
    assert_eq!(candles.data.details[2].close, Decimal::new(243_569, 2));

    let price_manifest: serde_json::Value =
        serde_json::from_str(SPOT_LAST_PRICE_ETH_USDC_MANIFEST).unwrap();
    let price: DeepXApiResponse<DeepXSpotLastPrice> =
        serde_json::from_str(SPOT_LAST_PRICE_ETH_USDC_RESPONSE).unwrap();
    assert_fixture_digest(&price_manifest, SPOT_LAST_PRICE_ETH_USDC_RESPONSE);
    assert_eq!(price.data.0, Decimal::new(242_894, 2));

    let volume_manifest: serde_json::Value =
        serde_json::from_str(SPOT_VOLUME_ETH_USDC_MANIFEST).unwrap();
    let volume: DeepXApiResponse<DeepXSpotVolume> =
        serde_json::from_str(SPOT_VOLUME_ETH_USDC_RESPONSE).unwrap();
    assert_fixture_digest(&volume_manifest, SPOT_VOLUME_ETH_USDC_RESPONSE);
    assert_eq!(
        volume.data.total_volume,
        "293.50766491243".parse::<Decimal>().unwrap()
    );
    assert_eq!(volume.data.trade_count, 1_186);
    assert_eq!(volume.data.end_time - volume.data.start_time, 3_600_000);

    let book_manifest: serde_json::Value =
        serde_json::from_str(SPOT_ORDER_BOOK_ETH_USDC_MANIFEST).unwrap();
    let book: DeepXApiResponse<DeepXSpotOrderBook> =
        serde_json::from_str(SPOT_ORDER_BOOK_ETH_USDC_RESPONSE).unwrap();
    assert_fixture_digest(&book_manifest, SPOT_ORDER_BOOK_ETH_USDC_RESPONSE);
    assert_eq!(book.data.order_buy_list.len(), 20);
    assert_eq!(book.data.order_sell_list.len(), 20);
    assert_eq!(book.data.mid_price, Decimal::new(2_435_745, 3));
    assert_eq!(book.data.order_buy_list[6].qty, Decimal::new(16_613, 4));
    assert_eq!(
        book.data.order_sell_list[0].value,
        Decimal::new(391_070_901, 5)
    );
}

#[rstest]
fn decodes_spot_market_directory_and_lookup_fixtures_exactly() {
    let directory_manifest: serde_json::Value =
        serde_json::from_str(SPOT_MARKETS_SPEC369_MANIFEST).unwrap();
    let directory: DeepXApiResponse<Vec<DeepXSpotMarket>> =
        serde_json::from_str(SPOT_MARKETS_SPEC369_RESPONSE).unwrap();
    assert_eq!(directory_manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&directory_manifest, SPOT_MARKETS_SPEC369_RESPONSE);
    assert_eq!(directory.data.len(), 2);
    assert_eq!(directory.data[0].name, "SOL/USDC");
    assert_eq!(directory.data[0].base_decimal, 9);
    assert_eq!(
        directory.data[0].last_24h_price_change_rate,
        Some(Decimal::new(303, 2))
    );
    assert_eq!(directory.data[1].name, "ETH/USDC");
    assert_eq!(directory.data[1].tick_size, Decimal::new(1, 2));

    let lookup_manifest: serde_json::Value =
        serde_json::from_str(SPOT_MARKET_ETH_USDC_MANIFEST).unwrap();
    let lookup: DeepXApiResponse<DeepXSpotMarket> =
        serde_json::from_str(SPOT_MARKET_ETH_USDC_RESPONSE).unwrap();
    assert_fixture_digest(&lookup_manifest, SPOT_MARKET_ETH_USDC_RESPONSE);
    assert_eq!(lookup.data.price, Decimal::new(244_059, 2));
    assert_eq!(lookup.data.last_24h_price_change_rate, None);
    assert_eq!(lookup.data.base_decimal, 18);
    assert_eq!(lookup.data.quote_decimal, 6);
}

#[rstest]
fn decodes_perp_order_book_fixture_exactly() {
    let manifest: serde_json::Value =
        serde_json::from_str(PERP_ORDER_BOOK_ETH_USDC_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXPerpOrderBook> =
        serde_json::from_str(PERP_ORDER_BOOK_ETH_USDC_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["endpoint_role"], "public_rest_market_data");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/market/perp/order-books"
    );
    assert_eq!(manifest["request"]["query"]["marketId"], 3);
    assert_eq!(manifest["request"]["query"]["tickSize"], "0.01");
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, PERP_ORDER_BOOK_ETH_USDC_RESPONSE);
    assert_eq!(response.data.market_id, 3);
    assert_eq!(response.data.last_update_id, 6_923_589);
    assert_eq!(response.data.engine_time, 1_789_626_369_132);
    assert_eq!(response.data.order_buy_list.len(), 20);
    assert_eq!(response.data.order_sell_list.len(), 20);
    assert_eq!(
        response.data.order_buy_list[0].price,
        Decimal::new(24_339, 1)
    );
    assert_eq!(response.data.order_buy_list[1].qty, Decimal::new(2_394, 3));
    assert_eq!(
        response.data.order_sell_list[0].value,
        Decimal::new(140_451_609, 5)
    );
    assert_eq!(response.data.order_sell_list[0].market_id, 3);
    assert_eq!(response.data.latest_price, Decimal::ZERO);
    assert_eq!(response.data.mid_price, Decimal::new(2_434_035, 3));
}

#[rstest]
fn decodes_perp_market_lookup_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(PERP_MARKET_ETH_USDC_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXPerpMarketLookup> =
        serde_json::from_str(PERP_MARKET_ETH_USDC_RESPONSE).unwrap();

    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, PERP_MARKET_ETH_USDC_RESPONSE);
    assert_eq!(response.data.id, 3);
    assert_eq!(response.data.name, "ETH-USDC");
    assert_eq!(response.data.funding_rate, Decimal::new(125, 7));
    assert_eq!(response.data.mark_price, Decimal::new(2_438_185_111, 6));
    assert_eq!(response.data.order_spec_min_qty, Decimal::new(1, 3));
    assert_eq!(response.data.liquidation_dust_value, Decimal::new(50, 0));
    assert_eq!(response.data.last_funding_rate_time, 1_789_623_085_669);
}

#[rstest]
fn decodes_current_exact_margin_without_legacy_rounding() {
    let manifest: serde_json::Value =
        serde_json::from_str(SUBACCOUNT_MARGIN_RATIO_EXACT_MANIFEST).unwrap();
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/margin-ratio"
    );
    assert_eq!(manifest["runtime_identity"], serde_json::Value::Null);
    assert_fixture_digest(&manifest, SUBACCOUNT_MARGIN_RATIO_EXACT_RESPONSE);

    let response: DeepXApiResponse<DeepXSubaccountMarginRatio> =
        serde_json::from_str(SUBACCOUNT_MARGIN_RATIO_EXACT_RESPONSE).unwrap();
    let exact = response.data.exact_margin.as_ref().unwrap();
    assert_eq!(response.data.collateral, Decimal::new(97_096, 2));
    assert_eq!(exact.total_collateral, Decimal::new(970_964_181, 6));
    assert_eq!(exact.maint_margin_req, Decimal::ZERO);
    assert_eq!(exact.uni_mmr, None);
}

#[rstest]
fn decodes_lending_fixtures_exactly() {
    for (payload, manifest, path) in [
        (
            LENDING_ASSETS_RESPONSE,
            LENDING_ASSETS_MANIFEST,
            "/internal/v1/market/lending/assets",
        ),
        (
            LENDING_INTEREST_RATE_RESPONSE,
            LENDING_INTEREST_RATE_MANIFEST,
            "/internal/v1/market/lending/interest-rate",
        ),
        (
            LENDING_INTEREST_RATE_HISTORY_RESPONSE,
            LENDING_INTEREST_RATE_HISTORY_MANIFEST,
            "/internal/v1/market/lending/interest-rate-history",
        ),
        (
            LENDING_STATUS_HISTORY_RESPONSE,
            LENDING_STATUS_HISTORY_MANIFEST,
            "/internal/v1/market/lending/status-history",
        ),
    ] {
        let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_market_data");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(manifest["request"]["path"], path);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_fixture_digest(&manifest, payload);
    }

    let assets: DeepXApiResponse<Vec<DeepXLendingAsset>> =
        serde_json::from_str(LENDING_ASSETS_RESPONSE).unwrap();
    assert_eq!(assets.data.len(), 3);
    assert_eq!(assets.data[2].asset, "usdc");
    assert_eq!(assets.data[2].height, 110_143);
    assert_eq!(assets.data[2].created_at, "2026-04-10T10:09:44.628Z");

    let curves: DeepXApiResponse<Vec<DeepXLendingInterestRateParams>> =
        serde_json::from_str(LENDING_INTEREST_RATE_RESPONSE).unwrap();
    let curve = &curves.data[0];
    assert_eq!(curve.u1, Decimal::new(8, 1));
    assert_eq!(curve.r3, Decimal::new(7, 1));
    assert_eq!(curve.u4, None);
    assert_eq!(curve.r5, None);
    assert_eq!(curve.r_max, Decimal::new(15, 1));
    assert_eq!(curve.rho, Decimal::new(1, 1));

    let rates: DeepXApiResponse<DeepXLendingInterestRateHistory> =
        serde_json::from_str(LENDING_INTEREST_RATE_HISTORY_RESPONSE).unwrap();
    assert_eq!(rates.data.details.len(), 3);
    assert_eq!(
        rates.data.details[0].supply_apr,
        "0.00002115040506874738".parse::<Decimal>().unwrap(),
    );
    assert_eq!(
        rates.data.details[0].borrow_apr,
        "0.001327604517817316".parse::<Decimal>().unwrap(),
    );

    let statuses: DeepXApiResponse<DeepXLendingStatusHistory> =
        serde_json::from_str(LENDING_STATUS_HISTORY_RESPONSE).unwrap();
    assert_eq!(statuses.data.details.len(), 3);
    assert_eq!(statuses.data.details[0].index_price, Decimal::ONE);
    assert_eq!(
        statuses.data.details[0].total_supplied,
        "830055808.591571".parse::<Decimal>().unwrap(),
    );
    assert_eq!(
        statuses.data.details[0].utilization_rate,
        "0.017701393570897547".parse::<Decimal>().unwrap(),
    );
}

#[rstest]
fn decodes_perp_wallet_trade_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(PERP_WALLET_TRADES_MANIFEST).unwrap();
    let response: DeepXApiResponse<Vec<DeepXPerpWalletTradeMarket>> =
        serde_json::from_str(PERP_WALLET_TRADES_RESPONSE).unwrap();

    assert_eq!(manifest["captured_at"], "2026-09-17T03:51:08Z");
    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/perp/trades-by-wallet"
    );
    assert_eq!(
        manifest["request"]["query"]["address"],
        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
    );
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, PERP_WALLET_TRADES_RESPONSE);
    assert_eq!(response.data.len(), 2);
    assert_eq!(response.data[0].subaccounts.len(), 3);
    assert_eq!(response.data[1].subaccounts.len(), 2);
    let zero_size = &response.data[1].subaccounts[0].trades.items[2];
    assert_eq!(zero_size.id, 50_726_011_012_081);
    assert_eq!(zero_size.price, Decimal::new(8_436, 2));
    assert_eq!(zero_size.size, Decimal::ZERO);
    assert_ne!(
        response.data[1].subaccounts[0].trades.next_cursor,
        response.data[1].subaccounts[1].trades.next_cursor,
    );
}

#[rstest]
fn decodes_perp_wallet_order_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(PERP_WALLET_ORDERS_MANIFEST).unwrap();
    let response: DeepXApiResponse<Vec<DeepXPerpWalletOrderMarket>> =
        serde_json::from_str(PERP_WALLET_ORDERS_RESPONSE).unwrap();

    assert_eq!(manifest["captured_at"], "2026-09-17T04:09:21Z");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/perp/orders-by-wallet"
    );
    assert_eq!(manifest["request"]["query"]["pageSize"], 5);
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, PERP_WALLET_ORDERS_RESPONSE);
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].subaccounts.len(), 2);
    assert_eq!(
        response.data[0].subaccounts[0].orders.items.len()
            + response.data[0].subaccounts[1].orders.items.len(),
        5
    );
    let order = &response.data[0].subaccounts[0].orders.items[0];
    assert_eq!(order.order_id, "1789522185370");
    assert_eq!(
        order.avg_fill_price,
        Some("2403.9885999999997".parse::<Decimal>().unwrap())
    );
    assert_eq!(order.fee, Decimal::new(-144_111, 6));
    assert_eq!(
        response.data[0].subaccounts[0].orders.next_cursor,
        response.data[0].subaccounts[1].orders.next_cursor,
    );
}

#[rstest]
fn decodes_canceled_spot_order_lookup_fixture_exactly() {
    let manifest: serde_json::Value =
        serde_json::from_str(SPOT_ORDER_BY_ID_CANCELED_MANIFEST).unwrap();
    let order: DeepXApiResponse<DeepXSpotOrderRecord> =
        serde_json::from_str(SPOT_ORDER_BY_ID_CANCELED_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/spot/order-by-id"
    );
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, SPOT_ORDER_BY_ID_CANCELED_RESPONSE);
    assert_eq!(order.data.order_id, "1789627379350");
    assert_eq!(order.data.status, "Canceled");
    assert_eq!(order.data.cancel_reason.as_deref(), Some("UserCanceled"));
    assert_eq!(order.data.cancel_height, Some(188_574_100));
    assert_eq!(order.data.price, Decimal::new(245_605, 2));
    assert_eq!(order.data.avg_fill_price, None);
}

#[rstest]
fn decodes_perp_order_by_tx_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(PERP_ORDER_BY_TX_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXPerpOrderRecord> =
        serde_json::from_str(PERP_ORDER_BY_TX_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["captured_at"], "2026-09-17T02:29:39Z");
    assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
    assert_eq!(manifest["request"]["method"], "GET");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/perp/order-by-tx"
    );
    assert_eq!(
        manifest["request"]["query"]["txHash"],
        "0x247ef3967338c6985714731a49752455ebe877d8678e0b261b3638892cfae554"
    );
    assert_eq!(manifest["response"]["status"], 200);
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, PERP_ORDER_BY_TX_RESPONSE);
    assert!(!response.fail);
    assert_eq!(response.data.order_id, "1789445193480");
    assert_eq!(response.data.market_id, 3);
    assert_eq!(response.data.size, Decimal::new(3, 1));
    assert_eq!(response.data.price, Decimal::new(2_523_683_573, 6));
    assert_eq!(response.data.avg_fill_price, Some(Decimal::new(24_996, 1)));
    assert_eq!(response.data.fee, Decimal::new(-149_921, 6));
    assert_eq!(
        response.data.tx_hash,
        "0x247ef3967338c6985714731a49752455ebe877d8678e0b261b3638892cfae554"
    );
    assert_eq!(response.data.tx_hash_type, "EXTRINSIC_HASH");
}

#[rstest]
fn decodes_wallet_funding_fee_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(WALLET_FUNDING_FEES_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXAccountPage<DeepXPerpFundingFeeRecord>> =
        serde_json::from_str(WALLET_FUNDING_FEES_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["request"]["method"], "GET");
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, WALLET_FUNDING_FEES_RESPONSE);
    assert_eq!(response.data.items.len(), 2);
    assert_eq!(response.data.items[0].position_size, Decimal::new(238, 3));
    assert_eq!(response.data.items[0].fee, Decimal::new(-17, 5));
    assert_eq!(
        response.data.items[1].fee_rate,
        "0.000469620169921581".parse::<Decimal>().unwrap()
    );
    assert!(response.data.has_next);
    assert!(response.data.next_cursor.is_some());
}

#[rstest]
fn decodes_spot_history_order_fixture_exactly() {
    let manifest: serde_json::Value =
        serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_MANIFEST).unwrap();
    let orders: DeepXApiResponse<DeepXAccountPage<DeepXSpotOrderRecord>> =
        serde_json::from_str(SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/spot/history-orders"
    );
    assert_eq!(manifest["request"]["query"]["name"], "ETH/USDC");
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, SPOT_HISTORY_ORDERS_ETH_USDC_RESPONSE);
    assert_eq!(orders.data.items.len(), 3);
    assert!(orders.data.has_next);
    assert_eq!(orders.data.items[0].order_id, "1789627379350");
    assert_eq!(orders.data.items[0].price, Decimal::new(245_605, 2));
    assert_eq!(orders.data.items[0].avg_fill_price, None);
    assert_eq!(orders.data.items[0].base_amount, Decimal::new(8_849, 4));
    assert_eq!(
        orders.data.items[0].quote_amount,
        Decimal::new(2_173_358_645, 6)
    );
    assert_eq!(orders.data.items[0].fee, Decimal::ZERO);
}

#[rstest]
fn decodes_spot_account_trade_fixture_exactly() {
    let manifest: serde_json::Value =
        serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_MANIFEST).unwrap();
    let trades: DeepXApiResponse<DeepXAccountPage<DeepXSpotAccountTradeRecord>> =
        serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/spot/trades"
    );
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE);
    assert_eq!(trades.data.items.len(), 3);
    assert!(trades.data.has_next);
    assert_eq!(trades.data.items[0].id, 188_573_374_000_099);
    assert_eq!(trades.data.items[0].order_id, "1789627349907");
    assert_eq!(
        trades.data.items[0].base_amount,
        "0.33079181307615474".parse::<Decimal>().unwrap()
    );
    assert_eq!(
        trades.data.items[0].quote_amount,
        Decimal::new(808_041_701, 6)
    );
    assert_eq!(trades.data.items[0].fee, Decimal::new(-323_216, 6));
    assert_eq!(trades.data.items[0].fee_asset, "");

    let mut without_fee_asset: serde_json::Value =
        serde_json::from_str(SPOT_ACCOUNT_TRADES_ETH_USDC_RESPONSE).unwrap();
    without_fee_asset["data"]["items"][0]
        .as_object_mut()
        .unwrap()
        .remove("feeAsset");
    let without_fee_asset: DeepXApiResponse<DeepXAccountPage<DeepXSpotAccountTradeRecord>> =
        serde_json::from_value(without_fee_asset).unwrap();
    assert_eq!(without_fee_asset.data.items[0].fee_asset, "");
}

#[rstest]
fn decodes_spot_wallet_order_fixture_exactly() {
    let manifest: serde_json::Value =
        serde_json::from_str(SPOT_WALLET_ORDERS_ETH_USDC_MANIFEST).unwrap();
    let response: DeepXApiResponse<Vec<DeepXSpotWalletOrderMarket>> =
        serde_json::from_str(SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/spot/orders-by-wallet"
    );
    assert_eq!(manifest["request"]["query"]["pageSize"], 5);
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, SPOT_WALLET_ORDERS_ETH_USDC_RESPONSE);
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].name, "ETH/USDC");
    assert_eq!(response.data[0].subaccounts[0].orders.items.len(), 5);
    let order = &response.data[0].subaccounts[0].orders.items[1];
    assert_eq!(order.order_id, "1789631508484");
    assert_eq!(order.price, Decimal::new(245_139, 2));
    assert_eq!(order.avg_fill_price, None);
    assert_eq!(order.base_remaining_amount, Decimal::new(7_926, 4));
    assert_eq!(order.quote_amount, Decimal::new(1_942_971_714, 6));
    assert_eq!(order.fee, Decimal::ZERO);
    assert!(response.data[0].subaccounts[0].orders.has_next);
}

#[rstest]
fn decodes_spot_wallet_trade_fixture_exactly() {
    let manifest: serde_json::Value =
        serde_json::from_str(SPOT_WALLET_TRADES_ETH_USDC_MANIFEST).unwrap();
    let response: DeepXApiResponse<Vec<DeepXSpotWalletTradeMarket>> =
        serde_json::from_str(SPOT_WALLET_TRADES_ETH_USDC_RESPONSE).unwrap();

    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/spot/trades-by-wallet"
    );
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, SPOT_WALLET_TRADES_ETH_USDC_RESPONSE);
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].subaccounts[0].trades.items.len(), 5);
    let trade = &response.data[0].subaccounts[0].trades.items[0];
    assert_eq!(trade.id, 188_632_828_000_015);
    assert_eq!(
        trade.base_amount,
        "0.26400108068721884".parse::<Decimal>().unwrap()
    );
    assert_eq!(trade.quote_amount, Decimal::new(6_449_256, 4));
    assert_eq!(trade.fee, Decimal::new(-64_492, 6));
    assert_eq!(trade.fee_asset, "");
    assert!(response.data[0].subaccounts[0].trades.has_next);
}

#[rstest]
fn decodes_user_stats_and_liquidation_price_fixtures_exactly() {
    for (payload, manifest) in [
        (WALLET_USER_STATS_RESPONSE, WALLET_USER_STATS_MANIFEST),
        (
            PERP_LIQUIDATION_PRICE_RESPONSE,
            PERP_LIQUIDATION_PRICE_MANIFEST,
        ),
    ] {
        let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_fixture_digest(&manifest, payload);
    }

    let stats: DeepXApiResponse<DeepXUserStats> =
        serde_json::from_str(WALLET_USER_STATS_RESPONSE).unwrap();
    let price: DeepXApiResponse<DeepXPerpLiquidationPrice> =
        serde_json::from_str(PERP_LIQUIDATION_PRICE_RESPONSE).unwrap();
    assert_eq!(stats.data.subaccounts.len(), 4);
    assert_eq!(stats.data.if_staked_quote_asset_amount, Decimal::ZERO);
    assert_eq!(stats.data.number_of_sub_accounts, 4);
    assert_eq!(stats.data.number_of_sub_accounts_created, 4);
    assert_eq!(price.data.market_id, 3);
    assert_eq!(price.data.market_name, "ETH-USDC");
    assert_eq!(price.data.liquidate_price, None);
}

#[rstest]
fn decodes_all_subaccounts_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(ALL_SUBACCOUNTS_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXAccountPage<DeepXSubaccountDirectoryRecord>> =
        serde_json::from_str(ALL_SUBACCOUNTS_RESPONSE).unwrap();

    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/subaccounts/all"
    );
    assert_eq!(manifest["request"]["query"]["pageSize"], 5);
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, ALL_SUBACCOUNTS_RESPONSE);
    assert_eq!(response.data.items.len(), 5);
    assert!(response.data.has_next);
    let record = &response.data.items[0];
    assert_eq!(record.owner, "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae");
    assert_eq!(
        record.subaccount,
        "0x260949c96c0d32a8126ab666a14131321e08f563"
    );
    assert_eq!(record.name, "Subaccount11");
    assert_eq!(record.status, None);
    assert_eq!(record.height, 181_249_057);
    assert_eq!(record.created_at, "2026-09-11T08:16:49.885Z");
}

#[rstest]
fn decodes_quota_history_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(QUOTA_HISTORY_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXAccountPage<DeepXQuotaHistoryRecord>> =
        serde_json::from_str(QUOTA_HISTORY_RESPONSE).unwrap();
    assert_fixture_digest(&manifest, QUOTA_HISTORY_RESPONSE);
    let record = &response.data.items[0];
    assert_eq!(record.history_type, DeepXQuotaHistoryType::Purchase);
    assert_eq!(record.buyer_type, Some(DeepXQuotaBuyerType::Wallet));
    assert_eq!(record.quota, 10_000);
    assert_eq!(record.block_number, 181_248_894);
    assert_eq!(record.event_index, 49);
}

#[rstest]
fn decodes_wallet_quota_summary_fixture_exactly() {
    let manifest: serde_json::Value = serde_json::from_str(WALLET_QUOTA_SUMMARY_MANIFEST).unwrap();
    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
    assert_eq!(manifest["request"]["method"], "GET");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/quota/summary"
    );
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, WALLET_QUOTA_SUMMARY_RESPONSE);

    let response: DeepXApiResponse<DeepXQuotaSummary> =
        serde_json::from_str(WALLET_QUOTA_SUMMARY_RESPONSE).unwrap();
    assert_eq!(
        response.data.owner,
        "0x781ed35b167068c93dfadab41dfb680edaca4e50"
    );
    assert_eq!(response.data.subaccount_count, 4);
    assert_eq!(response.data.spot_volume_usd, Decimal::ZERO);
    assert_eq!(
        response.data.perp_volume_usd,
        "2970.527580000000000000".parse::<Decimal>().unwrap()
    );
    assert_eq!(
        response.data.total_volume_usd,
        response.data.perp_volume_usd
    );
    assert_eq!(response.data.quota_earned, 2_970);
    assert_eq!(response.data.quota_pending, 2_970);
    assert_eq!(response.data.first_trade_ts_ms, Some(1_789_445_055_647));
    assert_eq!(response.data.last_trade_ts_ms, Some(1_789_522_187_243));
}

#[rstest]
fn decodes_hourly_unsettled_funding_fixtures_exactly() {
    for (payload, manifest) in [
        (
            WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE,
            WALLET_HOURLY_FUNDING_PAGE_1_MANIFEST,
        ),
        (
            WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE,
            WALLET_HOURLY_FUNDING_PAGE_2_MANIFEST,
        ),
    ] {
        let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(
            manifest["request"]["path"],
            "/internal/v1/account/hourly-unsettled-funding"
        );
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_fixture_digest(&manifest, payload);
    }

    let first: DeepXApiResponse<Vec<DeepXHourlyUnsettledFundingRecord>> =
        serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_1_RESPONSE).unwrap();
    let second: DeepXApiResponse<Vec<DeepXHourlyUnsettledFundingRecord>> =
        serde_json::from_str(WALLET_HOURLY_FUNDING_PAGE_2_RESPONSE).unwrap();
    let newest = &first.data[0];
    let first_boundary = first.data.last().unwrap();
    let second_boundary = &second.data[0];

    assert_eq!(first.data.len(), 5);
    assert_eq!(second.data.len(), 5);
    assert_eq!(newest.signed_position_size_raw, 300_000_000_000_000_000);
    assert_eq!(newest.baseline_index_raw, 77_064_622_395_991_726);
    assert_eq!(newest.cumulative_index_raw, 77_077_122_395_991_726);
    assert_eq!(newest.delta_index_raw, 12_500_000_000_000);
    assert_eq!(newest.mark_price_raw, 2_397_824_333);
    assert_eq!(newest.payment_raw, -8_991);
    assert_eq!(newest.boundary_timestamp_ms, 1_789_521_262_677);
    assert_eq!(newest.boundary_block, 187_058_004);
    assert_eq!(newest.boundary_event_index, 14);
    assert_eq!(first_boundary.baseline_index_raw, 76_843_886_126_453_109);
    assert_eq!(second_boundary.cumulative_index_raw, 76_843_886_126_453_109);
    assert!(first_boundary.boundary_timestamp_ms > second_boundary.boundary_timestamp_ms);
    let cursor = first_boundary.cursor();
    assert_eq!(
        cursor.boundary_timestamp_ms,
        first_boundary.boundary_timestamp_ms
    );
    assert_eq!(cursor.market_id, first_boundary.market_id);
    assert_eq!(cursor.subaccount, first_boundary.subaccount);
    assert_eq!(cursor.event_id, first_boundary.boundary_event_id);
}

#[rstest]
fn decodes_delegate_directory_fixtures_exactly() {
    for (payload, manifest, path) in [
        (
            WALLET_DELEGATE_ACCOUNTS_RESPONSE,
            WALLET_DELEGATE_ACCOUNTS_MANIFEST,
            "/internal/v1/account/delegate-accounts",
        ),
        (
            DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE,
            DELEGATE_DELEGATOR_ACCOUNTS_MANIFEST,
            "/internal/v1/account/delegator-accounts",
        ),
    ] {
        let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
        assert_eq!(manifest["captured_at"], "2026-09-17T02:43:04Z");
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_identity");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(manifest["request"]["path"], path);
        assert_eq!(manifest["response"]["status"], 200);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_fixture_digest(&manifest, payload);
    }

    let delegates: DeepXApiResponse<Vec<DeepXDelegateAccount>> =
        serde_json::from_str(WALLET_DELEGATE_ACCOUNTS_RESPONSE).unwrap();
    let delegators: DeepXApiResponse<Vec<String>> =
        serde_json::from_str(DELEGATE_DELEGATOR_ACCOUNTS_RESPONSE).unwrap();
    assert_eq!(delegates.data.len(), 1);
    assert_eq!(
        delegates.data[0].delegate_address,
        "0x1b856b9bf1d0ceeb0927a081c759813cd703a54f"
    );
    assert_eq!(delegates.data[0].delegate_name, "One-Click Trading");
    assert_eq!(delegates.data[0].valid_until, 1_804_997_046_158);
    assert_eq!(delegates.data[0].create_time, 1_789_445_050_397);
    assert_eq!(
        delegates.data[0].mode,
        DeepXDelegateMode::PlaceOrCancelOrder
    );
    assert!(delegates.data[0].active);
    assert_eq!(
        delegators.data,
        ["0x781ed35b167068c93dfadab41dfb680edaca4e50"]
    );
}

#[rstest]
fn decodes_wallet_liquidation_record_fixture_exactly() {
    let manifest: serde_json::Value =
        serde_json::from_str(WALLET_LIQUIDATION_RECORDS_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXAccountPage<DeepXLiquidationRecord>> =
        serde_json::from_str(WALLET_LIQUIDATION_RECORDS_RESPONSE).unwrap();

    assert_eq!(manifest["captured_at"], "2026-09-17T03:01:34Z");
    assert_eq!(manifest["deployment"], "testnet");
    assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
    assert_eq!(
        manifest["request"]["path"],
        "/internal/v1/account/subaccounts/liquidation-records"
    );
    assert_eq!(manifest["request"]["query"]["pageSize"], 5);
    assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
    assert_fixture_digest(&manifest, WALLET_LIQUIDATION_RECORDS_RESPONSE);
    assert!(response.data.has_next);
    assert!(response.data.next_cursor.is_some());
    assert_eq!(response.data.items.len(), 5);
    assert_eq!(response.data.items[0].id, 1_763);
    assert_eq!(
        response.data.items[0].liquidation_type,
        DeepXLiquidationType::LiquidatePerp
    );
    assert_eq!(response.data.items[0].margin_shortage, 2_771);
    assert_eq!(response.data.items[0].margin_freed, 16_917);
    assert_eq!(response.data.items[0].liquidator_fee, Some(16_911));
    assert_eq!(
        response.data.items[0].liquidate_base_amount,
        Some(20_000_000)
    );
    assert_eq!(response.data.items[0].oracle_price, Some(84_566_331));
    assert_eq!(response.data.items[0].target_account_order_id, Some(14));
    assert_eq!(response.data.items[0].tx_hash, None);
}

#[rstest]
fn liquidation_record_exact_integer_decoder_preserves_u128_boundaries() {
    let response: serde_json::Value =
        serde_json::from_str(WALLET_LIQUIDATION_RECORDS_RESPONSE).unwrap();
    let mut record = response["data"]["items"][0].clone();
    record["marginShortage"] = serde_json::json!(u128::MAX.to_string());
    let decoded: DeepXLiquidationRecord = serde_json::from_value(record.clone()).unwrap();
    assert_eq!(decoded.margin_shortage, u128::MAX);

    record["marginShortage"] = serde_json::json!("340282366920938463463374607431768211456");
    assert!(serde_json::from_value::<DeepXLiquidationRecord>(record).is_err());
}

#[rstest]
fn decodes_nonempty_account_record_fixtures_exactly() {
    for (payload, manifest) in [
        (
            PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE,
            PERP_HISTORY_ORDERS_ACCOUNT_MANIFEST,
        ),
        (
            PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE,
            PERP_ACCOUNT_TRADES_ACCOUNT_MANIFEST,
        ),
        (
            PERP_FUNDING_FEES_ACCOUNT_RESPONSE,
            PERP_FUNDING_FEES_ACCOUNT_MANIFEST,
        ),
        (
            PERP_POSITIONS_ACCOUNT_RESPONSE,
            PERP_POSITIONS_ACCOUNT_MANIFEST,
        ),
    ] {
        let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["endpoint_role"], "public_rest_account_data");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(manifest["request"]["query"]["marketId"], 3);
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_fixture_digest(&manifest, payload);
    }

    let orders: DeepXApiResponse<DeepXAccountPage<DeepXPerpOrderRecord>> =
        serde_json::from_str(PERP_HISTORY_ORDERS_ACCOUNT_RESPONSE).unwrap();
    let trades: DeepXApiResponse<DeepXAccountPage<DeepXPerpAccountTradeRecord>> =
        serde_json::from_str(PERP_ACCOUNT_TRADES_ACCOUNT_RESPONSE).unwrap();
    let funding_fees: DeepXApiResponse<DeepXAccountPage<DeepXPerpFundingFeeRecord>> =
        serde_json::from_str(PERP_FUNDING_FEES_ACCOUNT_RESPONSE).unwrap();
    let positions: DeepXApiResponse<DeepXAccountPage<DeepXPerpPositionRecord>> =
        serde_json::from_str(PERP_POSITIONS_ACCOUNT_RESPONSE).unwrap();

    assert!(!orders.fail);
    assert_eq!(orders.data.items.len(), 3);
    assert_eq!(orders.data.items[0].order_id, "1789445193480");
    assert_eq!(
        orders.data.items[0].avg_fill_price,
        Some(Decimal::new(24_996, 1))
    );
    assert_eq!(orders.data.items[0].fee, Decimal::new(-149_921, 6));
    assert_eq!(orders.data.items[1].status, "Filled");
    assert!(orders.data.items[1].reduce_only);

    assert!(!trades.fail);
    assert_eq!(trades.data.items.len(), 3);
    assert_eq!(trades.data.items[0].id, 185_971_383_000_008);
    assert_eq!(trades.data.items[0].order_id, orders.data.items[0].order_id);
    assert_eq!(trades.data.items[0].taker, "Buyer");
    assert_eq!(trades.data.items[0].filled_direction, "Long");
    assert_eq!(trades.data.items[0].price, Decimal::new(24_996, 1));
    assert_eq!(trades.data.items[0].fee, Decimal::new(-149_921, 6));

    assert!(!funding_fees.fail);
    assert_eq!(funding_fees.data.items.len(), 4);
    assert_eq!(funding_fees.data.items[0].market, 3);
    assert_eq!(
        funding_fees.data.items[0].position_size,
        Decimal::new(238, 3)
    );
    assert_eq!(funding_fees.data.items[0].fee, Decimal::new(-17, 5));
    assert_eq!(
        funding_fees.data.items[0].fee_rate,
        "0.000489097868208476".parse::<Decimal>().unwrap()
    );
    assert!(funding_fees.data.items[0].is_settled);

    assert!(!positions.fail);
    assert_eq!(positions.data.items.len(), 2);
    assert_eq!(positions.data.items[0].status, "Open");
    assert_eq!(
        positions.data.items[0].base_asset_amount,
        Decimal::new(3, 1)
    );
    assert_eq!(
        positions.data.items[0].pnl,
        "-0.4018472999999176".parse::<Decimal>().unwrap()
    );
    assert_eq!(positions.data.items[0].close_price, None);
    assert_eq!(positions.data.items[1].status, "Closed");
    assert_eq!(
        positions.data.items[1].close_price,
        Some(Decimal::new(249_941, 2))
    );
    assert_eq!(positions.data.items[1].version, 1);
}

#[rstest]
fn decodes_account_identity_and_state_fixtures_exactly() {
    for (payload, manifest) in [
        (WALLET_SUBACCOUNTS_RESPONSE, WALLET_SUBACCOUNTS_MANIFEST),
        (SUBACCOUNT_INFO_RESPONSE, SUBACCOUNT_INFO_MANIFEST),
        (SUBACCOUNT_BALANCES_RESPONSE, SUBACCOUNT_BALANCES_MANIFEST),
        (SUBACCOUNT_EQUITY_RESPONSE, SUBACCOUNT_EQUITY_MANIFEST),
        (
            SUBACCOUNT_MARGIN_RATIO_RESPONSE,
            SUBACCOUNT_MARGIN_RATIO_MANIFEST,
        ),
        (
            WALLET_BALANCE_CHANGES_RESPONSE,
            WALLET_BALANCE_CHANGES_MANIFEST,
        ),
    ] {
        let manifest: serde_json::Value = serde_json::from_str(manifest).unwrap();
        assert_eq!(manifest["deployment"], "testnet");
        assert_eq!(manifest["request"]["method"], "GET");
        assert_eq!(manifest["runtime_identity"]["spec_version"], 369);
        assert_fixture_digest(&manifest, payload);
    }

    let subaccounts: DeepXApiResponse<Vec<String>> =
        serde_json::from_str(WALLET_SUBACCOUNTS_RESPONSE).unwrap();
    let profile: DeepXApiResponse<DeepXSubaccountProfile> =
        serde_json::from_str(SUBACCOUNT_INFO_RESPONSE).unwrap();
    let balances: DeepXApiResponse<DeepXSubaccountBalances> =
        serde_json::from_str(SUBACCOUNT_BALANCES_RESPONSE).unwrap();
    let equity: DeepXApiResponse<DeepXSubaccountEquity> =
        serde_json::from_str(SUBACCOUNT_EQUITY_RESPONSE).unwrap();
    let margin: DeepXApiResponse<DeepXSubaccountMarginRatio> =
        serde_json::from_str(SUBACCOUNT_MARGIN_RATIO_RESPONSE).unwrap();
    let changes: DeepXApiResponse<DeepXAccountPage<DeepXBalanceChangeRecord>> =
        serde_json::from_str(WALLET_BALANCE_CHANGES_RESPONSE).unwrap();

    assert_eq!(subaccounts.data.len(), 4);
    assert_eq!(profile.data.name, "kazee");
    assert_eq!(profile.data.status, "Active");
    assert_eq!(profile.data.margin_strategy, "Cross");
    assert!(profile.data.spot_positions.is_empty());
    assert_eq!(balances.data.assets.len(), 3);
    assert_eq!(balances.data.assets[0].symbol, "USDC");
    assert_eq!(
        balances.data.assets[0].balance,
        Decimal::new(999_783_258, 6)
    );
    assert_eq!(
        balances.data.assets[1].price,
        Decimal::new(2_490_275_945, 6)
    );
    assert_eq!(equity.data.total_deposits_usd, Decimal::new(99_978, 2));
    assert_eq!(equity.data.unrealized_pnl_usd, Decimal::new(-273, 2));
    assert_eq!(equity.data.equity_usd, Decimal::new(99_705, 2));
    assert_eq!(margin.data.collateral, Decimal::new(97_096, 2));
    assert_eq!(margin.data.margin_required, Decimal::ZERO);
    assert_eq!(margin.data.margin_ratio, None);
    assert!(margin.data.exact_margin.is_none());
    assert_eq!(changes.data.items.len(), 2);
    assert_eq!(changes.data.items[0].balance_change, Decimal::new(-17, 5));
    assert_eq!(
        changes.data.items[0].change_type,
        DeepXBalanceChangeType::FundingFee
    );
    assert_eq!(
        changes.data.items[0].position.as_ref().unwrap().market_id,
        3
    );
    assert_eq!(
        changes.data.items[1].balance_change,
        Decimal::new(-10_235_558, 6)
    );
    assert_eq!(
        changes.data.next_cursor.as_deref(),
        Some("MTc4OTQ5ODIxMTExNToxODY3Mjg3MTcwMDAyMDAw")
    );
    assert!(changes.data.has_next);
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct FixtureIdentity {
    genesis_hash: String,
    metadata_sha256: String,
    spec_version: u32,
    transaction_version: u32,
}

#[derive(Debug, Deserialize)]
struct PerpVolumeFixtureManifest {
    deployment: String,
    endpoint_role: String,
    request: PerpVolumeFixtureRequest,
    response: PerpVolumeFixtureResponse,
    runtime_identity: PerpVolumeRuntimeIdentity,
}

#[derive(Debug, Deserialize)]
struct PerpVolumeFixtureRequest {
    method: String,
    path: String,
    query: PerpVolumeFixtureQuery,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpVolumeFixtureQuery {
    market_id: u32,
    period: String,
}

#[derive(Debug, Deserialize)]
struct PerpVolumeFixtureResponse {
    status: u16,
    content_type: String,
    payload_path: String,
}

#[derive(Debug, Deserialize)]
struct PerpVolumeRuntimeIdentity {
    #[serde(flatten)]
    identity: FixtureIdentity,
    captured_block: CapturedBlock,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct CapturedBlock {
    hash: String,
    number: Option<u64>,
}

fn validate_perp_volume_fixture_manifest(
    manifest: &PerpVolumeFixtureManifest,
) -> Result<(), String> {
    if manifest.deployment != "testnet"
        || manifest.endpoint_role != "public_rest_market_data"
        || manifest.request.method != "GET"
        || manifest.request.path != "/internal/v1/market/perp/volume"
        || manifest.request.query.market_id != 3
        || manifest.request.query.period != "1h"
        || manifest.response.status != 200
        || manifest.response.content_type != "application/json"
        || manifest.response.payload_path != "perp_volume_1h.json"
    {
        return Err("unexpected DeepX perpetual volume fixture provenance".to_string());
    }
    if manifest.runtime_identity.identity
        != (FixtureIdentity {
            genesis_hash: "0x86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b"
                .to_string(),
            metadata_sha256: "e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37"
                .to_string(),
            spec_version: 366,
            transaction_version: 1,
        })
        || manifest.runtime_identity.captured_block
            != (CapturedBlock {
                hash: "0x03e29c08d90b26697535dacbcfa940c8d2ae08653e4b4760ac1dd4a281ced7c6"
                    .to_string(),
                number: None,
            })
    {
        return Err("DeepX perpetual volume fixture runtime identity mismatch".to_string());
    }

    Ok(())
}

#[rstest]
fn decodes_sanitized_perp_volume_fixture() {
    let manifest: PerpVolumeFixtureManifest =
        serde_json::from_str(PERP_VOLUME_1H_MANIFEST).unwrap();
    let response: DeepXApiResponse<DeepXPerpVolume> =
        serde_json::from_str(PERP_VOLUME_1H_RESPONSE).unwrap();

    validate_perp_volume_fixture_manifest(&manifest).unwrap();
    assert_eq!(response.code, DeepXApiCode::Api(200));
    assert!(!response.fail);
    assert_eq!(response.data.total_volume, Decimal::new(2_117_975, 3));
    assert_eq!(response.data.trade_count, 2_492);
    assert_eq!(response.data.end_time - response.data.start_time, 3_600_000);
    assert_eq!(response.data.statistic_time, response.data.end_time);
}

#[rstest]
fn rejects_perp_volume_fixture_runtime_identity_drift() {
    let mut manifest: PerpVolumeFixtureManifest =
        serde_json::from_str(PERP_VOLUME_1H_MANIFEST).unwrap();
    manifest.runtime_identity.identity.spec_version += 1;

    assert_eq!(
        validate_perp_volume_fixture_manifest(&manifest),
        Err("DeepX perpetual volume fixture runtime identity mismatch".to_string()),
    );
}

#[rstest]
fn fixture_runtime_provenance_is_self_contained() {
    let http_dir = Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/http/testnet"
    ));
    let mut runtime_sidecars = 0;

    for entry in fs::read_dir(http_dir).unwrap() {
        let path = entry.unwrap().path();
        if !path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".manifest.json")
        {
            continue;
        }

        let manifest: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let Some(identity) = manifest
            .get("runtime_identity")
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };
        assert!(
            identity.get("source_manifest").is_none(),
            "{}",
            path.display()
        );
        assert!(
            identity["captured_block"]["hash"].is_string(),
            "{}",
            path.display()
        );
        runtime_sidecars += 1;
    }

    let websocket_path = Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/websocket/testnet/user_balances.manifest.json"
    ));
    let manifest: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(websocket_path).unwrap()).unwrap();
    let identity = &manifest["runtime_identity"];
    assert!(identity.get("source_manifest").is_none());
    assert!(identity["captured_block"]["hash"].is_string());

    assert_eq!(runtime_sidecars, 44);
}
