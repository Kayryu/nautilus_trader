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

//! Exact matching between finalized runtime trades and REST account trades.

use std::{borrow::Borrow, collections::HashSet};

use rust_decimal::Decimal;
use thiserror::Error;

use super::events::{DeepXFinalizedPerpTrade, DeepXFinalizedSpotTrade};
use crate::{
    error::DeepXError,
    parse::{decimal_to_scaled_i128, parse_canonical_order_id},
    rest::models::{
        DeepXPerpAccountTradeRecord, DeepXPerpMarketLookup, DeepXPerpWalletSubaccountTrades,
        DeepXPerpWalletTradeMarket, DeepXSpotAccountTradeRecord,
    },
};

/// Failures binding one REST account execution to finalized chain trade evidence.
#[derive(Debug, Error)]
pub enum DeepXPerpTradeMatchError {
    /// The REST order ID or market scale cannot be used for exact matching.
    #[error("invalid DeepX perpetual account trade identity or scale")]
    InvalidInput,
    /// A REST financial value cannot be represented in the chain's integer units.
    #[error("inexact DeepX perpetual account trade {field}: {source}")]
    InexactValue {
        /// Financial field that could not be converted exactly.
        field: &'static str,
        /// Exact decimal conversion failure.
        #[source]
        source: DeepXError,
    },
    /// No finalized trade agrees with every account, order, and financial field.
    #[error("DeepX account trade has no matching finalized trade event")]
    NotFound,
    /// More than one finalized event agrees with the same REST execution.
    #[error("DeepX account trade matches multiple finalized trade events")]
    Ambiguous,
}

/// Matches one REST perpetual account trade to one decoded finalized event.
///
/// The caller supplies validated market decimals and the quote symbol, and independently
/// establishes canonical finality. Matching does not prove the REST timestamp.
///
/// # Errors
///
/// Returns an error for malformed inputs, inexact values, no match, or multiple exact matches.
pub fn match_finalized_perp_account_trade<'a>(
    trades: &'a [DeepXFinalizedPerpTrade],
    record: &DeepXPerpAccountTradeRecord,
    subaccount: [u8; 20],
    base_decimal: u8,
    quote_decimal: u8,
    quote_symbol: &str,
) -> Result<&'a DeepXFinalizedPerpTrade, DeepXPerpTradeMatchError> {
    use DeepXPerpTradeMatchError as Error;
    if record.id == 0
        || record.market_id == 0
        || base_decimal > 28
        || quote_decimal > 28
        || quote_symbol.is_empty()
    {
        return Err(Error::InvalidInput);
    }
    let order_id = parse_canonical_order_id(&record.order_id).ok_or(Error::InvalidInput)?;
    if record.price <= Decimal::ZERO
        || record.size <= Decimal::ZERO
        || record.leverage <= Decimal::ZERO
        || !matches!(record.taker.as_str(), "Buyer" | "Seller")
        || !matches!(record.filled_direction.as_str(), "Long" | "Short" | "Both")
        || !record.fee_asset.is_empty() && !record.fee_asset.eq_ignore_ascii_case(quote_symbol)
    {
        return Err(Error::InvalidInput);
    }
    let price = decimal_to_scaled_i128(record.price, 6).map_err(|source| Error::InexactValue {
        field: "price",
        source,
    })?;
    let size = decimal_to_scaled_i128(record.size, u32::from(base_decimal)).map_err(|source| {
        Error::InexactValue {
            field: "size",
            source,
        }
    })?;
    let leverage =
        decimal_to_scaled_i128(record.leverage, 3).map_err(|source| Error::InexactValue {
            field: "leverage",
            source,
        })?;
    let fee = decimal_to_scaled_i128(record.fee, u32::from(quote_decimal)).map_err(|source| {
        Error::InexactValue {
            field: "fee",
            source,
        }
    })?;
    let mut matching = trades.iter().filter(|trade| {
        let (owner, venue_order_id, raw_leverage, raw_fee) = if record.is_long {
            (
                trade.buyer,
                trade.buyer_order_id,
                trade.buyer_leverage,
                if trade.taker == "Buyer" {
                    trade.taker_fee
                } else {
                    trade.maker_fee
                },
            )
        } else {
            (
                trade.seller,
                trade.seller_order_id,
                trade.seller_leverage,
                if trade.taker == "Seller" {
                    trade.taker_fee
                } else {
                    trade.maker_fee
                },
            )
        };
        trade.event_index < 1_000_000
            && trade
                .block_number
                .checked_mul(1_000_000)
                .and_then(|base| base.checked_add(u64::from(trade.event_index)))
                == Some(record.id)
            && u64::from(trade.market_id) == record.market_id
            && owner == subaccount
            && venue_order_id == order_id
            && trade.taker == record.taker
            && trade.filled_direction == record.filled_direction
            && trade
                .fee_asset
                .eq_ignore_ascii_case(quote_symbol.as_bytes())
            && i128::try_from(trade.price) == Ok(price)
            && i128::try_from(trade.size) == Ok(size)
            && i128::from(raw_leverage) == leverage
            && raw_fee.checked_neg() == Some(fee)
    });
    let matched = matching.next().ok_or(Error::NotFound)?;
    if matching.next().is_some() {
        return Err(Error::Ambiguous);
    }
    Ok(matched)
}

/// One wallet account trade matched to a finalized perpetual event.
#[derive(Clone, Copy, Debug)]
pub struct DeepXFinalizedWalletPerpTradeMatch<'a, R = DeepXPerpAccountTradeRecord> {
    /// Wallet subaccount on the event's buyer or seller side.
    pub subaccount: [u8; 20],
    /// Complete finalized event evidence.
    pub trade: &'a DeepXFinalizedPerpTrade,
    /// Mutable REST account record whose financial fields match the event.
    pub record: &'a R,
}

/// Failures binding a complete wallet trade page to finalized perpetual events.
#[derive(Debug, Error)]
pub enum DeepXFinalizedWalletPerpTradeMatchError {
    /// The supplied event slice mixes blocks or repeats an event index.
    #[error("DeepX finalized wallet trade events have conflicting block identity")]
    ConflictingEvents,
    /// The directory repeats a subaccount or supplies no subaccounts.
    #[error("DeepX wallet subaccount directory is empty or contains duplicates")]
    InvalidSubaccounts,
    /// Both sides of an event belong to one subaccount, which the REST identity cannot distinguish.
    #[error("DeepX self-trade cannot be bound to one account trade record")]
    SelfTrade,
    /// Exactly one market group, subaccount group, or metadata record was not available.
    #[error("DeepX finalized wallet trade market or subaccount identity is unavailable")]
    MissingIdentity,
    /// A current market identity disagrees with the REST wallet group.
    #[error("DeepX finalized wallet trade market identity conflicts with metadata")]
    MarketIdentityMismatch,
    /// An involved group has more pages, so absence in the observed page is not authoritative.
    #[error("DeepX finalized wallet trade group has an unresolved continuation cursor")]
    IncompletePage,
    /// The event's venue ID was absent from the complete observed group.
    #[error("DeepX finalized wallet trade event is absent from the wallet page")]
    MissingRecord,
    /// The same event ID appeared more than once in its wallet group.
    #[error("DeepX finalized wallet trade event has duplicate REST records")]
    DuplicateRecord,
    /// An account record disagrees with the event's exact fields.
    #[error(transparent)]
    Trade(#[from] DeepXPerpTradeMatchError),
}

/// Matches every event involving a supplied wallet subaccount against complete REST groups.
///
/// The caller independently validates wallet ownership and finalized event identity. Current REST
/// metadata and pages do not prove historical wallet ownership or cryptographic inclusion.
///
/// # Errors
///
/// Returns an error without partial matches for conflicting identity, incomplete pages, missing
/// records, duplicate records, or exact financial mismatches.
pub fn match_finalized_wallet_perp_trades<'a, G, S, R>(
    trades: &'a [DeepXFinalizedPerpTrade],
    groups: &'a [G],
    subaccounts: &[[u8; 20]],
    markets: &[DeepXPerpMarketLookup],
) -> Result<Vec<DeepXFinalizedWalletPerpTradeMatch<'a, R>>, DeepXFinalizedWalletPerpTradeMatchError>
where
    G: Borrow<DeepXPerpWalletTradeMarket<S>>,
    S: Borrow<DeepXPerpWalletSubaccountTrades<R>> + 'a,
    R: Borrow<DeepXPerpAccountTradeRecord> + 'a,
{
    use DeepXFinalizedWalletPerpTradeMatchError as Error;

    if subaccounts.is_empty()
        || subaccounts.iter().copied().collect::<HashSet<_>>().len() != subaccounts.len()
    {
        return Err(Error::InvalidSubaccounts);
    }
    if let Some(first) = trades.first() {
        let mut indices = HashSet::new();
        for trade in trades {
            if trade.block_hash != first.block_hash
                || trade.block_number != first.block_number
                || !indices.insert(trade.event_index)
            {
                return Err(Error::ConflictingEvents);
            }
        }
    }

    let mut matched = Vec::new();
    for trade in trades {
        for subaccount in subaccounts {
            if trade.buyer != *subaccount && trade.seller != *subaccount {
                continue;
            }
            if trade.buyer == *subaccount && trade.seller == *subaccount {
                return Err(Error::SelfTrade);
            }
            let mut matching_groups = groups
                .iter()
                .map(Borrow::borrow)
                .filter(|group| group.market_id == u64::from(trade.market_id));
            let group = matching_groups.next().ok_or(Error::MissingIdentity)?;
            if matching_groups.next().is_some() {
                return Err(Error::MarketIdentityMismatch);
            }
            let mut matching_markets = markets.iter().filter(|market| market.id == group.market_id);
            let market = matching_markets.next().ok_or(Error::MissingIdentity)?;
            if matching_markets.next().is_some() || market.name != group.market_name {
                return Err(Error::MarketIdentityMismatch);
            }
            let encoded_subaccount = format!("0x{}", hex::encode(subaccount));
            let mut matching_subaccounts =
                group
                    .subaccounts
                    .iter()
                    .map(Borrow::borrow)
                    .filter(|candidate| {
                        candidate
                            .subaccount
                            .eq_ignore_ascii_case(&encoded_subaccount)
                    });
            let account_group = matching_subaccounts.next().ok_or(Error::MissingIdentity)?;
            if matching_subaccounts.next().is_some() {
                return Err(Error::MissingIdentity);
            }
            if account_group.trades.has_next {
                return Err(Error::IncompletePage);
            }
            let expected_id = trade
                .block_number
                .checked_mul(1_000_000)
                .and_then(|base| base.checked_add(u64::from(trade.event_index)))
                .ok_or(Error::MissingRecord)?;
            let mut records = account_group.trades.items.iter().filter(|record| {
                <R as Borrow<DeepXPerpAccountTradeRecord>>::borrow(record).id == expected_id
            });
            let record = records.next().ok_or(Error::MissingRecord)?;
            if records.next().is_some() {
                return Err(Error::DuplicateRecord);
            }
            let confirmed = match_finalized_perp_account_trade(
                trades,
                record.borrow(),
                *subaccount,
                market.base_decimal,
                market.quote_decimal,
                &market.quote_symbol,
            )?;
            if confirmed.event_index != trade.event_index {
                return Err(Error::ConflictingEvents);
            }
            matched.push(DeepXFinalizedWalletPerpTradeMatch {
                subaccount: *subaccount,
                trade: confirmed,
                record,
            });
        }
    }
    Ok(matched)
}

/// Failures binding one Spot REST execution to finalized `OrderMatched` evidence.
#[derive(Debug, Error)]
pub enum DeepXSpotTradeMatchError {
    /// Market identity, side, or exact market scale is invalid.
    #[error("invalid DeepX Spot account trade identity or scale")]
    InvalidInput,
    /// A REST value cannot be represented exactly in raw runtime units.
    #[error("inexact DeepX Spot account trade {field}: {source}")]
    InexactValue {
        /// Financial field that could not be converted exactly.
        field: &'static str,
        /// Exact decimal conversion failure.
        #[source]
        source: DeepXError,
    },
    /// No finalized event agrees with all REST fields.
    #[error("DeepX Spot account trade has no matching finalized event")]
    NotFound,
    /// More than one event agrees with one REST trade.
    #[error("DeepX Spot account trade matches multiple finalized events")]
    Ambiguous,
}

/// Matches one Spot REST trade to a supplied finalized event and its global event index.
///
/// The caller validates the REST account scope and independently establishes canonical finality.
/// The match does not prove the REST timestamp or historical completeness.
///
/// # Errors
///
/// Returns an error for invalid market scales, inexact values, or missing or ambiguous evidence.
pub fn match_finalized_spot_account_trade<'a>(
    trades: &'a [DeepXFinalizedSpotTrade],
    record: &DeepXSpotAccountTradeRecord,
    subaccount: [u8; 20],
    base_decimal: u8,
    quote_decimal: u8,
    base_symbol: &str,
    quote_symbol: &str,
) -> Result<&'a DeepXFinalizedSpotTrade, DeepXSpotTradeMatchError> {
    use DeepXSpotTradeMatchError as Error;

    if record.id == 0
        || base_decimal > 28
        || quote_decimal > 28
        || base_symbol.is_empty()
        || quote_symbol.is_empty()
        || !matches!(record.order_side.as_str(), "Buy" | "Sell")
        || !matches!(record.taker.as_str(), "Buyer" | "Seller")
    {
        return Err(Error::InvalidInput);
    }
    let pair: [u8; 32] = record
        .pair
        .strip_prefix("0x")
        .and_then(|encoded| hex::decode(encoded).ok())
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(Error::InvalidInput)?;
    let order_id = parse_canonical_order_id(&record.order_id).ok_or(Error::InvalidInput)?;
    if record.price <= Decimal::ZERO
        || record.base_amount <= Decimal::ZERO
        || record.quote_amount <= Decimal::ZERO
    {
        return Err(Error::InvalidInput);
    }
    let scaled = |value, field, decimals| {
        decimal_to_scaled_i128(value, decimals)
            .map_err(|source| Error::InexactValue { field, source })
    };
    let price = scaled(record.price, "price", 6)?;
    let base_amount = scaled(record.base_amount, "base amount", u32::from(base_decimal))?;
    let quote_amount = scaled(
        record.quote_amount,
        "quote amount",
        u32::from(quote_decimal),
    )?;
    let fee_decimals = if record.order_side == "Buy" {
        base_decimal
    } else {
        quote_decimal
    };
    let fee = scaled(record.fee, "fee", u32::from(fee_decimals))?;
    let expected_fee_asset = if record.order_side == "Buy" {
        base_symbol
    } else {
        quote_symbol
    };
    if !record.fee_asset.is_empty() && !record.fee_asset.eq_ignore_ascii_case(expected_fee_asset) {
        return Err(Error::InvalidInput);
    }
    let as_u256 = |value| {
        let value = u128::try_from(value).ok()?;
        let mut bytes = [0_u8; 32];
        bytes[..16].copy_from_slice(&value.to_le_bytes());
        Some(bytes)
    };
    let (price, base_amount, quote_amount) = (
        as_u256(price).ok_or(Error::InvalidInput)?,
        as_u256(base_amount).ok_or(Error::InvalidInput)?,
        as_u256(quote_amount).ok_or(Error::InvalidInput)?,
    );
    let mut matching = trades.iter().filter(|trade| {
        let (owner, venue_order_id, raw_fee, fee_asset, other_fee_asset, other_symbol) =
            if record.order_side == "Buy" {
                (
                    trade.buyer,
                    trade.buy_id,
                    if trade.taker == "Buyer" {
                        trade.taker_fee
                    } else {
                        trade.maker_fee
                    },
                    if trade.taker == "Buyer" {
                        &trade.taker_fee_asset
                    } else {
                        &trade.maker_fee_asset
                    },
                    if trade.taker == "Buyer" {
                        &trade.maker_fee_asset
                    } else {
                        &trade.taker_fee_asset
                    },
                    quote_symbol,
                )
            } else {
                (
                    trade.seller,
                    trade.sell_id,
                    if trade.taker == "Seller" {
                        trade.taker_fee
                    } else {
                        trade.maker_fee
                    },
                    if trade.taker == "Seller" {
                        &trade.taker_fee_asset
                    } else {
                        &trade.maker_fee_asset
                    },
                    if trade.taker == "Seller" {
                        &trade.maker_fee_asset
                    } else {
                        &trade.taker_fee_asset
                    },
                    base_symbol,
                )
            };
        trade.event_index < 1_000_000
            && trade
                .block_number
                .checked_mul(1_000_000)
                .and_then(|base| base.checked_add(u64::from(trade.event_index)))
                == Some(record.id)
            && trade.pair == pair
            && owner == subaccount
            && venue_order_id == order_id
            && trade.taker == record.taker
            && trade.price == price
            && trade.trade_amount_b == base_amount
            && trade.trade_amount_u == quote_amount
            && fee_asset.eq_ignore_ascii_case(expected_fee_asset.as_bytes())
            && other_fee_asset.eq_ignore_ascii_case(other_symbol.as_bytes())
            && raw_fee.checked_neg() == Some(fee)
    });
    let matched = matching.next().ok_or(Error::NotFound)?;
    if matching.next().is_some() {
        return Err(Error::Ambiguous);
    }
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;
    use crate::rest::DeepXAccountPage;

    fn record() -> DeepXPerpAccountTradeRecord {
        serde_json::from_value(json!({
            "id": 2_000_003, "marketId": 7, "orderId": "42", "isLong": true,
            "taker": "Buyer", "price": "1.234567", "size": "2.50",
            "leverage": "3.125", "createdAt": "2026-01-01T00:00:00Z",
            "filledDirection": "Long", "fee": "-0.000004", "feeAsset": "USDC"
        }))
        .unwrap()
    }

    fn trade() -> DeepXFinalizedPerpTrade {
        DeepXFinalizedPerpTrade {
            block_hash: [1; 32],
            block_number: 2,
            event_index: 3,
            market_id: 7,
            buyer_order_id: 42,
            buyer: [2; 20],
            seller_order_id: 43,
            seller: [3; 20],
            price: 1_234_567,
            size: 250,
            buyer_leverage: 3_125,
            seller_leverage: 1_000,
            filled_direction: "Long".to_string(),
            taker: "Buyer".to_string(),
            taker_fee: 4,
            maker_fee: 0,
            fee_asset: b"USDC".to_vec(),
        }
    }

    fn spot_record() -> DeepXSpotAccountTradeRecord {
        serde_json::from_value(json!({
            "id": 42_000_000, "orderId": "7", "orderSide": "Buy",
            "pair": format!("0x{}", hex::encode([0x5a; 32])), "pairName": "ETH/USDC",
            "baseAmount": "1", "quoteAmount": "50", "price": "50",
            "fee": "-0.01", "feeAsset": "ETH", "taker": "Buyer",
            "createdAt": "2026-06-18T15:13:14.739Z"
        }))
        .unwrap()
    }

    fn spot_trade() -> DeepXFinalizedSpotTrade {
        let u256 = |value: u128| {
            let mut bytes = [0_u8; 32];
            bytes[..16].copy_from_slice(&value.to_le_bytes());
            bytes
        };
        DeepXFinalizedSpotTrade {
            block_hash: [3; 32],
            block_number: 42,
            event_index: 0,
            pair: [0x5a; 32],
            buyer: [0x11; 20],
            buy_id: 7,
            seller: [0x22; 20],
            sell_id: 8,
            trade_amount_u: u256(50_000_000),
            trade_amount_b: u256(1_000_000_000_000_000_000),
            taker: "Buyer".to_string(),
            taker_fee: 10_000_000_000_000_000,
            taker_fee_asset: b"ETH".to_vec(),
            maker_fee: 100_000,
            maker_fee_asset: b"USDC".to_vec(),
            price: u256(50_000_000),
        }
    }

    fn wallet_market() -> DeepXPerpMarketLookup {
        let mut response: serde_json::Value = serde_json::from_str(include_str!(
            "../../test_data/http/testnet/perp_market_eth_usdc_by_id.json"
        ))
        .unwrap();
        response["data"]["id"] = json!(7);
        response["data"]["baseDecimal"] = json!(2);
        serde_json::from_value(response["data"].clone()).unwrap()
    }

    fn wallet_group(record: DeepXPerpAccountTradeRecord) -> DeepXPerpWalletTradeMarket {
        DeepXPerpWalletTradeMarket {
            market_id: 7,
            market_name: "ETH-USDC".to_string(),
            subaccounts: vec![DeepXPerpWalletSubaccountTrades {
                subaccount: format!("0x{}", hex::encode([2; 20])),
                trades: DeepXAccountPage {
                    items: vec![record],
                    next_cursor: None,
                    has_next: false,
                },
            }],
        }
    }

    #[rstest]
    fn wallet_match_returns_complete_sdk_record_and_event() {
        let trades = [trade()];
        let groups = [wallet_group(record())];
        let matched =
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20]], &[wallet_market()])
                .unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].subaccount, [2; 20]);
        assert_eq!(matched[0].trade, &trades[0]);
        assert_eq!(matched[0].record, &groups[0].subaccounts[0].trades.items[0]);
    }

    #[rstest]
    fn wallet_match_rejects_incomplete_or_duplicate_records() {
        let trades = [trade()];
        let mut groups = [wallet_group(record())];
        let markets = [wallet_market()];
        groups[0].subaccounts[0].trades.has_next = true;
        assert!(matches!(
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::IncompletePage)
        ));

        groups[0].subaccounts[0].trades.has_next = false;
        groups[0].subaccounts[0].trades.items.push(record());
        assert!(matches!(
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::DuplicateRecord)
        ));
    }

    #[rstest]
    fn wallet_match_rejects_conflicting_identity_and_financial_values() {
        let trades = [trade()];
        let mut groups = [wallet_group(record())];
        let markets = [wallet_market()];
        assert!(matches!(
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20], [2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::InvalidSubaccounts)
        ));
        assert!(matches!(
            match_finalized_wallet_perp_trades(&[trade(), trade()], &groups, &[[2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::ConflictingEvents)
        ));

        groups[0].subaccounts[0].trades.items[0].price = Decimal::new(1_234_568, 6);
        assert!(matches!(
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::Trade(
                DeepXPerpTradeMatchError::NotFound
            ))
        ));
    }

    #[rstest]
    fn wallet_match_rejects_self_trade_and_unavailable_group_identity() {
        let mut event = trade();
        event.seller = event.buyer;
        let groups = [wallet_group(record())];
        let markets = [wallet_market()];
        assert!(matches!(
            match_finalized_wallet_perp_trades(&[event], &groups, &[[2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::SelfTrade)
        ));
        let trades = [trade()];
        assert!(matches!(
            match_finalized_wallet_perp_trades(
                &trades,
                &[] as &[DeepXPerpWalletTradeMarket],
                &[[2; 20]],
                &markets,
            ),
            Err(DeepXFinalizedWalletPerpTradeMatchError::MissingIdentity)
        ));
        assert!(matches!(
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20]], &[]),
            Err(DeepXFinalizedWalletPerpTradeMatchError::MissingIdentity)
        ));
    }

    #[rstest]
    fn wallet_match_rejects_market_conflicts_and_missing_records() {
        let trades = [trade()];
        let mut groups = [wallet_group(record())];
        let markets = [wallet_market()];
        groups[0].market_name = "OTHER".to_string();
        assert!(matches!(
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::MarketIdentityMismatch)
        ));

        groups[0].market_name = markets[0].name.clone();
        groups[0].subaccounts[0].trades.items[0].id += 1;
        assert!(matches!(
            match_finalized_wallet_perp_trades(&trades, &groups, &[[2; 20]], &markets),
            Err(DeepXFinalizedWalletPerpTradeMatchError::MissingRecord)
        ));
    }

    #[test]
    fn exact_trade_matches_and_duplicate_is_ambiguous() {
        let trade = trade();
        let trades = [trade.clone()];
        assert_eq!(
            match_finalized_perp_account_trade(&trades, &record(), [2; 20], 2, 6, "USDC").unwrap(),
            &trade,
        );
        assert!(matches!(
            match_finalized_perp_account_trade(
                &[trade.clone(), trade],
                &record(),
                [2; 20],
                2,
                6,
                "USDC"
            ),
            Err(DeepXPerpTradeMatchError::Ambiguous),
        ));
    }

    #[test]
    fn inexact_price_and_wrong_identity_are_rejected() {
        let trade = trade();
        let mut inexact_record = record();
        inexact_record.price = Decimal::new(12_345_671, 7);
        assert!(matches!(
            match_finalized_perp_account_trade(
                std::slice::from_ref(&trade),
                &inexact_record,
                [2; 20],
                2,
                6,
                "USDC"
            ),
            Err(DeepXPerpTradeMatchError::InexactValue { field: "price", .. }),
        ));
        assert!(matches!(
            match_finalized_perp_account_trade(&[trade], &record(), [4; 20], 2, 6, "USDC"),
            Err(DeepXPerpTradeMatchError::NotFound),
        ));
    }

    #[test]
    fn spot_buy_and_sell_match_exact_event_fields() {
        let trade = spot_trade();
        let trades = [trade.clone()];
        let record = spot_record();
        assert_eq!(
            match_finalized_spot_account_trade(&trades, &record, [0x11; 20], 18, 6, "ETH", "USDC",)
                .unwrap(),
            &trade,
        );

        let mut seller = record;
        seller.order_id = "8".to_string();
        seller.order_side = "Sell".to_string();
        seller.fee = "-0.1".parse().unwrap();
        seller.fee_asset = "USDC".to_string();
        assert_eq!(
            match_finalized_spot_account_trade(&trades, &seller, [0x22; 20], 18, 6, "ETH", "USDC",)
                .unwrap(),
            &trade,
        );
        seller.fee_asset = "ETH".to_string();
        assert!(matches!(
            match_finalized_spot_account_trade(&trades, &seller, [0x22; 20], 18, 6, "ETH", "USDC",),
            Err(DeepXSpotTradeMatchError::InvalidInput),
        ));
    }

    #[test]
    fn spot_inexact_and_conflicting_values_are_rejected() {
        let trade = spot_trade();
        let mut record = spot_record();
        record.quote_amount = "49.999999".parse().unwrap();
        assert!(matches!(
            match_finalized_spot_account_trade(
                std::slice::from_ref(&trade),
                &record,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC",
            ),
            Err(DeepXSpotTradeMatchError::NotFound),
        ));
        record.price = "50.0000001".parse().unwrap();
        assert!(matches!(
            match_finalized_spot_account_trade(
                std::slice::from_ref(&trade),
                &record,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC",
            ),
            Err(DeepXSpotTradeMatchError::InexactValue { field: "price", .. }),
        ));
    }
}
