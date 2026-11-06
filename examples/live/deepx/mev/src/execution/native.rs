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

//! Metadata-bound order evidence for pair-sharded active Spot storage.
//!
//! Absence proves terminality only after the caller binds successful placement and all finalized
//! trades. A successful dispatch alone does not prove that IOC matching or cancellation ran.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use deepx_rust_sdk::rpc::{DeepXRpcClient, spot_place::DeepXSpotPlaceExpected};
use subxt_core::{
    Metadata, dynamic,
    events::{Events, Phase},
    ext::scale_value::{
        At, Value, ValueDef,
        scale::{decode_as_type, encode_as_type},
    },
    storage,
};

pub(super) fn uses_pair_storage(metadata: &Metadata) -> anyhow::Result<bool> {
    let entries = metadata
        .pallet_by_name("SpotMarket")
        .context("missing Spot pallet")?
        .storage()
        .context("missing Spot storage")?;
    anyhow::ensure!(
        entries.entry_by_name("ActiveSpotOrdersFor").is_some(),
        "missing active Spot orders"
    );
    Ok(entries.entry_by_name("OrderInfo").is_none())
}

fn key(metadata: &Metadata, account: [u8; 20], pair: [u8; 32]) -> anyhow::Result<String> {
    let address = dynamic::storage(
        "SpotMarket",
        "ActiveSpotOrdersFor",
        vec![Value::from_bytes(account), Value::from_bytes(pair)],
    );
    Ok(super::encoded(storage::get_address_bytes(
        &address, metadata,
    )?))
}

pub(super) fn validate(metadata: &Metadata) -> anyhow::Result<()> {
    anyhow::ensure!(
        uses_pair_storage(metadata)?,
        "unsupported legacy Spot order storage"
    );
    let storage_entry = metadata
        .pallet_by_name("SpotMarket")
        .context("missing Spot pallet")?
        .storage()
        .context("missing Spot storage")?
        .entry_by_name("ActiveSpotOrdersFor")
        .context("missing active Spot orders")?;
    let ty = storage_entry
        .entry_type()
        .key_ty()
        .context("active Spot storage is not a map")?;
    let mut bytes = vec![1_u8; 20];
    bytes.extend([3_u8; 32]);
    let mut remaining = &bytes[..];
    decode_as_type(&mut remaining, ty, metadata.types())?;
    anyhow::ensure!(
        remaining.is_empty(),
        "unsupported pair-sharded order key type"
    );
    key(metadata, [1; 20], [3; 32])?;
    let amount = || {
        Value::unnamed_composite([Value::unnamed_composite([
            Value::u128(7),
            Value::u128(0),
            Value::u128(0),
            Value::u128(0),
        ])])
    };
    let order = Value::named_composite([
        ("id", Value::u128(1)),
        ("maker", Value::from_bytes([1; 20])),
        ("pair", Value::from_bytes([3; 32])),
        ("price", Value::unnamed_variant("Some", [amount()])),
        ("quote_amount", amount()),
        ("base_amount", amount()),
        ("create_time", Value::u128(42)),
        ("status", Value::unnamed_variant("Open", [])),
        (
            "order_type",
            Value::unnamed_variant("Limit", [Value::unnamed_variant("IOC", [])]),
        ),
        ("post_only", Value::unnamed_variant("None", [])),
        ("reduce_only", Value::bool(false)),
        ("is_buy", Value::bool(true)),
        ("stp_mode", Value::unnamed_variant("None", [])),
    ]);
    let mut encoded = Vec::new();
    encode_as_type(
        &Value::unnamed_composite([order]),
        storage_entry.entry_type().value_ty(),
        metadata.types(),
        &mut encoded,
    )?;
    let mut remaining = &encoded[..];
    let decoded = decode_as_type(
        &mut remaining,
        storage_entry.entry_type().value_ty(),
        metadata.types(),
    )?;
    anyhow::ensure!(remaining.is_empty(), "trailing native order bytes");
    let ids = active_ids(&decoded, [1; 20], [3; 32], &[1])?;
    anyhow::ensure!(
        ids.get(&1) == Some(&true),
        "unsupported pair-sharded Spot value layout"
    );
    Ok(())
}

pub(super) async fn active_ioc_ids(
    rpc: &DeepXRpcClient,
    metadata: &Metadata,
    head: &str,
    account: [u8; 20],
    pair: [u8; 32],
    tracked: &[u64],
) -> anyhow::Result<BTreeMap<u64, bool>> {
    validate(metadata)?;
    let Some(raw) = rpc
        .get_storage(&key(metadata, account, pair)?, head)
        .await?
    else {
        return Ok(BTreeMap::new());
    };
    let bytes = super::decode_hex(&raw)?;
    let ty = metadata
        .pallet_by_name("SpotMarket")
        .context("missing Spot pallet")?
        .storage()
        .context("missing Spot storage")?
        .entry_by_name("ActiveSpotOrdersFor")
        .context("missing active orders")?
        .entry_type()
        .value_ty();
    let mut remaining = &bytes[..];
    let value = decode_as_type(&mut remaining, ty, metadata.types())?;
    anyhow::ensure!(remaining.is_empty(), "trailing active Spot order bytes");
    active_ids(&value, account, pair, tracked)
}

/// Check every listed Spot pair: AccountInfo's legacy spot_positions may be empty even when the
/// separate native order buckets contain orders.
pub(super) async fn require_idle(
    rpc: &DeepXRpcClient,
    metadata: &Metadata,
    head: &str,
    account: [u8; 20],
) -> anyhow::Result<()> {
    validate(metadata)?;
    let address = dynamic::storage("SpotMarket", "PairList", Vec::<Value>::new());
    let raw = rpc
        .get_storage(
            &super::encoded(storage::get_address_bytes(&address, metadata)?),
            head,
        )
        .await?
        .context("missing Spot pair directory")?;
    let bytes = super::decode_hex(&raw)?;
    let ty = metadata
        .pallet_by_name("SpotMarket")
        .context("missing Spot pallet")?
        .storage()
        .context("missing Spot storage")?
        .entry_by_name("PairList")
        .context("missing Spot pair directory")?
        .entry_type()
        .value_ty();
    let mut remaining = &bytes[..];
    let pairs = decode_as_type(&mut remaining, ty, metadata.types())?;
    anyhow::ensure!(remaining.is_empty(), "trailing Spot directory bytes");
    let ValueDef::Composite(pairs) = pairs.value else {
        anyhow::bail!("invalid Spot pair directory")
    };
    anyhow::ensure!(
        pairs.len() <= 4096,
        "Spot directory exceeds account inspection bound"
    );
    let mut seen = BTreeSet::new();
    for pair in pairs.values() {
        let pair =
            crate::fixed_bytes::<32>(pair.at("pair").context("missing Spot directory pair")?)?;
        anyhow::ensure!(seen.insert(pair), "duplicate Spot pair directory entry");
        if let Some(raw) = rpc
            .get_storage(&key(metadata, account, pair)?, head)
            .await?
        {
            // The validated value type is Vec<SpotOrder>. SCALE's only empty vector is 0x00.
            anyhow::ensure!(
                super::decode_hex(&raw)? == [0],
                "execution subaccount has existing native Spot orders"
            );
        }
    }
    Ok(())
}

fn variant(value: &Value<u32>, name: &str) -> bool {
    matches!(&value.value,ValueDef::Variant(v) if v.name == name)
}

fn field<'a>(value: &'a Value<u32>, name: &str) -> anyhow::Result<&'a Value<u32>> {
    value
        .at(name)
        .with_context(|| format!("missing native order field {name}"))
}

fn is_ioc(value: &Value<u32>) -> bool {
    matches!(&value.value,ValueDef::Variant(v) if v.name == "Limit" && v.values.len() == 1 && v.values.values().next().is_some_and(|v|variant(v,"IOC")))
}

fn active_ids(
    value: &Value<u32>,
    account: [u8; 20],
    pair: [u8; 32],
    tracked: &[u64],
) -> anyhow::Result<BTreeMap<u64, bool>> {
    let ValueDef::Composite(orders) = &value.value else {
        anyhow::bail!("invalid active Spot order vector")
    };
    let mut ids = BTreeSet::new();
    let mut active = BTreeMap::new();
    for order in orders.values() {
        let id = u64::try_from(
            field(order, "id")?
                .as_u128()
                .context("invalid active order ID")?,
        )?;
        anyhow::ensure!(
            id > 0 && ids.insert(id),
            "duplicate or invalid active order ID"
        );
        anyhow::ensure!(
            crate::fixed_bytes::<20>(field(order, "maker")?)? == account
                && crate::fixed_bytes::<32>(field(order, "pair")?)? == pair,
            "active Spot order account/pair mismatch"
        );
        let status = field(order, "status")?;
        anyhow::ensure!(
            variant(status, "Open") || variant(status, "PartiallyFilled"),
            "terminal order in active Spot storage"
        );
        if tracked.contains(&id) {
            anyhow::ensure!(
                is_ioc(field(order, "order_type")?),
                "tracked order is not IOC"
            );
            active.insert(
                id,
                field(order, "is_buy")?
                    .as_bool()
                    .context("invalid active order side")?,
            );
        }
    }
    Ok(active)
}

pub(super) fn inclusion(
    metadata: &Metadata,
    expected: DeepXSpotPlaceExpected,
    bytes: &[u8],
) -> anyhow::Result<bool> {
    let success = super::dispatch_success(metadata, bytes, expected.extrinsic_index)?;
    let mut found = false;
    let name = if expected.params.is_buy {
        "StateOrderBuy"
    } else {
        "StateOrderSell"
    };
    for event in
        Events::<crate::DeepXRuntimeConfig>::decode_from(bytes.to_vec(), metadata.clone()).iter()
    {
        let event = event?;
        if event.phase() != Phase::ApplyExtrinsic(expected.extrinsic_index)
            || event.pallet_name() != "SpotMarket"
            || event.variant_name() != name
        {
            continue;
        }
        anyhow::ensure!(!found, "duplicate native placement event");
        let values = event.field_values()?;
        let order = values
            .at("order")
            .context("missing native placement order")?;
        anyhow::ensure!(
            field(order, "id")?.as_u128() == Some(u128::from(expected.order_id))
                && crate::fixed_bytes::<20>(field(order, "maker")?)? == expected.params.subaccount
                && crate::fixed_bytes::<32>(field(order, "pair")?)? == expected.params.pair
                && crate::spot_size(field(order, "base_amount")?)?
                    == super::narrow(super::U256::from_le_bytes(expected.params.base_amount))?
                && crate::spot_size(field(order, "quote_amount")?)?
                    == super::narrow(super::U256::from_le_bytes(expected.params.quote_amount))?
                && field(order, "is_buy")?.as_bool() == Some(expected.params.is_buy)
                && field(order, "reduce_only")?.as_bool() == Some(false)
                && variant(field(order, "post_only")?, "None")
                && is_ioc(field(order, "order_type")?),
            "native placement conflicts with signed terms"
        );
        if let Some(stp) = order.at("stp_mode") {
            anyhow::ensure!(variant(stp, "None"), "native placement changed STP mode");
        }
        found = true;
    }
    anyhow::ensure!(
        success == found,
        "native dispatch and business events disagree"
    );
    Ok(success)
}
