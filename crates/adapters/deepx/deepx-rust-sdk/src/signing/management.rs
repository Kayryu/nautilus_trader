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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! DeepX runtime management arguments and their metadata-driven encoding.

use subxt_core::dynamic::Value;

/// Exact raw arguments for an owner-signed global leverage setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSetGlobalLeverageParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub subaccount: [u8; 20],
    /// Raw runtime leverage, or `None` to clear the global setting.
    pub max_leverage: Option<u64>,
}

impl DeepXSetGlobalLeverageParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            optional_leverage(self.max_leverage),
        ]
    }
}

/// Exact raw arguments for an owner-signed per-market leverage override.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSetPerMarketLeverageParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub subaccount: [u8; 20],
    /// Runtime perpetual market identifier.
    pub market_id: u16,
    /// Raw runtime leverage, or `None` to remove this market override.
    pub max_leverage: Option<u64>,
}

impl DeepXSetPerMarketLeverageParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::u128(u128::from(self.market_id)),
            optional_leverage(self.max_leverage),
        ]
    }
}

fn optional_leverage(value: Option<u64>) -> Value {
    match value {
        Some(value) => Value::unnamed_variant("Some", [Value::u128(u128::from(value))]),
        None => Value::unnamed_variant("None", Vec::<Value>::new()),
    }
}

/// Exact raw arguments for subaccount initialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXInitializeSubaccountParams {
    /// Opaque name bytes, bounded to 32 by the runtime call type.
    pub name: Vec<u8>,
}

impl DeepXInitializeSubaccountParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.name)]
    }
}

/// Exact raw arguments for subaccount deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXDeleteSubaccountParams {
    /// Exact runtime subaccount identity, without inferred authorization.
    pub subaccount: [u8; 20],
}

impl DeepXDeleteSubaccountParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.subaccount)]
    }
}

/// Exact raw arguments for subaccount renaming.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRenameSubaccountParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub subaccount: [u8; 20],
    /// Opaque new name bytes, bounded to 32 by the runtime call type.
    pub new_name: Vec<u8>,
}

impl DeepXRenameSubaccountParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.subaccount),
            Value::from_bytes(self.new_name),
        ]
    }
}

/// Exact raw arguments for a subaccount Spot margin setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSetSpotMarginParams {
    /// Exact runtime subaccount identity, without inferred ownership.
    pub address: [u8; 20],
    /// Requested Spot margin setting.
    pub enable_spot_margin: bool,
}

impl DeepXSetSpotMarginParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.address),
            Value::bool(self.enable_spot_margin),
        ]
    }
}

/// Exact raw arguments for wallet delegate creation or renewal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXSetDelegateAccountParams {
    /// Exact runtime delegate identity, without inferred authorization.
    pub delegate: [u8; 20],
    /// Opaque runtime label bytes.
    pub name: Vec<u8>,
    /// Raw Unix millisecond expiry supplied by the caller.
    pub valid_until: u64,
}

impl DeepXSetDelegateAccountParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.delegate),
            Value::from_bytes(self.name),
            Value::u128(u128::from(self.valid_until)),
        ]
    }
}

/// Delegate modes accepted by the captured runtime's update call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXWritableDelegateMode {
    /// Allows order placement and cancellation.
    PlaceOrCancelOrder,
    /// Disables delegate permissions.
    Disable,
}

impl DeepXWritableDelegateMode {
    fn into_dynamic_value(self) -> Value {
        let variant = match self {
            Self::PlaceOrCancelOrder => "PlaceOrCancelOrder",
            Self::Disable => "Disable",
        };
        Value::unnamed_variant(variant, Vec::<Value>::new())
    }
}

/// Exact raw arguments for a wallet delegate mode update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXUpdateDelegateModeParams {
    /// Exact delegate identity, without inferred ownership or eligibility.
    pub address: [u8; 20],
    /// Mode accepted by the captured runtime dispatch logic.
    pub new_mode: DeepXWritableDelegateMode,
}

impl DeepXUpdateDelegateModeParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![
            Value::from_bytes(self.address),
            self.new_mode.into_dynamic_value(),
        ]
    }
}

/// Exact raw arguments for wallet delegate removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXRemoveDelegateAccountParams {
    /// Exact runtime delegate identity, without inferred authorization.
    pub delegate: [u8; 20],
}

impl DeepXRemoveDelegateAccountParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        vec![Value::from_bytes(self.delegate)]
    }
}

/// Exact raw arguments for a Lending quota purchase call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXBuyQuotaParams {
    /// AccountId20 whose quota would be increased.
    pub address: [u8; 20],
    /// Raw runtime quota count.
    pub quota: u32,
    /// Optional source subaccount for the runtime purchase path.
    pub from_subaccount: Option<[u8; 20]>,
}

impl DeepXBuyQuotaParams {
    /// Returns the encoded DeepX runtime call arguments.
    #[must_use]
    pub fn into_dynamic_arguments(self) -> Vec<Value> {
        let from_subaccount = match self.from_subaccount {
            Some(address) => Value::unnamed_variant("Some", [Value::from_bytes(address)]),
            None => Value::unnamed_variant("None", Vec::<Value>::new()),
        };
        vec![
            Value::from_bytes(self.address),
            Value::u128(u128::from(self.quota)),
            from_subaccount,
        ]
    }
}
