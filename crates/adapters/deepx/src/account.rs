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

//! Account ownership validation across signing and REST identity evidence.

use std::collections::HashSet;

use nautilus_core::{UUID4, UnixNanos, hex};
use nautilus_model::{
    enums::AccountType,
    events::AccountState,
    identifiers::AccountId,
    types::{AccountBalance, Currency, MarginBalance, Money},
};
use thiserror::Error;

use crate::{
    common::{DeepXPrivateKey, parse::scaled_i128_to_decimal},
    http::{
        DeepXDelegateMode, DeepXDelegateWallets, DeepXHttpError, DeepXSubaccountBalances,
        DeepXSubaccountMarginRatio, DeepXSubaccountProfile, DeepXWalletDelegateAccounts,
        DeepXWalletSubaccounts,
    },
    rpc::{
        DeepXCurrentMarginEvidence, DeepXFinalizedAccountEvidence, DeepXFinalizedCheckpoint,
        DeepXFinalizedMarginEvidence,
    },
    signing::{ApprovedRuntimeIdentity, SigningError, derive_signer_account_id},
};

const DEEPX_QUOTE_PRECISION: u32 = 6;

pub(crate) fn map_current_flat_usdc_balance(
    balances: &DeepXSubaccountBalances,
    margin: &DeepXSubaccountMarginRatio,
    current: &DeepXCurrentMarginEvidence,
) -> anyhow::Result<AccountBalance> {
    let observed = balances
        .address
        .strip_prefix("0x")
        .and_then(|value| hex::decode_array::<20>(value).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid DeepX backend balance account address"))?;
    anyhow::ensure!(
        observed == current.subaccount(),
        "DeepX backend and current-chain balance accounts differ",
    );
    let mut deposit = None;
    for asset in &balances.assets {
        anyhow::ensure!(
            asset.balance_borrowed.is_zero()
                && asset.balance_borrowed_usd.is_zero()
                && asset.borrow_interest.is_zero()
                && asset.borrow_interest_usd.is_zero(),
            "DeepX backend flat account has a borrow liability",
        );
        if asset.symbol.eq_ignore_ascii_case("USDC") {
            anyhow::ensure!(
                asset.decimals == DEEPX_QUOTE_PRECISION as u8
                    && deposit.replace(asset.balance).is_none(),
                "DeepX backend flat account has invalid USDC identity",
            );
        } else {
            anyhow::ensure!(
                asset.balance.is_zero(),
                "DeepX backend flat account has another deposited asset",
            );
        }
    }
    let deposit = deposit.ok_or_else(|| anyhow::anyhow!("DeepX backend USDC balance is absent"))?;
    anyhow::ensure!(
        deposit > rust_decimal::Decimal::ZERO
            && deposit.normalize().scale() <= DEEPX_QUOTE_PRECISION,
        "DeepX backend USDC balance is invalid or inexact",
    );
    let exact = margin
        .exact_margin
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("DeepX backend exact margin is absent"))?;
    let initial_collateral = scaled_i128_to_decimal(
        i128::try_from(current.initial().total_collateral())?,
        DEEPX_QUOTE_PRECISION,
    )?;
    let maintenance_collateral = scaled_i128_to_decimal(
        i128::try_from(current.maintenance().total_collateral())?,
        DEEPX_QUOTE_PRECISION,
    )?;
    anyhow::ensure!(
        current.initial().total_margin_required() == 0
            && current.maintenance().total_margin_required() == 0
            && exact.maint_margin_req.is_zero()
            && margin.margin_required.is_zero(),
        "DeepX flat account has a margin requirement",
    );

    // Current collateral can accrue after the backend balance observation
    anyhow::ensure!(
        deposit == exact.total_collateral
            && deposit <= initial_collateral
            && deposit <= maintenance_collateral,
        "DeepX backend deposit exceeds current-chain collateral or exact backend collateral differs",
    );
    let currency = Currency::USDC();
    let total = Money::from_decimal(deposit, currency)?;
    Ok(AccountBalance::new_checked(
        total,
        Money::zero(currency),
        total,
    )?)
}

/// Finalized framework margin with the chain observation that supports it.
#[derive(Clone, Debug)]
pub struct DeepXFinalizedMarginBalance {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    margin: MarginBalance,
}

impl DeepXFinalizedMarginBalance {
    /// Returns the finalized checkpoint supporting this margin balance.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the approved runtime identity observed at the checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the subaccount whose margin was observed.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the converted initial and maintenance requirements.
    #[must_use]
    pub const fn margin(&self) -> &MarginBalance {
        &self.margin
    }
}

/// Converts finalized margin evidence while retaining its checkpoint and account identity.
///
/// # Errors
///
/// Returns an error when either required margin exceeds the framework range or precision.
pub fn map_finalized_margin_balance_with_evidence(
    evidence: &DeepXFinalizedMarginEvidence,
) -> Result<DeepXFinalizedMarginBalance, DeepXMarginBalanceError> {
    Ok(DeepXFinalizedMarginBalance {
        checkpoint: evidence.checkpoint(),
        runtime_identity: evidence.runtime_identity().clone(),
        subaccount: evidence.subaccount(),
        margin: map_finalized_margin_balance(evidence)?,
    })
}

/// Errors raised while converting finalized DeepX margin requirements.
#[derive(Debug, Error)]
pub enum DeepXMarginBalanceError {
    /// A raw quote amount exceeds the framework's signed money range.
    #[error("DeepX finalized margin amount exceeds the framework range")]
    AmountOutOfRange,
    /// A raw quote amount cannot be represented exactly as a decimal.
    #[error("DeepX finalized margin amount cannot be represented: {0}")]
    AmountConversion(String),
}

/// Converts finalized initial and maintenance requirements to one account-level margin balance.
///
/// The DeepX runtime defines its quote asset as USDC and returns the aggregate amounts at
/// `QUOTE_PRECISION` (six decimal places). This maps required margins only; it does not map
/// collateral, free balances, or account state.
///
/// # Errors
///
/// Returns an error when an amount exceeds the framework range or cannot be represented by the
/// framework USDC currency.
pub fn map_finalized_margin_balance(
    evidence: &DeepXFinalizedMarginEvidence,
) -> Result<MarginBalance, DeepXMarginBalanceError> {
    fn to_money(amount: u128, currency: Currency) -> Result<Money, DeepXMarginBalanceError> {
        let amount =
            i128::try_from(amount).map_err(|_| DeepXMarginBalanceError::AmountOutOfRange)?;
        let decimal = scaled_i128_to_decimal(amount, DEEPX_QUOTE_PRECISION)
            .map_err(|e| DeepXMarginBalanceError::AmountConversion(e.to_string()))?;
        Money::from_decimal(decimal, currency)
            .map_err(|e| DeepXMarginBalanceError::AmountConversion(e.to_string()))
    }

    let currency = Currency::USDC();
    let initial = to_money(evidence.initial().total_margin_required(), currency)?;
    let maintenance = to_money(evidence.maintenance().total_margin_required(), currency)?;
    Ok(MarginBalance::new(initial, maintenance, None))
}

/// Errors raised while mapping a fully withdrawable flat account into framework balances.
#[derive(Debug, Error)]
pub enum DeepXFlatAccountBalanceError {
    /// Initial or maintenance margin is reserved, so funds cannot be classified as fully free.
    #[error("DeepX finalized account has a non-zero margin requirement")]
    MarginRequired,
    /// A perpetual position is open, so the flat-account boundary does not apply.
    #[error("DeepX finalized account has an open perpetual position")]
    PerpetualPositionOpen,
    /// A perpetual order is active, so the flat-account boundary does not apply.
    #[error("DeepX finalized account has an active perpetual order")]
    PerpetualOrderActive,
    /// A Spot order is active, so available balances cannot be proven fully free.
    #[error("DeepX finalized account has an active Spot order")]
    SpotOrderActive,
    /// A lending liability is present or its effective amount is contradictory.
    #[error("DeepX finalized lending asset {asset} has a borrow liability")]
    BorrowLiability { asset: String },
    /// An asset's effective deposit is absent, zero, or contradicts its stored deposit.
    #[error("DeepX finalized lending asset {asset} has invalid effective deposit evidence")]
    InvalidDeposit { asset: String },
    /// The complete effective deposit is not currently withdrawable.
    #[error("DeepX finalized lending asset {asset} is not fully withdrawable")]
    WithdrawalRestricted { asset: String },
    /// An asset symbol cannot be represented as one canonical framework currency code.
    #[error("DeepX finalized lending asset symbol is invalid")]
    InvalidAsset,
    /// The same normalized asset occurs in more than one lending position.
    #[error("DeepX finalized lending asset {0} occurs more than once")]
    DuplicateAsset(String),
    /// An exact runtime amount cannot be represented by the framework currency.
    #[error("DeepX finalized lending asset {asset} amount cannot be represented exactly: {reason}")]
    InexactAmount { asset: String, reason: String },
    /// No non-zero lending deposit exists from which to construct a framework balance.
    #[error("DeepX finalized flat account contains no balance")]
    Empty,
    /// Finalized chain time cannot be represented by the framework nanosecond timestamp.
    #[error("DeepX finalized account timestamp {0} milliseconds is out of range")]
    TimestampOutOfRange(u64),
}

/// Framework balances derived from a fully withdrawable, exposure-free finalized account.
#[derive(Clone, Debug)]
pub struct DeepXFinalizedFlatAccountBalances {
    checkpoint: DeepXFinalizedCheckpoint,
    runtime_identity: ApprovedRuntimeIdentity,
    subaccount: [u8; 20],
    timestamp_ms: u64,
    balances: Vec<AccountBalance>,
}

impl DeepXFinalizedFlatAccountBalances {
    /// Returns the finalized checkpoint supporting every mapped balance.
    #[must_use]
    pub const fn checkpoint(&self) -> DeepXFinalizedCheckpoint {
        self.checkpoint
    }

    /// Returns the read-only fixture-approved runtime identity at the checkpoint.
    #[must_use]
    pub const fn runtime_identity(&self) -> &ApprovedRuntimeIdentity {
        &self.runtime_identity
    }

    /// Returns the exact subaccount owning the balances.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns finalized `Timestamp.Now` supporting the mapped balances.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    /// Returns balances whose total amount is fully free and whose locked amount is zero.
    #[must_use]
    pub fn balances(&self) -> &[AccountBalance] {
        &self.balances
    }

    pub(crate) fn into_account_state(
        self,
        account_id: AccountId,
        account_type: AccountType,
        ts_init: UnixNanos,
    ) -> Result<AccountState, DeepXFlatAccountBalanceError> {
        let ts_event = i64::try_from(self.timestamp_ms)
            .ok()
            .and_then(UnixNanos::from_millis_checked)
            .ok_or(DeepXFlatAccountBalanceError::TimestampOutOfRange(
                self.timestamp_ms,
            ))?;
        Ok(AccountState::new(
            account_id,
            account_type,
            self.balances,
            Vec::new(),
            true,
            UUID4::new(),
            ts_event,
            ts_init,
            None,
        ))
    }
}

/// Maps finalized chain evidence into framework balances only for a fully withdrawable flat account.
///
/// The mapping is intentionally unavailable when any margin, perpetual or Spot exposure, borrow,
/// or withdrawal restriction exists. In the accepted state each non-zero effective lending
/// deposit is independently proven fully withdrawable, so `total == free` and `locked == 0`
/// require no inferred pending-order or collateral-allocation semantics.
///
/// # Errors
///
/// Returns an error for any exposure, liability, restricted withdrawal, duplicate or invalid asset,
/// empty account, integer overflow, or framework precision loss.
pub fn map_finalized_flat_account_balances(
    evidence: &DeepXFinalizedAccountEvidence,
) -> Result<DeepXFinalizedFlatAccountBalances, DeepXFlatAccountBalanceError> {
    if evidence.margin().initial().total_margin_required() != 0
        || evidence.margin().maintenance().total_margin_required() != 0
    {
        return Err(DeepXFlatAccountBalanceError::MarginRequired);
    }
    if !evidence.perp_positions().positions().is_empty() {
        return Err(DeepXFlatAccountBalanceError::PerpetualPositionOpen);
    }
    if !evidence.perp_orders().orders().is_empty() {
        return Err(DeepXFlatAccountBalanceError::PerpetualOrderActive);
    }
    if !evidence.spot_orders().active_order_ids().is_empty() {
        return Err(DeepXFlatAccountBalanceError::SpotOrderActive);
    }

    let mut observed_assets = HashSet::new();
    let mut balances = Vec::with_capacity(evidence.lending().positions().len());
    for position in evidence.lending().positions() {
        let asset = std::str::from_utf8(position.asset())
            .ok()
            .filter(|value| {
                !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
            .ok_or(DeepXFlatAccountBalanceError::InvalidAsset)?
            .to_ascii_uppercase();
        if !observed_assets.insert(asset.clone()) {
            return Err(DeepXFlatAccountBalanceError::DuplicateAsset(asset));
        }
        if position.scaled_borrow() != 0 || position.effective_borrow().is_some() {
            return Err(DeepXFlatAccountBalanceError::BorrowLiability { asset });
        }
        let deposit = position
            .effective_deposit()
            .filter(|value| *value != 0)
            .ok_or_else(|| DeepXFlatAccountBalanceError::InvalidDeposit {
                asset: asset.clone(),
            })?;
        if position.scaled_deposit() == 0 {
            return Err(DeepXFlatAccountBalanceError::InvalidDeposit { asset });
        }
        if position.max_withdraw_amount() != Some(deposit) {
            return Err(DeepXFlatAccountBalanceError::WithdrawalRestricted { asset });
        }
        let amount = i128::try_from(deposit)
            .map_err(|error| DeepXFlatAccountBalanceError::InexactAmount {
                asset: asset.clone(),
                reason: error.to_string(),
            })
            .and_then(|value| {
                scaled_i128_to_decimal(value, position.pool().decimal()).map_err(|error| {
                    DeepXFlatAccountBalanceError::InexactAmount {
                        asset: asset.clone(),
                        reason: error.to_string(),
                    }
                })
            })?;
        let currency = Currency::get_or_create_crypto(&asset);
        if amount.normalize().scale() > u32::from(currency.precision) {
            return Err(DeepXFlatAccountBalanceError::InexactAmount {
                asset,
                reason: format!(
                    "amount scale {} exceeds currency precision {}",
                    amount.normalize().scale(),
                    currency.precision,
                ),
            });
        }
        let total = Money::from_decimal(amount, currency).map_err(|error| {
            DeepXFlatAccountBalanceError::InexactAmount {
                asset: asset.clone(),
                reason: error.to_string(),
            }
        })?;
        let locked = Money::zero(currency);
        let balance = AccountBalance::new_checked(total, locked, total).map_err(|error| {
            DeepXFlatAccountBalanceError::InexactAmount {
                asset,
                reason: error.to_string(),
            }
        })?;
        balances.push(balance);
    }
    if balances.is_empty() {
        return Err(DeepXFlatAccountBalanceError::Empty);
    }
    balances.sort_unstable_by_key(|balance| balance.currency.code);

    Ok(DeepXFinalizedFlatAccountBalances {
        checkpoint: evidence.checkpoint(),
        runtime_identity: evidence.runtime_identity().clone(),
        subaccount: evidence.subaccount(),
        timestamp_ms: evidence.timestamp_ms(),
        balances,
    })
}

/// Errors raised while proving signer ownership of a configured subaccount.
#[derive(Debug, Error)]
pub enum DeepXAccountOwnershipError {
    /// The authoritative REST identity query failed.
    #[error(transparent)]
    Http(#[from] DeepXHttpError),
    /// The configured signing identity could not be derived.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// An address in the supplied identity evidence is not an exact AccountId20.
    #[error("invalid DeepX {field} address")]
    InvalidAddress { field: &'static str },
    /// The account directory was queried for a wallet other than the signer.
    #[error("DeepX account directory wallet does not match the signing identity")]
    DirectoryWalletMismatch,
    /// The account directory contains a repeated subaccount identity.
    #[error("DeepX account directory contains a duplicate subaccount")]
    DuplicateSubaccount,
    /// The configured subaccount is absent from the signer's account directory.
    #[error("configured DeepX subaccount is not owned by the signing identity")]
    SubaccountNotOwned,
    /// The account directory changed while the profile was being checked.
    #[error("DeepX account directory changed during ownership verification")]
    DirectoryChanged,
    /// The profile belongs to a subaccount other than the configured identity.
    #[error("DeepX subaccount profile address does not match the configured subaccount")]
    ProfileAddressMismatch,
    /// The profile authority is not the configured signing identity.
    #[error("DeepX subaccount profile authority does not match the signing identity")]
    ProfileAuthorityMismatch,
    /// The configured subaccount is not active and cannot pass execution startup.
    #[error("DeepX subaccount profile is not active: {0}")]
    InactiveSubaccount(String),
    /// The delegate's reverse directory does not contain the profile authority.
    #[error("DeepX delegate directory does not contain the subaccount authority")]
    DelegatorUnavailable,
    /// The authority's delegate directory does not contain the signer.
    #[error("DeepX subaccount authority has not configured the signing delegate")]
    DelegateUnavailable,
    /// The configured delegate has no order permission.
    #[error("DeepX signing delegate is not permitted to place or cancel orders")]
    DelegateModeUnsupported,
    /// The configured delegate has expired or the backend marked it inactive.
    #[error("DeepX signing delegate is inactive or expired")]
    DelegateInactive,
    /// A delegate directory belongs to an address other than the queried identity.
    #[error("DeepX delegate directory identity mismatch")]
    DelegateDirectoryMismatch,
}

/// Complete REST-backed ownership evidence for one signer and configured subaccount.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXAccountOwnershipProof {
    signer: [u8; 20],
    wallet: [u8; 20],
    subaccount: [u8; 20],
    profile_height: u64,
    profile_created_at: u64,
}

impl DeepXAccountOwnershipProof {
    /// Returns the signer AccountId20.
    #[must_use]
    pub const fn signer(&self) -> [u8; 20] {
        self.signer
    }

    /// Returns the wallet that owns the configured subaccount.
    #[must_use]
    pub const fn wallet(&self) -> [u8; 20] {
        self.wallet
    }

    /// Returns the owned subaccount AccountId20.
    #[must_use]
    pub const fn subaccount(&self) -> [u8; 20] {
        self.subaccount
    }

    /// Returns the profile observation block height reported by REST.
    #[must_use]
    pub const fn profile_height(&self) -> u64 {
        self.profile_height
    }

    /// Returns the subaccount creation timestamp in Unix milliseconds.
    #[must_use]
    pub const fn profile_created_at(&self) -> u64 {
        self.profile_created_at
    }
}

/// Verifies that REST directory and profile evidence bind a configured subaccount to a signer.
///
/// This proves consistency of the supplied point-in-time REST observations with the locally held
/// private key. It does not prove freshness, private-stream authentication, or trading authority.
///
/// # Errors
///
/// Returns an error for an invalid signing key or address, duplicate directory entries, an absent
/// configured subaccount, signer/profile mismatches, or an inactive profile.
pub(crate) fn verify_account_ownership(
    key: &DeepXPrivateKey,
    configured_subaccount: &str,
    directory: &DeepXWalletSubaccounts,
    profile: &DeepXSubaccountProfile,
) -> Result<DeepXAccountOwnershipProof, DeepXAccountOwnershipError> {
    let signer = derive_signer_account_id(key)?;
    verify_account_ownership_for_wallet(signer, signer, configured_subaccount, directory, profile)
}

pub(crate) fn verify_delegated_account_ownership(
    key: &DeepXPrivateKey,
    configured_subaccount: &str,
    directory: &DeepXWalletSubaccounts,
    profile: &DeepXSubaccountProfile,
    delegates: &DeepXWalletDelegateAccounts,
    delegators: &DeepXDelegateWallets,
    now_ms: u64,
) -> Result<DeepXAccountOwnershipProof, DeepXAccountOwnershipError> {
    let signer = derive_signer_account_id(key)?;
    let wallet = parse_account_id20(&profile.authority, "profile authority")?;
    if parse_account_id20(delegates.wallet(), "delegate directory wallet")? != wallet
        || parse_account_id20(delegators.delegate(), "delegator directory delegate")? != signer
    {
        return Err(DeepXAccountOwnershipError::DelegateDirectoryMismatch);
    }
    let mut observed_wallets = HashSet::new();
    for address in &delegators.wallets {
        let address = parse_account_id20(address, "delegator wallet")?;
        if !observed_wallets.insert(address) {
            return Err(DeepXAccountOwnershipError::DelegateDirectoryMismatch);
        }
    }
    if !observed_wallets.contains(&wallet) {
        return Err(DeepXAccountOwnershipError::DelegatorUnavailable);
    }
    let mut observed_delegates = HashSet::new();
    let mut delegate = None;
    for account in &delegates.accounts {
        let address = parse_account_id20(&account.delegate_address, "delegate address")?;
        if !observed_delegates.insert(address) {
            return Err(DeepXAccountOwnershipError::DelegateDirectoryMismatch);
        }
        if address == signer {
            delegate = Some(account);
        }
    }
    let delegate = delegate.ok_or(DeepXAccountOwnershipError::DelegateUnavailable)?;
    if delegate.mode != DeepXDelegateMode::PlaceOrCancelOrder {
        return Err(DeepXAccountOwnershipError::DelegateModeUnsupported);
    }
    if !delegate.active || (delegate.valid_until != 0 && delegate.valid_until <= now_ms) {
        return Err(DeepXAccountOwnershipError::DelegateInactive);
    }
    verify_account_ownership_for_wallet(signer, wallet, configured_subaccount, directory, profile)
}

fn verify_account_ownership_for_wallet(
    signer: [u8; 20],
    wallet: [u8; 20],
    configured_subaccount: &str,
    directory: &DeepXWalletSubaccounts,
    profile: &DeepXSubaccountProfile,
) -> Result<DeepXAccountOwnershipProof, DeepXAccountOwnershipError> {
    let subaccount = parse_account_id20(configured_subaccount, "configured subaccount")?;
    let directory_wallet = parse_account_id20(directory.wallet(), "account directory wallet")?;
    if directory_wallet != wallet {
        return Err(DeepXAccountOwnershipError::DirectoryWalletMismatch);
    }

    let mut observed = HashSet::new();
    let mut owns_subaccount = false;
    for address in &directory.addresses {
        let address = parse_account_id20(address, "account directory subaccount")?;
        if !observed.insert(address) {
            return Err(DeepXAccountOwnershipError::DuplicateSubaccount);
        }
        owns_subaccount |= address == subaccount;
    }
    if !owns_subaccount {
        return Err(DeepXAccountOwnershipError::SubaccountNotOwned);
    }

    if parse_account_id20(&profile.address, "profile subaccount")? != subaccount {
        return Err(DeepXAccountOwnershipError::ProfileAddressMismatch);
    }
    if parse_account_id20(&profile.authority, "profile authority")? != wallet {
        return Err(DeepXAccountOwnershipError::ProfileAuthorityMismatch);
    }
    if profile.status != "Active" {
        return Err(DeepXAccountOwnershipError::InactiveSubaccount(
            profile.status.clone(),
        ));
    }

    Ok(DeepXAccountOwnershipProof {
        signer,
        wallet,
        subaccount,
        profile_height: profile.height,
        profile_created_at: profile.created_at,
    })
}

fn parse_account_id20(
    value: &str,
    field: &'static str,
) -> Result<[u8; 20], DeepXAccountOwnershipError> {
    let value = value
        .strip_prefix("0x")
        .ok_or(DeepXAccountOwnershipError::InvalidAddress { field })?;
    hex::decode_array::<20>(value).map_err(|_| DeepXAccountOwnershipError::InvalidAddress { field })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        common::DeepXKeyScheme,
        http::{DeepXDelegateAccount, DeepXSubaccountProfile},
    };

    const PRIVATE_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn key() -> DeepXPrivateKey {
        DeepXPrivateKey::new(PRIVATE_KEY, &DeepXKeyScheme::Secp256k1).unwrap()
    }

    fn evidence() -> (DeepXWalletSubaccounts, DeepXSubaccountProfile, String) {
        let signer = derive_signer_account_id(&key()).unwrap();
        let wallet = format!("0x{}", hex::encode(signer));
        let subaccount = format!("0x{}", hex::encode([0x11; 20]));
        let directory = DeepXWalletSubaccounts::new(
            wallet.clone(),
            vec![subaccount.clone(), format!("0x{}", hex::encode([0x22; 20]))],
        );
        let profile = DeepXSubaccountProfile {
            authority: wallet,
            address: subaccount.clone(),
            name: "test".to_string(),
            status: "Active".to_string(),
            spot_positions: Vec::new(),
            next_order_id: 1,
            spot_margin_trading_enabled: false,
            margin_strategy: "Cross".to_string(),
            height: 50_070_126,
            created_at: 1_779_848_876_302,
        };
        (directory, profile, subaccount)
    }

    fn delegated_evidence() -> (
        DeepXWalletSubaccounts,
        DeepXSubaccountProfile,
        DeepXWalletDelegateAccounts,
        DeepXDelegateWallets,
        String,
    ) {
        let (directory, mut profile, subaccount) = evidence();
        let wallet = format!("0x{}", hex::encode([0x33; 20]));
        let signer = format!(
            "0x{}",
            hex::encode(derive_signer_account_id(&key()).unwrap())
        );
        profile.authority = wallet.clone();
        let directory = DeepXWalletSubaccounts::new(wallet.clone(), directory.addresses);
        let delegates = DeepXWalletDelegateAccounts::new(
            wallet.clone(),
            vec![DeepXDelegateAccount {
                delegate_address: signer.clone(),
                delegate_name: "order bot".to_string(),
                valid_until: 2_000,
                mode: DeepXDelegateMode::PlaceOrCancelOrder,
                create_time: 100,
                active: true,
            }],
        );
        let delegators = DeepXDelegateWallets::new(signer, vec![wallet]);
        (directory, profile, delegates, delegators, subaccount)
    }

    #[rstest]
    fn ownership_proof_binds_signer_directory_and_profile() {
        let (directory, profile, subaccount) = evidence();

        let proof = verify_account_ownership(&key(), &subaccount, &directory, &profile).unwrap();

        assert_eq!(proof.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(proof.wallet(), proof.signer());
        assert_eq!(proof.subaccount(), [0x11; 20]);
        assert_eq!(proof.profile_height(), profile.height);
        assert_eq!(proof.profile_created_at(), profile.created_at);
    }

    #[rstest]
    fn delegated_ownership_requires_both_directories_and_current_order_permission() {
        let (directory, profile, mut delegates, mut delegators, subaccount) = delegated_evidence();
        let verify =
            |delegates: &DeepXWalletDelegateAccounts, delegators: &DeepXDelegateWallets, now_ms| {
                verify_delegated_account_ownership(
                    &key(),
                    &subaccount,
                    &directory,
                    &profile,
                    delegates,
                    delegators,
                    now_ms,
                )
            };

        let proof = verify(&delegates, &delegators, 1_999).unwrap();
        assert_eq!(proof.wallet(), [0x33; 20]);
        assert_eq!(proof.signer(), derive_signer_account_id(&key()).unwrap());
        assert_eq!(proof.subaccount(), [0x11; 20]);

        assert!(matches!(
            verify(&delegates, &delegators, 2_000),
            Err(DeepXAccountOwnershipError::DelegateInactive),
        ));
        delegates.accounts[0].mode = DeepXDelegateMode::Disable;
        assert!(matches!(
            verify(&delegates, &delegators, 1_999),
            Err(DeepXAccountOwnershipError::DelegateModeUnsupported),
        ));
        delegates.accounts[0].mode = DeepXDelegateMode::PlaceOrCancelOrder;
        delegators.wallets.clear();
        assert!(matches!(
            verify(&delegates, &delegators, 1_999),
            Err(DeepXAccountOwnershipError::DelegatorUnavailable),
        ));
    }

    #[rstest]
    fn ownership_proof_rejects_foreign_directory_wallet() {
        let (directory, profile, subaccount) = evidence();
        let directory = DeepXWalletSubaccounts::new(
            format!("0x{}", hex::encode([0x33; 20])),
            directory.addresses,
        );

        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::DirectoryWalletMismatch),
        ));
    }

    #[rstest]
    fn ownership_proof_rejects_missing_and_duplicate_subaccounts() {
        let (directory, profile, subaccount) = evidence();
        let missing = DeepXWalletSubaccounts::new(
            directory.wallet().to_string(),
            vec![format!("0x{}", hex::encode([0x22; 20]))],
        );
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &missing, &profile),
            Err(DeepXAccountOwnershipError::SubaccountNotOwned),
        ));

        let duplicate = DeepXWalletSubaccounts::new(
            directory.wallet().to_string(),
            vec![subaccount.clone(), subaccount.clone()],
        );
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &duplicate, &profile),
            Err(DeepXAccountOwnershipError::DuplicateSubaccount),
        ));
    }

    #[rstest]
    fn ownership_proof_rejects_profile_identity_and_status_mismatches() {
        let (directory, mut profile, subaccount) = evidence();
        profile.address = format!("0x{}", hex::encode([0x44; 20]));
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::ProfileAddressMismatch),
        ));

        let (_, mut profile, _) = evidence();
        profile.authority = format!("0x{}", hex::encode([0x44; 20]));
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::ProfileAuthorityMismatch),
        ));

        let (_, mut profile, _) = evidence();
        profile.status = "Closed".to_string();
        assert!(matches!(
            verify_account_ownership(&key(), &subaccount, &directory, &profile),
            Err(DeepXAccountOwnershipError::InactiveSubaccount(status)) if status == "Closed",
        ));
    }

    #[rstest]
    fn ownership_proof_rejects_malformed_account_id20() {
        let (directory, profile, _) = evidence();

        assert!(matches!(
            verify_account_ownership(&key(), "subaccount", &directory, &profile),
            Err(DeepXAccountOwnershipError::InvalidAddress {
                field: "configured subaccount"
            }),
        ));
    }
}
