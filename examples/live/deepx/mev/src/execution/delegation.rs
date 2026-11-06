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

//! Block-pinned delegate authorization and shared-wallet concurrency checks.

use std::collections::BTreeSet;

use subxt_core::ext::scale_value::ValueDef;

use super::*;

impl Execution {
    pub(super) async fn check_delegation(
        &self,
        rpc: &DeepXRpcClient,
        metadata: &Metadata,
        head: &str,
        pending: &[String],
    ) -> anyhow::Result<()> {
        let wallet = self.wallet();
        let account = read_storage(
            rpc,
            metadata,
            head,
            "Subaccount",
            "SubaccountInfo",
            self.settings.subaccount,
        )
        .await?;
        anyhow::ensure!(
            super::super::fixed_bytes::<20>(account.at("authority").context("missing authority")?)?
                == wallet
                && super::super::fixed_bytes::<20>(
                    account.at("address").context("missing address")?
                )? == self.settings.subaccount,
            "delegated subaccount ownership changed"
        );
        let status = account.at("status").context("missing account status")?;
        anyhow::ensure!(
            matches!(&status.value, ValueDef::Variant(status) if status.name == "Active"),
            "delegated execution requires an active subaccount"
        );
        anyhow::ensure!(
            account
                .at("spot_margin_trading_enabled")
                .and_then(ScaleValue::as_bool)
                == Some(false),
            "delegated execution requires Spot margin disabled"
        );
        let delegates = read_storage(
            rpc,
            metadata,
            head,
            "Subaccount",
            "DelegateAccountsFor",
            wallet,
        )
        .await?;
        let reverse = read_storage(
            rpc,
            metadata,
            head,
            "Subaccount",
            "DelegatorAccountsFor",
            self.signer,
        )
        .await?;
        let now = query_timestamp_now_at_block(rpc, super::super::parse_hash(head)?).await?;
        let related = authorized_signers(&delegates, &reverse, wallet, self.signer, now)?;
        require_no_shared_pending(metadata, pending, &related)
    }
}

fn authorized_signers(
    delegates: &ScaleValue<u32>,
    reverse: &ScaleValue<u32>,
    wallet: [u8; 20],
    signer: [u8; 20],
    now: u64,
) -> anyhow::Result<BTreeSet<[u8; 20]>> {
    anyhow::ensure!(wallet != signer, "delegate signer must differ from wallet");
    let ValueDef::Composite(records) = &delegates.value else {
        anyhow::bail!("invalid delegate records");
    };
    let ValueDef::Composite(owners) = &reverse.value else {
        anyhow::bail!("invalid delegator records");
    };
    anyhow::ensure!(
        records.len() <= 256 && owners.len() <= 256,
        "delegation records exceed bound"
    );
    let owners = owners
        .values()
        .map(super::super::fixed_bytes::<20>)
        .collect::<anyhow::Result<BTreeSet<_>>>()?;
    anyhow::ensure!(
        owners.contains(&wallet),
        "delegate reverse wallet binding is missing"
    );
    let mut related = BTreeSet::from([wallet]);
    let mut permitted = false;
    for record in records.values() {
        let address = super::super::fixed_bytes::<20>(
            record
                .at("delegate_address")
                .context("missing delegate address")?,
        )?;
        anyhow::ensure!(
            address != [0; 20] && related.insert(address),
            "invalid or duplicate delegate address"
        );
        let deadline = record
            .at("valid_until")
            .and_then(ScaleValue::as_u128)
            .and_then(|n| u64::try_from(n).ok())
            .context("invalid delegate deadline")?;
        let mode = record.at("mode").context("missing delegate mode")?;
        let ValueDef::Variant(mode) = &mode.value else {
            anyhow::bail!("invalid delegate mode");
        };
        if address == signer {
            permitted = (deadline == 0 || deadline > now) && mode.name == "PlaceOrCancelOrder";
        }
    }
    anyhow::ensure!(
        permitted,
        "signer has no unexpired order delegation from wallet"
    );
    Ok(related)
}

pub(super) fn require_no_shared_pending(
    metadata: &Metadata,
    pending: &[String],
    related: &BTreeSet<[u8; 20]>,
) -> anyhow::Result<()> {
    // Conservatively reject all operations by these signers, including calls for other wallets.
    // They may share this wallet's time-nonce window, quota or balances.
    for bytes in pending {
        let extrinsics = Extrinsics::<super::super::DeepXRuntimeConfig>::decode_from(
            vec![decode_hex(bytes)?],
            metadata.clone(),
        )?;
        let details = extrinsics
            .iter()
            .next()
            .context("missing shared pending operation")?;
        if let Some(address) = details.address_bytes() {
            let address: [u8; 20] = address
                .try_into()
                .context("invalid pending signer address")?;
            anyhow::ensure!(
                !related.contains(&address),
                "wallet or delegate has another pending operation"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contextual(value: ScaleValue) -> ScaleValue<u32> {
        value.map_context(|()| 0)
    }

    fn records(deadline: u64, mode: &str) -> ScaleValue<u32> {
        contextual(ScaleValue::unnamed_composite([
            ScaleValue::named_composite([
                ("delegate_address", ScaleValue::from_bytes([2; 20])),
                ("valid_until", ScaleValue::u128(u128::from(deadline))),
                ("mode", ScaleValue::unnamed_variant(mode, [])),
            ]),
        ]))
    }

    #[test]
    fn order_delegation_requires_both_bindings_mode_and_strict_deadline() {
        let reverse = contextual(ScaleValue::unnamed_composite([ScaleValue::from_bytes(
            [1; 20],
        )]));
        for (deadline, mode, expected) in [
            (101, "PlaceOrCancelOrder", true),
            (100, "PlaceOrCancelOrder", false),
            (99, "PlaceOrCancelOrder", false),
            (0, "PlaceOrCancelOrder", true),
            (101, "Disable", false),
            (101, "DepositOrWithdraw", false),
        ] {
            let result =
                authorized_signers(&records(deadline, mode), &reverse, [1; 20], [2; 20], 100);
            assert_eq!(result.is_ok(), expected);
            if let Ok(related) = result {
                assert_eq!(related, BTreeSet::from([[1; 20], [2; 20]]));
            }
        }
        let other_owner = contextual(ScaleValue::unnamed_composite([ScaleValue::from_bytes(
            [3; 20],
        )]));
        assert!(
            authorized_signers(
                &records(101, "PlaceOrCancelOrder"),
                &other_owner,
                [1; 20],
                [2; 20],
                100
            )
            .is_err()
        );
        assert!(
            authorized_signers(
                &records(101, "PlaceOrCancelOrder"),
                &reverse,
                [1; 20],
                [3; 20],
                100
            )
            .is_err()
        );
    }
}
