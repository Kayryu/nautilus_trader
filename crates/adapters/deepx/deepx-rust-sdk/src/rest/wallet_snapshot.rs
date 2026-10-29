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

//! Complete wallet account observations from typed REST reads.

use super::{
    DeepXRestError, DeepXRestTransport,
    models::{
        DeepXSubaccountProfile, DeepXSubaccountSnapshot, DeepXUserStats,
        DeepXWalletAccountSnapshot, DeepXWalletSubaccounts,
    },
};

/// Checks identity continuity for a complete wallet account collection.
#[derive(Debug)]
pub struct DeepXWalletCollectionValidation<'a> {
    directory: &'a DeepXWalletSubaccounts,
    endpoint: &'static str,
    changed_message: &'static str,
}

impl<'a> DeepXWalletCollectionValidation<'a> {
    /// Creates account-snapshot identity validation for the initial wallet directory.
    #[must_use]
    pub const fn for_account_snapshot(directory: &'a DeepXWalletSubaccounts) -> Self {
        Self {
            directory,
            endpoint: "wallet account snapshot",
            changed_message: "wallet directory changed during account collection",
        }
    }

    /// Creates position-collection identity validation for the initial wallet directory.
    #[must_use]
    pub const fn for_perp_positions(directory: &'a DeepXWalletSubaccounts) -> Self {
        Self {
            directory,
            endpoint: "wallet perp positions",
            changed_message: "wallet directory changed during position collection",
        }
    }

    /// Checks that user statistics describe exactly the initial wallet directory.
    ///
    /// # Errors
    ///
    /// Returns an invalid-account-response error when membership differs.
    pub fn validate_stats(&self, stats: &DeepXUserStats) -> Result<(), DeepXRestError> {
        if !self.directory.has_same_membership(&stats.subaccounts) {
            return Err(DeepXRestError::InvalidAccountResponse {
                endpoint: self.endpoint,
                message: "user stats subaccounts differ from wallet directory".to_string(),
            });
        }
        Ok(())
    }

    /// Checks that a subaccount profile belongs to the requested wallet.
    ///
    /// # Errors
    ///
    /// Returns an invalid-account-response error for another authority.
    pub fn validate_profile(&self, profile: &DeepXSubaccountProfile) -> Result<(), DeepXRestError> {
        if !profile
            .authority
            .eq_ignore_ascii_case(self.directory.wallet())
        {
            return Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "subaccount info",
                message: "profile authority does not match the expected wallet".to_string(),
            });
        }
        Ok(())
    }

    /// Checks that wallet membership remained stable during the collection.
    ///
    /// # Errors
    ///
    /// Returns an invalid-account-response error when the final directory differs.
    pub fn validate_final_directory(
        &self,
        final_directory: &DeepXWalletSubaccounts,
    ) -> Result<(), DeepXRestError> {
        if !self
            .directory
            .has_same_membership(&final_directory.addresses)
        {
            return Err(DeepXRestError::InvalidAccountResponse {
                endpoint: self.endpoint,
                message: self.changed_message.to_string(),
            });
        }
        Ok(())
    }
}

impl DeepXRestTransport {
    /// Returns all locally validated account reads for one wallet directory.
    ///
    /// Statistics and the final directory must agree with the initial directory. Profiles must
    /// identify the queried wallet as their authority. Constituent REST reads are not block-pinned
    /// and do not form an atomic venue snapshot.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, or inconsistent account evidence error. No partial
    /// collection is returned.
    pub async fn get_wallet_account_snapshot(
        &self,
        base_url: &str,
        wallet: &str,
    ) -> Result<DeepXWalletAccountSnapshot, DeepXRestError> {
        let directory = self.get_wallet_subaccounts(base_url, wallet).await?;
        let stats = self.get_user_stats(base_url, wallet).await?;
        let validation = DeepXWalletCollectionValidation::for_account_snapshot(&directory);
        validation.validate_stats(&stats)?;

        let mut subaccounts = Vec::with_capacity(directory.addresses.len());
        for address in &directory.addresses {
            let profile = self.get_subaccount_info(base_url, address).await?;
            validation.validate_profile(&profile)?;
            let balances = self.get_subaccount_balances(base_url, address).await?;
            let equity = self.get_subaccount_equity(base_url, address).await?;
            let margin_ratio = self.get_subaccount_margin_ratio(base_url, address).await?;
            subaccounts.push(DeepXSubaccountSnapshot {
                profile,
                balances,
                equity,
                margin_ratio,
            });
        }

        let final_directory = self.get_wallet_subaccounts(base_url, wallet).await?;
        validation.validate_final_directory(&final_directory)?;
        Ok(DeepXWalletAccountSnapshot {
            directory,
            stats,
            subaccounts,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{Json, Router, routing::get};
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const STATS: &str = include_str!("../../test_data/http/testnet/wallet_user_stats.json");
    const PROFILE: &str = include_str!("../../test_data/http/testnet/subaccount_info.json");
    const BALANCES: &str = include_str!("../../test_data/http/testnet/subaccount_balances.json");
    const EQUITY: &str = include_str!("../../test_data/http/testnet/subaccount_equity.json");
    const MARGIN: &str =
        include_str!("../../test_data/http/testnet/subaccount_margin_ratio_exact.json");

    #[rstest]
    fn membership_ignores_order_and_case_but_rejects_duplicates() {
        let directory = super::super::models::DeepXWalletSubaccounts::new(
            WALLET.to_string(),
            vec![
                SUBACCOUNT.to_string(),
                "0x1111111111111111111111111111111111111111".to_string(),
            ],
        );
        assert!(directory.has_same_membership(&[
            "0x1111111111111111111111111111111111111111".to_string(),
            SUBACCOUNT.to_ascii_uppercase().replacen("0X", "0x", 1),
        ]));
        assert!(!directory.has_same_membership(&[SUBACCOUNT.to_string(), SUBACCOUNT.to_string()]));
    }

    #[rstest]
    #[case("valid")]
    #[case("foreign-stats")]
    #[case("foreign-authority")]
    #[case("foreign-equity")]
    #[case("negative-margin")]
    #[case("changed-directory")]
    #[tokio::test]
    async fn complete_wallet_snapshot_validates_every_read(#[case] scenario: &'static str) {
        let directory = json!({"code": 200, "msg": "success", "data": [SUBACCOUNT], "fail": false});
        let reads = Arc::new(AtomicUsize::new(0));
        let changed = scenario == "changed-directory";
        let mut stats: Value = serde_json::from_str(STATS).unwrap();
        stats["data"]["subaccounts"] = json!([if scenario == "foreign-stats" {
            "0x1111111111111111111111111111111111111111"
        } else {
            SUBACCOUNT
        }]);
        stats["data"]["numberOfSubAccounts"] = json!(1);
        let mut profile: Value = serde_json::from_str(PROFILE).unwrap();
        if scenario == "foreign-authority" {
            profile["data"]["authority"] = json!("0x1111111111111111111111111111111111111111");
        }
        let mut equity: Value = serde_json::from_str(EQUITY).unwrap();
        if scenario == "foreign-equity" {
            equity["data"]["subaccount"] = json!("0x1111111111111111111111111111111111111111");
        }
        let mut margin: Value = serde_json::from_str(MARGIN).unwrap();
        if scenario == "negative-margin" {
            margin["data"]["marginRequired"] = json!(-1);
        }
        let router = Router::new()
            .route(
                super::super::WALLET_SUBACCOUNTS_PATH,
                get(move || {
                    let directory = directory.clone();
                    let reads = Arc::clone(&reads);
                    async move {
                        let mut response = directory;
                        if changed && reads.fetch_add(1, Ordering::SeqCst) > 0 {
                            response["data"] = json!([]);
                        }
                        Json(response)
                    }
                }),
            )
            .route(
                "/internal/v1/account/user-stats",
                get(move || {
                    let stats = stats.clone();
                    async move { Json(stats) }
                }),
            )
            .route(
                "/internal/v1/account/subaccount-info",
                get(move || {
                    let profile = profile.clone();
                    async move { Json(profile) }
                }),
            )
            .route("/internal/v1/account/balances", get(|| async { BALANCES }))
            .route(
                "/internal/v1/account/equity",
                get(move || {
                    let equity = equity.clone();
                    async move { Json(equity) }
                }),
            )
            .route(
                "/internal/v1/account/margin-ratio",
                get(move || {
                    let margin = margin.clone();
                    async move { Json(margin) }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRestTransport::new(Some(2), None).unwrap();

        let result = client.get_wallet_account_snapshot(&base_url, WALLET).await;
        match scenario {
            "valid" => {
                let snapshot = result.unwrap();
                assert_eq!(snapshot.directory.addresses, [SUBACCOUNT]);
                assert_eq!(snapshot.stats.subaccounts, [SUBACCOUNT]);
                assert_eq!(snapshot.subaccounts.len(), 1);
                assert_eq!(snapshot.subaccounts[0].profile.address, SUBACCOUNT);
                assert_eq!(snapshot.subaccounts[0].balances.address, SUBACCOUNT);
                assert_eq!(snapshot.subaccounts[0].equity.subaccount, SUBACCOUNT);
                assert_eq!(
                    snapshot.subaccounts[0].margin_ratio.collateral,
                    Decimal::new(97_096, 2)
                );
            }
            "foreign-authority" => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "subaccount info",
                    ..
                })
            )),
            "foreign-equity" => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "subaccount equity",
                    ..
                })
            )),
            "negative-margin" => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "subaccount margin ratio",
                    ..
                })
            )),
            _ => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "wallet account snapshot",
                    ..
                })
            )),
        }
        server.abort();
    }
}
