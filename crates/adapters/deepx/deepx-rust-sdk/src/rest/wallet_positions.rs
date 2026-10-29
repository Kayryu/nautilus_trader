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

//! Bounded position lifecycle observations for one wallet directory.

use std::collections::HashSet;

use super::{
    DeepXRestError, DeepXRestTransport,
    models::{DeepXSubaccountPerpPositions, DeepXWalletPerpPositions},
    wallet_snapshot::DeepXWalletCollectionValidation,
};
use crate::query::{DeepXPerpPositionsRequest, DeepXWalletPerpPositionsRequest};

/// Tracks position identities across every subaccount in one wallet collection.
#[derive(Debug, Default)]
pub struct DeepXWalletPerpPositionContinuity {
    position_ids: HashSet<u64>,
}

impl DeepXWalletPerpPositionContinuity {
    /// Checks that positions do not repeat across the wallet collection.
    ///
    /// # Errors
    ///
    /// Returns an invalid-account-response error for a repeated position ID.
    pub fn observe_ids(
        &mut self,
        ids: impl IntoIterator<Item = u64>,
    ) -> Result<(), DeepXRestError> {
        for id in ids {
            if !self.position_ids.insert(id) {
                return Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "wallet perp positions",
                    message: format!("position ID {id} repeats across subaccounts"),
                });
            }
        }
        Ok(())
    }
}

impl DeepXRestTransport {
    /// Returns complete position lifecycle pages for every validated wallet subaccount.
    ///
    /// Directory membership is checked against statistics and a final directory read. Profiles
    /// must identify the requested wallet, and position IDs must be unique across subaccounts.
    /// These mutable REST reads do not establish a block-pinned snapshot or finalized state.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, response, pagination, or inconsistent account evidence
    /// error. No partial collection is returned.
    pub async fn get_wallet_perp_positions(
        &self,
        base_url: &str,
        request: &DeepXWalletPerpPositionsRequest,
    ) -> Result<DeepXWalletPerpPositions, DeepXRestError> {
        request.validate()?;
        let wallet = &request.wallet;
        let directory = self.get_wallet_subaccounts(base_url, wallet).await?;
        let stats = self.get_user_stats(base_url, wallet).await?;
        let validation = DeepXWalletCollectionValidation::for_perp_positions(&directory);
        validation.validate_stats(&stats)?;
        if directory.addresses.len() > request.max_subaccounts {
            return Err(DeepXRestError::InvalidRequest(format!(
                "wallet directory exceeds max_subaccounts limit of {}",
                request.max_subaccounts
            )));
        }

        let mut subaccounts = Vec::with_capacity(directory.addresses.len());
        let mut position_ids = DeepXWalletPerpPositionContinuity::default();
        for address in &directory.addresses {
            let profile = self.get_subaccount_info(base_url, address).await?;
            validation.validate_profile(&profile)?;
            let page_request = DeepXPerpPositionsRequest {
                subaccount: address.clone(),
                market_id: request.market_id,
                only_closed: request.only_closed,
                cursor: None,
                page_size: request.page_size,
            };
            let pages = self
                .get_perp_position_pages(base_url, &page_request, request.max_pages_per_subaccount)
                .await?;
            position_ids.observe_ids(
                pages
                    .iter()
                    .flat_map(|page| page.items.iter().map(|position| position.id)),
            )?;
            subaccounts.push(DeepXSubaccountPerpPositions { profile, pages });
        }

        let final_directory = self.get_wallet_subaccounts(base_url, wallet).await?;
        validation.validate_final_directory(&final_directory)?;
        Ok(DeepXWalletPerpPositions {
            directory,
            stats,
            subaccounts,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, extract::Query, routing::get};
    use rstest::rstest;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;

    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const FIRST: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const SECOND: &str = "0x1111111111111111111111111111111111111111";
    const STATS: &str = include_str!("../../test_data/http/testnet/wallet_user_stats.json");
    const PROFILE: &str = include_str!("../../test_data/http/testnet/subaccount_info.json");
    const POSITIONS: &str =
        include_str!("../../test_data/http/testnet/perp_positions_account_market_3.json");

    fn request() -> DeepXWalletPerpPositionsRequest {
        DeepXWalletPerpPositionsRequest {
            wallet: WALLET.to_string(),
            max_subaccounts: 2,
            max_pages_per_subaccount: 2,
            market_id: Some(3),
            only_closed: None,
            page_size: Some(5),
        }
    }

    #[rstest]
    fn wallet_position_continuity_rejects_repeated_ids_across_batches() {
        let mut continuity = DeepXWalletPerpPositionContinuity::default();
        continuity.observe_ids([1, 2]).unwrap();
        assert!(matches!(
            continuity.observe_ids([3, 2]),
            Err(DeepXRestError::InvalidAccountResponse {
                endpoint: "wallet perp positions",
                message,
            }) if message == "position ID 2 repeats across subaccounts"
        ));
    }

    #[rstest]
    #[case("valid")]
    #[case("foreign-stats")]
    #[case("foreign-authority")]
    #[case("changed-directory")]
    #[case("subaccount-limit")]
    #[case("duplicate-position")]
    #[tokio::test]
    async fn wallet_positions_validate_directory_and_position_identity(
        #[case] scenario: &'static str,
    ) {
        let two_accounts = matches!(scenario, "subaccount-limit" | "duplicate-position");
        let addresses = if two_accounts {
            vec![FIRST, SECOND]
        } else {
            vec![FIRST]
        };
        let directory = json!({"code": 200, "msg": "success", "data": addresses, "fail": false});
        let reads = Arc::new(AtomicUsize::new(0));
        let changed = scenario == "changed-directory";
        let mut stats: Value = serde_json::from_str(STATS).unwrap();
        stats["data"]["subaccounts"] = json!(if scenario == "foreign-stats" {
            vec![SECOND]
        } else {
            addresses
        });
        stats["data"]["numberOfSubAccounts"] = json!(if two_accounts { 2 } else { 1 });
        let profile: Value = serde_json::from_str(PROFILE).unwrap();
        let positions: Value = serde_json::from_str(POSITIONS).unwrap();
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
                get(move |Query(query): Query<HashMap<String, String>>| {
                    let mut profile = profile.clone();
                    async move {
                        profile["data"]["address"] = json!(query["address"]);
                        if scenario == "foreign-authority" {
                            profile["data"]["authority"] = json!(SECOND);
                        }
                        Json(profile)
                    }
                }),
            )
            .route(
                super::super::PERP_POSITIONS_PATH,
                get(move |Query(query): Query<HashMap<String, String>>| {
                    let mut positions = positions.clone();
                    async move {
                        assert_eq!(query["marketId"], "3");
                        assert_eq!(query["pageSize"], "5");
                        if query["user"] == SECOND {
                            positions["data"]["items"]
                                .as_array_mut()
                                .unwrap()
                                .truncate(1);
                            positions["data"]["items"][0]["owner"] = json!(SECOND);
                        }
                        Json(positions)
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRestTransport::new(Some(2), None).unwrap();
        let mut request = request();
        if scenario == "subaccount-limit" {
            request.max_subaccounts = 1;
        }
        let result = client.get_wallet_perp_positions(&base_url, &request).await;
        match scenario {
            "valid" => {
                let result = result.unwrap();
                assert_eq!(result.directory.addresses, [FIRST]);
                assert_eq!(result.subaccounts.len(), 1);
                assert_eq!(result.subaccounts[0].profile.address, FIRST);
                assert_eq!(result.subaccounts[0].pages[0].items.len(), 2);
                assert_eq!(result.subaccounts[0].pages[0].items[0].id, 1_659_167);
            }
            "foreign-authority" => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "subaccount info",
                    ..
                })
            )),
            "subaccount-limit" => assert!(matches!(result, Err(DeepXRestError::InvalidRequest(_)))),
            _ => assert!(matches!(
                result,
                Err(DeepXRestError::InvalidAccountResponse {
                    endpoint: "wallet perp positions",
                    ..
                })
            )),
        }
        server.abort();
    }

    #[rstest]
    #[case("subaccounts")]
    #[case("pages")]
    #[case("wallet")]
    #[tokio::test]
    async fn invalid_wallet_position_request_fails_before_transport(#[case] invalid: &str) {
        let client = DeepXRestTransport::new(Some(1), None).unwrap();
        let mut request = request();
        match invalid {
            "subaccounts" => request.max_subaccounts = 0,
            "pages" => request.max_pages_per_subaccount = 0,
            "wallet" => request.wallet = "not-an-address".to_string(),
            _ => unreachable!(),
        }
        let result = client
            .get_wallet_perp_positions("http://127.0.0.1:1", &request)
            .await;
        assert!(matches!(
            result,
            Err(DeepXRestError::InvalidRequest(_) | DeepXRestError::InvalidPaginationLimit)
        ));
    }
}
