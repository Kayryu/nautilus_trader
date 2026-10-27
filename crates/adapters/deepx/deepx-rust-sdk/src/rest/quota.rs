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

//! Raw wallet quota REST queries.

use serde::Serialize;
use serde_json::value::RawValue;

use super::{DeepXRawAccountPage, DeepXRestError, DeepXRestTransport, is_account_address};
use crate::query::DeepXQuotaHistoryRequest;

const QUOTA_SUMMARY_PATH: &str = "/internal/v1/account/quota/summary";
const QUOTA_CLAIM_PATH: &str = "/internal/v1/account/quota/claim";
const QUOTA_HISTORY_PATH: &str = "/internal/v1/account/quota/history";

#[derive(Serialize)]
struct WalletQuery<'a> {
    wallet: &'a str,
}

#[derive(Serialize)]
struct ClaimQuery {
    id: u64,
}

impl DeepXRestTransport {
    /// Returns the uninterpreted quota summary for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_quota_summary_raw(
        &self,
        base_url: &str,
        wallet: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if !is_account_address(wallet) {
            return Err(DeepXRestError::InvalidRequest(
                "quota summary requires a 20-byte hex wallet address".to_string(),
            ));
        }
        self.get_api_data_with_query(base_url, QUOTA_SUMMARY_PATH, &WalletQuery { wallet })
            .await
    }

    /// Returns the uninterpreted quota claim task state.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_quota_claim_raw(
        &self,
        base_url: &str,
        claim_id: u64,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if claim_id == 0 {
            return Err(DeepXRestError::InvalidRequest(
                "quota-claim id must be greater than zero".to_string(),
            ));
        }
        self.get_api_data_with_query(base_url, QUOTA_CLAIM_PATH, &ClaimQuery { id: claim_id })
            .await
    }

    /// Returns one uninterpreted quota history page.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_quota_history_raw(
        &self,
        base_url: &str,
        request: &DeepXQuotaHistoryRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            QUOTA_HISTORY_PATH,
            &request.as_query(),
            "quota history",
        )
        .await
    }
}
