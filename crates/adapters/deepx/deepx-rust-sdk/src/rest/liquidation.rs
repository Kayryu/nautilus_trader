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

//! Raw liquidation and ADL REST queries.

use serde::Serialize;
use serde_json::value::RawValue;

use super::{DeepXRawAccountPage, DeepXRestError, DeepXRestTransport, is_account_address};
use crate::{
    pagination::validate_cursor_page,
    query::{
        DeepXLiquidationAdlRecordsRequest, DeepXLiquidationExecutionRecordsRequest,
        DeepXLiquidationRecordsRequest, DeepXPerpLiquidationPriceRequest,
        DeepXSpotAdlCandidatesRequest,
    },
};

const SPOT_ADL_CANDIDATES_PATH: &str = "/internal/v1/adl/spot/candidates";
const LIQUIDATION_ADL_RECORDS_PATH: &str = "/internal/v1/liquidation/adl-records";
const LIQUIDATION_KEEPER_STATE_PATH: &str = "/internal/v1/liquidation/keeper-state";
const LIQUIDATION_RECORDS_PATH: &str = "/internal/v1/account/subaccounts/liquidation-records";
const PERP_LIQUIDATION_PRICE_PATH: &str = "/internal/v1/account/perp/liquidate-price";

/// DeepX liquidation execution-record endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXLiquidationExecutionRoute {
    Ordinary,
    Cranker,
    TpSl,
}

impl DeepXLiquidationExecutionRoute {
    const fn path(self) -> &'static str {
        match self {
            Self::Ordinary => "/internal/v1/liquidation/records",
            Self::Cranker => "/internal/v1/liquidation/cranker-records",
            Self::TpSl => "/internal/v1/liquidation/tp-sl-records",
        }
    }

    /// Returns the endpoint name used in validation errors and pagination state.
    #[must_use]
    pub const fn endpoint(self) -> &'static str {
        match self {
            Self::Ordinary => "liquidation execution records",
            Self::Cranker => "liquidation cranker records",
            Self::TpSl => "liquidation TP/SL records",
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct KeeperStateQuery<'a> {
    subaccount: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<&'a str>,
}

impl DeepXRestTransport {
    /// Returns the uninterpreted perpetual liquidation-price response.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_liquidation_price_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpLiquidationPriceRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_LIQUIDATION_PRICE_PATH, &request.as_query())
            .await
    }

    /// Returns the uninterpreted Spot ADL candidate response.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_adl_candidates_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotAdlCandidatesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_ADL_CANDIDATES_PATH, &request.as_query())
            .await
    }

    /// Returns one uninterpreted page of ADL records.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_liquidation_adl_records_raw(
        &self,
        base_url: &str,
        request: &DeepXLiquidationAdlRecordsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            LIQUIDATION_ADL_RECORDS_PATH,
            &request.as_query(),
            "liquidation ADL records",
        )
        .await
    }

    /// Returns one uninterpreted execution-record page from the selected route.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, page-size, or pagination error.
    pub async fn get_liquidation_execution_records_raw(
        &self,
        base_url: &str,
        route: DeepXLiquidationExecutionRoute,
        request: &DeepXLiquidationExecutionRecordsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        let page: DeepXRawAccountPage = self
            .get_api_data_with_query(base_url, route.path(), &request.as_query())
            .await?;
        validate_execution_page(&page, route, request.page_size.unwrap_or(20))?;
        Ok(page)
    }

    /// Returns the uninterpreted liquidation keeper state for one subaccount.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_liquidation_keeper_state_raw(
        &self,
        base_url: &str,
        subaccount: &str,
        chain_id: Option<&str>,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        if !is_account_address(subaccount) {
            return Err(DeepXRestError::InvalidRequest(
                "liquidation keeper state requires a 20-byte hex subaccount address".to_string(),
            ));
        }
        if chain_id.is_some_and(|value| value.trim().is_empty()) {
            return Err(DeepXRestError::InvalidRequest(
                "liquidation keeper state chain_id must not be empty".to_string(),
            ));
        }
        self.get_api_data_with_query(
            base_url,
            LIQUIDATION_KEEPER_STATE_PATH,
            &KeeperStateQuery {
                subaccount,
                chain_id,
            },
        )
        .await
    }

    /// Returns one uninterpreted page of account liquidation records.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_liquidation_records_raw(
        &self,
        base_url: &str,
        request: &DeepXLiquidationRecordsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            LIQUIDATION_RECORDS_PATH,
            &request.as_query(),
            "liquidation records",
        )
        .await
    }
}

fn validate_execution_page(
    page: &DeepXRawAccountPage,
    route: DeepXLiquidationExecutionRoute,
    page_size: u32,
) -> Result<(), DeepXRestError> {
    if page.items.len() > page_size as usize {
        return Err(DeepXRestError::InvalidHistoryResponse {
            endpoint: route.endpoint(),
            message: "page exceeds requested page size".to_string(),
        });
    }
    validate_cursor_page(route.endpoint(), page.has_next, page.next_cursor.as_deref())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn execution_routes_map_to_distinct_paths_and_names() {
        let routes = [
            DeepXLiquidationExecutionRoute::Ordinary,
            DeepXLiquidationExecutionRoute::Cranker,
            DeepXLiquidationExecutionRoute::TpSl,
        ];
        let paths = routes.map(DeepXLiquidationExecutionRoute::path);
        assert_eq!(paths.len(), std::collections::HashSet::from(paths).len());
        assert_eq!(routes[0].endpoint(), "liquidation execution records");
        assert_eq!(routes[1].endpoint(), "liquidation cranker records");
        assert_eq!(routes[2].endpoint(), "liquidation TP/SL records");
    }

    #[rstest]
    fn execution_page_checks_size_before_cursor() {
        let page: DeepXRawAccountPage = serde_json::from_value(json!({
            "items": [{"id": 1}, {"id": 2}], "hasNext": true, "nextCursor": null,
        }))
        .unwrap();
        assert!(matches!(
            validate_execution_page(&page, DeepXLiquidationExecutionRoute::Ordinary, 1),
            Err(DeepXRestError::InvalidHistoryResponse { .. }),
        ));
    }
}
