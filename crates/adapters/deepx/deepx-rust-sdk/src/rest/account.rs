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

//! Raw account state and directory REST queries.

use serde::Serialize;
use serde_json::value::RawValue;

use super::{DeepXRawAccountPage, DeepXRestError, DeepXRestTransport, is_account_address};
use crate::query::{
    DeepXAllSubaccountsRequest, DeepXHourlyUnsettledFundingRequest, DeepXPerpWalletOrdersRequest,
    DeepXPerpWalletTradesRequest, DeepXSpotWalletOrdersRequest, DeepXSpotWalletTradesRequest,
};

const ALL_SUBACCOUNTS_PATH: &str = "/internal/v1/account/subaccounts/all";
const SUBACCOUNT_INFO_PATH: &str = "/internal/v1/account/subaccount-info";
const SUBACCOUNT_BALANCES_PATH: &str = "/internal/v1/account/balances";
const SUBACCOUNT_EQUITY_PATH: &str = "/internal/v1/account/equity";
const SUBACCOUNT_MARGIN_RATIO_PATH: &str = "/internal/v1/account/margin-ratio";
const DELEGATE_ACCOUNTS_PATH: &str = "/internal/v1/account/delegate-accounts";
const DELEGATOR_ACCOUNTS_PATH: &str = "/internal/v1/account/delegator-accounts";
const SPOT_WALLET_ORDERS_PATH: &str = "/internal/v1/account/spot/orders-by-wallet";
const SPOT_WALLET_TRADES_PATH: &str = "/internal/v1/account/spot/trades-by-wallet";
const PERP_WALLET_ORDERS_PATH: &str = "/internal/v1/account/perp/orders-by-wallet";
const PERP_WALLET_TRADES_PATH: &str = "/internal/v1/account/perp/trades-by-wallet";
const HOURLY_UNSETTLED_FUNDING_PATH: &str = "/internal/v1/account/hourly-unsettled-funding";
const INVITATION_CHECK_PATH: &str = "/internal/v1/invitation/check";
const INVITATION_CODE_PATH: &str = "/internal/v1/invitation/invitation-code";
const USER_STATS_PATH: &str = "/internal/v1/account/user-stats";

#[derive(Serialize)]
struct AddressQuery<'a> {
    address: &'a str,
}

#[derive(Serialize)]
struct SubaccountQuery<'a> {
    subaccount: &'a str,
}

impl DeepXRestTransport {
    /// Returns uninterpreted wallet statistics.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_user_stats_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("user stats", address)?;
        self.get_api_data_with_query(base_url, USER_STATS_PATH, &AddressQuery { address })
            .await
    }

    /// Returns the uninterpreted invitation status for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_invitation_status_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("invitation status", address)?;
        self.get_api_data_with_query(base_url, INVITATION_CHECK_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted invitation codes for a wallet.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_invitation_codes_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("invitation codes", address)?;
        self.get_api_data_with_query(base_url, INVITATION_CODE_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted grouped spot wallet orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_wallet_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletOrdersRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_WALLET_ORDERS_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted grouped spot wallet trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_spot_wallet_trades_raw(
        &self,
        base_url: &str,
        request: &DeepXSpotWalletTradesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, SPOT_WALLET_TRADES_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted grouped perpetual wallet orders.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_wallet_orders_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletOrdersRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_WALLET_ORDERS_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted grouped perpetual wallet trades.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_perp_wallet_trades_raw(
        &self,
        base_url: &str,
        request: &DeepXPerpWalletTradesRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, PERP_WALLET_TRADES_PATH, &request.as_query())
            .await
    }

    /// Returns uninterpreted hourly unsettled funding records.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, or decoding error.
    pub async fn get_hourly_unsettled_funding_raw(
        &self,
        base_url: &str,
        request: &DeepXHourlyUnsettledFundingRequest,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        request.validate()?;
        self.get_api_data_with_query(base_url, HOURLY_UNSETTLED_FUNDING_PATH, &request.as_query())
            .await
    }

    /// Returns one raw page of the global subaccount directory.
    ///
    /// # Errors
    ///
    /// Returns a request, transport, API, decoding, or pagination error.
    pub async fn get_all_subaccounts_raw(
        &self,
        base_url: &str,
        request: &DeepXAllSubaccountsRequest,
    ) -> Result<DeepXRawAccountPage, DeepXRestError> {
        request.validate()?;
        self.get_account_page(
            base_url,
            ALL_SUBACCOUNTS_PATH,
            &request.as_query(),
            "all subaccounts",
        )
        .await
    }

    /// Returns an uninterpreted subaccount profile.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_info_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount info", address)?;
        self.get_api_data_with_query(base_url, SUBACCOUNT_INFO_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted subaccount balances.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_balances_raw(
        &self,
        base_url: &str,
        subaccount: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount balances", subaccount)?;
        self.get_api_data_with_query(
            base_url,
            SUBACCOUNT_BALANCES_PATH,
            &SubaccountQuery { subaccount },
        )
        .await
    }

    /// Returns uninterpreted subaccount equity.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_equity_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount equity", address)?;
        self.get_api_data_with_query(base_url, SUBACCOUNT_EQUITY_PATH, &AddressQuery { address })
            .await
    }

    /// Returns an uninterpreted subaccount margin ratio.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_subaccount_margin_ratio_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("subaccount margin ratio", address)?;
        self.get_api_data_with_query(
            base_url,
            SUBACCOUNT_MARGIN_RATIO_PATH,
            &AddressQuery { address },
        )
        .await
    }

    /// Returns uninterpreted wallet delegate accounts.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_delegate_accounts_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("delegate accounts", address)?;
        self.get_api_data_with_query(base_url, DELEGATE_ACCOUNTS_PATH, &AddressQuery { address })
            .await
    }

    /// Returns uninterpreted wallets bound to a delegate account.
    ///
    /// # Errors
    ///
    /// Returns a transport, API, or decoding error.
    pub async fn get_delegator_accounts_raw(
        &self,
        base_url: &str,
        address: &str,
    ) -> Result<Box<RawValue>, DeepXRestError> {
        validate_address("delegator accounts", address)?;
        self.get_api_data_with_query(base_url, DELEGATOR_ACCOUNTS_PATH, &AddressQuery { address })
            .await
    }
}

fn validate_address(endpoint: &str, address: &str) -> Result<(), DeepXRestError> {
    if !is_account_address(address) {
        return Err(DeepXRestError::InvalidRequest(format!(
            "{endpoint} requires a 20-byte hex address"
        )));
    }
    Ok(())
}
