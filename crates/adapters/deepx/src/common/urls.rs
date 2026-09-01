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

//! DeepX deployment URL resolution.

use super::{
    consts::{DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL, DEEPX_TESTNET_WS_URL},
    enums::DeepXEnvironment,
    error::{DeepXError, Result},
};

pub fn rest_url(environment: &DeepXEnvironment) -> Result<&'static str> {
    require_testnet(environment, DEEPX_TESTNET_REST_URL)
}

pub fn ws_url(environment: &DeepXEnvironment) -> Result<&'static str> {
    require_testnet(environment, DEEPX_TESTNET_WS_URL)
}

pub fn rpc_url(environment: &DeepXEnvironment) -> Result<&'static str> {
    require_testnet(environment, DEEPX_TESTNET_RPC_URL)
}

fn require_testnet(
    environment: &DeepXEnvironment,
    testnet_url: &'static str,
) -> Result<&'static str> {
    if environment.is_testnet() {
        Ok(testnet_url)
    } else {
        Err(DeepXError::UnsupportedEnvironment(environment.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn testnet_urls_match_verified_deployment() {
        assert_eq!(
            rest_url(&DeepXEnvironment::Testnet).unwrap(),
            DEEPX_TESTNET_REST_URL
        );
        assert_eq!(
            ws_url(&DeepXEnvironment::Testnet).unwrap(),
            DEEPX_TESTNET_WS_URL
        );
        assert_eq!(
            rpc_url(&DeepXEnvironment::Testnet).unwrap(),
            DEEPX_TESTNET_RPC_URL
        );
    }

    #[rstest]
    #[case(DeepXEnvironment::Mainnet, "mainnet")]
    #[case(DeepXEnvironment::Unknown("devnet".to_string()), "devnet")]
    fn unsupported_environments_have_typed_errors(
        #[case] environment: DeepXEnvironment,
        #[case] expected: &str,
    ) {
        assert_eq!(
            rest_url(&environment),
            Err(DeepXError::UnsupportedEnvironment(expected.to_string())),
        );
    }
}
