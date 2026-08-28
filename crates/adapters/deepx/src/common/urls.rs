// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use super::{
    consts::{
        DEEPX_TESTNET_EVM_RPC_URL, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_SUBSTRATE_WS_URL,
        DEEPX_TESTNET_WS_URL,
    },
    enums::DeepXEnvironment,
};

/// Returns the REST API URL for the environment.
#[must_use]
pub const fn rest_url(environment: DeepXEnvironment) -> &'static str {
    match environment {
        DeepXEnvironment::Testnet => DEEPX_TESTNET_REST_URL,
    }
}

/// Returns the public WebSocket URL for the environment.
#[must_use]
pub const fn ws_url(environment: DeepXEnvironment) -> &'static str {
    match environment {
        DeepXEnvironment::Testnet => DEEPX_TESTNET_WS_URL,
    }
}

/// Returns the Substrate WebSocket URL for the environment.
#[must_use]
pub const fn substrate_ws_url(environment: DeepXEnvironment) -> &'static str {
    match environment {
        DeepXEnvironment::Testnet => DEEPX_TESTNET_SUBSTRATE_WS_URL,
    }
}

/// Returns the EVM RPC URL for the environment.
#[must_use]
pub const fn evm_rpc_url(environment: DeepXEnvironment) -> &'static str {
    match environment {
        DeepXEnvironment::Testnet => DEEPX_TESTNET_EVM_RPC_URL,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn resolves_testnet_urls() {
        assert_eq!(rest_url(DeepXEnvironment::Testnet), DEEPX_TESTNET_REST_URL);
        assert_eq!(ws_url(DeepXEnvironment::Testnet), DEEPX_TESTNET_WS_URL);
        assert_eq!(
            substrate_ws_url(DeepXEnvironment::Testnet),
            DEEPX_TESTNET_SUBSTRATE_WS_URL,
        );
        assert_eq!(
            evm_rpc_url(DeepXEnvironment::Testnet),
            DEEPX_TESTNET_EVM_RPC_URL,
        );
    }
}
