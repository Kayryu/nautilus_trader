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

//! Spot market specification precompile ABI.

use alloy::{primitives::FixedBytes, sol, sol_types::SolCall};

sol! {
    struct SpotMarketSpec {
        uint128 minOrderSize;
        uint128 tickSize;
        uint128 stepSize;
    }

    function getSpotMarketSpec(bytes32 pair) external view returns (SpotMarketSpec);
}

/// Raw integer Spot order constraints returned by the DeepX precompile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXSpotMarketSpec {
    /// Raw minimum order size.
    pub min_order_size: u128,
    /// Raw price tick size.
    pub tick_size: u128,
    /// Raw quantity step size.
    pub step_size: u128,
}

/// Encodes a Spot market specification query for one deployment pair.
#[must_use]
pub fn encode_spot_market_spec_call(pair: [u8; 32]) -> Vec<u8> {
    getSpotMarketSpecCall {
        pair: FixedBytes::from(pair),
    }
    .abi_encode()
}

/// Decodes the raw Spot market specification returned by the precompile.
///
/// # Errors
///
/// Returns an error if the response does not match the verified ABI tuple.
pub fn decode_spot_market_spec_return(response: &[u8]) -> anyhow::Result<DeepXSpotMarketSpec> {
    let decoded = getSpotMarketSpecCall::abi_decode_returns(response)?;
    Ok(DeepXSpotMarketSpec {
        min_order_size: decoded.minOrderSize,
        tick_size: decoded.tickSize,
        step_size: decoded.stepSize,
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn spot_market_spec_abi_preserves_integer_fields() {
        let pair = [0x42; 32];
        let call = encode_spot_market_spec_call(pair);
        let selector = alloy::primitives::keccak256("getSpotMarketSpec(bytes32)");
        assert_eq!(&call[..4], &selector[..4]);
        assert_eq!(&call[4..], &pair);

        let mut response = [0_u8; 96];
        response[31] = 11;
        response[63] = 22;
        response[95] = 33;
        assert_eq!(
            decode_spot_market_spec_return(&response).unwrap(),
            DeepXSpotMarketSpec {
                min_order_size: 11,
                tick_size: 22,
                step_size: 33,
            }
        );
        assert!(decode_spot_market_spec_return(&response[..95]).is_err());
    }
}
