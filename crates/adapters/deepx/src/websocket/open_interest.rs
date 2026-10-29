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

//! Bounded public perpetual open-interest observations without unit interpretation.

use std::{num::NonZeroUsize, time::Duration};

use anyhow::{Result, ensure};
use deepx_rust_sdk::websocket::open_interest::parse_open_interest;
pub use deepx_rust_sdk::websocket::open_interest::{
    DeepXWsOpenInterestObservation, match_open_interest_to_rest,
};

use super::public::{DeepXWsPublicChannel, DeepXWsPublicConnection};

const PRE_ACK_BUFFER_CAPACITY: NonZeroUsize = NonZeroUsize::new(8).expect("nonzero capacity");

/// Reads the first acknowledged open-interest update for one validated perpetual market.
///
/// This is a mutable public observation, not a complete history or a framework event.
///
/// # Errors
///
/// Returns an error for invalid identity or deadlines, transport or acknowledgement failure,
/// foreign frames, invalid values, or a missing first update.
pub async fn observe_first_open_interest(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    operation_timeout: Duration,
    observation_timeout: Duration,
    market_id: u16,
    pair: &str,
) -> Result<DeepXWsOpenInterestObservation> {
    ensure!(
        market_id != 0 && !pair.is_empty(),
        "invalid DeepX open-interest market"
    );
    ensure!(
        !operation_timeout.is_zero() && !observation_timeout.is_zero(),
        "DeepX open-interest timeouts must be positive"
    );
    let mut connection = DeepXWsPublicConnection::connect(
        network,
        proxy_url,
        operation_timeout,
        PRE_ACK_BUFFER_CAPACITY,
    )
    .await?;
    if let Err(e) = connection
        .subscribe(market_id, vec![DeepXWsPublicChannel::OpenInterest])
        .await
    {
        let _ = connection.close().await;
        return Err(e);
    }
    let result = tokio::time::timeout(observation_timeout, async {
        loop {
            let frame = match connection.next_frame().await {
                Ok(frame) => frame,
                Err(e)
                    if matches!(
                        e.downcast_ref::<super::DeepXWsError>(),
                        Some(super::DeepXWsError::ReceiveTimeout)
                    ) =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            };
            if let Some(observation) = parse_open_interest(frame, market_id, pair)? {
                break Ok::<_, anyhow::Error>(observation);
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("DeepX open-interest deadline exceeded")));
    let close = connection.close().await;
    match result {
        Ok(observation) => {
            close?;
            Ok(observation)
        }
        Err(e) => Err(e),
    }
}
