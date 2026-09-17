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

//! Live read-only capture of documented DeepX perpetual price and funding channels.

use std::{collections::BTreeSet, num::NonZeroUsize, time::Duration};

use anyhow::Context;
use nautilus_deepx::{
    config::DeepXNetworkConfig,
    websocket::public::{DeepXWsPublicChannel, DeepXWsPublicConnection, DeepXWsPublicFrame},
};

const ETH_MARKET_ID: u16 = 3;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let channels = [
        DeepXWsPublicChannel::LatestPrice,
        DeepXWsPublicChannel::OraclePrice,
        DeepXWsPublicChannel::MarkPrice,
        DeepXWsPublicChannel::FundingRate,
    ];
    let mut connection = DeepXWsPublicConnection::connect(
        &DeepXNetworkConfig::default(),
        None,
        Duration::from_secs(10),
        NonZeroUsize::new(32).expect("nonzero capacity"),
    )
    .await?;
    connection
        .subscribe(ETH_MARKET_ID, channels.to_vec())
        .await
        .context("public price-channel acknowledgement failed")?;
    println!("DeepX ETH public price/funding acknowledgement verified");

    let mut observed = BTreeSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while observed.len() < channels.len() {
            let frame = connection.next_frame().await?;
            if let DeepXWsPublicFrame::Data {
                market,
                channel,
                data,
                timestamp,
            } = frame
                && observed.insert(channel)
            {
                println!(
                    "market={} channel={channel:?} timestamp={timestamp} data={}",
                    market.id,
                    data.get(),
                );
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("public price-channel data deadline exceeded")??;

    connection.close().await?;
    println!(
        "DeepX public price/funding connection closed; no account or transaction command sent"
    );
    Ok(())
}
