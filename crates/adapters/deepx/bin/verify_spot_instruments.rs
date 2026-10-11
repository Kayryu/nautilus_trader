// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Verify finalized Spot and perpetual instrument discovery against the DeepX testnet.
//! No account access, signing, WebSocket subscriptions, or transaction submission.

use std::{cell::RefCell, rc::Rc};

use anyhow::ensure;
use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    live::{clock::LiveClock, runner::set_data_event_sender},
    messages::DataEvent,
};
use nautilus_deepx::{config::DeepXDataClientConfig, data::DeepXDataClient};
use nautilus_model::{
    identifiers::ClientId,
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::mpsc::unbounded_channel;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let (sender, mut receiver) = unbounded_channel();
    set_data_event_sender(sender);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock = Rc::new(RefCell::new(LiveClock::default()));
    let mut client = DeepXDataClient::new(
        ClientId::from("DEEPX-SPOT-INSTRUMENT-CHECK"),
        DeepXDataClientConfig::default(),
        cache.into(),
        clock,
    )?;

    client.connect().await?;
    let mut spot_count = 0;
    let mut perpetual_count = 0;
    while let Ok(event) = receiver.try_recv() {
        let DataEvent::Instrument(instrument) = event else {
            anyhow::bail!("unexpected startup event");
        };
        match instrument {
            InstrumentAny::CurrencyPair(pair) => {
                spot_count += 1;
                println!(
                    "spot={} price_increment={} size_increment={} min_quantity={}",
                    pair.id,
                    pair.price_increment.as_decimal(),
                    pair.size_increment.as_decimal(),
                    pair.min_quantity.map_or_else(
                        || "none".to_string(),
                        |quantity| quantity.as_decimal().to_string()
                    ),
                );
            }
            instrument => {
                perpetual_count += 1;
                println!("perpetual={}", instrument.id());
            }
        }
    }

    ensure!(
        spot_count > 0,
        "no finalized Spot instruments were published"
    );
    ensure!(
        perpetual_count > 0,
        "no perpetual instruments were published"
    );
    client.disconnect().await?;
    ensure!(client.is_disconnected(), "client did not disconnect");
    println!(
        "DeepX finalized instrument discovery verified spot={spot_count} perpetual={perpetual_count}; no credentials or transactions sent",
    );
    Ok(())
}
