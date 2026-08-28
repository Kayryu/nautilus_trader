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

//! Instrument provider for the DeepX adapter.

use std::{collections::HashMap, fmt::Debug};

use async_trait::async_trait;
use nautilus_common::providers::{InstrumentProvider, InstrumentStore};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{identifiers::InstrumentId, instruments::Instrument};

use crate::{
    common::{enums::DeepXProductType, symbol::raw_symbol_from_instrument_id},
    http::{client::DeepXRawHttpClient, parse::parse_perpetual_instrument},
};

/// Provides DeepX perpetual instruments via the REST API.
///
/// Spot loading remains unavailable until a canonical DeepX spot market schema is verified.
pub struct DeepXInstrumentProvider {
    store: InstrumentStore,
    http_client: DeepXRawHttpClient,
}

impl Debug for DeepXInstrumentProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXInstrumentProvider))
            .field("store", &self.store)
            .field("http_client", &self.http_client)
            .finish()
    }
}

impl DeepXInstrumentProvider {
    /// Creates a new provider with an empty instrument store.
    #[must_use]
    pub fn new(http_client: DeepXRawHttpClient) -> Self {
        Self {
            store: InstrumentStore::new(),
            http_client,
        }
    }
}

#[async_trait(?Send)]
impl InstrumentProvider for DeepXInstrumentProvider {
    fn store(&self) -> &InstrumentStore {
        &self.store
    }

    fn store_mut(&mut self) -> &mut InstrumentStore {
        &mut self.store
    }

    async fn load_all(&mut self, _filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        let markets = self.http_client.get_perp_markets().await?;
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        let instruments = markets
            .iter()
            .map(|market| parse_perpetual_instrument(market, ts_init))
            .collect::<anyhow::Result<Vec<_>>>()?;

        self.store.clear();
        self.store.add_bulk(instruments);
        self.store.set_initialized();

        Ok(())
    }

    async fn load_ids(
        &mut self,
        instrument_ids: &[InstrumentId],
        _filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        for instrument_id in instrument_ids {
            if self.store.contains(instrument_id) {
                continue;
            }

            let raw_symbol =
                raw_symbol_from_instrument_id(*instrument_id, DeepXProductType::Perpetual)?;
            let market = self.http_client.get_perp_market(&raw_symbol).await?;
            let instrument =
                parse_perpetual_instrument(&market, get_atomic_clock_realtime().get_time_ns())?;
            anyhow::ensure!(
                instrument.id() == *instrument_id,
                "DeepX returned instrument `{}` for requested `{instrument_id}`",
                instrument.id()
            );
            self.store.add(instrument);
        }

        Ok(())
    }

    async fn load(
        &mut self,
        instrument_id: &InstrumentId,
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        self.load_ids(&[*instrument_id], filters).await
    }
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, routing::get};
    use nautilus_common::providers::InstrumentProvider;

    use super::*;

    fn market_json() -> serde_json::Value {
        serde_json::json!({
            "baseAsset": "ETH",
            "makerFeeRate": "-0.0001",
            "marketId": 3,
            "maxOpenOrders": 128,
            "minNotional": "1",
            "minQty": "0.001",
            "orderTypes": ["LIMIT", "MARKET"],
            "quoteAsset": "USDC",
            "status": "TRADING",
            "stepSize": "0.001",
            "symbol": "ETH-USDC",
            "takerFeeRate": "0.0002",
            "tickSize": "0.01"
        })
    }

    async fn provider(router: Router) -> DeepXInstrumentProvider {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXRawHttpClient::new(Some(format!("http://{address}")), 5, None).unwrap();
        DeepXInstrumentProvider::new(client)
    }

    #[tokio::test]
    async fn loads_all_perpetual_instruments() {
        let router = Router::new().route(
            "/v1/perp/markets",
            get(|| async { Json(vec![market_json()]) }),
        );
        let mut provider = provider(router).await;

        provider.load_all(None).await.unwrap();

        assert!(provider.store().is_initialized());
        assert!(
            provider
                .store()
                .find(&InstrumentId::from("ETH-USDC-PERP.DEEPX"))
                .is_some()
        );
    }

    #[tokio::test]
    async fn loads_perpetual_instrument_by_id() {
        let router = Router::new().route(
            "/v1/perp/markets/ETH-USDC",
            get(|| async { Json(market_json()) }),
        );
        let mut provider = provider(router).await;
        let instrument_id = InstrumentId::from("ETH-USDC-PERP.DEEPX");

        provider.load(&instrument_id, None).await.unwrap();

        assert!(provider.store().find(&instrument_id).is_some());
    }

    #[tokio::test]
    async fn rejects_spot_instrument_until_schema_is_verified() {
        let mut provider = provider(Router::new()).await;
        let instrument_id = InstrumentId::from("ETH-USDC.DEEPX");

        let result = provider.load(&instrument_id, None).await;

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not a DeepX perpetual")
        );
    }
}
