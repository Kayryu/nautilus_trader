// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Nautilus `InstrumentProvider` boundary over the verified DeepX market catalog.

use std::collections::HashMap;

use anyhow::{Result, bail};
use async_trait::async_trait;
use nautilus_common::providers::{InstrumentProvider, InstrumentStore};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{identifiers::InstrumentId, instruments::InstrumentAny};

use crate::{
    config::DeepXNetworkConfig,
    instruments::{parse_current_spot_instrument, parse_perpetual_instrument},
    providers::{DeepXMarketMetadata, DeepXMarketProvider},
    rpc::{
        observe_and_validate_rpc_endpoint_identities, observe_and_validate_rpc_method_capabilities,
        observe_current_spot_market_spec_batch,
    },
};

/// Fail-closed error raised when Spot instrument construction is attempted.
///
/// The public Spot market-list response does not expose a verified order quantity increment or
/// order limits, so Spot instruments cannot be constructed from it. This remains a hard capability
/// gate until chain-verified `SpotMarketSpec` scaling evidence exists.
#[derive(Debug, thiserror::Error)]
#[error(
    "DeepX Spot instrument construction is unsupported: the market-list response does not prove a \
     verified order quantity increment or order limits"
)]
pub struct DeepXSpotInstrumentUnsupported;

/// Provides DeepX instruments from the failure-atomic public market catalog.
///
/// With network configuration, it converts perpetual metadata and current-block-verified Spot
/// metadata into standard instruments. Without network configuration it loads perpetuals
/// only.
///
/// Loading is failure-atomic: the store is replaced only after the complete catalog loads and
/// every supported market converts successfully. One unconvertible market fails the whole load so
/// a partial instrument set can never be marked initialized.
pub struct DeepXInstrumentProvider {
    store: InstrumentStore,
    catalog: DeepXMarketProvider,
    network: Option<DeepXNetworkConfig>,
    perpetual_only: bool,
    ts_init: nautilus_core::UnixNanos,
}

impl std::fmt::Debug for DeepXInstrumentProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepXInstrumentProvider))
            .field("store", &self.store)
            .field("catalog", &self.catalog)
            .field("network", &self.network)
            .field("perpetual_only", &self.perpetual_only)
            .finish_non_exhaustive()
    }
}

impl DeepXInstrumentProvider {
    /// Creates a new provider with an empty store over the given market catalog.
    #[must_use]
    pub fn new(catalog: DeepXMarketProvider) -> Self {
        Self {
            store: InstrumentStore::new(),
            catalog,
            network: None,
            perpetual_only: false,
            ts_init: get_atomic_clock_realtime().get_time_ns(),
        }
    }

    /// Creates a provider which verifies Spot constraints against current chain state.
    #[must_use]
    pub fn with_network(catalog: DeepXMarketProvider, network: DeepXNetworkConfig) -> Self {
        let mut provider = Self::new(catalog);
        provider.network = Some(network);
        provider
    }

    /// Creates a provider which loads only public perpetual instruments.
    #[must_use]
    pub(crate) fn perpetual_only(catalog: DeepXMarketProvider) -> Self {
        let mut provider = Self::new(catalog);
        provider.perpetual_only = true;
        provider
    }

    /// Returns a reference to the wrapped market catalog.
    #[must_use]
    pub const fn catalog(&self) -> &DeepXMarketProvider {
        &self.catalog
    }

    /// Returns a mutable reference to the wrapped market catalog.
    #[must_use]
    pub fn catalog_mut(&mut self) -> &mut DeepXMarketProvider {
        &mut self.catalog
    }

    /// Returns the fixed conversion timestamp applied to every converted instrument.
    #[must_use]
    pub const fn ts_init(&self) -> nautilus_core::UnixNanos {
        self.ts_init
    }

    /// Converts one catalog entry, failing closed for Spot metadata.
    fn convert(&self, market: &DeepXMarketMetadata) -> Result<InstrumentAny> {
        let DeepXMarketMetadata::Perpetual(perpetual) = market else {
            return Err(DeepXSpotInstrumentUnsupported.into());
        };
        parse_perpetual_instrument(perpetual, self.ts_init)
    }

    /// Loads and converts a complete catalog without publishing partial catalog or store state.
    pub(crate) async fn fetch_instruments(
        &self,
    ) -> Result<(DeepXMarketProvider, Vec<InstrumentAny>)> {
        let mut catalog = self.catalog.clone();
        if self.perpetual_only {
            catalog.load_perpetuals().await?;
        } else {
            catalog.load_all().await?;
        }
        let mut instruments = Vec::new();
        let spots = catalog
            .markets()
            .into_iter()
            .filter_map(|market| match market {
                DeepXMarketMetadata::Spot(spot) => Some(spot),
                DeepXMarketMetadata::Perpetual(_) => None,
            })
            .collect::<Vec<_>>();
        let spot_evidence = if let Some(network) = &self.network {
            if spots.is_empty() {
                Vec::new()
            } else {
                let endpoints = observe_and_validate_rpc_endpoint_identities(network)
                    .await
                    .map_err(anyhow::Error::from)?;
                let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
                    .await
                    .map_err(anyhow::Error::from)?;
                let pairs = spots
                    .iter()
                    .map(|spot| {
                        nautilus_core::hex::decode_array::<32>(
                            spot.pair.strip_prefix("0x").unwrap_or(&spot.pair),
                        )
                        .map_err(anyhow::Error::from)
                    })
                    .collect::<Result<Vec<_>>>()?;
                observe_current_spot_market_spec_batch(
                    &network.environment,
                    &endpoints,
                    &capabilities,
                    &pairs,
                )
                .await?
            }
        } else {
            Vec::new()
        };

        if self.network.is_some() && spot_evidence.len() != spots.len() {
            bail!("DeepX current Spot spec batch does not match the REST market count");
        }
        let mut evidence_by_pair = std::collections::BTreeMap::new();
        for evidence in spot_evidence {
            if evidence_by_pair.insert(evidence.pair(), evidence).is_some() {
                bail!("duplicate DeepX current Spot spec pair");
            }
        }

        for market in catalog.markets() {
            match market {
                DeepXMarketMetadata::Perpetual(_) => instruments.push(self.convert(market)?),
                DeepXMarketMetadata::Spot(spot) if self.network.is_some() => {
                    let pair = nautilus_core::hex::decode_array::<32>(
                        spot.pair.strip_prefix("0x").unwrap_or(&spot.pair),
                    )?;
                    let evidence = evidence_by_pair
                        .remove(&pair)
                        .ok_or_else(|| anyhow::anyhow!("missing current DeepX Spot spec"))?;
                    instruments.push(parse_current_spot_instrument(
                        spot,
                        &evidence,
                        self.ts_init,
                    )?);
                }
                DeepXMarketMetadata::Spot(_) => {}
            }
        }
        if !evidence_by_pair.is_empty() {
            bail!("current DeepX Spot specs contain pairs absent from the REST catalog");
        }
        Ok((catalog, instruments))
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

    async fn load_all(&mut self, filters: Option<&HashMap<String, String>>) -> Result<()> {
        reject_filters(filters, "load_all")?;
        let (catalog, instruments) = self.fetch_instruments().await?;

        self.catalog = catalog;
        self.store.clear();
        self.store.add_bulk(instruments);
        self.store.set_initialized();
        Ok(())
    }

    async fn load_ids(
        &mut self,
        instrument_ids: &[InstrumentId],
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        reject_filters(filters, "load_ids")?;
        let missing = instrument_ids
            .iter()
            .filter(|id| !self.store.contains(id))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }

        let (catalog, instruments) = self.fetch_instruments().await?;
        let mut store = InstrumentStore::new();
        store.add_bulk(instruments);
        for instrument in self.store.list_all() {
            store.add(instrument.clone());
        }

        let absent = missing
            .iter()
            .filter(|id| !store.contains(id))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !absent.is_empty() {
            bail!("DeepX instruments not found: {}", absent.join(", "));
        }

        self.catalog = catalog;
        self.store = store;
        self.store.set_initialized();
        Ok(())
    }

    async fn load(
        &mut self,
        instrument_id: &InstrumentId,
        filters: Option<&HashMap<String, String>>,
    ) -> Result<()> {
        reject_filters(filters, "load")?;
        if self.store.contains(instrument_id) {
            return Ok(());
        }
        self.load_ids(&[*instrument_id], None).await
    }
}

fn reject_filters(filters: Option<&HashMap<String, String>>, method: &str) -> Result<()> {
    if let Some(filters) = filters.filter(|filters| !filters.is_empty()) {
        bail!("DeepX instrument provider does not support filters in `{method}`: {filters:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{Router, routing::get};
    use nautilus_model::instruments::Instrument;
    use rstest::rstest;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        common::{DeepXProductType, format_instrument_id},
        http::DeepXHttpClient,
    };

    const SPOT_RESPONSE: &str =
        include_str!("../../deepx-rust-sdk/test_data/http/testnet/spot_markets.json");
    const PERP_RESPONSE: &str =
        include_str!("../../deepx-rust-sdk/test_data/http/testnet/perp_markets.json");

    async fn catalog_router() -> Router {
        Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(|| async { PERP_RESPONSE }),
            )
    }

    async fn provider() -> DeepXInstrumentProvider {
        let router = catalog_router().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http_client = DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        DeepXInstrumentProvider::new(DeepXMarketProvider::new(http_client))
    }

    #[tokio::test]
    async fn load_all_converts_every_perpetual_and_marks_initialized() {
        let mut provider = provider().await;

        provider.load_all(None).await.unwrap();

        assert!(provider.store().is_initialized());
        assert_eq!(provider.store().count(), 1);
        let perp = format_instrument_id("ETH-USDC", &DeepXProductType::Perpetual).unwrap();
        assert!(provider.store().contains(&perp));
        let spot = format_instrument_id("ETH-USDC", &DeepXProductType::Spot).unwrap();
        assert!(!provider.store().contains(&spot));
    }

    #[tokio::test]
    async fn perpetual_only_provider_loads_without_spot_route_or_rpc() {
        let router = Router::new().route(
            "/internal/v1/market/perp/markets",
            get(|| async { PERP_RESPONSE }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http = DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        let mut provider = DeepXInstrumentProvider::perpetual_only(DeepXMarketProvider::new(http));

        provider.load_all(None).await.unwrap();

        assert!(provider.store().is_initialized());
        assert_eq!(provider.store().count(), 1);
        assert_eq!(provider.catalog().markets().len(), 1);
    }

    #[tokio::test]
    async fn load_all_fails_closed_when_any_market_converts_invalidly() {
        // Serve a perp page whose market has a zero step size: strict instrument
        // conversion must fail and the whole load must leave the store untouched.
        let mut perp_page: serde_json::Value = serde_json::from_str(PERP_RESPONSE).unwrap();
        perp_page["data"][0]["orderSpecStepSize"] = serde_json::json!("0");
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(move || async move { axum::Json(perp_page.clone()) }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http_client = DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        let mut provider = DeepXInstrumentProvider::new(DeepXMarketProvider::new(http_client));

        let error = provider.load_all(None).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("invalid DeepX perpetual instrument"),
            "unexpected error: {error}"
        );
        assert!(!provider.store().is_initialized());
        assert!(provider.store().is_empty());
    }

    #[tokio::test]
    async fn finalized_spot_rpc_failure_does_not_publish_catalog_or_store() {
        let router = catalog_router().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http_client = DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        let network = crate::config::DeepXNetworkConfig {
            base_url_rpc: Some("http://127.0.0.1:1".to_string()),
            ..Default::default()
        };
        let mut provider =
            DeepXInstrumentProvider::with_network(DeepXMarketProvider::new(http_client), network);

        assert!(provider.load_all(None).await.is_err());

        assert!(!provider.catalog().initialized());
        assert!(provider.catalog().is_empty());
        assert!(!provider.store().is_initialized());
        assert!(provider.store().is_empty());
    }

    #[tokio::test]
    async fn load_all_preserves_previous_store_when_refresh_fails() {
        let mut provider = provider().await;
        provider.load_all(None).await.unwrap();
        let expected = provider.store().count();
        assert!(expected > 0);

        // Point the catalog at a dead endpoint: the refresh must fail without
        // touching the previously initialized store.
        let dead = DeepXHttpClient::new("http://127.0.0.1:1".to_string(), Some(1), None).unwrap();
        provider.catalog_mut().replace_client(dead);

        assert!(provider.load_all(None).await.is_err());

        assert!(provider.store().is_initialized());
        assert_eq!(provider.store().count(), expected);
    }

    #[tokio::test]
    async fn load_ids_reports_identity_absent_from_complete_response() {
        let mut provider = provider().await;
        let missing = InstrumentId::from_as_ref("SOL-USDC-PERP.DEEPX").unwrap();

        let error = provider.load_ids(&[missing], None).await.unwrap_err();

        assert!(error.to_string().contains("SOL-USDC-PERP.DEEPX"));
        assert!(!provider.catalog().initialized());
        assert!(provider.catalog().is_empty());
        assert!(!provider.store().is_initialized());
        assert!(provider.store().is_empty());
    }

    #[tokio::test]
    async fn load_single_missing_identity_fails_and_existing_identity_is_idempotent() {
        let mut provider = provider().await;
        provider.load_all(None).await.unwrap();
        let perp = format_instrument_id("ETH-USDC", &DeepXProductType::Perpetual).unwrap();
        let before = provider.store().count();

        provider.load(&perp, None).await.unwrap();

        assert_eq!(provider.store().count(), before);

        let missing = InstrumentId::from_as_ref("BTC-USDC-PERP.DEEPX").unwrap();
        assert!(provider.load(&missing, None).await.is_err());
    }

    #[tokio::test]
    async fn load_ids_preserves_cached_instruments_across_refresh() {
        let mut provider = provider().await;
        provider.load_all(None).await.unwrap();
        let perp = format_instrument_id("ETH-USDC", &DeepXProductType::Perpetual).unwrap();
        let original = provider.store().find(&perp).unwrap().clone();

        provider.load_ids(&[perp], None).await.unwrap();

        let refreshed = provider.store().find(&perp).unwrap();
        assert_eq!(original.id(), refreshed.id());
        assert!(provider.store().is_initialized());
    }

    #[tokio::test]
    async fn non_empty_filters_are_rejected_synchronously() {
        let mut provider = provider().await;
        let mut filters = HashMap::new();
        filters.insert("currency".to_string(), "ETH".to_string());

        let error = provider.load_all(Some(&filters)).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not support filters in `load_all`")
        );
    }

    #[rstest]
    fn convert_fails_closed_for_spot_metadata() {
        let http_client =
            DeepXHttpClient::new("https://api.testnet.deepx.trade".to_string(), Some(5), None)
                .unwrap();
        let provider = DeepXInstrumentProvider::new(DeepXMarketProvider::new(http_client));
        let spot = serde_json::from_str::<
            crate::http::DeepXApiResponse<Vec<crate::http::DeepXSpotMarket>>,
        >(SPOT_RESPONSE)
        .unwrap()
        .data
        .into_iter()
        .next()
        .unwrap();

        let error = provider
            .convert(&DeepXMarketMetadata::Spot(Box::new(spot)))
            .unwrap_err();

        assert!(error.is::<DeepXSpotInstrumentUnsupported>());
    }
}
