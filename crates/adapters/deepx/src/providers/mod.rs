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

//! Failure-atomic public market metadata catalog.

mod instrument;

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
pub use instrument::{DeepXInstrumentProvider, DeepXSpotInstrumentUnsupported};
use nautilus_core::hex;
use nautilus_model::identifiers::InstrumentId;

use crate::{
    common::{DeepXProductType, format_instrument_id},
    http::{DeepXHttpClient, DeepXPerpMarket, DeepXSpotMarket},
};

/// Public DeepX market metadata keyed by canonical Nautilus identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXMarketMetadata {
    /// Spot market metadata, including its deployment-provided bytes32 pair ID.
    Spot(Box<DeepXSpotMarket>),
    /// Perpetual market metadata, including its deployment-provided numeric market ID.
    Perpetual(Box<DeepXPerpMarket>),
}

impl DeepXMarketMetadata {
    /// Returns the canonical Nautilus identity for this market.
    ///
    /// # Errors
    ///
    /// Returns an error when the venue symbols cannot form a canonical pair.
    pub fn instrument_id(&self) -> Result<InstrumentId> {
        match self {
            Self::Spot(market) => format_instrument_id(
                &format!("{}-{}", market.base_symbol, market.quote_symbol),
                &DeepXProductType::Spot,
            ),
            Self::Perpetual(market) => format_instrument_id(
                &format!("{}-{}", market.base_symbol, market.quote_symbol),
                &DeepXProductType::Perpetual,
            ),
        }
        .map_err(Into::into)
    }

    /// Returns the deployment-provided market identity without normalization.
    #[must_use]
    pub fn deployment_id(&self) -> String {
        match self {
            Self::Spot(market) => market.pair.clone(),
            Self::Perpetual(market) => market.id.to_string(),
        }
    }
}

/// Read-only catalog of verified public Spot and perpetual market metadata.
#[derive(Clone, Debug)]
pub struct DeepXMarketProvider {
    client: DeepXHttpClient,
    markets: BTreeMap<InstrumentId, DeepXMarketMetadata>,
    spot_instrument_ids: BTreeMap<[u8; 32], InstrumentId>,
    perpetual_instrument_ids: BTreeMap<u64, InstrumentId>,
    initialized: bool,
}

impl DeepXMarketProvider {
    /// Creates an empty market catalog.
    #[must_use]
    pub fn new(client: DeepXHttpClient) -> Self {
        Self {
            client,
            markets: BTreeMap::new(),
            spot_instrument_ids: BTreeMap::new(),
            perpetual_instrument_ids: BTreeMap::new(),
            initialized: false,
        }
    }

    /// Returns whether a complete Spot and perpetual load has succeeded.
    #[must_use]
    pub const fn initialized(&self) -> bool {
        self.initialized
    }

    /// Returns whether the market catalog contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }

    /// Returns the primary REST endpoint used to load the market catalog.
    #[must_use]
    pub fn base_url(&self) -> &str {
        self.client.base_url()
    }

    /// Returns all REST endpoints used to load the market catalog in failover order.
    #[must_use]
    pub fn base_urls(&self) -> &[String] {
        self.client.base_urls()
    }

    /// Returns all markets in canonical identity order.
    #[must_use]
    pub fn markets(&self) -> Vec<&DeepXMarketMetadata> {
        self.markets.values().collect()
    }

    /// Returns all canonical market identities.
    #[must_use]
    pub fn instrument_ids(&self) -> Vec<InstrumentId> {
        self.markets.keys().copied().collect()
    }

    /// Returns market metadata for a canonical identity.
    #[must_use]
    pub fn market(&self, instrument_id: &InstrumentId) -> Option<&DeepXMarketMetadata> {
        self.markets.get(instrument_id)
    }

    /// Returns the canonical Spot instrument identity for a deployment pair ID.
    #[must_use]
    pub fn spot_instrument_id(&self, pair: &str) -> Option<InstrumentId> {
        let pair = decode_spot_pair_id(pair).ok()?;
        self.spot_instrument_ids.get(&pair).copied()
    }

    /// Returns the canonical perpetual instrument identity for a deployment market ID.
    #[must_use]
    pub fn perpetual_instrument_id(&self, market_id: u64) -> Option<InstrumentId> {
        self.perpetual_instrument_ids.get(&market_id).copied()
    }

    /// Loads both public market lists and replaces the catalog only after complete validation.
    ///
    /// # Errors
    ///
    /// Returns an error when either request, identity conversion, or duplicate validation fails.
    /// The previous catalog remains unchanged.
    pub async fn load_all(&mut self) -> Result<()> {
        let (spot, perpetual) = tokio::try_join!(
            self.client.get_spot_markets(),
            self.client.get_perp_markets(),
        )
        .context("failed to load complete DeepX market metadata")?;

        let mut markets = BTreeMap::new();
        let mut spot_instrument_ids = BTreeMap::new();
        let mut perpetual_instrument_ids = BTreeMap::new();
        for market in spot {
            let pair = decode_spot_pair_id(&market.pair)?;
            let instrument_id = format_instrument_id(
                &format!("{}-{}", market.base_symbol, market.quote_symbol),
                &DeepXProductType::Spot,
            )?;
            if spot_instrument_ids.insert(pair, instrument_id).is_some() {
                bail!("duplicate DeepX Spot pair ID: {}", market.pair);
            }
            insert_unique(&mut markets, DeepXMarketMetadata::Spot(Box::new(market)))?;
        }
        for market in perpetual {
            let market_id = market.id;
            let instrument_id = format_instrument_id(
                &format!("{}-{}", market.base_symbol, market.quote_symbol),
                &DeepXProductType::Perpetual,
            )?;
            if perpetual_instrument_ids
                .insert(market_id, instrument_id)
                .is_some()
            {
                bail!("duplicate DeepX perpetual market ID: {market_id}");
            }
            insert_unique(
                &mut markets,
                DeepXMarketMetadata::Perpetual(Box::new(market)),
            )?;
        }

        self.markets = markets;
        self.spot_instrument_ids = spot_instrument_ids;
        self.perpetual_instrument_ids = perpetual_instrument_ids;
        self.initialized = true;
        Ok(())
    }

    /// Loads the complete catalog when any requested identity is absent, then validates all IDs.
    ///
    /// # Errors
    ///
    /// Returns an error when loading fails or an identity is not present in the complete response.
    pub async fn load_ids(&mut self, instrument_ids: &[InstrumentId]) -> Result<()> {
        if instrument_ids
            .iter()
            .any(|id| !self.markets.contains_key(id))
        {
            self.load_all().await?;
        }
        let missing = instrument_ids
            .iter()
            .filter(|id| !self.markets.contains_key(id))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            bail!("DeepX markets not found: {}", missing.join(", "));
        }
        Ok(())
    }

    /// Loads the complete catalog when the requested identity is absent.
    ///
    /// # Errors
    ///
    /// Returns an error when loading fails or the identity is not present in the response.
    pub async fn load(&mut self, instrument_id: &InstrumentId) -> Result<()> {
        self.load_ids(&[*instrument_id]).await
    }

    /// Replaces the underlying HTTP client, for example to point the catalog at another
    /// endpoint family after construction.
    pub fn replace_client(&mut self, client: DeepXHttpClient) -> DeepXHttpClient {
        std::mem::replace(&mut self.client, client)
    }
}

fn decode_spot_pair_id(pair: &str) -> Result<[u8; 32]> {
    hex::decode_array::<32>(pair.strip_prefix("0x").unwrap_or(pair)).with_context(|| {
        format!("invalid DeepX Spot pair '{pair}': expected 32-byte hexadecimal identity")
    })
}

fn insert_unique(
    markets: &mut BTreeMap<InstrumentId, DeepXMarketMetadata>,
    market: DeepXMarketMetadata,
) -> Result<()> {
    let instrument_id = market.instrument_id()?;
    if markets.insert(instrument_id, market).is_some() {
        bail!("duplicate DeepX market identity: {instrument_id}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
    use nautilus_model::identifiers::{InstrumentId, Symbol};
    use nautilus_network::retry::RetryConfig;
    use rstest::rstest;
    use tokio::net::TcpListener;

    use super::*;
    use crate::common::consts::DEEPX_VENUE;

    const SPOT_RESPONSE: &str = include_str!("../../test_data/http/testnet/spot_markets.json");
    const PERP_RESPONSE: &str = include_str!("../../test_data/http/testnet/perp_markets.json");

    async fn provider(
        fail_perp: Arc<AtomicBool>,
        duplicate_spot_pair: Arc<AtomicBool>,
        duplicate_perp_id: Arc<AtomicBool>,
        duplicate_perp_identity: Arc<AtomicBool>,
    ) -> DeepXMarketProvider {
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(move || {
                    let duplicate_spot_pair = Arc::clone(&duplicate_spot_pair);
                    async move {
                        if duplicate_spot_pair.load(Ordering::Relaxed) {
                            let mut response: serde_json::Value =
                                serde_json::from_str(SPOT_RESPONSE).unwrap();
                            let mut duplicate = response["data"][0].clone();
                            duplicate["pair"] = duplicate["pair"]
                                .as_str()
                                .unwrap()
                                .trim_start_matches("0x")
                                .to_ascii_uppercase()
                                .into();
                            response["data"][0]["baseSymbol"] = "btc".into();
                            response["data"].as_array_mut().unwrap().push(duplicate);
                            return Json(response).into_response();
                        }
                        SPOT_RESPONSE.into_response()
                    }
                }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(move || {
                    let fail_perp = Arc::clone(&fail_perp);
                    let duplicate_perp_id = Arc::clone(&duplicate_perp_id);
                    let duplicate_perp_identity = Arc::clone(&duplicate_perp_identity);
                    async move {
                        if fail_perp.load(Ordering::Relaxed) {
                            return (StatusCode::BAD_REQUEST, "perp unavailable").into_response();
                        }
                        if duplicate_perp_id.load(Ordering::Relaxed) {
                            let mut response: serde_json::Value =
                                serde_json::from_str(PERP_RESPONSE).unwrap();
                            let duplicate = response["data"][0].clone();
                            response["data"][0]["name"] = "BTC-USDC".into();
                            response["data"][0]["baseSymbol"] = "btc".into();
                            response["data"].as_array_mut().unwrap().push(duplicate);
                            return Json(response).into_response();
                        }
                        if duplicate_perp_identity.load(Ordering::Relaxed) {
                            let mut response: serde_json::Value =
                                serde_json::from_str(PERP_RESPONSE).unwrap();
                            let mut duplicate = response["data"][0].clone();
                            duplicate["id"] = 4.into();
                            response["data"].as_array_mut().unwrap().push(duplicate);
                            return Json(response).into_response();
                        }
                        PERP_RESPONSE.into_response()
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = DeepXHttpClient::new(format!("http://{address}"), Some(5), None).unwrap();
        DeepXMarketProvider::new(client)
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[rstest]
    #[case("0x00")]
    #[case("not-hex")]
    #[case("000000000000000000000000000000000000000000000000000000000000000000")]
    fn rejects_invalid_spot_pair_id(#[case] pair: &str) {
        assert!(decode_spot_pair_id(pair).is_err());
    }

    #[tokio::test]
    async fn loads_complete_catalog_and_preserves_deployment_identities() {
        let mut provider = provider(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        provider.load_all().await.unwrap();

        assert!(provider.initialized());
        assert_eq!(provider.instrument_ids().len(), 2);
        let spot = format_instrument_id("ETH-USDC", &DeepXProductType::Spot).unwrap();
        let perp = format_instrument_id("ETH-USDC", &DeepXProductType::Perpetual).unwrap();
        assert_eq!(
            provider.market(&spot).unwrap().deployment_id(),
            "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"
        );
        assert_eq!(provider.market(&perp).unwrap().deployment_id(), "3");
        assert_eq!(
            provider.spot_instrument_id(
                "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37",
            ),
            Some(spot),
        );
        assert_eq!(
            provider.spot_instrument_id(
                "9068D4AC891A14784C17877EB74BD8489B3367C71D72766DBFA4DFBFB662FA37",
            ),
            Some(spot),
        );
        assert_eq!(provider.spot_instrument_id("0x00"), None);
        assert_eq!(provider.perpetual_instrument_id(3), Some(perp));
        assert_eq!(provider.perpetual_instrument_id(u64::MAX), None);
    }

    #[tokio::test]
    async fn loads_complete_catalog_when_one_request_fails_over() {
        let primary_spot_requests = Arc::new(AtomicUsize::new(0));
        let primary_perp_requests = Arc::new(AtomicUsize::new(0));
        let secondary_spot_requests = Arc::new(AtomicUsize::new(0));
        let secondary_perp_requests = Arc::new(AtomicUsize::new(0));

        let primary = spawn_server(
            Router::new()
                .route(
                    "/internal/v1/market/spot/markets",
                    get({
                        let requests = Arc::clone(&primary_spot_requests);
                        move || {
                            let requests = Arc::clone(&requests);
                            async move {
                                requests.fetch_add(1, Ordering::SeqCst);
                                SPOT_RESPONSE
                            }
                        }
                    }),
                )
                .route(
                    "/internal/v1/market/perp/markets",
                    get({
                        let requests = Arc::clone(&primary_perp_requests);
                        move || {
                            let requests = Arc::clone(&requests);
                            async move {
                                requests.fetch_add(1, Ordering::SeqCst);
                                (StatusCode::SERVICE_UNAVAILABLE, "perp unavailable")
                            }
                        }
                    }),
                ),
        )
        .await;
        let secondary = spawn_server(
            Router::new()
                .route(
                    "/internal/v1/market/spot/markets",
                    get({
                        let requests = Arc::clone(&secondary_spot_requests);
                        move || {
                            let requests = Arc::clone(&requests);
                            async move {
                                requests.fetch_add(1, Ordering::SeqCst);
                                SPOT_RESPONSE
                            }
                        }
                    }),
                )
                .route(
                    "/internal/v1/market/perp/markets",
                    get({
                        let requests = Arc::clone(&secondary_perp_requests);
                        move || {
                            let requests = Arc::clone(&requests);
                            async move {
                                requests.fetch_add(1, Ordering::SeqCst);
                                PERP_RESPONSE
                            }
                        }
                    }),
                ),
        )
        .await;
        let client = DeepXHttpClient::new_with_endpoints(
            [primary, secondary],
            Some(5),
            None,
            RetryConfig {
                max_retries: 1,
                initial_delay_ms: 1,
                max_delay_ms: 1,
                backoff_factor: 1.0,
                jitter_ms: 0,
                operation_timeout_ms: Some(5_000),
                immediate_first: true,
                max_elapsed_ms: Some(10_000),
            },
        )
        .unwrap();
        let mut provider = DeepXMarketProvider::new(client);

        provider.load_all().await.unwrap();

        assert!(provider.initialized());
        let spot = format_instrument_id("ETH-USDC", &DeepXProductType::Spot).unwrap();
        let perpetual = format_instrument_id("ETH-USDC", &DeepXProductType::Perpetual).unwrap();
        assert_eq!(provider.instrument_ids(), vec![spot, perpetual]);
        assert_eq!(
            provider.market(&spot).unwrap().deployment_id(),
            "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37",
        );
        assert_eq!(provider.perpetual_instrument_id(3), Some(perpetual));
        assert_eq!(primary_spot_requests.load(Ordering::SeqCst), 1);
        assert_eq!(primary_perp_requests.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_spot_requests.load(Ordering::SeqCst), 0);
        assert_eq!(secondary_perp_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_initial_load_keeps_catalog_uninitialized() {
        let mut provider = provider(
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        assert!(provider.load_all().await.is_err());

        assert!(!provider.initialized());
        assert!(provider.is_empty());
        assert!(provider.instrument_ids().is_empty());
        assert_eq!(provider.spot_instrument_id("0x00"), None);
        assert_eq!(provider.perpetual_instrument_id(3), None);
    }

    #[tokio::test]
    async fn failed_refresh_preserves_previous_complete_catalog() {
        let fail_perp = Arc::new(AtomicBool::new(false));
        let mut provider = provider(
            Arc::clone(&fail_perp),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        provider.load_all().await.unwrap();
        let expected_ids = provider.instrument_ids();
        let expected_perpetual_id = provider.perpetual_instrument_id(3);
        fail_perp.store(true, Ordering::Relaxed);

        assert!(provider.load_all().await.is_err());

        assert!(provider.initialized());
        assert_eq!(provider.instrument_ids(), expected_ids);
        assert_eq!(provider.perpetual_instrument_id(3), expected_perpetual_id);
    }

    #[tokio::test]
    async fn duplicate_perpetual_market_id_preserves_previous_complete_catalog() {
        let duplicate_perp_id = Arc::new(AtomicBool::new(false));
        let mut provider = provider(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&duplicate_perp_id),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        provider.load_all().await.unwrap();
        let expected_ids = provider.instrument_ids();
        let expected_perpetual_id = provider.perpetual_instrument_id(3);
        duplicate_perp_id.store(true, Ordering::Relaxed);

        let error = provider.load_all().await.unwrap_err();

        assert_eq!(error.to_string(), "duplicate DeepX perpetual market ID: 3");
        assert!(provider.initialized());
        assert_eq!(provider.instrument_ids(), expected_ids);
        assert_eq!(provider.perpetual_instrument_id(3), expected_perpetual_id);
    }

    #[tokio::test]
    async fn duplicate_perpetual_identity_preserves_previous_complete_catalog() {
        let duplicate_perp_identity = Arc::new(AtomicBool::new(false));
        let mut provider = provider(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&duplicate_perp_identity),
        )
        .await;
        provider.load_all().await.unwrap();
        let expected_ids = provider.instrument_ids();
        let expected_perpetual_id = provider.perpetual_instrument_id(3);
        duplicate_perp_identity.store(true, Ordering::Relaxed);

        let error = provider.load_all().await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "duplicate DeepX market identity: ETH-USDC-PERP.DEEPX",
        );
        assert!(provider.initialized());
        assert_eq!(provider.instrument_ids(), expected_ids);
        assert_eq!(provider.perpetual_instrument_id(3), expected_perpetual_id);
        assert_eq!(provider.perpetual_instrument_id(4), None);
    }

    #[tokio::test]
    async fn duplicate_spot_pair_id_preserves_previous_complete_catalog() {
        let duplicate_spot_pair = Arc::new(AtomicBool::new(false));
        let mut provider = provider(
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&duplicate_spot_pair),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        provider.load_all().await.unwrap();
        let expected_ids = provider.instrument_ids();
        let pair = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
        let expected_spot_id = provider.spot_instrument_id(pair);
        let expected_perpetual_id = provider.perpetual_instrument_id(3);
        duplicate_spot_pair.store(true, Ordering::Relaxed);

        let error = provider.load_all().await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "duplicate DeepX Spot pair ID: \
             9068D4AC891A14784C17877EB74BD8489B3367C71D72766DBFA4DFBFB662FA37",
        );
        assert!(provider.initialized());
        assert_eq!(provider.instrument_ids(), expected_ids);
        assert_eq!(provider.spot_instrument_id(pair), expected_spot_id);
        assert_eq!(provider.perpetual_instrument_id(3), expected_perpetual_id);
    }

    #[tokio::test]
    async fn load_ids_reports_identity_absent_from_complete_response() {
        let mut provider = provider(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let missing = InstrumentId::new(Symbol::new("SOL-USDC"), *DEEPX_VENUE);

        let error = provider.load_ids(&[missing]).await.unwrap_err();

        assert!(provider.initialized());
        assert!(error.to_string().contains("SOL-USDC.DEEPX"));
    }
}
