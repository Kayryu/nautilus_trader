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

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
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
    initialized: bool,
}

impl DeepXMarketProvider {
    /// Creates an empty market catalog.
    #[must_use]
    pub fn new(client: DeepXHttpClient) -> Self {
        Self {
            client,
            markets: BTreeMap::new(),
            initialized: false,
        }
    }

    /// Returns whether a complete Spot and perpetual load has succeeded.
    #[must_use]
    pub const fn initialized(&self) -> bool {
        self.initialized
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
        for market in spot {
            insert_unique(&mut markets, DeepXMarketMetadata::Spot(Box::new(market)))?;
        }
        for market in perpetual {
            insert_unique(
                &mut markets,
                DeepXMarketMetadata::Perpetual(Box::new(market)),
            )?;
        }

        self.markets = markets;
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
        atomic::{AtomicBool, Ordering},
    };

    use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
    use nautilus_model::identifiers::{InstrumentId, Symbol};
    use tokio::net::TcpListener;

    use super::*;
    use crate::common::consts::DEEPX_VENUE;

    const SPOT_RESPONSE: &str = include_str!("../test_data/http/testnet/spot_markets.json");
    const PERP_RESPONSE: &str = include_str!("../test_data/http/testnet/perp_markets.json");

    async fn provider(fail_perp: Arc<AtomicBool>) -> DeepXMarketProvider {
        let router = Router::new()
            .route(
                "/internal/v1/market/spot/markets",
                get(|| async { SPOT_RESPONSE }),
            )
            .route(
                "/internal/v1/market/perp/markets",
                get(move || {
                    let fail_perp = Arc::clone(&fail_perp);
                    async move {
                        if fail_perp.load(Ordering::Relaxed) {
                            return (StatusCode::BAD_REQUEST, "perp unavailable").into_response();
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

    #[tokio::test]
    async fn loads_complete_catalog_and_preserves_deployment_identities() {
        let mut provider = provider(Arc::new(AtomicBool::new(false))).await;

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
    }

    #[tokio::test]
    async fn failed_refresh_preserves_previous_complete_catalog() {
        let fail_perp = Arc::new(AtomicBool::new(false));
        let mut provider = provider(Arc::clone(&fail_perp)).await;
        provider.load_all().await.unwrap();
        let expected_ids = provider.instrument_ids();
        fail_perp.store(true, Ordering::Relaxed);

        assert!(provider.load_all().await.is_err());

        assert!(provider.initialized());
        assert_eq!(provider.instrument_ids(), expected_ids);
    }

    #[tokio::test]
    async fn load_ids_reports_identity_absent_from_complete_response() {
        let mut provider = provider(Arc::new(AtomicBool::new(false))).await;
        let missing = InstrumentId::new(Symbol::new("SOL-USDC"), *DEEPX_VENUE);

        let error = provider.load_ids(&[missing]).await.unwrap_err();

        assert!(provider.initialized());
        assert!(error.to_string().contains("SOL-USDC.DEEPX"));
    }
}
