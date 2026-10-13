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

//! Historical public market data for offline Python backtests.

use std::num::NonZeroUsize;

use anyhow::Context;
use nautilus_core::{python::to_pyvalue_err, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::TradeTick, instruments::Instrument, python::instruments::instrument_any_to_pyobject,
};
use pyo3::prelude::*;

use crate::{
    config::DeepXDataClientConfig,
    data::{parse_spot_trade_tick, parse_trade_tick},
    http::{
        DeepXAccountSortOrder, DeepXHttpClient, DeepXPerpTradesHistoryRequest,
        DeepXSpotTradesRequest,
    },
    instruments::parse_perpetual_instrument,
    providers::{DeepXInstrumentProvider, DeepXMarketMetadata, DeepXMarketProvider},
};

/// Loads bounded public perpetual trades and their verified instrument for a backtest.
///
/// The returned trades are chronological with replay initialization times equal to event times.
/// This blocking read never submits transactions.
///
/// # Errors
///
/// Returns an error for invalid bounds, failed pagination, unavailable market metadata, or a trade
/// that cannot be represented exactly by the validated instrument.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "load_deepx_perp_trade_ticks", signature = (market_id, start_ms, end_ms, limit=1000, max_pages=100, config=None))]
#[gen_stub(override_return_type(
    type_repr = "tuple[nautilus_trader.model.CryptoPerpetual, list[nautilus_trader.model.TradeTick]]",
    imports = ("nautilus_trader.model",),
))]
pub(super) fn py_load_deepx_perp_trade_ticks(
    py: Python<'_>,
    market_id: u64,
    start_ms: u64,
    end_ms: u64,
    limit: usize,
    max_pages: usize,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<(Py<PyAny>, Vec<TradeTick>)> {
    let limit = NonZeroUsize::new(limit).ok_or_else(|| to_pyvalue_err("limit must be nonzero"))?;
    if limit.get() > 10_000 {
        return Err(to_pyvalue_err("limit must not exceed 10000"));
    }
    let request = DeepXPerpTradesHistoryRequest {
        market_id,
        start_ms,
        end_ms,
        page_size: 100,
        max_pages,
    };
    request.validate().map_err(to_pyvalue_err)?;
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    let (instrument, ticks) = py
        .detach(move || {
            runtime.block_on(async move {
                let mut catalog = DeepXMarketProvider::new(client.clone());
                catalog.load_perpetuals().await?;
                let instrument_id = catalog
                    .perpetual_instrument_id(market_id)
                    .context("DeepX perpetual market ID is unavailable")?;
                let DeepXMarketMetadata::Perpetual(market) = catalog
                    .market(&instrument_id)
                    .context("DeepX perpetual market metadata is unavailable")?
                else {
                    anyhow::bail!("DeepX market ID does not identify a perpetual market");
                };
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                let instrument = parse_perpetual_instrument(market, ts_init)?;
                let raw = client
                    .get_perp_trades_history_limited(&request, limit)
                    .await?;
                let mut ticks = raw
                    .iter()
                    .map(|trade| {
                        let mut tick = parse_trade_tick(trade, &instrument, market_id, ts_init)?;
                        tick.ts_init = tick.ts_event;
                        Ok(tick)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                ticks.reverse();
                Ok::<_, anyhow::Error>((instrument, ticks))
            })
        })
        .map_err(to_pyvalue_err)?;
    Ok((instrument_any_to_pyobject(py, instrument)?, ticks))
}

/// Loads bounded public Spot trades and their finalized-spec-verified instrument for a backtest.
///
/// The returned trades are chronological with replay initialization times equal to event times.
/// Current market and finalized Spot specs do not reconstruct historical trading rules.
///
/// # Errors
///
/// Returns an error for invalid bounds, unavailable or inconsistent market evidence, failed
/// pagination, or a trade that cannot be represented exactly by the validated instrument.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.deepx")]
#[pyo3(name = "load_deepx_spot_trade_ticks", signature = (market_name, start_ms, end_ms, limit=1000, max_pages=100, config=None))]
#[gen_stub(override_return_type(
    type_repr = "tuple[nautilus_trader.model.CurrencyPair, list[nautilus_trader.model.TradeTick]]",
    imports = ("nautilus_trader.model",),
))]
pub(super) fn py_load_deepx_spot_trade_ticks(
    py: Python<'_>,
    market_name: String,
    start_ms: u64,
    end_ms: u64,
    limit: usize,
    max_pages: usize,
    config: Option<DeepXDataClientConfig>,
) -> PyResult<(Py<PyAny>, Vec<TradeTick>)> {
    let limit = NonZeroUsize::new(limit).ok_or_else(|| to_pyvalue_err("limit must be nonzero"))?;
    if limit.get() > 10_000 {
        return Err(to_pyvalue_err("limit must not exceed 10000"));
    }
    if max_pages == 0 {
        return Err(to_pyvalue_err("max_pages must be nonzero"));
    }
    let request = DeepXSpotTradesRequest {
        name: Some(market_name.clone()),
        pair: None,
        wallet: None,
        start_ms: Some(start_ms),
        end_ms: Some(end_ms),
        cursor: None,
        sort: DeepXAccountSortOrder::Descending,
        page_size: Some(100),
    };
    request.validate().map_err(to_pyvalue_err)?;
    let config = config.unwrap_or_default();
    config.validate().map_err(to_pyvalue_err)?;
    if config.perpetual_only {
        return Err(to_pyvalue_err("Spot trade loading requires Spot discovery"));
    }
    let client = DeepXHttpClient::from_network_config(
        &config.network,
        Some(config.http_timeout_secs),
        config.proxy_url,
    )
    .map_err(to_pyvalue_err)?;
    let network = config.network;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(to_pyvalue_err)?;
    let (instrument, ticks) = py
        .detach(move || {
            runtime.block_on(async move {
                let provider = DeepXInstrumentProvider::with_network(
                    DeepXMarketProvider::new(client.clone()),
                    network,
                );
                let (catalog, instruments) = provider.fetch_instruments().await?;
                let mut matches = catalog
                    .markets()
                    .into_iter()
                    .filter_map(|entry| match entry {
                        DeepXMarketMetadata::Spot(market) if market.name == market_name => {
                            Some(market)
                        }
                        _ => None,
                    });
                let market = matches
                    .next()
                    .context("DeepX Spot market name is unavailable")?;
                anyhow::ensure!(matches.next().is_none(), "ambiguous DeepX Spot market name");
                let instrument_id = catalog
                    .spot_instrument_id(&market.pair)
                    .context("DeepX Spot pair identity is unavailable")?;
                let instrument = instruments
                    .into_iter()
                    .find(|instrument| instrument.id() == instrument_id)
                    .context("finalized DeepX Spot instrument is unavailable")?;
                let raw = client
                    .get_spot_trades_limited(&request, limit, max_pages)
                    .await?;
                let ts_init = get_atomic_clock_realtime().get_time_ns();
                let mut ticks = raw
                    .iter()
                    .map(|trade| {
                        let mut tick = parse_spot_trade_tick(trade, market, &instrument, ts_init)?;
                        tick.ts_init = tick.ts_event;
                        Ok(tick)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                ticks.reverse();
                Ok::<_, anyhow::Error>((instrument, ticks))
            })
        })
        .map_err(to_pyvalue_err)?;
    Ok((instrument_any_to_pyobject(py, instrument)?, ticks))
}
