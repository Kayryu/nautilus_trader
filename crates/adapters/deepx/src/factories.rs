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

//! Factory functions for creating DeepX clients.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{common::consts::DEEPX, config::DeepXDataClientConfig, data::DeepXDataClient};

impl ClientConfig for DeepXDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating DeepX data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.deepx", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.deepx")
)]
pub struct DeepXDataClientFactory;

impl DeepXDataClientFactory {
    /// Creates a new [`DeepXDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for DeepXDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for DeepXDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let deepx_config = config
            .as_any()
            .downcast_ref::<DeepXDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for DeepXDataClientFactory. \
                     Expected DeepXDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = DeepXDataClient::new(client_id, deepx_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        DEEPX
    }

    fn config_type(&self) -> &'static str {
        "DeepXDataClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::{
        cache::Cache,
        clock::TestClock,
        factories::{ClientConfig, DataClientFactory},
        live::runner::replace_data_event_sender,
        messages::DataEvent,
    };
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn data_factory_reports_adapter_identity() {
        let factory = DeepXDataClientFactory::new();

        assert_eq!(factory.name(), DEEPX);
        assert_eq!(factory.config_type(), "DeepXDataClientConfig");
    }

    #[rstest]
    fn data_config_supports_factory_downcast() {
        let config = DeepXDataClientConfig::default();
        let erased: &dyn ClientConfig = &config;

        assert!(
            erased
                .as_any()
                .downcast_ref::<DeepXDataClientConfig>()
                .is_some()
        );
    }

    #[rstest]
    fn data_factory_creates_deepx_client() {
        let factory = DeepXDataClientFactory::new();
        let config = DeepXDataClientConfig::default();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(sender);

        let client = factory
            .create("DEEPX-TEST", &config, cache.into(), clock)
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("DEEPX-TEST"));
        assert_eq!(client.venue(), Some(*crate::DEEPX_VENUE));
    }

    #[derive(Debug)]
    struct WrongConfig;

    impl ClientConfig for WrongConfig {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[rstest]
    fn data_factory_rejects_wrong_config_type() {
        let factory = DeepXDataClientFactory::new();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let result = factory.create("DEEPX-TEST", &WrongConfig, cache.into(), clock);
        let error = match result {
            Ok(_) => panic!("expected invalid config type error"),
            Err(e) => e,
        };

        assert!(
            error
                .to_string()
                .contains("Invalid config type for DeepXDataClientFactory")
        );
    }
}
