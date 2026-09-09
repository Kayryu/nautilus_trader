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

//! Factories for creating DeepX data and execution clients.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId},
};

use crate::{
    common::consts::{DEEPX, DEEPX_VENUE},
    config::{DeepXDataClientConfig, DeepXExecutionClientConfig},
    data::DeepXDataClient,
    execution::DeepXExecutionClient,
};

impl ClientConfig for DeepXDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for DeepXExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating disconnected DeepX execution clients.
#[derive(Clone, Debug, Default)]
pub struct DeepXExecutionClientFactory;

/// Factory for creating disconnected DeepX data clients.
#[derive(Clone, Debug, Default)]
pub struct DeepXDataClientFactory;

impl DeepXDataClientFactory {
    /// Creates a new [`DeepXDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for DeepXDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<DeepXDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for DeepXDataClientFactory. Expected DeepXDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        Ok(Box::new(DeepXDataClient::new(
            ClientId::from(name),
            config,
            cache,
            clock,
        )?))
    }

    fn name(&self) -> &'static str {
        DEEPX
    }

    fn config_type(&self) -> &'static str {
        stringify!(DeepXDataClientConfig)
    }
}

impl DeepXExecutionClientFactory {
    /// Creates a new [`DeepXExecutionClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for DeepXExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<DeepXExecutionClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for DeepXExecutionClientFactory. Expected DeepXExecutionClientConfig, was {config:?}",
                )
            })?
            .clone();
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *DEEPX_VENUE,
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache,
        );

        Ok(Box::new(DeepXExecutionClient::new(core, config)?))
    }

    fn name(&self) -> &'static str {
        DEEPX
    }

    fn config_type(&self) -> &'static str {
        "DeepXExecutionClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use nautilus_common::{
        cache::Cache,
        clock::TestClock,
        factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
    };
    use nautilus_model::identifiers::{AccountId, TraderId};
    use rstest::rstest;

    use super::*;
    use crate::config::DeepXNetworkConfig;

    #[derive(Debug)]
    struct WrongConfig;

    impl ClientConfig for WrongConfig {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn test_config() -> DeepXExecutionClientConfig {
        DeepXExecutionClientConfig::builder()
            .account_id(AccountId::from("DEEPX-001"))
            .subaccount_id("test-subaccount".to_string())
            .private_key(
                "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            )
            .network(DeepXNetworkConfig::default())
            .build()
    }

    #[rstest]
    fn factory_creates_disconnected_framework_client() {
        let factory = DeepXExecutionClientFactory::new();
        let cache = Rc::new(RefCell::new(Cache::default()));

        let client = factory
            .create(
                TraderId::from("TRADER-001"),
                "DEEPX-TEST",
                &test_config(),
                cache.into(),
            )
            .unwrap();

        assert_eq!(factory.name(), DEEPX);
        assert_eq!(factory.config_type(), "DeepXExecutionClientConfig");
        assert_eq!(client.client_id(), ClientId::from("DEEPX-TEST"));
        assert_eq!(client.account_id(), AccountId::from("DEEPX-001"));
        assert_eq!(client.venue(), *DEEPX_VENUE);
        assert_eq!(client.oms_type(), OmsType::Netting);
        assert!(!client.is_connected());
    }

    #[rstest]
    fn factory_rejects_wrong_config_type() {
        let cache = Rc::new(RefCell::new(Cache::default()));

        let error = DeepXExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "DEEPX-TEST",
                &WrongConfig,
                cache.into(),
            )
            .err()
            .unwrap();

        assert!(error.to_string().contains("Invalid config type"));
    }

    #[rstest]
    fn factory_rejects_invalid_deepx_config() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut config = test_config();
        config.subaccount_id = None;

        let result = DeepXExecutionClientFactory::new().create(
            TraderId::from("TRADER-001"),
            "DEEPX-TEST",
            &config,
            cache.into(),
        );

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn data_factory_creates_disconnected_fail_closed_client() {
        let factory = DeepXDataClientFactory::new();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let mut client = factory
            .create(
                "DEEPX-DATA",
                &DeepXDataClientConfig::default(),
                cache.into(),
                clock,
            )
            .unwrap();

        assert_eq!(factory.name(), DEEPX);
        assert_eq!(factory.config_type(), "DeepXDataClientConfig");
        assert_eq!(client.client_id(), ClientId::from("DEEPX-DATA"));
        assert_eq!(client.venue(), Some(*DEEPX_VENUE));
        assert!(client.is_disconnected());
        let error = client.connect().await.unwrap_err();
        assert!(error.to_string().contains("fixture-proven"));
        assert!(client.is_disconnected());
    }

    #[rstest]
    fn data_factory_rejects_wrong_config_type() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let result =
            DeepXDataClientFactory::new().create("DEEPX-DATA", &WrongConfig, cache.into(), clock);

        assert!(result.is_err());
    }
}
