# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Test DeepX config and factory boundaries.
"""

import pytest

from nautilus_trader.adapters.deepx import DEEPX
from nautilus_trader.adapters.deepx import DEEPX_CLIENT_ID
from nautilus_trader.adapters.deepx import DEEPX_VENUE
from nautilus_trader.adapters.deepx import DeepXDataClientConfig
from nautilus_trader.adapters.deepx import DeepXDataClientFactory
from nautilus_trader.adapters.deepx import DeepXExecutionClientConfig
from nautilus_trader.adapters.deepx import DeepXExecutionClientFactory
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.model import ClientId
from nautilus_trader.model import Venue


def test_deepx_exports_canonical_identity() -> None:
    """
    Test DeepX exports canonical client and venue identifiers.
    """
    assert DEEPX == "DEEPX"
    assert ClientId.from_str(DEEPX) == DEEPX_CLIENT_ID
    assert Venue.from_str(DEEPX) == DEEPX_VENUE


def test_deepx_factories_expose_python_names() -> None:
    """
    Test DeepX factories expose Python names.
    """
    assert DeepXDataClientFactory().name() == DEEPX
    assert DeepXExecutionClientFactory().name() == DEEPX


def test_deepx_configs_default_to_testnet() -> None:
    """
    Test DeepX configs default to the testnet deployment.
    """
    network = DeepXNetworkConfig()
    data_config = DeepXDataClientConfig(network=network)

    assert network.environment == "testnet"
    assert data_config.network.environment == "testnet"


def test_deepx_network_config_rejects_mainnet() -> None:
    """
    Test DeepX network config rejects mainnet.
    """
    with pytest.raises(ValueError, match="mainnet"):
        DeepXNetworkConfig(environment="mainnet")


def test_deepx_execution_config_rejects_unknown_backend() -> None:
    """
    Test DeepX execution config rejects an unknown backend.
    """
    with pytest.raises(ValueError, match="unsupported DeepX execution backend"):
        DeepXExecutionClientConfig(
            subaccount_id="test-subaccount",
            execution_backend="automatic",
        )


def test_deepx_execution_config_repr_redacts_private_key() -> None:
    """
    Test DeepX execution config repr redacts its private key.
    """
    private_key = "0000000000000000000000000000000000000000000000000000000000000001"
    config = DeepXExecutionClientConfig(
        subaccount_id="test-subaccount",
        private_key=private_key,
    )

    assert config.has_private_key
    assert "<redacted>" in repr(config)
    assert private_key not in repr(config)
