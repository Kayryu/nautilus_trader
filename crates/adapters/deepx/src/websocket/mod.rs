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

//! Transport-neutral WebSocket protocol state for DeepX testnet.

pub mod error;
pub mod frame;
pub mod handler;
pub mod protocol;
pub mod task;

pub use error::DeepXWsError;
pub use frame::DeepXWsFrame;
pub use handler::{
    DeepXWsProtocolHandle, DeepXWsProtocolHandler, DeepXWsRegisteredRequest,
    deepx_ws_protocol_handler,
};
pub use protocol::{DeepXWsProtocolCore, DeepXWsRequest, DeepXWsRequestId};
pub use task::{DeepXWsTaskHandles, DeepXWsTaskOutcome};
