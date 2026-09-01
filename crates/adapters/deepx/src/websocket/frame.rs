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

//! Schema-conservative decoding for inbound DeepX WebSocket text frames.

use serde_json::Value;

use super::{DeepXWsError, DeepXWsRequestId};

/// A JSON frame classified without assuming DeepX channel or business schemas.
#[derive(Clone, Debug, PartialEq)]
pub enum DeepXWsFrame {
    /// A frame with an unsigned numeric request correlator.
    Correlated {
        /// Request ID found at the top-level `id` field.
        id: DeepXWsRequestId,
        /// Complete decoded frame retained for the registered waiter.
        value: Value,
    },
    /// Valid JSON without a proven numeric request correlator.
    Unknown(Value),
}

impl DeepXWsFrame {
    /// Decodes a text frame exactly once and performs only evidence-independent classification.
    ///
    /// # Errors
    ///
    /// Returns [`DeepXWsError::InvalidJsonFrame`] when `text` is not valid JSON.
    pub fn parse(text: &str) -> Result<Self, DeepXWsError> {
        let value: Value = serde_json::from_str(text)
            .map_err(|error| DeepXWsError::InvalidJsonFrame(error.to_string()))?;
        let id = value
            .as_object()
            .and_then(|object| object.get("id"))
            .and_then(Value::as_u64);

        Ok(match id {
            Some(id) => Self::Correlated {
                id: DeepXWsRequestId::new(id),
                value,
            },
            None => Self::Unknown(value),
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn parses_numeric_request_id_without_discarding_frame() {
        let frame = DeepXWsFrame::parse(r#"{"id":7,"result":{"ok":true}}"#).unwrap();

        assert_eq!(
            frame,
            DeepXWsFrame::Correlated {
                id: DeepXWsRequestId::new(7),
                value: json!({"id": 7, "result": {"ok": true}}),
            },
        );
    }

    #[rstest]
    #[case(r#"{"channel":"trades","data":[]}"#)]
    #[case(r#"{"id":"server-defined"}"#)]
    #[case(r#"["valid", "unknown", "frame"]"#)]
    #[case(r#""heartbeat""#)]
    fn preserves_valid_unknown_json(#[case] text: &str) {
        assert!(matches!(
            DeepXWsFrame::parse(text).unwrap(),
            DeepXWsFrame::Unknown(_),
        ));
    }

    #[rstest]
    #[case("")]
    #[case("not-json")]
    #[case(r#"{"id":1"#)]
    fn rejects_malformed_json_without_panicking(#[case] text: &str) {
        assert!(matches!(
            DeepXWsFrame::parse(text),
            Err(DeepXWsError::InvalidJsonFrame(_)),
        ));
    }
}
