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

//! Shared bounds for DeepX HTTP transports.

use anyhow::Context;

const MAX_RESPONSE_BYTES: usize = 100 * 1024 * 1024;

pub(crate) fn install_tls_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

pub(crate) async fn read_bounded_body(mut response: reqwest::Response) -> anyhow::Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read DeepX response")?
    {
        let length = body
            .len()
            .checked_add(chunk.len())
            .context("DeepX response length overflow")?;
        anyhow::ensure!(
            length <= MAX_RESPONSE_BYTES,
            "DeepX response exceeds 100 MiB"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
