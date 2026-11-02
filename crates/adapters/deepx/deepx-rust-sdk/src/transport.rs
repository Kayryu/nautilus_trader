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
