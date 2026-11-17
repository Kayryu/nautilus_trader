# Build the binary natively from the repository root first:
# cargo build --release --locked \
#   --manifest-path examples/live/deepx/mev/Cargo.toml --target-dir target
# Then build the runtime image:
# docker build -f examples/live/deepx/mev/Dockerfile -t deepx-mev .
#
# Run the read-only observer:
# docker run --rm --init deepx-mev \
#   https://rpc-testnet.deepx.fi EXPECTED_GENESIS_HASH MARKET 1 250 --once

FROM debian:trixie-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin deepx

COPY target/release/deepx-mev \
    /usr/local/bin/deepx-mev

RUN mkdir --parents /var/lib/deepx-mev \
    && chown deepx:deepx /var/lib/deepx-mev

USER deepx
WORKDIR /var/lib/deepx-mev

ENV RUST_LOG=info

VOLUME ["/var/lib/deepx-mev"]
ENTRYPOINT ["/usr/local/bin/deepx-mev"]