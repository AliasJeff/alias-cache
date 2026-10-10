# Build on the toolchain the CI pins so release images match tested code.
FROM rust:1.88-slim AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY alias-cache ./alias-cache
COPY alias-cache-cli ./alias-cache-cli
RUN cargo build --release --locked -p alias-cache-cli

FROM debian:bookworm-slim
# CA certificates back the TLS client/server paths; the binary itself is
# dynamically linked against glibc which bookworm provides.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/alias-cache-cli /usr/local/bin/alias-cache
RUN useradd --system --no-create-home alias-cache
USER alias-cache
EXPOSE 6379 16379
ENTRYPOINT ["alias-cache"]
