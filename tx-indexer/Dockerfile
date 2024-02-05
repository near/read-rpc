FROM rust:1.73 AS builder
ARG features="default"
WORKDIR /tmp/

COPY Cargo.lock ./
COPY Cargo.toml ./
COPY config.toml ./
RUN sed '/perf-testing/d; /rpc-server/d; /state-indexer/d; /epoch-indexer/d' Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
COPY tx-indexer/Cargo.toml tx-indexer/Cargo.toml
COPY configuration configuration
COPY database database
COPY readnode-primitives readnode-primitives
RUN mkdir tx-indexer/src && echo 'fn main() {}' > tx-indexer/src/main.rs cargo build --release && rm -r tx-indexer/src

# copy your source tree
COPY tx-indexer/src ./tx-indexer/src

# build for release
RUN cargo build --release --no-default-features --features "$features"

FROM ubuntu:22.04
RUN apt update && apt install -yy openssl ca-certificates libcurl4 libpq5
COPY --from=builder /tmp/target/release/tx-indexer .
COPY --from=builder /tmp/config.toml .
ENTRYPOINT ["./tx-indexer"]
