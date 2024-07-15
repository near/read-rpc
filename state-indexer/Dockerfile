FROM rust:1.78 AS builder
ARG features="default"
ARG profile="release"
WORKDIR /tmp/

COPY .cargo .cargo
COPY Cargo.lock ./
COPY Cargo.toml ./
COPY config.toml ./
RUN sed '/perf-testing/d; /rpc-server/d; /tx-indexer/d; /near-state-indexer/d; /tx-details-storage/d; /cache-storage/d' Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
COPY state-indexer/Cargo.toml state-indexer/Cargo.toml
COPY configuration configuration
COPY database database
COPY readnode-primitives readnode-primitives
COPY logic-state-indexer logic-state-indexer
RUN mkdir state-indexer/src && echo 'fn main() {}' > state-indexer/src/main.rs cargo build --release && rm -r state-indexer/src

# copy your source tree
COPY state-indexer/src ./state-indexer/src

# build for release
RUN cargo build --profile "$profile" --no-default-features --features "$features"

FROM ubuntu:24.04
ARG profile="release"
RUN apt update && apt install -yy openssl ca-certificates libcurl4 libpq5
COPY --from=builder /tmp/target/$profile/state-indexer .
COPY --from=builder /tmp/config.toml .
ENTRYPOINT ["./state-indexer"]
