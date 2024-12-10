FROM rust:1.82 AS builder
ARG features="default"
ARG profile="release"
WORKDIR /tmp/

COPY .cargo .cargo
COPY Cargo.lock ./
COPY Cargo.toml ./
RUN sed '/perf-testing/d; /rpc-server/d; /state-indexer/d; /near-state-indexer/d; /logic-state-indexer/d' Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
COPY tx-indexer/Cargo.toml tx-indexer/Cargo.toml
COPY configuration configuration
COPY database database
COPY readnode-primitives readnode-primitives
COPY tx-details-storage tx-details-storage
COPY cache-storage cache-storage
RUN mkdir tx-indexer/src && echo 'fn main() {}' > tx-indexer/src/main.rs cargo build --release && rm -r tx-indexer/src

# copy your source tree
COPY tx-indexer/src ./tx-indexer/src

# build for release
RUN cargo build --profile "$profile" --no-default-features --features "$features"

FROM ubuntu:24.04
ARG profile="release"
RUN apt update && apt install -yy openssl ca-certificates libcurl4 libpq5
COPY --from=builder /tmp/target/$profile/tx-indexer .
ENTRYPOINT ["./tx-indexer"]
