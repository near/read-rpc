FROM rust:1.73 AS builder
ARG features="default"
WORKDIR /tmp/

COPY Cargo.lock ./
COPY Cargo.toml ./
RUN sed '/perf-testing/d; /rpc-server/d; /tx-indexer/d' Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
COPY state-indexer/Cargo.toml state-indexer/Cargo.toml
COPY database database
COPY readnode-primitives readnode-primitives
COPY epoch-indexer epoch-indexer
RUN mkdir state-indexer/src && echo 'fn main() {}' > state-indexer/src/main.rs cargo build --release && rm -r state-indexer/src

# copy your source tree
COPY state-indexer/src ./state-indexer/src

# build for release
RUN cargo build --release --features "$features"

FROM ubuntu:22.04
RUN apt update && apt install -yy openssl ca-certificates
COPY --from=builder /tmp/target/release/state-indexer .
ENTRYPOINT ["./state-indexer"]
