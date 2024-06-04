FROM rust:1.77 AS builder
ARG features="default"
WORKDIR /tmp/

COPY Cargo.lock ./
COPY Cargo.toml ./
COPY config.toml ./
RUN sed '/perf-testing/d; /rpc-server/d; /tx-indexer/d; /near-state-indexer/d; /state-indexer/d' Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
COPY scylla-indexer/Cargo.toml scylla-indexer/Cargo.toml
COPY configuration configuration
COPY database database
COPY readnode-primitives readnode-primitives
COPY epoch-indexer epoch-indexer
RUN mkdir scylla-indexer/src && echo 'fn main() {}' > scylla-indexer/src/main.rs cargo build --release && rm -r scylla-indexer/src

# copy your source tree
COPY scylla-indexer/src ./scylla-indexer/src

# build for release
RUN cargo build --release --no-default-features --features "$features"

FROM ubuntu:22.04
RUN apt update && apt install -yy openssl ca-certificates libcurl4 libpq5
COPY --from=builder /tmp/target/release/scylla-indexer .
COPY --from=builder /tmp/config.toml .
ENTRYPOINT ["./scylla-indexer"]
