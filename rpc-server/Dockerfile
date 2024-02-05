FROM rust:1.73 AS builder
ARG features="default"
WORKDIR /tmp/

COPY Cargo.lock ./
COPY Cargo.toml ./
COPY config.toml ./
RUN sed '/perf-testing/d; /state-indexer/d; /tx-indexer/d; /epoch-indexer/d' Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
COPY rpc-server/Cargo.toml rpc-server/Cargo.toml
COPY configuration configuration
COPY database database
COPY readnode-primitives readnode-primitives
RUN mkdir rpc-server/src && echo 'fn main() {}' > rpc-server/src/main.rs cargo build --release && rm -r rpc-server/src

# copy your source tree
COPY rpc-server/src ./rpc-server/src

# build for release
RUN cargo build --release --no-default-features --features "$features"

FROM ubuntu:22.04
RUN apt update && apt install -yy openssl ca-certificates libcurl4 libpq5
COPY --from=builder /tmp/target/release/read-rpc-server .
COPY --from=builder /tmp/config.toml .
ENTRYPOINT ["./read-rpc-server"]
