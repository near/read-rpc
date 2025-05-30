FROM rust:1.85 AS builder
ARG features="default"
ARG profile="release"
WORKDIR /tmp/

COPY .cargo .cargo
COPY Cargo.lock ./
COPY Cargo.toml ./
RUN sed '/perf-testing/d; /state-indexer/d; /tx-indexer/d; /near-state-indexer/d; /logic-state-indexer/d' Cargo.toml > Cargo.toml.new && mv Cargo.toml.new Cargo.toml
COPY rpc-server/Cargo.toml rpc-server/Cargo.toml
COPY configuration configuration
COPY database database
COPY readnode-primitives readnode-primitives
COPY tx-details-storage tx-details-storage
COPY cache-storage cache-storage
RUN apt update && apt install -yy llvm \
    clang libssl-dev
RUN mkdir rpc-server/src && echo 'fn main() {}' > rpc-server/src/main.rs cargo build --release && rm -r rpc-server/src

# copy your source tree
COPY rpc-server/build.rs rpc-server/build.rs
COPY rpc-server/src ./rpc-server/src

# build for release
RUN cargo build --profile "$profile" --no-default-features --features "$features"

FROM ubuntu:24.04
ARG profile="release"
RUN apt update && apt install -yy openssl ca-certificates libcurl4 libpq5
COPY --from=builder /tmp/target/$profile/read-rpc-server .
ENTRYPOINT ["./read-rpc-server"]
