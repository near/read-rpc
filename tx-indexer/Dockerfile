FROM rust:1.73 AS builder
ARG features="default"
WORKDIR /tmp/

COPY Cargo.lock ./
RUN echo '[workspace]\nmembers = ["tx-indexer", "database", "readnode-primitives"]' > Cargo.toml
COPY tx-indexer/Cargo.toml tx-indexer/Cargo.toml
COPY database database
COPY readnode-primitives readnode-primitives
RUN mkdir tx-indexer/src && echo 'fn main() {}' > tx-indexer/src/main.rs cargo build --release && rm -r tx-indexer/src

# copy your source tree
COPY tx-indexer/src ./tx-indexer/src

# build for release
RUN cargo build --release --features "$features"

FROM ubuntu:20.04
RUN apt update && apt install -yy openssl ca-certificates libcurl4
COPY --from=builder /tmp/target/release/tx-indexer .
ENTRYPOINT ["./tx-indexer"]
