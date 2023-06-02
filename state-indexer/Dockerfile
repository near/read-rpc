FROM rust:1.70 AS builder
ARG features=""
WORKDIR /tmp/

COPY Cargo.lock ./
RUN echo '[workspace]\nmembers = ["state-indexer", "database"]' > Cargo.toml
COPY state-indexer/Cargo.toml state-indexer/Cargo.toml
COPY database database
RUN mkdir state-indexer/src && echo 'fn main() {}' > state-indexer/src/main.rs cargo build --release && rm -r state-indexer/src

# copy your source tree
COPY state-indexer/src ./state-indexer/src

# build for release
RUN cargo build --release --features "$features"

FROM ubuntu:20.04
RUN apt update && apt install -yy openssl ca-certificates
COPY --from=builder /tmp/target/release/state-indexer .
ENTRYPOINT ["./state-indexer"]
