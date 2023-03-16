FROM rust:1.67 AS builder
WORKDIR /tmp/

COPY Cargo.lock ./
RUN echo '[workspace]\nmembers = ["rpc-server", "database"]' > Cargo.toml
COPY rpc-server/Cargo.toml rpc-server/Cargo.toml
COPY database database
RUN mkdir rpc-server/src && echo 'fn main() {}' > rpc-server/src/main.rs cargo build --release && rm -r rpc-server/src

# copy your source tree
COPY rpc-server/src ./rpc-server/src

# build for release
RUN cargo build --release

FROM ubuntu:20.04
RUN apt update && apt install -yy openssl ca-certificates
COPY --from=builder /tmp/target/release/json-rpc-100x .
ENTRYPOINT ["./json-rpc-100x"]
