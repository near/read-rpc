# json-rpc-100x

## Create `.env` file in the project root
```
AWS_ACCESS_KEY_ID=ACCESS_KEY
AWS_SECRET_ACCESS_KEY=SECRET_ACCESS_KEY
AWS_DEFAULT_REGION=eu-central-1
AWS_BUCKET_NAME=buket_name
NEAR_RPC_URL=https://rpc.testnet.near.org
SCYLLA_URL=127.0.0.1:9042
SCYLLA_KEYSPACE=key_space_name
SCYLLA_USER=username
SCYLLA_PASSWORD=password
SCYLLA_KEEPALIVE_INTERVAL=60
SERVER_PORT=8888
RUST_LOG=info
```

### Compile

```bash
$ cargo build --release
```

### Run

#### Logging
To run an instance of Jaeger, you can use Docker:
```bash
$ docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 -p14268:14268 jaegertracing/all-in-one:latest
```
You can visit the Jaeger page by going to http://localhost:16686

#### Run RPC service 
```bash
$ ./target/release/json-rpc-100x
```

* mainnet https://rpc.mainnet.near.org
* testnet https://rpc.testnet.near.org
* betanet https://rpc.betanet.near.org (may be unstable)
* localnet http://localhost:3030

### NEAR RPC API
```asm
https://docs.near.org/api/rpc/introduction
```
