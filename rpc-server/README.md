# read-rpc-server

## Create `.env` file in the project root
```
AWS_ACCESS_KEY_ID=ACCESS_KEY
AWS_SECRET_ACCESS_KEY=SECRET_ACCESS_KEY
AWS_DEFAULT_REGION=eu-central-1
AWS_BUCKET_NAME=buket_name
NEAR_RPC_URL=https://rpc.testnet.near.org
DATABASE_URL=127.0.0.1:9042
DATABASE_USER=username
DATABASE_PASSWORD=password
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
Run project with command to get metrics
```bash
cargo run --release --features tracing-instrumentation
```
You can visit the Jaeger page by going to http://localhost:16686

#### Run RPC service
```bash
$ ./target/release/read-rpc-server
```

* mainnet https://rpc.mainnet.near.org
* testnet https://rpc.testnet.near.org
* betanet https://rpc.betanet.near.org (may be unstable)
* localnet http://localhost:3030

### NEAR RPC API
```asm
https://docs.near.org/api/rpc/introduction
```

## Feature flags

### `send_tx_methods` (default: `true`)

This feature flag enabled the methods for sending transactions to the network. This includes the following methods:

- `broadcast_tx_commit`
- `broadcast_tx_async`

Might be useful to disable if you want to run a totally read-only node that doesn't even proxies transactions to the network.

**Note** the methods will be still available in the API, but will return an error if the feature flag is disabled.

### `tracing-instrumentation` (default: `false`)

This feature flag enables the tracing instrumentation for the RPC server. See the [Logging](#logging) section for more details.

### `scylla_db` (default: `true`)

See documentation [here](../database/README.md)

### `scylla_db_tracing` (default: `false`)

This feature flag enables the tracing instrumentation for the ScyllaDB client ([`database` crate](../database)). Database tracing means additional debug-level logs for all the database queries.

### `shadow_data_consistency` (default: `false`)

This feature flag enables the shadow data consistency checks. With this feature on, all the queries to the read-rpc-server will be executed twice: once against the main database, and once against the real NEAR RPC. If the results are different, warning-level logs are emitted. **Note** we've decided that read-rpc-server will return its own response if the results are incorrect to make the debugging easier.

**Warning** This feature is created for the early-stage of lunching the read-rpc-server to catch all the bugs and inconsistencies. You don't need this feature if you are not a contributor to the read-rpc-server.

### `account_access_keys` (default: `false`)

We encountered a problem with the design of the table `account_access_keys` and overall design of the logic around it. We had to disable the table and proxy the calls of the `query.access_key_list` method to the real NEAR RPC. However, we still aren't ready to get rid of the code and that's why we hid it under the feature flag. We are planning to remove the code in the future and remove the feature flag.

## Metrics (Prometheus)

The read-rpc-server exposes Prometheus-compatible metrics at the `/metrics` endpoint.

All the metrics are defined in `src/metrics.rs` file. Two main categories of metrics are:

- Total number of requests for a specific method
- Number of errors for a specific method (works only if the `shadow_data_consistency` feature flag is enabled)

The latter is split by the suffix code to differentiate the reason for the error when possible:

- **0** - ReadRPC returns a Success result, and NEAR RPC returns a Success result, but the results don't match
- **1** - ReadRPC returns Success result, and NEAR RPC returns an Error result
- **2** - ReadRPC returns an Error result, and NEAR RPC returns Success result
- **3** - ReadRPC returns an Error result, and NEAR RPC returns an Error result, but the results don't match
- **4** - Could not perform consistency check because of the error (either network or parsing the results)

For example, method `block` will have these metrics:

- `BLOCK_REQUESTS_TOTAL`
- `BLOCK_ERROR_0`
- `BLOCK_ERROR_1`
- `BLOCK_ERROR_2`
- `BLOCK_ERROR_3`
- `BLOCK_ERROR_4`