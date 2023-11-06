# Read RPC

This workspace holds the collection of components for the Read RPC.

## Current content

### [rpc-server](rpc_server/README.md)


The JSON RPC server implementation that repeats all the APIs current real NEAR JSON RPC but using a different data sources:
- The Read RPC Storage (ScyllaDB currently)
- NEAR Lake Data buckets (AWS S3 currently)
- real NEAR JSON RPC

### [state-indexer](state-indexer/README.md)

The indexer built on top of Lake Framework that watches the network and stores the `StateChanges` into the Storage (ScyllaDB) using the designed data schemas.

### [tx-indexer](tx-indexer/README.md)

The indexer built on top of Lake Framework that watches the network and stores the `Transactions` along with all the related entities (`Receipts`, `ExecutionOutcomes`) into the Storage (ScyllaDB) using the specifically defined `TransactionDetails` structure in a dumped way (using the simplest key-value schema)


## Docker compose

**Note!** The docker compose is not fully ready yet. It's still in progress. However, you can run the entire project to play around with it. It is still not convenient for development or debugging purposes. We are working on improving it.

### Run the entire project

Add the `.env` file (update the credentials with your own to access the S3 bucket)

```
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=eu-central-1
```

Run the docker compose:

```
$ docker-compose up
```

This will spin up:
- `read-rpc-server` - the JSON RPC server
- `state-indexer` - the indexer that watches the network and stores the `StateChanges` into the Storage (ScyllaDB) using the designed data schemas.
- `tx-indexer` - the indexer that watches the network and stores the `Transactions` along with all the related entities (`Receipts`, `ExecutionOutcomes`) into the Storage (ScyllaDB) using the specifically defined `TransactionDetails` structure in a dumped way (using the simplest key-value schema)
- `jaeger` - the Jaeger instance for tracing (http://localhost:16686)

### TODO:

- [ ] Setup all the services to use volumes for the codebase and using `cargo watch` to recompile the code on the fly
- [ ] Come up with the way of changin the configuration for the indexers in a convenient way
- [ ] Robust documentation for the docker compose setup


## Development

### Check the entire product

```
cargo check
```

### Fmt the entire product

```
cargo fmt --all
```

### Run a specific package

```
cargo run --package <package_name>
```

Example:

```
cargo run --release --package rpc-server
```

**Please don't forget** to handle `.env` files properly if you're running the package from the root using the proposed approach.

#### Keep in mind

You can move to the folder with a package

```
cd rpc-server/
```

And run a package as usually from the folder

```
cargo run --release
```

**In this case** you would want to have an `.env` file in the package folder.

### Tracing

See the tracing documentation [here](./docs/TRACING.md)