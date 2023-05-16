# Read RPC

This workspace holds the collection of components for the Read RPC.

## Current content

### `rpc-server`

The JSON RPC server implementation that repeats all the APIs current real NEAR JSON RPC but using a different data sources:
- The Read RPC Storage (ScyllaDB currently)
- NEAR Lake Data buckets (AWS S3 currently)
- real NEAR JSON RPC

### `state-indexer`

The indexer built on top of Lake Framework that watches the network and stores the `StateChanges` into the Storage (ScyllaDB) using the designed data schemas.

### `tx-indexer`

The indexer built on top of Lake Framework that watches the network and stores the `Transactions` along with all the related entities (`Receipts`, `ExecutionOutcomes`) into the Storage (ScyllaDB) using the specifically defined `TransactionDetails` structure in a dumped way (using the simplest key-value schema)


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
