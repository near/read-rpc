# state-indexer

The indexer built on top of Lake Framework that watches the network and stores the `StateChanges` into the Storage (ScyllaDB) using the designed data schemas.

## Create `.env` file in the project root

```
INDEXER_ID=state-indexer-1
DATABASE_URL=127.0.0.1:9042
DATABASE_USER=cassandra
DATABASE_PASSWORD=cassandra
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=eu-central-1
```

### Features
```
scylla_db - enables ScyllaDB support (default)
```
See the [documentation here](../database/README.md)

### Command to run

```
cargo run --release -- <chain_id> <start_options>
```

- `chain_id` (\*) `testnet` or `mainnet`
- `start_options`:
    - `from-latest` fetches the final block height from the RPC and starts indexing from that block
    - `from-interruption <N?>` is used to retrieve the `last_processed_block_height` from the Scylla database. This value is used as the starting point for processing blocks. If a specific value `<N?>` is provided, it will be used as the fallback option. If `<N?>` is not provided or if the database does not have a record (for example, in the case of a fresh start with an empty storage), the fallback option will be `from-latest`.
    - `from-block <N>` starts indexing from the block height `<N>`


