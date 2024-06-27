# Epoch indexer

This indexer is responsible for indexing the epoch changes and storing them in the database.
We're indexing the following epoch changes:
    1. Validator set changes
    2. Protocol config changes

This indexer is intended for collecting the history of epochs and should not be run permanently and parallel with the state_indexer.
The `state-indexer` will monitor the network and collect all new epochs.
**If you run state-indexer from the genesis block, you don't need to run epoch-indexer to collect the history of epochs.**

## Create `.env` file in the project root

```
INDEXER_ID=epoch-indexer-1
DATABASE_URL=127.0.0.1:9042
DATABASE_USER=cassandra
DATABASE_PASSWORD=cassandra
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=eu-central-1
```

### Features

### Command to run

```
cargo run --release -- <chain_id> <start_options>
```

- `chain_id` (\*) `testnet` or `mainnet`
- `start_options`:
    - `from-genesis` start indexing epoch changes from the genesis block.
    - `from-interruption ` is used to retrieve the `last_processed_block_height` from the database. This value is used as the starting point for processing blocks.
