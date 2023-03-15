# TX indexer

This is a micro-indexer built on top of [NEAR Lake Framework](https://github.com/near/near-lake-framework-rs).

The purpose of this indexer is to watch for every single `Transaction` in NEAR Protocol and collect all the children entities related to the `Transaction` into a single structure `TransactionDetails`.


## `TransactionDetails` definition

TODO: Update the README once the structure is finalized.

ref: [`struct TransactionDetails`](./src/types.rs)


## How it works

An instance of `tx-indexer` requires to have a dedicated `Redis` instance for the cache.

The `tx-indexer` works like any other Lake-based indexer watching the network.

In order to collect a `Transactions` with all related entities it does the following:

1. Converts `Transaction` into a `tx-indexer`-defined structure `TransactionDetails`
2. Stores the data into cache (Redis)
3. Watched for `ExecutionOutcomes` and its statuses. Depending on the status:
    - `SuccessValue` means the `Receipt` "branch" has finished successfully
    - `SuccessReceiptId` means a new `Receipt` branch has started and we need to follow it
    - Other error/failure statuses means the `Receipt` branch has finished with an error
4. Whenever we observe a new `Receipt` branch we increment the `Transaction`'s anticipated `ExecutionOutcomes`
5. Whenever a `Receipt` branch is finishes we decrement the `Transaction`'s anticipated `ExecutionOutcomes`
6. We add related `Receipts` and `ExecutionOutcomes` to the corresponding fields of the `TransactionDetails` during the indexing
7. Once the anticipated `ExecutionOutcomes` counter becomes zero we consider the `TransactionDetails` collecting as over and can serialize and store in in the Storage (ScyllaDB)

## How to run

### Prerequisites

In order to run we need to provide the necessary parameters, it can be done either via command line arguments or via environmental variables.

Start the Redis instance for the `tx-indexer` instance. You can use the Docker command for that:
```bash
$ docker run --name redis -p 6379:6379 -d redis redis-server --save 60 1 --loglevel warning
```
An example `.env` file:

```
REDIS_CONNECTION_STRING=redis://127.0.0.1
INDEXER_ID=tx-indexer
SCYLLA_URL=127.0.0.1:9042
SCYLLA_KEYSPACE=transactions
SCYLLA_USER=admin
SCYLLA_PASSWORD=password

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=eu-central-1
```

### Command to run

```
cargo run --release -- <chain_id> <start_options>
```

- `chain_id` (\*) `testnet` or `mainnet`
- `start_options`:
    - `from-latest` fetches the final block height from the RPC and starts indexing from that block
    - `from-interruption` will fetch the `last_processed_block_height` from Redis in order to start from that block (will fallback to `from-latest` if Redis doesn't have a record, for instance in case of fresh start with empty Redis storage)
    - `from-block <N>` starts indexing from the block height `<N>`

