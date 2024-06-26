# TX indexer

This is a micro-indexer built on top of [NEAR Lake Framework](https://github.com/near/near-lake-framework-rs).

The purpose of this indexer is to watch for every single `Transaction` in NEAR Protocol and collect all the children entities related to the `Transaction` into a single structure `TransactionDetails`.


## `TransactionDetails` definition

TODO: Update the README once the structure is finalized.

ref: [`struct TransactionDetails`](../readnode-primitives/src/lib.rs)


## How it works

The `tx-indexer` works like any other Lake-based indexer watching the network.

In order to collect a `Transactions` with all related entities it does the following:

1. Converts `Transaction` into a `tx-indexer`-defined structure `TransactionDetails`
2. Stores the data into cache
3. Watched for `ExecutionOutcomes` and its statuses. Depending on the status:
    - `SuccessValue` means the `Receipt` "branch" has finished successfully
    - `SuccessReceiptId` means a new `Receipt` branch has started and we need to follow it
    - Other error/failure statuses means the `Receipt` branch has finished with an error
4. Whenever we observe a new `Receipt` branch we increment the `Transaction`'s anticipated `ExecutionOutcomes`
5. Whenever a `Receipt` branch is finishes we decrement the `Transaction`'s anticipated `ExecutionOutcomes`
6. We add related `Receipts` and `ExecutionOutcomes` to the corresponding fields of the `TransactionDetails` during the indexing
7. Once the anticipated `ExecutionOutcomes` counter becomes zero we consider the `TransactionDetails` collecting as over and can serialize and store in the Storage

## How to run

### Prerequisites

An example `.env` file:
```
INDEXER_ID=tx-indexer
DATABASE_URL=127.0.0.1:9042
DATABASE_USER=admin
DATABASE_PASSWORD=password

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=eu-central-1
```
### Features

### Command to run

```
cargo run --release -- <start_options>
```
- `start_options`:
    - `from-latest` fetches the final block height from the RPC and starts indexing from that block
    - `from-interruption <N?>` is used to retrieve the `last_processed_block_height` from the Scylla database. This value is used as the starting point for processing blocks. If a specific value `<N?>` is provided, it will be used as the fallback option. If `<N?>` is not provided or if the database does not have a record (for example, in the case of a fresh start with an empty storage), the fallback option will be `from-latest`.
    - `from-block <N>` starts indexing from the block height `<N>`

