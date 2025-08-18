# tx-details-storage

This is a small library that provides the interface to store and retrieve `TransactionDetails` to/from different storage engines:
* ScyllaDB - recommeded for archival nodes
* PostgreSQL - recommended for regular nodes

The choice of the storage engine is made in configuration `tx_details_storage_provider` parameter in `config.toml` file in the project root. The default value is `postgres`, but it can be changed to `scylla` if needed.

The functionality is extracted into this separate library to provide a common interface for all the components that need to store and retrieve `TransactionDetails` (e.g. `tx-indexer` and `rpc-server`).

`TransactionDetails` is a data class defined in the `readnode-primitives` module, it contains all the details of the transactions:
- `SignedTransactionView` itself
- `ExecutionOutcomeWithIdView` of the transaction
- All `ReceiptView`s of the transaction
- All `ExecutionOutcomeWithIdView`s of the receipts
- `FinalExecutionStatus` of the transaction

This entire structuer is borsh-serialized and stored in the bucket.

**This library doesn't handle the serialization/deserialization of the `TransactionDetails` struct. It is the responsibility of the caller to serialize/deserialize the struct before storing/retrieving it from the bucket.**

