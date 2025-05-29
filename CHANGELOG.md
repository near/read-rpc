# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/near/read-rpc/compare/main...develop)

## [0.3.4](https://github.com/near/read-rpc/releases/tag/v0.3.4)

### What's Changed
* Migrate from lake data to fastnear data
* Add metrics to calculate the number of blocks which fetched from the cache and fastnear
* Add blocks chunks cache and optimize cache layer for read-rpc-server
* Add authorization token supports
* Migrate transaction details from GCS to ScyllaDB
* Migrate `receipts_map` and `outcomes_map` from PostgreSQL to ScyllaDB
* Remove `near_state_indexer`
* Move the helper function `shard_layout` from `logic-state-indexer` to `configuration` crate
  * `ShardLayout` depends on the `near-chain-configs::GenesisConfig` so now the `configuration` crate is responsible for downloading the genesis config and providing the `ShardLayout` automatically. This process is surrounded by additional logs to help with debugging.
  * Provide `Option<ShardLayout>` to `DatabaseConfig` since it's the most common case to care about shard layout in the database context
  * Refactor the configuration of the `rpc-server` crate to use the `ShardLayout` from the `configuration` crate
  * `tx-details-storage` has been updated:
    * Now it can work with either PostgreSQL or ScyllaDB
    * The actual database interactions have been moved to the `database` crate and traits have been introduced to abstract the database interactions, so we can add more storage engines in the future
    * `tx_details_storage_provider` parameter has been added to the `configuration` and defaults to `postgres` storage engine`
  * `configuration` does not require `scylla_url` and will panic if the `tx_details_storage_provider` is set to `scylla` without the `scylla_url` parameter. The panic will occur during the initialization of the `tx-details-storage` in the `rpc-server` or `tx-indexer` crates.
  * `rpc-server` will instantiate the `tx-details-storage` with the `tx_details_storage_provider` from the configuration
  * `rpc-server` doesn't not ignore the `sernder_account_id` parameter when fetching the `TransactionDetails`. With ScyllaDB this parameter is not needed, but required for PostgreSQL to fetch the `TransactionDetails` from the corresponding shard database

### Supported Nearcore Version
- nearcore v2.6.3
- rust v1.85.0

## [0.3.3](https://github.com/near/read-rpc/releases/tag/v0.3.3)

### Supported Nearcore Version
- nearcore v2.4.0
- rust v1.82.0

## [0.3.2](https://github.com/near/read-rpc/releases/tag/v0.3.2)

### What's Changed
* Corrected state size calculation logic.
* Integrated cargo_pkg_version metric to reflect the current server version.
* Delete unnecessary debug logs about update blocks by finalities
* Support new method `EXPERIMENTAL_congestion_level`

### Supported Nearcore Version
- nearcore v2.3.1
- rust v1.81.0

## [0.3.1](https://github.com/near/read-rpc/releases/tag/v0.3.1)

### Supported Nearcore Version
- nearcore v2.3.0
- rust v1.81.0

### What's Changed
* Corrected state size calculation logic.
* Integrated cargo_pkg_version metric to reflect the current server version.
* Delete unnecessary debug logs about update blocks by finalities

## [0.3.1](https://github.com/near/read-rpc/releases/tag/v0.3.1)

### Supported Nearcore Version
- nearcore v2.3.0
- rust v1.81.0

### What's Changed
* Improved bulk insertion of state_changes, reducing database requests from hundreds to a maximum of 7 per block.
* Configuration improvement. Create default config.toml on start application to loaded parameters from the environment variables.
* Fix to fetch state by pages (view_state_paginated).
* Change logic to get `shard_layout` for indexers. Main idea to avoid request `protocol_config` via RPC, `protocol_config` could be changed with new nearcore release and we should support old and new versions of `protocol_config`.

## [0.3.0](https://github.com/near/read-rpc/releases/tag/v0.3.0)

### BREAKING CHANGES

Please, see the [PostgreSQL & ShardLayout Pull Request](https://github.com/near/read-rpc/pull/282) for details. This is a completely new version of the ReadRPC with a new data storage layout and a new data storage engine. The main changes are:
- **Migrate from ScyllaDB to PostgreSQL for storing the data**. We adopted the ShardLayout from the `nearcore` and split the data to separate PostgreSQL databases for each shard.
- Changed the way we store `TransactionDetails` (JSON blobs again, but it might change soon)

All the work in this release allowed us to increase the performance of the ReadRPC and make it more reliable. We are still working on the performance improvements and will continue to work on the ReadRPC to make it even better.

### Supported Nearcore Version (not changed)
- nearcore v2.2.1
- rust v1.79.0

## [0.2.16](https://github.com/near/read-rpc/releases/tag/v0.2.16)
### Supported Nearcore Version
- nearcore v2.2.1
- rust v1.79.0

## [0.2.15](https://github.com/near/read-rpc/releases/tag/v0.2.15)
### Supported Nearcore Version
- nearcore v2.2.0
- rust v1.79.0

## [0.2.14](https://github.com/near/read-rpc/releases/tag/v0.2.14)
### Supported Nearcore Version
- nearcore v2.1.1
- rust v1.79.0

### What's Changed
* Fix docker build

## [0.2.13](https://github.com/near/read-rpc/releases/tag/v0.2.13)
### Supported Nearcore Version
- nearcore v2.1.1
- rust v1.79.0

### What's Changed
* Update `nearcore` to v2.1.1
* Update rust version to v1.79.0


## [0.2.12](https://github.com/near/read-rpc/releases/tag/v0.2.12)
### Supported Nearcore Version
- nearcore v2.0.0
- rust v1.78.0

### What's Changed
* Update `nearcore` to v2.0.0
* Update rust version to v1.78.0
* Fixed transaction borsh deserialization error

## [0.2.11](https://github.com/near/read-rpc/releases/tag/v0.2.11)
### Supported Nearcore Version
- nearcore v1.40.0
- rust v1.77.0

### What's Changed
* Change main data source for `transaction_details` from database to the object storage (GCS Bucket)
  * Add `tx-details-storage` library to handle the GCS communication
  * Update `rpc-server` to read from the GCS Bucket when transaction is requested
  * Keep the database as a backup data source for the migration period
  * Add metric `legacy_database_tx_details` to track the number of requests to the database to monitor the transition progress (expected to decrease over time to zero)
  * Extend the number of save attempts to ensure the transaction is saved to the GCS Bucket and is deserializable correctly
* Refactor `tx-indexer` to store the transaction details in the GCS Bucket
  * Still storing `ExecutionOutcome` and `Receipt` in the database

## [0.2.10](https://github.com/near/read-rpc/releases/tag/v0.2.10)
### Supported Nearcore Version
- nearcore v1.40.0
- rust v1.77.0

## [0.2.9](https://github.com/near/read-rpc/releases/tag/v0.2.9)
### Supported Nearcore Version
- nearcore v1.39.1
- rust v1.77.0

### What's Changed
* Refactoring, extending and improving the metrics
* Refactoring and improving `query.call_function`
* Added `view_receipt_record` rpc endpoint

## [0.2.8](https://github.com/near/read-rpc/releases/tag/v0.2.8)
### Supported Nearcore Version
- nearcore v1.39.1
- rust v1.77.0

## [0.2.7](https://github.com/near/read-rpc/releases/tag/v0.2.7)
### Supported Nearcore Version
- nearcore v1.39.1
- rust v1.76.0

## [0.2.6](https://github.com/near/read-rpc/releases/tag/v0.2.6)
### Supported Nearcore Version
- nearcore v1.39.0
- rust v1.76.0

## [0.2.5](https://github.com/near/read-rpc/releases/tag/v0.2.5)
### Supported Nearcore Version
- nearcore v1.38.0
- rust v1.76.0

### What's Changed
* Fix transactions request parser
* Fix tx request returns "Invalid params" to a totally valid request
* Fix Dockerfile for read-rpc server
* Fix proxy broadcast-tx-commit
* Improvement logs for shadow_compare_results_handler
* Extending and improving optimistic handler
* Add waiting until node will be fully synced
* Added RUST_LOG=info as the default directive to enhance visibility and provide informative logs for better troubleshooting and monitoring.
* Included additional informational logs at the start of the near-state-indexer process, providing deeper insights into its functionality and progress.
* Implemented a handler to check the regularity of optimistic block updates.
* If updates are irregular, the system initiates an update task to synchronize the final block from the lake, ensuring data consistency and integrity.

## [0.2.0-rc.1](https://github.com/near/read-rpc/releases/tag/v0.2.0-rc.1)

### Supported Nearcore Version
- nearcore v.38.0-rc.2
- rust v1.76.0

### Added
- Added support for `SyncCheckpoint` in the `block` method for better block handling and synchronization.
- Added support for `OptimisticBlock` in the `block` method for better block handling and synchronization.
- Added `ARCHIVAL_PROXY_QUERY_VIEW_STATE_WITH_INCLUDE_PROOFS` metric to track the number of archival proxy requests for view state with include proofs.
- Added `TOTAL_REQUESTS_COUNTER` metric to counting total rpc requests.
- Added `GET /health` for the healthcheck of rpc-server.
- Implemented the `status` method to accommodate `near_primitives::views::StatusResponse`.
- Implemented the `health` method. Health includes the info about the syncing state of the node of `rpc-server`.
- Implemented near-state-indexer to index the state of the nearcore node.

### Changed
- Enhanced the tx method to show in-progress transaction status, avoiding `UNKNOWN_TRANSACTION` responses and providing more accurate feedback.
- Reverted the logic behind the `block_height` and `block_hash` parameters in the `query` method to match the behavior of the `nearcore` JSON-RPC API.

### Removed
- Dropped the `SYNC_CHECKPOINT_REQUESTS_TOTAL` metric for redundancy.
- Removed in-memory cache for `tx-indexer` to optimize resource usage and streamline the process.

## [0.1.0](https://github.com/near/read-rpc/releases/tag/v0.1.0)

* nearcore v1.36.0
* rust v1.73.0

> Release Page: <https://github.com/near/read-rpc/releases/tag/v0.1.0>
