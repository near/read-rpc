# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/near/read-rpc/compare/main...develop)

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
