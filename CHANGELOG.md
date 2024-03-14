# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/near/read-rpc/compare/0.2.0-rc.1...HEAD)

## [0.2.0-rc.1](https://github.com/near/read-rpc/releases/tag/0.2.0-rc.1)

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
