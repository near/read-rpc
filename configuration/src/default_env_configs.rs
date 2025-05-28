// This file to present all confiuration around th environment variables
// Not present environment variables will be set to default values
// See more details and information about configuration in configuration/README.md

pub const DEFAULT_CONFIG: &str = r#"
### General configuration for NEAR ReadRPC
[general]
## Chain ID: testnet or mainnet
chain_id = "${CHAIN_ID}"

## Near network rpc url
## Using for proxying some requests to near network and to handle finality block
near_rpc_url = "${NEAR_RPC_URL}"

## near network archival rpc url
## Using for proxying some requests to near network and to handle historical data like proofs
## default value is None
near_archival_rpc_url = "${ARCHIVAL_NEAR_RPC_URL}"

## Referer header value
## We want to set a custom referer to let NEAR JSON RPC nodes know that we are a read-rpc instance
## Default value is "http://read-rpc.local"
referer_header_value = "${REFERER_HEADER_VALUE}"

## Authorization token for accessing protected resources
## Default value is None
rpc_auth_token = "${RPC_AUTH_TOKEN}"

## redis url using for cache to store the tx in progress
## Default value is redis://127.0.0.1/
redis_url = "${REDIS_URL}"

### Rpc server general configuration
[general.rpc_server]

## Port for RPC server
## Default port is 8000
server_port = "${SERVER_PORT}"

## Max gas burnt for contract function call
## We allow to use max gas bunt to run contract function call
## Default value is 300_000_000_000_000
max_gas_burnt = "${MAX_GAS_BURNT}"

## Contract code cache in gigabytes
## By default we use 2.0 gigabytes
## We divide the cache size 1/4 for contract code and 3/4 for compiled contract code
## Because the compiled contract code is bigger in 3 times than the contract code from the database
contract_code_cache_size = "${CONTRACT_CODE_CACHE_SIZE}"

## Block cache size in gigabytes
## By default we use 3 gigabytes
## We devide the cache size 1/3 for block cache and 2/3 for chunks cache
## Because the chunks for block is bigger in 2 times than the block
block_cache_size = "${BLOCK_CACHE_SIZE}"

## How many requests we should check for data consistency
## By default we use 100% of requests
## If you want to check 1% of requests, you should set 1
## thet means for every method calls will be checked every 100th request
shadow_data_consistency_rate = "${SHADOW_DATA_CONSISTENCY_RATE}"

## Max size (in bytes) for state prefetch during a view_call
## Limits the amount of data prefetched to speed up the view_call
## By default, it is set to 100KB (100_000 bytes).
prefetch_state_size_limit = "${PREFETCH_STATE_SIZE_LIMIT}"

### Tx indexer general configuration
[general.tx_indexer]

## Indexer ID to handle meta data about the instance
## Unique indexer ID
## Default value is "tx-indexer"
## If you run multiple instances of the indexer, you should change this value for each instance
indexer_id = "${TX_INDEXER_ID}"

## Port for metrics server
## By default it 8080 for tx-indexer and 8081 for state-indexer
metrics_server_port = "${TX_SERVER_PORT}"

## Transaction details storage provider
## Options: "scylla", "postgres"
## Default value is "postgres"
#tx_details_storage_provider = "${TX_DETAILS_STORAGE_PROVIDER}"

### State indexer general configuration
[general.state_indexer]

## Indexer ID to handle meta data about the instance
## Unique indexer ID
## Default value is "state-indexer"
## If you run multiple instances of the indexer, you should change this value for each instance
indexer_id = "${STATE_INDEXER_ID}"

## Port for metrics server
## By default it 8080 for tx-indexer and 8081 for state-indexer
metrics_server_port = "${STATE_SERVER_PORT}"

## Concurrency for state-indexer
## Default value is 1
concurrency = "${CONCURRENCY}"

### Tracking acconunts and state changes configuration
[rightsizing]

## Accounts to track. By default we track all accounts.
## You can specify a list of accounts to track.
## tracked_accounts = ["test.near"]
## By default we track all accounts.
tracked_accounts = "${TRACKED_ACCOUNTS}"

## State changes to track. By default we track all state changes.
## You can specify a list of state changes to track.
## Possible values: "state", "access_key", "contract_code"
## "accounts" are tracked from the `tracked_accounts` section
## tracked_changes = ["state", "access_key", "contract_code"]
## By default we track all state changes.
tracked_changes = "${TRACKED_CHANGES}"

### Lake framework configuration
[lake_config]
# Number of threads to use for fetching data from fatnear
# Default: 2 * available threads
#num_threads = 8

# Authorization token for accessing neardata resources
# Default: None
lake_auth_token = "${LAKE_AUTH_TOKEN}"

## Transaction details are stored in the Google Cloud Storage
[tx_details_storage]

## ScyllaDB database connection URL
## Example: "127.0.0.1:9042"
scylla_url = "${SCYLLA_URL}"

## Scylla user(login)
## Optional database user
scylla_user = "${SCYLLA_USER}"

## Scylla password
## Optional database password
scylla_password = "${SCYLLA_PASSWORD}"

## ScyllaDB preferred DataCenter
## Accepts the DC name of the ScyllaDB to filter the connection to that DC only (preferrably).
## If you connect to multi-DC cluter, you might experience big latencies while working with the DB.
## This is due to the fact that ScyllaDB driver tries to connect to any of the nodes in the cluster disregarding of the location of the DC.
## This option allows to filter the connection to the DC you need.
## Example: "DC1" where DC1 is located in the same region as the application.
## Default value is None
scylla_preferred_dc = "${SCYLLA_PREFERRED_DC}"

## Scylla keepalive interval
## By default we use 60 seconds
scylla_keepalive_interval = "${SCYLLA_KEEPALIVE_INTERVAL}"

## Database configuration
[database]

## Database connection string
## You can use database connection URL
## postgresql://{user}:{password}@localhost:5432/{db_name}
database_url = "${META_DATABASE_URL}"

## Database max connections
## Default value is 10
##The maximum number of connections that this pool should maintain.
## Be mindful of the connection limits for your database as well
## as other applications which may want to connect to the same database
## (or even multiple instances of the same application in high-availability deployments).

## A production application will want to set a higher limit than this.
## Start with connections based on 4x the number of CPU cores.
## For example, a 4-core machine might start with 16 connections.

## This parameter not needed to encrise for indexers
## 10 connections is enough for indexers to save changes to the database
max_connections = "${MAX_CONNECTIONS}"

## Database shards
## You can use multiple database shards
## Each shard should have a unique shard_id
## You can use the same database_url for all shards
## or use different database_url for each shard
## If you use different database_url for each shard, you should create a separate database for each shard
[[database.shards]]
shard_id = 0
database_url = "${SHARD_0_DATABASE_URL}"

[[database.shards]]
shard_id = 1
database_url = "${SHARD_1_DATABASE_URL}"

[[database.shards]]
shard_id = 2
database_url = "${SHARD_2_DATABASE_URL}"

[[database.shards]]
shard_id = 3
database_url = "${SHARD_3_DATABASE_URL}"

[[database.shards]]
shard_id = 4
database_url = "${SHARD_4_DATABASE_URL}"

[[database.shards]]
shard_id = 5
database_url = "${SHARD_5_DATABASE_URL}"
"#;
