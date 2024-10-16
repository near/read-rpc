// This file to present all confiuration around th environment variables
// Not present environment variables will be set to default values
// See more details and information about each parameter in configuration/README.md and configuration/example.config.toml

pub const DEFAULT_CONFIG: &str = r#"
[general]
chain_id = "${CHAIN_ID}"
near_rpc_url = "${NEAR_RPC_URL}"
near_archival_rpc_url = "${ARCHIVAL_NEAR_RPC_URL}"
referer_header_value = "${REFERER_HEADER_VALUE}"
redis_url = "${REDIS_URL}"

[general.rpc_server]
server_port = "${SERVER_PORT}"
max_gas_burnt = "${MAX_GAS_BURNT}"
contract_code_cache_size = "${CONTRACT_CODE_CACHE_SIZE}"
block_cache_size = "${BLOCK_CACHE_SIZE}"
shadow_data_consistency_rate = "${SHADOW_DATA_CONSISTENCY_RATE}"
prefetch_state_size_limit = "${PREFETCH_STATE_SIZE_LIMIT}"

[general.tx_indexer]
indexer_id = "${TX_INDEXER_ID}"
metrics_server_port = "${TX_SERVER_PORT}"

[general.state_indexer]
indexer_id = "${STATE_INDEXER_ID}"
metrics_server_port = "${STATE_SERVER_PORT}"
concurrency = "${CONCURRENCY}"

[general.near_state_indexer]
metrics_server_port = "${NEAR_STATE_SERVER_PORT}"
concurrency = "${NEAR_STATE_CONCURRENCY}"

[rightsizing]
tracked_accounts = "${TRACKED_ACCOUNTS}"
tracked_changes = "${TRACKED_CHANGES}"

[lake_config]
aws_access_key_id = "${AWS_ACCESS_KEY_ID}"
aws_secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
aws_default_region = "${AWS_DEFAULT_REGION}"
aws_bucket_name = "${AWS_BUCKET_NAME}"

[tx_details_storage]
bucket_name = "${TX_BUCKET_NAME}"

[database]
database_url = "${META_DATABASE_URL}"
max_connections = "${MAX_CONNECTIONS}"

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
