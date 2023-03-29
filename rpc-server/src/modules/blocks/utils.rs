use crate::config::{ScyllaDBManager, ServerContext};
use crate::modules::blocks::methods::fetch_block;
use crate::modules::blocks::CacheBlock;
use num_traits::ToPrimitive;

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(s3_client))
)]
pub async fn fetch_block_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
) -> Result<near_primitives::views::BlockView, near_jsonrpc_primitives::types::blocks::RpcBlockError>
{
    tracing::debug!("`fetch_block_from_s3` call");
    match s3_client
        .get_object()
        .bucket(s3_bucket_name)
        .key(format!("{:0>12}/block.json", block_height))
        .send()
        .await
    {
        Ok(response) => {
            let body_bytes = response.body.collect().await.unwrap().into_bytes();
            match serde_json::from_slice::<near_primitives::views::BlockView>(body_bytes.as_ref()) {
                Ok(block) => Ok(block),
                Err(err) => Err(
                    near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                        error_message: err.to_string(),
                    },
                ),
            }
        }
        Err(err) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: err.to_string(),
            },
        ),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(s3_client))
)]
pub async fn fetch_shard_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
    shard_id: near_primitives::types::ShardId,
) -> anyhow::Result<near_indexer_primitives::IndexerShard> {
    tracing::debug!("`fetch_shard_from_s3` call");
    match s3_client
        .get_object()
        .bucket(s3_bucket_name)
        .key(format!("{:0>12}/shard_{shard_id}.json", block_height))
        .send()
        .await
    {
        Ok(response) => {
            let body_bytes = match response.body.collect().await {
                Ok(body) => body.into_bytes(),
                Err(err) => anyhow::bail!("Invalid data from s3 {:?}", err),
            };
            match serde_json::from_slice::<near_indexer_primitives::IndexerShard>(
                body_bytes.as_ref(),
            ) {
                Ok(val) => Ok(val),
                Err(err) => anyhow::bail!("Invalid serialised data {:?}.", err),
            }
        }
        Err(err) => anyhow::bail!("Error to get object from s3 {:?}", err),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(s3_client))
)]
pub async fn fetch_chunk_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
    shard_id: near_primitives::types::ShardId,
) -> Result<near_primitives::views::ChunkView, near_jsonrpc_primitives::types::chunks::RpcChunkError>
{
    tracing::debug!("`fetch_chunk_from_s3` call");
    match fetch_shard_from_s3(s3_client, s3_bucket_name, block_height, shard_id).await {
        Ok(shard) => match shard.chunk {
            Some(chunk) => Ok(near_primitives::views::ChunkView {
                author: chunk.author,
                header: chunk.header,
                transactions: chunk
                    .transactions
                    .into_iter()
                    .map(|indexer_transaction| indexer_transaction.transaction)
                    .collect(),
                receipts: chunk.receipts,
            }),
            None => Err(
                near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                    error_message: "Unavailable chunk".to_string(),
                },
            ),
        },
        Err(err) => Err(
            near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                error_message: err.to_string(),
            },
        ),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn scylla_db_convert_block_hash_to_block_height(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    block_hash: near_primitives::hash::CryptoHash,
) -> Result<u64, near_jsonrpc_primitives::types::blocks::RpcBlockError> {
    tracing::debug!("`scylla_db_convert_block_hash_to_block_height` call");
    let result = scylla_db_manager.get_block_by_hash(block_hash).await;
    if let Ok(row) = result {
        let (block_height,): (num_bigint::BigInt,) = row
            .into_typed::<(num_bigint::BigInt,)>()
            .expect("Invalid block `block_height` value from db");
        Ok(block_height
            .to_u64()
            .expect("Error to convert BigInt into u64"))
    } else {
        Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: "Unknown block hash".to_string(),
            },
        )
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn scylla_db_convert_chunk_hash_to_block_height_and_shard_id(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    chunk_id: near_primitives::hash::CryptoHash,
) -> Result<
    (
        near_primitives::types::BlockHeight,
        near_primitives::types::ShardId,
    ),
    near_jsonrpc_primitives::types::chunks::RpcChunkError,
> {
    tracing::debug!("`scylla_db_convert_chunk_hash_to_block_height_and_shard_id` call");
    let result = scylla_db_manager.get_block_by_chunk_id(chunk_id).await;
    if let Ok(row) = result {
        let (block_height, chunks): (num_bigint::BigInt, Vec<String>) = row
            .into_typed::<(num_bigint::BigInt, Vec<String>)>()
            .expect("Invalid value from db");
        let block_height = block_height
            .to_u64()
            .expect("Error to convert BigInt into u64");
        let shard_id = chunks
            .iter()
            .position(|chunk_hash| chunk_hash == &chunk_id.to_string())
            .unwrap();
        Ok((block_height, shard_id as u64))
    } else {
        Err(
            near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                error_message: format!("Unknown chunk hash: {chunk_id}"),
            },
        )
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_block_from_cache_or_get(
    data: &jsonrpc_v2::Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> CacheBlock {
    let block = match block_reference.clone() {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => data
                .blocks_cache
                .write()
                .unwrap()
                .get(&block_height)
                .cloned(),
            near_primitives::types::BlockId::Hash(_) => None,
        },
        near_primitives::types::BlockReference::Finality(_) => {
            // Returns the final_block_height for all the finalities.
            let block_height = &data
                .final_block_height
                .load(std::sync::atomic::Ordering::SeqCst);
            data.blocks_cache
                .write()
                .unwrap()
                .get(block_height)
                .cloned()
        }
        // TODO: return the height of the first block height from S3 (cache it once on the start)
        near_primitives::types::BlockReference::SyncCheckpoint(_) => None,
    };
    match block {
        Some(block) => block,
        None => {
            let block_from_s3 = fetch_block(data, block_reference)
                .await
                .expect("Error to fetch block");
            let block = CacheBlock {
                block_hash: block_from_s3.header.hash,
                block_height: block_from_s3.header.height,
                block_timestamp: block_from_s3.header.timestamp,
                latest_protocol_version: block_from_s3.header.latest_protocol_version,
            };

            data.blocks_cache
                .write()
                .unwrap()
                .put(block_from_s3.header.height, block);
            block
        }
    }
}
