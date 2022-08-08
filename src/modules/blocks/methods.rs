use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{
    fetch_block_from_s3, fetch_block_height_from_db, fetch_latest_block_height_from_db,
};
use jsonrpc_v2::{Data, Params};

pub async fn block(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::blocks::RpcBlockResponse, RPCError> {
    let block_height = match params.block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => block_height,
            near_primitives::types::BlockId::Hash(block_hash) => {
                match fetch_block_height_from_db(&data.db_client, block_hash).await {
                    Ok(block_height) => block_height,
                    Err(err) => return Err(RPCError::from(err)),
                }
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => match finality {
            near_primitives::types::Finality::Final => {
                match fetch_latest_block_height_from_db(&data.db_client).await {
                    Ok(block_height) => block_height,
                    Err(err) => return Err(RPCError::from(err)),
                }
            }
            _ => return Err(RPCError::invalid_request()),
        },
        _ => return Err(RPCError::invalid_params()),
    };

    match fetch_block_from_s3(&data.s3_client, &data.s3_bucket_name, block_height).await {
        Ok(block_view) => {
            Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
        }
        Err(err) => Err(RPCError::from(err)),
    }
}

pub async fn chunk(
    Params(_params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<
    near_jsonrpc_primitives::types::chunks::RpcChunkResponse,
    // near_jsonrpc_primitives::types::chunks::RpcChunkError,
    RPCError,
> {
    unreachable!("This method is not implemented yet")
}
