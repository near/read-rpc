use crate::config::ServerContext;
use crate::errors::RPCError;

use actix_web::web::Data;

pub async fn light_client_proof(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::light_client::RpcLightClientExecutionProofRequest,
) -> Result<
    near_jsonrpc::primitives::types::light_client::RpcLightClientExecutionProofResponse,
    RPCError,
> {
    Ok(data
        .near_rpc_client
        .archival_call(request_data, Some("light_client_proof"))
        .await?)
}

pub async fn next_light_client_block(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockRequest,
) -> Result<near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockResponse, RPCError>
{
    match data
        .near_rpc_client
        .call(request_data, Some("next_light_client_block"))
        .await?
    {
        Some(light_client_block) => Ok(
            near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockResponse {
                light_client_block: Some(std::sync::Arc::new(light_client_block)),
            },
        ),
        None => Ok(
            near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockResponse {
                light_client_block: None,
            },
        ),
    }
}
