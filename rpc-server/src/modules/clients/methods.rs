use actix_web::web::Data;

use crate::config::ServerContext;

pub async fn light_client_proof(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::light_client::RpcLightClientExecutionProofRequest,
) -> Result<
    near_jsonrpc::primitives::types::light_client::RpcLightClientExecutionProofResponse,
    near_jsonrpc::primitives::types::light_client::RpcLightClientProofError,
> {
    Ok(data
        .near_rpc_client
        .archival_call(request_data, Some("light_client_proof"))
        .await.map_err(|err| {
            err.handler_error().cloned().unwrap_or(
                near_jsonrpc::primitives::types::light_client::RpcLightClientProofError::InternalError {
                    error_message: err.to_string(),
            })
        })?
    )
}

pub async fn next_light_client_block(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockRequest,
) -> Result<
    near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockResponse,
    near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockError,
> {
    match data
        .near_rpc_client
        .call(request_data, Some("next_light_client_block"))
        .await.map_err(|err| {
            err.handler_error().cloned().unwrap_or(
                near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockError::InternalError {
                    error_message: err.to_string(),
            })
        })?
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
