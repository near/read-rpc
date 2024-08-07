use crate::config::ServerContext;
use crate::errors::RPCError;
use jsonrpc_v2::{Data, Params};
use near_jsonrpc::RpcRequest;

pub async fn light_client_proof(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<
    near_jsonrpc::primitives::types::light_client::RpcLightClientExecutionProofResponse,
    RPCError,
> {
    let request =
        near_jsonrpc::primitives::types::light_client::RpcLightClientExecutionProofRequest::parse(
            params,
        )?;
    Ok(data
        .near_rpc_client
        .archival_call(request, Some("light_client_proof"))
        .await?)
}

pub async fn next_light_client_block(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockResponse, RPCError>
{
    let request =
        near_jsonrpc::primitives::types::light_client::RpcLightClientNextBlockRequest::parse(
            params,
        )?;
    match data
        .near_rpc_client
        .call(request, Some("next_light_client_block"))
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
