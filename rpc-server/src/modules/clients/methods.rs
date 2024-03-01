use crate::config::ServerContext;
use crate::errors::RPCError;
use jsonrpc_v2::{Data, Params};

pub async fn light_client_proof(
    data: Data<ServerContext>,
    Params(params): Params<
        near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest,
    >,
) -> Result<
    near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofResponse,
    RPCError,
> {
    crate::metrics::ARCHIVAL_PROXY_LIGHT_CLIENT_PROOF.inc();
    Ok(data.near_rpc_client.archival_call(params).await?)
}

pub async fn next_light_client_block(
    data: Data<ServerContext>,
    Params(params): Params<
        near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockResponse, RPCError>
{
    crate::metrics::ARCHIVAL_PROXY_NEXT_LIGHT_CLIENT_BLOCK.inc();
    match data.near_rpc_client.archival_call(params).await? {
        Some(light_client_block) => Ok(
            near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockResponse {
                light_client_block: Some(std::sync::Arc::new(light_client_block)),
            },
        ),
        None => Ok(
            near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockResponse {
                light_client_block: None,
            },
        ),
    }
}
