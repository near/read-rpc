use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use jsonrpc_v2::{Data, Params};

pub async fn gas_price(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest>,
) -> Result<near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse, RPCError> {
    let block_reference = match params.block_id {
        Some(block_id) => near_primitives::types::BlockReference::BlockId(block_id),
        None => near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        ),
    };
    let gas_price_view = gas_price_call(&data, block_reference)
        .await
        .map_err(near_jsonrpc_primitives::errors::RpcError::from)?;
    Ok(near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse { gas_price_view })
}

async fn gas_price_call(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<
    near_primitives::views::GasPriceView,
    near_jsonrpc_primitives::types::gas_price::RpcGasPriceError,
> {
    let block = fetch_block_from_cache_or_get(data, block_reference)
        .await
        .map_err(|err| {
            near_jsonrpc_primitives::types::gas_price::RpcGasPriceError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?;
    Ok(near_primitives::views::GasPriceView {
        gas_price: block.gas_price,
    })
}
