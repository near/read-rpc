use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::blocks::CacheBlock;
use jsonrpc_v2::{Data, Params};
use near_jsonrpc::RpcRequest;

#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn gas_price(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::gas_price::RpcGasPriceResponse, RPCError> {
    let mut gas_price_request =
        near_jsonrpc::primitives::types::gas_price::RpcGasPriceRequest::parse(params)?;
    tracing::debug!(
        "`gas_price` called with parameters: {:?}",
        gas_price_request
    );
    let block_reference = match gas_price_request.block_id.clone() {
        Some(block_id) => near_primitives::types::BlockReference::BlockId(block_id),
        None => near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        ),
    };
    let cache_block = gas_price_call(&data, block_reference).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        let result = match &cache_block {
            Ok(block) => {
                if let None = gas_price_request.block_id {
                    gas_price_request.block_id =
                        Some(near_primitives::types::BlockId::Height(block.block_height));
                };
                Ok(near_primitives::views::GasPriceView {
                    gas_price: block.gas_price,
                })
            }
            Err(err) => Err(err),
        };

        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            gas_price_request,
            "gas_price",
        )
        .await;
    };
    let gas_price_view = near_primitives::views::GasPriceView {
        gas_price: cache_block
            .map_err(near_jsonrpc::primitives::errors::RpcError::from)?
            .gas_price,
    };
    Ok(near_jsonrpc::primitives::types::gas_price::RpcGasPriceResponse { gas_price_view })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn gas_price_call(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<CacheBlock, near_jsonrpc::primitives::types::gas_price::RpcGasPriceError> {
    let block = fetch_block_from_cache_or_get(data, block_reference)
        .await
        .map_err(|err| {
            near_jsonrpc::primitives::types::gas_price::RpcGasPriceError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?;
    Ok(block)
}
