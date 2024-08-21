use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::blocks::CacheBlock;

use actix_web::web::Data;

#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn gas_price(
    data: Data<ServerContext>,
    mut request_data: near_jsonrpc::primitives::types::gas_price::RpcGasPriceRequest,
) -> Result<near_jsonrpc::primitives::types::gas_price::RpcGasPriceResponse, RPCError> {
    tracing::debug!("`gas_price` called with parameters: {:?}", request_data);
    let block_reference = match request_data.block_id.clone() {
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
                if request_data.block_id.is_none() {
                    request_data.block_id =
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
            request_data,
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
    let block = fetch_block_from_cache_or_get(data, &block_reference, "gas_price")
        .await
        .map_err(|err| {
            near_jsonrpc::primitives::types::gas_price::RpcGasPriceError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?;
    Ok(block)
}
