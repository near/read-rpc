use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::blocks::CacheBlock;
use jsonrpc_v2::{Data, Params};

#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn gas_price(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest>,
) -> Result<near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse, RPCError> {
    tracing::debug!("`gas_price` called with parameters: {:?}", params);
    crate::metrics::GAS_PRICE_REQUESTS_TOTAL.inc();
    let block_reference = match params.block_id.clone() {
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
                if let None = params.block_id {
                    params.block_id =
                        Some(near_primitives::types::BlockId::Height(block.block_height));
                };
                Ok(near_primitives::views::GasPriceView {
                    gas_price: block.gas_price,
                })
            }
            Err(err) => Err(err),
        };

        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::GAS_PRICE_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "GAS_PRICE",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "GAS_PRICE")
        };
    };
    let gas_price_view = near_primitives::views::GasPriceView {
        gas_price: cache_block
            .map_err(near_jsonrpc_primitives::errors::RpcError::from)?
            .gas_price,
    };
    Ok(near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse { gas_price_view })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn gas_price_call(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<CacheBlock, near_jsonrpc_primitives::types::gas_price::RpcGasPriceError> {
    let block = fetch_block_from_cache_or_get(data, block_reference)
        .await
        .map_err(|err| {
            near_jsonrpc_primitives::types::gas_price::RpcGasPriceError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?;
    Ok(block)
}
