use jsonrpc_v2::{Data, Params, Router, Server};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[macro_use]
extern crate lazy_static;

mod cache;
mod config;
mod errors;
mod health;
mod metrics;
mod middlewares;
mod modules;
mod utils;

// Categories for logging
pub(crate) const RPC_SERVER: &str = "read_rpc_server";

macro_rules! handle_rpc_method {
    ($method:expr, $method_name:expr) => {
        |data, params| async move { handle_rpc_method($method, $method_name, data, params).await }
    };
}

pub async fn handle_rpc_method<F, Fut, T, P>(
    method: F,
    method_name: &str,
    data: Data<config::ServerContext>,
    params: Params<P>,
) -> Result<T, crate::errors::RPCError>
where
    F: FnOnce(Data<config::ServerContext>, Params<P>) -> Fut,
    Fut: std::future::Future<Output = Result<T, crate::errors::RPCError>>,
{
    let result = method(data, params).await;
    if let Err(err) = &result {
        if let Some(error_struct) = &err.error_struct {
            match error_struct {
                near_jsonrpc::primitives::errors::RpcErrorKind::RequestValidationError(
                    request_validation_error,
                ) => {
                    if let near_jsonrpc::primitives::errors::RpcRequestValidationErrorKind::ParseError { .. } = request_validation_error
                    {
                        metrics::METHOD_ERRORS_TOTAL
                            .with_label_values(&[method_name, "PARSE_ERROR"])
                            .inc();
                    }
                }
                near_jsonrpc::primitives::errors::RpcErrorKind::HandlerError(error_struct) => {
                    if let Some(stringified_error_name) = error_struct.get("name").and_then(|name| name.as_str()) {
                        metrics::METHOD_ERRORS_TOTAL
                            .with_label_values(&[method_name, stringified_error_name])
                            .inc();
                    }
                }
                near_jsonrpc::primitives::errors::RpcErrorKind::InternalError(_) => {
                    metrics::METHOD_ERRORS_TOTAL
                        .with_label_values(&[method_name, "INTERNAL_ERROR"])
                        .inc();
                }
            }
        }
    }
    result
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    configuration::init_tracing(RPC_SERVER).await?;
    tracing::info!(
        "Starting {} v{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
    );

    let rpc_server_config =
        configuration::read_configuration::<configuration::RpcServerConfig>().await?;

    let near_rpc_client = utils::JsonRpcClient::new(
        rpc_server_config.general.near_rpc_url.clone(),
        rpc_server_config.general.near_archival_rpc_url.clone(),
    );
    // We want to set a custom referer to let NEAR JSON RPC nodes know that we are a read-rpc instance
    let near_rpc_client = near_rpc_client.header(
        "Referer".to_string(),
        rpc_server_config.general.referer_header_value.clone(),
    )?;

    let server_port = rpc_server_config.general.server_port;

    let server_context =
        config::ServerContext::init(rpc_server_config.clone(), near_rpc_client.clone()).await?;

    let blocks_cache_clone = std::sync::Arc::clone(&server_context.blocks_cache);
    let blocks_info_by_finality_clone =
        std::sync::Arc::clone(&server_context.blocks_info_by_finality);
    let near_rpc_client_clone = near_rpc_client.clone();

    let finality_blocks_storage =
        cache_storage::BlocksByFinalityCache::new(rpc_server_config.general.redis_url.to_string())
            .await
            .map_err(|err| {
                crate::metrics::OPTIMISTIC_UPDATING.set_not_working();
                tracing::warn!("Failed to connect to Redis: {:?}", err);
            })
            .ok();

    // We need to update final block from Redis and Lake
    // Because we can't be sure that Redis has the latest block
    // And Lake can be used as a backup source

    // Update final block from Redis if Redis is available
    if let Some(finality_blocks_storage) = finality_blocks_storage.clone() {
        tokio::spawn(async move {
            utils::update_final_block_regularly_from_redis(
                blocks_cache_clone,
                blocks_info_by_finality_clone,
                finality_blocks_storage,
                near_rpc_client_clone,
            )
            .await
        });
    }

    // Update final block from Lake
    let blocks_cache_clone = std::sync::Arc::clone(&server_context.blocks_cache);
    let blocks_info_by_finality_clone =
        std::sync::Arc::clone(&server_context.blocks_info_by_finality);
    tokio::spawn(async move {
        utils::update_final_block_regularly_from_lake(
            blocks_cache_clone,
            blocks_info_by_finality_clone,
            rpc_server_config,
            near_rpc_client,
        )
        .await
    });

    // Update optimistic block from Redis if Redis is available
    if let Some(finality_blocks_storage) = finality_blocks_storage {
        let blocks_info_by_finality =
            std::sync::Arc::clone(&server_context.blocks_info_by_finality);
        tokio::spawn(async move {
            utils::update_optimistic_block_regularly(
                blocks_info_by_finality,
                finality_blocks_storage,
            )
            .await
        });
    }

    let rpc = Server::new()
        .with_data(Data::new(server_context.clone()))
        // custom requests methods
        .with_method(
            "view_state_paginated",
            handle_rpc_method!(
                modules::state::methods::view_state_paginated,
                "view_state_paginated"
            ),
        )
        .with_method(
            "view_receipt_record",
            handle_rpc_method!(
                modules::receipts::methods::view_receipt_record,
                "view_receipt_record"
            ),
        )
        // requests methods
        .with_method(
            "query",
            handle_rpc_method!(modules::queries::methods::query, "query"),
        )
        // basic requests methods
        .with_method(
            "block",
            handle_rpc_method!(modules::blocks::methods::block, "block"),
        )
        .with_method(
            "broadcast_tx_async",
            handle_rpc_method!(
                modules::transactions::methods::broadcast_tx_async,
                "broadcast_tx_async"
            ),
        )
        .with_method(
            "broadcast_tx_commit",
            handle_rpc_method!(
                modules::transactions::methods::broadcast_tx_commit,
                "broadcast_tx_commit"
            ),
        )
        .with_method(
            "chunk",
            handle_rpc_method!(modules::blocks::methods::chunk, "chunk"),
        )
        .with_method(
            "gas_price",
            handle_rpc_method!(modules::gas::methods::gas_price, "gas_price"),
        )
        .with_method(
            "health",
            handle_rpc_method!(modules::network::methods::health, "health"),
        )
        .with_method(
            "light_client_proof",
            handle_rpc_method!(
                modules::clients::methods::light_client_proof,
                "light_client_proof"
            ),
        )
        .with_method(
            "next_light_client_block",
            handle_rpc_method!(
                modules::clients::methods::next_light_client_block,
                "next_light_client_block"
            ),
        )
        .with_method(
            "network_info",
            handle_rpc_method!(modules::network::methods::network_info, "network_info"),
        )
        .with_method(
            "send_tx",
            handle_rpc_method!(modules::transactions::methods::send_tx, "send_tx"),
        )
        .with_method(
            "status",
            handle_rpc_method!(modules::network::methods::status, "status"),
        )
        .with_method(
            "tx",
            handle_rpc_method!(modules::transactions::methods::tx, "tx"),
        )
        .with_method(
            "validators",
            handle_rpc_method!(modules::network::methods::validators, "validators"),
        )
        .with_method(
            "client_config",
            handle_rpc_method!(modules::network::methods::client_config, "client_config"),
        )
        .with_method(
            "EXPERIMENTAL_changes",
            handle_rpc_method!(
                modules::blocks::methods::changes_in_block_by_type,
                "EXPERIMENTAL_changes"
            ),
        )
        .with_method(
            "EXPERIMENTAL_changes_in_block",
            handle_rpc_method!(
                modules::blocks::methods::changes_in_block,
                "EXPERIMENTAL_changes_in_block"
            ),
        )
        .with_method(
            "EXPERIMENTAL_genesis_config",
            handle_rpc_method!(
                modules::network::methods::genesis_config,
                "EXPERIMENTAL_genesis_config"
            ),
        )
        .with_method(
            "EXPERIMENTAL_light_client_proof",
            handle_rpc_method!(
                modules::clients::methods::light_client_proof,
                "EXPERIMENTAL_light_client_proof"
            ),
        )
        .with_method(
            "EXPERIMENTAL_protocol_config",
            handle_rpc_method!(
                modules::network::methods::protocol_config,
                "EXPERIMENTAL_protocol_config"
            ),
        )
        .with_method(
            "EXPERIMENTAL_receipt",
            handle_rpc_method!(modules::receipts::methods::receipt, "EXPERIMENTAL_receipt"),
        )
        .with_method(
            "EXPERIMENTAL_tx_status",
            handle_rpc_method!(
                modules::transactions::methods::tx_status,
                "EXPERIMENTAL_tx_status"
            ),
        )
        .with_method(
            "EXPERIMENTAL_validators_ordered",
            handle_rpc_method!(
                modules::network::methods::validators_ordered,
                "EXPERIMENTAL_validators_ordered"
            ),
        )
        .with_method(
            "EXPERIMENTAL_maintenance_windows",
            handle_rpc_method!(
                modules::network::methods::maintenance_windows,
                "EXPERIMENTAL_maintenance_windows"
            ),
        )
        .with_method(
            "EXPERIMENTAL_split_storage_info",
            handle_rpc_method!(
                modules::network::methods::split_storage_info,
                "EXPERIMENTAL_split_storage_info"
            ),
        )
        .finish();

    // Insert all rpc methods to the hashmap after init the server
    metrics::RPC_METHODS.insert(rpc.router.routers()).await;

    actix_web::HttpServer::new(move || {
        let rpc = rpc.clone();

        // Configure CORS
        let cors = actix_cors::Cors::permissive();

        actix_web::App::new()
            .wrap(cors)
            .wrap(tracing_actix_web::TracingLogger::default())
            // wrapper to count rpc total requests
            .wrap(middlewares::RequestsCounters)
            .app_data(actix_web::web::Data::new(server_context.clone()))
            .service(
                actix_web::web::service("/")
                    .guard(actix_web::guard::Post())
                    .finish(rpc.into_web_service()),
            )
            .service(metrics::get_metrics)
            .service(health::get_health_status)
    })
    .bind(format!("0.0.0.0:{:0>5}", server_port))?
    .run()
    .await?;

    Ok(())
}
