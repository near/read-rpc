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

pub async fn handle_rpc_error<F, Fut, T, P>(
    method: F,
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
            // TODO: Increase appropriate metrics
            match error_struct {
                near_jsonrpc::primitives::errors::RpcErrorKind::RequestValidationError(
                    request_validation_error,
                ) => {
                    match request_validation_error {
                            near_jsonrpc::primitives::errors::RpcRequestValidationErrorKind::MethodNotFound { method_name } => {
                                // This is unreachable because the method is registered in the router
                                unreachable!("Method not found: {:?}", method_name);
                            }
                            near_jsonrpc::primitives::errors::RpcRequestValidationErrorKind::ParseError { error_message } => {
                                println!("Invalid params: {:?}", error_message);
                            }
                        }
                }
                near_jsonrpc::primitives::errors::RpcErrorKind::HandlerError(_) => {
                    println!("Handler error: {:?}", err);
                }
                near_jsonrpc::primitives::errors::RpcErrorKind::InternalError(_) => {
                    println!("Internal error: {:?}", err);
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
        .with_method("view_state_paginated", |data, params| async move {
            handle_rpc_error(modules::state::methods::view_state_paginated, data, params).await
        })
        .with_method("view_receipt_record", |data, params| async move {
            handle_rpc_error(
                modules::receipts::methods::view_receipt_record,
                data,
                params,
            )
            .await
        })
        // requests methods
        .with_method("query", |data, params| async move {
            handle_rpc_error(modules::queries::methods::query, data, params).await
        })
        // basic requests methods
        .with_method("block", |data, params| async move {
            handle_rpc_error(modules::blocks::methods::block, data, params).await
        })
        .with_method("broadcast_tx_async", |data, params| async move {
            handle_rpc_error(
                modules::transactions::methods::broadcast_tx_async,
                data,
                params,
            )
            .await
        })
        .with_method("broadcast_tx_commit", |data, params| async move {
            handle_rpc_error(
                modules::transactions::methods::broadcast_tx_commit,
                data,
                params,
            )
            .await
        })
        .with_method("chunk", |data, params| async move {
            handle_rpc_error(modules::blocks::methods::chunk, data, params).await
        })
        .with_method("gas_price", |data, params| async move {
            handle_rpc_error(modules::gas::methods::gas_price, data, params).await
        })
        .with_method("health", |data, params| async move {
            handle_rpc_error(modules::network::methods::health, data, params).await
        })
        .with_method("light_client_proof", |data, params| async move {
            handle_rpc_error(modules::clients::methods::light_client_proof, data, params).await
        })
        .with_method("next_light_client_block", |data, params| async move {
            handle_rpc_error(
                modules::clients::methods::next_light_client_block,
                data,
                params,
            )
            .await
        })
        .with_method("network_info", |data, params| async move {
            handle_rpc_error(modules::network::methods::network_info, data, params).await
        })
        .with_method("send_tx", |data, params| async move {
            handle_rpc_error(modules::transactions::methods::send_tx, data, params).await
        })
        .with_method("status", |data, params| async move {
            handle_rpc_error(modules::network::methods::status, data, params).await
        })
        .with_method("tx", |data, params| async move {
            handle_rpc_error(modules::transactions::methods::tx, data, params).await
        })
        .with_method("validators", |data, params| async move {
            handle_rpc_error(modules::network::methods::validators, data, params).await
        })
        .with_method("client_config", |data, params| async move {
            handle_rpc_error(modules::network::methods::client_config, data, params).await
        })
        .with_method("EXPERIMENTAL_changes", |data, params| async move {
            handle_rpc_error(
                modules::blocks::methods::changes_in_block_by_type,
                data,
                params,
            )
            .await
        })
        .with_method("EXPERIMENTAL_changes_in_block", |data, params| async move {
            handle_rpc_error(modules::blocks::methods::changes_in_block, data, params).await
        })
        .with_method("EXPERIMENTAL_genesis_config", |data, params| async move {
            handle_rpc_error(modules::network::methods::genesis_config, data, params).await
        })
        .with_method(
            "EXPERIMENTAL_light_client_proof",
            |data, params| async move {
                handle_rpc_error(modules::clients::methods::light_client_proof, data, params).await
            },
        )
        .with_method("EXPERIMENTAL_protocol_config", |data, params| async move {
            handle_rpc_error(modules::network::methods::protocol_config, data, params).await
        })
        .with_method("EXPERIMENTAL_receipt", |data, params| async move {
            handle_rpc_error(modules::receipts::methods::receipt, data, params).await
        })
        .with_method("EXPERIMENTAL_tx_status", |data, params| async move {
            handle_rpc_error(modules::transactions::methods::tx_status, data, params).await
        })
        .with_method(
            "EXPERIMENTAL_validators_ordered",
            |data, params| async move {
                handle_rpc_error(modules::network::methods::validators_ordered, data, params).await
            },
        )
        .with_method(
            "EXPERIMENTAL_maintenance_windows",
            |data, params| async move {
                handle_rpc_error(modules::network::methods::maintenance_windows, data, params).await
            },
        )
        .with_method(
            "EXPERIMENTAL_split_storage_info",
            |data, params| async move {
                handle_rpc_error(modules::network::methods::split_storage_info, data, params).await
            },
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
