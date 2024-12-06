use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[macro_use]
extern crate lazy_static;

mod cache;
mod config;
mod health;
mod metrics;
mod modules;
mod utils;

// Categories for logging
pub(crate) const RPC_SERVER: &str = "read_rpc_server";

/// Serialises response of a query into JSON to be sent to the client.
///
/// Returns an internal server error if the value fails to serialise.
fn serialize_response(
    value: impl serde::ser::Serialize,
) -> Result<serde_json::Value, near_jsonrpc::primitives::errors::RpcError> {
    serde_json::to_value(value).map_err(|err| {
        near_jsonrpc::primitives::errors::RpcError::serialization_error(err.to_string())
    })
}

/// Processes a specific method call.
///
/// The arguments for the method (which is implemented by the `callback`) will
/// be parsed (using [`RpcRequest::parse`]) from the `request.params`.  Ok
/// results of the `callback` will be converted into a [`Value`] via serde
/// serialisation.
async fn process_method_call<R, V, E, F>(
    request: near_jsonrpc::primitives::message::Request,
    callback: impl FnOnce(R) -> F,
) -> Result<serde_json::Value, near_jsonrpc::primitives::errors::RpcError>
where
    R: near_jsonrpc::RpcRequest,
    V: serde::ser::Serialize,
    near_jsonrpc::primitives::errors::RpcError: From<E>,
    F: std::future::Future<Output = Result<V, E>>,
{
    serialize_response(callback(R::parse(request.params)?).await?)
}

async fn rpc_handler(
    data: actix_web::web::Data<config::ServerContext>,
    payload: actix_web::web::Json<near_jsonrpc::primitives::message::Message>,
) -> actix_web::HttpResponse {
    let near_jsonrpc::primitives::message::Message::Request(request) = payload.0 else {
        return actix_web::HttpResponse::BadRequest().finish();
    };

    let id = request.id.clone();

    let method_name = request.method.clone();
    let mut method_not_found = false;

    let result = match method_name.as_ref() {
        // custom request methods
        "view_state_paginated" => {
            if let Ok(request_data) = serde_json::from_value(request.params) {
                serialize_response(
                    modules::state::methods::view_state_paginated(data, request_data).await,
                )
            } else {
                Err(near_jsonrpc::primitives::errors::RpcError::parse_error(
                    "Failed to parse request data".to_string(),
                ))
            }
        }
        "view_receipt_record" => {
            process_method_call(request, |params| {
                modules::receipts::methods::view_receipt_record(data, params)
            })
            .await
        }
        // request methods
        "query" => {
            process_method_call(request, |params| {
                modules::queries::methods::query(data, params)
            })
            .await
        }
        // basic requests methods
        "block" => {
            process_method_call(request, |params| {
                modules::blocks::methods::block(data, params)
            })
            .await
        }
        "broadcast_tx_async" => {
            process_method_call(request, |params| {
                modules::transactions::methods::broadcast_tx_async(data, params)
            })
            .await
        }
        "broadcast_tx_commit" => {
            process_method_call(request, |params| {
                modules::transactions::methods::broadcast_tx_commit(data, params)
            })
            .await
        }
        "chunk" => {
            process_method_call(request, |params| {
                modules::blocks::methods::chunk(data, params)
            })
            .await
        }
        "gas_price" => {
            process_method_call(request, |params| {
                modules::gas::methods::gas_price(data, params)
            })
            .await
        }
        "health" => {
            process_method_call(request, |_: ()| modules::network::methods::health(data)).await
        }
        "light_client_proof" => {
            process_method_call(request, |params| {
                modules::clients::methods::light_client_proof(data, params)
            })
            .await
        }
        "next_light_client_block" => {
            process_method_call(request, |params| {
                modules::clients::methods::next_light_client_block(data, params)
            })
            .await
        }
        "network_info" => {
            process_method_call(request, |_: ()| {
                modules::network::methods::network_info(data)
            })
            .await
        }
        "send_tx" => {
            process_method_call(request, |params| {
                modules::transactions::methods::send_tx(data, params)
            })
            .await
        }
        "status" => {
            process_method_call(request, |_: ()| modules::network::methods::status(data)).await
        }
        "tx" => {
            process_method_call(request, |params| {
                modules::transactions::methods::tx(data, params)
            })
            .await
        }
        "validators" => {
            process_method_call(request, |params| {
                modules::network::methods::validators(data, params)
            })
            .await
        }
        "client_config" => {
            process_method_call(request, |_: ()| {
                modules::network::methods::client_config(data)
            })
            .await
        }
        "EXPERIMENTAL_changes" => {
            process_method_call(request, |params| {
                modules::blocks::methods::changes_in_block_by_type(data, params)
            })
            .await
        }
        "EXPERIMENTAL_changes_in_block" => {
            process_method_call(request, |params| {
                modules::blocks::methods::changes_in_block(data, params)
            })
            .await
        }
        "EXPERIMENTAL_congestion_level" => {
            process_method_call(request, |params| {
                modules::blocks::methods::congestion_level(data, params)
            })
            .await
        }
        "EXPERIMENTAL_genesis_config" => {
            process_method_call(request, |_: ()| {
                modules::network::methods::genesis_config(data)
            })
            .await
        }
        "EXPERIMENTAL_light_client_proof" => {
            process_method_call(request, |params| {
                modules::clients::methods::light_client_proof(data, params)
            })
            .await
        }
        "EXPERIMENTAL_protocol_config" => {
            process_method_call(request, |params| {
                modules::network::methods::protocol_config(data, params)
            })
            .await
        }
        "EXPERIMENTAL_receipt" => {
            process_method_call(request, |params| {
                modules::receipts::methods::receipt(data, params)
            })
            .await
        }
        "EXPERIMENTAL_tx_status" => {
            process_method_call(request, |params| {
                modules::transactions::methods::tx_status(data, params)
            })
            .await
        }
        "EXPERIMENTAL_validators_ordered" => {
            process_method_call(request, |params| {
                modules::network::methods::validators_ordered(data, params)
            })
            .await
        }
        "EXPERIMENTAL_maintenance_windows" => {
            process_method_call(request, |_: ()| {
                modules::network::methods::maintenance_windows(data)
            })
            .await
        }
        "EXPERIMENTAL_split_storage_info" => {
            process_method_call(request, |_: ()| {
                modules::network::methods::split_storage_info(data)
            })
            .await
        }
        _ => {
            method_not_found = true;
            Err(near_jsonrpc::primitives::errors::RpcError::method_not_found(method_name.clone()))
        }
    };

    // increase METHOD_CALLS_COUNTER for each method call
    if method_not_found {
        metrics::METHOD_CALLS_COUNTER
            .with_label_values(&["METHOD_NOT_FOUND"])
            .inc();
    } else {
        // For query method we calculate the number of total calls in the method
        // and calculate the number of query by types in the inside query handler
        metrics::METHOD_CALLS_COUNTER
            .with_label_values(&[method_name.as_ref()])
            .inc();
    };

    // calculate method error metrics
    if let Err(err) = &result {
        match &err.error_struct {
            Some(near_jsonrpc::primitives::errors::RpcErrorKind::RequestValidationError(
                near_jsonrpc::primitives::errors::RpcRequestValidationErrorKind::ParseError {
                    ..
                },
            )) => metrics::METHOD_ERRORS_TOTAL
                .with_label_values(&[method_name.as_ref(), "PARSE_ERROR"])
                .inc(),
            Some(near_jsonrpc::primitives::errors::RpcErrorKind::HandlerError(error_struct)) => {
                if let Some(error_name) =
                    error_struct.get("name").and_then(serde_json::Value::as_str)
                {
                    metrics::METHOD_ERRORS_TOTAL
                        .with_label_values(&[method_name.as_ref(), error_name])
                        .inc();
                }
            }
            Some(near_jsonrpc::primitives::errors::RpcErrorKind::InternalError(_)) => {
                metrics::METHOD_ERRORS_TOTAL
                    .with_label_values(&[method_name.as_ref(), "INTERNAL_ERROR"])
                    .inc();
            }
            None => {}
            _ => {}
        }
    }

    let mut response = if cfg!(not(feature = "detailed-status-codes")) {
        actix_web::HttpResponse::Ok()
    } else {
        match &result {
            Ok(_) => actix_web::HttpResponse::Ok(),
            Err(err) => match &err.error_struct {
                Some(near_jsonrpc::primitives::errors::RpcErrorKind::RequestValidationError(_)) => {
                    actix_web::HttpResponse::BadRequest()
                }
                Some(near_jsonrpc::primitives::errors::RpcErrorKind::HandlerError(
                    error_struct,
                )) => {
                    if let Some(error_name) =
                        error_struct.get("name").and_then(serde_json::Value::as_str)
                    {
                        if error_name == "TIMEOUT_ERROR" {
                            actix_web::HttpResponse::RequestTimeout()
                        } else {
                            actix_web::HttpResponse::Ok()
                        }
                    } else {
                        actix_web::HttpResponse::Ok()
                    }
                }
                Some(near_jsonrpc::primitives::errors::RpcErrorKind::InternalError(_)) => {
                    actix_web::HttpResponse::InternalServerError()
                }
                None => actix_web::HttpResponse::Ok(),
            },
        }
    };

    response.json(near_jsonrpc::primitives::message::Message::response(
        id,
        result.map_err(near_jsonrpc::primitives::errors::RpcError::from),
    ))
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

    let server_context =
        actix_web::web::Data::new(config::ServerContext::init(rpc_server_config.clone()).await?);

    utils::task_regularly_update_blocks_by_finality(
        std::sync::Arc::clone(&server_context.blocks_info_by_finality),
        std::sync::Arc::clone(&server_context.blocks_cache),
        server_context.fastnear_client.clone(),
        server_context.near_rpc_client.clone(),
    )
    .await;

    actix_web::HttpServer::new(move || {
        let cors = actix_cors::Cors::permissive();

        actix_web::App::new()
            .wrap(cors)
            .wrap(tracing_actix_web::TracingLogger::default())
            .app_data(server_context.clone())
            .service(actix_web::web::scope("/").route("", actix_web::web::post().to(rpc_handler)))
            .service(metrics::get_metrics)
            .service(health::get_health_status)
    })
    .bind(format!(
        "0.0.0.0:{:0>5}",
        rpc_server_config.general.server_port
    ))?
    .run()
    .await?;

    Ok(())
}
