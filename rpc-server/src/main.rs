use actix_web::{
    web::{self},
    App, HttpResponse, HttpServer,
};
use errors::RPCError;
//use jsonrpc_v2::{Data, Params, Router, Server};
use mimalloc::MiMalloc;
use near_jsonrpc::{
    primitives::{
        errors::{RpcError, RpcErrorKind},
        message::{Message, Request},
    },
    RpcRequest,
};
use serde_json::Value;

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

/// Serialises response of a query into JSON to be sent to the client.
///
/// Returns an internal server error if the value fails to serialise.
fn serialize_response(value: impl serde::ser::Serialize) -> Result<Value, RPCError> {
    serde_json::to_value(value).map_err(|err| RPCError::internal_error(&err.to_string()))
}

/// Processes a specific method call.
///
/// The arguments for the method (which is implemented by the `callback`) will
/// be parsed (using [`RpcRequest::parse`]) from the `request.params`.  Ok
/// results of the `callback` will be converted into a [`Value`] via serde
/// serialisation.
async fn process_method_call<R, V, E, F>(
    request: Request,
    callback: impl FnOnce(R) -> F,
) -> Result<Value, RPCError>
where
    R: RpcRequest,
    V: serde::ser::Serialize,
    RPCError: std::convert::From<E>,
    F: std::future::Future<Output = Result<V, E>>,
{
    serialize_response(callback(R::parse(request.params)?).await?)
}

async fn rpc_handler(
    data: web::Data<config::ServerContext>,
    payload: web::Json<Message>,
) -> HttpResponse {
    let Message::Request(request) = payload.0 else {
        return HttpResponse::BadRequest().finish();
    };

    let id = request.id.clone();

    let result = match request.method.as_ref() {
        "block" => {
            process_method_call(request, |params| {
                modules::blocks::methods::block(data, params)
            })
            .await
        }
        "view_receipt_record" => {
            process_method_call(request, |params| {
                modules::receipts::methods::view_receipt_record(data, params)
            })
            .await
        }
        _ => return HttpResponse::NotFound().finish(),
    };

    // TODO: add metrics here
    let mut response = match &result {
        Ok(_) => HttpResponse::Ok(),
        Err(err) => match &err.error_struct {
            Some(RpcErrorKind::RequestValidationError(_)) => HttpResponse::BadRequest(),
            Some(RpcErrorKind::HandlerError(error_struct)) => {
                if error_struct["name"] == "TIMEOUT_ERROR" {
                    HttpResponse::RequestTimeout()
                } else {
                    HttpResponse::Ok()
                }
            }
            Some(RpcErrorKind::InternalError(_)) => HttpResponse::InternalServerError(),
            None => HttpResponse::Ok(),
        },
    };

    response.json(Message::response(id, result.map_err(RpcError::from)))
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

    let server_context = web::Data::new(
        config::ServerContext::init(rpc_server_config.clone(), near_rpc_client.clone()).await?,
    );

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

    // TODO: decide what to do with metrics here
    // Insert all rpc methods to the hashmap after init the server
    // metrics::RPC_METHODS.insert(rpc.router.routers()).await;

    HttpServer::new(move || {
        let cors = actix_cors::Cors::permissive();

        App::new()
            .wrap(cors)
            .wrap(tracing_actix_web::TracingLogger::default())
            .wrap(middlewares::RequestsCounters)
            .app_data(server_context.clone())
            .service(web::scope("/").route("", web::post().to(rpc_handler)))
            .service(metrics::get_metrics)
            .service(health::get_health_status)
    })
    .bind(format!("0.0.0.0:{:0>5}", server_port))?
    .run()
    .await?;

    Ok(())
}
