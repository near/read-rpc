use clap::Parser;
use config::{Opts, ServerContext};
use dotenv::dotenv;
use jsonrpc_v2::{Data, Server};
use utils::{prepare_db_client, prepare_s3_client};

mod config;
mod errors;
mod modules;
mod utils;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let opts: Opts = Opts::parse();
    let state = ServerContext {
        s3_client: prepare_s3_client(
            &opts.access_key_id,
            &opts.secret_access_key,
            opts.region.clone(),
        )
        .await,
        db_client: prepare_db_client(&opts.database_url).await,
        s3_bucket_name: opts.s3_bucket_name,
    };

    let rpc = Server::new()
        .with_data(Data::new(state))
        .with_method("query", modules::queries::methods::query)
        .with_method("block", modules::blocks::methods::block)
        .with_method("chunk", modules::blocks::methods::chunk)
        .with_method("tx", modules::transactions::methods::tx_status_common)
        .with_method(
            "broadcast_tx_async",
            modules::transactions::methods::send_tx_async,
        )
        .with_method(
            "broadcast_tx_commit",
            modules::transactions::methods::send_tx_commit,
        )
        .with_method("gas_price", modules::gas::methods::gas_price)
        .with_method("status", modules::network::methods::status)
        .with_method("network_info", modules::network::methods::network_info)
        .with_method("validators", modules::network::methods::validators)
        .finish();

    actix_web::HttpServer::new(move || {
        let rpc = rpc.clone();
        actix_web::App::new().service(
            actix_web::web::service("/")
                .guard(actix_web::guard::Post())
                .finish(rpc.into_web_service()),
        )
    })
    .bind("0.0.0.0:8888")?
    .run()
    .await
}
