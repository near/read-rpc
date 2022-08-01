use jsonrpc_v2::{Data, Error, Params, Server};
use config::Cli;
use clap::Parser;
mod config;


async fn proxy_block(
    data: Data<Cli>,
    Params(params): Params<serde_json::Value>
) -> Result<serde_json::Value, Error> {
    let mut post_data = serde_json::json!(
        {
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "block",
        }
    );
    post_data["params"] = params;
    let client = reqwest::Client::new();
    let resp = client.post(&data.rpc_url.to_string()).json(&post_data).send().await.unwrap();
    let resp_json: serde_json::Value = resp.json().await.unwrap();
    return Ok(resp_json["result"].clone());
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli_args = Cli::parse();
    let rpc = Server::new()
        .with_data(Data::new(cli_args))
        .with_method("block", proxy_block)
        .finish();

    actix_web::HttpServer::new(move || {
        let rpc = rpc.clone();
        actix_web::App::new().service(
            actix_web::web::service("/")
                .guard(actix_web::guard::Post())
                .finish(rpc.into_web_service()),
        )
    }).bind("0.0.0.0:8888")?.run().await
}
