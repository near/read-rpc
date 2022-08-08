use jsonrpc_v2::{Error, Params};

pub async fn query(
    Params(_params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    // near_jsonrpc_primitives::types::query::RpcQueryError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}
