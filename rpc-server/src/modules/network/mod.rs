pub mod methods;

async fn parse_validator_request(
    value: serde_json::Value,
) -> anyhow::Result<near_jsonrpc_primitives::types::validator::RpcValidatorRequest> {
    let epoch_reference = match value[0].clone() {
        serde_json::Value::Null => near_primitives::types::EpochReference::Latest,
        _ => {
            let (block_id,) = serde_json::from_value::<(near_primitives::types::BlockId,)>(value)?;
            near_primitives::types::EpochReference::BlockId(block_id)
        }
    };
    Ok(near_jsonrpc_primitives::types::validator::RpcValidatorRequest { epoch_reference })
}
