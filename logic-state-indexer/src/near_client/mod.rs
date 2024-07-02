//! NEAR Client is a Trait that defines the interface for retrieving the necessary data from the NEAR Protocol.
//! The data is available on the node and can be retrieved via JSON-RPC calls that are using ViewClientActor and ClientActor under the hood.
//! We degine the trait to abstract the actual implementation of the client and make it possible to switch between different implementations.

pub trait NearClient {
    /// Returns the final block height from the NEAR Protocol or an error if the call fails.
    fn final_block_height(&self) -> impl std::future::Future<Output = anyhow::Result<u64>> + Send;

    /// Returns the ProtocolConfigView from the NEAR Protocol or an error if the call fails.
    fn protocol_config(
        &self,
    ) -> impl std::future::Future<Output = anyhow::Result<near_chain_configs::ProtocolConfigView>> + Send;

    /// Returns the current epoch information from the NEAR Protocol or an error if the call fails.
    fn validators_by_epoch_id(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
    ) -> impl std::future::Future<Output = anyhow::Result<near_primitives::views::EpochValidatorInfo>>
           + Send;
}

/// NEAR JSON-RPC Client is an implementation of the NearClient trait that uses the JSON-RPC calls
/// to retrieve the necessary data from the NEAR Protocol.
#[derive(Debug, Clone)]
pub struct NearJsonRpc {
    client: near_jsonrpc_client::JsonRpcClient,
}

impl NearJsonRpc {
    pub fn new(client: near_jsonrpc_client::JsonRpcClient) -> Self {
        Self { client }
    }
}

impl NearClient for NearJsonRpc {
    async fn final_block_height(&self) -> anyhow::Result<u64> {
        let block_height = self
            .client
            .call(near_jsonrpc_client::methods::block::RpcBlockRequest {
                block_reference: near_primitives::types::BlockReference::Finality(
                    near_primitives::types::Finality::Final,
                ),
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get final block via JSON-RPC: {:?}", e))?;
        Ok(block_height.header.height)
    }

    async fn protocol_config(&self) -> anyhow::Result<near_chain_configs::ProtocolConfigView> {
        let protocol_config = self
            .client
            .call(near_jsonrpc_client::methods::EXPERIMENTAL_protocol_config::RpcProtocolConfigRequest {
                block_reference: near_primitives::types::BlockReference::Finality(
                    near_primitives::types::Finality::Final,
                ),
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get protocol config: {:?}", e))?;
        Ok(protocol_config)
    }

    async fn validators_by_epoch_id(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
        let validators_info = self
            .client
            .call(
                near_jsonrpc_client::methods::validators::RpcValidatorRequest {
                    epoch_reference: near_primitives::types::EpochReference::EpochId(
                        near_primitives::types::EpochId(epoch_id),
                    ),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get validators: {:?}", e))?;
        Ok(validators_info)
    }
}
