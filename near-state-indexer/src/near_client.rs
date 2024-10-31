use actix::Addr;
use near_indexer::near_primitives;
use near_o11y::WithSpanContextExt;

#[derive(Clone, Debug)]
pub(crate) struct NearViewClient {
    view_client: Addr<near_client::ViewClientActor>,
}

impl NearViewClient {
    pub fn new(view_client: Addr<near_client::ViewClientActor>) -> Self {
        Self { view_client }
    }
}

impl crate::NearClient for NearViewClient {
    async fn final_block_height(&self) -> anyhow::Result<u64> {
        let block = self
            .view_client
            .send(
                near_client::GetBlock(near_primitives::types::BlockReference::Finality(
                    near_primitives::types::Finality::Final,
                ))
                .with_span_context(),
            )
            .await??;
        Ok(block.header.height)
    }

    async fn validators_by_epoch_id(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
        Ok(self
            .view_client
            .send(
                near_client::GetValidatorInfo {
                    epoch_reference: near_primitives::types::EpochReference::EpochId(
                        near_primitives::types::EpochId(epoch_id),
                    ),
                }
                .with_span_context(),
            )
            .await??)
    }
}
