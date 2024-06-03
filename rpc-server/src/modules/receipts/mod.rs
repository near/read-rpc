pub mod methods;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcReceiptRecordResponse {
    pub receipt_id: near_indexer_primitives::CryptoHash,
    pub parent_transaction_hash: near_indexer_primitives::CryptoHash,
    pub block_height: near_indexer_primitives::types::BlockHeight,
    pub shard_id: near_indexer_primitives::types::ShardId,
}

impl From<readnode_primitives::ReceiptRecord> for RpcReceiptRecordResponse {
    fn from(receipt: readnode_primitives::ReceiptRecord) -> Self {
        Self {
            receipt_id: receipt.receipt_id,
            parent_transaction_hash: receipt.parent_transaction_hash,
            block_height: receipt.block_height,
            shard_id: receipt.shard_id,
        }
    }
}
