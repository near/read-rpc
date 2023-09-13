#[async_trait::async_trait]
pub trait TxCollectingStorage {
    async fn push_receipt_to_watching_list(
        &mut self,
        receipt_id: String,
        transaction_hash: String,
    ) -> anyhow::Result<()>;

    async fn remove_receipt_from_watching_list(
        &mut self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>>;

    async fn receipts_transaction_hash_count(&self, transaction_hash: &str) -> anyhow::Result<u64>;

    async fn set_tx(
        &mut self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()>;

    async fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> Option<readnode_primitives::CollectingTransactionDetails>;

    async fn push_tx_to_save(
        &mut self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()>;

    async fn get_transaction_hash_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>>;

    async fn transactions_to_save(
        &mut self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>>;

    async fn push_outcome_and_receipt(
        &mut self,
        transaction_hash: &str,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()>;
}
