#[async_trait::async_trait]
pub trait TxCollectingStorage {
    async fn push_receipt_to_watching_list(
        &self,
        receipt_id: String,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()>;

    async fn remove_receipt_from_watching_list(&self, receipt_id: &str) -> anyhow::Result<()>;

    async fn receipts_transaction_hash_count(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64>;

    async fn update_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()>;

    async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()>;

    async fn get_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails>;

    async fn move_tx_to_save(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()>;

    async fn get_transaction_hash_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionKey>;

    async fn transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>>;

    async fn push_outcome_and_receipt(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()>;
}
