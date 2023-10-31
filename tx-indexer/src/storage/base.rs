#[async_trait::async_trait]
pub trait TxCollectingStorage {
    // In 2021 on the nearcore side an unfortunate situation/bug occurred.
    // For some transactions Receipts haven't been created and were considered as _missing_.
    // It was fixed with the patch and the protocol upgrade.
    // The protocol team decided to include those Receipts in the first block of the "next" epoch.
    // However, the number of missing receipts was 383 and they didn't fit into a single block,
    // so they were included in the two sequential blocks: 47317863 and 47317864
    // See the [PR#4248](https://github.com/near/nearcore/pull/4248)
    // This method helps to collect the transactions we miss.
    async fn restore_transaction_by_receipt_id(&self, receipt_id: &str) -> anyhow::Result<()>;

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
