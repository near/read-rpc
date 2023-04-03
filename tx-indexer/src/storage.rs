use near_indexer_primitives::IndexerExecutionOutcomeWithReceipt;

const STORAGE: &str = "storage_tx";

pub struct HashStorage {
    transactions:
        std::collections::HashMap<String, readnode_primitives::CollectingTransactionDetails>,
    receipts_counters: std::collections::HashMap<String, u64>,
    receipts_watching_list: std::collections::HashMap<String, String>,
    transactions_to_save:
        std::collections::HashMap<String, readnode_primitives::CollectingTransactionDetails>,
}

impl HashStorage {
    pub(crate) fn new() -> Self {
        Self {
            transactions: std::collections::HashMap::new(),
            receipts_counters: std::collections::HashMap::new(),
            receipts_watching_list: std::collections::HashMap::new(),
            transactions_to_save: std::collections::HashMap::new(),
        }
    }

    pub fn push_receipt_to_watching_list(
        &mut self,
        receipt_id: String,
        cache_value: String,
    ) -> anyhow::Result<()> {
        let counter = self
            .receipts_counters
            .entry(cache_value.to_string())
            .or_insert(0);
        *counter += 1;
        self.receipts_watching_list.insert(receipt_id, cache_value);
        Ok(())
    }

    pub fn remove_receipt_from_watching_list(
        &mut self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>> {
        if let Some(cache_value) = self.receipts_watching_list.remove(receipt_id) {
            let counter = self
                .receipts_counters
                .entry(cache_value.clone())
                .or_insert(0);
            *counter -= 1;
            Ok(Some(cache_value))
        } else {
            Ok(None)
        }
    }

    pub fn receipts_transaction_hash_count(&self, transaction_hash: &str) -> anyhow::Result<u64> {
        self.receipts_counters
            .get(transaction_hash)
            .map(|x| *x)
            .ok_or(anyhow::anyhow!("No such transaction hash"))
    }

    pub fn set_tx(
        &mut self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.transactions.insert(
            transaction_details.transaction.hash.to_string(),
            transaction_details,
        );
        Ok(())
    }

    pub fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> Option<readnode_primitives::CollectingTransactionDetails> {
        self.transactions.get(transaction_hash).map(|x| x.clone())
    }

    pub fn push_tx_to_save(
        &mut self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.transactions_to_save.insert(
            transaction_details.transaction.hash.to_string(),
            transaction_details,
        );
        Ok(())
    }

    pub fn transactions_to_save(
        &mut self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let transaction = self
            .transactions_to_save
            .values()
            .map(|x| x.clone())
            .collect();
        let keys: Vec<_> = self
            .transactions_to_save
            .keys()
            .map(|x| x.clone())
            .collect();
        for transaction in keys {
            self.transactions_to_save.remove(&transaction);
            tracing::debug!(target: STORAGE, "-T {}", transaction);
        }
        Ok(transaction)
    }

    pub fn push_outcome_and_receipt(
        &mut self,
        transaction_hash: &str,
        indexer_execution_outcome_with_receipt: IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        if let Some(mut transaction_details) = self.get_tx(transaction_hash) {
            self.remove_receipt_from_watching_list(
                &indexer_execution_outcome_with_receipt
                    .receipt
                    .receipt_id
                    .to_string(),
            )?;
            transaction_details
                .receipts
                .push(indexer_execution_outcome_with_receipt.receipt);
            transaction_details
                .execution_outcomes
                .push(indexer_execution_outcome_with_receipt.execution_outcome);
            let transaction_receipts_watching_count =
                self.receipts_transaction_hash_count(transaction_hash)?;
            if transaction_receipts_watching_count == 0 {
                self.push_tx_to_save(transaction_details.clone())?;
                self.transactions.remove(transaction_hash);
                self.receipts_counters.remove(transaction_hash);
            } else {
                self.set_tx(transaction_details.clone())?;
            }
        } else {
            tracing::error!(
                target: STORAGE,
                "No such transaction hash {}",
                transaction_hash
            );
        }
        Ok(())
    }
}
