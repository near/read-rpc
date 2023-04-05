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
        transaction_hash: String,
    ) -> anyhow::Result<()> {
        let receipts_counter = self
            .receipts_counters
            .entry(transaction_hash.to_string())
            .or_insert(0);
        *receipts_counter += 1;
        self.receipts_watching_list
            .insert(receipt_id.clone(), transaction_hash.clone());
        tracing::debug!(target: STORAGE, "+R {} - {}", receipt_id, transaction_hash);
        Ok(())
    }

    pub fn remove_receipt_from_watching_list(
        &mut self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>> {
        if let Some(transaction_hash) = self.receipts_watching_list.remove(receipt_id) {
            if let Some(receipts_counter) = self.receipts_counters.get_mut(&transaction_hash) {
                *receipts_counter -= 1;
            }
            tracing::debug!(target: STORAGE, "-R {} - {}", receipt_id, transaction_hash);
            Ok(Some(transaction_hash))
        } else {
            Ok(None)
        }
    }

    pub fn receipts_transaction_hash_count(&self, transaction_hash: &str) -> anyhow::Result<u64> {
        self.receipts_counters
            .get(transaction_hash)
            .map(|count| *count)
            .ok_or(anyhow::anyhow!(
                "No such transaction hash {}",
                transaction_hash
            ))
    }

    pub fn set_tx(
        &mut self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.transactions.insert(
            transaction_details.transaction.hash.to_string(),
            transaction_details,
        );
        tracing::debug!(target: STORAGE, "+T {}", transaction_hash,);
        Ok(())
    }

    pub fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> Option<readnode_primitives::CollectingTransactionDetails> {
        self.transactions.get(transaction_hash).cloned()
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

    pub fn get_transaction_hash_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(self.receipts_watching_list.get(receipt_id).cloned())
    }

    pub fn transactions_to_save(
        &mut self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut transactions = vec![];
        let mut transactions_hashes = vec![];
        for (transaction_hash, transaction_details) in self.transactions_to_save.iter() {
            transactions.push(transaction_details.clone());
            transactions_hashes.push(transaction_hash.clone());
        }
        for transaction_hash in transactions_hashes.iter() {
            self.transactions_to_save.remove(transaction_hash);
        }
        Ok(transactions)
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
                tracing::debug!(target: STORAGE, "-T {}", transaction_hash);
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
