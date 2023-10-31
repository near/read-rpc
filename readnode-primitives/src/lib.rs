use borsh::{BorshDeserialize, BorshSerialize};
use near_indexer_primitives::{views, IndexerTransactionWithOutcome};
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct TransactionKey {
    pub transaction_hash: String,
    pub block_height: u64,
}

impl TransactionKey {
    pub fn new(transaction_hash: String, block_height: u64) -> Self {
        Self {
            transaction_hash,
            block_height,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct CollectingTransactionDetails {
    pub transaction: views::SignedTransactionView,
    pub receipts: Vec<views::ReceiptView>,
    pub execution_outcomes: Vec<views::ExecutionOutcomeWithIdView>,
    // block_height using to handle transaction hash collisions
    pub block_height: u64,
}

impl CollectingTransactionDetails {
    pub fn from_indexer_tx(transaction: IndexerTransactionWithOutcome, block_height: u64) -> Self {
        Self {
            transaction: transaction.transaction.clone(),
            receipts: vec![],
            execution_outcomes: vec![transaction.outcome.execution_outcome],
            block_height,
        }
    }

    /// Build unique transaction key based on transaction_hash and block_height
    /// Help to handle transaction hash collisions
    pub fn transaction_key(&self) -> TransactionKey {
        TransactionKey::new(self.transaction.hash.clone().to_string(), self.block_height)
    }

    pub fn to_final_transaction_result(&self) -> anyhow::Result<TransactionDetails> {
        let mut outcomes = self.execution_outcomes.clone();
        let mut looking_for_id = self.transaction.hash;
        let num_outcomes = outcomes.len();
        let finale_status = outcomes.iter().find_map(|outcome_with_id| {
            if outcome_with_id.id == looking_for_id {
                match &outcome_with_id.outcome.status {
                    views::ExecutionStatusView::Unknown if num_outcomes == 1 => {
                        Some(views::FinalExecutionStatus::NotStarted)
                    }
                    views::ExecutionStatusView::Unknown => {
                        Some(views::FinalExecutionStatus::Started)
                    }
                    views::ExecutionStatusView::Failure(e) => {
                        Some(views::FinalExecutionStatus::Failure(e.clone()))
                    }
                    views::ExecutionStatusView::SuccessValue(v) => {
                        Some(views::FinalExecutionStatus::SuccessValue(v.clone()))
                    }
                    views::ExecutionStatusView::SuccessReceiptId(id) => {
                        looking_for_id = *id;
                        None
                    }
                }
            } else {
                None
            }
        });
        match finale_status {
            Some(status) => {
                let receipts_outcome = outcomes.split_off(1);
                let transaction_outcome = outcomes.pop().unwrap();
                Ok(TransactionDetails {
                    receipts: self.receipts.clone(),
                    receipts_outcome,
                    status,
                    transaction: self.transaction.clone(),
                    transaction_outcome,
                })
            }
            None => anyhow::bail!("Results should resolve to a final outcome"),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct TransactionDetails {
    pub receipts: Vec<views::ReceiptView>,
    pub receipts_outcome: Vec<views::ExecutionOutcomeWithIdView>,
    pub status: views::FinalExecutionStatus,
    pub transaction: views::SignedTransactionView,
    pub transaction_outcome: views::ExecutionOutcomeWithIdView,
}

impl TransactionDetails {
    pub fn to_final_execution_outcome(&self) -> views::FinalExecutionOutcomeView {
        views::FinalExecutionOutcomeView {
            status: self.status.clone(),
            transaction: self.transaction.clone(),
            transaction_outcome: self.transaction_outcome.clone(),
            receipts_outcome: self.receipts_outcome.clone(),
        }
    }

    pub fn to_final_execution_outcome_with_receipts(
        &self,
    ) -> views::FinalExecutionOutcomeWithReceiptView {
        views::FinalExecutionOutcomeWithReceiptView {
            final_outcome: self.to_final_execution_outcome(),
            receipts: self
                .receipts
                .iter()
                // We need to filter out the local receipts
                // (which is the receipt transaction was converted into if transaction's signer and receiver are the same)
                // because NEAR JSON RPC doesn't return them. We need to filter them out because they are not
                // expected to be present in the final response from the JSON RPC.
                .filter(|receipt|
                    if self.transaction.signer_id == self.transaction.receiver_id {
                        receipt.receipt_id != *self
                    .transaction_outcome
                    .outcome
                    .receipt_ids
                    .first()
                    .expect("Transaction ExecutionOutcome must have exactly one receipt id in `receipt_ids`")
                    } else {
                        true
                    }
                )
                .cloned()
                .collect(),
        }
    }
}
