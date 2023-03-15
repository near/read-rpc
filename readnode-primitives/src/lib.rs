use borsh::{BorshDeserialize, BorshSerialize};
use near_indexer_primitives::{views, IndexerTransactionWithOutcome};
use serde::{Deserialize, Serialize};

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct TransactionDetails {
    pub receipts: Vec<views::ReceiptView>,
    pub receipts_outcome: Vec<views::ExecutionOutcomeWithIdView>,
    pub status: views::ExecutionStatusView,
    pub transaction: views::SignedTransactionView,
    pub transaction_outcome: views::ExecutionOutcomeWithIdView,
}

impl TransactionDetails {
    pub fn from_indexer_tx(transaction: IndexerTransactionWithOutcome) -> Self {
        Self {
            receipts: vec![],
            receipts_outcome: vec![],
            status: transaction.outcome.execution_outcome.outcome.status.clone(),
            transaction: transaction.transaction.clone(),
            transaction_outcome: transaction.outcome.execution_outcome.clone(),
        }
    }
}
