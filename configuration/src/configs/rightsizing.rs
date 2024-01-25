use near_indexer_primitives::views::StateChangeValueView;
use serde_derive::Deserialize;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct RightsizingConfig {
    #[serde(default)]
    pub tracked_accounts: Vec<near_indexer_primitives::types::AccountId>,
    #[serde(default)]
    pub tracked_changes: Vec<ChangeType>,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    State,
    AccessKey,
    ContractCode,
}

impl RightsizingConfig {
    fn is_indexed_account(&self, account: &near_indexer_primitives::types::AccountId) -> bool {
        if self.tracked_accounts.is_empty() {
            true
        } else {
            self.tracked_accounts.contains(account)
        }
    }

    fn is_indexed_change(&self, change_type: ChangeType) -> bool {
        if self.tracked_changes.is_empty() {
            true
        } else {
            self.tracked_changes.contains(&change_type)
        }
    }

    pub fn state_should_be_indexed(&self, state_change_value: &StateChangeValueView) -> bool {
        match state_change_value {
            StateChangeValueView::DataUpdate { account_id, .. }
            | StateChangeValueView::DataDeletion { account_id, .. } => {
                self.is_indexed_account(account_id) && self.is_indexed_change(ChangeType::State)
            }
            StateChangeValueView::AccessKeyUpdate { account_id, .. }
            | StateChangeValueView::AccessKeyDeletion { account_id, .. } => {
                self.is_indexed_account(account_id) && self.is_indexed_change(ChangeType::AccessKey)
            }
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id, .. } => {
                self.is_indexed_account(account_id)
                    && self.is_indexed_change(ChangeType::ContractCode)
            }
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id, .. } => {
                self.is_indexed_account(account_id)
            }
        }
    }

    /// For now we index only transactions that are related to indexed accounts as signer_id and receiver_id
    /// But we know about transactions which include indexing accounts not only as signer_id and receiver_id
    /// but also include indexing accounts in a args of a function call
    /// So in future we should to index such transactions too if it will be needed
    pub fn tx_should_be_indexed(
        &self,
        transaction: &near_indexer_primitives::IndexerTransactionWithOutcome,
    ) -> bool {
        self.is_indexed_account(&transaction.transaction.signer_id)
            || self.is_indexed_account(&transaction.transaction.receiver_id)
    }
}
