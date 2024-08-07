use num_traits::ToPrimitive;
use std::convert::TryFrom;
use std::str::FromStr;

use near_indexer_primitives::{views, CryptoHash, IndexerTransactionWithOutcome};

#[derive(
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
pub struct ExecutionOutcomeWithReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: views::ReceiptView,
}

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

#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
)]
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

    pub fn final_status(&self) -> Option<views::FinalExecutionStatus> {
        let mut looking_for_id = self.transaction.hash;
        let num_outcomes = self.execution_outcomes.len();
        self.execution_outcomes.iter().find_map(|outcome_with_id| {
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
        })
    }

    pub fn to_final_transaction_result(&self) -> anyhow::Result<TransactionDetails> {
        let mut outcomes = self.execution_outcomes.clone();
        match self.final_status() {
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

impl From<CollectingTransactionDetails> for TransactionDetails {
    fn from(tx: CollectingTransactionDetails) -> Self {
        let mut outcomes = tx.execution_outcomes.clone();
        let receipts_outcome = outcomes.split_off(1);
        let transaction_outcome = outcomes.pop().unwrap();
        // Execution status defined by nearcore/chain.rs:get_final_transaction_result
        // FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet
        // FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution
        // FinalExecutionStatus::Failure - the result of the first leaf receipt_id
        // FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id
        let status = tx
            .final_status()
            .unwrap_or(views::FinalExecutionStatus::NotStarted);
        Self {
            receipts: tx.receipts,
            receipts_outcome,
            status,
            transaction: tx.transaction,
            transaction_outcome,
        }
    }
}

#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
)]
pub struct TransactionDetails {
    pub receipts: Vec<views::ReceiptView>,
    pub receipts_outcome: Vec<views::ExecutionOutcomeWithIdView>,
    pub status: views::FinalExecutionStatus,
    pub transaction: views::SignedTransactionView,
    pub transaction_outcome: views::ExecutionOutcomeWithIdView,
}

// Since https://github.com/near/nearcore/pull/11228
// the SignedTransactionView has been changed to include `priority_fee` field
// which is not present in the old version of the SignedTransactionView
// This change is not backward compatible and we need to handle it
// by deserializing the old version of the SignedTransactionView and converting it to the new version
#[derive(borsh::BorshDeserialize, Debug, Clone)]
pub struct SignedTransactionViewWithOutPriorityFee {
    pub signer_id: near_indexer_primitives::types::AccountId,
    pub public_key: near_crypto::PublicKey,
    pub nonce: near_indexer_primitives::types::Nonce,
    pub receiver_id: near_indexer_primitives::types::AccountId,
    pub actions: Vec<views::ActionView>,
    pub signature: near_crypto::Signature,
    pub hash: CryptoHash,
}

impl From<SignedTransactionViewWithOutPriorityFee> for views::SignedTransactionView {
    fn from(tx: SignedTransactionViewWithOutPriorityFee) -> Self {
        Self {
            signer_id: tx.signer_id,
            public_key: tx.public_key,
            nonce: tx.nonce,
            receiver_id: tx.receiver_id,
            actions: tx.actions,
            priority_fee: 0, // priority_fee for Transaction::V0 => None,
            signature: tx.signature,
            hash: tx.hash,
        }
    }
}

// Since https://github.com/near/nearcore/pull/10676
// the ReceiptEnumView has been changed to include `is_promise_yield` field
// which is not present in the old version of the ReceiptEnumView
// This change is not backward compatible and we need to handle it
// by deserializing the old version of the ReceiptEnumView and converting it to the new version
#[derive(borsh::BorshDeserialize, Debug, Clone)]
enum ReceiptEnumViewWithoutIsPromiseYield {
    Action {
        signer_id: near_indexer_primitives::types::AccountId,
        signer_public_key: near_crypto::PublicKey,
        gas_price: near_indexer_primitives::types::Balance,
        output_data_receivers: Vec<views::DataReceiverView>,
        input_data_ids: Vec<CryptoHash>,
        actions: Vec<views::ActionView>,
    },
    Data {
        data_id: CryptoHash,
        data: Option<Vec<u8>>,
    },
}

// Convert the old version of the ReceiptEnumView to the new version
impl From<ReceiptEnumViewWithoutIsPromiseYield> for views::ReceiptEnumView {
    fn from(receipt: ReceiptEnumViewWithoutIsPromiseYield) -> Self {
        match receipt {
            ReceiptEnumViewWithoutIsPromiseYield::Action {
                signer_id,
                signer_public_key,
                gas_price,
                output_data_receivers,
                input_data_ids,
                actions,
            } => Self::Action {
                signer_id,
                signer_public_key,
                gas_price,
                output_data_receivers,
                input_data_ids,
                actions,
                is_promise_yield: false,
            },
            ReceiptEnumViewWithoutIsPromiseYield::Data { data_id, data } => Self::Data {
                data_id,
                data,
                is_promise_resume: false,
            },
        }
    }
}

// Deserialize the old version of the ReceiptEnumView
// and convert it to the new version
#[derive(borsh::BorshDeserialize, Debug, Clone)]
struct ReceiptViewWithReceiptWithoutIsPromiseYield {
    predecessor_id: near_indexer_primitives::types::AccountId,
    receiver_id: near_indexer_primitives::types::AccountId,
    receipt_id: CryptoHash,
    receipt: ReceiptEnumViewWithoutIsPromiseYield,
}

// Convert the old version of the ReceiptView to the new version
impl From<ReceiptViewWithReceiptWithoutIsPromiseYield> for views::ReceiptView {
    fn from(receipt: ReceiptViewWithReceiptWithoutIsPromiseYield) -> Self {
        Self {
            predecessor_id: receipt.predecessor_id,
            receiver_id: receipt.receiver_id,
            receipt_id: receipt.receipt_id,
            receipt: receipt.receipt.into(),
            priority: 0, // For ReceiptV0 ReceiptPriority::NoPriority => 0
        }
    }
}

// Deserialize the old version of the TransactionDetails
// and convert it to the new version
#[derive(borsh::BorshDeserialize, Debug, Clone)]
struct TransactionDetailsWithReceiptWithoutIsPromiseYield {
    receipts: Vec<ReceiptViewWithReceiptWithoutIsPromiseYield>,
    receipts_outcome: Vec<views::ExecutionOutcomeWithIdView>,
    status: views::FinalExecutionStatus,
    transaction: views::SignedTransactionView,
    transaction_outcome: views::ExecutionOutcomeWithIdView,
}

// Convert the old version of the TransactionDetails to the new version
impl From<TransactionDetailsWithReceiptWithoutIsPromiseYield> for TransactionDetails {
    fn from(tx: TransactionDetailsWithReceiptWithoutIsPromiseYield) -> Self {
        Self {
            receipts: tx.receipts.into_iter().map(|r| r.into()).collect(),
            receipts_outcome: tx.receipts_outcome,
            status: tx.status,
            transaction: tx.transaction,
            transaction_outcome: tx.transaction_outcome,
        }
    }
}

#[derive(borsh::BorshDeserialize, Debug, Clone)]
struct ReceiptViewWithoutPriority {
    pub predecessor_id: near_indexer_primitives::types::AccountId,
    pub receiver_id: near_indexer_primitives::types::AccountId,
    pub receipt_id: CryptoHash,
    pub receipt: views::ReceiptEnumView,
}

impl From<ReceiptViewWithoutPriority> for views::ReceiptView {
    fn from(receipt: ReceiptViewWithoutPriority) -> Self {
        Self {
            predecessor_id: receipt.predecessor_id,
            receiver_id: receipt.receiver_id,
            receipt_id: receipt.receipt_id,
            receipt: receipt.receipt,
            priority: 0,
        }
    }
}

#[derive(borsh::BorshDeserialize, Debug, Clone)]
struct TransactionDetailsWithReceiptWithoutPriorityFee {
    receipts: Vec<ReceiptViewWithoutPriority>,
    receipts_outcome: Vec<views::ExecutionOutcomeWithIdView>,
    status: views::FinalExecutionStatus,
    transaction: SignedTransactionViewWithOutPriorityFee,
    transaction_outcome: views::ExecutionOutcomeWithIdView,
}

// Convert the old version of the TransactionDetails to the new version
impl From<TransactionDetailsWithReceiptWithoutPriorityFee> for TransactionDetails {
    fn from(tx: TransactionDetailsWithReceiptWithoutPriorityFee) -> Self {
        Self {
            receipts: tx.receipts.into_iter().map(|r| r.into()).collect(),
            receipts_outcome: tx.receipts_outcome,
            status: tx.status,
            transaction: tx.transaction.into(),
            transaction_outcome: tx.transaction_outcome,
        }
    }
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

    // Deserialize TransactionDetails from bytes
    // If the deserialization fails, try to deserialize the old version of the TransactionDetails
    // and convert it to the new version
    // This is needed to handle the backward incompatible changes in the TransactionDetails
    // https://github.com/near/nearcore/pull/10676/files#diff-1e4fc99d32e48420a9bd37050fa1412758cba37825851edea40cbdfcab406944R1927
    pub fn borsh_deserialize(data: &[u8]) -> anyhow::Result<Self> {
        match borsh::from_slice::<Self>(data) {
            Ok(tx_details) => Ok(tx_details),
            Err(_) => {
                match borsh::from_slice::<TransactionDetailsWithReceiptWithoutPriorityFee>(data) {
                    Ok(tx_details) => Ok(tx_details.into()),
                    Err(_) => {
                        match borsh::from_slice::<TransactionDetailsWithReceiptWithoutIsPromiseYield>(
                            data,
                        ) {
                            Ok(tx_details) => Ok(tx_details.into()),
                            Err(err) => Err(anyhow::anyhow!(
                                "Failed to deserialize TransactionDetails: {}",
                                err
                            )),
                        }
                    }
                }
            }
        }
    }
}

pub type StateKey = Vec<u8>;
pub type StateValue = Vec<u8>;
pub struct BlockHeightShardId(pub u64, pub u64);
pub struct QueryData<T: borsh::BorshDeserialize> {
    pub data: T,
    // block_height and block_hash we return here represents the moment
    // when the data was last updated in the database
    // We used to return it in the `QueryResponse` but it was replaced with
    // the logic that corresponds the logic of the `nearcore` RPC API
    pub block_height: near_indexer_primitives::types::BlockHeight,
    pub block_hash: CryptoHash,
}

pub struct ReceiptRecord {
    pub receipt_id: CryptoHash,
    pub parent_transaction_hash: CryptoHash,
    pub block_height: near_indexer_primitives::types::BlockHeight,
    pub shard_id: near_indexer_primitives::types::ShardId,
}

pub struct BlockRecord {
    pub height: u64,
    pub hash: CryptoHash,
}

#[derive(Debug)]
pub struct EpochValidatorsInfo {
    pub epoch_id: CryptoHash,
    pub epoch_height: u64,
    pub epoch_start_height: u64,
    pub validators_info: views::EpochValidatorInfo,
}

#[derive(Debug)]
pub struct IndexedEpochInfo {
    pub epoch_id: CryptoHash,
    pub epoch_height: u64,
    pub epoch_start_height: u64,
    pub validators_info: views::EpochValidatorInfo,
}

#[derive(Debug)]
pub struct IndexedEpochInfoWithPreviousAndNextEpochId {
    pub previous_epoch_id: Option<CryptoHash>,
    pub epoch_info: IndexedEpochInfo,
    pub next_epoch_id: CryptoHash,
}

// TryFrom impls for defined types

impl<T> TryFrom<(T, T)> for BlockHeightShardId
where
    T: ToPrimitive,
{
    type Error = anyhow::Error;

    fn try_from(value: (T, T)) -> Result<Self, Self::Error> {
        let stored_at_block_height = value
            .0
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `stored_at_block_height` to u64"))?;

        let parsed_shard_id = value
            .1
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `shard_id` to u64"))?;

        Ok(BlockHeightShardId(stored_at_block_height, parsed_shard_id))
    }
}

impl<T>
    TryFrom<(
        Vec<u8>,
        near_indexer_primitives::types::BlockHeight,
        CryptoHash,
    )> for QueryData<T>
where
    T: borsh::BorshDeserialize,
{
    type Error = anyhow::Error;

    fn try_from(
        value: (
            Vec<u8>,
            near_indexer_primitives::types::BlockHeight,
            CryptoHash,
        ),
    ) -> Result<Self, Self::Error> {
        let data = T::try_from_slice(&value.0)?;

        Ok(Self {
            data,
            block_height: value.1,
            block_hash: value.2,
        })
    }
}

impl<T> TryFrom<(String, String, T, T)> for ReceiptRecord
where
    T: ToPrimitive,
{
    type Error = anyhow::Error;

    fn try_from(value: (String, String, T, T)) -> Result<Self, Self::Error> {
        let receipt_id = CryptoHash::from_str(&value.0).map_err(|err| {
            anyhow::anyhow!("Failed to parse `receipt_id` to CryptoHash: {}", err)
        })?;
        let parent_transaction_hash = CryptoHash::from_str(&value.1).map_err(|err| {
            anyhow::anyhow!(
                "Failed to parse `parent_transaction_hash` to CryptoHash: {}",
                err
            )
        })?;
        let block_height = value
            .2
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))?;
        let shard_id = value
            .3
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `shard_id` to u64"))?;

        Ok(ReceiptRecord {
            receipt_id,
            parent_transaction_hash,
            block_height,
            shard_id,
        })
    }
}

impl<T> TryFrom<(String, T)> for BlockRecord
where
    T: ToPrimitive,
{
    type Error = anyhow::Error;

    fn try_from(value: (String, T)) -> Result<Self, Self::Error> {
        let height = value
            .1
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))?;
        let hash = CryptoHash::from_str(&value.0).map_err(|err| {
            anyhow::anyhow!("Failed to parse `block_hash` to CryptoHash: {}", err)
        })?;

        Ok(BlockRecord { height, hash })
    }
}
