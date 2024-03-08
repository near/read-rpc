use near_primitives::views::{StateChangeCauseView, StateChangeValueView};

pub mod methods;
pub mod utils;

#[derive(Clone, Copy, Debug)]
pub struct CacheBlock {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_timestamp: u64,
    pub gas_price: near_primitives::types::Balance,
    pub latest_protocol_version: near_primitives::types::ProtocolVersion,
    pub chunks_included: u64,
    pub state_root: near_primitives::hash::CryptoHash,
    pub epoch_id: near_primitives::hash::CryptoHash,
}

impl From<&near_primitives::views::BlockView> for CacheBlock {
    fn from(block: &near_primitives::views::BlockView) -> Self {
        Self {
            block_hash: block.header.hash,
            block_height: block.header.height,
            block_timestamp: block.header.timestamp,
            gas_price: block.header.gas_price,
            latest_protocol_version: block.header.latest_protocol_version,
            chunks_included: block.header.chunks_included,
            state_root: block.header.prev_state_root,
            epoch_id: block.header.epoch_id,
        }
    }
}

#[derive(Debug)]
pub struct BlockInfo {
    pub block_cache: CacheBlock,
    pub stream_message: near_indexer_primitives::StreamerMessage,
}

pub struct BlockStateChangeValue {
    pub value: Option<Vec<u8>>,
}

impl BlockStateChangeValue {
    pub async fn to_account(&self) -> anyhow::Result<near_primitives::account::Account> {
        let account = borsh::from_slice::<near_primitives::account::Account>(
            self.value
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Account deleted"))?,
        )?;
        Ok(account)
    }

    pub async fn to_access_key(&self) -> anyhow::Result<near_primitives::account::AccessKey> {
        let access_key = borsh::from_slice::<near_primitives::account::AccessKey>(
            self.value
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Access key deleted"))?,
        )?;
        Ok(access_key)
    }

    pub async fn to_contract_code(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self
            .value
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Contract code deleted"))?
            .clone())
    }
    // TODO: handle added and deleted keys
    // pub async fn to_data(&self) -> anyhow::Result<Vec<u8>> {
    //     Ok(self
    //         .value
    //         .as_ref()
    //         .ok_or_else(|| anyhow::anyhow!("Data deleted"))?
    //         .clone())
    // }
}

impl BlockInfo {
    pub fn new_from_block_view(block_view: near_primitives::views::BlockView) -> Self {
        Self {
            block_cache: CacheBlock::from(&block_view),
            stream_message: near_indexer_primitives::StreamerMessage {
                block: block_view,
                shards: vec![],
            },
        }
    }

    pub fn new_from_streamer_message(
        stream_message: near_indexer_primitives::StreamerMessage,
    ) -> Self {
        Self {
            block_cache: CacheBlock::from(&stream_message.block),
            stream_message,
        }
    }

    pub fn block_view(&self) -> near_primitives::views::BlockView {
        near_primitives::views::BlockView {
            author: self.stream_message.block.author.clone(),
            header: self.stream_message.block.header.clone(),
            chunks: self.stream_message.block.chunks.clone(),
        }
    }

    pub fn shards(&self) -> Vec<near_indexer_primitives::IndexerShard> {
        self.stream_message
            .shards
            .iter()
            .map(|shard| {
                let state_changes = shard
                    .state_changes
                    .iter()
                    .map(|state_changes| {
                        let cause = match &state_changes.cause {
                            StateChangeCauseView::NotWritableToDisk => {
                                StateChangeCauseView::NotWritableToDisk
                            }
                            StateChangeCauseView::InitialState => {
                                StateChangeCauseView::InitialState
                            }
                            StateChangeCauseView::TransactionProcessing { tx_hash } => {
                                StateChangeCauseView::TransactionProcessing { tx_hash: *tx_hash }
                            }
                            StateChangeCauseView::ActionReceiptProcessingStarted {
                                receipt_hash,
                            } => StateChangeCauseView::ActionReceiptProcessingStarted {
                                receipt_hash: *receipt_hash,
                            },
                            StateChangeCauseView::ActionReceiptGasReward { receipt_hash } => {
                                StateChangeCauseView::ActionReceiptGasReward {
                                    receipt_hash: *receipt_hash,
                                }
                            }
                            StateChangeCauseView::ReceiptProcessing { receipt_hash } => {
                                StateChangeCauseView::ReceiptProcessing {
                                    receipt_hash: *receipt_hash,
                                }
                            }
                            StateChangeCauseView::PostponedReceipt { receipt_hash } => {
                                StateChangeCauseView::PostponedReceipt {
                                    receipt_hash: *receipt_hash,
                                }
                            }
                            StateChangeCauseView::UpdatedDelayedReceipts => {
                                StateChangeCauseView::UpdatedDelayedReceipts
                            }
                            StateChangeCauseView::ValidatorAccountsUpdate => {
                                StateChangeCauseView::ValidatorAccountsUpdate
                            }
                            StateChangeCauseView::Migration => StateChangeCauseView::Migration,
                            StateChangeCauseView::Resharding => StateChangeCauseView::Resharding,
                        };
                        let value = match &state_changes.value {
                            StateChangeValueView::AccountUpdate {
                                account_id,
                                account,
                            } => StateChangeValueView::AccountUpdate {
                                account_id: account_id.clone(),
                                account: account.clone(),
                            },
                            StateChangeValueView::AccountDeletion { account_id } => {
                                StateChangeValueView::AccountDeletion {
                                    account_id: account_id.clone(),
                                }
                            }
                            StateChangeValueView::AccessKeyUpdate {
                                account_id,
                                access_key,
                                public_key,
                            } => StateChangeValueView::AccessKeyUpdate {
                                account_id: account_id.clone(),
                                access_key: access_key.clone(),
                                public_key: public_key.clone(),
                            },
                            StateChangeValueView::AccessKeyDeletion {
                                account_id,
                                public_key,
                            } => StateChangeValueView::AccessKeyDeletion {
                                account_id: account_id.clone(),
                                public_key: public_key.clone(),
                            },
                            StateChangeValueView::DataUpdate {
                                account_id,
                                key,
                                value,
                            } => StateChangeValueView::DataUpdate {
                                account_id: account_id.clone(),
                                key: key.clone(),
                                value: value.clone(),
                            },
                            StateChangeValueView::DataDeletion { account_id, key } => {
                                StateChangeValueView::DataDeletion {
                                    account_id: account_id.clone(),
                                    key: key.clone(),
                                }
                            }
                            StateChangeValueView::ContractCodeUpdate { account_id, code } => {
                                StateChangeValueView::ContractCodeUpdate {
                                    account_id: account_id.clone(),
                                    code: code.clone(),
                                }
                            }
                            StateChangeValueView::ContractCodeDeletion { account_id } => {
                                StateChangeValueView::ContractCodeDeletion {
                                    account_id: account_id.clone(),
                                }
                            }
                        };
                        near_primitives::views::StateChangeWithCauseView { cause, value }
                    })
                    .collect();
                near_indexer_primitives::IndexerShard {
                    shard_id: shard.shard_id,
                    chunk: shard.chunk.as_ref().map(|chunk| {
                        near_indexer_primitives::IndexerChunkView {
                            author: chunk.author.clone(),
                            header: chunk.header.clone(),
                            transactions: chunk.transactions.clone(),
                            receipts: chunk.receipts.clone(),
                        }
                    }),
                    receipt_execution_outcomes: shard.receipt_execution_outcomes.clone(),
                    state_changes,
                }
            })
            .collect()
    }

    pub fn changes_in_block(&self) -> std::collections::HashMap<String, BlockStateChangeValue> {
        let mut block_state_changes =
            std::collections::HashMap::<String, BlockStateChangeValue>::new();

        let initial_state_changes = self
            .stream_message
            .shards
            .iter()
            .flat_map(|shard| shard.state_changes.iter());

        for state_change in initial_state_changes {
            let (key, value) = match &state_change.value {
                StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } => {
                    let key: &[u8] = key.as_ref();
                    (
                        format!("{}_data_{}", account_id.as_str(), hex::encode(key)),
                        Some(value.to_vec()),
                    )
                }
                StateChangeValueView::DataDeletion { account_id, key } => {
                    let key: &[u8] = key.as_ref();
                    (
                        format!("{}_data_{}", account_id.as_str(), hex::encode(key)),
                        None,
                    )
                }
                StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    access_key,
                } => {
                    let data_key =
                        borsh::to_vec(&public_key).expect("Error to serialize public key");
                    (
                        format!(
                            "{}_access_key_{}",
                            account_id.as_str(),
                            hex::encode(data_key)
                        ),
                        Some(borsh::to_vec(access_key).expect("Error to serialize access key")),
                    )
                }
                StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    public_key,
                } => {
                    let data_key =
                        borsh::to_vec(&public_key).expect("Error to serialize public key");
                    (
                        format!(
                            "{}_access_key_{}",
                            account_id.as_str(),
                            hex::encode(data_key)
                        ),
                        None,
                    )
                }
                // ContractCode and Account changes is not separate-able by any key, we can omit the suffix
                StateChangeValueView::ContractCodeUpdate { account_id, code } => (
                    format!("{}_contract", account_id.as_str()),
                    Some(code.clone()),
                ),
                StateChangeValueView::ContractCodeDeletion { account_id } => {
                    (format!("{}_contract", account_id.as_str()), None)
                }
                StateChangeValueView::AccountUpdate {
                    account_id,
                    account,
                } => (
                    format!("{}_account", account_id.as_str()),
                    Some(
                        borsh::to_vec(&near_primitives::account::Account::from(account))
                            .expect("Error to serialize account"),
                    ),
                ),
                StateChangeValueView::AccountDeletion { account_id } => {
                    (format!("{}_account", account_id.as_str()), None)
                }
            };
            block_state_changes.insert(key, BlockStateChangeValue { value });
        }
        block_state_changes
    }
}

#[derive(Debug)]
pub struct FinalBlockInfo {
    pub final_block: BlockInfo,
    pub optimistic_block: BlockInfo,
    pub current_validators: near_primitives::views::EpochValidatorInfo,
}

impl FinalBlockInfo {
    pub async fn new(
        near_rpc_client: &crate::utils::JsonRpcClient,
        blocks_cache: &std::sync::Arc<
            futures_locks::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>,
        >,
    ) -> Self {
        let final_block_future = crate::utils::get_final_block(near_rpc_client, false);
        let optimistic_block_future = crate::utils::get_final_block(near_rpc_client, true);
        let validators_future = crate::utils::get_current_validators(near_rpc_client);
        let (final_block, optimistic_block, validators) = tokio::try_join!(
            final_block_future,
            optimistic_block_future,
            validators_future,
        )
        .map_err(|err| {
            tracing::error!("Error to fetch final block info: {:?}", err);
            err
        })
        .expect("Error to get final block info");

        blocks_cache
            .write()
            .await
            .put(final_block.header.height, CacheBlock::from(&final_block));

        Self {
            final_block: BlockInfo::new_from_block_view(final_block),
            optimistic_block: BlockInfo::new_from_block_view(optimistic_block),
            current_validators: validators,
        }
    }
}
