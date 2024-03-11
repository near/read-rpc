use near_primitives::views::StateChangeValueView;

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

impl BlockInfo {
    pub async fn new_from_block_view(block_view: near_primitives::views::BlockView) -> Self {
        Self {
            block_cache: CacheBlock::from(&block_view),
            stream_message: near_indexer_primitives::StreamerMessage {
                block: block_view,
                shards: vec![],
            },
        }
    }

    pub async fn new_from_streamer_message(
        stream_message: near_indexer_primitives::StreamerMessage,
    ) -> Self {
        Self {
            block_cache: CacheBlock::from(&stream_message.block),
            stream_message,
        }
    }

    pub async fn block_view(&self) -> near_primitives::views::BlockView {
        self.stream_message.block.clone()
    }

    pub async fn shards(&self) -> Vec<near_indexer_primitives::IndexerShard> {
        self.stream_message.shards.clone()
    }

    pub async fn account_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Option<near_primitives::views::AccountView>> {
        let mut result = None;
        let mut is_account_updated = false;
        let initial_state_changes = self
            .shards()
            .await
            .into_iter()
            .flat_map(|shard| shard.state_changes.into_iter())
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValueView::AccountUpdate { .. }
                        | StateChangeValueView::AccountDeletion { .. }
                )
            });

        for state_change in initial_state_changes {
            match state_change.value {
                StateChangeValueView::AccountUpdate {
                    account_id,
                    account,
                } => {
                    if &account_id == target_account_id {
                        result = Some(account);
                        is_account_updated = true;
                    }
                }
                StateChangeValueView::AccountDeletion { account_id } => {
                    if &account_id == target_account_id {
                        result = None;
                        is_account_updated = true;
                    }
                }
                _ => anyhow::bail!("Invalid state change value"),
            };
        }
        if !is_account_updated {
            anyhow::bail!("Account not updated in this block");
        }
        Ok(result)
    }

    pub async fn code_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let mut result = None;
        let mut is_code_updated = false;
        let initial_state_changes = self
            .shards()
            .await
            .into_iter()
            .flat_map(|shard| shard.state_changes.into_iter())
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValueView::ContractCodeUpdate { .. }
                        | StateChangeValueView::ContractCodeDeletion { .. }
                )
            });

        for state_change in initial_state_changes {
            match state_change.value {
                StateChangeValueView::ContractCodeUpdate { account_id, code } => {
                    if &account_id == target_account_id {
                        result = Some(code);
                        is_code_updated = true;
                    }
                }
                StateChangeValueView::ContractCodeDeletion { account_id } => {
                    if &account_id == target_account_id {
                        result = None;
                        is_code_updated = true;
                    }
                }
                _ => anyhow::bail!("Invalid state change value"),
            };
        }
        if !is_code_updated {
            anyhow::bail!("Contract code not updated in this block");
        }
        Ok(result)
    }

    pub async fn access_key_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
        target_public_key: &near_crypto::PublicKey,
    ) -> anyhow::Result<Option<near_primitives::views::AccessKeyView>> {
        let mut result = None;
        let mut is_access_key_updated = false;
        let initial_state_changes = self
            .shards()
            .await
            .into_iter()
            .flat_map(|shard| shard.state_changes.into_iter())
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValueView::AccessKeyUpdate { .. }
                        | StateChangeValueView::AccessKeyDeletion { .. }
                )
            });

        for state_change in initial_state_changes {
            match state_change.value {
                StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    access_key,
                } => {
                    if &account_id == target_account_id && &public_key == target_public_key {
                        result = Some(access_key);
                        is_access_key_updated = true;
                    }
                }
                StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    public_key,
                } => {
                    if &account_id == target_account_id && &public_key == target_public_key {
                        result = None;
                        is_access_key_updated = true;
                    }
                }
                _ => anyhow::bail!("Invalid state change value"),
            };
        }
        if !is_access_key_updated {
            anyhow::bail!("Access key not updated in this block");
        }
        Ok(result)
    }

    pub async fn state_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> std::collections::HashMap<
        readnode_primitives::StateKey,
        Option<readnode_primitives::StateValue>,
    > {
        let mut block_state_changes = std::collections::HashMap::<
            readnode_primitives::StateKey,
            Option<readnode_primitives::StateValue>,
        >::new();
        let hex_str_prefix = hex::encode(prefix);

        let initial_state_changes = self
            .shards()
            .await
            .into_iter()
            .flat_map(|shard| shard.state_changes.into_iter())
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValueView::DataUpdate { .. }
                        | StateChangeValueView::DataDeletion { .. }
                )
            });

        for state_change in initial_state_changes {
            match state_change.value {
                StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } => {
                    if &account_id == target_account_id {
                        let key: &[u8] = key.as_ref();
                        if hex::encode(key).starts_with(&hex_str_prefix) {
                            block_state_changes.insert(key.to_vec(), Some(value.to_vec()));
                        }
                    }
                }
                StateChangeValueView::DataDeletion { account_id, key } => {
                    if &account_id == target_account_id {
                        let key: &[u8] = key.as_ref();
                        if hex::encode(key).starts_with(&hex_str_prefix) {
                            block_state_changes.insert(key.to_vec(), None);
                        }
                    }
                }
                _ => {}
            }
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
            final_block: BlockInfo::new_from_block_view(final_block).await,
            optimistic_block: BlockInfo::new_from_block_view(optimistic_block).await,
            current_validators: validators,
        }
    }
}
