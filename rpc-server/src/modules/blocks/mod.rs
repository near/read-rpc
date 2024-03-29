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

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub block_cache: CacheBlock,
    pub stream_message: near_indexer_primitives::StreamerMessage,
}

impl BlockInfo {
    // Create new BlockInfo from BlockView. this method is useful only for start rpc-server.
    pub async fn new_from_block_view(block_view: near_primitives::views::BlockView) -> Self {
        Self {
            block_cache: CacheBlock::from(&block_view),
            stream_message: near_indexer_primitives::StreamerMessage {
                block: block_view,
                shards: vec![], // We left shards empty because block_view doesn't contain shards.
            },
        }
    }

    // Create new BlockInfo from StreamerMessage.
    // This is using to update final and optimistic blocks regularly.
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

    // This method using for optimistic blocks info.
    // We fetch the account changes in the block by specific AccountId.
    // For optimistic block s we don't have information about account changes in the database,
    // so we need to fetch it from the optimistic block streamer_message and merge it with data from database.
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

    // This method using for optimistic blocks info.
    // We fetch the contract code changes in the block by specific AccountId.
    // For optimistic block we don't have information about code changes in the database,
    // so we need to fetch it from the optimistic block streamer_message and merge it with data from database.
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

    // This method using for optimistic blocks info.
    // We fetch the access_key changes in the block by specific AccountId and PublicKey.
    // For optimistic block we don't have information about AccessKey changes in the database,
    // so we need to fetch it from the optimistic block streamer_message and merge it with data from database.
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

    // This method using for optimistic blocks info.
    // We fetch the state changes in the block by specific AccountId and key_prefix.
    // if prefix is empty, we fetch all state changes by specific AccountId.
    // For optimistic block we don't have information about state changes in the database,
    // so we need to fetch it from the optimistic block streamer_message and merge it with data from database.
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

#[derive(Debug, Clone)]
pub struct CurrentValidatorInfo {
    pub validators: near_primitives::views::EpochValidatorInfo,
}

#[derive(Debug, Clone)]
pub struct BlocksInfoByFinality {
    pub final_block: futures_locks::RwLock<BlockInfo>,
    pub optimistic_block: futures_locks::RwLock<BlockInfo>,
    pub current_validators: futures_locks::RwLock<CurrentValidatorInfo>,
}

impl BlocksInfoByFinality {
    pub async fn new(
        near_rpc_client: &crate::utils::JsonRpcClient,
        blocks_cache: &std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, CacheBlock>>,
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
            .put(final_block.header.height, CacheBlock::from(&final_block))
            .await;

        Self {
            final_block: futures_locks::RwLock::new(
                BlockInfo::new_from_block_view(final_block).await,
            ),
            optimistic_block: futures_locks::RwLock::new(
                BlockInfo::new_from_block_view(optimistic_block).await,
            ),
            current_validators: futures_locks::RwLock::new(CurrentValidatorInfo { validators }),
        }
    }

    pub async fn update_final_block(&self, block_info: BlockInfo) {
        tracing::debug!(
            "Update final block info: {:?}",
            block_info.block_cache.block_height
        );
        let mut final_block_lock = self.final_block.write().await;
        final_block_lock.block_cache = block_info.block_cache;
        final_block_lock.stream_message = block_info.stream_message;
    }

    pub async fn update_optimistic_block(&self, block_info: BlockInfo) {
        tracing::debug!(
            "Update optimistic block info: {:?}",
            block_info.block_cache.block_height
        );
        let mut optimistic_block_lock = self.optimistic_block.write().await;
        optimistic_block_lock.block_cache = block_info.block_cache;
        optimistic_block_lock.stream_message = block_info.stream_message;
    }

    pub async fn update_current_validators(
        &self,
        near_rpc_client: &crate::utils::JsonRpcClient,
    ) -> anyhow::Result<()> {
        self.current_validators.write().await.validators =
            crate::utils::get_current_validators(near_rpc_client).await?;
        Ok(())
    }
    pub async fn final_block_info(&self) -> BlockInfo {
        let final_block_info_lock = self.final_block.read().await;
        BlockInfo {
            block_cache: final_block_info_lock.block_cache,
            stream_message: final_block_info_lock.stream_message.clone(),
        }
    }

    pub async fn final_cache_block(&self) -> CacheBlock {
        self.final_block.read().await.block_cache
    }

    pub async fn optimistic_block_info(&self) -> BlockInfo {
        let optimistic_block_info_lock = self.optimistic_block.read().await;
        BlockInfo {
            block_cache: optimistic_block_info_lock.block_cache,
            stream_message: optimistic_block_info_lock.stream_message.clone(),
        }
    }

    pub async fn optimistic_cache_block(&self) -> CacheBlock {
        self.optimistic_block.read().await.block_cache
    }

    pub async fn validators(&self) -> near_primitives::views::EpochValidatorInfo {
        self.current_validators.read().await.validators.clone()
    }
}
