use near_primitives::views::{StateChangeValueView, StateChangesView};

pub mod methods;
pub mod utils;

#[derive(Clone, Copy, Debug)]
pub struct CacheBlock {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_timestamp: u64,
    pub gas_price: near_primitives::types::Balance,
    pub latest_protocol_version: near_primitives::types::ProtocolVersion,
    pub state_root: near_primitives::hash::CryptoHash,
    pub epoch_id: near_primitives::hash::CryptoHash,
}

#[derive(Debug, Clone)]
pub enum AccountChanges {
    None,
    Changes(Option<near_primitives::views::AccountView>),
}

#[derive(Debug, Clone)]
pub enum CodeChanges {
    None,
    Changes(Option<Vec<u8>>),
}

// Struct to store in the cache the account changes in the block
#[derive(Debug, Clone)]
pub struct AccountChangesInBlock {
    pub account_changes: AccountChanges,
    pub code_changes: CodeChanges,
    pub access_key_changes: std::collections::HashMap<
        near_crypto::PublicKey,
        Option<near_primitives::views::AccessKeyView>,
    >,
    pub state_changes: std::collections::HashMap<
        readnode_primitives::StateKey,
        Option<readnode_primitives::StateValue>,
    >,
}

impl AccountChangesInBlock {
    pub fn new() -> Self {
        Self {
            account_changes: AccountChanges::None,
            code_changes: CodeChanges::None,
            access_key_changes: std::collections::HashMap::new(),
            state_changes: std::collections::HashMap::new(),
        }
    }
}

impl From<&near_primitives::views::BlockView> for CacheBlock {
    fn from(block: &near_primitives::views::BlockView) -> Self {
        Self {
            block_hash: block.header.hash,
            block_height: block.header.height,
            block_timestamp: block.header.timestamp,
            gas_price: block.header.gas_price,
            latest_protocol_version: block.header.latest_protocol_version,
            state_root: block.header.prev_state_root,
            epoch_id: block.header.epoch_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub block_cache: CacheBlock,
    pub block_view: near_primitives::views::BlockView,
    pub changes: StateChangesView,
}

impl BlockInfo {
    // Create new BlockInfo from BlockView. this method is useful only for start rpc-server.
    pub async fn new_from_block_view(block_view: near_primitives::views::BlockView) -> Self {
        Self {
            block_cache: CacheBlock::from(&block_view),
            block_view,
            changes: vec![], // We left changes empty because block_view doesn't contain state changes.
        }
    }

    // Create new BlockInfo from StreamerMessage.
    // This is using to update final and optimistic blocks regularly.
    pub async fn new_from_streamer_message(
        streamer_message: near_indexer_primitives::StreamerMessage,
    ) -> Self {
        Self {
            block_cache: CacheBlock::from(&streamer_message.block),
            block_view: streamer_message.block,
            changes: streamer_message
                .shards
                .into_iter()
                .flat_map(|shard| shard.state_changes)
                .collect(),
        }
    }

    // extract block changes and store it in the HashMap by AccountId.
    pub async fn changes_in_block_account_map(
        &self,
    ) -> std::collections::HashMap<near_primitives::types::AccountId, AccountChangesInBlock> {
        let mut changes_map = std::collections::HashMap::new();
        for changes in &self.changes {
            match changes.value.clone() {
                StateChangeValueView::AccountUpdate {
                    account_id,
                    account,
                } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.account_changes = AccountChanges::Changes(Some(account));
                }
                StateChangeValueView::AccountDeletion { account_id } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.account_changes = AccountChanges::Changes(None);
                }
                StateChangeValueView::ContractCodeUpdate { account_id, code } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.code_changes = CodeChanges::Changes(Some(code));
                }
                StateChangeValueView::ContractCodeDeletion { account_id } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.code_changes = CodeChanges::Changes(None);
                }
                StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    access_key,
                } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.access_key_changes.insert(public_key, Some(access_key));
                }
                StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    public_key,
                } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.access_key_changes.insert(public_key, None);
                }
                StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.state_changes.insert(key.to_vec(), Some(value.to_vec()));
                }
                StateChangeValueView::DataDeletion { account_id, key } => {
                    let acc = changes_map
                        .entry(account_id)
                        .or_insert_with(AccountChangesInBlock::new);
                    acc.state_changes.insert(key.to_vec(), None);
                }
            }
        }
        changes_map
    }
}

// Struct to store the validators info for current epoch.
#[derive(Debug, Clone)]
pub struct CurrentValidatorInfo {
    pub validators: near_primitives::views::EpochValidatorInfo,
}

// Struct to store the optimistic changes in the block.
#[derive(Debug, Clone)]
pub struct OptimisticChanges {
    pub account_changes:
        std::collections::HashMap<near_primitives::types::AccountId, AccountChangesInBlock>,
}

impl OptimisticChanges {
    pub fn new() -> Self {
        Self {
            account_changes: std::collections::HashMap::new(),
        }
    }

    // This method is used for optimistic block info.
    // We fetch the account changes in the block by specific AccountId.
    // For optimistic blocks, we don't have information about account changes in the database,
    // so we need to fetch it from the optimistic block's streamer_message and merge it with data from the database.
    pub async fn account_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Option<near_primitives::views::AccountView>> {
        if let Some(account_changes) = self.account_changes.get(target_account_id) {
            if let AccountChanges::Changes(account) = &account_changes.account_changes {
                Ok(account.clone())
            } else {
                anyhow::bail!("Account not updated in this block");
            }
        } else {
            anyhow::bail!("Account not found in this block");
        }
    }

    // This method is used for optimistic block info.
    // We fetch the contract code changes in the block by specific AccountId.
    // For optimistic blocks, we don't have information about code changes in the database,
    // so we need to fetch it from the optimistic block's streamer_message and merge it with data from the database.
    pub async fn code_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(account_changes) = self.account_changes.get(target_account_id) {
            if let CodeChanges::Changes(code) = &account_changes.code_changes {
                Ok(code.clone())
            } else {
                anyhow::bail!("Code not updated in this block");
            }
        } else {
            anyhow::bail!("Code not found in this block");
        }
    }

    // This method is used for optimistic block info.
    // We fetch the access_key changes in the block by specific AccountId and PublicKey.
    // For optimistic blocks, we don't have information about AccessKey changes in the database,
    // so we need to fetch it from the optimistic block's streamer_message and merge it with data from the database.
    pub async fn access_key_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
        target_public_key: &near_crypto::PublicKey,
    ) -> anyhow::Result<Option<near_primitives::views::AccessKeyView>> {
        if let Some(account_changes) = self.account_changes.get(target_account_id) {
            if let Some(access_key) = account_changes.access_key_changes.get(target_public_key) {
                Ok(access_key.clone())
            } else {
                anyhow::bail!("AccessKey not found in this block");
            }
        } else {
            anyhow::bail!("Account not found in this block");
        }
    }

    // This method is used for optimistic block info.
    // We fetch the state changes in the block by specific AccountId and key_prefix.
    // if prefix is empty, we fetch all state changes by specific AccountId.
    // For optimistic blocks, we don't have information about state changes in the database,
    // so we need to fetch it from the optimistic block's streamer_message and merge it with data from the database.
    pub async fn state_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> std::collections::HashMap<
        readnode_primitives::StateKey,
        Option<readnode_primitives::StateValue>,
    > {
        let mut state_changes = std::collections::HashMap::new();
        if let Some(account_changes) = self.account_changes.get(target_account_id) {
            for (key, value) in account_changes.state_changes.iter() {
                if key.starts_with(prefix) {
                    state_changes.insert(key.clone(), value.clone());
                }
            }
        }
        state_changes
    }
}

#[derive(Debug, Clone)]
pub struct CurrentProtocolVersion {
    pub protocol_version: near_primitives::types::ProtocolVersion,
}

#[derive(Debug, Clone)]
pub struct BlocksInfoByFinality {
    pub final_block: futures_locks::RwLock<BlockInfo>,
    pub optimistic_block: futures_locks::RwLock<BlockInfo>,
    pub optimistic_changes: futures_locks::RwLock<OptimisticChanges>,
    pub current_validators: futures_locks::RwLock<CurrentValidatorInfo>,
    pub current_protocol_version: futures_locks::RwLock<CurrentProtocolVersion>,
}

impl BlocksInfoByFinality {
    pub async fn new(
        near_rpc_client: &crate::utils::JsonRpcClient,
        blocks_cache: &std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, CacheBlock>>,
    ) -> Self {
        let final_block_future = crate::utils::get_final_block(near_rpc_client, false);
        let optimistic_block_future = crate::utils::get_final_block(near_rpc_client, true);
        let validators_future = crate::utils::get_current_validators(near_rpc_client);
        let protocol_version_future = crate::utils::get_current_protocol_version(near_rpc_client);

        let (final_block, optimistic_block, validators, protocol_version) = futures::try_join!(
            final_block_future,
            optimistic_block_future,
            validators_future,
            protocol_version_future
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
            optimistic_changes: futures_locks::RwLock::new(OptimisticChanges::new()),
            current_validators: futures_locks::RwLock::new(CurrentValidatorInfo { validators }),
            current_protocol_version: futures_locks::RwLock::new(CurrentProtocolVersion {
                protocol_version,
            }),
        }
    }

    // Update final block info in the cache.
    // Executes every second.
    pub async fn update_final_block(&self, block_info: BlockInfo) {
        tracing::debug!(
            "Update final block info: {:?}",
            block_info.block_cache.block_height
        );
        let mut final_block_lock = self.final_block.write().await;
        final_block_lock.block_cache = block_info.block_cache;
        final_block_lock.block_view = block_info.block_view;
        final_block_lock.changes = block_info.changes;
    }

    // Update optimistic block changes and optimistic block info in the cache.
    // Executes every second.
    pub async fn update_optimistic_block(&self, block_info: BlockInfo) {
        tracing::debug!(
            "Update optimistic block info: {:?}",
            block_info.block_cache.block_height
        );

        let mut optimistic_changes_lock = self.optimistic_changes.write().await;
        optimistic_changes_lock.account_changes = block_info.changes_in_block_account_map().await;

        let mut optimistic_block_lock = self.optimistic_block.write().await;
        optimistic_block_lock.block_cache = block_info.block_cache;
        optimistic_block_lock.block_view = block_info.block_view;
        optimistic_block_lock.changes = block_info.changes;
    }

    // Update current validators info in the cache.
    // This method executes when the epoch changes.
    pub async fn update_current_epoch_info(
        &self,
        near_rpc_client: &crate::utils::JsonRpcClient,
    ) -> anyhow::Result<()> {
        let current_validators_future = crate::utils::get_current_validators(near_rpc_client);
        let current_protocol_version_future =
            crate::utils::get_current_protocol_version(near_rpc_client);
        let (current_validators, current_protocol_version) =
            futures::try_join!(current_validators_future, current_protocol_version_future,)?;
        self.current_validators.write().await.validators = current_validators;
        self.current_protocol_version.write().await.protocol_version = current_protocol_version;
        Ok(())
    }

    // return final block changes
    pub async fn final_block_changes(&self) -> StateChangesView {
        self.final_block.read().await.changes.clone()
    }

    // return final block changes
    pub async fn final_cache_block(&self) -> CacheBlock {
        self.final_block.read().await.block_cache
    }

    // return final block view
    pub async fn final_block_view(&self) -> near_primitives::views::BlockView {
        self.final_block.read().await.block_view.clone()
    }

    // return optimistic block changes
    pub async fn optimistic_block_changes(&self) -> StateChangesView {
        self.optimistic_block.read().await.changes.clone()
    }

    // return optimistic block cache
    pub async fn optimistic_cache_block(&self) -> CacheBlock {
        self.optimistic_block.read().await.block_cache
    }

    // return optimistic block view
    pub async fn optimistic_block_view(&self) -> near_primitives::views::BlockView {
        self.optimistic_block.read().await.block_view.clone()
    }

    // Get account changes in the block by specific AccountId.
    pub async fn optimistic_account_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Option<near_primitives::views::AccountView>> {
        self.optimistic_changes
            .read()
            .await
            .account_changes_in_block(target_account_id)
            .await
    }

    // Get contract code changes in the block by specific AccountId.
    pub async fn optimistic_code_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        self.optimistic_changes
            .read()
            .await
            .code_changes_in_block(target_account_id)
            .await
    }

    // Get access_key changes in the block by specific AccountId and PublicKey.
    pub async fn optimistic_access_key_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
        target_public_key: &near_crypto::PublicKey,
    ) -> anyhow::Result<Option<near_primitives::views::AccessKeyView>> {
        self.optimistic_changes
            .read()
            .await
            .access_key_changes_in_block(target_account_id, target_public_key)
            .await
    }

    // Get state changes in the block by specific AccountId and key_prefix.
    pub async fn optimistic_state_changes_in_block(
        &self,
        target_account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> std::collections::HashMap<
        readnode_primitives::StateKey,
        Option<readnode_primitives::StateValue>,
    > {
        self.optimistic_changes
            .read()
            .await
            .state_changes_in_block(target_account_id, prefix)
            .await
    }

    // Return validators info for current epoch
    pub async fn validators(&self) -> near_primitives::views::EpochValidatorInfo {
        self.current_validators.read().await.validators.clone()
    }

    pub async fn current_epoch_start_height(&self) -> near_primitives::types::BlockHeight {
        self.current_validators
            .read()
            .await
            .validators
            .epoch_start_height
    }

    // Return current protocol version
    pub async fn current_protocol_version(&self) -> near_primitives::types::ProtocolVersion {
        self.current_protocol_version.read().await.protocol_version
    }
}
