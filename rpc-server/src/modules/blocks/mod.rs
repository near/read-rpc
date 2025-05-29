use near_primitives::views::{StateChangeValueView, StateChangesView};

pub mod methods;
pub mod utils;

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

// Struct to store the chunk
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkInfo {
    pub shard_id: near_primitives::types::ShardId,
    pub chunk: Option<near_primitives::views::ChunkView>,
}

impl Clone for ChunkInfo {
    fn clone(&self) -> Self {
        let chunk_clone = self
            .chunk
            .as_ref()
            .map(|chunk| near_primitives::views::ChunkView {
                author: chunk.author.clone(),
                header: chunk.header.clone(),
                transactions: chunk.transactions.clone(),
                receipts: chunk.receipts.clone(),
            });

        Self {
            shard_id: self.shard_id,
            chunk: chunk_clone,
        }
    }
}

pub type ChunksInfo = Vec<ChunkInfo>;

impl ChunkInfo {
    pub fn from_indexer_shards(shards: Vec<near_indexer_primitives::IndexerShard>) -> ChunksInfo {
        shards.into_iter().map(ChunkInfo::from).collect()
    }
}

impl From<near_indexer_primitives::IndexerShard> for ChunkInfo {
    fn from(shard: near_indexer_primitives::IndexerShard) -> Self {
        Self {
            shard_id: shard.shard_id,
            chunk: shard.chunk.map(utils::from_indexer_chunk_to_chunk_view),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub block_view: near_primitives::views::BlockView,
    pub changes: StateChangesView,
    pub shards: Vec<near_indexer_primitives::IndexerShard>,
}

impl BlockInfo {
    // Create new BlockInfo from StreamerMessage.
    // This is using to update final and optimistic blocks regularly.
    pub async fn new_from_streamer_message(
        streamer_message: &near_indexer_primitives::StreamerMessage,
    ) -> Self {
        Self {
            block_view: streamer_message.block.clone(),
            changes: streamer_message
                .shards
                .clone()
                .into_iter()
                .flat_map(|shard| shard.state_changes)
                .collect(),
            shards: streamer_message.shards.clone(),
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
        fast_near_client: &near_lake_framework::FastNearClient,
    ) -> Self {
        let final_block =
            near_lake_framework::fastnear::fetchers::fetch_streamer_message_by_finality(
                fast_near_client,
                near_indexer_primitives::types::Finality::Final,
            )
            .await;
        let optimistic_block =
            near_lake_framework::fastnear::fetchers::fetch_streamer_message_by_finality(
                fast_near_client,
                near_indexer_primitives::types::Finality::None,
            )
            .await;
        let validators = crate::utils::get_current_validators(near_rpc_client)
            .await
            .expect("Failed to get current validators");
        let protocol_version = crate::utils::get_current_protocol_version(near_rpc_client)
            .await
            .expect("Failed to get current protocol version");

        Self {
            final_block: futures_locks::RwLock::new(
                BlockInfo::new_from_streamer_message(&final_block).await,
            ),
            optimistic_block: futures_locks::RwLock::new(
                BlockInfo::new_from_streamer_message(&optimistic_block).await,
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
        let mut final_block_lock = self.final_block.write().await;
        final_block_lock.block_view = block_info.block_view;
        final_block_lock.changes = block_info.changes;
        final_block_lock.shards = block_info.shards;
    }

    // Update optimistic block changes and optimistic block info in the cache.
    // Executes every second.
    pub async fn update_optimistic_block(&self, block_info: BlockInfo) {
        let mut optimistic_changes_lock = self.optimistic_changes.write().await;
        optimistic_changes_lock.account_changes = block_info.changes_in_block_account_map().await;

        let mut optimistic_block_lock = self.optimistic_block.write().await;
        optimistic_block_lock.block_view = block_info.block_view;
        optimistic_block_lock.changes = block_info.changes;
        optimistic_block_lock.shards = block_info.shards;
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

    // return final block shards
    pub async fn final_block_shards(&self) -> Vec<near_indexer_primitives::IndexerShard> {
        self.final_block.read().await.shards.clone()
    }

    // return final block view
    pub async fn final_block_view(&self) -> near_primitives::views::BlockView {
        self.final_block.read().await.block_view.clone()
    }

    // return optimistic block changes
    pub async fn optimistic_block_changes(&self) -> StateChangesView {
        self.optimistic_block.read().await.changes.clone()
    }

    // return optimistic block shards
    pub async fn optimistic_block_shards(&self) -> Vec<near_indexer_primitives::IndexerShard> {
        self.optimistic_block.read().await.shards.clone()
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
