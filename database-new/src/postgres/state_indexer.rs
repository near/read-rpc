use bigdecimal::ToPrimitive;

impl crate::PostgresDBManager {
    async fn save_state_changes_data_to_shard(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO state_changes_data (account_id, block_height, block_hash, data_key, data_value) ",
        );
        query_builder.push_values(state_changes.iter(), |mut values, state_change| {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } => {
                    let data_key: &[u8] = key.as_ref();
                    let data_value: &[u8] = value.as_ref();
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(hex::encode(&data_key).to_string())
                        .push_bind(data_value);
                }
                near_primitives::views::StateChangeValueView::DataDeletion { account_id, key } => {
                    let data_key: &[u8] = key.as_ref();
                    let data_value: Option<&[u8]> = None;
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(hex::encode(data_key).to_string())
                        .push_bind(data_value);
                }
                _ => {}
            }
        });
        query_builder
            .build()
            .execute(
                self.shards_pool
                    .get(&shard_id)
                    .ok_or(anyhow::anyhow!("Shard not found"))?,
            )
            .await?;
        Ok(())
    }

    async fn save_state_changes_access_key_to_shard(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO state_changes_access_key (account_id, block_height, block_hash, data_key, data_value) ",
        );
        query_builder.push_values(state_changes.iter(), |mut values, state_change| {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    access_key,
                } => {
                    let data_key =
                        borsh::to_vec(public_key).expect("Failed to borsh serialize public key");
                    let data_value =
                        borsh::to_vec(access_key).expect("Failed to borsh serialize access key");
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(hex::encode(&data_key).to_string())
                        .push_bind(data_value);
                }
                near_primitives::views::StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    public_key,
                } => {
                    let data_key =
                        borsh::to_vec(public_key).expect("Failed to borsh serialize public key");
                    let data_value: Option<&[u8]> = None;
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(hex::encode(data_key).to_string())
                        .push_bind(data_value);
                }
                _ => {}
            }
        });
        query_builder
            .build()
            .execute(
                self.shards_pool
                    .get(&shard_id)
                    .ok_or(anyhow::anyhow!("Shard not found"))?,
            )
            .await?;
        Ok(())
    }

    async fn save_state_changes_contract_to_shard(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO state_changes_contract (account_id, block_height, block_hash, data_value) ",
        );
        query_builder.push_values(state_changes.iter(), |mut values, state_change| {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::ContractCodeUpdate {
                    account_id,
                    code,
                } => {
                    let data_value: &[u8] = code.as_ref();
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(data_value);
                }
                near_primitives::views::StateChangeValueView::ContractCodeDeletion {
                    account_id,
                } => {
                    let data_value: Option<&[u8]> = None;
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(data_value);
                }
                _ => {}
            }
        });
        query_builder
            .build()
            .execute(
                self.shards_pool
                    .get(&shard_id)
                    .ok_or(anyhow::anyhow!("Shard not found"))?,
            )
            .await?;
        Ok(())
    }

    async fn save_state_changes_account_to_shard(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO state_changes_account (account_id, block_height, block_hash, data_value) ",
        );
        query_builder.push_values(state_changes.iter(), |mut values, state_change| {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::AccountUpdate {
                    account_id,
                    account,
                } => {
                    let data_value =
                        borsh::to_vec(&near_primitives::account::Account::from(account))
                            .expect("Failed to borsh serialize account");
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(data_value);
                }
                near_primitives::views::StateChangeValueView::AccountDeletion { account_id } => {
                    let data_value: Option<&[u8]> = None;
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(block_hash.to_string())
                        .push_bind(data_value);
                }
                _ => {}
            }
        });
        query_builder
            .build()
            .execute(
                self.shards_pool
                    .get(&shard_id)
                    .ok_or(anyhow::anyhow!("Shard not found"))?,
            )
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::StateIndexerDbManager for crate::PostgresDBManager {
    async fn add_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "
            INSERT INTO blocks (block_height, block_hash)
            VALUES ($1, $2)
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(block_hash.to_string())
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    async fn add_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let chunks_uniq = chunks
            .iter()
            .filter(|(_chunk_hash, _shard_id, height_included)| height_included == &block_height)
            .collect::<Vec<_>>();

        if !chunks_uniq.is_empty() {
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new("INSERT INTO chunks (chunk_hash, block_height, shard_id) ");

            query_builder.push_values(
                chunks_uniq.iter(),
                |mut values, (chunk_hash, shard_id, height_included)| {
                    values
                        .push_bind(chunk_hash.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(height_included.clone()))
                        .push_bind(bigdecimal::BigDecimal::from(shard_id.clone()));
                },
            );

            query_builder.build().execute(&self.meta_db_pool).await?;
        }

        let chunks_duplicate = chunks
            .iter()
            .filter(|(_chunk_hash, _shard_id, height_included)| height_included != &block_height)
            .collect::<Vec<_>>();
        if !chunks_duplicate.is_empty() {
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new("INSERT INTO chunks_duplicate (chunk_hash, block_height, shard_id, included_in_block_height) ");

            query_builder.push_values(
                chunks.iter(),
                |mut values, (chunk_hash, shard_id, height_included)| {
                    values
                        .push_bind(chunk_hash.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(bigdecimal::BigDecimal::from(shard_id.clone()))
                        .push_bind(bigdecimal::BigDecimal::from(height_included.clone()));
                },
            );

            query_builder.build().execute(&self.meta_db_pool).await?;
        }

        Ok(())
    }

    async fn get_block_by_hash(
        &self,
        block_hash: near_indexer_primitives::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<u64> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "blocks"])
            .inc();
        let (block_height,): (bigdecimal::BigDecimal,) = sqlx::query_as(
            "
                SELECT block_height
                FROM blocks
                WHERE block_hash = $1
                LIMIT 1;
                ",
        )
        .bind(block_hash.to_string())
        .fetch_one(&self.meta_db_pool)
        .await?;
        block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        sqlx::query(
            "
            INSERT INTO meta (indexer_id, last_processed_block_height)
            VALUES ($1, $2)
            ON CONFLICT (indexer_id)
            DO UPDATE SET last_processed_block_height = $2;
            ",
        )
        .bind(indexer_id)
        .bind(bigdecimal::BigDecimal::from(block_height))
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        let (last_processed_block_height,): (bigdecimal::BigDecimal,) = sqlx::query_as(
            "
            SELECT last_processed_block_height
            FROM meta
            WHERE indexer_id = ?
            LIMIT 1;
            ",
        )
        .bind(indexer_id)
        .fetch_one(&self.meta_db_pool)
        .await?;
        last_processed_block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `last_processed_block_height` to u64"))
    }

    async fn add_validators(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &near_primitives::views::EpochValidatorInfo,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "
            INSERT INTO validators (epoch_id, epoch_height, epoch_start_height, epoch_end_height, protocol_config)
            VALUES (?, ?, ?, NULL, ?);
            "
        )
            .bind(&epoch_id.to_string())
            .bind(bigdecimal::BigDecimal::from(epoch_height))
            .bind(bigdecimal::BigDecimal::from(epoch_start_height))
            .bind(&serde_json::to_string(validators_info)?)
            .execute(&self.meta_db_pool)
            .await?;
        Ok(())
    }

    async fn add_protocol_config(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        protocol_config: &near_chain_configs::ProtocolConfigView,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "
            INSERT INTO protocol_configs (epoch_id, epoch_height, epoch_start_height, epoch_end_height, protocol_config)
            VALUES (?, ?, ?, NULL, ?);
            "
        )
            .bind(&epoch_id.to_string())
            .bind(bigdecimal::BigDecimal::from(epoch_height))
            .bind(bigdecimal::BigDecimal::from(epoch_start_height))
            .bind(&serde_json::to_string(protocol_config)?)
            .execute(&self.meta_db_pool)
            .await?;
        Ok(())
    }

    async fn update_epoch_end_height(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_end_block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let block_height = self
            .get_block_by_hash(epoch_end_block_hash, "update_epoch_end_height")
            .await?;
        let epoch_id_str = epoch_id.to_string();
        let updated_validators_future = sqlx::query(
            "
            UPDATE validators
            SET epoch_end_height = ?
            WHERE epoch_id = ?;
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(&epoch_id_str)
        .execute(&self.meta_db_pool);

        let updated_protocol_config_future = sqlx::query(
            "
            UPDATE protocol_configs
            SET epoch_end_height = ?
            WHERE epoch_id = ?;
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(&epoch_id_str)
        .execute(&self.meta_db_pool);

        futures::try_join!(updated_validators_future, updated_protocol_config_future)?;
        Ok(())
    }

    async fn save_state_changes_data(
        &self,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut state_changes_by_shards: std::collections::HashMap<
            u64,
            Vec<near_primitives::views::StateChangeWithCauseView>,
        > = std::collections::HashMap::new();
        for state_change in state_changes {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::DataUpdate { account_id, .. }
                | near_primitives::views::StateChangeValueView::DataDeletion {
                    account_id, ..
                } => {
                    let shard_id = near_primitives::shard_layout::account_id_to_shard_id(
                        account_id,
                        &self.shard_layout,
                    );
                    state_changes_by_shards
                        .entry(shard_id)
                        .or_insert_with(Vec::new)
                        .push(state_change);
                }
                _ => {}
            }
        }
        let futures = state_changes_by_shards
            .into_iter()
            .map(|(shard_id, changes)| {
                self.save_state_changes_data_to_shard(shard_id, changes, block_height, block_hash)
            });
        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn save_state_changes_access_key(
        &self,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut state_changes_by_shards: std::collections::HashMap<
            u64,
            Vec<near_primitives::views::StateChangeWithCauseView>,
        > = std::collections::HashMap::new();
        for state_change in state_changes {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    ..
                }
                | near_primitives::views::StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    ..
                } => {
                    let shard_id = near_primitives::shard_layout::account_id_to_shard_id(
                        account_id,
                        &self.shard_layout,
                    );
                    state_changes_by_shards
                        .entry(shard_id)
                        .or_insert_with(Vec::new)
                        .push(state_change);
                }
                _ => {}
            }
        }
        let futures = state_changes_by_shards
            .into_iter()
            .map(|(shard_id, changes)| {
                self.save_state_changes_access_key_to_shard(
                    shard_id,
                    changes,
                    block_height,
                    block_hash,
                )
            });
        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn save_state_changes_contract(
        &self,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut state_changes_by_shards: std::collections::HashMap<
            u64,
            Vec<near_primitives::views::StateChangeWithCauseView>,
        > = std::collections::HashMap::new();
        for state_change in state_changes {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::ContractCodeUpdate {
                    account_id,
                    ..
                }
                | near_primitives::views::StateChangeValueView::ContractCodeDeletion {
                    account_id,
                    ..
                } => {
                    let shard_id = near_primitives::shard_layout::account_id_to_shard_id(
                        account_id,
                        &self.shard_layout,
                    );
                    state_changes_by_shards
                        .entry(shard_id)
                        .or_insert_with(Vec::new)
                        .push(state_change);
                }
                _ => {}
            }
        }
        let futures = state_changes_by_shards
            .into_iter()
            .map(|(shard_id, changes)| {
                self.save_state_changes_contract_to_shard(
                    shard_id,
                    changes,
                    block_height,
                    block_hash,
                )
            });
        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn save_state_changes_account(
        &self,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let mut state_changes_by_shards: std::collections::HashMap<
            u64,
            Vec<near_primitives::views::StateChangeWithCauseView>,
        > = std::collections::HashMap::new();
        for state_change in state_changes {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::AccountUpdate {
                    account_id, ..
                }
                | near_primitives::views::StateChangeValueView::AccountDeletion {
                    account_id,
                    ..
                } => {
                    let shard_id = near_primitives::shard_layout::account_id_to_shard_id(
                        account_id,
                        &self.shard_layout,
                    );
                    state_changes_by_shards
                        .entry(shard_id)
                        .or_insert_with(Vec::new)
                        .push(state_change);
                }
                _ => {}
            }
        }
        let futures = state_changes_by_shards
            .into_iter()
            .map(|(shard_id, changes)| {
                self.save_state_changes_account_to_shard(
                    shard_id,
                    changes,
                    block_height,
                    block_hash,
                )
            });
        futures::future::try_join_all(futures).await?;
        Ok(())
    }
}
