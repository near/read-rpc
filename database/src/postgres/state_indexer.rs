use bigdecimal::ToPrimitive;
use futures::FutureExt;

impl crate::PostgresDBManager {
    async fn save_chunks_unique(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let unique_chunks = chunks
            .iter()
            .filter(|(_chunk_hash, _shard_id, height_included)| height_included == &block_height)
            .collect::<Vec<_>>();

        if !unique_chunks.is_empty() {
            crate::metrics::META_DATABASE_WRITE_QUERIES
                .with_label_values(&["save_chunks", "chunks"])
                .inc();
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new("INSERT INTO chunks (chunk_hash, block_height, shard_id) ");

            query_builder.push_values(
                unique_chunks.iter(),
                |mut values, (chunk_hash, shard_id, height_included)| {
                    values
                        .push_bind(chunk_hash.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(*height_included))
                        .push_bind(bigdecimal::BigDecimal::from(*shard_id));
                },
            );
            query_builder.push(" ON CONFLICT DO NOTHING;");
            query_builder.build().execute(&self.meta_db_pool).await?;
        }
        Ok(())
    }

    async fn save_chunks_duplicate(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let chunks_duplicate = chunks
            .iter()
            .filter(|(_chunk_hash, _shard_id, height_included)| height_included != &block_height)
            .collect::<Vec<_>>();
        if !chunks_duplicate.is_empty() {
            crate::metrics::META_DATABASE_WRITE_QUERIES
                .with_label_values(&["save_chunks", "chunks_duplicate"])
                .inc();
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new("INSERT INTO chunks_duplicate (chunk_hash, block_height, shard_id, included_in_block_height) ");

            query_builder.push_values(
                chunks.iter(),
                |mut values, (chunk_hash, shard_id, height_included)| {
                    values
                        .push_bind(chunk_hash.to_string())
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(bigdecimal::BigDecimal::from(*shard_id))
                        .push_bind(bigdecimal::BigDecimal::from(*height_included));
                },
            );
            query_builder.push(" ON CONFLICT DO NOTHING;");
            query_builder.build().execute(&self.meta_db_pool).await?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl crate::StateIndexerDbManager for crate::PostgresDBManager {
    async fn save_block(
        &self,
        block_height: u64,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::metrics::META_DATABASE_WRITE_QUERIES
            .with_label_values(&["save_block", "blocks"])
            .inc();
        sqlx::query(
            "
            INSERT INTO blocks (block_height, block_hash)
            VALUES ($1, $2) ON CONFLICT DO NOTHING;
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(block_hash.to_string())
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    async fn save_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let save_chunks_unique_future = self.save_chunks_unique(block_height, chunks.clone());
        let save_chunks_duplicate_future = self.save_chunks_duplicate(block_height, chunks);

        futures::future::join_all([
            save_chunks_unique_future.boxed(),
            save_chunks_duplicate_future.boxed(),
        ])
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()
    }

    async fn get_block_height_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
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
        crate::metrics::META_DATABASE_WRITE_QUERIES
            .with_label_values(&["update_meta", "meta"])
            .inc();
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
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&["get_last_processed_block_height", "meta"])
            .inc();
        let (last_processed_block_height,): (bigdecimal::BigDecimal,) = sqlx::query_as(
            "
            SELECT last_processed_block_height
            FROM meta
            WHERE indexer_id = $1
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

    async fn save_validators(
        &self,
        epoch_id: near_primitives::hash::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &near_primitives::views::EpochValidatorInfo,
        epoch_end_block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::metrics::META_DATABASE_WRITE_QUERIES
            .with_label_values(&["add_validators", "validators"])
            .inc();
        let epoch_end_block_height = self
            .get_block_height_by_hash(epoch_end_block_hash, "add_validators")
            .await?;
        sqlx::query(
            "
            INSERT INTO validators (epoch_id, epoch_height, epoch_start_height, epoch_end_height, validators_info)
            VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING;
            "
        )
            .bind(epoch_id.to_string())
            .bind(bigdecimal::BigDecimal::from(epoch_height))
            .bind(bigdecimal::BigDecimal::from(epoch_start_height))
            .bind(bigdecimal::BigDecimal::from(epoch_end_block_height))
            .bind(&serde_json::to_value(validators_info)?)
            .execute(&self.meta_db_pool)
            .await?;
        Ok(())
    }

    async fn save_state_changes_data(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_data",
                "state_changes_data",
            ])
            .inc();
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
                        .push_bind(hex::encode(data_key).to_string())
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
        query_builder.push(" ON CONFLICT (account_id, data_key, block_height) DO UPDATE SET data_value = EXCLUDED.data_value;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn save_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_access_key",
                "state_changes_access_key",
            ])
            .inc();
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
                        .push_bind(hex::encode(data_key).to_string())
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
        query_builder.push(" ON CONFLICT (account_id, data_key, block_height) DO UPDATE SET data_value = EXCLUDED.data_value;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn save_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_contract",
                "state_changes_contract",
            ])
            .inc();
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
        query_builder.push(" ON CONFLICT (account_id, block_height) DO UPDATE SET data_value = EXCLUDED.data_value;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn save_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_account",
                "state_changes_account",
            ])
            .inc();
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
        query_builder.push(" ON CONFLICT (account_id, block_height) DO UPDATE SET data_value = EXCLUDED.data_value;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }
}
