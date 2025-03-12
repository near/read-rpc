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

    async fn insert_state_changes_data(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_data",
                "state_changes_data",
            ])
            .inc();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO new_state_changes_data (account_id, data_key, data_value, block_height_from, block_height_to) ",
        );

        query_builder.push_values(
            state_changes.iter().filter(|c| {
                matches!(
                    c.value,
                    near_primitives::views::StateChangeValueView::DataUpdate { .. }
                )
            }),
            |mut values, state_change| {
                if let near_primitives::views::StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } = &state_change.value
                {
                    let data_key: &[u8] = key.as_ref();
                    let data_value: &[u8] = value.as_ref();
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(hex::encode(data_key).to_string())
                        .push_bind(data_value)
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                }
            },
        );
        query_builder.push(" ON CONFLICT DO NOTHING;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn update_state_changes_data(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_data",
                "state_changes_data",
            ])
            .inc();

        // let mut accounts_ids = vec![];
        // let mut data_keys = vec![];
        let mut accounts: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for state_change in state_changes.iter() {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    ..
                }
                | near_primitives::views::StateChangeValueView::DataDeletion { account_id, key } => {
                    let data_key: &[u8] = key.as_ref();
                    let data_key = hex::encode(data_key).to_string();
                    accounts
                        .entry(account_id.to_string())
                        .or_default()
                        .push(data_key);
                    // accounts_ids.push(account_id.to_string());
                    // data_keys.push(hex::encode(data_key).to_string());
                }
                _ => {}
            }
        }
        let mut tasks = Vec::new();

        for (account_id, data_keys) in accounts.into_iter() {
            let pool = self.shards_pool.get(&shard_id).unwrap().clone();
            let task = tokio::task::spawn(async move {
                sqlx::query("CALL state_data_update_latest_block_height($1, $2, $3)")
                    .bind(account_id)
                    .bind(data_keys)
                    .bind(bigdecimal::BigDecimal::from(block_height))
                    .execute(&pool)
                    .await
            });

            tasks.push(task);
        }
        for task in tasks {
            task.await??;
        }
        // sqlx::query(
        //     "
        //     WITH latest_rows AS (
        //         SELECT account_id, data_key, MAX(block_height_from) AS block_height_from
        //         FROM new_state_changes_data
        //         WHERE account_id = ANY($1)
        //           AND data_key = ANY($2)
        //           AND block_height_from < $3
        //         GROUP BY account_id, data_key
        //     )
        //     UPDATE new_state_changes_data AS target
        //     SET block_height_to = $3
        //     FROM latest_rows
        //     WHERE target.account_id = latest_rows.account_id
        //       AND target.data_key = latest_rows.data_key
        //       AND target.block_height_from = latest_rows.block_height_from;
        //     ",
        // )
        // .bind(accounts_ids)
        // .bind(data_keys)
        // .bind(bigdecimal::BigDecimal::from(block_height))
        // .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
        //     "Database connection for Shard_{} not found",
        //     shard_id
        // ))?)
        // .await?;
        Ok(())
    }

    async fn insert_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_access_key",
                "state_changes_access_key",
            ])
            .inc();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO new_state_changes_access_key (account_id, data_key, data_value, block_height_from, block_height_to) ",
        );
        query_builder.push_values(
            state_changes.iter().filter(|c| {
                matches!(
                    c.value,
                    near_primitives::views::StateChangeValueView::AccessKeyUpdate { .. }
                )
            }),
            |mut values, state_change| {
                if let near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    access_key,
                } = &state_change.value
                {
                    let data_key =
                        borsh::to_vec(public_key).expect("Failed to borsh serialize public key");
                    let data_value =
                        borsh::to_vec(access_key).expect("Failed to borsh serialize access key");
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(hex::encode(data_key).to_string())
                        .push_bind(data_value)
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                }
            },
        );
        query_builder.push(" ON CONFLICT DO NOTHING;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn update_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_access_key",
                "state_changes_access_key",
            ])
            .inc();
        // let mut accounts_ids = vec![];
        // let mut data_keys = vec![];
        let mut accounts: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for state_change in state_changes.iter() {
            match &state_change.value {
                near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    ..
                }
                | near_primitives::views::StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    public_key,
                } => {
                    // accounts_ids.push(account_id.to_string());
                    // data_keys.push(hex::encode(public_key.key_data()).to_string());
                    accounts
                        .entry(account_id.to_string())
                        .or_default()
                        .push(hex::encode(public_key.key_data()).to_string());
                }
                _ => {}
            }
        }
        let mut tasks = Vec::new();

        for (account_id, data_keys) in accounts.into_iter() {
            let pool = self.shards_pool.get(&shard_id).unwrap().clone();
            let task = tokio::task::spawn(async move {
                sqlx::query("CALL access_keys_update_latest_block_height($1, $2, $3)")
                    .bind(account_id)
                    .bind(data_keys)
                    .bind(bigdecimal::BigDecimal::from(block_height))
                    .execute(&pool)
                    .await
            });

            tasks.push(task);
        }
        for task in tasks {
            task.await??;
        }

        // sqlx::query(
        //     "
        //     WITH latest_rows AS (
        //         SELECT account_id, data_key, MAX(block_height_from) AS block_height_from
        //         FROM new_state_changes_access_key
        //         WHERE account_id = ANY($1)
        //           AND data_key = ANY($2)
        //           AND block_height_from < $3
        //         GROUP BY account_id, data_key
        //     )
        //     UPDATE new_state_changes_access_key AS target
        //     SET block_height_to = $3
        //     FROM latest_rows
        //     WHERE target.account_id = latest_rows.account_id
        //       AND target.data_key = latest_rows.data_key
        //       AND target.block_height_from = latest_rows.block_height_from;
        //     ",
        // )
        // .bind(accounts_ids)
        // .bind(data_keys)
        // .bind(bigdecimal::BigDecimal::from(block_height))
        // .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
        //     "Database connection for Shard_{} not found",
        //     shard_id
        // ))?)
        // .await?;
        Ok(())
    }

    async fn insert_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_contract",
                "state_changes_contract",
            ])
            .inc();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO new_state_changes_contract (account_id, data_value, block_height_from, block_height_to) ",
        );
        query_builder.push_values(
            state_changes.iter().filter(|c| {
                matches!(
                    c.value,
                    near_primitives::views::StateChangeValueView::ContractCodeUpdate { .. }
                )
            }),
            |mut values, state_change| {
                if let near_primitives::views::StateChangeValueView::ContractCodeUpdate {
                    account_id,
                    code,
                } = &state_change.value
                {
                    let data_value: &[u8] = code.as_ref();
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(data_value)
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                }
            },
        );
        query_builder.push(" ON CONFLICT DO NOTHING;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn update_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_contract",
                "state_changes_contract",
            ])
            .inc();
        let mut accounts_ids = vec![];
        for state_change in state_changes.into_iter() {
            match state_change.value.clone() {
                near_primitives::views::StateChangeValueView::ContractCodeUpdate {
                    account_id,
                    ..
                }
                | near_primitives::views::StateChangeValueView::ContractCodeDeletion {
                    account_id,
                } => {
                    accounts_ids.push(account_id.to_string());
                }
                _ => {}
            }
        }

        sqlx::query("CALL contracts_update_latest_block_height($1, $2)")
            .bind(accounts_ids)
            .bind(bigdecimal::BigDecimal::from(block_height))
            .execute(self.shards_pool.get(&shard_id).unwrap())
            .await?;

        // sqlx::query(
        //     "
        //     WITH latest_rows AS (
        //     SELECT account_id, MAX(block_height_from) AS block_height_from
        //     FROM new_state_changes_contract
        //     WHERE account_id = ANY($1)
        //       AND block_height_from < $2
        //     GROUP BY account_id
        //     )
        //     UPDATE new_state_changes_contract AS target
        //     SET block_height_to = $2
        //     FROM latest_rows
        //     WHERE target.account_id = latest_rows.account_id
        //       AND target.block_height_from = latest_rows.block_height_from;
        //     ",
        // )
        // .bind(accounts_ids)
        // .bind(bigdecimal::BigDecimal::from(block_height))
        // .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
        //     "Database connection for Shard_{} not found",
        //     shard_id
        // ))?)
        // .await?;
        Ok(())
    }

    async fn insert_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_account",
                "state_changes_account",
            ])
            .inc();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO new_state_changes_account (account_id, data_value, block_height_from, block_height_to) ",
        );
        query_builder.push_values(
            state_changes.iter().filter(|c| {
                matches!(
                    c.value,
                    near_primitives::views::StateChangeValueView::AccountUpdate { .. }
                )
            }),
            |mut values, state_change| {
                if let near_primitives::views::StateChangeValueView::AccountUpdate {
                    account_id,
                    account,
                } = &state_change.value
                {
                    let data_value =
                        borsh::to_vec(&near_primitives::account::Account::from(account))
                            .expect("Failed to borsh serialize account");
                    values
                        .push_bind(account_id.to_string())
                        .push_bind(data_value)
                        .push_bind(bigdecimal::BigDecimal::from(block_height))
                        .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                }
            },
        );
        query_builder.push(" ON CONFLICT DO NOTHING;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn update_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_account",
                "state_changes_account",
            ])
            .inc();

        let mut accounts_ids = vec![];
        for state_change in state_changes.into_iter() {
            match state_change.value.clone() {
                near_primitives::views::StateChangeValueView::AccountUpdate {
                    account_id, ..
                }
                | near_primitives::views::StateChangeValueView::AccountDeletion { account_id } => {
                    accounts_ids.push(account_id.to_string());
                }
                _ => {}
            }
        }

        sqlx::query("CALL accounts_update_latest_block_height($1, $2)")
            .bind(accounts_ids)
            .bind(bigdecimal::BigDecimal::from(block_height))
            .execute(self.shards_pool.get(&shard_id).unwrap())
            .await?;

        // sqlx::query(
        //     "
        //     WITH latest_rows AS (
        //     SELECT account_id, MAX(block_height_from) AS block_height_from
        //     FROM new_state_changes_account
        //     WHERE account_id = ANY($1)
        //       AND block_height_from < $2
        //     GROUP BY account_id
        //     )
        //     UPDATE new_state_changes_account AS target
        //     SET block_height_to = $2
        //     FROM latest_rows
        //     WHERE target.account_id = latest_rows.account_id
        //       AND target.block_height_from = latest_rows.block_height_from;
        //     ",
        // )
        // .bind(accounts_ids)
        // .bind(bigdecimal::BigDecimal::from(block_height))
        // .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
        //     "Database connection for Shard_{} not found",
        //     shard_id
        // ))?)
        // .await?;
        Ok(())
    }
}
