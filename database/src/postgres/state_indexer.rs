use std::collections::HashMap;
use std::time::Instant;

use bigdecimal::ToPrimitive;
use futures::{future::try_join_all, FutureExt};
use sqlx::Row;

const PARTITIONS: i32 = 100;
const MAX_CONCURRENT_QUERIES: usize = 16;

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

    async fn partition_map(
        &self,
        pool: &sqlx::PgPool,
        account_ids: &Vec<String>,
    ) -> anyhow::Result<HashMap<String, i32>> {
        let now = std::time::Instant::now();
        let partition_rows = sqlx::query(
            "SELECT account_id, mod(hashtext(account_id), $2)::int AS partition
             FROM unnest($1::text[]) AS account_id",
        )
        .bind(account_ids)
        .bind(PARTITIONS)
        .fetch_all(pool)
        .await?;

        let partition_map: HashMap<String, i32> = partition_rows
            .into_iter()
            .map(|row| {
                let account_id: String = row.try_get("account_id").unwrap();
                let partition: i32 = row.try_get("partition").unwrap();
                (account_id, partition)
            })
            .collect();
        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Partition map computed in {:?} for {} accounts",
            now.elapsed(),
            account_ids.len()
        );
        Ok(partition_map)
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

        // Extract relevant data
        let inserts: Vec<(String, String, Vec<u8>, bigdecimal::BigDecimal)> = state_changes
            .iter()
            .filter_map(|change| {
                if let near_primitives::views::StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } = &change.value
                {
                    let data_key: String = hex::encode(key.as_slice());
                    Some((
                        account_id.to_string(),
                        data_key,
                        value.clone().to_vec(),
                        bigdecimal::BigDecimal::from(block_height),
                    ))
                } else {
                    None
                }
            })
            .collect();

        if inserts.is_empty() {
            return Ok(());
        }

        // Get all account_ids to compute partition map
        let account_ids: Vec<String> = inserts.iter().map(|(id, _, _, _)| id.clone()).collect();

        let pool = self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
            "Database connection for Shard_{} not found",
            shard_id
        ))?;

        // TODO: Watch for PARTITION_MAP_ELAPSED_TIME metric
        // This happens in each method, but we don't have a place to call it once for all operations.
        // Right now it seems like a neglectable overhead, but we should consider optimizing this.
        let partition_map = self.partition_map(&pool, &account_ids).await?;

        // Group inserts per partition
        let mut inserts_per_partition: HashMap<i32, Vec<_>> = HashMap::new();
        for (account_id, data_key, data_value, block_height) in inserts {
            if let Some(&partition) = partition_map.get(&account_id) {
                inserts_per_partition.entry(partition).or_default().push((
                    account_id,
                    data_key,
                    data_value,
                    block_height,
                ));
            } else {
                tracing::warn!("Partition not found for account_id: {}", account_id);
            }
        }

        // Build and execute inserts in parallel
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, rows) in inserts_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let table_name = format!("state_changes_data_compact_{}", partition_id);

            let task = tokio::spawn(async move {
                let start = Instant::now();
                let _permit = semaphore.acquire_owned().await.unwrap();
                let mut qb = sqlx::QueryBuilder::new(format!(
                "INSERT INTO {} (account_id, data_key, data_value, block_height_from, block_height_to) ",
                table_name,
            ));

                qb.push_values(
                    rows.iter(),
                    |mut row, (account_id, data_key, data_value, block_height)| {
                        row.push_bind(account_id)
                            .push_bind(data_key)
                            .push_bind(data_value)
                            .push_bind(block_height)
                            .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                    },
                );

                qb.push(" ON CONFLICT DO NOTHING");

                let result = qb.build().execute(&pool).await.map_err(anyhow::Error::from);

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Insert done partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    rows.len()
                );

                result
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;
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

        let updates: Vec<(String, String, bigdecimal::BigDecimal)> =
            state_changes
                .iter()
                .filter_map(|change| match &change.value {
                    near_primitives::views::StateChangeValueView::DataUpdate {
                        account_id,
                        key,
                        ..
                    }
                    | near_primitives::views::StateChangeValueView::DataDeletion {
                        account_id,
                        key,
                    } => {
                        let data_key: &[u8] = key.as_ref();
                        let data_key = hex::encode(data_key).to_string();
                        Some((
                            account_id.to_string(),
                            data_key,
                            bigdecimal::BigDecimal::from(block_height),
                        ))
                    }
                    _ => None,
                })
                .collect();

        let account_ids: Vec<String> = updates
            .iter()
            .map(|(account_id, _, _)| account_id.clone())
            .collect();

        let pool = self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
            "Database connection for Shard_{} not found",
            shard_id
        ))?;

        let partition_map = self.partition_map(&pool, &account_ids).await?;

        let mut updates_per_partition: HashMap<i32, Vec<(String, String, bigdecimal::BigDecimal)>> =
            HashMap::new();

        for (account_id, data_key, block_height) in updates {
            if let Some(&partition) = partition_map.get(&account_id) {
                updates_per_partition.entry(partition).or_default().push((
                    account_id,
                    data_key,
                    block_height,
                ));
            } else {
                tracing::warn!("Partition not found for account_id: {}", account_id);
            }
        }

        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, rows) in updates_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let table_name = format!("state_changes_data_compact_{}", partition_id);

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();
                let start = Instant::now();
                let mut qb = sqlx::QueryBuilder::new(
                    "WITH new_data (account_id, data_key, block_height) AS (",
                );

                qb.push_values(
                    rows.iter(),
                    |mut row, (account_id, data_key, block_height)| {
                        row.push_bind(account_id)
                            .push_bind(data_key)
                            .push_bind(block_height);
                    },
                );

                qb.push(format!(
                    ") UPDATE {} AS old \
               SET block_height_to = new_data.block_height \
               FROM new_data \
               WHERE old.account_id = new_data.account_id \
               AND old.data_key = new_data.data_key \
               AND old.block_height_from < new_data.block_height \
               AND old.block_height_to IS NULL;",
                    table_name,
                ));

                let result = qb.build().execute(&pool).await.map_err(anyhow::Error::from);

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Update done partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    rows.len()
                );

                result
            });

            tasks.push(task);
        }

        // Wait for all partition updates to complete
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn insert_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let overall_start = Instant::now();

        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_access_key",
                "state_changes_access_key",
            ])
            .inc();

        // Extract relevant updates
        let inserts: Vec<(String, String, Vec<u8>, bigdecimal::BigDecimal)> = state_changes
            .iter()
            .filter_map(|change| {
                if let near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    access_key,
                } = &change.value
                {
                    let data_key = hex::encode(
                        borsh::to_vec(public_key).expect("Failed to borsh serialize public key"),
                    );
                    let data_value =
                        borsh::to_vec(access_key).expect("Failed to borsh serialize access key");
                    Some((
                        account_id.to_string(),
                        data_key,
                        data_value,
                        bigdecimal::BigDecimal::from(block_height),
                    ))
                } else {
                    None
                }
            })
            .collect();

        if inserts.is_empty() {
            return Ok(());
        }

        // Get all account_ids for partition mapping
        let account_ids: Vec<String> = inserts.iter().map(|(id, _, _, _)| id.clone()).collect();

        let pool = self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
            "Database connection for Shard_{} not found",
            shard_id
        ))?;

        let partition_map = self.partition_map(&pool, &account_ids).await?;

        // Group inserts per partition
        let mut inserts_per_partition: HashMap<i32, Vec<_>> = HashMap::new();
        for (account_id, data_key, data_value, block_height) in inserts {
            if let Some(&partition) = partition_map.get(&account_id) {
                inserts_per_partition.entry(partition).or_default().push((
                    account_id,
                    data_key,
                    data_value,
                    block_height,
                ));
            }
        }

        // Insert in parallel per partition
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();
        for (partition_id, rows) in inserts_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let table_name = format!("state_changes_access_key_compact_{}", partition_id);

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();
                let start = Instant::now();

                let mut qb = sqlx::QueryBuilder::new(format!(
                "INSERT INTO {} (account_id, data_key, data_value, block_height_from, block_height_to) ",
                table_name
            ));

                qb.push_values(
                    rows.iter(),
                    |mut row, (account_id, data_key, data_value, block_height)| {
                        row.push_bind(account_id)
                            .push_bind(data_key)
                            .push_bind(data_value)
                            .push_bind(block_height)
                            .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                    },
                );

                qb.push(" ON CONFLICT DO NOTHING");

                let result = qb.build().execute(&pool).await.map_err(anyhow::Error::from);

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Insert done partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    rows.len()
                );

                result
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;

        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Total insert_state_changes_access_key duration shard={} elapsed={:?}",
            shard_id,
            overall_start.elapsed()
        );

        Ok(())
    }

    async fn update_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let overall_start = Instant::now();

        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_access_key",
                "state_changes_access_key",
            ])
            .inc();

        // Collect updates: (account_id, data_key)
        let updates: Vec<(String, String)> = state_changes
            .iter()
            .filter_map(|c| match &c.value {
                near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    ..
                }
                | near_primitives::views::StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    public_key,
                } => Some((account_id.to_string(), hex::encode(public_key.key_data()))),
                _ => None,
            })
            .collect();

        if updates.is_empty() {
            return Ok(());
        }

        // Compute partitions
        let account_ids: Vec<String> = updates.iter().map(|(id, _)| id.clone()).collect();
        let pool = self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
            "Database connection for Shard_{} not found",
            shard_id
        ))?;

        let partition_map = self.partition_map(&pool, &account_ids).await?;

        // Group updates per partition
        let mut updates_per_partition: HashMap<i32, Vec<(String, String)>> = HashMap::new();
        for (account_id, data_key) in updates {
            if let Some(&partition) = partition_map.get(&account_id) {
                updates_per_partition
                    .entry(partition)
                    .or_default()
                    .push((account_id, data_key));
            }
        }

        // Parallel update execution per partition
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();
        for (partition_id, rows) in updates_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let block_height_bd = bigdecimal::BigDecimal::from(block_height);

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();
                let start = Instant::now();
                let (account_ids, data_keys): (Vec<_>, Vec<_>) = rows.into_iter().unzip();

                // Use UNNEST for batch update
                let query = format!(
                    r#"
                UPDATE state_changes_access_key_compact_{partition_id} AS t
                SET block_height_to = $3
                FROM (
                    SELECT unnest($1::text[]) AS account_id, unnest($2::text[]) AS data_key
                ) AS u
                WHERE t.account_id = u.account_id
                  AND t.data_key = u.data_key
                  AND t.block_height_to IS NULL;
                "#,
                    partition_id = partition_id
                );

                sqlx::query(&query)
                    .bind(&account_ids)
                    .bind(&data_keys)
                    .bind(&block_height_bd)
                    .execute(&pool)
                    .await?;

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Update done partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    account_ids.len()
                );

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;

        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Total update_state_changes_access_key duration shard={} elapsed={:?}",
            shard_id,
            overall_start.elapsed()
        );

        Ok(())
    }

    async fn insert_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let overall_start = Instant::now();
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_contract",
                "state_changes_contract",
            ])
            .inc();

        // Extract only ContractCodeUpdate
        let inserts: Vec<(String, Vec<u8>, bigdecimal::BigDecimal)> = state_changes
            .into_iter()
            .filter_map(|change| {
                if let near_primitives::views::StateChangeValueView::ContractCodeUpdate {
                    account_id,
                    code,
                } = change.value
                {
                    Some((
                        account_id.to_string(),
                        code.to_vec(),
                        bigdecimal::BigDecimal::from(block_height),
                    ))
                } else {
                    None
                }
            })
            .collect();

        if inserts.is_empty() {
            return Ok(());
        }

        // Compute partitions
        let account_ids: Vec<String> = inserts.iter().map(|(id, _, _)| id.clone()).collect();
        let pool = self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
            "Database connection for Shard_{} not found",
            shard_id
        ))?;

        let partition_map = self.partition_map(&pool, &account_ids).await?;

        // Group rows by partition
        let mut inserts_per_partition: HashMap<i32, Vec<_>> = HashMap::new();
        for (account_id, data_value, block_height) in inserts {
            if let Some(&partition) = partition_map.get(&account_id) {
                inserts_per_partition.entry(partition).or_default().push((
                    account_id,
                    data_value,
                    block_height,
                ));
            }
        }

        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, rows) in inserts_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let start = Instant::now();

                let table_name = format!("state_changes_contract_compact_{}", partition_id);
                let mut qb = sqlx::QueryBuilder::new(format!(
                    "INSERT INTO {} (account_id, data_value, block_height_from, block_height_to) ",
                    table_name
                ));

                qb.push_values(
                    rows.iter(),
                    |mut row, (account_id, data_value, block_height)| {
                        row.push_bind(account_id)
                            .push_bind(data_value)
                            .push_bind(block_height)
                            .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                    },
                );

                qb.push(" ON CONFLICT DO NOTHING");
                qb.build().execute(&pool).await?;

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Insert contract partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    rows.len()
                );

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;
        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Total insert_state_changes_contract duration shard={} elapsed={:?}",
            shard_id,
            overall_start.elapsed()
        );

        Ok(())
    }

    /// Update contract state changes with partitions under semaphore
    async fn update_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let overall_start = Instant::now();
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_contract",
                "state_changes_contract",
            ])
            .inc();

        // Collect account_ids for updates
        let accounts: Vec<String> = state_changes
            .into_iter()
            .filter_map(|change| match change.value {
                near_primitives::views::StateChangeValueView::ContractCodeUpdate {
                    account_id,
                    ..
                }
                | near_primitives::views::StateChangeValueView::ContractCodeDeletion {
                    account_id,
                } => Some(account_id.to_string()),
                _ => None,
            })
            .collect();

        if accounts.is_empty() {
            return Ok(());
        }

        let pool = self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
            "Database connection for Shard_{} not found",
            shard_id
        ))?;

        let partition_map = self.partition_map(&pool, &accounts).await?;

        let mut updates_per_partition: HashMap<i32, Vec<String>> = HashMap::new();
        for account_id in accounts {
            if let Some(&partition) = partition_map.get(&account_id) {
                updates_per_partition
                    .entry(partition)
                    .or_default()
                    .push(account_id);
            } else {
                tracing::warn!("Partition not found for account_id: {}", account_id);
            }
        }

        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, account_ids) in updates_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let block_height_bd = bigdecimal::BigDecimal::from(block_height);

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let start = Instant::now();

                let query = format!(
                    r#"
                UPDATE state_changes_contract_compact_{partition_id} AS t
                SET block_height_to = $2
                FROM (SELECT unnest($1::text[]) AS account_id) AS u
                WHERE t.account_id = u.account_id
                  AND t.block_height_to IS NULL;
                "#
                );

                sqlx::query(&query)
                    .bind(&account_ids)
                    .bind(&block_height_bd)
                    .execute(&pool)
                    .await?;

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Update contract partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    account_ids.len()
                );

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;
        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Total update_state_changes_contract duration shard={} elapsed={:?}",
            shard_id,
            overall_start.elapsed()
        );

        Ok(())
    }

    async fn insert_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let overall_start = Instant::now();
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_account",
                "state_changes_account",
            ])
            .inc();

        // Extract account updates
        let inserts: Vec<(String, Vec<u8>, bigdecimal::BigDecimal)> = state_changes
            .into_iter()
            .filter_map(|change| {
                if let near_primitives::views::StateChangeValueView::AccountUpdate {
                    account_id,
                    account,
                } = change.value
                {
                    let data_value =
                        borsh::to_vec(&near_primitives::account::Account::from(&account))
                            .expect("Failed to borsh serialize account");
                    Some((
                        account_id.to_string(),
                        data_value,
                        bigdecimal::BigDecimal::from(block_height),
                    ))
                } else {
                    None
                }
            })
            .collect();

        if inserts.is_empty() {
            return Ok(());
        }

        // Compute partitions
        let account_ids: Vec<String> = inserts.iter().map(|(id, _, _)| id.clone()).collect();
        let pool = self.shards_pool.get(&shard_id).unwrap();

        let partition_map = self.partition_map(&pool, &account_ids).await?;

        let mut inserts_per_partition: HashMap<i32, Vec<_>> = HashMap::new();
        for (account_id, data_value, block_height) in inserts {
            if let Some(&partition) = partition_map.get(&account_id) {
                inserts_per_partition.entry(partition).or_default().push((
                    account_id,
                    data_value,
                    block_height,
                ));
            }
        }

        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, rows) in inserts_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let start = Instant::now();

                let table_name = format!("state_changes_account_compact_{}", partition_id);
                let mut qb = sqlx::QueryBuilder::new(format!(
                    "INSERT INTO {} (account_id, data_value, block_height_from, block_height_to) ",
                    table_name
                ));

                qb.push_values(
                    rows.iter(),
                    |mut row, (account_id, data_value, block_height)| {
                        row.push_bind(account_id)
                            .push_bind(data_value)
                            .push_bind(block_height)
                            .push_bind(None::<Option<bigdecimal::BigDecimal>>);
                    },
                );

                qb.push(" ON CONFLICT DO NOTHING");
                qb.build().execute(&pool).await?;

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Insert account partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    rows.len()
                );

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;
        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Total insert_state_changes_account duration shard={} elapsed={:?}",
            shard_id,
            overall_start.elapsed()
        );

        Ok(())
    }

    /// Update Account state changes using partitions + concurrency limit
    async fn update_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let overall_start = Instant::now();
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id.to_string(),
                "save_state_changes_account",
                "state_changes_account",
            ])
            .inc();

        // Collect accounts for update
        let accounts: Vec<String> = state_changes
            .into_iter()
            .filter_map(|change| match change.value {
                near_primitives::views::StateChangeValueView::AccountUpdate {
                    account_id, ..
                }
                | near_primitives::views::StateChangeValueView::AccountDeletion { account_id } => {
                    Some(account_id.to_string())
                }
                _ => None,
            })
            .collect();

        if accounts.is_empty() {
            return Ok(());
        }

        // Compute partitions
        let pool = self.shards_pool.get(&shard_id).unwrap();

        let partition_map = self.partition_map(&pool, &accounts).await?;

        let mut updates_per_partition: HashMap<i32, Vec<String>> = HashMap::new();
        for account_id in accounts {
            if let Some(&partition) = partition_map.get(&account_id) {
                updates_per_partition
                    .entry(partition)
                    .or_default()
                    .push(account_id);
            } else {
                tracing::warn!("Partition not found for account_id: {}", account_id);
            }
        }

        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, account_ids) in updates_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let block_height_bd = bigdecimal::BigDecimal::from(block_height);

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let start = Instant::now();

                let query = format!(
                    r#"
                UPDATE state_changes_account_compact_{partition_id} AS t
                SET block_height_to = $2
                FROM (SELECT unnest($1::text[]) AS account_id) AS u
                WHERE t.account_id = u.account_id
                  AND t.block_height_to IS NULL;
                "#
                );

                sqlx::query(&query)
                    .bind(&account_ids)
                    .bind(&block_height_bd)
                    .execute(&pool)
                    .await?;

                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Update account partition={} elapsed={:?} rows={}",
                    partition_id,
                    start.elapsed(),
                    account_ids.len()
                );

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;
        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Total update_state_changes_account duration shard={} elapsed={:?}",
            shard_id,
            overall_start.elapsed()
        );

        Ok(())
    }
}
