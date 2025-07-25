use std::collections::HashMap;
use std::time::Instant;

use futures::future::try_join_all;
use sqlx::Row;

/// PostgreSQL State Indexer Implementation
///
/// ARCHITECTURAL OVERVIEW:
/// This module implements efficient batch processing for NEAR Protocol state changes
/// across partitioned PostgreSQL tables. The design focuses on:
///
/// 1. **Partitioned Tables**: All state tables are horizontally partitioned by account_id
///    using PostgreSQL's hashtext() function for consistent distribution
///
/// 2. **Batch Processing**: Operations are grouped by partition and executed in parallel
///    to maximize throughput while respecting connection pool limits
///
/// 3. **Helper Function Strategy**: Four specialized helpers handle different data patterns:
///    - execute_partitioned_account_update: Account-only updates (UNNEST pattern)
///    - execute_partitioned_key_update: Composite key updates (CTE pattern)
///    - execute_partitioned_standard_insert: 4-column inserts with data_key
///    - execute_partitioned_keyless_insert: 3-column inserts without data_key
///
/// 4. **SQL Pattern Selection**: Different update patterns optimize for different scenarios:
///    - UNNEST: Efficient for uniform operations (same block_height, simple matching)
///    - CTE: Required for variable data operations (different block_heights, complex conditions)
///
/// 5. **Concurrency Control**: Semaphores limit parallel operations to prevent database
///    connection pool exhaustion while maximizing throughput
///
/// SPECIAL CASES:
/// - update_state_changes_access_key: Uses in-place UNNEST pattern instead of CTE helper
///   due to uniform block_height and simpler matching requirements (see method comments)
///
impl crate::PostgresDBManager {
    /// Helper function for partitioned update operations using account-only updates
    ///
    /// This helper is used for tables that only have account_id as the primary key component
    /// (like account and contract tables), where we need to update block_height_to for all
    /// rows matching specific account_ids.
    ///
    /// SQL Pattern: Uses UNNEST with a single array for account_ids
    /// ```sql
    /// UPDATE table_partition AS t
    /// SET block_height_to = $2
    /// FROM (SELECT unnest($1::text[]) AS account_id) AS u
    /// WHERE t.account_id = u.account_id AND t.block_height_to IS NULL;
    /// ```
    ///
    /// Used by: update_state_changes_account, update_state_changes_contract
    pub(crate) async fn execute_partitioned_account_update(
        &self,
        shard_id: near_primitives::types::ShardId,
        table_prefix: String,
        operation_name: String,
        account_ids: Vec<String>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        if account_ids.is_empty() {
            return Ok(());
        }

        // Get database connection pool for this shard
        let pool = self.get_shard_pool(shard_id)?;

        // Compute partition assignments for all account_ids using PostgreSQL's hashtext() function
        // This ensures consistent partition distribution matching the table partitioning scheme
        let partition_map = self.partition_map(&shard_id, &pool, &account_ids).await?;

        // Group account_ids by their target partition for batch processing
        // This reduces the number of database queries by updating entire partitions at once
        let mut accounts_per_partition: HashMap<i32, Vec<String>> = HashMap::new();
        for account_id in account_ids {
            if let Some(&partition) = partition_map.get(&account_id) {
                accounts_per_partition
                    .entry(partition)
                    .or_default()
                    .push(account_id);
            } else {
                tracing::warn!("Partition not found for account_id: {}", account_id);
            }
        }
        crate::metrics::PARTITIONS_TOUCHED_COUNT
            .with_label_values(&[
                &shard_id.to_string(),
                &operation_name,
                &accounts_per_partition.len().to_string(),
            ])
            .inc();

        // Execute updates in parallel across partitions with concurrency control
        // Each partition can be updated independently, improving throughput
        let semaphore =
            std::sync::Arc::new(tokio::sync::Semaphore::new(super::MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, partition_accounts) in accounts_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let operation_name = operation_name.clone();
            let table_prefix = table_prefix.clone();
            let block_height_bd = bigdecimal::BigDecimal::from(block_height);

            let task = tokio::spawn(async move {
                // Acquire semaphore permit to limit concurrent database operations
                let _permit = semaphore.acquire_owned().await.unwrap();
                let start = Instant::now();

                // Build UPDATE query using UNNEST to batch-process multiple account_ids
                // UNNEST converts the array parameter into rows for efficient JOIN operations
                let query = format!(
                    r#"
                UPDATE {table_prefix}_{partition_id} AS t
                SET block_height_to = $2
                FROM (SELECT unnest($1::text[]) AS account_id) AS u
                WHERE t.account_id = u.account_id
                  AND t.block_height_to IS NULL;
                "#,
                    table_prefix = table_prefix,
                    partition_id = partition_id
                );

                sqlx::query(&query)
                    .bind(&partition_accounts)
                    .bind(&block_height_bd)
                    .execute(&pool)
                    .await?;

                crate::metrics::SHARD_DATABASE_WRITE_ELAPSED_TIME
                    .with_label_values(&[
                        &shard_id.to_string(),
                        &operation_name,
                        &start.elapsed().as_millis().to_string(),
                    ])
                    .inc();
                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Update done operation={} partition={} elapsed={:?} rows={}",
                    operation_name,
                    partition_id,
                    start.elapsed(),
                    partition_accounts.len()
                );

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        try_join_all(tasks).await?;
        Ok(())
    }

    /// Helper function for partitioned updates with composite key (account_id + data_key)
    ///
    /// This helper is used for tables that have composite primary keys with both account_id
    /// and data_key components (like state_changes_data table), where we need precise
    /// row-level updates based on both key components.
    ///
    /// SQL Pattern: Uses CTE (Common Table Expression) with VALUES for structured data
    /// ```sql
    /// WITH new_data (account_id, data_key, block_height) AS (
    ///   VALUES ('acc1', 'key1', 100), ('acc2', 'key2', 100), ...
    /// )
    /// UPDATE table AS old
    /// SET block_height_to = new_data.block_height
    /// FROM new_data
    /// WHERE old.account_id = new_data.account_id
    ///   AND old.data_key = new_data.data_key
    ///   AND old.block_height_from < new_data.block_height
    ///   AND old.block_height_to IS NULL;
    /// ```
    ///
    /// The CTE approach is necessary here because we need to match on multiple columns
    /// with different block_height values per row, which UNNEST cannot handle efficiently.
    ///
    /// Used by: update_state_changes_data
    pub(crate) async fn execute_partitioned_key_update(
        &self,
        shard_id: near_primitives::types::ShardId,
        table_prefix: String,
        operation_name: String,
        updates: Vec<(String, String, bigdecimal::BigDecimal)>, // (account_id, data_key, block_height)
    ) -> anyhow::Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let pool = self.get_shard_pool(shard_id)?;
        // Extract account_ids for partition mapping (data_key distribution is handled by account_id partitioning)
        let account_ids: Vec<String> = updates.iter().map(|(id, _, _)| id.clone()).collect();
        crate::metrics::AFFECTED_ACCOUNTS_COUNT
            .with_label_values(&[
                &shard_id.to_string(),
                &operation_name,
                &account_ids.len().to_string(),
            ])
            .inc();
        let partition_map = self.partition_map(&shard_id, &pool, &account_ids).await?;

        // Group updates by partition, preserving the complete tuple for CTE processing
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
        crate::metrics::PARTITIONS_TOUCHED_COUNT
            .with_label_values(&[
                &shard_id.to_string(),
                &operation_name,
                &updates_per_partition.len().to_string(),
            ])
            .inc();

        let semaphore =
            std::sync::Arc::new(tokio::sync::Semaphore::new(super::MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, rows) in updates_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let table_name = format!("{}_{}", table_prefix, partition_id);
            let operation_name = operation_name.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();
                let start = Instant::now();

                // Build CTE-based UPDATE query using sqlx QueryBuilder for type safety
                // CTE allows us to provide structured data (account_id, data_key, block_height)
                // and join it efficiently with the target table for precise updates
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

                // Complete the CTE and add the UPDATE clause with all necessary conditions
                // The four AND conditions ensure data integrity and proper versioning:
                // 1. account_id match - partition-level key
                // 2. data_key match - row-level key
                // 3. block_height comparison - prevents updating newer data with older data
                // 4. NULL check - only update active records (not already closed)
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
                crate::metrics::SHARD_DATABASE_WRITE_ELAPSED_TIME
                    .with_label_values(&[
                        &shard_id.to_string(),
                        &operation_name,
                        &start.elapsed().as_millis().to_string(),
                    ])
                    .inc();
                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Update done operation={} partition={} elapsed={:?} rows={}",
                    operation_name,
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

    /// Helper function for standard 4-column inserts with data_key
    ///
    /// This helper handles inserts into tables with composite keys that include both
    /// account_id and data_key (like state_changes_data and state_changes_access_key).
    ///
    /// Table Schema: (account_id, data_key, data_value, block_height_from, block_height_to)
    /// - account_id: partition key for data distribution
    /// - data_key: secondary key component (hex-encoded for data, borsh-serialized for access keys)
    /// - data_value: the actual state data (raw bytes)
    /// - block_height_from: when this version became active
    /// - block_height_to: when this version was superseded (NULL for current)
    ///
    /// Used by: insert_state_changes_data, insert_state_changes_access_key
    pub(crate) async fn execute_partitioned_standard_insert(
        &self,
        shard_id: near_primitives::types::ShardId,
        table_prefix: String,
        operation_name: String,
        inserts: Vec<(String, String, Vec<u8>, bigdecimal::BigDecimal)>, // (account_id, data_key, data_value, block_height)
    ) -> anyhow::Result<()> {
        if inserts.is_empty() {
            return Ok(());
        }

        let pool = self.get_shard_pool(shard_id)?;
        let account_ids: Vec<String> = inserts.iter().map(|(id, _, _, _)| id.clone()).collect();
        crate::metrics::AFFECTED_ACCOUNTS_COUNT
            .with_label_values(&[
                &shard_id.to_string(),
                &operation_name,
                &account_ids.len().to_string(),
            ])
            .inc();

        let partition_map = self.partition_map(&shard_id, &pool, &account_ids).await?;

        // Group inserts by partition for efficient batch processing
        let mut inserts_per_partition: HashMap<
            i32,
            Vec<(String, String, Vec<u8>, bigdecimal::BigDecimal)>,
        > = HashMap::new();
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
        crate::metrics::PARTITIONS_TOUCHED_COUNT
            .with_label_values(&[
                &shard_id.to_string(),
                &operation_name,
                &inserts_per_partition.len().to_string(),
            ])
            .inc();

        let semaphore =
            std::sync::Arc::new(tokio::sync::Semaphore::new(super::MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, rows) in inserts_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let table_name = format!("{}_{}", table_prefix, partition_id);
            let operation_name = operation_name.clone();

            let task = tokio::spawn(async move {
                let start = Instant::now();
                let _permit = semaphore.acquire_owned().await.unwrap();

                // Build batch INSERT using sqlx QueryBuilder for type safety and performance
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
                            .push_bind(None::<Option<bigdecimal::BigDecimal>>); // block_height_to starts as NULL (active)
                    },
                );

                // Use ON CONFLICT DO NOTHING to handle duplicate inserts gracefully
                // This is important for idempotency during indexer restarts or replays
                qb.push(" ON CONFLICT DO NOTHING");

                let result = qb.build().execute(&pool).await.map_err(anyhow::Error::from);

                crate::metrics::SHARD_DATABASE_WRITE_ELAPSED_TIME
                    .with_label_values(&[
                        &shard_id.to_string(),
                        &operation_name,
                        &start.elapsed().as_millis().to_string(),
                    ])
                    .inc();
                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Insert done operation={} partition={} elapsed={:?} rows={}",
                    operation_name,
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

    /// Helper function for keyless 3-column inserts without data_key
    ///
    /// This helper handles inserts into tables that only use account_id as the key
    /// (like state_changes_account and state_changes_contract), where the data_value
    /// represents the entire state of the account or contract.
    ///
    /// Table Schema: (account_id, data_value, block_height_from, block_height_to)
    /// - account_id: primary partition key
    /// - data_value: complete state data (borsh-serialized account or contract code)
    /// - block_height_from: when this version became active
    /// - block_height_to: when this version was superseded (NULL for current)
    ///
    /// The absence of data_key means each account/contract has exactly one active
    /// record at any given block height, representing its complete state.
    ///
    /// Used by: insert_state_changes_account, insert_state_changes_contract
    pub(crate) async fn execute_partitioned_keyless_insert(
        &self,
        shard_id: near_primitives::types::ShardId,
        table_prefix: String,
        operation_name: String,
        inserts: Vec<(String, Vec<u8>, bigdecimal::BigDecimal)>, // (account_id, data_value, block_height)
    ) -> anyhow::Result<()> {
        if inserts.is_empty() {
            return Ok(());
        }

        let pool = self.get_shard_pool(shard_id)?;
        let account_ids: Vec<String> = inserts.iter().map(|(id, _, _)| id.clone()).collect();
        crate::metrics::AFFECTED_ACCOUNTS_COUNT
            .with_label_values(&[
                &shard_id.to_string(),
                &operation_name,
                &account_ids.len().to_string(),
            ])
            .inc();
        let partition_map = self.partition_map(&shard_id, &pool, &account_ids).await?;

        // Group inserts per partition
        let mut inserts_per_partition: HashMap<
            i32,
            Vec<(String, Vec<u8>, bigdecimal::BigDecimal)>,
        > = HashMap::new();
        for (account_id, data_value, block_height) in inserts {
            if let Some(&partition) = partition_map.get(&account_id) {
                inserts_per_partition.entry(partition).or_default().push((
                    account_id,
                    data_value,
                    block_height,
                ));
            } else {
                tracing::warn!("Partition not found for account_id: {}", account_id);
            }
        }
        crate::metrics::PARTITIONS_TOUCHED_COUNT
            .with_label_values(&[
                &shard_id.to_string(),
                &operation_name,
                &inserts_per_partition.len().to_string(),
            ])
            .inc();

        let semaphore =
            std::sync::Arc::new(tokio::sync::Semaphore::new(super::MAX_CONCURRENT_QUERIES));
        let mut tasks = Vec::new();

        for (partition_id, rows) in inserts_per_partition {
            let pool = pool.clone();
            let semaphore = semaphore.clone();
            let table_name = format!("{}_{}", table_prefix, partition_id);
            let operation_name = operation_name.clone();

            let task = tokio::spawn(async move {
                let start = Instant::now();
                let _permit = semaphore.acquire_owned().await.unwrap();
                // Build batch INSERT for keyless tables (no data_key column)
                let mut qb = sqlx::QueryBuilder::new(format!(
                    "INSERT INTO {} (account_id, data_value, block_height_from, block_height_to) ",
                    table_name,
                ));

                qb.push_values(
                    rows.iter(),
                    |mut row, (account_id, data_value, block_height)| {
                        row.push_bind(account_id)
                            .push_bind(data_value)
                            .push_bind(block_height)
                            .push_bind(None::<Option<bigdecimal::BigDecimal>>); // block_height_to starts as NULL
                    },
                );

                qb.push(" ON CONFLICT DO NOTHING");

                let result = qb.build().execute(&pool).await.map_err(anyhow::Error::from);

                crate::metrics::SHARD_DATABASE_WRITE_ELAPSED_TIME
                    .with_label_values(&[
                        &shard_id.to_string(),
                        &operation_name,
                        &start.elapsed().as_millis().to_string(),
                    ])
                    .inc();
                tracing::debug!(
                    target: "database::postgres::state_indexer",
                    "Insert done operation={} partition={} elapsed={:?} rows={}",
                    operation_name,
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

    /// Helper function to record metrics consistently across all database operations
    ///
    /// Increments the SHARD_DATABASE_WRITE_QUERIES metric with standardized labels
    /// for monitoring and observability of database write patterns.
    pub(crate) fn record_shard_write_metric(
        &self,
        shard_id: near_primitives::types::ShardId,
        operation: &str,
        table: &str,
    ) {
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[&shard_id.to_string(), operation, table])
            .inc();
    }

    /// Helper function to get database pool with consistent error handling
    ///
    /// Centralizes the pool retrieval logic and provides meaningful error messages
    /// when a shard's database connection is not available.
    pub(crate) fn get_shard_pool(
        &self,
        shard_id: near_primitives::types::ShardId,
    ) -> anyhow::Result<&sqlx::PgPool> {
        self.shards_pool
            .get(&shard_id)
            .ok_or_else(|| anyhow::anyhow!("Database connection for Shard_{} not found", shard_id))
    }

    /// Compute partition assignments for account_ids using PostgreSQL's hashtext() function
    ///
    /// This function is critical for maintaining consistency with the database partitioning scheme.
    /// It uses the same hash function (hashtext) and modulo operation that PostgreSQL uses
    /// for automatic partition routing, ensuring our manual partition targeting matches
    /// the database's internal partition selection.
    ///
    /// The computation is done in PostgreSQL rather than Rust to guarantee identical
    /// hash results regardless of client-side hash implementations or endianness differences.
    pub(crate) async fn partition_map(
        &self,
        shard_id: &near_primitives::types::ShardId,
        pool: &sqlx::PgPool,
        account_ids: &Vec<String>,
    ) -> anyhow::Result<HashMap<String, i32>> {
        let now = std::time::Instant::now();

        // Execute partition calculation in PostgreSQL to ensure consistency
        // This MUST use the same hash function and modulo as the partitioned table definitions
        let partition_rows = sqlx::query(
            "SELECT account_id, mod(hashtext(account_id), $2)::int AS partition
             FROM unnest($1::text[]) AS account_id",
        )
        .bind(account_ids)
        .bind(super::PARTITIONS)
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
        crate::metrics::PARTITION_MAP_TIME_ELAPSED
            .with_label_values(&[
                &shard_id.to_string(),
                &now.elapsed().as_millis().to_string(),
            ])
            .inc();
        tracing::debug!(
            target: "database::postgres::state_indexer",
            "Partition map computed in {:?} for {} accounts",
            now.elapsed(),
            account_ids.len()
        );
        Ok(partition_map)
    }
}
