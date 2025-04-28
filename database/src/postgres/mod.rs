mod rpc_server;
mod state_indexer;
mod tx_indexer;

static META_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/meta_db");
static SHARD_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/shard_db");

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug)]
struct PageState {
    pub last_data_key: Option<String>,
    pub page_size: i64,
}

impl PageState {
    fn new(page_size: i64) -> Self {
        Self {
            last_data_key: None,
            page_size,
        }
    }

    fn next_page(&self, last_data_key: String) -> Self {
        Self {
            last_data_key: Some(last_data_key),
            page_size: self.page_size,
        }
    }
}

pub struct ShardIdPool<'a> {
    shard_id: near_primitives::types::ShardId,
    table_number: u64,
    pool: &'a sqlx::Pool<sqlx::Postgres>,
}

#[derive(Clone)]
pub struct PostgresDBManager {
    shard_layout: near_primitives::shard_layout::ShardLayout,
    shards_pool:
        std::collections::HashMap<near_primitives::types::ShardId, sqlx::Pool<sqlx::Postgres>>,
    meta_db_pool: sqlx::Pool<sqlx::Postgres>,
    earliest_available_block: Option<near_primitives::types::BlockHeight>,
}

impl PostgresDBManager {
    async fn create_meta_db_pool(
        database_url: &str,
        read_only: bool,
        max_connections: u32,
    ) -> anyhow::Result<sqlx::Pool<sqlx::Postgres>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        if !read_only {
            Self::run_migrations(&META_DB_MIGRATOR, &pool).await?;
        }
        Ok(pool)
    }

    async fn create_shard_db_pool(
        database_url: &str,
        read_only: bool,
        max_connections: u32,
    ) -> anyhow::Result<sqlx::Pool<sqlx::Postgres>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        if !read_only {
            Self::run_migrations(&SHARD_DB_MIGRATOR, &pool).await?;
        }
        Ok(pool)
    }

    async fn get_shard_connection(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: &near_primitives::types::BlockHeight,
    ) -> anyhow::Result<ShardIdPool> {
        let shard_id = self.shard_layout.account_id_to_shard_id(account_id);
        Ok(ShardIdPool {
            shard_id,
            table_number: self.get_table_number(block_height).await?,
            pool: self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?,
        })
    }
    
    async fn get_shard_connection_by_id(
        &self,
        shard_id: &near_primitives::types::ShardId,
    ) -> anyhow::Result<ShardIdPool> {
        Ok(ShardIdPool {
            shard_id: *shard_id,
            table_number: 0, // Table number is not used in this case
            pool: self.shards_pool.get(shard_id).ok_or(anyhow::anyhow!(
                "Database connection for shard_{} not found",
                shard_id
            ))?,
        })
    }

    /// # Explanation of the logic:
    ///
    /// 1. `block_height / 500000`
    ///
    ///     * This integer division truncates any remainder, effectively grouping numbers into bins of 500,000.
    ///
    ///     * Example: 73908345 / 500000 = 147 (as integer division discards the remainder).
    ///
    /// 2. `* 5`
    ///
    ///     * Since each bin represents a multiple of 500,000, multiplying by 5 scales the result to match the pattern you provided.
    ///
    /// # Example Calculations:
    /// Example 1: 73908345
    ///
    ///     1. 73908345 / 500000 = 147
    ///
    ///     2. 147 * 5 = 735
    ///
    ///     Output: 735
    ///
    /// Example 2: 130501000
    ///     
    ///     1. 130501000 / 500000 = 261
    ///
    ///     2. 261 * 5 = 1305
    ///
    ///     Output: 1305
    async fn get_table_number(
        &self,
        block_height: &near_primitives::types::BlockHeight,
    ) -> anyhow::Result<u64> {
        if let Some(earliest_available_block) = self.earliest_available_block {
            if *block_height < earliest_available_block {
                anyhow::bail!(
                    "Block {} has been garbage collected. The earliest available block is {}",
                    block_height,
                    earliest_available_block
                )
            } else {
                Ok((block_height / 500000) * 5)
            }
        } else {
            Ok((block_height / 500000) * 5)
        }
    }

    async fn run_migrations(
        migrator: &sqlx::migrate::Migrator,
        pool: &sqlx::Pool<sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        migrator.run(pool).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::BaseDbManager for PostgresDBManager {
    async fn new(config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>> {
        let meta_db_pool = Self::create_meta_db_pool(
            &config.database_url,
            config.read_only,
            config.max_connections,
        )
        .await?;
        let mut shards_pool = std::collections::HashMap::new();
        let shard_layout = config
            .shard_layout
            .clone()
            .unwrap_or_else(|| panic!("Shard layout is not provided in the configuration. Please check if you have genesis_config.json in the root of the project."));
        for shard_id in shard_layout.shard_ids() {
            let database_url = config
                .shards_config
                .get(&shard_id)
                .unwrap_or_else(|| panic!("Shard_{shard_id} - database config not found"));
            let pool =
                Self::create_shard_db_pool(database_url, config.read_only, config.max_connections)
                    .await?;
            shards_pool.insert(shard_id, pool);
        }
        Ok(Box::new(Self {
            shard_layout,
            shards_pool,
            meta_db_pool,
            // TODO: This should be set from the config and not hardcoded
            // Should be updated when garbage collection is run
            // or should be None for archive node_mode
            earliest_available_block: None,
        }))
    }
}
