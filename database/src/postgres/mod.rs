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

/// This struct is used to manage the connection to a specific shard database.
/// It contains the shard_id, data_range_id, and a reference to the database pool.
/// `shard_id` is used to identify the shard database.
/// `data_range_id` is used to identify the range of data in the shard database.
/// `pool` is a reference to the database pool that is used to connect to the shard database.
///
/// `data_range_id` is calculated based on the block height.
/// Since we split the database into ranges of 500,000 blocks,
/// we can use the block height to determine the range.
/// For database, we store ranges in different tables and use `data_range_id` as the suffix.
/// `state_changes_data_{data_range_id}`
///
/// Example:
/// block_height 73_908_345, the `data_range_id` will be 735 -> `state_changes_data_735`
/// block_height 130_501_000, the `data_range_id` will be 1305 -> `state_changes_data_1305`
///
/// How to get `data_range_id` from block_height
/// see more details in the function `configuration::utils::get_data_range_id`.
pub struct ShardIdPool<'a> {
    shard_id: near_primitives::types::ShardId,
    data_range_id: u64,
    pool: &'a sqlx::Pool<sqlx::Postgres>,
}

#[derive(Clone)]
pub struct PostgresDBManager {
    shard_layout: near_primitives::shard_layout::ShardLayout,
    shards_pool:
        std::collections::HashMap<near_primitives::types::ShardId, sqlx::Pool<sqlx::Postgres>>,
    meta_db_pool: sqlx::Pool<sqlx::Postgres>,
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
            data_range_id: configuration::utils::get_data_range_id(block_height).await?,
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
        }))
    }
}
