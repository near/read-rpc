mod rpc_server;
mod state_indexer;
mod tx_indexer;

static META_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/meta_db");
static SHARD_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/shard_db");

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug)]
struct PageState {
    pub page_size: i64,
    pub offset: i64,
}

impl PageState {
    fn new(page_size: i64) -> Self {
        Self {
            page_size,
            offset: 0,
        }
    }

    fn next_page(&self) -> Self {
        Self {
            page_size: self.page_size,
            offset: self.offset + self.page_size,
        }
    }
}

pub struct ShardIdPool<'a> {
    shard_id: near_primitives::types::ShardId,
    pool: &'a sqlx::Pool<sqlx::Postgres>,
}

pub struct PostgresDBManager {
    shard_layout: near_primitives::shard_layout::ShardLayout,
    shards_pool:
        std::collections::HashMap<near_primitives::types::ShardId, sqlx::Pool<sqlx::Postgres>>,
    meta_db_pool: sqlx::Pool<sqlx::Postgres>,
}

impl PostgresDBManager {
    async fn create_meta_db_pool(database_url: &str, read_only: bool) -> anyhow::Result<sqlx::Pool<sqlx::Postgres>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
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
    ) -> anyhow::Result<sqlx::Pool<sqlx::Postgres>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
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
    ) -> anyhow::Result<ShardIdPool> {
        let shard_id =
            near_primitives::shard_layout::account_id_to_shard_id(account_id, &self.shard_layout);
        Ok(ShardIdPool {
            shard_id,
            pool: self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
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
    async fn new(
        config: &configuration::DatabaseConfig,
        shard_layout: near_primitives::shard_layout::ShardLayout,
    ) -> anyhow::Result<Box<Self>> {
        let meta_db_pool = Self::create_meta_db_pool(&config.database_url, config.read_only).await?;
        let mut shards_pool = std::collections::HashMap::new();
        for shard_id in shard_layout.shard_ids() {
            let database_url = config
                .shards_config
                .get(&shard_id)
                .unwrap_or_else(|| panic!("Shard_{shard_id} - database config not found"));
            let pool = Self::create_shard_db_pool(database_url, config.read_only).await?;
            shards_pool.insert(shard_id, pool);
        }
        Ok(Box::new(Self {
            shard_layout,
            shards_pool,
            meta_db_pool,
        }))
    }
}
