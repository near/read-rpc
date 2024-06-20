mod rpc_server;
mod state_indexer;

static META_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/meta_db");
static SHARD_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/shard_db");

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
    async fn create_meta_db_pool(database_url: &str) -> anyhow::Result<sqlx::Pool<sqlx::Postgres>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(database_url)
            .await?;
        Self::run_migrations(&META_DB_MIGRATOR, &pool).await?;
        Ok(pool)
    }

    async fn create_shard_db_pool(
        database_url: &str,
    ) -> anyhow::Result<sqlx::Pool<sqlx::Postgres>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(database_url)
            .await?;
        Self::run_migrations(&SHARD_DB_MIGRATOR, &pool).await?;
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
            pool: self
                .shards_pool
                .get(&shard_id)
                .ok_or(anyhow::anyhow!("Shard not found"))?,
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
        let mut shards_pool = std::collections::HashMap::new();
        for (shard_id, database_url) in &config.shards_config {
            let pool = Self::create_shard_db_pool(database_url).await?;
            shards_pool.insert(*shard_id, pool);
        }
        let meta_db_pool = Self::create_meta_db_pool(&config.database_url).await?;
        Ok(Box::new(Self {
            shard_layout,
            shards_pool,
            meta_db_pool,
        }))
    }
}
