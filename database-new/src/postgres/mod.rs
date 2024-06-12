mod rpc_server;

static META_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/meta_db");
static SHARD_DB_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/shard_db");

pub struct PostgresDBManager {
    shards_pool: std::collections::HashMap<u64, sqlx::Pool<sqlx::Postgres>>,
    meta_db_pool: sqlx::Pool<sqlx::Postgres>,
}

impl PostgresDBManager {

    async fn create_meta_db_pool(
        database_url: &str,
    ) -> anyhow::Result<sqlx::Pool<sqlx::Postgres>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(database_url)
            .await?;
        Self::run_migrations(&META_DB_MIGRATOR, &pool).await?;
        Ok(pool)
    }

    async fn create_shards_db_pool(
        database_url: &str,
    ) -> anyhow::Result<std::collections::HashMap<u64, sqlx::Pool<sqlx::Postgres>>> {
        let mut shards_pool = std::collections::HashMap::new();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(database_url)
            .await?;
        Self::run_migrations(&SHARD_DB_MIGRATOR, &pool).await?;
        shards_pool.insert(0, pool);
        Ok(shards_pool)
    }

    async fn get_connection(
        &self,
        account_id: &near_primitives::types::AccountId,
        shard_layout: &near_primitives::shard_layout::ShardLayout,
    ) -> anyhow::Result<&sqlx::Pool<sqlx::Postgres>> {
        let shard_id =
            near_primitives::shard_layout::account_id_to_shard_id(account_id, shard_layout);
        self
            .shards_pool
            .get(&shard_id)
            .ok_or(anyhow::anyhow!("Shard not found"))
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
        let shards_pool = Self::create_shards_db_pool(
            &config.database_url,
        )
        .await?;
        let meta_db_pool = Self::create_meta_db_pool(
            &config.database_url,
        )
        .await?;
        Ok(Box::new(Self {
            shards_pool,
            meta_db_pool,
        }))
    }
}
