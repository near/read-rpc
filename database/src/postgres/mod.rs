pub mod rpc_server;
pub mod state_indexer;
pub mod tx_indexer;
mod models;

pub struct AdditionalDatabaseOptions {
    pub database_name: Option<String>,
}

#[async_trait::async_trait]
pub trait PostgresStorageManager {
    async fn create_pool(
        database_url: &str,
        database_user: Option<&str>,
        database_password: Option<&str>,
        database_options: AdditionalDatabaseOptions,
    ) -> anyhow::Result<
        diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    > {
        let connecting_url = if database_url.starts_with("postgres://") {
            database_url.to_string()
        } else {
            format!(
                "postgres://{}:{}@{}/{}",
                database_user.unwrap(),
                database_password.unwrap(),
                database_url,
                database_options.database_name.unwrap()
            )
        };
        let config = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(connecting_url);
        let pool = diesel_async::pooled_connection::deadpool::Pool::builder(config).build()?;
        Ok(pool)
    }

    async fn get_connection(
        pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    ) -> anyhow::Result<
        diesel_async::pooled_connection::deadpool::Object<diesel_async::AsyncPgConnection>,
    > {
        Ok(pool.get().await?)
    }
}