pub mod models;
pub mod rpc_server;
pub mod schema;
pub mod state_indexer;
pub mod tx_indexer;

pub type PgAsyncPool =
    diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>;
pub type PgAsyncConn =
    diesel_async::pooled_connection::deadpool::Object<diesel_async::AsyncPgConnection>;

#[async_trait::async_trait]
pub trait PostgresStorageManager {
    async fn create_pool(
        database_url: &str,
        database_user: Option<&str>,
        database_password: Option<&str>,
        database_name: Option<&str>,
    ) -> anyhow::Result<PgAsyncPool> {
        let connection_string = if database_url.starts_with("postgres://") {
            database_url.to_string()
        } else {
            format!(
                "postgres://{}:{}@{}/{}",
                database_user.unwrap(),
                database_password.unwrap(),
                database_url,
                database_name.unwrap()
            )
        };
        let config = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(connection_string);
        let pool = diesel_async::pooled_connection::deadpool::Pool::builder(config).build()?;
        Ok(pool)
    }

    async fn get_connection(pool: &PgAsyncPool) -> anyhow::Result<PgAsyncConn> {
        Ok(pool.get().await?)
    }
}
