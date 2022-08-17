pub async fn prepare_s3_client(
    access_key_id: &str,
    secret_access_key: &str,
    region: String,
) -> aws_sdk_s3::Client {
    let credentials = aws_types::Credentials::new(access_key_id, secret_access_key, None, None, "");
    let s3_config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::Region::new(region))
        .build();
    aws_sdk_s3::Client::from_conf(s3_config)
}

pub async fn prepare_db_client(database_url: &str) -> sqlx::PgPool {
    sqlx::PgPool::connect(database_url).await.unwrap()
}

pub async fn prepare_redis_client(redis_url: &str) -> redis::aio::ConnectionManager {
    redis::Client::open(redis_url)
        .expect("can create Redis client")
        .get_tokio_connection_manager()
        .await
        .unwrap()
}
