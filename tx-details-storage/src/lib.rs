use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

pub struct ScyllaTxDetailsStorage {
    add_transaction: scylla::prepared_statement::PreparedStatement,
    get_transaction: scylla::prepared_statement::PreparedStatement,
    scylla_session: scylla::Session,
}

impl ScyllaTxDetailsStorage {
    pub async fn new(scylla_session: scylla::Session) -> anyhow::Result<Self> {
        Self::create_keyspace(&scylla_session).await?;
        Self::create_table(&scylla_session).await?;
        Ok(Self {
            add_transaction: Self::prepare_query(
                &scylla_session,
                "INSERT INTO tx_details.transactions
                (transaction_hash, transaction_details)
                VALUES(?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            get_transaction: Self::prepare_query(
                &scylla_session,
                "SELECT block_height, transaction_details FROM tx_details.transactions WHERE transaction_hash = ? LIMIT 1",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            scylla_session,
        })
    }

    async fn prepare_query(
        scylla_db_session: &scylla::Session,
        query_text: &str,
        consistency: scylla::frame::types::Consistency,
    ) -> anyhow::Result<scylla::prepared_statement::PreparedStatement> {
        let mut query = scylla::statement::query::Query::new(query_text);
        query.set_consistency(consistency);
        Ok(scylla_db_session.prepare(query).await?)
    }

    pub async fn create_keyspace(scylla_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged(
                "CREATE KEYSPACE IF NOT EXISTS tx_details
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }",
                &[],
            )
            .await?;
        Ok(())
    }

    pub async fn create_table(scylla_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS transactions (
                    transaction_hash varchar PRIMARY KEY,
                    transaction_details BLOB
                )",
                &[],
            )
            .await?;
        Ok(())
    }

    pub async fn store(&self, key: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.scylla_session
            .execute_unpaged(&self.add_transaction, (key, data))
            .await?;
        Ok(())
    }

    pub async fn retrieve(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let (data,) = self
            .scylla_session
            .execute_unpaged(&self.get_transaction, (key.to_string(),))
            .await?
            .into_rows_result()?
            .single_row::<(Vec<u8>,)>()?;
        Ok(data)
    }
}

pub struct TxDetailsStorage {
    client: google_cloud_storage::client::Client,
    bucket_name: String,
}

impl TxDetailsStorage {
    /// Create a new instance of the `TxDetailsStorage` struct.
    pub fn new(client: google_cloud_storage::client::Client, bucket_name: String) -> Self {
        Self {
            client,
            bucket_name,
        }
    }

    pub async fn store(&self, key: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.bucket_name.to_string(),
                    ..Default::default()
                },
                data,
                &UploadType::Simple(Media::new(key.to_string())),
            )
            .await?;
        Ok(())
    }

    pub async fn retrieve(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let data = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket_name.to_string(),
                    object: key.to_string(),
                    ..Default::default()
                },
                &Range::default(),
            )
            .await?;
        Ok(data)
    }
}
