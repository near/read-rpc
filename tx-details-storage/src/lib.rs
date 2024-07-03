use anyhow::Context;
use aws_sdk_s3::primitives::ByteStream;

pub struct TxDetailsStorage {
    client: aws_sdk_s3::Client,
    bucket_name: String,
}

impl TxDetailsStorage {
    /// Create a new instance of the `TxDetailsStorage` struct.
    pub fn new(client: aws_sdk_s3::Client, bucket_name: String) -> Self {
        Self {
            client,
            bucket_name,
        }
    }

    pub async fn store(&self, key: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map(|_| ())
            .context("Failed to store the data")
    }

    pub async fn retrieve(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .client
            .get_object()
            .bucket(self.bucket_name.clone())
            .key(key)
            .send()
            .await
            .context("Failed to retrieve the data")?;

        let body = response
            .body
            .collect()
            .await
            .context("Failed to read the body")?;
        Ok(body.to_vec())
    }
}
