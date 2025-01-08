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

    /// Store data in the S3 bucket.
    pub async fn store(&self, key: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let byte_stream = aws_sdk_s3::primitives::ByteStream::from(data);
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(byte_stream)
            .send()
            .await?;
        Ok(())
    }

    /// Retrieve data from the S3 bucket.
    pub async fn retrieve(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await?;
        let data = object.body.collect().await?.into_bytes().to_vec();
        Ok(data)
    }
}
