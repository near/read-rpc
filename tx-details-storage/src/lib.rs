use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

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
