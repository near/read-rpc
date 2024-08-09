use serde_derive::Deserialize;

use crate::configs::{deserialize_optional_data_or_env, required_value_or_panic};

#[derive(Debug, Clone)]
pub struct TxDetailsStorageConfig {
    pub bucket_name: String,
}

impl TxDetailsStorageConfig {
    pub async fn gcs_config(&self) -> google_cloud_storage::client::ClientConfig {
        let default_config = google_cloud_storage::client::ClientConfig::default();
        if std::env::var("STORAGE_EMULATOR_HOST").is_ok() {
            // if we are running local, and we are using gcs emulator
            // we need to use anonymous access with the emulator host
            let mut config = default_config.anonymous();
            config.storage_endpoint = std::env::var("STORAGE_EMULATOR_HOST").unwrap();
            config
        } else {
            default_config.with_auth().await.unwrap()
        }
    }

    pub async fn storage_client(&self) -> google_cloud_storage::client::Client {
        let gcs_config = self.gcs_config().await;
        google_cloud_storage::client::Client::new(gcs_config)
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonTxDetailStorageConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub bucket_name: Option<String>,
}

impl From<CommonTxDetailStorageConfig> for TxDetailsStorageConfig {
    fn from(common_config: CommonTxDetailStorageConfig) -> Self {
        Self {
            bucket_name: required_value_or_panic("bucket_name", common_config.bucket_name),
        }
    }
}
