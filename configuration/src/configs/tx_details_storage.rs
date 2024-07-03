use serde_derive::Deserialize;

use crate::configs::{deserialize_optional_data_or_env, required_value_or_panic};

const DEFAULT_GCP_ENDPOINT_URL: &str = "https://storage.googleapis.com";

#[derive(Debug, Clone)]
pub struct TxDetailsStorageConfig {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub aws_default_region: String,
    pub aws_bucket_name: String,
    pub aws_endpoint: Option<String>,
}

impl TxDetailsStorageConfig {
    pub async fn s3_config(&self) -> aws_sdk_s3::Config {
        let credentials = aws_credential_types::Credentials::new(
            &self.aws_access_key_id,
            &self.aws_secret_access_key,
            None,
            None,
            "",
        );
        aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .endpoint_url(
                self.aws_endpoint
                    .clone()
                    .unwrap_or_else(|| DEFAULT_GCP_ENDPOINT_URL.to_string()),
            )
            .region(aws_types::region::Region::new(
                self.aws_default_region.clone(),
            ))
            .build()
    }

    pub async fn storage_client(&self) -> aws_sdk_s3::Client {
        let s3_config = self.s3_config().await;
        aws_sdk_s3::Client::from_conf(s3_config)
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonTxDetailStorageConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_access_key_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_default_region: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_bucket_name: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_endpoint: Option<String>,
}

impl From<CommonTxDetailStorageConfig> for TxDetailsStorageConfig {
    fn from(common_config: CommonTxDetailStorageConfig) -> Self {
        Self {
            aws_access_key_id: required_value_or_panic(
                "aws_access_key_id",
                common_config.aws_access_key_id,
            ),
            aws_secret_access_key: required_value_or_panic(
                "aws_secret_access_key",
                common_config.aws_secret_access_key,
            ),
            aws_default_region: required_value_or_panic(
                "aws_default_region",
                common_config.aws_default_region,
            ),
            aws_bucket_name: required_value_or_panic(
                "aws_bucket_name",
                common_config.aws_bucket_name,
            ),
            aws_endpoint: common_config.aws_endpoint,
        }
    }
}
