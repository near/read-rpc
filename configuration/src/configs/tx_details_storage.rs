use serde_derive::Deserialize;

use crate::configs::{deserialize_optional_data_or_env, required_value_or_panic};

#[derive(Debug, Clone)]
pub struct TxDetailsStorageConfig {
    pub bucket_name: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub endpoint: Option<String>,
}

impl TxDetailsStorageConfig {
    pub async fn s3_config(&self) -> aws_sdk_s3::Config {
        // Define custom region and endpoint
        let region = aws_sdk_s3::config::Region::new(self.region.clone());

        // Create custom credentials
        let credentials = aws_sdk_s3::config::Credentials::new(
            self.access_key.clone(),
            self.secret_key.clone(),
            None,
            None,
            "Static",
        );

        // Build AWS SDK config
        let mut s3_conf = aws_sdk_s3::Config::builder()
            .region(region)
            .credentials_provider(credentials);

        // Override S3 endpoint in case you want to use custom solution
        if let Some(s3_endpoint) = self.endpoint.clone() {
            s3_conf = s3_conf.endpoint_url(s3_endpoint.to_string());
        }

        s3_conf.build()
    }

    pub async fn s3_client(&self) -> aws_sdk_s3::Client {
        let s3_config = self.s3_config().await;
        aws_sdk_s3::Client::from_conf(s3_config)
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonTxDetailStorageConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub bucket_name: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub access_key: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub secret_key: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub region: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub endpoint: Option<String>,
}

impl From<CommonTxDetailStorageConfig> for TxDetailsStorageConfig {
    fn from(common_config: CommonTxDetailStorageConfig) -> Self {
        Self {
            bucket_name: required_value_or_panic("bucket_name", common_config.bucket_name),
            access_key: required_value_or_panic("access_key", common_config.access_key),
            secret_key: required_value_or_panic("secret_key", common_config.secret_key),
            region: required_value_or_panic("region", common_config.region),
            endpoint: common_config.endpoint,
        }
    }
}
