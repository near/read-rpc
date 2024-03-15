use near_lake_framework::near_indexer_primitives::near_primitives;
use serde_derive::Deserialize;

use crate::configs::{deserialize_optional_data_or_env, required_value_or_panic};

#[derive(Debug, Clone)]
pub struct LakeConfig {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub aws_default_region: String,
    pub aws_bucket_name: String,
}

impl LakeConfig {
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
            .region(aws_types::region::Region::new(
                self.aws_default_region.clone(),
            ))
            .build()
    }

    pub async fn lake_config(
        &self,
        start_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<near_lake_framework::LakeConfig> {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();
        Ok(config_builder
            .s3_config(self.s3_config().await)
            .s3_region_name(&self.aws_default_region)
            .s3_bucket_name(&self.aws_bucket_name)
            .start_block_height(start_block_height)
            .build()
            .expect("Failed to build LakeConfig"))
    }

    pub async fn lake_s3_client(&self) -> near_lake_framework::s3_fetchers::LakeS3Client {
        let s3_config = self.s3_config().await;
        near_lake_framework::s3_fetchers::LakeS3Client::new(aws_sdk_s3::Client::from_conf(
            s3_config,
        ))
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonLakeConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_access_key_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_default_region: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub aws_bucket_name: Option<String>,
}

impl From<CommonLakeConfig> for LakeConfig {
    fn from(common_config: CommonLakeConfig) -> Self {
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
        }
    }
}
