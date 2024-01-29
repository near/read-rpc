use near_lake_framework::near_indexer_primitives::near_primitives;
use serde_derive::Deserialize;

use crate::configs::deserialize_data_or_env;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct LakeConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub aws_access_key_id: String,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub aws_secret_access_key: String,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub aws_default_region: String,
    #[serde(deserialize_with = "deserialize_data_or_env")]
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
}
