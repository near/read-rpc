use near_lake_framework::near_indexer_primitives::near_primitives;
use serde_derive::Deserialize;

use crate::configs::deserialize_optional_data_or_env;

#[derive(Debug, Clone)]
pub struct LakeConfig {
    pub num_threads: Option<u64>,
}

impl LakeConfig {
    pub async fn lake_config(
        &self,
        start_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<near_lake_framework::FastNearConfig> {
        let config_builder = near_lake_framework::FastNearConfigBuilder::default();
        Ok(config_builder
            .mainnet()
            .start_block_height(start_block_height)
            .build()
            .expect("Failed to build LakeConfig"))
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonLakeConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub num_threads: Option<u64>,
}

impl From<CommonLakeConfig> for LakeConfig {
    fn from(common_config: CommonLakeConfig) -> Self {
        Self {
            num_threads: common_config.num_threads,
        }
    }
}
