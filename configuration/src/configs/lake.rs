use crate::configs::deserialize_optional_data_or_env;
use near_lake_framework::near_indexer_primitives::near_primitives;
use serde_derive::Deserialize;

#[derive(Debug, Clone)]
pub struct LakeConfig {
    pub num_threads: Option<u64>,
    pub lake_auth_token: Option<String>,
}

impl LakeConfig {
    pub async fn lake_config(
        &self,
        start_block_height: near_primitives::types::BlockHeight,
        chain_id: crate::ChainId,
    ) -> anyhow::Result<near_lake_framework::FastNearConfig> {
        let mut config_builder = near_lake_framework::FastNearConfigBuilder::default();
        match chain_id {
            crate::ChainId::Mainnet => config_builder = config_builder.mainnet(),
            // Testnet is the default chain for other chain_id
            _ => config_builder = config_builder.testnet(),
        };
        if let Some(num_threads) = self.num_threads {
            config_builder = config_builder.num_threads(num_threads);
        };
        Ok(config_builder
            .start_block_height(start_block_height)
            .build()?)
    }

    pub async fn lake_client(
        &self,
        chain_id: crate::ChainId,
    ) -> anyhow::Result<near_lake_framework::FastNearClient> {
        let fast_near_endpoint = match chain_id {
            crate::ChainId::Mainnet => String::from("https://mainnet.neardata.xyz"),
            // Testnet is the default chain for other chain_id
            _ => String::from("https://testnet.neardata.xyz"),
        };
        Ok(near_lake_framework::FastNearClient::new(
            fast_near_endpoint,
            self.lake_auth_token.clone(),
        ))
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonLakeConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub num_threads: Option<u64>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub lake_auth_token: Option<String>,
}

impl From<CommonLakeConfig> for LakeConfig {
    fn from(common_config: CommonLakeConfig) -> Self {
        Self {
            num_threads: common_config.num_threads,
            lake_auth_token: common_config.lake_auth_token,
        }
    }
}
