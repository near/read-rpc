use serde_derive::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub state_indexer: StateIndexerConfig,
}

#[derive(Deserialize)]
pub struct StateIndexerConfig {
    pub accounts: Vec<String>,
    pub changes: Vec<String>,
}

async fn read_toml_file() -> anyhow::Result<Config> {
    let filename = "config.toml";

    let contents = match std::fs::read_to_string(filename) {
        Ok(content) => content,
        Err(err) => {
            anyhow::bail!("Could not read file: {}.\n Error: {}", filename, err);
        }
    };

    let config: Config = match toml::from_str::<Config>(&contents) {
        Ok(config) => config,
        Err(err) => {
            anyhow::bail!("Unable to load data from: {}.\n Error: {}", filename, err);
        }
    };

    Ok(config)
}

pub async fn read_configuration() -> anyhow::Result<Config> {
    let config = read_toml_file().await?;

    Ok(config)
}
