use std::path::PathBuf;
mod configs;

pub use crate::configs::database::DatabaseConfig;
pub use crate::configs::general::ChainId;
pub use crate::configs::Config;

pub async fn read_configuration() -> anyhow::Result<Config> {
    let path_root = project_root::get_project_root()?;
    load_env(path_root.clone()).await?;
    read_toml_file(path_root).await
}

async fn load_env(mut path_root: PathBuf) -> anyhow::Result<()> {
    path_root.push(".env");
    if path_root.exists() {
        dotenv::from_path(path_root.as_path()).ok();
    } else {
        dotenv::dotenv().ok();
    }
    Ok(())
}

async fn read_toml_file(mut path_root: PathBuf) -> anyhow::Result<Config> {
    path_root.push("config.toml");
    match std::fs::read_to_string(path_root.as_path()) {
        Ok(content) => match toml::from_str::<Config>(&content) {
            Ok(config) => Ok(config),
            Err(err) => {
                anyhow::bail!(
                    "Unable to load data from: {:?}.\n Error: {}",
                    path_root.to_str(),
                    err
                );
            }
        },
        Err(err) => {
            anyhow::bail!(
                "Could not read file: {:?}.\n Error: {}",
                path_root.to_str(),
                err
            );
        }
    }
}
