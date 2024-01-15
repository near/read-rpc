use std::path::Path;
mod configs;

pub use crate::configs::database::DatabaseConfig;
pub use crate::configs::Config;

pub async fn read_configuration_from_file(path_file: &str) -> anyhow::Result<Config> {
    let path_file = Path::new(path_file);
    read_toml_file(path_file).await
}

pub async fn read_configuration() -> anyhow::Result<Config> {
    let mut path_root = project_root::get_project_root()?;
    path_root.push("config.toml");
    if path_root.exists() {
        read_toml_file(path_root.as_path()).await
    } else {
        Ok(Config::default())
    }
}

async fn read_toml_file(path_file: &Path) -> anyhow::Result<Config> {
    dotenv::dotenv().ok();
    match std::fs::read_to_string(path_file) {
        Ok(content) => match toml::from_str::<Config>(&content) {
            Ok(config) => Ok(config),
            Err(err) => {
                anyhow::bail!(
                    "Unable to load data from: {:?}.\n Error: {}",
                    path_file.to_str(),
                    err
                );
            }
        },
        Err(err) => {
            anyhow::bail!(
                "Could not read file: {:?}.\n Error: {}",
                path_file.to_str(),
                err
            );
        }
    }
}
