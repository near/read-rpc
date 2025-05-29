use serde_derive::Deserialize;

use crate::configs::deserialize_optional_data_or_env;

#[derive(Debug, Clone)]
pub struct TxDetailsStorageConfig {
    pub scylla_url: Option<String>,
    pub scylla_user: Option<String>,
    pub scylla_password: Option<String>,
    pub scylla_preferred_dc: Option<String>,
    pub scylla_keepalive_interval: u64,
}

impl TxDetailsStorageConfig {
    async fn create_ssl_context(&self) -> anyhow::Result<openssl::ssl::SslContext> {
        // Initialize SslContextBuilder with TLS method
        let ca_cert_path = std::env::var("SCYLLA_CA_CERT")?;
        let client_cert_path = std::env::var("SCYLLA_CLIENT_CERT")?;
        let client_key_path = std::env::var("SCYLLA_CLIENT_KEY")?;

        let mut builder = openssl::ssl::SslContextBuilder::new(openssl::ssl::SslMethod::tls())?;
        builder.set_ca_file(ca_cert_path)?;
        builder.set_certificate_file(client_cert_path, openssl::ssl::SslFiletype::PEM)?;
        builder.set_private_key_file(client_key_path, openssl::ssl::SslFiletype::PEM)?;
        builder.check_private_key()?;
        Ok(builder.build())
    }

    pub async fn scylla_client(&self) -> Option<scylla::Session> {
        let scylla_url = match &self.scylla_url {
            Some(url) => url.clone(),
            None => {
                panic!("Scylla URL is not set but scylla_client() was called. This method should only be used when Scylla is configured.");
            }
        };

        let mut load_balancing_policy_builder =
            scylla::transport::load_balancing::DefaultPolicy::builder();

        if let Some(scylla_preferred_dc) = self.scylla_preferred_dc.clone() {
            load_balancing_policy_builder =
                load_balancing_policy_builder.prefer_datacenter(scylla_preferred_dc);
        }

        let scylla_execution_profile_handle = scylla::transport::ExecutionProfile::builder()
            .load_balancing_policy(load_balancing_policy_builder.build())
            .build()
            .into_handle();
        let ssl_context = if let Ok(ssl_context) = self.create_ssl_context().await {
            Some(ssl_context)
        } else {
            None
        };

        let mut session: scylla::SessionBuilder = scylla::SessionBuilder::new()
            .known_node(scylla_url.clone())
            .keepalive_interval(std::time::Duration::from_secs(
                self.scylla_keepalive_interval,
            ))
            .default_execution_profile_handle(scylla_execution_profile_handle)
            .ssl_context(ssl_context);

        if let Some(user) = self.scylla_user.clone() {
            if let Some(password) = self.scylla_password.clone() {
                session = session.user(user, password);
            }
        }
        Some(
            session
                .build()
                .await
                .expect("Failed to create scylla session"),
        )
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonTxDetailStorageConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub scylla_url: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub scylla_user: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub scylla_password: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub scylla_preferred_dc: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub scylla_keepalive_interval: Option<u64>,
}

impl CommonTxDetailStorageConfig {
    pub fn default_scylla_keepalive_interval() -> u64 {
        60
    }
}

impl From<CommonTxDetailStorageConfig> for TxDetailsStorageConfig {
    fn from(common_config: CommonTxDetailStorageConfig) -> Self {
        Self {
            scylla_url: common_config.scylla_url,
            scylla_user: common_config.scylla_user,
            scylla_password: common_config.scylla_password,
            scylla_preferred_dc: common_config.scylla_preferred_dc,
            scylla_keepalive_interval: common_config
                .scylla_keepalive_interval
                .unwrap_or_else(CommonTxDetailStorageConfig::default_scylla_keepalive_interval),
        }
    }
}
