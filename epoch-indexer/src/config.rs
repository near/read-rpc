use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[clap(
    version,
    author,
    about,
    setting(clap::AppSettings::DisableHelpSubcommand),
    setting(clap::AppSettings::PropagateVersion),
    setting(clap::AppSettings::NextLineHelp)
)]
pub struct Opts {
    // Indexer bucket name
    #[clap(long, env = "AWS_BUCKET_NAME")]
    pub s3_bucket_name: String,
    // AWS access key id
    #[clap(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key_id: String,
    // AWS secret access key
    #[clap(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_access_key: String,
    // AWS default region
    #[clap(long, env = "AWS_DEFAULT_REGION")]
    pub region: String,

    /// DB connection string
    #[clap(long, default_value = "127.0.0.1:9042", env)]
    pub database_url: String,
    /// DB user(login)
    #[clap(long, env)]
    pub database_user: Option<String>,
    /// DB password
    #[clap(long, env)]
    pub database_password: Option<String>,

    #[clap(subcommand)]
    pub chain_id: ChainId,

    /// ScyllaDB preferred DataCenter
    /// Accepts the DC name of the ScyllaDB to filter the connection to that DC only (preferrably).
    /// If you connect to multi-DC cluter, you might experience big latencies while working with the DB. This is due to the fact that ScyllaDB driver tries to connect to any of the nodes in the cluster disregarding of the location of the DC. This option allows to filter the connection to the DC you need. Example: "DC1" where DC1 is located in the same region as the application.
    #[cfg(feature = "scylla_db")]
    #[clap(long, env)]
    pub preferred_dc: Option<String>,

    /// Max retry count for ScyllaDB if `strict_mode` is `false`
    #[cfg(feature = "scylla_db")]
    #[clap(long, env, default_value_t = 5)]
    pub max_retry: u8,

    /// Attempts to store data in the database should be infinite to ensure no data is missing.
    /// Disable it to perform a limited write attempts (`max_retry`)
    /// before giving up, and moving to the next piece of data
    #[cfg(feature = "scylla_db")]
    #[clap(long, env, default_value_t = true)]
    pub strict_mode: bool,

    /// Postgres database name
    #[cfg(feature = "postgres_db")]
    #[clap(long, env)]
    pub database_name: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ChainId {
    Mainnet,
    Testnet,
}

impl Opts {
    pub async fn to_additional_database_options(&self) -> database::AdditionalDatabaseOptions {
        database::AdditionalDatabaseOptions {
            #[cfg(feature = "scylla_db")]
            preferred_dc: self.preferred_dc.clone(),
            #[cfg(feature = "scylla_db")]
            keepalive_interval: None,
            #[cfg(feature = "scylla_db")]
            max_retry: self.max_retry,
            #[cfg(feature = "scylla_db")]
            strict_mode: self.strict_mode,
            #[cfg(feature = "postgres_db")]
            database_name: self.database_name.clone(),
        }
    }

    pub fn rpc_url(&self) -> &str {
        match &self.chain_id {
            ChainId::Mainnet => "https://archival-rpc.mainnet.near.org",
            ChainId::Testnet => "https://archival-rpc.testnet.near.org",
        }
    }
    pub async fn to_s3_client(&self) -> near_lake_framework::s3_fetchers::LakeS3Client {
        let credentials = aws_credential_types::Credentials::new(
            &self.access_key_id,
            &self.secret_access_key,
            None,
            None,
            "",
        );
        let s3_config = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(aws_sdk_s3::Region::new(self.region.clone()))
            .build();

        near_lake_framework::s3_fetchers::LakeS3Client::new(aws_sdk_s3::Client::from_conf(
            s3_config,
        ))
    }
}
