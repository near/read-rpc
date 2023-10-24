use crate::ScyllaStorageManager;

// TODO: Move this into configuration parameters for scylladb. For now it is hardcoded

// /// ScyllaDB preferred DataCenter
// /// Accepts the DC name of the ScyllaDB to filter the connection to that DC only (preferrably).
// /// If you connect to multi-DC cluter, you might experience big latencies while working with the DB. This is due to the fact that ScyllaDB driver tries to connect to any of the nodes in the cluster disregarding of the location of the DC. This option allows to filter the connection to the DC you need. Example: "DC1" where DC1 is located in the same region as the application.
// #[clap(long, env)]
// pub scylla_preferred_dc: Option<String>,

// /// ScyllaDB keepalive interval
// #[clap(long, env, default_value = "60")]
// pub scylla_keepalive_interval: u64,

// /// Max retry count for ScyllaDB if `strict_mode` is `false`
// #[clap(long, default_value = "2", env)]
// pub max_retry: u8,

// /// Attempts to store data in the database should be infinite to ensure no data is missing.
// /// Disable it to perform a limited write attempts (`max_retry`)
// /// before skipping giving up and moving to the next piece of data
// #[clap(long, default_value = "false", env)]
// pub strict_mode: bool,

async fn prepare_scylla_db_manager(
    scylla_url: &str,
    scylla_user: Option<&str>,
    scylla_password: Option<&str>,
    scylla_preferred_dc: Option<&str>,
    scylla_keepalive_interval: Option<u64>,
    max_retry: u8,
    strict_mode: bool,
) -> anyhow::Result<impl crate::StateIndexerDbManager> {
    Ok(*crate::scylladb::state_indexer::ScyllaDBManager::new(
        scylla_url,
        scylla_user,
        scylla_password,
        scylla_preferred_dc,
        scylla_keepalive_interval,
        max_retry,
        strict_mode,
    )
    .await?)
}

pub async fn prepare_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
) -> anyhow::Result<impl crate::StateIndexerDbManager> {
    #[cfg(feature = "scylla_db")]
    prepare_scylla_db_manager(
        database_url,
        database_user,
        database_password,
        None,
        None,
        5,
        true,
    )
    .await
}
