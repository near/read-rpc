use prometheus::{IntCounterVec, Opts};

type Result<T, E> = std::result::Result<T, E>;

fn register_int_counter_vec(
    name: &str,
    help: &str,
    label_names: &[&str],
) -> Result<IntCounterVec, prometheus::Error> {
    let opts = Opts::new(name, help);
    let counter = IntCounterVec::new(opts, label_names)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

lazy_static! {
    pub(crate) static ref SHARD_DATABASE_READ_QUERIES: IntCounterVec = register_int_counter_vec(
        "shard_database_read_queries_counter",
        "Total number of shard database read queries by shard_id, method_name and table_name",
        &["shard_id", "method_name", "table_name"]
    )
    .unwrap();
    pub(crate) static ref META_DATABASE_READ_QUERIES: IntCounterVec = register_int_counter_vec(
        "meta_database_read_queries_counter",
        "Total number of meta database read queries by method_name and table_name",
        &["method_name", "table_name"]
    )
    .unwrap();
}
