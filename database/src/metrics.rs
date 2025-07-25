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
    pub(crate) static ref SHARD_DATABASE_WRITE_QUERIES: IntCounterVec = register_int_counter_vec(
        "shard_database_write_queries_counter",
        "Total number of shard database write queries by shard_id, method_name and table_name",
        &["shard_id", "method_name", "table_name"]
    )
    .unwrap();
    pub(crate) static ref META_DATABASE_READ_QUERIES: IntCounterVec = register_int_counter_vec(
        "meta_database_read_queries_counter",
        "Total number of meta database read queries by method_name and table_name",
        &["method_name", "table_name"]
    )
    .unwrap();
    pub(crate) static ref META_DATABASE_WRITE_QUERIES: IntCounterVec = register_int_counter_vec(
        "meta_database_write_queries_counter",
        "Total number of meta database write queries by method_name and table_name",
        &["method_name", "table_name"]
    )
    .unwrap();

    pub(crate) static ref SHARD_DATABASE_WRITE_ELAPSED_TIME: IntCounterVec = register_int_counter_vec(
        "shard_database_write_elapsed_time",
        "Total elapsed time of shard database write query by shard_id, operation_name, time_elapsed",
        &["shard_id", "operation_name", "time_elapsed"]
    )
    .unwrap();

    pub(crate) static ref AFFECTED_ACCOUNTS_COUNT: IntCounterVec = register_int_counter_vec(
        "affected_accounts_count",
        "Total number of affected accounts by shard_id, operation_name and accounts_number",
        &["shard_id", "operation_name", "accounts_number"]
    )
    .unwrap();

    pub(crate) static ref PARTITIONS_TOUCHED_COUNT: IntCounterVec = register_int_counter_vec(
        "partitions_touched_count",
        "Total number of partitions touched by shard_id, operation_name and partitions_number",
        &["shard_id", "operation_name", "partitions_number"]
    )
    .unwrap();

    pub(crate) static ref PARTITION_MAP_TIME_ELAPSED: IntCounterVec = register_int_counter_vec(
        "partition_map_time_elapsed",
        "Total elapsed time of partition map by shard_id and time_elapsed",
        &["shard_id", "time_elapsed"]
    )
    .unwrap();
}
