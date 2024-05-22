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

// TODO: Implement metrics for postgres database
// https://github.com/near/read-rpc/issues/260
lazy_static! {
    pub(crate) static ref DATABASE_QUERIES: IntCounterVec = register_int_counter_vec(
        "database_queries_counter",
        "Total number of database queries by method_name and table_name",
        &["method_name", "table_name"]
    )
    .unwrap();
}
