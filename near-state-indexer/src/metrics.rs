use near_o11y::metrics::*;
use once_cell::sync::Lazy;

pub(crate) use logic_state_indexer::metrics::*;

// This metric is present in the near_o11y crate but it's not public
// so we can't use it directly. We have to redefine it here.
pub static NODE_BUILD_INFO: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_lake_build_info",
        "Metric whose labels indicate node’s version; see \
             <https://www.robustperception.io/exposing-the-software-version-to-prometheus>.",
        &["release", "build", "rustc_version"],
    )
    .unwrap()
});
