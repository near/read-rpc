// Example:
//
// pub(crate) struct MyScyllaManager {
//     scylla_session: std::sync::Arc<scylla::Session>,
//     add_transaction: PreparedStatement,
// }
//
// impl ScyllaStorageManager for MyScyllaManager {
//     async fn prepare(scylla_db_session: std::sync::Arc<scylla::Session>) -> anyhow::Result<Self> {
//         Ok(Self {
//             scylla_session: scylla_db_session.clone(),
//             add_transaction: Self::prepare_query(
//                 &scylla_db_session,
//                 "INSERT INTO transactions_details
//                     (transaction_hash, block_height, account_id, transaction_details)
//                     VALUES(?, ?, ?, ?)",
//             )
//                 .await?,
//         })
//     }
// }
//
// If you need migrations describe the tables in the `create_tables`
//
//     async fn create_tables(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
//         scylla_db_session.query(
//             "CREATE TABLE IF NOT EXISTS transactions_details (
//                 transaction_hash varchar,
//                 block_height varint,
//                 account_id varchar,
//                 transaction_details BLOB,
//                 PRIMARY KEY ((transaction_hash, account_id), block_height)
//             ) WITH CLUSTERING ORDER BY (block_height DESC)
//             ",
//             &[],
//         ).await?;
//     Ok(())
//     }
// }
//
// // Implement prepered queries
// impl MyScyllaManager {
//     pub async fn add_transaction(
//         &self,
//         transaction: readnode_primitives::TransactionDetails,
//         block_height: u64,
//     ) -> anyhow::Result<()> {
//         let transaction_details = transaction
//             .try_to_vec()
//             .expect("Failed to borsh-serialize the Transaction");
//          Self::execute_prepared_query(
//             &self.scylla_session,
//             &self.add_transaction,
//             (
//                 transaction.transaction.hash.to_string(),
//                 num_bigint::BigInt::from(block_height),
//                 transaction.transaction.signer_id.to_string(),
//                 &transaction_details,
//             )
//         ).await?;
//         Ok(())
//     }
// }
//
// Usage:
// let scylla_db_client = MyScyllaManager::new(
//         scylla_url,
//         scylla_keyspace,
//         scylla_user,
//         scylla_password,
//         Some(keepalive_interval),
// ).await?,

use scylla::prepared_statement::PreparedStatement;
use scylla::retry_policy::{QueryInfo, RetryDecision};
use scylla::transport::errors::QueryError;

#[derive(Debug)]
pub struct CustomDBRetryPolicy {
    max_retry: u8,
    strict_mode: bool,
}

impl CustomDBRetryPolicy {
    pub fn new(max_retry: u8, strict_mode: bool) -> CustomDBRetryPolicy {
        CustomDBRetryPolicy {
            max_retry,
            strict_mode,
        }
    }
}

impl Default for CustomDBRetryPolicy {
    /// By default using `strict_mode`
    fn default() -> CustomDBRetryPolicy {
        CustomDBRetryPolicy::new(0, true)
    }
}

impl scylla::retry_policy::RetryPolicy for CustomDBRetryPolicy {
    /// Called for each new query, starts a session of deciding about retries
    fn new_session(&self) -> Box<dyn scylla::retry_policy::RetrySession> {
        Box::new(CustomRetrySession::new(self.max_retry, self.strict_mode))
    }

    /// Used to clone this RetryPolicy
    fn clone_boxed(&self) -> Box<dyn scylla::retry_policy::RetryPolicy> {
        Box::new(CustomDBRetryPolicy::new(self.max_retry, self.strict_mode))
    }
}

struct CustomRetrySession {
    max_retry: u8,
    max_retry_count: u8,
    strict_mode: bool,
}

impl CustomRetrySession {
    pub fn new(max_retry: u8, strict_mode: bool) -> CustomRetrySession {
        CustomRetrySession {
            max_retry,
            max_retry_count: 0,
            strict_mode,
        }
    }

    /// Decide if we should retry the query
    pub fn should_retry(&mut self) -> bool {
        // If `strict_mode` always retry
        if self.strict_mode {
            return true;
        };
        // If `max_retry_count` is more than `max_retry` don't retry
        if self.max_retry_count > self.max_retry {
            return false;
        };
        // If `max_retry_count` is less than `max_retry` increment `max_retry_count` and retry
        self.max_retry_count += 1;
        true
    }
}

impl Default for CustomRetrySession {
    /// By default using `strict_mode`
    fn default() -> CustomRetrySession {
        CustomRetrySession::new(0, true)
    }
}

impl scylla::retry_policy::RetrySession for CustomRetrySession {
    /// Called after the query failed - decide what to do next
    fn decide_should_retry(&mut self, query_info: QueryInfo) -> RetryDecision {
        if let scylla::frame::types::LegacyConsistency::Serial(_) = query_info.consistency {
            return RetryDecision::DontRetry;
        };
        tracing::warn!("ScyllaDB QueryError: {:?}", query_info.error);
        match query_info.error {
            QueryError::IoError(_)
            | QueryError::DbError(scylla::transport::errors::DbError::Overloaded, _)
            | QueryError::DbError(scylla::transport::errors::DbError::ServerError, _)
            | QueryError::DbError(scylla::transport::errors::DbError::TruncateError, _) => {
                tracing::warn!(
                    "Basic errors - there are some problems on this node.
                    Retry on a different one if possible"
                );
                RetryDecision::RetryNextNode(None)
            }
            QueryError::DbError(scylla::transport::errors::DbError::Unavailable { .. }, _) => {
                tracing::warn!(
                    "Unavailable - the current node believes that not enough nodes
                    are alive to satisfy specified consistency requirements.
                    Maybe this node has network problems - try a different one.
                    Perform at most one retry - it's unlikely that two nodes
                    have network problems at the same time"
                );
                if self.should_retry() {
                    tracing::warn!("Retrying on a different node");
                    RetryDecision::RetryNextNode(None)
                } else {
                    tracing::warn!("Not retrying");
                    RetryDecision::DontRetry
                }
            }
            QueryError::DbError(
                scylla::transport::errors::DbError::ReadTimeout {
                    received,
                    required,
                    data_present,
                    ..
                },
                _,
            ) => {
                tracing::warn!(
                    "ReadTimeout - coordinator didn't receive enough replies in time.
                    Retry at most once and only if there were actually enough replies
                    to satisfy consistency but they were all just checksums (data_present == false).
                    This happens when the coordinator picked replicas that were overloaded/dying.
                    Retried request should have some useful response because the node will detect
                    that these replicas are dead."
                );
                if self.should_retry() && received >= required && !*data_present {
                    tracing::warn!("Retrying on the same node");
                    RetryDecision::RetrySameNode(None)
                } else {
                    tracing::warn!("Not retrying");
                    RetryDecision::DontRetry
                }
            }
            QueryError::DbError(scylla::transport::errors::DbError::WriteTimeout { .. }, _) => {
                tracing::warn!(
                    "WriteTimeout - coordinator didn't receive enough replies in time.
                    Retry at most once and only for BatchLog write.
                    Coordinator probably didn't detect the nodes as dead.
                    By the time we retry they should be detected as dead."
                );
                if self.should_retry() {
                    tracing::warn!("Retrying on the next node");
                    RetryDecision::RetryNextNode(None)
                } else {
                    tracing::warn!("Not retrying");
                    RetryDecision::DontRetry
                }
            }
            QueryError::DbError(scylla::transport::errors::DbError::IsBootstrapping, _) => {
                tracing::warn!("The node is still bootstrapping it can't execute the query, we should try another one");
                RetryDecision::RetryNextNode(None)
            }
            QueryError::UnableToAllocStreamId => {
                tracing::warn!("Connection to the contacted node is overloaded, try another one");
                RetryDecision::RetryNextNode(None)
            }
            _ => {
                tracing::warn!("In all other cases propagate the error to the user. Don't retry");
                RetryDecision::DontRetry
            }
        }
    }

    /// Reset `max_retry_count` before using for a new query
    fn reset(&mut self) {
        self.max_retry_count = 0;
    }
}

#[async_trait::async_trait]
pub trait ScyllaStorageManager {
    async fn new_from_session(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>> {
        tracing::info!("Running migrations into the scylla database...");
        Self::migrate(&scylla_db_session).await?;
        Self::prepare(scylla_db_session).await
    }

    async fn new(
        scylla_url: &str,
        scylla_user: Option<&str>,
        scylla_password: Option<&str>,
        scylla_preferred_dc: Option<&str>,
        keepalive_interval: Option<u64>,
        max_retry: u8,
        strict_mode: bool,
    ) -> anyhow::Result<Box<Self>> {
        let scylla_db_session = std::sync::Arc::new(
            Self::get_scylladb_session(
                scylla_url,
                scylla_user,
                scylla_password,
                scylla_preferred_dc,
                keepalive_interval,
                max_retry,
                strict_mode,
            )
            .await?,
        );
        Self::new_from_session(scylla_db_session).await
    }

    async fn migrate(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        Self::create_keyspace(scylla_db_session).await?;
        Ok(Self::create_tables(scylla_db_session).await?)
    }

    // Create tables
    // Example:
    //         scylla_db_session.use_keyspace(`scylla_keyspace`, false).await?;
    //         scylla_db_session.query(
    //             "CREATE TABLE IF NOT EXISTS transactions_details (
    //                 transaction_hash varchar,
    //                 block_height varint,
    //                 account_id varchar,
    //                 transaction_details BLOB,
    //                 PRIMARY KEY ((transaction_hash, account_id), block_height)
    //             ) WITH CLUSTERING ORDER BY (block_height DESC)
    //             ",
    //             &[],
    //         ).await?;
    //     Ok(())
    async fn create_tables(_scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        tracing::info!("Please describe the tables in the `create_tables`, if needed.");
        Ok(())
    }

    // Create keyspace
    // Example:
    //      scylla_db_session.query(
    //          "CREATE KEYSPACE IF NOT EXISTS `scylla_keyspace` WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    //          &[]
    //      ).await?;
    // Ok(())
    async fn create_keyspace(_scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        tracing::info!("Please create  keyspace in the `create_keyspace`, if needed.");
        Ok(())
    }

    async fn prepare_query(
        scylla_db_session: &std::sync::Arc<scylla::Session>,
        mut query: scylla::statement::query::Query,
        consistency: Option<scylla::frame::types::Consistency>,
    ) -> anyhow::Result<PreparedStatement> {
        if let Some(consistency) = consistency {
            query.set_consistency(consistency);
        } else {
            // set `Consistency::LocalQuorum` as a default consistency
            query.set_consistency(scylla::frame::types::Consistency::LocalQuorum);
        }

        #[cfg(not(feature = "scylla_db_tracing"))]
        {
            let prepared = scylla_db_session.prepare(query).await?;
            Ok(prepared)
        }

        #[cfg(feature = "scylla_db_tracing")]
        {
            let mut prepared = scylla_db_session.prepare(query).await?;
            prepared.set_tracing(true);
            Ok(prepared)
        }
    }

    /// Wrapper to prepare read queries
    /// Just a simpler way to prepare a query with `Consistency::LocalQuorum`
    /// we use it as a default consistency for read queries
    async fn prepare_read_query(
        scylla_db_session: &std::sync::Arc<scylla::Session>,
        query_text: &str,
    ) -> anyhow::Result<PreparedStatement> {
        let query = scylla::statement::query::Query::new(query_text);
        Self::prepare_query(
            scylla_db_session,
            query,
            Some(scylla::frame::types::Consistency::LocalQuorum),
        )
        .await
    }

    /// Wrapper to prepare write queries
    /// Just a simpler way to prepare a query with `Consistency::LocalQuorum`
    /// we use it as a default consistency for write queries
    async fn prepare_write_query(
        scylla_db_session: &std::sync::Arc<scylla::Session>,
        query_text: &str,
    ) -> anyhow::Result<PreparedStatement> {
        let query = scylla::statement::query::Query::new(query_text);
        Self::prepare_query(
            scylla_db_session,
            query,
            Some(scylla::frame::types::Consistency::LocalQuorum),
        )
        .await
    }

    async fn get_scylladb_session(
        scylla_url: &str,
        scylla_user: Option<&str>,
        scylla_password: Option<&str>,
        scylla_preferred_dc: Option<&str>,
        keepalive_interval: Option<u64>,
        max_retry: u8,
        strict_mode: bool,
    ) -> anyhow::Result<scylla::Session> {
        let mut load_balancing_policy_builder =
            scylla::transport::load_balancing::DefaultPolicy::builder();

        if let Some(scylla_preferred_dc) = scylla_preferred_dc {
            load_balancing_policy_builder =
                load_balancing_policy_builder.prefer_datacenter(scylla_preferred_dc.to_string());
        }

        let scylla_execution_profile_handle = scylla::transport::ExecutionProfile::builder()
            .retry_policy(Box::new(CustomDBRetryPolicy::new(max_retry, strict_mode)))
            .load_balancing_policy(load_balancing_policy_builder.build())
            .build()
            .into_handle();

        let mut session: scylla::SessionBuilder = scylla::SessionBuilder::new()
            .known_node(scylla_url)
            .default_execution_profile_handle(scylla_execution_profile_handle);

        if let Some(keepalive) = keepalive_interval {
            session = session.keepalive_interval(std::time::Duration::from_secs(keepalive));
        }
        if let Some(user) = scylla_user {
            if let Some(password) = scylla_password {
                session = session.user(user, password);
            }
        }

        Ok(session.build().await?)
    }

    // Prepare manager and queries
    async fn prepare(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>>;
    // Example:
    // {
    //     Ok(Self {
    //         scylla_session: scylla_db_session.clone(),
    //         add_transaction: Self::prepare_query(
    //             &scylla_db_session,
    //             "INSERT INTO transactions_details
    //                 (transaction_hash, block_height, account_id, transaction_details)
    //                 VALUES(?, ?, ?, ?)",
    //         )
    //             .await?,
    //     })
    // }

    async fn execute_prepared_query(
        scylla_session: &scylla::Session,
        query: &PreparedStatement,
        values: impl scylla::frame::value::ValueList + std::marker::Send,
    ) -> anyhow::Result<scylla::QueryResult> {
        let result = scylla_session.execute(query, values).await?;

        #[cfg(feature = "scylla_db_tracing")]
        if query.get_tracing() {
            let tracing_id: Option<uuid::Uuid> = result.tracing_id;
            if let Some(id) = tracing_id {
                // Query tracing info from system_traces.sessions and system_traces.events
                let tracing_info: scylla::tracing::TracingInfo =
                    scylla_session.get_tracing_info(&id).await?;
                Self::log_tracing_info(tracing_info).await;
            }
        }

        Ok(result)
    }

    // For now we show all scylla tracing_info datails
    // In future we can left only needed
    #[cfg(feature = "scylla_db_tracing")]
    async fn log_tracing_info(tracing_info: scylla::tracing::TracingInfo) {
        let mut tracing_info_table = prettytable::table!(["Parameter", "Info"]);

        if let Some(client) = tracing_info.client {
            tracing_info_table.add_row(prettytable::row!["Client", client]);
        }

        if let Some(cmd) = tracing_info.command {
            tracing_info_table.add_row(prettytable::row!["Command", cmd]);
        }

        if let Some(coord) = tracing_info.coordinator {
            tracing_info_table.add_row(prettytable::row!["Coordinator", coord]);
        }

        if let Some(drn) = tracing_info.duration {
            tracing_info_table.add_row(prettytable::row!["Duration", drn]);
        }

        if let Some(params) = tracing_info.parameters {
            let mut params_table = prettytable::table!(["Param", "Value"]);
            for (param, value) in params.iter() {
                params_table.add_row(prettytable::row![param, value]);
            }
            tracing_info_table.add_row(prettytable::row!["Parameters", params_table]);
        }

        if let Some(req) = tracing_info.request {
            tracing_info_table.add_row(prettytable::row!["Request", req]);
        }

        if let Some(start_at) = tracing_info.started_at {
            tracing_info_table.add_row(prettytable::row!["Started at", start_at]);
        }

        let mut events_table =
            prettytable::table!(["Activity", "Thread", "Source", "Source elapsed"]);
        for event in tracing_info.events.iter() {
            let mut event_row = vec![];
            let none_val = prettytable::Cell::new(&String::from("Unknown"));
            match &event.activity {
                Some(act) => event_row.push(prettytable::Cell::new(act)),
                None => event_row.push(none_val.clone()),
            }

            match &event.thread {
                Some(thr) => event_row.push(prettytable::Cell::new(thr)),
                None => event_row.push(none_val.clone()),
            }

            match &event.source {
                Some(src) => event_row.push(prettytable::Cell::new(&src.to_string())),
                None => event_row.push(none_val.clone()),
            }

            match &event.source_elapsed {
                Some(src_el) => event_row.push(prettytable::Cell::new(&src_el.to_string())),
                None => event_row.push(none_val),
            }
            events_table.add_row(prettytable::Row::new(event_row));
        }
        tracing_info_table.add_row(prettytable::row!["Events", events_table]);

        tracing_info_table.printstd();
    }
}
