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
// and use apply_migrations = true
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
//         apply_migrations=false
// ).await?,

use scylla::prepared_statement::PreparedStatement;

#[async_trait::async_trait]
pub trait ScyllaStorageManager {
    async fn new(
        scylla_url: &str,
        scylla_keyspace: &str,
        scylla_user: Option<&str>,
        scylla_password: Option<&str>,
        apply_migrations: bool,
    ) -> anyhow::Result<Box<Self>> {
        let scylla_db_session = std::sync::Arc::new(
            Self::get_scylladb_session(scylla_url, scylla_user, scylla_password).await?,
        );
        if apply_migrations {
            tracing::info!("Running migrations into the scylla database...");
            Self::migrate(&scylla_db_session, scylla_keyspace).await?
        }
        scylla_db_session
            .use_keyspace(scylla_keyspace, false)
            .await?;
        Self::prepare(scylla_db_session).await
    }

    async fn migrate(
        scylla_db_session: &scylla::Session,
        scylla_keyspace: &str,
    ) -> anyhow::Result<()> {
        Self::create_keyspace(scylla_db_session, scylla_keyspace).await?;
        scylla_db_session
            .use_keyspace(scylla_keyspace, false)
            .await?;
        Ok(Self::create_tables(scylla_db_session).await?)
    }

    // Create tables
    // Example:
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
        anyhow::bail!("Please describe the tables in the `create_tables`")
    }

    // Create keyspace
    async fn create_keyspace(
        scylla_db_session: &scylla::Session,
        scylla_keyspace: &str,
    ) -> anyhow::Result<()> {
        let mut str_query = format!("CREATE KEYSPACE IF NOT EXISTS {scylla_keyspace} ");
        str_query
            .push_str("WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        scylla_db_session.query(str_query, &[]).await?;
        Ok(())
    }

    async fn prepare_query(
        scylla_db_session: &std::sync::Arc<scylla::Session>,
        query_text: &str,
    ) -> anyhow::Result<PreparedStatement> {
        let mut query = scylla::statement::query::Query::new(query_text);
        query.set_consistency(scylla::frame::types::Consistency::All);

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

    async fn get_scylladb_session(
        scylla_url: &str,
        scylla_user: Option<&str>,
        scylla_password: Option<&str>,
    ) -> anyhow::Result<scylla::Session> {
        let mut session: scylla::SessionBuilder =
            scylla::SessionBuilder::new().known_node(scylla_url);

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
                let tracing_info: scylla::tracing::TracingInfo = scylla_session.get_tracing_info(&id).await?;
                Self::log_tracing_info(tracing_info).await;
            }
        }

        Ok(result)
    }

    // For now we show all scylla tracing_info datails
    // In future we can left only needed
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

        let mut events_table = prettytable::table!(["Activity", "Thread", "Source", "Source elapsed"]);
        for event in tracing_info.events.iter() {
            let mut event_row = vec![];
            let none_val = prettytable::Cell::new(&String::from("Unknown"));
            match &event.activity {
                Some(act) => event_row.push(prettytable::Cell::new(act)),
                None => event_row.push(none_val.clone())
            }

            match &event.thread {
                Some(thr) => event_row.push(prettytable::Cell::new(thr)),
                None => event_row.push(none_val.clone())
            }

            match &event.source {
                Some(src) => event_row.push(prettytable::Cell::new(&src.to_string())),
                None => event_row.push(none_val.clone())
            }

            match &event.source_elapsed {
                Some(src_el) => event_row.push(prettytable::Cell::new(&src_el.to_string())),
                None => event_row.push(none_val)
            }
            events_table.add_row(prettytable::Row::new(event_row));
        }
        tracing_info_table.add_row(prettytable::row!["Events", events_table]);

        tracing_info_table.printstd();
    }

}
