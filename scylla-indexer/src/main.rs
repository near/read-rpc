use database::StateIndexerDbManager;
use futures::StreamExt;
use std::future::Future;
use std::time::Instant;

#[derive(Clone)]
struct ScyllaSession {
    session: std::sync::Arc<scylla::Session>,
    postgres_db: std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,

    get_state_data_changes: scylla::prepared_statement::PreparedStatement,
    get_state_access_key_changes: scylla::prepared_statement::PreparedStatement,
    get_state_account_changes: scylla::prepared_statement::PreparedStatement,
    get_state_contract_changes: scylla::prepared_statement::PreparedStatement,

    get_chunks: scylla::prepared_statement::PreparedStatement,
    get_blocks: scylla::prepared_statement::PreparedStatement,
    
}

impl ScyllaSession {
    async fn new(postgres_db: database::postgres::state_indexer::PostgresDBManager) -> anyhow::Result<Self> {
        let scylla_session = scylla::SessionBuilder::new()
            .known_node("node-0.gce-us-central-1.454a6a0fee691a4ef489.clusters.scylla.cloud:9042")
            .user("scylla", "0KNn1oVkJsIzM4g")
            .build()
            .await?;

        let get_state_data_changes_query = scylla::statement::query::Query::new(
            "SELECT account_id, block_height, block_hash, data_key, data_value FROM state_indexer.state_changes_data WHERE token(account_id, data_key) >= ? AND token(account_id, data_key) <= ?"
        );
        let get_state_data_changes_prepared = scylla_session.prepare(get_state_data_changes_query).await?;

        let get_state_access_key_changes_query = scylla::statement::query::Query::new(
            "SELECT account_id, block_height, block_hash, data_key, data_value FROM state_indexer.state_changes_access_key WHERE token(account_id, data_key) >= ? AND token(account_id, data_key) <= ?"
        );
        let get_state_access_key_changes_prepared = scylla_session.prepare(get_state_access_key_changes_query).await?;

        let get_state_account_changes_query = scylla::statement::query::Query::new(
            "SELECT account_id, block_height, block_hash, data_value FROM state_indexer.state_changes_account WHERE token(account_id) >= ? AND token(account_id) <= ?"
        );
        let get_state_account_changes_prepared = scylla_session.prepare(get_state_account_changes_query).await?;

        let get_state_contract_changes_query = scylla::statement::query::Query::new(
            "SELECT account_id, block_height, block_hash, data_value FROM state_indexer.state_changes_contract WHERE token(account_id) >= ? AND token(account_id) <= ?"
        );
        let get_state_contract_changes_prepared = scylla_session.prepare(get_state_contract_changes_query).await?;

        let get_chunks_query = scylla::statement::query::Query::new(
            "SELECT chunk_hash, block_height, shard_id, stored_at_block_height FROM state_indexer.chunks WHERE token(chunk_hash) >= ? AND token(chunk_hash) <= ?"
        );
        let get_chunks_prepared = scylla_session.prepare(get_chunks_query).await?;

        let get_blocks_query = scylla::statement::query::Query::new(
            "SELECT block_hash, block_height FROM state_indexer.blocks WHERE token(block_hash) >= ? AND token(block_hash) <= ?"
        );
        let get_blocks_prepared = scylla_session.prepare(get_blocks_query).await?;
        
        Ok(Self {
            session: std::sync::Arc::new(scylla_session),
            postgres_db: std::sync::Arc::new(postgres_db),
            
            get_state_data_changes: get_state_data_changes_prepared,
            get_state_access_key_changes: get_state_access_key_changes_prepared,
            get_state_account_changes: get_state_account_changes_prepared,
            get_state_contract_changes: get_state_contract_changes_prepared,
            
            get_chunks: get_chunks_prepared,
            get_blocks: get_blocks_prepared,
        })
    }

    async fn task_migrate_data_changes_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        postgres_db: std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,
        prepared: scylla::prepared_statement::PreparedStatement,
        rang_number: i64,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<()> {
        println!(
            "Start range number: {} -token range: {} - {}",
            rang_number, token_val_range_start, token_val_range_end,
        );
        let now = Instant::now();
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(String, num_bigint::BigInt, String, String, Option<Vec<u8>>)>();
        let mut counter = 0;
        let mut general_counter = 0;
        while let Some(next_row_res) = rows_stream.next().await {
            counter += 1;
            general_counter += 1;
            let (account_id, block_height, block_hash, data_key, data_value) = next_row_res?;
            let changes = database::postgres::models::StateChangesData {
                account_id: account_id.clone(),
                block_height: bigdecimal::BigDecimal::from(block_height.clone()),
                block_hash,
                data_key: data_key.clone(),
                data_value,
            };
            result.push(changes);
            if counter == 5000 {
                match postgres_db.save_state_changes(&result).await {
                    Ok(_) => {
                        result.clear();
                        counter = 0;
                    }
                    Err(e) => {
                        println!("range number: {} Error saving changes: {}", rang_number, e);
                        anyhow::bail!("Error saving changes: {}", e)
                    }
                };
            }
            if general_counter % 50000 == 0 {
                println!("range number: {} - processed: {}", rang_number, general_counter);
            }
        }
        if !result.is_empty() {
            match postgres_db.save_state_changes(&result).await {
                Ok(_) => {
                    println!("range number: {} Saved last changes", rang_number);
                }
                Err(e) => {
                    println!("range number: {} Error saving changes: {}", rang_number, e);
                    anyhow::bail!("Error saving changes: {}", e)
                }
            };
        }

        println!("Range number: {} Elapsed: {:.2?}", rang_number, now.elapsed());
        Ok(())
    }

    async fn task_migrate_access_key_changes_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        postgres_db: std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,
        prepared: scylla::prepared_statement::PreparedStatement,
        rang_number: i64,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<()> {
        println!(
            "Start range number access_key: {} -token range: {} - {}",
            rang_number, token_val_range_start, token_val_range_end,
        );
        let now = Instant::now();
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(String, num_bigint::BigInt, String, String, Option<Vec<u8>>)>();
        let mut counter = 0;
        let mut general_counter = 0;
        while let Some(next_row_res) = rows_stream.next().await {
            counter += 1;
            general_counter += 1;
            let (account_id, block_height, block_hash, data_key, data_value) = next_row_res?;
            let changes = database::postgres::models::StateChangesAccessKey {
                account_id: account_id.clone(),
                block_height: bigdecimal::BigDecimal::from(block_height.clone()),
                block_hash,
                data_key: data_key.clone(),
                data_value,
            };
            result.push(changes);
            if counter == 5000 {
                match postgres_db.save_access_key_changes(&result).await {
                    Ok(_) => {
                        result.clear();
                        counter = 0;
                    }
                    Err(e) => {
                        println!("range number: {} Error saving access_key changes: {}", rang_number, e);
                        anyhow::bail!("Error saving changes: {}", e)
                    }
                };
            }
            if general_counter % 50000 == 0 {
                println!("access_key range number: {} - processed: {}", rang_number, general_counter);
            }
        }
        if !result.is_empty() {
            match postgres_db.save_access_key_changes(&result).await {
                Ok(_) => {
                    println!("access_key range number: {} Saved last access_key changes", rang_number);
                }
                Err(e) => {
                    println!("range number: {} Error saving access_key changes: {}", rang_number, e);
                    anyhow::bail!("Error saving access_key changes: {}", e)
                }
            };
        }

        println!("Range number access_key: {} Elapsed: {:.2?}", rang_number, now.elapsed());
        Ok(())
    }

    async fn task_migrate_account_changes_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        postgres_db: std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,
        prepared: scylla::prepared_statement::PreparedStatement,
        rang_number: i64,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<()> {
        println!(
            "Start range number account: {} -token range: {} - {}",
            rang_number, token_val_range_start, token_val_range_end,
        );
        let now = Instant::now();
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(String, num_bigint::BigInt, String, Option<Vec<u8>>)>();
        let mut counter = 0;
        let mut general_counter = 0;
        while let Some(next_row_res) = rows_stream.next().await {
            counter += 1;
            general_counter += 1;
            let (account_id, block_height, block_hash, data_value) = next_row_res?;
            let changes = database::postgres::models::StateChangesAccount {
                account_id: account_id.clone(),
                block_height: bigdecimal::BigDecimal::from(block_height.clone()),
                block_hash,
                data_value,
            };
            result.push(changes);
            if counter == 5000 {
                match postgres_db.save_account_changes(&result).await {
                    Ok(_) => {
                        result.clear();
                        counter = 0;
                    }
                    Err(e) => {
                        println!("range number: {} Error saving account changes: {}", rang_number, e);
                        anyhow::bail!("Error saving changes: {}", e)
                    }
                };
            }
            if general_counter % 50000 == 0 {
                println!("account range number: {} - processed: {}", rang_number, general_counter);
            }
        }
        if !result.is_empty() {
            match postgres_db.save_account_changes(&result).await {
                Ok(_) => {
                    println!("account range number: {} Saved last account changes", rang_number);
                }
                Err(e) => {
                    println!("range number: {} Error saving account changes: {}", rang_number, e);
                    anyhow::bail!("Error saving account changes: {}", e)
                }
            };
        }

        println!("Range number account: {} Elapsed: {:.2?}", rang_number, now.elapsed());
        Ok(())
    }

    async fn task_migrate_contract_changes_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        postgres_db: std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,
        prepared: scylla::prepared_statement::PreparedStatement,
        rang_number: i64,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<()> {
        println!(
            "Start range number contract: {} -token range: {} - {}",
            rang_number, token_val_range_start, token_val_range_end,
        );
        let now = Instant::now();
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(String, num_bigint::BigInt, String, Option<Vec<u8>>)>();
        let mut counter = 0;
        let mut general_counter = 0;
        while let Some(next_row_res) = rows_stream.next().await {
            counter += 1;
            general_counter += 1;
            let (account_id, block_height, block_hash, data_value) = next_row_res?;
            let changes = database::postgres::models::StateChangesContract {
                account_id: account_id.clone(),
                block_height: bigdecimal::BigDecimal::from(block_height.clone()),
                block_hash,
                data_value,
            };
            result.push(changes);
            if counter == 5000 {
                match postgres_db.save_contract_changes(&result).await {
                    Ok(_) => {
                        result.clear();
                        counter = 0;
                    }
                    Err(e) => {
                        println!("range number: {} Error saving contract changes: {}", rang_number, e);
                        anyhow::bail!("Error saving changes: {}", e)
                    }
                };
            }
            if general_counter % 50000 == 0 {
                println!("contract range number: {} - processed: {}", rang_number, general_counter);
            }
        }
        if !result.is_empty() {
            match postgres_db.save_contract_changes(&result).await {
                Ok(_) => {
                    println!("contract range number: {} Saved last contract changes", rang_number);
                }
                Err(e) => {
                    println!("range number: {} Error saving contract changes: {}", rang_number, e);
                    anyhow::bail!("Error saving contract changes: {}", e)
                }
            };
        }

        println!("Range number contract: {} Elapsed: {:.2?}", rang_number, now.elapsed());
        Ok(())
    }

    async fn task_migrate_chunks_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        postgres_db: std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,
        prepared: scylla::prepared_statement::PreparedStatement,
        rang_number: i64,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<()> {
        println!(
            "Start range number chunks: {} -token range: {} - {}",
            rang_number, token_val_range_start, token_val_range_end,
        );
        let now = Instant::now();
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(String, num_bigint::BigInt, num_bigint::BigInt, num_bigint::BigInt)>();
        let mut counter = 0;
        let mut general_counter = 0;
        while let Some(next_row_res) = rows_stream.next().await {
            counter += 1;
            general_counter += 1;
            let (chunk_hash, block_height, shard_id, stored_at_block_height) = next_row_res?;
            let chunk = database::postgres::models::Chunk {
                chunk_hash,
                block_height: bigdecimal::BigDecimal::from(block_height.clone()),
                shard_id: bigdecimal::BigDecimal::from(shard_id.clone()),
                stored_at_block_height: bigdecimal::BigDecimal::from(stored_at_block_height.clone()),
            };
            result.push(chunk);
            if counter == 5000 {
                match postgres_db.save_chunks(&result).await {
                    Ok(_) => {
                        result.clear();
                        counter = 0;
                    }
                    Err(e) => {
                        println!("range number: {} Error saving chunks: {}", rang_number, e);
                        anyhow::bail!("Error saving chunks: {}", e)
                    }
                };
            }
            if general_counter % 50000 == 0 {
                println!("chunks range number: {} - processed: {}", rang_number, general_counter);
            }
        }
        if !result.is_empty() {
            match postgres_db.save_chunks(&result).await {
                Ok(_) => {
                    println!("chunks range number: {} Saved last chunks", rang_number);
                }
                Err(e) => {
                    println!("range number: {} Error saving chunks: {}", rang_number, e);
                    anyhow::bail!("Error saving chunks: {}", e)
                }
            };
        }

        println!("Range number chunks: {} Elapsed: {:.2?}", rang_number, now.elapsed());
        Ok(())
    }

    async fn task_migrate_blocks_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        postgres_db: std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,
        prepared: scylla::prepared_statement::PreparedStatement,
        rang_number: i64,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<()> {
        println!(
            "Start range number blocks: {} -token range: {} - {}",
            rang_number, token_val_range_start, token_val_range_end,
        );
        let now = Instant::now();
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(String, num_bigint::BigInt)>();
        let mut counter = 0;
        let mut general_counter = 0;
        while let Some(next_row_res) = rows_stream.next().await {
            counter += 1;
            general_counter += 1;
            let (block_hash, block_height) = next_row_res?;
            let block = database::postgres::models::Block {
                block_hash,
                block_height: bigdecimal::BigDecimal::from(block_height.clone()),
            };
            result.push(block);
            if counter == 5000 {
                match postgres_db.save_blocks(&result).await {
                    Ok(_) => {
                        result.clear();
                        counter = 0;
                    }
                    Err(e) => {
                        println!("range number: {} Error saving blocks: {}", rang_number, e);
                        anyhow::bail!("Error saving blocks: {}", e)
                    }
                };
            }
            if general_counter % 50000 == 0 {
                println!("chunks range number: {} - processed: {}", rang_number, general_counter);
            }
        }
        if !result.is_empty() {
            match postgres_db.save_blocks(&result).await {
                Ok(_) => {
                    println!("blocks range number: {} Saved last chunks", rang_number);
                }
                Err(e) => {
                    println!("range number: {} Error saving blocks: {}", rang_number, e);
                    anyhow::bail!("Error saving chunks: {}", e)
                }
            };
        }

        println!("Range number blocks: {} Elapsed: {:.2?}", rang_number, now.elapsed());
        Ok(())
    }


    async fn run_table_migration<F, Fut>(
        &self,
        task_table_migration: std::sync::Arc<F>,
        prepared: scylla::prepared_statement::PreparedStatement,
    ) -> anyhow::Result<()>
    where
        F: Fn(
                std::sync::Arc<scylla::Session>,
                std::sync::Arc<database::postgres::state_indexer::PostgresDBManager>,
                scylla::prepared_statement::PreparedStatement,
                i64,
                i64,
                i64,
            ) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let sub_ranges = 144 * 100;
        let step = i64::MAX / (sub_ranges / 2);
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(144));
        let mut handlers = vec![];
        let mut rang_number = 0;
        for token_val_range_start in (i64::MIN..=i64::MAX).step_by(step as usize) {
            rang_number += 1;
            let session = self.session.clone();
            let postgres_db = self.postgres_db.clone();
            let prepared_clone = prepared.clone();
            let permit = sem.clone().acquire_owned().await;
            let task_table_migration_clone = task_table_migration.clone();
            handlers.push(tokio::task::spawn(async move {
                let result = task_table_migration_clone(
                    session,
                    postgres_db,
                    prepared_clone,
                    rang_number,
                    token_val_range_start,
                    token_val_range_start + step - 1,
                )
                .await;
                let _permit = permit;
                result
            }));
        }

        for thread in handlers {
            match thread.await {
                Ok(result) => match result {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error saving changes: {}", e)
                    }
                },
                Err(e) => {
                    println!("Error saving changes: {}", e)
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let indexer_config = configuration::read_configuration::<configuration::StateIndexerConfig>().await?;

    let db_manager =
        database::prepare_db_manager::<database::postgres::state_indexer::PostgresDBManager>(&indexer_config.database)
            .await?;
    let scylla_session = ScyllaSession::new(db_manager).await?;

    let now = Instant::now();
    let mut handles = vec![];
    let session = scylla_session.clone();
    let prepared_query = scylla_session.get_state_data_changes.clone();
    handles.push(tokio::spawn(async move {
        session
            .run_table_migration(std::sync::Arc::new(ScyllaSession::task_migrate_data_changes_by_token_range), prepared_query)
            .await
    }));
    
    let session = scylla_session.clone();
    let prepared_query = scylla_session.get_state_access_key_changes.clone();
    handles.push(tokio::spawn(async move {
        session
            .run_table_migration(std::sync::Arc::new(ScyllaSession::task_migrate_access_key_changes_by_token_range), prepared_query)
            .await
    }));

    let session = scylla_session.clone();
    let prepared_query = scylla_session.get_state_account_changes.clone();
    handles.push(tokio::spawn(async move {
        session
            .run_table_migration(std::sync::Arc::new(ScyllaSession::task_migrate_account_changes_by_token_range), prepared_query)
            .await
    }));

    let session = scylla_session.clone();
    let prepared_query = scylla_session.get_state_contract_changes.clone();
    handles.push(tokio::spawn(async move {
        session
            .run_table_migration(std::sync::Arc::new(ScyllaSession::task_migrate_contract_changes_by_token_range), prepared_query)
            .await
    }));
    
    let session = scylla_session.clone();
    let prepared_query = scylla_session.get_chunks.clone();
    handles.push(tokio::spawn(async move {
        session
            .run_table_migration(std::sync::Arc::new(ScyllaSession::task_migrate_chunks_by_token_range), prepared_query)
            .await
    }));
    
    let session = scylla_session.clone();
    let prepared_query = scylla_session.get_blocks.clone();
    handles.push(tokio::spawn(async move {
        session
            .run_table_migration(std::sync::Arc::new(ScyllaSession::task_migrate_blocks_by_token_range), prepared_query)
            .await
    }));

    futures::future::join_all(handles).await;
    
    println!("Migration Elapsed: {:.2?}", now.elapsed());
    
    Ok(())
}
