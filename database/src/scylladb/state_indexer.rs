use crate::ScyllaStorageManager;
use num_traits::ToPrimitive;
use scylla::prepared_statement::PreparedStatement;

#[derive(Debug)]
pub(crate) struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,

    add_state_changes: PreparedStatement,
    delete_state_changes: PreparedStatement,

    add_access_key: PreparedStatement,
    delete_access_key: PreparedStatement,

    #[cfg(feature = "account_access_keys")]
    add_account_access_keys: PreparedStatement,
    #[cfg(feature = "account_access_keys")]
    get_account_access_keys: PreparedStatement,

    add_contract: PreparedStatement,
    delete_contract: PreparedStatement,

    add_account: PreparedStatement,
    delete_account: PreparedStatement,

    add_block: PreparedStatement,
    add_chunk: PreparedStatement,
    add_account_state: PreparedStatement,
    update_meta: PreparedStatement,
    last_processed_block_height: PreparedStatement,
}

#[async_trait::async_trait]
impl crate::BaseDbManager for ScyllaDBManager {
    async fn new(
        database_url: &str,
        database_user: Option<&str>,
        database_password: Option<&str>,
        database_options: crate::AdditionalDatabaseOptions,
    ) -> anyhow::Result<Box<Self>> {
        let scylla_db_session = std::sync::Arc::new(
            Self::get_scylladb_session(
                database_url,
                database_user,
                database_password,
                database_options.preferred_dc.as_deref(),
                database_options.keepalive_interval,
                database_options.max_retry,
                database_options.strict_mode,
            )
            .await?,
        );
        Self::new_from_session(scylla_db_session).await
    }
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaDBManager {
    async fn create_tables(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session
            .use_keyspace("state_indexer", false)
            .await?;
        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_data (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_key varchar,
                    data_value BLOB,
                    PRIMARY KEY ((account_id, data_key), block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_access_key (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_key varchar,
                    data_value BLOB,
                    PRIMARY KEY ((account_id, data_key), block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        #[cfg(feature = "account_access_keys")]
        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS account_access_keys (
                    account_id varchar,
                    block_height varint,
                    active_access_keys map<varchar, BLOB>,
                    PRIMARY KEY (account_id, block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_contract (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_value BLOB,
                    PRIMARY KEY (account_id, block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_account (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_value BLOB,
                    PRIMARY KEY (account_id, block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS blocks (
                    block_hash varchar,
                    block_height varint,
                    PRIMARY KEY (block_hash)
                )
                ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_hash varchar,
                    block_height varint,
                    shard_id varint,
                    stored_at_block_height varint,
                    PRIMARY KEY (chunk_hash, block_height)
                )
            ",
                &[],
            )
            .await?;
        scylla_db_session
            .query(
                "
                CREATE INDEX IF NOT EXISTS chunk_block_height ON chunks (block_height);
            ",
                &[],
            )
            .await?;

        // This index is used for the cases where we need to fetch the block hash by block height
        // Most of the cases this is required to serve `query` requests since the response includes the
        // so-called block reference which is represented by `block_height` and `block_hash`
        // We want to include the block reference pointing to the data we have in the database.
        // But we don't have a field `block_hash` there, so it's currently impossible to fetch the block hash
        // from the `state_*` tables.
        // We might want to consider denormalizing those tables and adding `block_hash` there, but for now
        // it doesn't look like a good reason for recollecting all the data.
        scylla_db_session
            .query(
                "
                CREATE INDEX IF NOT EXISTS blocks_block_height ON blocks (block_height);
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS meta (
                    indexer_id varchar PRIMARY KEY,
                    last_processed_block_height varint
                )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS account_state (
                    account_id varchar,
                    data_key varchar,
                    PRIMARY KEY (account_id, data_key),
                )
            ",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn create_keyspace(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.query(
            "CREATE KEYSPACE IF NOT EXISTS state_indexer WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;
        Ok(())
    }

    async fn prepare(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {
            scylla_session: scylla_db_session.clone(),
            add_state_changes: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_data
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?)",
            )
            .await?,
            delete_state_changes: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_data
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            )
            .await?,

            add_access_key: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?)",
            )
            .await?,
            delete_access_key: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            )
            .await?,

            #[cfg(feature = "account_access_keys")]
            add_account_access_keys: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.account_access_keys
                    (account_id, block_height, active_access_keys)
                    VALUES(?, ?, ?)",
            )
            .await?,

            #[cfg(feature = "account_access_keys")]
            get_account_access_keys: Self::prepare_write_query(
                &scylla_db_session,
                "SELECT active_access_keys FROM state_indexer.account_access_keys
                    WHERE account_id = ? AND block_height < ? LIMIT 1",
            )
            .await?,

            add_contract: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            delete_contract: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            )
            .await?,

            add_account: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            delete_account: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            )
            .await?,
            add_block: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.blocks
                    (block_hash, block_height)
                    VALUES (?, ?)",
            )
            .await?,
            add_chunk: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.chunks
                    (chunk_hash, block_height, shard_id, stored_at_block_height)
                    VALUES (?, ?, ?, ?)",
            )
            .await?,
            add_account_state: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.account_state
                    (account_id, data_key)
                    VALUES(?, ?)",
            )
            .await?,
            update_meta: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.meta
                    (indexer_id, last_processed_block_height)
                    VALUES (?, ?)",
            )
            .await?,
            last_processed_block_height: Self::prepare_write_query(
                &scylla_db_session,
                "SELECT last_processed_block_height FROM state_indexer.meta WHERE indexer_id = ?",
            )
            .await?,
        }))
    }
}

#[async_trait::async_trait]
impl crate::StateIndexerDbManager for ScyllaDBManager {
    async fn add_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_state_changes,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
                hex::encode(key).to_string(),
                value.to_vec(),
            ),
        )
        .await?;
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_account_state,
            (account_id.to_string(), hex::encode(key).to_string()),
        )
        .await?;
        Ok(())
    }

    async fn delete_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_state_changes,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
                hex::encode(key).to_string(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn add_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
        access_key: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_access_key,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
                hex::encode(public_key).to_string(),
                access_key.to_vec(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn delete_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_access_key,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
                hex::encode(public_key).to_string(),
            ),
        )
        .await?;
        Ok(())
    }

    #[cfg(feature = "account_access_keys")]
    async fn get_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        let (result,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account_access_keys,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(std::collections::HashMap<String, Vec<u8>>,)>()?;
        Ok(result)
    }

    #[cfg(feature = "account_access_keys")]
    async fn add_account_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        public_key: &[u8],
        access_key: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let public_key_hex = hex::encode(public_key).to_string();

        let mut account_keys = match self.get_access_keys(account_id.clone(), block_height).await {
            Ok(account_keys) => account_keys,
            Err(_) => std::collections::HashMap::new(),
        };

        match access_key {
            Some(access_key) => {
                account_keys.insert(public_key_hex, access_key.to_vec());
            }
            None => {
                account_keys.remove(&public_key_hex);
            }
        }
        self.update_account_access_keys(account_id.to_string(), block_height, account_keys)
            .await?;
        Ok(())
    }

    #[cfg(feature = "account_access_keys")]
    async fn update_account_access_keys(
        &self,
        account_id: String,
        block_height: u64,
        account_keys: std::collections::HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_account_access_keys,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                &account_keys,
            ),
        )
        .await?;
        Ok(())
    }

    async fn add_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        code: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_contract,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
                code.to_vec(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn delete_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_contract,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn add_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        account: Vec<u8>,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_account,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
                account,
            ),
        )
        .await?;
        Ok(())
    }

    async fn delete_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_account,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                block_hash.to_string(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn add_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_block,
            (
                block_hash.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?;
        Ok(())
    }

    async fn add_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let save_chunks_futures = chunks
            .iter()
            .map(|(chunk_hash, shard_id, height_included)| {
                Self::execute_prepared_query(
                    &self.scylla_session,
                    &self.add_chunk,
                    (
                        chunk_hash,
                        num_bigint::BigInt::from(block_height),
                        num_bigint::BigInt::from(*shard_id),
                        num_bigint::BigInt::from(*height_included),
                    ),
                )
            });
        futures::future::try_join_all(save_chunks_futures).await?;
        Ok(())
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.update_meta,
            (indexer_id, num_bigint::BigInt::from(block_height)),
        )
        .await?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        let (block_height,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.last_processed_block_height,
            (indexer_id,),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt,)>()?;
        block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }
}
