---
--- State-indexer tables
---
CREATE TABLE IF NOT EXISTS state_changes_data (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NULL
);

ALTER TABLE ONLY state_changes_data
    ADD CONSTRAINT state_changes_data_pk PRIMARY KEY (account_id, data_key, block_height);


CREATE TABLE IF NOT EXISTS state_changes_access_key (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NULL
);

ALTER TABLE ONLY state_changes_access_key
    ADD CONSTRAINT state_changes_access_key_pk PRIMARY KEY (account_id, data_key, block_height);


CREATE TABLE IF NOT EXISTS state_changes_access_keys (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    active_access_keys jsonb NULL
);

ALTER TABLE ONLY state_changes_access_keys
    ADD CONSTRAINT state_changes_access_keys_pk PRIMARY KEY (account_id, block_height);

CREATE TABLE IF NOT EXISTS state_changes_contract (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_value bytea NULL
);

ALTER TABLE ONLY state_changes_contract
    ADD CONSTRAINT state_changes_contract_pk PRIMARY KEY (account_id, block_height);


CREATE TABLE IF NOT EXISTS state_changes_account (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_value bytea NULL
);

ALTER TABLE ONLY state_changes_account
    ADD CONSTRAINT state_changes_account_pk PRIMARY KEY (account_id, block_height);

CREATE TABLE IF NOT EXISTS block (
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL
);

ALTER TABLE ONLY block
    ADD CONSTRAINT block_pk PRIMARY KEY (block_hash);

CREATE TABLE IF NOT EXISTS chunk (
    chunk_hash text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    shard_id numeric(20,0) NOT NULL,
    stored_at_block_height numeric(20,0) NOT NULL
);

ALTER TABLE ONLY chunk
    ADD CONSTRAINT chunk_pk PRIMARY KEY (chunk_hash, block_height);

CREATE TABLE IF NOT EXISTS account_state (
    account_id text NOT NULL,
    data_key text NOT NULL
);

ALTER TABLE ONLY account_state
    ADD CONSTRAINT account_state_pk PRIMARY KEY (account_id, data_key);

---
--- Tx-indexer tables
---

CREATE TABLE IF NOT EXISTS transaction_detail (
    transaction_hash text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    account_id text NOT NULL,
    transaction_details bytea NOT NULL
);

ALTER TABLE ONLY transaction_detail
    ADD CONSTRAINT transaction_detail_pk PRIMARY KEY (transaction_hash, block_height);

CREATE TABLE IF NOT EXISTS receipt_map (
    receipt_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    parent_transaction_hash text NOT NULL,
    shard_id numeric(20,0) NOT NULL
);

ALTER TABLE ONLY receipt_map
    ADD CONSTRAINT receipt_map_pk PRIMARY KEY (receipt_id);

---
--- Tx-indexer cache tables
---

CREATE TABLE IF NOT EXISTS transaction_cache (
    block_height numeric(20,0) NOT NULL,
    transaction_hash text NOT NULL,
    transaction_details bytea NOT NULL
);

ALTER TABLE ONLY transaction_cache
    ADD CONSTRAINT transaction_cache_pk PRIMARY KEY (block_height, transaction_hash);

CREATE TABLE IF NOT EXISTS receipt_outcome (
    block_height numeric(20,0) NOT NULL,
    transaction_hash text NOT NULL,
    receipt_id text NOT NULL,
    receipt bytea NOT NULL,
    outcome bytea NOT NULL
);

ALTER TABLE ONLY receipt_outcome
    ADD CONSTRAINT receipt_outcome_pk PRIMARY KEY (block_height, transaction_hash, receipt_id);

---
--- Metadata table
---
CREATE TABLE IF NOT EXISTS meta (
    indexer_id text NOT NULL,
    last_processed_block_height numeric(20,0) NOT NULL
);

ALTER TABLE ONLY meta
    ADD CONSTRAINT meta_pk PRIMARY KEY (indexer_id);

