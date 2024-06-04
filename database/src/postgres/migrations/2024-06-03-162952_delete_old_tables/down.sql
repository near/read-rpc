-- This file should undo anything in `up.sql`
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

CREATE TABLE IF NOT EXISTS account_state (
    account_id text NOT NULL,
    data_key text NOT NULL
);

ALTER TABLE ONLY account_state
    ADD CONSTRAINT account_state_pk PRIMARY KEY (account_id, data_key);
