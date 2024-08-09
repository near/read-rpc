-- Add up migration script here

-- Create state_changes_data table
CREATE TABLE IF NOT EXISTS state_changes_data (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, data_key, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_data partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_data_%s PARTITION OF state_changes_data FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create state_changes_access_key table
CREATE TABLE IF NOT EXISTS state_changes_access_key (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, data_key, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_access_key partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_access_key_%s PARTITION OF state_changes_access_key FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create state_changes_contract table
CREATE TABLE IF NOT EXISTS state_changes_contract (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_contract partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_contract_%s PARTITION OF state_changes_contract FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create state_changes_account table
CREATE TABLE IF NOT EXISTS state_changes_account (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_account partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_account_%s PARTITION OF state_changes_account FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create receipts_map table
CREATE TABLE IF NOT EXISTS receipts_map (
    receipt_id text NOT NULL PRIMARY KEY,
    parent_transaction_hash text NOT NULL,
    receiver_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    shard_id numeric(20,0) NOT NULL
) PARTITION BY HASH (receipt_id);

-- Create receipts_map partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS receipts_map_%s PARTITION OF receipts_map FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create outcomes_map table
CREATE TABLE IF NOT EXISTS outcomes_map (
    outcome_id text NOT NULL PRIMARY KEY,
    parent_transaction_hash text NOT NULL,
    receiver_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    shard_id numeric(20,0) NOT NULL
) PARTITION BY HASH (outcome_id);

-- Create outcomes_map partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS outcomes_map_%s PARTITION OF outcomes_map FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;