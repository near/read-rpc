-- Your SQL goes here
CREATE TABLE IF NOT EXISTS state_changes_data (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, data_key, block_height)
) PARTITION BY HASH (account_id);

-- Create 100 hash partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE state_changes_data_%s PARTITION OF state_changes_data FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS state_changes_access_key (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, data_key, block_height)
) PARTITION BY HASH (account_id);

-- Create 100 hash partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE state_changes_access_key_%s PARTITION OF state_changes_access_key FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS state_changes_contract (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create 100 hash partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE state_changes_contract_%s PARTITION OF state_changes_contract FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS state_changes_account (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create 100 hash partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE state_changes_account_%s PARTITION OF state_changes_account FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;
