-- Create new_state_changes_data table
CREATE TABLE IF NOT EXISTS state_changes_data_1300_compact (
    account_id text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NOT NULL,
    block_height_from numeric(20,0) NOT NULL,
    block_height_to numeric(20,0) NULL,
    PRIMARY KEY (account_id, data_key, block_height_from)
) PARTITION BY HASH (account_id);

-- Create state_changes_data partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_data_1300_compact_%s PARTITION OF state_changes_data_1300_compact FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;


-- Create new_state_changes_access_key table
CREATE TABLE IF NOT EXISTS state_changes_access_key_1300_compact (
     account_id text NOT NULL,
     data_key text NOT NULL,
     data_value bytea NOT NULL,
     block_height_from numeric(20,0) NOT NULL,
     block_height_to numeric(20,0) NULL,
     PRIMARY KEY (account_id, data_key, block_height_from)
) PARTITION BY HASH (account_id);

-- Create state_changes_access_key partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_access_key_1300_compact_%s PARTITION OF state_changes_access_key_1300_compact FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;


-- Create new_state_changes_contract table
CREATE TABLE IF NOT EXISTS state_changes_contract_1300_compact (
    account_id text NOT NULL,
    data_value bytea NOT NULL,
    block_height_from numeric(20,0) NOT NULL,
    block_height_to numeric(20,0) NULL,
    PRIMARY KEY (account_id, block_height_from)
) PARTITION BY HASH (account_id);

-- Create state_changes_contract partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_contract_1300_compact_%s PARTITION OF state_changes_contract_1300_compact FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;


-- Create new_state_changes_account table
CREATE TABLE IF NOT EXISTS state_changes_account_1300_compact (
    account_id text NOT NULL,
    data_value bytea NULL,
    block_height_from numeric(20,0) NOT NULL,
    block_height_to numeric(20,0) NULL,
    PRIMARY KEY (account_id, block_height_from)
) PARTITION BY HASH (account_id);

-- Create state_changes_account partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_account_1300_compact_%s PARTITION OF state_changes_account_1300_compact FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;
