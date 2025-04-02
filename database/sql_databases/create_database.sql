-- Create state_changes_data table
CREATE TABLE IF NOT EXISTS state_changes_data_1300 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
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
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_data_1300_%s PARTITION OF state_changes_data_1300 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;



-- Create state_changes_access_key table
CREATE TABLE IF NOT EXISTS state_changes_access_key_1300 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
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
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_access_key_1300_%s PARTITION OF state_changes_access_key_1300 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;



-- Create state_changes_account table
CREATE TABLE IF NOT EXISTS state_changes_account_1300 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_account partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_account_1300_%s PARTITION OF state_changes_account_1300 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;


-- Create state_changes_contract table
CREATE TABLE IF NOT EXISTS state_changes_contract_1300 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_contract partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_contract_1300_%s PARTITION OF state_changes_contract_1300 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;

-- Create state_changes_data table
CREATE TABLE IF NOT EXISTS state_changes_data_1305 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
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
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_data_1305_%s PARTITION OF state_changes_data_1305 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;



-- Create state_changes_access_key table
CREATE TABLE IF NOT EXISTS state_changes_access_key_1305 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
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
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_access_key_1305_%s PARTITION OF state_changes_access_key_1305 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;



-- Create state_changes_account table
CREATE TABLE IF NOT EXISTS state_changes_account_1305 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_account partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_account_1305_%s PARTITION OF state_changes_account_1305 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;


-- Create state_changes_contract table
CREATE TABLE IF NOT EXISTS state_changes_contract_1305 (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    data_value bytea NULL,
    PRIMARY KEY (account_id, block_height)
) PARTITION BY HASH (account_id);

-- Create state_changes_contract partitions
DO $$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 0..99 LOOP
                EXECUTE format('CREATE TABLE IF NOT EXISTS state_changes_contract_1305_%s PARTITION OF state_changes_contract_1305 FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
            END LOOP;
    END $$;