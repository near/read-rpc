CREATE OR REPLACE PROCEDURE create_new_range_tables(range_id INT)
    LANGUAGE plpgsql
AS $$
DECLARE
    i INT;
BEGIN
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS state_changes_data_%s (
            account_id text NOT NULL,
            block_height numeric(20,0) NOT NULL,
            data_key text NOT NULL,
            data_value bytea NULL,
            PRIMARY KEY (account_id, data_key, block_height)
        ) PARTITION BY HASH (account_id);
    ', range_id);

    -- Create 100 partitions
    FOR i IN 0..99 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS state_changes_data_%s_%s PARTITION OF state_changes_data_%s FOR VALUES WITH (MODULUS 100, REMAINDER %s);
            ', range_id, i, range_id, i);
    END LOOP;


    EXECUTE format('
        CREATE TABLE IF NOT EXISTS state_changes_access_key_%s (
            account_id text NOT NULL,
            block_height numeric(20,0) NOT NULL,
            data_key text NOT NULL,
            data_value bytea NULL,
            PRIMARY KEY (account_id, data_key, block_height)
        ) PARTITION BY HASH (account_id);
    ', range_id);

    -- Create 100 partitions
    FOR i IN 0..99 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS state_changes_access_key_%s_%s PARTITION OF state_changes_access_key_%s FOR VALUES WITH (MODULUS 100, REMAINDER %s);
            ', range_id, i, range_id, i);
    END LOOP;

    EXECUTE format('
        CREATE TABLE IF NOT EXISTS state_changes_contract_%s (
            account_id text NOT NULL,
            block_height numeric(20,0) NOT NULL,
            data_value bytea NULL,
            PRIMARY KEY (account_id, block_height)
        ) PARTITION BY HASH (account_id);
    ', range_id);

    -- Create 100 partitions
    FOR i IN 0..99 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS state_changes_contract_%s_%s PARTITION OF state_changes_contract_%s FOR VALUES WITH (MODULUS 100, REMAINDER %s);
            ', range_id, i, range_id, i);
    END LOOP;

    EXECUTE format('
        CREATE TABLE IF NOT EXISTS state_changes_account_%s (
            account_id text NOT NULL,
            block_height numeric(20,0) NOT NULL,
            data_value bytea NULL,
            PRIMARY KEY (account_id, block_height)
        ) PARTITION BY HASH (account_id);
    ', range_id);

    -- Create 100 partitions
    FOR i IN 0..99 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS state_changes_account_%s_%s PARTITION OF state_changes_account_%s FOR VALUES WITH (MODULUS 100, REMAINDER %s);
            ', range_id, i, range_id, i);
    END LOOP;
END;
$$;
