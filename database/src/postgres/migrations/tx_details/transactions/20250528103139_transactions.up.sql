-- Add up migration script here

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
                    transaction_hash text NOT NULL,
                    sender_account_id text NOT NULL,
                    block_height numeric(20,0) NOT NULL,
                    transaction_details bytea NOT NULL,
                    PRIMARY KEY (transaction_hash)
                ) PARTITION BY HASH (transaction_hash);

-- Create transactions partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS transactions_%s PARTITION OF transactions FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i
        );
    END LOOP;
END $$;