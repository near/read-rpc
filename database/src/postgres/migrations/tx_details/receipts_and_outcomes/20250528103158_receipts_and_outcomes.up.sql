-- Add up migration script here

-- Create receipts table
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id VARCHAR PRIMARY KEY,
    parent_transaction_hash VARCHAR NOT NULL,
    receiver_id VARCHAR NOT NULL,
    block_height NUMERIC(20,0) NOT NULL,
    block_hash VARCHAR NOT NULL,
    shard_id BIGINT NOT NULL
) PARTITION BY HASH (receipt_id);

-- Create receipts partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS receipts_%s PARTITION OF receipts FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i
        );
    END LOOP;
END $$;

-- Create outcomes table
CREATE TABLE IF NOT EXISTS outcomes (
    outcome_id VARCHAR PRIMARY KEY,
    parent_transaction_hash VARCHAR NOT NULL,
    receiver_id VARCHAR NOT NULL,
    block_height NUMERIC(20,0) NOT NULL,
    block_hash VARCHAR NOT NULL,
    shard_id BIGINT NOT NULL
) PARTITION BY HASH (outcome_id);

-- Create outcomes partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS outcomes_%s PARTITION OF outcomes FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i
        );
    END LOOP;
END $$;

-- Create meta table if not exists
CREATE TABLE IF NOT EXISTS meta (
    indexer_id VARCHAR PRIMARY KEY,
    last_processed_block_height NUMERIC(20,0) NOT NULL
);