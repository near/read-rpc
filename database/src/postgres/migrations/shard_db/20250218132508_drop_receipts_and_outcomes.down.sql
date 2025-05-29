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
