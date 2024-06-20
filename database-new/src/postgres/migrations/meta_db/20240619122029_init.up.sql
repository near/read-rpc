-- Add up migration script here

-- Create blocks table
CREATE TABLE IF NOT EXISTS blocks (
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL PRIMARY KEY
) PARTITION BY HASH (block_hash);

-- Create blocks partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS blocks_%s PARTITION OF blocks FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create chunks table
CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash text NOT NULL PRIMARY KEY,
    block_height numeric(20,0) NOT NULL,
    shard_id numeric(20,0) NOT NULL
) PARTITION BY HASH (chunk_hash);

-- Create chunks partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS chunks_%s PARTITION OF chunks FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create chunks_duplicate table
-- For some reason chunk for a block can be missed and NEAR protocol includes the Chunk from previous block.
-- Lake doesn’t duplicate the data on S3.
CREATE TABLE IF NOT EXISTS chunks_duplicate (
    chunk_hash text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    shard_id numeric(20,0) NOT NULL,
    included_in_block_height numeric(20,0) NOT NULL,
    PRIMARY KEY (chunk_hash, block_height)
) PARTITION BY HASH (block_height);

-- Create chunks_duplicate partitions
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS chunks_duplicate_%s PARTITION OF chunks_duplicate FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


-- Create validators table to store validators info for each epoch
CREATE TABLE IF NOT EXISTS validators (
    epoch_id text NOT NULL PRIMARY KEY,
    epoch_height numeric(20,0) NOT NULL,
    epoch_start_height numeric(20,0) NOT NULL,
    epoch_end_height numeric(20,0) NULL,
    validators_info jsonb NOT NULL
);


-- Create protocol_configs table to store protocol config for each epoch
CREATE TABLE IF NOT EXISTS protocol_configs (
    epoch_id text NOT NULL PRIMARY KEY,
    epoch_height numeric(20,0) NOT NULL,
    epoch_start_height numeric(20,0) NOT NULL,
    epoch_end_height numeric(20,0) NULL,
    protocol_config jsonb NOT NULL
);


-- Create meta table to store last processed block height for each indexer
CREATE TABLE IF NOT EXISTS meta (
    indexer_id text NOT NULL PRIMARY KEY,
    last_processed_block_height numeric(20,0) NOT NULL
);
