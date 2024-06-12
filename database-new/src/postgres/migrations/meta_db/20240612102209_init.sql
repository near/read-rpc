-- Add migration script here

---
--- Blocks table
---
CREATE TABLE IF NOT EXISTS blocks (
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL
);

ALTER TABLE ONLY blocks
    ADD CONSTRAINT block_pk PRIMARY KEY (block_hash);

-- Create 100 hash partitions for blocks table
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE blocks_%s PARTITION OF blocks FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


---
--- Chunks table
---
CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    shard_id numeric(20,0) NOT NULL,
);

ALTER TABLE ONLY chunks
    ADD CONSTRAINT chunk_pk PRIMARY KEY (chunk_hash);

-- Create 100 hash partitions for chunks table
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE chunks_%s PARTITION OF chunks FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;


---
--- Chunks Duplicate table
---
CREATE TABLE IF NOT EXISTS chunks_duplicate (
    chunk_hash text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    shard_id numeric(20,0) NOT NULL,
    included_in_block_height numeric(20,0) NOT NULL
);

ALTER TABLE ONLY chunks_duplicate
    ADD CONSTRAINT chunk_pk PRIMARY KEY (chunk_hash);

-- Create 100 hash partitions for chunks_duplicate table
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..99 LOOP
        EXECUTE format('CREATE TABLE chunks_duplicate_%s PARTITION OF chunks_duplicate FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
    END LOOP;
END $$;

---
--- Metadata table
---
CREATE TABLE IF NOT EXISTS meta (
    indexer_id text NOT NULL,
    last_processed_block_height numeric(20,0) NOT NULL
);

ALTER TABLE ONLY meta
    ADD CONSTRAINT meta_pk PRIMARY KEY (indexer_id);