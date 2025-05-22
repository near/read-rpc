-- Add up migration script here
DO $$
DECLARE
    i INT;
BEGIN
    -- Drop column from each partition
    FOR i IN 0..99 LOOP
        EXECUTE format('ALTER TABLE state_changes_data_%s DROP COLUMN IF EXISTS block_hash', i);
        EXECUTE format('ALTER TABLE state_changes_access_key_%s DROP COLUMN IF EXISTS block_hash', i);
        EXECUTE format('ALTER TABLE state_changes_contract_%s DROP COLUMN IF EXISTS block_hash', i);
        EXECUTE format('ALTER TABLE state_changes_account_%s DROP COLUMN IF EXISTS block_hash', i);
    END LOOP;

    -- Drop column from parent table
    ALTER TABLE state_changes_data DROP COLUMN IF EXISTS block_hash;
    ALTER TABLE state_changes_access_key DROP COLUMN IF EXISTS block_hash;
    ALTER TABLE state_changes_contract DROP COLUMN IF EXISTS block_hash;
    ALTER TABLE state_changes_account DROP COLUMN IF EXISTS block_hash;
END $$;
