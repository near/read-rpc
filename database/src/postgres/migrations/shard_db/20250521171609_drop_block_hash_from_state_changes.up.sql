-- Add up migration script here
DO $$
DECLARE
    i INT;
BEGIN
    -- Drop column from parent table
    ALTER TABLE state_changes_data DROP COLUMN IF EXISTS block_hash;
    ALTER TABLE state_changes_access_key DROP COLUMN IF EXISTS block_hash;
    ALTER TABLE state_changes_contract DROP COLUMN IF EXISTS block_hash;
    ALTER TABLE state_changes_account DROP COLUMN IF EXISTS block_hash;
END $$;
