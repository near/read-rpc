-- Add up migration script here
ALTER TABLE state_changes_data DROP COLUMN IF EXISTS block_hash;
ALTER TABLE state_changes_access_key DROP COLUMN IF EXISTS block_hash;
ALTER TABLE state_changes_contract DROP COLUMN IF EXISTS block_hash;
ALTER TABLE state_changes_account DROP COLUMN IF EXISTS block_hash;

-- TODO: REMOVE ALL DEPRECATED TABLES.
DROP TABLE IF EXISTS state_changes_data;
DROP TABLE IF EXISTS state_changes_access_key;
DROP TABLE IF EXISTS state_changes_contract;
DROP TABLE IF EXISTS state_changes_account;
