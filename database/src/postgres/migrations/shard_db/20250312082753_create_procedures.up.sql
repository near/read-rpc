-- Update the latest block height for the given account_ids and data_keys in the new_state_changes_data table
CREATE OR REPLACE PROCEDURE state_data_update_latest_block_height(
    account text,
    data_keys TEXT[],
    new_block_height numeric(20,0)
)
    LANGUAGE plpgsql
AS $$
DECLARE
    key text;
BEGIN
    FOREACH key IN ARRAY data_keys LOOP
        UPDATE new_state_changes_data
        SET block_height_to = new_block_height
        WHERE account_id = account
          AND data_key = key
          AND block_height_from < new_block_height
          AND block_height_to IS NULL;
    END LOOP;
END $$;

-- Update the latest block height for the given account_ids and data_keys in the new_state_changes_access_key table
CREATE OR REPLACE PROCEDURE access_keys_update_latest_block_height(
    account text,
    data_keys TEXT[],
    new_block_height numeric(20,0)
)
    LANGUAGE plpgsql
AS $$
DECLARE
    key text;
BEGIN
    FOREACH key IN ARRAY data_keys LOOP
        UPDATE new_state_changes_access_key
        SET block_height_to = new_block_height
        WHERE account_id = account
          AND data_key = key
          AND block_height_from < new_block_height
          AND block_height_to IS NULL;
    END LOOP;
END $$;


-- Update the latest block height for the given account_ids in the new_state_changes_contract table
CREATE OR REPLACE PROCEDURE contracts_update_latest_block_height(
    account_ids TEXT[],
    new_block_height numeric(20,0)
)
    LANGUAGE plpgsql
AS $$
DECLARE
    account text;
BEGIN
    FOREACH account IN ARRAY account_ids LOOP
        UPDATE new_state_changes_contract
        SET block_height_to = new_block_height
        WHERE account_id = account
          AND block_height_from < new_block_height
          AND block_height_to IS NULL;
    END LOOP;
END $$;

-- Update the latest block height for the given account_ids in the new_state_changes_account table
CREATE OR REPLACE PROCEDURE accounts_update_latest_block_height(
    account_ids TEXT[],
    new_block_height numeric(20,0)
)
    LANGUAGE plpgsql
AS $$
DECLARE
    account text;
BEGIN
    FOREACH account IN ARRAY account_ids LOOP
        UPDATE new_state_changes_account
        SET block_height_to = new_block_height
        WHERE account_id = account
          AND block_height_from < new_block_height
          AND block_height_to IS NULL;
    END LOOP;
END $$;
