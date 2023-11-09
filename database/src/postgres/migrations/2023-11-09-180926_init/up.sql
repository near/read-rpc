CREATE TABLE IF NOT EXISTS state_chages_data (
    account_id text NOT NULL,
    block_height numeric(20,0) NOT NULL,
    block_hash text NOT NULL,
    data_key text NOT NULL,
    data_value bytea NULL,
);

ALTER TABLE ONLY state_chages_data
    ADD CONSTRAINT state_chage_data_pk PRIMARY KEY (account_id, data_key, block_height);
