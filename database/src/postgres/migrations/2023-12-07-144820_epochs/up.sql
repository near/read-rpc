CREATE TABLE IF NOT EXISTS validators (
    epoch_id text NOT NULL,
    epoch_height numeric(20,0) NOT NULL,
    epoch_start_height numeric(20,0) NOT NULL,
    epoch_end_height numeric(20,0) NULL,
    validators_info jsonb NOT NULL
);

ALTER TABLE ONLY validators
    ADD CONSTRAINT validators_pk PRIMARY KEY (epoch_id);


CREATE TABLE IF NOT EXISTS protocol_configs (
    epoch_id text NOT NULL,
    epoch_height numeric(20,0) NOT NULL,
    epoch_start_height numeric(20,0) NOT NULL,
    epoch_end_height numeric(20,0) NULL,
    protocol_config jsonb NOT NULL
);

ALTER TABLE ONLY protocol_configs
    ADD CONSTRAINT protocol_config_pk PRIMARY KEY (epoch_id);
