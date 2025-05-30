#!/bin/bash

# Function to migrate a single partition
migrate_partition() {
    local partition=$1
    # shellcheck disable=SC2155
    local start_time=$(date +"%T")

    echo "[INFO] Starting migration for partition state_changes_access_key_$partition at $start_time"
    echo "[INFO] Starting migration for partition state_changes_access_key_$partition at $start_time" >> "$LOG_FILE"

    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -p "$DB_PORT" -c "
        INSERT INTO state_changes_access_key_1300_$partition (
            account_id,
            block_height,
            data_key,
            data_value
        )
        SELECT 
            account_id,
            block_height,
            data_key,
            data_value
        FROM state_changes_access_key_$partition
        WHERE 
            block_height >= 130000000
            AND block_height < 130500000
        ON CONFLICT (account_id, data_key, block_height) DO NOTHING;
    " 2>&1 | tee -a "$LOG_FILE"

    # shellcheck disable=SC2155
    local end_time=$(date +"%T")
    echo "[INFO] Finished migration for partition state_changes_access_key_$partition at $end_time"
    echo "[INFO] Finished migration for partition state_changes_access_key_$partition at $end_time" >> "$LOG_FILE"
}

# Run migrations in parallel for partitions 0 to 99
for i in $(seq 0 99); do
    migrate_partition "$i" &
done

# Wait for all background jobs to finish
wait
