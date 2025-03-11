#!/bin/bash

# Function to migrate a single partition
migrate_partition() {
    local partition=$1
    # shellcheck disable=SC2155
    local start_time=$(date +"%T")

    echo "[INFO] Starting migration for partition state_changes_data_$partition at $start_time"
    echo "[INFO] Starting migration for partition state_changes_data_$partition at $start_time" >> "$LOG_FILE"

    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -p "$DB_PORT" -c "
        WITH ordered_data AS (
            SELECT
                account_id,
                data_key,
                data_value,
                block_height AS block_height_from,
                LAG(block_height) OVER (PARTITION BY account_id, data_key ORDER BY block_height DESC) AS block_height_to
            FROM state_changes_data_$partition
        )
        INSERT INTO new_state_changes_data_$partition (account_id, data_key, data_value, block_height_from, block_height_to)
        SELECT
            account_id,
            data_key,
            data_value,
            block_height_from,
            block_height_to
        FROM ordered_data
        WHERE data_value IS NOT NULL
        ON CONFLICT (account_id, data_key, block_height_from) DO NOTHING;
    " 2>&1 | tee -a "$LOG_FILE"

    # shellcheck disable=SC2155
    local end_time=$(date +"%T")
    echo "[INFO] Finished migration for partition state_changes_data_$partition at $end_time"
    echo "[INFO] Finished migration for partition state_changes_data_$partition at $end_time" >> "$LOG_FILE"
}

# Run migrations in parallel for partitions 0 to 99
for i in $(seq 0 99); do
    migrate_partition "$i" &
done

# Wait for all background jobs to finish
wait
