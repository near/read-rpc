#!/bin/bash

DATABASE_URL=$1
PREVIOUS_RANGE_ID=$2
CURRENT_RANGE_ID=$3
CURRENT_RANGE_BLOCK_HEIGHT_START=$4
MAX_PARALLEL=${MAX_PARALLEL_JOBS}

# Function to migrate a single partition
migrate_partition() {
    local partition=$1

    # shellcheck disable=SC2155
    local start_time=$(date +"%T")
    echo "[INFO] Starting partition state_changes_access_key_$partition at $start_time" >> "$LOG_FILE"
        
    psql "$DATABASE_URL" -c "
        WITH ordered_data AS (
            SELECT
                account_id,
                data_key,
                data_value,
                block_height AS block_height_from,
                LAG(block_height) OVER (
                    PARTITION BY account_id, data_key
                    ORDER BY block_height DESC
                ) AS block_height_to
            FROM state_changes_access_key_${PREVIOUS_RANGE_ID}_$partition
        ),
        insert_compact AS (
            INSERT INTO state_changes_access_key_${PREVIOUS_RANGE_ID}_compact_$partition (
                account_id, data_key, data_value, block_height_from, block_height_to
            )
            SELECT
                account_id, data_key, data_value, block_height_from, block_height_to
            FROM ordered_data
            WHERE data_value IS NOT NULL
            ON CONFLICT (account_id, data_key, block_height_from) DO NOTHING
        )
        INSERT INTO state_changes_access_key_${CURRENT_RANGE_ID}_$partition (
            account_id, block_height, data_key, data_value
        )
        SELECT
            account_id,
            block_height_from AS block_height,
            data_key,
            data_value
        FROM ordered_data
        WHERE data_value IS NOT NULL
          AND block_height_from <= ${CURRENT_RANGE_BLOCK_HEIGHT_START}
          AND (block_height_to IS NULL OR block_height_to > ${CURRENT_RANGE_BLOCK_HEIGHT_START})
        ON CONFLICT (account_id, data_key, block_height) DO NOTHING;
    " 2>&1 | tee -a "$LOG_FILE"
    
    # shellcheck disable=SC2155
    local end_time=$(date +"%T")
    echo "[INFO] Finished partition state_changes_access_key_$partition at $end_time" >> "$LOG_FILE"
}

# Run migrations in parallel for partitions 0 to 99
running=0
for i in $(seq 0 99); do
    migrate_partition "$i" &

    ((running+=1))
    if ((running >= MAX_PARALLEL)); then
        wait -n  # Wait for at least one job to finish
        ((running-=1))
    fi
done

# Wait for all background jobs to finish
wait
