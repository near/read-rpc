#!/bin/bash

# Set database credentials
export PGPASSWORD="S8TSz8vz4sQ80D7ycWSnT5B4"
export DB_NAME="shard5"
export DB_USER="readrpc"
export DB_HOST="localhost"
export DB_PORT=5432

# Set log file
export LOG_FILE="migration5.log"
# Remove old log file if it exists
rm -f "$LOG_FILE"
touch "$LOG_FILE"

echo "Starting migration at $(date)" | tee -a "$LOG_FILE"

./migrate_access_keys.sh &
./migrate_accounts.sh &
./migrate_contracts.sh &
./migrate_state_changes.sh &

wait

echo "Migration completed at $(date)" | tee -a "$LOG_FILE"
