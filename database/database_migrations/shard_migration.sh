#!/bin/bash

# Parse arguments or use environment variables
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --db_name)
            export DB_NAME="$2"
            shift 2
            ;;
        --db_user)
            export DB_USER="$2"
            shift 2
            ;;
        --db_password)
            export PGPASSWORD="$2"
            shift 2
            ;;
        --host)
            export DB_HOST="$2"
            shift 2
            ;;
        --port)
            export DB_PORT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Set defaults from environment if not set by args
: "${DB_NAME:=${DB_NAME}}"
: "${DB_USER:=${DB_USER}}"
: "${PGPASSWORD:=${PGPASSWORD}}"
: "${DB_HOST:=${DB_HOST}}"
: "${DB_PORT:=${DB_PORT}}"

# Check required variables
if [[ -z "$DB_NAME" || -z "$DB_USER" || -z "$PGPASSWORD" || -z "$DB_HOST" || -z "$DB_PORT" ]]; then
    echo "All arguments are required: --db_name, --db_user, --db_password, --host, --port (or set corresponding env vars)"
    exit 1
fi


# Set log file
export LOG_FILE="migration_${DB_NAME}.log"
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
