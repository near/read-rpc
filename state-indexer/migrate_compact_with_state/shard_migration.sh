#!/bin/bash

# Optional: Limit parallelism
# Spawning 100 parallel psql connections may overwhelm your PostgreSQL server.
# You can control parallelism with a semaphore.
# Here we use a simple approach to limit the number of parallel jobs.
export MAX_PARALLEL_JOBS=100 # Maximum number of parallel jobs

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting snapshot state for $2 and compact for $3 at $(date)"

# wait -n is only supported in Bash version 4.3 and above — but on macOS use older Bash version (e.g., 3.2).
# You can also install a newer Bash with Homebrew:
# brew install bash
# Then run script with the new Bash:
# /opt/homebrew/bin/bash "$SCRIPT_DIR/migrate_access_keys.sh" "$@" &
# Then wait -n will work just fine!
# If you are using macOS, you can use the Homebrew-installed Bash to run the scripts.
if [[ "$(uname)" == "Darwin" ]]; then
    /opt/homebrew/bin/bash "$SCRIPT_DIR/migrate_access_keys.sh" "$@" &
    /opt/homebrew/bin/bash "$SCRIPT_DIR/migrate_accounts.sh" "$@" &
    /opt/homebrew/bin/bash "$SCRIPT_DIR/migrate_contracts.sh" "$@" &
    /opt/homebrew/bin/bash "$SCRIPT_DIR/migrate_state_changes.sh" "$@" &
else
    "$SCRIPT_DIR/migrate_access_keys.sh" "$@" &
    "$SCRIPT_DIR/migrate_accounts.sh" "$@" &
    "$SCRIPT_DIR/migrate_contracts.sh" "$@" &
    "$SCRIPT_DIR/migrate_state_changes.sh" "$@" &
fi

wait

echo "Snapshot state for $2 and compact for $3 completed at $(date)"
