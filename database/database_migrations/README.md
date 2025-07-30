> **Note:** If you are starting the project from scratch (with a new, empty database), you do not need to run the migration scripts in this directory. These scripts are only necessary when migrating data from an existing database or upgrading shards. For new deployments, follow the standard database initialization procedures instead.

Database Migration Scripts

This directory contains scripts for migrating database shards and related data.

## Shard Migration Script

The `shard_migration.sh` script is the main migration orchestrator that runs multiple migration scripts in parallel.

### Usage

The script accepts the following command-line arguments:

- `--db_name`: Database name
- `--db_user`: Database username  
- `--db_password`: Database password
- `--host`: Database host
- `--port`: Database port

### Examples

#### Basic Example:
```bash
./shard_migration.sh --db_name my_database --db_user postgres --db_password mypassword --host localhost --port 5432
```

#### Local PostgreSQL Database:
```bash
./shard_migration.sh \
  --db_name read_rpc_db \
  --db_user postgres \
  --db_password secretpassword \
  --host localhost \
  --port 5432
```

#### Remote Database:
```bash
./shard_migration.sh \
  --db_name production_db \
  --db_user readrpc_user \
  --db_password prod_password123 \
  --host db.example.com \
  --port 5432
```

#### Using Environment Variables:
```bash
# Set environment variables first
export DB_NAME="my_database"
export DB_USER="postgres" 
export PGPASSWORD="mypassword"
export DB_HOST="localhost"
export DB_PORT="5432"

# Then run the script (it will use the environment variables)
./shard_migration.sh
```

### Prerequisites

1. **Make the script executable:**
   ```bash
   chmod +x shard_migration.sh
   ```

2. **Make all migration scripts executable:**
   ```bash
   chmod +x migrate_*.sh
   ```

3. **Ensure all required migration scripts exist:**
   - `migrate_access_keys.sh`
   - `migrate_accounts.sh`
   - `migrate_contracts.sh`
   - `migrate_state_changes.sh`

### How it Works

The `shard_migration.sh` script:

1. Parses command-line arguments and sets environment variables
2. Creates a log file named `migration_${DB_NAME}.log`
3. Runs four migration scripts in parallel using the `&` operator
4. Waits for all migrations to complete using the `wait` command
5. Logs start and completion times

### Output

- Migration progress and results are logged to `migration_${DB_NAME}.log`
- Console output shows start and completion timestamps
- Each individual migration script may produce its own output

### Notes

- All arguments are required for the script to function properly
- The script runs migrations in parallel to improve performance
- Make sure you have proper database permissions before running the migration
- Review the individual migration scripts to understand what data will be migrated
