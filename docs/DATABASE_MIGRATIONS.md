# Database Migrations with SQLx

This document provides a comprehensive guide for managing database migrations in the NEAR Read RPC project using SQLx.

## Overview

The NEAR Read RPC project uses PostgreSQL databases with SQLx for migration management. The project has three distinct database types:

1. **Meta Database** - Stores blocks, chunks, validator metadata, and receipts/outcomes
2. **Shard Databases** - Stores state changes, partition-specific data, and transactions
3. **Transaction Details** - Split between meta database (receipts/outcomes) and shard databases (transactions)

## Database Structure

```
database/src/postgres/migrations/
├── meta_db/           # Meta database migrations (blocks, chunks, validators, receipts/outcomes)
├── shard_db/          # Shard database migrations (state changes, transactions)
└── tx_details/        # Transaction details database migrations
    ├── receipts_and_outcomes/  # Applied to meta database
    └── transactions/           # Applied to shard databases
```

## Prerequisites

### Install SQLx CLI

```bash
cargo install sqlx-cli --no-default-features --features native-tls,postgres
```

### Environment Variables

Set up the required database connection URLs:

```bash
# Meta database
export META_DATABASE_URL="postgresql://username:password@localhost:5432/meta_db"

# Shard databases (one per shard)
export SHARD_0_DATABASE_URL="postgresql://username:password@localhost:5432/shard_0_db"
export SHARD_1_DATABASE_URL="postgresql://username:password@localhost:5432/shard_1_db"
# ... additional shards as needed
```

## Migration Management

### Creating New Migrations

#### 1. Meta Database Migrations

```bash
# Navigate to meta database migrations directory
cd database/src/postgres/migrations/meta_db

# Create a new migration
sqlx migrate add -r <migration_name>
```

Example:
```bash
sqlx migrate add -r add_new_index_to_blocks
```

#### 2. Shard Database Migrations

```bash
# Navigate to shard database migrations directory
cd database/src/postgres/migrations/shard_db

# Create a new migration
sqlx migrate add -r <migration_name>
```

Example:
```bash
sqlx migrate add -r optimize_state_changes_table
```

#### 3. Transaction Details Migrations

Transaction details are split between different databases:

**For Receipts and Outcomes (Meta Database):**
```bash
# Navigate to receipts and outcomes migrations directory
cd database/src/postgres/migrations/tx_details/receipts_and_outcomes

# Create a new migration for receipts and outcomes
sqlx migrate add -r <migration_name>
```

**For Transactions (Shard Databases):**
```bash
# Navigate to transactions migrations directory
cd database/src/postgres/migrations/tx_details/transactions

# Create a new migration for transactions
sqlx migrate add -r <migration_name>
```

### Applying Migrations

#### 1. Meta Database

```bash
# Set the database URL
export DATABASE_URL=$META_DATABASE_URL

# Run migrations
cd database/src/postgres/migrations/meta_db
sqlx migrate run
```

#### 2. Shard Databases

Apply migrations to each shard database:

```bash
# For each shard
export DATABASE_URL=$SHARD_0_DATABASE_URL
cd database/src/postgres/migrations/shard_db
sqlx migrate run

export DATABASE_URL=$SHARD_1_DATABASE_URL
sqlx migrate run

# Repeat for all shards...
```

#### 3. Transaction Details Migrations

Transaction details migrations are applied to different databases:

```bash
# Receipts and outcomes migrations (applied to META database)
export DATABASE_URL=$META_DATABASE_URL
cd database/src/postgres/migrations/tx_details/receipts_and_outcomes
sqlx migrate run

# Transactions migrations (applied to SHARD databases)
export DATABASE_URL=$SHARD_0_DATABASE_URL
cd ../transactions
sqlx migrate run

export DATABASE_URL=$SHARD_1_DATABASE_URL
sqlx migrate run

# Repeat for all shards...
```

### Migration Scripts for Automation

Create convenience scripts to manage all databases at once:

#### Apply All Migrations Script

```bash
#!/bin/bash
# save as: scripts/apply_all_migrations.sh

set -e

echo "Applying migrations to Meta Database..."
export DATABASE_URL=$META_DATABASE_URL
cd database/src/postgres/migrations/meta_db
sqlx migrate run

# Apply receipts and outcomes migrations to Meta Database
echo "Applying receipts and outcomes migrations to Meta Database..."
cd ../tx_details/receipts_and_outcomes
sqlx migrate run

echo "Applying migrations to Shard Databases..."
for shard_url in $SHARD_0_DATABASE_URL $SHARD_1_DATABASE_URL $SHARD_2_DATABASE_URL $SHARD_3_DATABASE_URL $SHARD_4_DATABASE_URL $SHARD_5_DATABASE_URL; do
    if [ ! -z "$shard_url" ]; then
        echo "Migrating shard: $shard_url"
        export DATABASE_URL=$shard_url
        
        # Apply shard database migrations
        cd ../../shard_db
        sqlx migrate run
        
        # Apply transactions migrations to each shard
        cd ../tx_details/transactions
        sqlx migrate run
    fi
done

echo "All migrations applied successfully!"
```

### Checking Migration Status

#### View Applied Migrations

```bash
# Set appropriate DATABASE_URL for the target database
export DATABASE_URL=$META_DATABASE_URL

# Navigate to migrations directory
cd database/src/postgres/migrations/meta_db

# Check migration status
sqlx migrate info
```

#### View Migration History

```bash
# Show detailed migration history
sqlx migrate info --verbose
```

### Rolling Back Migrations

#### Revert Last Migration

```bash
# Set appropriate DATABASE_URL
export DATABASE_URL=$META_DATABASE_URL

# Navigate to migrations directory  
cd database/src/postgres/migrations/meta_db

# Revert the last migration
sqlx migrate revert
```

#### Revert to Specific Version

```bash
# Revert to a specific migration version
sqlx migrate revert --target-version <version_number>
```

## Database Setup from Scratch

### 1. Create Databases

```sql
-- Connect to PostgreSQL as superuser
CREATE DATABASE meta_db;
CREATE DATABASE shard_0_db;
CREATE DATABASE shard_1_db;
-- ... create additional shard databases as needed
```

### 2. Apply All Migrations

```bash
# Use the apply_all_migrations.sh script
chmod +x scripts/apply_all_migrations.sh
./scripts/apply_all_migrations.sh
```

## Migration Best Practices

### 1. Migration File Naming

SQLx uses timestamp-based naming:
```
YYYYMMDDHHMMSS_migration_name.up.sql
YYYYMMDDHHMMSS_migration_name.down.sql
```

### 2. Writing Safe Migrations

- Always test migrations on a copy of production data
- Use `IF EXISTS` and `IF NOT EXISTS` clauses where appropriate
- Make migrations atomic and reversible
- Include proper indexes for performance

Example migration:
```sql
-- up.sql
CREATE TABLE IF NOT EXISTS new_table (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_new_table_created_at ON new_table(created_at);

-- down.sql  
DROP INDEX IF EXISTS idx_new_table_created_at;
DROP TABLE IF EXISTS new_table;
```

### 3. Large Table Migrations

For large tables, consider:
- Creating new tables and migrating data in batches
- Using `CONCURRENTLY` for index creation
- Planning for downtime if necessary

### 4. Data Migrations

When migrating data, use transactions and include rollback logic:

```sql
-- up.sql
BEGIN;

-- Migration logic here
UPDATE existing_table SET new_column = 'default_value' WHERE new_column IS NULL;

COMMIT;
```

## Troubleshooting

### Common Issues

1. **Migration fails midway**
   ```bash
   # Check current state
   sqlx migrate info
   
   # Fix the issue and retry
   sqlx migrate run
   ```

2. **Database connection issues**
   ```bash
   # Test connection
   psql $DATABASE_URL -c "SELECT version();"
   ```

3. **Migration version conflicts**
   ```bash
   # Reset migrations (DANGEROUS - only for development)
   sqlx migrate reset
   ```

### Recovery Procedures

1. **Partial migration failure**
   - Review the error logs
   - Manually fix any partial changes
   - Re-run the migration

2. **Rollback when down migration fails**
   - Manually revert changes using SQL
   - Update the `_sqlx_migrations` table if necessary

## Integration with Application

The project's database module automatically handles connections. Ensure migrations are applied before starting the application:

```bash
# In your deployment script
./scripts/apply_all_migrations.sh

# Then start the application
cargo run --bin rpc-server
```

## Development Workflow

1. Create feature branch
2. Add necessary migrations using `sqlx migrate add`
3. Test migrations on development database
4. Commit migration files with your changes
5. Include migration instructions in PR description
6. Apply migrations to staging/production after deployment

## Environment-Specific Considerations

### Development
- Use local PostgreSQL instance
- Apply migrations manually or via script

### Production
- Always backup databases before migration
- Test migrations on staging first
- Plan for minimal downtime
- Have rollback plan ready

### Docker Compose
The project includes Docker Compose setup. Migrations should be applied after containers are up:

```bash
docker-compose up -d postgres
# Wait for PostgreSQL to be ready
./scripts/apply_all_migrations.sh
docker-compose up
```
