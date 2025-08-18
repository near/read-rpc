#!/bin/bash
# Apply all database migrations for NEAR Read RPC project
#
# This script applies migrations to all database types:
# - Meta database
# - Shard databases  
# - Transaction details databases
#
# Prerequisites:
# - SQLx CLI installed: cargo install sqlx-cli --no-default-features --features native-tls,postgres
# - Environment variables set for database URLs
#
# Usage: ./scripts/apply_all_migrations.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if SQLx CLI is installed
if ! command -v sqlx &> /dev/null; then
    print_error "SQLx CLI is not installed. Please install it with:"
    echo "cargo install sqlx-cli --no-default-features --features native-tls,postgres"
    exit 1
fi

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS_DIR="$PROJECT_ROOT/database/src/postgres/migrations"

# Check if migrations directory exists
if [ ! -d "$MIGRATIONS_DIR" ]; then
    print_error "Migrations directory not found: $MIGRATIONS_DIR"
    exit 1
fi

print_status "Starting database migrations for NEAR Read RPC..."
print_status "Project root: $PROJECT_ROOT"

# Function to run migrations for a specific database and path
run_migrations() {
    local db_url="$1"
    local migration_path="$2"
    local db_name="$3"
    
    if [ -z "$db_url" ]; then
        print_warning "Skipping $db_name - database URL not set"
        return 0
    fi
    
    print_status "Applying migrations to $db_name..."
    print_status "Database URL: $db_url"
    print_status "Migration path: $migration_path"
    
    export DATABASE_URL="$db_url"
    
    if [ ! -d "$migration_path" ]; then
        print_warning "Migration directory not found: $migration_path"
        return 0
    fi
    
    # Check if there are any migration files
    if [ -z "$(find "$migration_path" -name "*.sql" -type f)" ]; then
        print_warning "No migration files found in $migration_path"
        return 0
    fi
    
    # Change to migration directory and run migrations
    cd "$migration_path"
    
    # Test database connection first
    if ! sqlx database create 2>/dev/null; then
        print_status "Database already exists or connection successful"
    fi
    
    # Run migrations
    if sqlx migrate run; then
        print_status "✓ Successfully applied migrations to $db_name"
    else
        print_error "✗ Failed to apply migrations to $db_name"
        return 1
    fi
    
    echo ""
}

# 1. Apply Meta Database migrations
print_status "=== Meta Database Migrations ==="
run_migrations "$META_DATABASE_URL" "$MIGRATIONS_DIR/meta_db" "Meta Database"

# Apply receipts and outcomes migrations to Meta Database
if [ ! -z "$META_DATABASE_URL" ]; then
    run_migrations "$META_DATABASE_URL" "$MIGRATIONS_DIR/tx_details/receipts_and_outcomes" "Meta Database - Receipts and Outcomes"
fi

# 2. Apply Shard Database migrations
print_status "=== Shard Database Migrations ==="

# List of shard database environment variables
SHARD_DBS=(
    "SHARD_0_DATABASE_URL"
    "SHARD_1_DATABASE_URL"
    "SHARD_2_DATABASE_URL"
    "SHARD_3_DATABASE_URL"
    "SHARD_4_DATABASE_URL"
    "SHARD_5_DATABASE_URL"
)

for shard_var in "${SHARD_DBS[@]}"; do
    shard_url="${!shard_var}"
    if [ ! -z "$shard_url" ]; then
        run_migrations "$shard_url" "$MIGRATIONS_DIR/shard_db" "Shard Database ($shard_var)"
        # Apply transactions migrations to each shard database
        run_migrations "$shard_url" "$MIGRATIONS_DIR/tx_details/transactions" "Shard Database ($shard_var) - Transactions"
    fi
done

# 3. Summary
print_status "=== Migration Summary ==="
print_status "All database migrations completed successfully!"
print_status ""
print_status "Next steps:"
print_status "1. Verify migrations with: sqlx migrate info"
print_status "2. Start your application services"
print_status "3. Check application logs for any issues"

# Return to original directory
cd "$PROJECT_ROOT"
