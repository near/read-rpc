# Database

This is a helper crate that provides Postgres db manager.

Set up local PostgresDB
```
$ docker-compose -f database/src/postgres/docker-compose.yml up -d
```

### Install `sqlx-cli`
```
$ cargo install sqlx-cli --no-default-features --features postgres
```

### Create separate migration directories for each database:
```
$ mkdir -p src/postgres/migrations/shard_db
$ mkdir -p src/postgres/migrations/meta_db
```

### Create Migration Files for Each Database
```
sqlx migrate add -r --source src/postgres/migrations/shard_db <migration_name>
sqlx migrate add -r --source src/postgres/migrations/meta_db <migration_name>
```
#### Migration automatically applies to the database when the service starts

### psql
```
$ docker exec -it postgres-shard_<id> psql -U postgres -d near_data
$ docker exec -it postgres-metadata psql -U postgres -d near_data
```

### Additional postgres db options
Put into `.env` file in the service root
```
META_DATABASE_URL=postgres://postgres:password@localhost:5422/near_data
SHARD_0_DATABASE_URL=postgres://postgres:password@localhost:5430/near_data
SHARD_1_DATABASE_URL=postgres://postgres:password@localhost:5431/near_data
SHARD_2_DATABASE_URL=postgres://postgres:password@localhost:5432/near_data
SHARD_3_DATABASE_URL=postgres://postgres:password@localhost:5433/near_data
SHARD_4_DATABASE_URL=postgres://postgres:password@localhost:5434/near_data
SHARD_5_DATABASE_URL=postgres://postgres:password@localhost:5435/near_data
```
