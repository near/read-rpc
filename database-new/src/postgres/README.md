# Database

This is a helper crate that provides Postgres db manager.

Set up local PostgresDB
```
$ docker run --name some-postgres -p 5432:5432 --hostname some-postgres -e POSTGRES_PASSWORD=password -d postgres
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
sqlx migrate add --source src/postgres/migrations/shard_db <migration_name>
sqlx migrate add --source src/postgres/migrations/meta_db <migration_name>
```
#### Migration automatically applies to the database when the service starts

### psql
```
$ docker exec -it some-postgres psql -U postgres -d shard_<id>
$ docker exec -it some-postgres psql -U postgres -d meta
```

### Additional postgres db options
Put into `.env` file in the service root
```
DATABASE_META_URL = postgres://postgres:password@localhost:<port>/meta
DATABASE_SHARD_<id>_URL = postgres://postgres:password@localhost:<port>/shard_<id>
```
