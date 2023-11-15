# Database

This is a helper crate that provides Postgres db manager.

Set up local PostgresDB
```
$ docker run --name some-postgres -p 5432:5432 --hostname some-postgres -e POSTGRES_PASSWORD=password -d postgres
```
## Install diesel cli
```
$ cargo install diesel_cli --no-default-features  --features postgres --locked
```

## Create database
```
$ cd database
$ diesel setup --database-url postgres://postgres:password@localhost/near_data
```

## Run migrations
```
$ diesel migration redo --database-url postgres://postgres:password@localhost/near_data
```

### psql
```
$ docker exec -it some-postgres psql -U postgres -d near_data
```

### Additional postgres db options
Put into `.env` file in the service root
```
DATABASE_URL=postgres://postgres:password@localhost/near_data
DATABASE_NAME = "near_data"
```
