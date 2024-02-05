# Examples

# Postgres
This Docker Compose file demonstrating how to set up services using a PostgreSQL database as an alternative to ScyllaDB.
```
examples/postgres-compose.yml
```
In this Docker Compose file:

1. The database service sets up a PostgreSQL database using the official PostgreSQL Docker image.
2. The all services builds and runs with `postgres_db` feature.
3. Environment variables are used to configure the service to use PostgreSQL as the database backend:
    - `DATABASE_URL` specifies the connection URL to the PostgreSQL database.

This Docker Compose configuration sets up your PostgreSQL database and your service with the necessary environment variables to use PostgreSQL as an alternative to ScyllaDB.

To use these services with a Postgres database, you would run the command
```bash
docker-compose -f examples/postgres-compose.yml up --build
```

# Rightsizing

```
examples/rightsizing-compose.yml
```
In this Docker Compose file:

1. The `indexers` runs with your custom rightsizing.
2. Environment variables are used to configure the rightsizing:
    - `TRACKED_ACCOUNTS` specifies the specific accounts you want to track. Replace `["social.near"]` with the actual account names you want to track, separated by commas.
    - `TRACKED_CHANGES` specifies the specific changes you want to track. Replace `["state"]` with the actual types of changes you want to track, separated by commas.

This Docker Compose configuration sets up your database and the rightsizing service with the necessary environment variables to track specific accounts and specific changes.

To use these services with rightsizing, you would run the command
```bash
docker-compose -f examples/rightsizing-compose.yml up --build
```