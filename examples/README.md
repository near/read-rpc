# Examples

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