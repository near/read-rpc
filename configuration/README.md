# Configuration

The configuration module is responsible for managing the configuration settings of the NEAR ReadRPC project.
It reads and parses the configuration from a TOML file and provides these settings to other parts of the application.

## Environment Variables

The configuration settings can also be provided through environment variables. 
This is useful when you want to change the settings without modifying the TOML file, 
such as when deploying the application in different environments. 
The environment variables are specified in the TOML file using the syntax `${VARIABLE_NAME}`.
For example, `${DATABASE_URL}` specifies the `DATABASE_URL` environment variable.

## Files

- `example.config.toml`: This file contains an example configuration for the NEAR ReadRPC. 
It includes settings for the general configuration, 
RPC server, transaction indexer, state indexer, epoch indexer, rightsizing, lake framework, and database.


## Configuration

The configuration settings are stored in a TOML file. The settings include:

- General settings like the chain ID and NEAR network RPC URL.
- RPC server settings like the server port and max gas burnt for contract function call.
- Transaction indexer settings like the indexer ID and port for the metrics server.
- State indexer settings like the indexer ID and port for the metrics server.
- Near State indexer settings like the port for the metrics server.
- Epoch indexer settings like the indexer ID.
- Rightsizing settings like the accounts and state changes to track.
- Lake framework settings like the AWS access key ID and secret access key.
- Database settings like the database connection string and user credentials.

## Usage

Put TOML file `config.toml` with configuration in the home root of the project.

## Default Configuration
Configuration improvement. Create default config.toml on start application to loaded parameters from the environment variables.
If `config.toml` is not found, the application will create default config toml with env variables.
This file to present all configuration around th environment variables [default_env_config.rs](configuration/src/default_env_config.rs)
Not present environment variables will be set to default values
See more details and information about each parameter in [example.config.toml](configuration/example.config.toml)

## Note

Please ensure that the configuration file is correctly formatted and all the required settings are provided. 
Incorrect or missing settings may cause the application to behave unexpectedly or fail.
